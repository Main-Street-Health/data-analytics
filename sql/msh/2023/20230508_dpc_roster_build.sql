------------------------------------------------------------------------------------------------------------------------
/*
  DPC
  Moving to Monthly data pulls on the 5th
  Prior to data pull need to flag any patients no longer qualifying for dpc as inactive
  Need to update all pat provider attribution
    - Use most recent successful npi relationship if exists
    - Otherwise use the patient contacts
    - Otherwise use the phys hier
*/
------------------------------------------------------------------------------------------------------------------------
-- COOP Side
-- CREATE OR REPLACE VIEW _dpc_roster AS
-- WITH
--     patient_docs AS ( SELECT
--                           sp.patient_id
--                         , mp.first_name
--                         , mp.last_name
--                         , mp.npi
--                         , 0 rank -- appts_physician_rank starts at 1, primary phys will trump
--                       FROM
--                           supreme_pizza sp
--                           left JOIN patient_contacts pc ON pc.patient_id = sp.patient_id and pc.relationship = 'physician'::TEXT
--                           left JOIN msh_physicians mp ON pc.contact_id = mp.contact_id AND LENGTH(mp.npi::TEXT) = 10 and not mp.deleted
--                       WHERE
--                         sp.is_dpc
--                       and pc.is_primary
--                       UNION
--                       SELECT DISTINCT ON (sp2.patient_id)
--                           sp2.patient_id
--                         , mp2.first_name
--                         , mp2.last_name
--                         , mp2.npi
--                         , hier.appts_physician_rank
--                       FROM
--                           supreme_pizza sp2
--                           left join stage.patient_physician_location_hierarchy hier on hier.patient_id = sp2.patient_id
--                           left join msh_physicians mp2 ON hier.msh_physician_id = mp2.id AND LENGTH(mp2.npi::TEXT) = 10 and not mp2.deleted
--                       WHERE
--                         sp2.is_dpc
--                       )
-- SELECT DISTINCT
--     p.patient_id
--   , p.patient_first_name
--   , p.patient_last_name
--   , p.gender              AS patient_gender
--   , p.date_of_birth       AS patient_dob
--   , p.date_of_death       AS patient_dod
--   , p.address_line1       AS patient_address_1
--   , p.address_line2       AS patient_address_2
--   , p.address_city        AS patient_city
--   , p.address_state       AS patient_state
--   , p.address_postal_code AS patient_zip
--   , p.mbi                 AS patient_mbi
--   , doc.first_name        AS provider_first_name
--   , doc.last_name         AS provider_last_name
--   , doc.npi               AS provider_npi
--   , doc.rank              AS provider_rank
-- FROM
--     ent.patients p
--     JOIN patient_docs doc ON doc.patient_id = p.patient_id
-- WHERE
--     p.mbi IS NOT NULL;
--
-- SELECT count(*), count(distinct patient_id) FROM _dpc_roster;
call cb.x_util_create_fdw_member_doc();


-- get mbi<->npi's that were successful in most recent job for that mbi
DROP TABLE IF EXISTS _previously_successfull_mbi_to_npi;
CREATE TEMP TABLE _previously_successfull_mbi_to_npi AS
WITH
    exploded    AS ( SELECT
                         j.id           job_id
                       , j.npi
                       , UNNEST(j.mbis) mbi
                     FROM
                         dpoc_bulk_export_jobs j
                     WHERE
                           j.npi IS NOT NULL
                       AND j.mbis IS NOT NULL )
  , most_recent AS ( SELECT DISTINCT ON (e.mbi)
                         e.mbi
                       , e.npi
                       , e.job_id
                     FROM
                         exploded e
                     ORDER BY
                         e.mbi, e.job_id DESC )
SELECT
    mr.mbi
  , mr.npi
FROM
    most_recent mr
WHERE
    NOT EXISTS( SELECT
                    1
                FROM
                    dpoc_bulk_export_job_errors je
                WHERE
                      je.dpoc_bulk_export_job_id = mr.job_id
                  AND je.mbi = mr.mbi )
;
SELECT count(*) FROM _previously_successfull_mbi_to_npi;

DROP TABLE IF EXISTS _failed_mbi_to_npi;
CREATE TEMP TABLE _failed_mbi_to_npi AS
SELECT DISTINCT
    j.npi
  , je.mbi
FROM
    dpoc_bulk_export_jobs j
    JOIN dpoc_bulk_export_job_errors je ON j.id = je.dpoc_bulk_export_job_id
WHERE
    j.npi IS NOT NULL;
SELECT count(*) FROM _failed_mbi_to_npi;


-- Layer in prev successful relationships to ranked relationships in golgi view
DROP TABLE IF EXISTS _new_pats;
CREATE TEMP TABLE _new_pats AS
SELECT DISTINCT ON (r.patient_id)
    r.patient_id
  , r.patient_first_name
  , r.patient_last_name
  , r.patient_gender
  , r.patient_dob
  , r.patient_dod
  , r.patient_address_1
  , r.patient_address_2
  , r.patient_city
  , r.patient_state
  , r.patient_zip
  , r.patient_mbi
  , r.provider_first_name
  , r.provider_last_name
  , r.provider_npi
  , r.provider_rank
FROM
    fdw_member_doc._dpc_roster r
LEFT JOIN _failed_mbi_to_npi f ON f.mbi = r.patient_mbi and f.npi = r.provider_npi::text
WHERE
    r.provider_npi is not null
    and NOT EXISTS( SELECT 1 FROM _previously_successfull_mbi_to_npi ps WHERE ps.mbi = r.patient_mbi )
ORDER BY
    r.patient_id, case when f.mbi is null then 1 else 2 end, r.provider_rank
;
SELECT count(*) FROM _new_pats;

-- combine both previously successful npi-mbi with coop mapping
drop table _pract_pats;
create temp table _pract_pats (
    mbi text not null,
    npi text not null,
    unique(mbi, npi)
);
INSERT
INTO
    _pract_pats (mbi, npi)
SELECT
    mbi
  , npi
FROM
    _previously_successfull_mbi_to_npi ps
WHERE
    EXISTS( SELECT 1 FROM fdw_member_doc._dpc_roster r WHERE r.patient_mbi = ps.mbi )
;


INSERT
INTO
    _pract_pats (mbi, npi)
SELECT DISTINCT
    patient_mbi
  , provider_npi
FROM
    _new_pats;





-- TODO: check for any source ID MBI conflicts and update

/* Patients */
-- inactive: update existing dpoc_patients no longer in the roster
UPDATE dpoc_patients dp
SET
    is_active = FALSE, updated_at = now()
-- ;select count(*) from dpoc_patients dp
WHERE
    dp.is_active
    and NOT EXISTS(SELECT 1 FROM fdw_member_doc._dpc_roster pdp WHERE pdp.patient_id = dp.source_id);

-- new: insert patients
INSERT
INTO
    public.dpoc_patients (mbi, address_line_1, address_line_2, city, dob,
                          dod, first_name, gender, last_name, state, zip, inserted_at, updated_at, source_id,
                          dpoc_provider_org_id)
SELECT
    patient_mbi
  , patient_address_1
  , patient_address_2
  , patient_city
  , patient_dob
  , patient_dod
  , patient_first_name
  , LOWER(patient_gender) gender
  , patient_last_name
  , patient_state
  , patient_zip
  , NOW()                 inserted_at
  , NOW()                 updated_at
  , patient_id
  , 1 dpoc_org_id -- msh org = 1
FROM
    fdw_member_doc._dpc_roster pats
where not exists(select 1 from dpoc_patients dp where dp.source_id = pats.patient_id)
ON CONFLICT (mbi) DO NOTHING
;

-- reactivate
UPDATE dpoc_patients dp
SET
    is_active = true, updated_at = now()
-- ;select count(*) from dpoc_patients dp
WHERE
    not dp.is_active
    and EXISTS(SELECT 1 FROM fdw_member_doc._dpc_roster pdp WHERE pdp.patient_id = dp.source_id);

/* practitioners */
INSERT
INTO
    public.dpoc_practitioners(npi, first_name, last_name, inserted_at, updated_at, dpoc_provider_org_id)
SELECT DISTINCT
    provider_npi
  , provider_first_name
  , provider_last_name
  , NOW() inserted_at
  , NOW() updated_at
  , 1     dpoc_org_id -- msh org = 1
FROM
--     fdw_member_doc._dpc_roster pats
    _new_pats pats
WHERE
    NOT EXISTS(SELECT 1 FROM dpoc_practitioners p WHERE p.npi = pats.provider_npi::TEXT)
;


/* practitioner to patient */
-- archive old
WITH
    archived AS (
        INSERT
            INTO
                dpoc_practitioner_group_patients_history(id, npi, mbi, is_registered, last_refreshed_at,
                                                         dpoc_provider_org_id, archived_at, inserted_at, updated_at)
                SELECT
                    id
                  , npi
                  , mbi
                  , is_registered
                  , last_refreshed_at
                  , dpoc_provider_org_id
                  , NOW() archived_at
                  , inserted_at
                  , updated_at
                FROM
                    dpoc_practitioner_group_patients gp
                WHERE
                    NOT EXISTS(SELECT
                                   1
                               FROM
                               _pract_pats pp
                               WHERE
                                     pp.npi = gp.npi
                                 AND pp.mbi = gp.mbi)
                RETURNING * )
DELETE
FROM
    dpoc_practitioner_group_patients pgp
WHERE
    EXISTS(SELECT 1 FROM archived a WHERE a.id = pgp.id)
;

-- create new attribution groups (npi <-> mbi)
INSERT
INTO
    dpoc_practitioner_group_patients (npi, mbi, inserted_at, updated_at, dpoc_provider_org_id)
SELECT
    pp.npi
  , pp.mbi
  , NOW() inserted_at
  , NOW() updated_at
  , 1     dpoc_org_id -- msh org = 1
FROM _pract_pats pp
ON CONFLICT (npi, mbi) DO NOTHING ;

-- one provider has 1212 pats, 1k is the limit
-- DROP TABLE IF EXISTS _prov_overflow_trunc;
-- CREATE TEMP TABLE _prov_overflow_trunc AS
-- SELECT gp.id, case when p.source_id in ( '367376', '367396', '367398', '367422', '367445', '367451', '367457', '367459', '367469', '367472', '367474', '367478', '367481', '367485', '367488', '367494', '367503', '367509', '367521', '367522', '367523', '367525', '367535', '367544', '367568', '367569', '367577', '367581', '367584', '367586', '367589', '367605', '367633', '367637', '367638', '367648', '367665', '367673', '367683', '367686', '367690', '367692', '367693', '367697', '367700', '367706', '367710', '367720', '367732', '367736', '367738', '367745', '367759', '367767', '367778', '367781', '367800', '367823', '367832', '367835', '367846', '367848', '367851', '367872', '367877',
-- '367886', '367889', '367896', '367899', '367926', '367932', '367935', '367937', '367944', '367957', '367961', '367967', '367975', '367991', '368009', '368015', '368022', '368041', '368042', '368047', '368048', '368054', '368056', '368057', '368060', '368064', '368073', '368085', '368089', '368092', '368095', '368103', '368106', '368114', '368115', '368121', '368139', '368156', '368160', '368162', '368167', '368182', '368185', '368186', '368191', '368214',
-- '368227', '368229', '368265', '368297', '368298', '368305', '368306', '368307', '368308', '368339', '368340', '368343', '368374', '368405', '368408', '368412', '368419', '368427', '368435', '368442', '368459', '368468', '368469', '368478', '368482', '368488', '368494', '368513', '368517', '368529', '368533', '368543', '368544', '368546', '368564', '368572', '368576', '368586', '368590', '368598',
-- '368618', '368632', '368643', '368660', '368666', '368670', '368683', '368687', '368693', '368695', '368704', '368711', '368713', '368714', '368721', '368728', '375935', '375940', '375951', '375956', '375963'
--     ) then 1 else 2 end on_list
-- FROM
--     dpoc_practitioner_group_patients gp
-- join dpoc_patients p on p.mbi = gp.mbi
-- WHERE
--     npi = '1881642791'
-- order by on_list, p.source_id
-- limit 999;
-- DELETE
-- FROM
--     dpoc_practitioner_group_patients gp
-- WHERE gp.npi = '1881642791' and not exists(select 1 from _prov_overflow_trunc t where t.id = gp.id);
;
SELECT count(*) FROM _new_pats WHERE provider_npi = '1881642791';
SELECT *
FROM
    dpoc_patients WHERE source_id = '294912' or mbi = '4P34UP3JT06' or  '4P34UP3JT06' = any(mbis);

SELECT *
FROM
    ;
------------------------------------------------------------------------------------------------------------------------
/* Run Deus.DPOC.DPOC.register_all_unregistered() to register pat, providers, attribution groups */
------------------------------------------------------------------------------------------------------------------------
-- need to export Patient
create table junk.dpc_pat_export_providers_20230509 as
SELECT DISTINCT
    p.*
FROM
    dpoc_practitioners p
    JOIN dpoc_practitioner_group_patients gp ON p.npi = gp.npi and gp.is_registered
    JOIN dpoc_patients dp ON dp.mbi = gp.mbi and dp.is_registered
WHERE
      p.is_registered
  AND dp.bene_id ISNULL;

delete FROM
    junk.dpc_pat_export_providers_20230509 p
    where exists(select 1 from dpoc_practitioner_group_patients gp where p.npi = gp.npi and not gp.is_registered)
;

drop table junk.dpc_pat_export_providers_20230509_exl;
create table junk.dpc_pat_export_providers_20230509_exl as
SELECT npi FROM dpoc_bulk_export_jobs where inserted_at::date = now()::date;


SELECT *
FROM
    dpoc_bulk_export_jobs where inserted_at::date = now()::date order by id ;

SELECT *
FROM
    dpoc_practitioner_group_patients where npi = '1063494193';

-- need to export Coverage
SELECT DISTINCT
    p.*
FROM
    dpoc_practitioners p
    JOIN dpoc_practitioner_group_patients gp ON p.npi = gp.npi
    JOIN dpoc_patients dp ON dp.mbi = gp.mbi
WHERE
      p.is_registered
  AND NOT EXISTS(SELECT 1 FROM dpoc_bulk_export_jobs j WHERE dp.mbi = ANY (j.mbis) AND j.export_type ~* 'Coverage')



select * from v_dpoc_prov_to_pull
-- drop VIEW v_dpoc_prov_to_pull
CREATE or replace VIEW v_dpoc_prov_to_pull
            (npi, last_pull_date, export_type, pats, nd_pats, nd_new_pats, most_recent_pat) AS
    ;
WITH
    _split_exports   AS ( SELECT
                             pract.npi
                           , pract.id                                                                 AS pract_id
                           , UNNEST(REGEXP_SPLIT_TO_ARRAY(j.export_type, ','))                           export_type
                           , COALESCE(j.inserted_at, '2020-01-01'::DATE::TIMESTAMP WITHOUT TIME ZONE) AS last_pull_date
                         FROM
                             dpoc_practitioners pract
                             LEFT JOIN dpoc_bulk_export_jobs j
                                       ON j.dpoc_practitioner_id = pract.id AND j.completed_at IS NOT NULL
                         WHERE
                             pract.attribution_group_id IS NOT NULL )
  , _last_prov_pull AS ( SELECT DISTINCT ON (npi, export_type)
                             npi
                           , pract_id
                           , export_type
                           , last_pull_date
                         FROM
                             _split_exports
                         ORDER BY npi, export_type, last_pull_date desc )
  , _rollup         AS ( SELECT
                             lpp.npi
                           , lpp.pract_id
                           , lpp.last_pull_date
                           , lpp.export_type
                           , COUNT(pgp.mbi)                                            AS pats
                           , COUNT(DISTINCT pgp.mbi)                                   AS nd_pats
                           , COUNT(DISTINCT pgp.mbi)
                             FILTER (WHERE pgp.inserted_at::DATE > lpp.last_pull_date) AS nd_new_pats
                           , MAX(pgp.inserted_at::DATE)                                AS most_recent_pat
                         FROM
                             _last_prov_pull lpp
                             JOIN dpoc_practitioner_group_patients pgp ON pgp.npi::TEXT = lpp.npi::TEXT
                         GROUP BY lpp.npi, lpp.pract_id, lpp.last_pull_date, lpp.export_type )
SELECT
    r.npi
  , r.last_pull_date
  , r.export_type
  , r.pats
  , r.nd_pats
  , r.nd_new_pats
  , r.most_recent_pat
FROM
    _rollup r
where r.most_recent_pat > r.last_pull_date;



------------------------------------------------------------------------------------------------------------------------
/* pull since date */
------------------------------------------------------------------------------------------------------------------------
-- create table junk.dpc_prov_to_pull_20230407 as
WITH
    _split_exports   AS ( SELECT
                             pract.npi
                           , pract.id                                                                 AS pract_id
                           , UNNEST(REGEXP_SPLIT_TO_ARRAY(j.export_type, ','))                           export_type
                           , COALESCE(j.inserted_at, '2020-01-01'::DATE::TIMESTAMP WITHOUT TIME ZONE) AS last_pull_date
                         FROM
                             dpoc_practitioners pract
                             LEFT JOIN dpoc_bulk_export_jobs j
                                       ON j.dpoc_practitioner_id = pract.id AND j.completed_at IS NOT NULL
                         WHERE
                             pract.attribution_group_id IS NOT NULL )
  , _last_prov_pull AS ( SELECT DISTINCT ON (npi, export_type)
                             npi
                           , pract_id
                           , export_type
                           , last_pull_date
                         FROM
                             _split_exports
                         ORDER BY npi, export_type, last_pull_date desc )
  , _rollup         AS ( SELECT
                             lpp.npi
                           , lpp.pract_id
                           , lpp.last_pull_date
                           , lpp.export_type
                           , COUNT(pgp.mbi)                                            AS pats
                           , COUNT(DISTINCT pgp.mbi)                                   AS nd_pats
                           , COUNT(DISTINCT pgp.mbi)
                             FILTER (WHERE pgp.inserted_at::DATE > lpp.last_pull_date) AS nd_new_pats
                           , MAX(pgp.inserted_at::DATE)                                AS most_recent_pat
                         FROM
                             _last_prov_pull lpp
                             JOIN dpoc_practitioner_group_patients pgp ON pgp.npi::TEXT = lpp.npi::TEXT
                         GROUP BY lpp.npi, lpp.pract_id, lpp.last_pull_date, lpp.export_type )
SELECT
    r.npi
  , min(r.last_pull_date)::date - 1 pull_from_date
--   , string_agg(r.export_type, ',')
--   , r.last_pull_date
--   , r.export_type
--   , r.pats
--   , r.nd_pats
--   , r.nd_new_pats
--   , r.most_recent_pat
FROM
    _rollup r
where last_pull_date < '2023-04-01'
and export_type in ('Coverage', 'ExplanationOfBenefit')
GROUP BY 1;

SELECT state, count(*)
FROM oban.oban_jobs
WHERE
      worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND state != 'completed'
GROUP BY 1
ORDER BY
    2
;


SELECT *
FROM
    oban.oban_crons;
--     update oban.oban_jobs
--     set state = 'available', scheduled_at = now(), max_attempts = max_attempts + 1
SELECT * FROM oban.oban_jobs
WHERE
      worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND state = 'discarded';

SELECT * FROM oban.oban_queues;
SELECT *
FROM
    oban.oban_producers;

SELECT * FROM prd.patient_med_adherence_measures;

DROP TABLE IF EXISTS _unregistered_providers;
CREATE TEMP TABLE _unregistered_providers AS
SELECT
    p.npi
  , mp.first_name
  , mp.last_name
  , rp.name  rp
  , rpo.name rpo
, count(distinct gp.mbi)
FROM
    dpoc_practitioners p
    LEFT JOIN dpoc_practitioner_group_patients gp ON p.npi = gp.npi
    LEFT JOIN fdw_member_doc.msh_physicians mp ON p.npi = mp.npi::text
    LEFT JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_mbi = gp.mbi
    LEFT JOIN fdw_member_doc.referring_partners rp ON rp.id = sp.primary_referring_partner_id
    LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id
WHERE
    NOT p.is_registered
GROUP BY 1,2,3,4,5
;

SELECT * FROM _unregistered_providers order by count DESC ;