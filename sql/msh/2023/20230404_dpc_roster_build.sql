------------------------------------------------------------------------------------------------------------------------
/*
  DPC
  Moving to Monthly data pulls on the 5th
  Prior to data pull need to flag any patients no longer qualifying for dpc as inactive
  Need to update all pat provider attribution
*/
------------------------------------------------------------------------------------------------------------------------
-- COOP Side
-- CREATE OR REPLACE VIEW _dpc_roster AS
-- WITH
--     patient_docs AS ( SELECT DISTINCT ON (pc.patient_id)
--                           pc.patient_id
--                         , pc.contact_id
--                         , pc.is_primary
--                         , mp.first_name
--                         , mp.last_name
--                         , mp.npi
--                       FROM
--                           msh_patient_integration_configs ic
--                           JOIN patient_contacts pc ON pc.patient_id = ic.patient_id
--                           JOIN msh_physicians mp ON pc.contact_id = mp.contact_id
--                       WHERE
--                             pc.relationship = 'physician'::TEXT
--                         AND ic.dpc
--                         AND LENGTH(mp.npi::TEXT) = 10
--                       ORDER BY pc.patient_id, pc.is_primary DESC, pc.inserted_at DESC )
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
-- FROM
--     ent.patients p
--     JOIN patient_docs doc ON doc.patient_id = p.patient_id
-- WHERE
--     p.mbi IS NOT NULL;
--
-- SELECT count(*) FROM _dpc_roster;




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
    fdw_member_doc._dpc_roster pats
WHERE
    NOT EXISTS(SELECT 1 FROM dpoc_practitioners p WHERE p.npi = pats.provider_npi::TEXT)
RETURNING *;


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
                                   fdw_member_doc._dpc_roster r
                               WHERE
                                     r.provider_npi::TEXT = gp.npi
                                 AND r.patient_mbi = gp.mbi)
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
    provider_npi
  , patient_mbi
  , NOW() inserted_at
  , NOW() updated_at
  , 1     dpoc_org_id -- msh org = 1
FROM
    fdw_member_doc._dpc_roster pats
ON CONFLICT (npi, mbi) DO NOTHING ;


------------------------------------------------------------------------------------------------------------------------
/* Run Deus.DPOC.DPOC.register_all_unregistered() to register pat, providers, attribution groups */
------------------------------------------------------------------------------------------------------------------------
-- need to export Patient
SELECT DISTINCT
    p.*
FROM
    dpoc_practitioners p
    JOIN dpoc_practitioner_group_patients gp ON p.npi = gp.npi
    JOIN dpoc_patients dp ON dp.mbi = gp.mbi
WHERE
      p.is_registered
  AND NOT EXISTS(SELECT 1 FROM dpoc_bulk_export_jobs j WHERE dp.mbi = ANY (j.mbis) AND j.export_type ~* 'Patient');

CREATE INDEX on dpoc_bulk_export_jobs USING GIN (mbis);
SELECT *
FROM
    dpoc_bulk_export_jobs;

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