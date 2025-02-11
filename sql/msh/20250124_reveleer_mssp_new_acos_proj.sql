
-- Current (OLD) table
SELECT * FROM raw.reveleer_mssp_cqm_aco_membrs_20240624;

-- new table from sean, existing aco is A5404
select * from raw.reveleer_mssp_cqm_aco_members_2024_q3_alr;

CREATE INDEX ON raw.reveleer_mssp_cqm_aco_members_2024_q3_alr(patient_id);

ALTER TABLE raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
    ADD reveleer_project_id bigint;

SELECT *
FROM
    reveleer_projects where id = 298 ;

INSERT
INTO
    public.reveleer_projects (name, payer_id, state_payer_id, reveleer_id, inserted_at, updated_at,
                              measures_to_send, yr, is_active)
VALUES
    ('mssp_cqm_aco_the_rest', 81, NULL, '2886', NOW(), NOW(),
     '{cbp_controlling_high_blood_pressure,hbd_hemoglobin_a1c_control_for_patients_with_diabetes,dsf_depression}', 2024,
     TRUE)
returning *
;

update raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
    set reveleer_project_id = case when contract_id = 'A5404' then 298 else 397 end;



SELECT
    reveleer_project_id
  , COUNT(*)
FROM
    raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
GROUP BY
    1
;
SELECT
    COUNT(*) n
  , COUNT(DISTINCT (patient_id, reveleer_project_id)) nd_pat_contract
  , COUNT(DISTINCT patient_id) nd_pat
FROM
    raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
WHERE
    patient_id IS NOT NULL;


SELECT *
FROM
    ( SELECT
          patient_id
        , COUNT(DISTINCT contract_id)     n
        , ARRAY_AGG(DISTINCT contract_id) contracts
      FROM
          raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
      WHERE
          patient_id IS NOT NULL
      GROUP BY
          1
      HAVING
          COUNT(DISTINCT contract_id) > 1 ) x
WHERE
    'A5404' = ANY (x.contracts)

;


call sp_reveleer_data_stager();
    ;
SELECT reveleer_project_id, count(*)
FROM
    reveleer_chase_file_details cfd
where inserted_at > now() - '2 weeks'::interval and reveleer_project_id in (298, 397)
GROUP BY reveleer_project_id
;
SELECT *
FROM
    fdw_file_router.ftp_servers ;
SELECT *
FROM
    reveleer_projects order by id desc;
SELECT external_chase_id is null missing_reveleer_chase_id,  count(*)
FROM
    reveleer_chases rc
WHERE
    reveleer_project_id = 397
GROUP BY external_chase_id
;

------------------------------------------------------------------------------------------------------------------------
/* delete to resend */
------------------------------------------------------------------------------------------------------------------------
--
-- DELETE
-- -- select *
-- FROM
--     reveleer_attribute_file_details
-- WHERE reveleer_project_id = 397;
-- DELETE
-- -- select *
-- FROM
--     reveleer_chase_file_details
-- WHERE reveleer_project_id = 397;
-- DELETE
-- -- select *
-- FROM
--     reveleer_compliance_file_details
-- WHERE reveleer_project_id = 397;
-- DELETE
-- -- select *
-- FROM
--     reveleer_files
-- WHERE reveleer_project_id = 397;

SELECT distinct contract_id
FROM
    raw.reveleer_mssp_cqm_aco_members_2024_q3_alr;

SELECT *
FROM
    analytics.public.reveleer_cca_pdfs p
    JOIN reveleer_chases c ON p.patient_id = c.patient_id
WHERE
    c.reveleer_project_id = 397
and p.yr = 2024
;

SELECT count(*)
FROM
    analytics.public.reveleer_chase_file_details p
WHERE
    p.reveleer_project_id = 397
and p.reveleer_file_id is not null
;
SELECT *
FROM
    analytics.oban.oban_jobs
where queue = 'reveleer'
ORDER BY
    id DESC;


SELECT *
FROM
    reveleer_chases c
WHERE
    c.reveleer_project_id = 397
and c.external_chase_id is not null
      ;


DROP TABLE IF EXISTS _potentially_missing_docs;
CREATE TEMP TABLE _potentially_missing_docs AS
SELECT
    c.id chase_id
  , d.id doc_id
  , d.type_id
FROM
    reveleer_chases c
    JOIN fdw_member_doc.documents d ON d.patient_id = c.patient_id
        AND d.deleted_at ISNULL
WHERE
      c.reveleer_project_id = 397
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      reveleer_cca_pdfs p
                  WHERE
                      p.document_id = d.id )
;
SELECT type_id, count(*)
FROM
    _potentially_missing_docs
GROUP BY 1
order by 2 desc
;

SELECT *
FROM
    fdw_file_router.ftp_servers where name ~* 'reveleer';

SELECT *
FROM
    reveleer_projects where id = 397;
-- 2886
SELECT *
FROM
    fdw_file_router.external_files
WHERE
    ftp_server_id = 403
and s3_bucket is not null
ORDER BY
    id DESC;
SELECT *
FROM
--     inbound_file_logs
inbound_file_config
WHERE
    ftp_server_name = 'reveleer_chase_success'
ORDER BY
    id DESC;

SELECT *
FROM
    analytics.oban.oban_jobs
WHERE
      queue = 'inbound_files_processing'
  AND args ->> 'name' = 'reveleer_chase_success'
ORDER BY
    id DESC;



call sp_reveleer_stage_new_cca_pdfs_for_upload();
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
         raw.reveleer_mssp_cqm_aco_members_2024_q3_alr a
;
SELECT *
FROM
    fdw_member_doc.qm_ref_measures where key ~* 'cbp';

-- 4294 3415 total
-- 492  460 emr
-- 3802 2955 proxy

DROP TABLE IF EXISTS _coop_measures;
CREATE TEMP TABLE _coop_measures AS
SELECT
    pm.measure_source_key
  , pm.id
  , pm.patient_id
  , pm.measure_status_key
FROM
    raw.reveleer_mssp_cqm_aco_members_2024_q3_alr a
    JOIN fdw_member_doc.qm_patient_measures pm
         ON pm.patient_id = a.patient_id
             AND pm.measure_key = 'cbp_controlling_high_blood_pressure'
             AND pm.operational_year = 2024
             AND pm.is_active
WHERE
      contract_id = 'A5404'
  AND a.patient_id IS NOT NULL;


SELECT
    measure_source_key
  , COUNT(*)
FROM
    _coop_measures
GROUP BY
    measure_source_key
;


SELECT
    measure_status_key
  , COUNT(*)
FROM
    _coop_measures
GROUP BY
    1
;

SELECT *
FROM
    _coop_measures;

-- in reveleer 3213
SELECT count(*)
FROM
    reveleer_chases rc
    JOIN _coop_measures cm ON rc.patient_id = cm.patient_id
                                  AND rc.measure_code = 'CBP'
                                  AND rc.yr = 2024
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _aco_lookup;
CREATE TEMP TABLE _aco_lookup AS
SELECT *
FROM
    ( VALUES
          ('A5334', 'Hickory'),
      ('A5399', 'Juniper'),
          ('A5401', 'Maple'),
          ('A5402', 'Poplar'),
          ('A5403', 'Sycamore'),
          ('A5404', 'Willow'),
          ('A5421', 'Dogwood'),
          ('A5438', 'Hawthorn'),
          ('A5439', 'Cottonwood'),
          ('A5502', 'Cedar'),
          ('A5400', 'Magnolia') ) x(contract_id, payer_name);

SELECT
    rc.id             msh_chase_id
  , external_chase_id reveleer_chase_id
  , rp.reveleer_id    reveleer_project_id
  , al.payer_name
  , rc.patient_id
  , rc.measure_code
  , rc.due_date
  , rc.qm_patient_measure_ids
FROM
    raw.reveleer_mssp_cqm_aco_members_2024_q3_alr r
    JOIN _aco_lookup al ON al.contract_id = r.contract_id
    JOIN reveleer_chases rc ON rc.patient_id = r.patient_id AND rc.yr = 2024
    JOIN reveleer_projects rp ON r.reveleer_project_id = rp.id
;






------------------------------------------------------------------------------------------------------------------------
/* missing docs */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    reveleer_chases
WHERE
    external_chase_id = '28651766';
SELECT * FROM reveleer_cca_pdfs where patient_id = 438;
SELECT * FROM reveleer_cca_pdfs where patient_id = 554;
SELECT * FROM reveleer_cca_pdfs where patient_id = 603;
SELECT file_name FROM reveleer_cca_pdfs where patient_id in(438, 554,603) ;
------------------------------------------------------------------------------------------------------------------------
/* from sean */
------------------------------------------------------------------------------------------------------------------------
drop table if exists _pats;
create temp table _pats as
with cte as (
select
    x.inbound_file_id,
    x.id raw_id,
    gm.patient_id,
    x.bene_mbi_id mbi,
    x.payer_id,
    smm.member_id,
    substring(x.meta->'extra_args'->>'original_name' from 'A\d{4}') contract_id,
    (substring(x.meta->'extra_args'->>'original_name' from '.*(\d{4})Q'))::int measure_year,
    coalesce(htn_age = '1' and htn_dx = '1' and htn_encounter = '1' and htn_exclusion = '.',false) is_cbp,
    coalesce(dep_age = '1' and dep_encounter = '1' and dep_exclusion = '.',false) is_depression,
    coalesce(dm_age = '1' and dm_dx = '1' and dm_encounter = '1' and dm_exclusion = '.',false) is_hbd

from
    raw.mssp_alr_mcqm x
        left join prd.source_member_mappings smm on smm.payer_member_id = lower(trim(x.bene_mbi_id))
            and smm.is_mbi
            and smm.payer_id = 81
        left join gmm.global_mco_member_mappings mco on mco.member_id = smm.member_id
        left join gmm.global_members gm on gm.id = mco.global_member_id and gm.patient_id is not null
where
    inbound_file_id >= 20936004 /*2024Q4*/), filt as (
select *
from cte
where is_cbp or is_depression or is_hbd)
select distinct on (mbi, measure_key)
    raw_id,
    inbound_file_id,
    patient_id,
    measure_year,
    mbi,
    payer_id,
    member_id,
    contract_id,
    measure_key,
    'proxy' measure_source_key,
    'pending_scheduling_clinic' measure_status_key
from filt
        cross join lateral ( values
            ('cbp_controlling_high_blood_pressure', is_cbp),
            ('hbd_hemoglobin_a1c_control_for_patients_with_diabetes', is_hbd),
            ('dsf_depression', is_depression)
            ) dx (measure_key, is_active)
where
    is_active
order by mbi, measure_key, contract_id
;

drop table if exists _qms;
create temp table _qms as
select * from fdw_member_doc.qm_patient_measures where operational_year = 2024;

drop table if exists _chase;
create temp table _chase as
select distinct unnest(qm_patient_measure_ids) qm_patient_measure_id, external_chase_id, id msh_chase_id
from public.reveleer_chases;
create unique index udx__chase on _chase(qm_patient_measure_id, external_chase_id);
create index on _chase( msh_chase_id);

drop table if exists _unpivoted;
CREATE TEMP TABLE _unpivoted AS
SELECT
    p.raw_id
  , p.inbound_file_id
  , p.patient_id
  , p.measure_year
  , p.mbi
  , p.payer_id
  , p.member_id
  , p.contract_id
  , p.measure_key
  , qpm.id               qm_patient_measure_id
     , qpm.is_active
  , ch.external_chase_id rev_chase_id -- NEED THIS
  , ch.msh_chase_id
FROM
    _pats p
    LEFT JOIN _qms qpm ON qpm.patient_id = p.patient_id
        AND qpm.measure_key = p.measure_key
    LEFT JOIN _chase ch ON ch.qm_patient_measure_id = qpm.id
;
SELECT count(qm_patient_measure_id), count(distinct qm_patient_measure_id)
FROM
    _unpivoted;
create index on _unpivoted(patient_id);
DROP TABLE IF EXISTS _za;
CREATE TEMP TABLE _za AS
    select patient_id, is_quality_measures from fdw_member_doc.supreme_pizza

SELECT
    COUNT(*)                                                                             n
  , COUNT(*) FILTER ( WHERE qm_patient_measure_id IS NOT NULL )                          n_in_coop
  , COUNT(*) FILTER ( WHERE msh_chase_id IS NOT NULL )                                   n_in_sent_to_rev
  , COUNT(*) FILTER ( WHERE qm_patient_measure_id IS NOT NULL AND msh_chase_id IS NULL ) n_not_sent_to_rev
  , COUNT(*) FILTER ( WHERE is_active and qm_patient_measure_id IS NOT NULL AND msh_chase_id IS NULL ) n_not_sent_to_rev
FROM
    _unpivoted




;

create index on _za(patient_id);
SELECT
--     q.*
--     q.operational_year, count(*)
--     q.*
--     is_active, count(*)
    COUNT(*)                                                  n
--   , COUNT(sp.patient_id)                                      n_pizza
--   , COUNT(sp.patient_id) FILTER ( WHERE is_quality_measures ) n_pizza_qual
FROM
    _unpivoted u
    JOIN _qms q ON q.id = u.qm_patient_measure_id
    LEFT JOIN _za sp ON sp.patient_id = u.patient_id
-- join raw.reveleer_mssp_cqm_aco_members_2024_q3_alr r on r.patient_id = u.patient_id
WHERE
      u.msh_chase_id ISNULL
  AND u.qm_patient_measure_id IS NOT NULL
  AND sp.patient_id IS NOT NULL
  AND q.is_active
and u.rev_chase_id is null
-- GROUP BY  1
;


SELECT
    count(*)
    FROM
        fdw_member_doc.qm_patient_measures pqm
        JOIN _unpivoted u on u.qm_patient_measure_id = pqm.id
--         JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
        JOIN (select distinct patient_id, reveleer_project_id, contract_id from raw.reveleer_mssp_cqm_aco_members_2024_q3_alr) r on r.patient_id = pqm.patient_id
        JOIN _aco_lookup al on al.contract_id = r.contract_id
        JOIN ( SELECT id, UNNEST(measures_to_send) measures_to_send, payer_id FROM public.reveleer_projects ) ptr
             ON r.reveleer_project_id = ptr.id
--         JOIN fdw_member_doc.payers pay ON pay.id = sp.patient_payer_id
        JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
        JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
    -- BP: moved to new table 1/24/2025
--         JOIN raw.reveleer_mssp_cqm_aco_membrs_20240624 r ON r.patient_id = sp.patient_id
    WHERE
          pqm.operational_year = 2024-- ( SELECT yr FROM _controls )
      -- BP: updated to include emr per MT 2025-02-10
      AND pqm.measure_source_key in ('proxy', 'emr')
--       AND pqm.is_active
--       AND sp.is_quality_measures
          -- need to include closed system for compliance file
--       AND (st.send_to_reveleer OR pqm.measure_status_key = 'closed_system')
      AND pqm.measure_key = ptr.measures_to_send
--       AND ptr.id = 298 -- mssp cqm aco
    ;

--   AND u.is_active;

-- group by 1
-- GROUP BY is_active
;


SELECT *
FROM
    fdw_member_doc.qm_patient_measures
WHERE
    id IN (1207, 1226, 1234, 1240, 1252, 1270);




select
    patient_id,
    mbi,
    payer_id,
    member_id,
    contract_id,
    bool_or(measure_key ~* 'cbp') has_cbp,
    max(qm_patient_measure_id) filter ( where measure_key ~* 'cbp' ) cbp_qm_patient_measure_id,
    max(rev_chase_id) filter ( where measure_key ~* 'cbp' ) cbp_rev_chase_id,
    bool_or(measure_key ~* 'dsf') has_dsf,
    max(qm_patient_measure_id) filter ( where measure_key ~* 'dsf' ) dsf_qm_patient_measure_id,
    max(rev_chase_id) filter ( where measure_key ~* 'dsf' ) dsf_rev_chase_id,
    bool_or(measure_key ~* 'hbd') has_hbd,
    max(qm_patient_measure_id) filter ( where measure_key ~* 'hbd' ) hbd_qm_patient_measure_id,
    max(rev_chase_id) filter ( where measure_key ~* 'hbd' ) hbd_rev_chase_id
from _unpivoted
group by patient_id, mbi, payer_id, member_id, contract_id
;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
call sp_reveleer_data_stager();
SELECT *
FROM
    _patient_measures where patient_id = 534125;