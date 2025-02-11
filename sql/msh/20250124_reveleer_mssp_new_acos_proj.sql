
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

SELECT external_chase_id, al.payer_name, rc.patient_id, rc.measure_code
FROM
    raw.reveleer_mssp_cqm_aco_members_2024_q3_alr r
    join _aco_lookup al on al.contract_id = r.contract_id
   join reveleer_chases rc on rc.patient_id = r.patient_id and rc.yr = 2024

