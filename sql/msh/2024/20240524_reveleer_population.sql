SELECT DISTINCT
    measure
FROM
    ( SELECT
          UNNEST(measures_to_send) measure
      FROM
          reveleer_projects p
      where p.yr = 2024

      ) x ;

SELECT *
FROM
    reveleer_projects WHERE yr = 2024;

SELECT * FROM analytics.audit.sms_monitor_contactinfo;
SELECT *
FROM
    fdw_member_doc.payers order by id;

-- call sp_reveleer_data_stager();
-- call sp_reveleer_stage_new_cca_pdfs_for_upload();
SELECT
    COUNT(*)
-- *
FROM
    reveleer_chase_file_details
WHERE
    reveleer_project_id = 236;
--     reveleer_file_id ISNULL ;
SELECT pbp.*
FROM
    reveleer_chases rc
join fdw_member_doc.qm_patient_measures pm on pm.id = any(rc.qm_patient_measure_ids)
JOIN fdw_member_doc.patient_blood_pressures pbp ON pbp.patient_id = pm.patient_id
WHERE
    rc.id IN ( 71228,23985,39044,79729 )

;
SELECT
--     d.*
--     count(*)
*
FROM
    reveleer_cca_pdfs p
left join junk.reveer_upload_20240523 j on j.file_name = p.file_name
-- join fdw_member_doc.documents d on d.id = p.document_id
-- where uploaded_at ISNULL
where yr = 2024 and uploaded_at is not null
and uploaded_at::date = '2024-05-23'::date
and j.file_name ISNULL
order by p.id desc
-- ORDER BY
--     id DESC;
SELECT *
FROM
    analytics.junk.reveer_upload_20240523;
------------------------------------------------------------------------------------------------------------------------
/* stupid state payer work around for proxies

   Send BCS, COL and EED proxy gaps
   for Humana SC and GA to the "Humana Proxies MY2024" project (project ID 2239)
*/
------------------------------------------------------------------------------------------------------------------------

SELECT * FROM fdw_member_doc.payers where id = 44;

SELECT *
FROM
    fdw_member_doc.msh_state_payers
WHERE
      payer_id = 44
  AND state IN ('SC', 'GA'); -- id = (453,57)

SELECT *
FROM
    public.reveleer_projects
WHERE
    yr = 2024;

SELECT key
FROM
    fdw_member_doc.qm_ref_measures WHERE code in ('BCS', 'COL', 'EED');

INSERT
INTO
    reveleer_projects (name, payer_id, reveleer_id, inserted_at, updated_at, yr, is_active, measures_to_send)
VALUES
    ('humana_sc_ga_proxies', -44, 2239, NOW(), NOW(), 2024, TRUE,
     '{col_colorectal_screening,eed_eye_exam_for_patients_with_diabetes,bcs_breast_cancer_screening}');

SELECT DISTINCT
    sp.patient_id
  , NULL::BIGINT chase_id
  , sp.patient_mbi
  , pay.name     payer_name
  , 2239         reveleer_project_id
  , CASE
        WHEN m.code = 'HBD' THEN 'A1C9'
        ELSE m.code
        END      measure_code
  , pqm.measure_key
  , pqm.id       patient_quality_measure_id
  , pqm.operational_year
  , pqm.measure_source_key
  , pqm.must_close_by_date
  , sp.subscriber_id
FROM
    fdw_member_doc.qm_patient_measures pqm
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN fdw_member_doc.payers pay ON pay.id = sp.patient_payer_id
    JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
    JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
WHERE
      pqm.operational_year = 2024
  and sp.patient_state_payer_id in (453,57) -- Humana SC=453, GA=57
  AND pqm.measure_source_key = 'proxy'
  AND pqm.is_active
  AND sp.is_quality_measures
  AND st.send_to_reveleer
  AND pqm.measure_key IN
      ('col_colorectal_screening', 'eed_eye_exam_for_patients_with_diabetes', 'bcs_breast_cancer_screening');


------------------------------------------------------------------------------------------------------------------------
/* issues with future dates */
------------------------------------------------------------------------------------------------------------------------
-- Example: ChaseID=249940, EventID=1083788758950693698, EventType=Colonoscopy Test, AttributeCode=ColonoscopyTestDate, AttributeValue=06/04/2024, ValidationError=Colonoscopy Test Date may not be in a future; ;
-- Example: ChaseID=46361, EventID=726203937590326567, EventType=Fit-Dna Test, AttributeCode=FitDnaTestDate;

SELECT *
FROM
    fdw_member_doc.patient_colorectal_screening_results pcsr
WHERE
      pcsr.encounter_date >= '2024-01-01'::DATE
  AND pcsr.encounter_date >= NOW()::DATE
  AND pcsr.screening_type = 'fit_dna_test';
SELECT *
FROM
    fdw_member_doc.patient_colorectal_screening_results pcsr
WHERE
  pcsr.encounter_date >= NOW()::DATE;
--   AND pcsr.screening_type = 'colonoscopy';

SELECT *
FROM
    reveleer_projects WHERE yr = 2024 and is_active;
SELECT *
FROM
    ;
update
    reveleer_compliance_file_details
--         reveleer_chase_file_details
set reveleer_project_id = 265
WHERE reveleer_file_id ISNULL and reveleer_project_id = 2239 ;


------------------------------------------------------------------------------------------------------------------------
/* nuke out humana  */
------------------------------------------------------------------------------------------------------------------------
/*
delete FROM reveleer_chase_file_details WHERE reveleer_project_id = 236;
delete FROM reveleer_attribute_file_details WHERE reveleer_project_id = 236;
delete FROM reveleer_compliance_file_details WHERE reveleer_project_id = 236;
delete FROM reveleer_chases WHERE reveleer_project_id = 236;
delete FROM reveleer_files WHERE reveleer_project_id = 236;

DELETE
FROM
    reveleer_cca_pdfs
WHERE
    id IN (1095879, 1095771, 1095622, 1094747, 1094587, 1093972, 1093833, 1093488, 1093291, 1092312, 1091970, 1091796);

 */

