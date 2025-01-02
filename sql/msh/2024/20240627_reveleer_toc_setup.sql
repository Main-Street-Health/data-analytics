
SELECT *
FROM
    fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs
        ;
SELECT *
FROM
    fdw_member_doc.payers where name ~* 'wellmed';
key,code
/* fmc_follow_up_after_ed_visit_multiple_chronic_conditions,FMC */  select * from fdw_member_doc.qm_pm_toc_er_followup_chronic_conditions_wfs;
/* trc_mrp_medication_reconciliation_post_discharge,MRP         */  select * from fdw_member_doc.qm_pm_toc_med_rec_wfs;
/* trc_peid_patient_engagement_after_ip_discharge,PEID          */  select followup_visit_scheduled_date, * from fdw_member_doc.qm_pm_toc_engagement_after_discharge_wfs;
/* trc_rdi_receipt_of_discharge_information,RDI                 */  select * from fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs;
/* trc_nia_notification_of_ip_admission,NIA                     */  select * from fdw_member_doc.qm_pm_toc_nia_wfs;
;
with pat_admits as (
select admit_date, patient_id, patient_measure_id from fdw_member_doc.qm_pm_toc_med_rec_wfs wf join fdw_member_doc.qm_patient_measures pm on pm.id = wf.patient_measure_id union
select admit_date, patient_id, patient_measure_id from fdw_member_doc.qm_pm_toc_engagement_after_discharge_wfs wf join fdw_member_doc.qm_patient_measures pm on pm.id = wf.patient_measure_id union
select admit_date, patient_id, patient_measure_id from fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs wf join fdw_member_doc.qm_patient_measures pm on pm.id = wf.patient_measure_id union
select admit_date, patient_id, patient_measure_id from fdw_member_doc.qm_pm_toc_nia_wfs wf join fdw_member_doc.qm_patient_measures pm on pm.id = wf.patient_measure_id)
SELECT pa.*
, mrp.admit_date
, peid.admit_date
, rdi.admit_date
, nia.admit_date
FROM
    pat_admits pa
left join (select admit_date, patient_id from fdw_member_doc.qm_pm_toc_med_rec_wfs i_mrp join fdw_member_doc.qm_patient_measures pm on pm.id = i_mrp.patient_measure_id) mrp on pa.patient_id = mrp.patient_id and mrp.admit_date = pa.admit_date
left join (select admit_date, patient_id from fdw_member_doc.qm_pm_toc_engagement_after_discharge_wfs i_peid join fdw_member_doc.qm_patient_measures pm on pm.id = i_peid.patient_measure_id) peid on pa.patient_id =  peid.patient_id and peid.admit_date = pa.admit_date
left join (select admit_date, patient_id from fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs i_rdi join fdw_member_doc.qm_patient_measures pm on pm.id = i_rdi.patient_measure_id) rdi on pa.patient_id =  rdi.patient_id and rdi.admit_date = pa.admit_date
left join (select admit_date, patient_id from fdw_member_doc.qm_pm_toc_nia_wfs i_nia join fdw_member_doc.qm_patient_measures pm on pm.id = i_nia.patient_measure_id) nia on pa.patient_id =  nia.patient_id and nia.admit_date = pa.admit_date
;

with pat_admits as ( SELECT
                         admit_date
                       , patient_id
--                        , patient_measure_id
                       , hospitalization_id
                     FROM
                         fdw_member_doc.qm_pm_toc_med_rec_wfs wf
                         JOIN fdw_member_doc.qm_patient_measures pm ON pm.id = wf.patient_measure_id
                     UNION
                     SELECT
                         admit_date
                       , patient_id
--                        , patient_measure_id
                       , hospitalization_id
                     FROM
                         fdw_member_doc.qm_pm_toc_engagement_after_discharge_wfs wf
                         JOIN fdw_member_doc.qm_patient_measures pm ON pm.id = wf.patient_measure_id
                     UNION
                     SELECT
                         admit_date
                       , patient_id
--                        , patient_measure_id
                       , hospitalization_id
                     FROM
                         fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs wf
                         JOIN fdw_member_doc.qm_patient_measures pm ON pm.id = wf.patient_measure_id )--                                                                       union
-- select admit_date, patient_id, patient_measure_id, hospitalization_id from fdw_member_doc.qm_pm_toc_nia_wfs wf join fdw_member_doc.qm_patient_measures pm on pm.id = wf.patient_measure_id)
SELECT pa.*
, mrp.admit_date
, peid.admit_date
, rdi.admit_date
FROM
    pat_admits pa
left join fdw_member_doc.qm_pm_toc_med_rec_wfs mrp on pa.hospitalization_id = mrp.hospitalization_id
left join fdw_member_doc.qm_pm_toc_engagement_after_discharge_wfs peid on pa.hospitalization_id =  peid.hospitalization_id
left join fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs rdi on pa.hospitalization_id =  rdi.hospitalization_id
-- left join fdw_member_doc.qm_pm_toc_nia_wfs nia on pa.hospitalization_id = nia.hospitalization_id
;



SELECT
    *
FROM
   reveleer_projects rp
WHERE
      rp.measures_to_send &&
array [
                      'trc_peid_patient_engagement_after_ip_discharge',
                      'trc_rdi_receipt_of_discharge_information',
                      'trc_mrp_medication_reconciliation_post_discharge',
                      'trc_nia_notification_of_ip_admission'
                      'fmc_follow_up_after_ed_visit_multiple_chronic_conditions'
          ]
;

SELECT
    mrp_wf.worksheet_status
  , mrp_wf.encounter_date
  , d.inserted_at AS doc_uploaded_at
FROM
    fdw_member_doc.qm_pm_toc_med_rec_wfs mrp_wf
    LEFT JOIN fdw_member_doc.documents d ON mrp_wf.worksheet_document_id = d.id;


-- mrp and fmc already configured

-- FMC all but united, centene
-- SELECT
--     id
--   , name
--   , measures_to_send
--   , ARRAY_APPEND(measures_to_send, 'fmc_follow_up_after_ed_visit_multiple_chronic_conditions')
-- FROM
--     reveleer_projects
-- WHERE
--       yr = 2024
--   AND id IN (
-- --              236 -- united
--              238 -- humana
--            , 241 -- elevance
--            , 233 -- bcbs
--            , 234 -- wellcare
--            , 232 -- aetna
--            , 240 -- cigna
--            , 235 -- wellmark
--            , 237 -- viva
-- --            , 239 -- centene
--     )
-- ;
-- MRP all but aetna, centene
-- SELECT
--     id
--   , name
--   , measures_to_send
--   , ARRAY_APPEND(measures_to_send, 'trc_mrp_medication_reconciliation_post_discharge')
-- FROM
--     reveleer_projects
-- WHERE
--       yr = 2024
--   AND id IN (
--              236 -- united
--            , 238 -- humana
--            , 241 -- elevance
--            , 233 -- bcbs
--            , 234 -- wellcare
-- --            , 232 -- aetna
--            , 240 -- cigna
--            , 235 -- wellmark
--            , 237 -- viva
-- --            , 239 -- centene
--     )
;
-- PEID all but united, aetna, centene
-- SELECT
--     id
--   , name
--   , measures_to_send
update reveleer_projects
  set measures_to_send = ARRAY_APPEND(measures_to_send, 'trc_peid_patient_engagement_after_ip_discharge')
WHERE
      yr = 2024
  AND id IN (
--              236 -- united
             238 -- humana
           , 241 -- elevance
           , 233 -- bcbs
           , 234 -- wellcare
--            , 232 -- aetna
           , 240 -- cigna
           , 235 -- wellmark
           , 237 -- viva
--            , 239 -- centene
    )
;
-- RDI only bcbs
-- SELECT
--     id
--   , name
--   , measures_to_send
update reveleer_projects
  set measures_to_send = ARRAY_APPEND(measures_to_send, 'trc_rdi_receipt_of_discharge_information')
WHERE
      yr = 2024
  AND id IN (
--              236 -- united
--            , 238 -- humana
--            , 241 -- elevance
            233 -- bcbs
--            , 234 -- wellcare
--            , 232 -- aetna
--            , 240 -- cigna
--            , 235 -- wellmark
--            , 237 -- viva
--            , 239 -- centene
    )
;
-- nia only bcbs
-- SELECT
--     id
--   , name
--   , measures_to_send
--   , ARRAY_APPEND(measures_to_send, 'trc_nia_notification_of_ip_admission')
update reveleer_projects
set measures_to_send = ARRAY_APPEND(measures_to_send, 'trc_nia_notification_of_ip_admission')
WHERE
      yr = 2024
  AND id IN (
--              236 -- united
--            , 238 -- humana
--            , 241 -- elevance
            233 -- bcbs
--            , 234 -- wellcare
--            , 232 -- aetna
--            , 240 -- cigna
--            , 235 -- wellmark
--            , 237 -- viva
--            , 239 -- centene
    )
;

United	Humana	Elevance	BCBSTN	Wellcare	Aetna	Cigna	Wellmark	Viva
name
236,united
238,humana
241,elevance
233,bcbs
234,wellcare
232,aetna
240,cigna
235,wellmark
237,viva

239,centene


-- 331
INSERT
INTO
    public.reveleer_projects (name, payer_id, state_payer_id, reveleer_id, inserted_at, updated_at,
                              measures_to_send, yr, is_active)
VALUES
    ('wellmed', 147, NULL, '2394', NOW(), NOW(),
     ARRAY [
         'trc_peid_patient_engagement_after_ip_discharge',
         'trc_rdi_receipt_of_discharge_information',
         'trc_mrp_medication_reconciliation_post_discharge',
--          'trc_nia_notification_of_ip_admission'
--          , 'fmc_follow_up_after_ed_visit_multiple_chronic_conditions'
         ], 2024,
     TRUE)
returning *
;
UPDATE public.reveleer_projects
SET
    measures_to_send = ARRAY [
        'bcs_breast_cancer_screening',
        'cbp_controlling_high_blood_pressure',
        'col_colorectal_screening',
        'eed_eye_exam_for_patients_with_diabetes',
        'fmc_follow_up_after_ed_visit_multiple_chronic_conditions',
        'hbd_hemoglobin_a1c_control_for_patients_with_diabetes',
        'ked_kidney_health_evaluation_for_patients_with_diabetes',
        'omw_osteoporosis_management',
        'trc_mrp_medication_reconciliation_post_discharge',
-- 'trc_nia_notification_of_ip_admission',
        'trc_peid_patient_engagement_after_ip_discharge',
        'trc_rdi_receipt_of_discharge_information'
        ]
WHERE
    id = 331;


SELECT *
FROM
    reveleer_chases;
SELECT *
FROM
    fdw_member_doc.qm_ref_measures
WHERE
    code IN (
        'BCS',
        'COL',
        'CBP',
        'EED',
        'FMC',
        'HBD',
        'KED',
        'OMW',
        'MRP',
        'NIA',
        'PEID',
        'RDI'

        )
;

SELECT *
FROM
    reveleer_projects;
SELECT distinct measure from (
SELECT unnest(measures_to_send) measure
FROM
    reveleer_projects
where yr = 2024
) x;


-- DELETE
delete FROM reveleer_attribute_file_details a WHERE a.reveleer_project_id = 331
delete FROM reveleer_chase_file_details a WHERE a.reveleer_project_id = 331
delete FROM reveleer_compliance_file_details a WHERE a.reveleer_project_id = 331
