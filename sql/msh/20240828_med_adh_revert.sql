-- find the one by patient id
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
     , pm.is_active
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
, sp.is_quality_measures
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
join supreme_pizza sp on sp.patient_id = pm.patient_id
        join public.qm_patient_config qpc on qpc.patient_id = m.patient_id and qpc.measure_key = m.measure_key and qpc.is_active

    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 1272946
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;
SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'Next fill date >= today when in lost_adr_gt_zero status' reason
        FROM
            public.qm_pm_med_adh_metrics m
        JOIN public.qm_patient_measures pm on m.patient_measure_id = pm.id
                                          and pm.is_active
        where not m.is_excluded
          and m.next_fill_date >= now()::date
          and m.patient_id = 1272946
          and pm.measure_status_key = 'lost_adr_gt_zero';
SELECT *
FROM
    qm_pm_med_adh_handoffs where patient_id = 1272946 order by id;

-- UPDATE
--     qm_pm_med_adh_handoffs
-- SET
--     processed_at = NULL
-- WHERE
--     id = 8216958;
SELECT * from patients where id = 1272946;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------


-- Johnny Sanders Patient ID: 795391

-- SELECT pf.patient_task_id, wf.*
DROP TABLE IF EXISTS _to_revert;
CREATE TEMP TABLE _to_revert AS
SELECT
    pf.patient_task_id
  , pf.qm_pm_med_adh_wf_id
     , wf.patient_measure_id
  , wf.is_active
  , wf.is_system_verified_closed
  , newer_wf.id IS NOT NULL has_newer_wf
FROM
    qm_pm_med_adh_wfs wf
    JOIN member_doc.public.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN qm_pm_med_adh_wfs newer_wf ON wf.patient_measure_id = newer_wf.patient_measure_id AND newer_wf.id > wf.id
WHERE
      pf.patient_task_id IN
      (1699618, 1658927, 1668129, 1572041, 1611246, 1089713, 1605145, 1093616, 1064800, 1078021, 1433484, 1609763,
       1732639, 1702454, 1106093)
--   AND NOT wf.is_active
--   AND wf.is_active
--   AND NOT wf.is_system_verified_closed;

-- and exists(select 1 from qm_pm_med_adh_wfs wf2
--                    where wf2.patient_measure_id = wf.patient_measure_id and wf2.id > wf.id
--                     )
;
SELECT
    patient_task_id,
    CASE WHEN is_system_verified_closed THEN 'Workflow is system verified closed'
         ELSE 'newer wf exists' END reason_not_reverted
FROM
    _to_revert
WHERE
  is_system_verified_closed
  OR has_newer_wf;


| patient\_task\_id | reason\_not\_reverted |
| :--- | :--- |
| 1093616 | Workflow is system verified closed |
| 1605145 | Workflow is system verified closed |
| 1089713 | Workflow is system verified closed |
| 1702454 | Workflow is system verified closed |



UPDATE qm_pm_med_adh_wfs wf
SET
    is_active = TRUE, compliance_check_date = NOW()::DATE, updated_at = NOW(), updated_by_id = 98
FROM
    _to_revert tr
WHERE
      NOT tr.is_system_verified_closed
--   AND NOT tr.is_active
  AND NOT tr.has_newer_wf
  AND tr.qm_pm_med_adh_wf_id = wf.id
;


