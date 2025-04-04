SELECT
   m.patient_measure_id
   , m.patient_id
  , m.measure_key
  , pm.measure_status_key
  , m.priority_status
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
     , pf.days_supply
     , pf.last_filled_date
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    fdw_member_doc.qm_patient_measures pm
    JOIN fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
--     pm.patient_id =
    pt.id = 2976115
    and m.measure_year = 2025
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_synth_periods
WHERE
      patient_id = 11663
  and yr = 2025
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';

SELECT *
FROM
    fdw_member_doc.patient_medication_fills
WHERE
      patient_id = 11663
  and last_filled_date >= '2025-01-01'
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';
