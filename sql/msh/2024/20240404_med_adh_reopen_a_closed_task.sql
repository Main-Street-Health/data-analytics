SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pt.id = 1084974
ORDER BY pm.measure_key, pf.id
;;

select * from qm_pm_med_adh_wfs WHERE id = 19961;
SELECT * FROM qm_pm_med_adh_potential_fills WHERE id = 28882;
SELECT * FROM patient_tasks pt WHERE pt.id = 1084974 ;

update qm_pm_med_adh_wfs set is_active = true, updated_at = now() WHERE id = 19961;
update qm_pm_med_adh_potential_fills set order_status = null, updated_at = now() WHERE id = 28882;
update patient_tasks pt set status = 'in_progress' WHERE pt.id = 1084974;

INSERT
INTO
    patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at, updated_at)
VALUES
    (1084974, 98, 'update_status', 'in_progress', 'reopen', now(), now());

