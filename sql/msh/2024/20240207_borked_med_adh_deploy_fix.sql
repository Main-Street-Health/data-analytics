DROP TABLE IF EXISTS _tasks_to_delete;
CREATE TEMP TABLE _tasks_to_delete AS
SELECT DISTINCT
    pt.patient_id
  , pt.id       patient_task_id
FROM
    patient_tasks pt
    JOIN patients p ON pt.patient_id = p.id
WHERE
      pt.task_type IN ('med_adherence_diabetes', 'med_adherence_hypertension', 'med_adherence_cholesterol',
                       'med_adherence_cholesterol_side_effects_check')
  AND pt.inserted_at >= '2024-02-05'
ORDER BY
    1, 2
;

delete from qm_pm_tasks qpt using _tasks_to_delete ttd where ttd.patient_task_id = qpt.patient_task_id;
delete from patient_task_activities a using _tasks_to_delete ttd where ttd.patient_task_id = a.patient_task_id;
delete from qm_pm_med_adh_wfs w where true;
delete from patient_tasks pt using _tasks_to_delete ttd where pt.id = ttd.patient_task_id;

-- needed to switch to side effects check
delete from qm_pm_tasks qpt where patient_task_id = 934236;
delete from patient_task_activities a where patient_task_id = 934236;
delete from qm_pm_med_adh_potential_fills a where patient_task_id = 934236;
delete from qm_pm_med_adh_wfs w where patient_measure_id = 344644;
delete from patient_tasks pt where id = 934236;




SELECT *
FROM
    qm_patient_measures pqm
    join qm_pm_activities a on pqm.id = a.patient_measure_id
WHERE pqm.measure_key in ('med_adherence_diabetes', 'med_adherence_hypertension', 'med_adherence_cholesterol') ;


SELECT * FROM qm_pm_med_adh_wfs wf WHERE patient_measure_id = 344326;
SELECT * FROM qm_pm_med_adh_metrics wf WHERE patient_measure_id = 344326;
SELECT * FROM qm_patient_measures WHERE id = 344326;
SELECT * FROM patient_medication_fills where patient_id = 7343 and measure_key = 'med_adherence_cholesterol';




delete from qm_pm_med_adh_potential_fills where patient_task_id = 35869;
delete from patient_tasks where id = 35869;
