
SELECT
    pt.status, count(*)
FROM
    qm_pm_med_adh_potential_fills pf
join patient_tasks pt on pf.patient_task_id = pt.id
WHERE
    pf.inserted_at > NOW() - '1 day'::INTERVAL
GROUP BY pt.status



begin;
end;
WITH
    new_tasks AS ( SELECT DISTINCT
                       pt.id patient_task_id
                   FROM
                       qm_pm_med_adh_potential_fills pf
                       JOIN patient_tasks pt ON pf.patient_task_id = pt.id
                   WHERE
                       pt.status = 'new' )
  , upd_tasks AS (
    UPDATE patient_tasks pt
        SET status = 'cancelled', updated_at = NOW()
        FROM new_tasks nt
        WHERE nt.patient_task_id = pt.id
        RETURNING * )
INSERT
INTO
    patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at, updated_at)
SELECT
    ut.id
  , 98
  , 'cancelled'
  , 'AutoClosed - Cancelled - Created in Error'
  , NULL
  , NOW()
  , NOW()
FROM
    upd_tasks ut;



SELECT
    distinct pf.patient_task_id
FROM
    qm_pm_med_adh_potential_fills pf
    JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    JOIN patient_task_activities pta ON pta.patient_task_id = pf.patient_task_id
WHERE
      action = 'cancelled'
  and pta.user_id = 98
  AND value = 'AutoClosed - Cancelled - Created in Error'
  AND pf.inserted_at > NOW() - '1 day'::INTERVAL


SELECT
    pm.measure_status_key
  , COUNT(*)
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
GROUP BY
    1
;

SELECT * FROM qm_pm_med_adh_priority_histories;
SELECT * FROM qm_pm_status_periods;
SELECT * FROM qm_pm_activities;
SELECT * FROM patient_task_activities;
