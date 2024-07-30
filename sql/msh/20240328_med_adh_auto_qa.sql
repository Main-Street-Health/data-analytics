SELECT pta.inserted_at::date, count(*)
FROM
    patient_tasks pt
    JOIN patient_task_activities pta ON pt.id = pta.patient_task_id
    JOIN qm_pm_med_adh_potential_fills pf ON pt.id = pf.patient_task_id
WHERE
      pt.status = 'closed'
  AND pta.value = 'AutoClosed - Lost'
GROUP BY 1
ORDER BY 2 desc
;

| inserted\_at | count |
| :--- | :--- |
| 2024-03-23 | 1661 |
| 2024-03-03 | 36 |

SELECT order_status, count(*)
FROM
    qm_pm_med_adh_potential_fills
GROUP BY 1
;

SELECT
    COUNT(DISTINCT pf.patient_measure_id) total_measures
  , COUNT(DISTINCT pf.patient_measure_id) FILTER ( WHERE e.patient_id IS not NULL ) total_excluded
FROM
    patient_tasks pt
    JOIN patient_task_activities pta ON pt.id = pta.patient_task_id
    JOIN qm_pm_med_adh_potential_fills pf ON pt.id = pf.patient_task_id
    LEFT JOIN qm_pm_med_adh_exclusions e ON e.patient_id = pt.patient_id
WHERE
      pt.status = 'closed'
  AND pta.value = 'AutoClosed - Lost'
  AND pta.inserted_at::DATE = '2024-03-03'
;
