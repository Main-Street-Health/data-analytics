SELECT errors[attempt], *
FROM
    oban_jobs where queue = 'qm_pm_med_adherence'
and worker = 'MD.QualityMeasures2.Workflows.ConcurrentMedsWorker'
;

SELECT *
FROM
    qm_pm_status_periods WHERE patient_measure_id = 3011621;

SELECT *
FROM
    qm_patient_measures pm
    JOIN qm_pm_concurrent_med_metrics m ON pm.patient_id = m.patient_id
        AND pm.measure_key = m.measure_key
        AND pm.operational_year = m.measure_year
where m.patient_measure_id ISNULL

;
UPDATE qm_pm_concurrent_med_metrics m
SET
    patient_measure_id = pm.id, updated_at = now()
FROM
    qm_patient_measures pm
WHERE
      pm.patient_id = m.patient_id
  AND pm.measure_key = m.measure_key
  AND pm.operational_year = m.measure_year
  AND m.patient_measure_id ISNULL



;
SELECT pm.patient_id, pm.measure_key, pm.operational_year
FROM
    qm_patient_measures pm
where id = 3011621

;
SELECT *
FROM
    qm_pm_concurrent_med_metrics
WHERE
    patient_measure_id = 3011621;

SELECT *
FROM
    patients p
where p.id in (1633322, 1474816)
;

1474816 - inactive
1633322 - hard delete

-- remove pqm id from the hard delete patient metric
    update qm_pm_concurrent_med_metrics
    set patient_measure_id = null, updated_at = now()
WHERE
    patient_measure_id = 3011621;

-- update metric with inactive patient to have the pqm id
update qm_pm_concurrent_med_metrics
set patient_measure_id = 3011621, updated_at = now()
WHERE
    id = 106631;
