595952
3964550

SELECT *
FROM
    member_doc.public.patient_tasks WHERE id = 3964550
;

SELECT *
FROM
    patient_medication_fills pmf where pmf.patient_id = 595952
and pmf.drug_description ~* 'enal'
;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    qm_pm_med_adh_metrics
WHERE
      patient_id = 1510591
  AND measure_year = 2025;

SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
      patient_id = 1510591
  AND yr = 2025;
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    patient_tasks
WHERE
    id = 3862992
;
SELECT *
FROM
    qm_pm_med_adh_90_day_conversions WHERE patient_task_id = 3862992;

UPDATE
    patient_tasks
SET
    status = 'in_progress', updated_at = NOW()
WHERE
    id = 3862992
;


INSERT
INTO
    public.patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at, updated_at)
VALUES
    (3862992, 2, 'update_status', 'in_progress',
    'Manual Reopen', now(),
     now());



UPDATE
    patient_tasks
SET
    status = 'in_progress', updated_at = NOW()
WHERE
    id = 3902762
;


INSERT
INTO
    public.patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at, updated_at)
VALUES
    (3902762, 2, 'update_status', 'in_progress',
    'Manual Reopen', now(),
     now());


------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    patient_medication_fills pmf where pmf.patient_id = 419777
and pmf.drug_description ~* 'prav'
;