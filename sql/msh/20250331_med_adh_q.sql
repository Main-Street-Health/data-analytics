------------------------------------------------------------------------------------------------------------------------
/*
Hey - this working correctly?
Seems like they shouldn't be getting a task if the pt has meds on hand per MCO.
The MCO shows a 3/10 fill that goes until 6/8

Pt ID = 1112408
Measure = MAD
*/
------------------------------------------------------------------------------------------------------------------------
    SELECT
        m.patient_id
      , m.measure_key
      , m.patient_measure_id
      , pm.measure_status_key
      , m.next_fill_date
      , m.adr
      , m.pdc_to_date
      , m.measure_source_key
      , m.priority_status
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
        pm.operational_year = 2025
        and pm.patient_id =  1112408
        -- pt.id =
        and pm.measure_key = 'med_adherence_diabetes'
    ORDER BY pm.measure_key, pf.id
    ;

-- look at data from the mco
SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
    patient_id = 1112408 and measure_key = 'med_adherence_diabetes'
order by signal_date desc
;

------------------------------------------------------------------------------------------------------------------------
/*
 Response
 The most recent data from the mco based on signal date shows a fill on 1/5
 The 3/10 fill is the second most recent data. It's likely this one should be flagged as a reversal
 */
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
/* Do we not ping SS before creating tasks for patients? Example below:

PT ID = 285227
Measure = MAC
Task ID = 3273283
Task Created on 3/31 due to being late on a pick up but we never pinged SS before creating the task. */
------------------------------------------------------------------------------------------------------------------------


SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
     , m.id
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , m.priority_status
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
    pm.operational_year = 2025
    and pm.patient_id = 285227
    -- pt.id = 
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
-- NFD = 3/22
-- task ins = 3/31
-- query 2/26

SELECT *
FROM
    qm_pm_med_adh_priority_histories WHERE med_adh_metric_id = 83824244;

------------------------------------------------------------------------------------------------------------------------
/* again 4/9/25
   
Ok @Brendon Pierson - found a real one.
   Pt ID = 1575921; Task ID = 3347026. Task created today, last time they got a SS ping was 3/12.

   */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , m.priority_status
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
    pm.operational_year = 2025
    and pm.patient_id = 1575921
and    pt.id = 3347026
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT
    pf.patient_measure_id
  , pt.patient_id
  , pt.task_type
FROM
    patient_tasks pt
    JOIN qm_pm_med_adh_potential_fills pf ON pt.id = pf.patient_task_id
--     JOIN public.qm_pm_med_adh_metrics m ON pf.patient_measure_id = m.patient_measure_id
WHERE
    pt.id = 3347026;
+------------------+----------+----------------------+
|patient_measure_id|patient_id|task_type             |
+------------------+----------+----------------------+
|3568465           |1575921   |med_adherence_diabetes|
+------------------+----------+----------------------+



SELECT id, patient_id, measure_key
FROM
    qm_patient_measures pm where id = 3568465
+-------+----------+----------------------+
|id     |patient_id|measure_key           |
+-------+----------+----------------------+
|3568465|1575921   |med_adherence_diabetes|
+-------+----------+----------------------+


SELECT *
FROM
    qm_pm_med_adh_metrics WHERE patient_measure_id = 3568465;

SELECT id, patient_measure_id
FROM
    qm_pm_med_adh_metrics m
WHERE
      patient_id = 1575921
  AND measure_key = 'med_adherence_diabetes'
  AND measure_year = 2025
;

+--------+------------------+
|id      |patient_measure_id|
+--------+------------------+
|90392260|null              |
+--------+------------------+

begin;
ROLLBACK ;
UPDATE qm_pm_med_adh_metrics m
SET
    patient_measure_id = pm.id, updated_at = NOW()
FROM
    qm_patient_measures pm
WHERE
      m.patient_measure_id ISNULL
  AND pm.patient_id = m.patient_id
  AND pm.measure_key = m.measure_key
  AND pm.operational_year = m.measure_year
  AND m.measure_year = 2025;
-- failed with dupe key exists 2968523

SELECT *
FROM
    qm_pm_med_adh_metrics where patient_measure_id = 2968523;
+--------+----------+----------------------+------------+------------------+
|id      |patient_id|measure_key           |measure_year|patient_measure_id|
+--------+----------+----------------------+------------+------------------+
|83895404|968749    |med_adherence_diabetes|2025        |2968523           |
+--------+----------+----------------------+------------+------------------+

SELECT id, patient_id, measure_key
FROM
    qm_patient_measures where id = 2968523           ;
+-------+----------+----------------------+
|id     |patient_id|measure_key           |
+-------+----------+----------------------+
|2968523|568613    |med_adherence_diabetes|
+-------+----------+----------------------+

SELECT *
FROM
    patients p
where p.id in (568613,968749)
;
568613 is active
968749 is hard delete

-- need to remove pqm id from the 968749  metric
-- SELECT *
-- FROM
UPDATE
    qm_pm_med_adh_metrics
SET
    patient_measure_id = NULL, updated_at = NOW()
WHERE
      patient_id = 968749
  AND measure_year = 2025;

-- try everyone again
begin;
ROLLBACK ;
UPDATE qm_pm_med_adh_metrics m
SET
    patient_measure_id = pm.id, updated_at = NOW()
FROM
    qm_patient_measures pm
WHERE
      m.patient_measure_id ISNULL
  AND pm.patient_id = m.patient_id
  AND pm.measure_key = m.measure_key
  AND pm.operational_year = m.measure_year
  AND m.measure_year = 2025;
-- [23505] ERROR: duplicate key value violates unique constraint "qm_pm_med_adh_metrics_patient_measure_id_index" Detail: Key (patient_measure_id)=(3365223) already exists.

SELECT id, patient_id, measure_key
FROM
    qm_pm_med_adh_metrics WHERE patient_measure_id = 3365223;
+--------+----------+--------------------------+
|id      |patient_id|measure_key               |
+--------+----------+--------------------------+
|86290923|1635366   |med_adherence_hypertension|
+--------+----------+--------------------------+



SELECT patient_id, measure_key
FROM
    qm_patient_measures WHERE id = 3365223;
+----------+--------------------------+
|patient_id|measure_key               |
+----------+--------------------------+
|812588    |med_adherence_hypertension|
+----------+--------------------------+



SELECT p.id, p.status
FROM
    patients p where p.id in ( 812588,1635366 );
+-------+-----------+
|id     |status     |
+-------+-----------+
|812588 |active     |
|1635366|hard_delete|
+-------+-----------+

-- update 1635366 metric to remove patientmeasureid
UPDATE
-- select * from
    qm_pm_med_adh_metrics
SET
    patient_measure_id = NULL, updated_at = NOW()
WHERE
      patient_id = 1635366
  AND measure_year = 2025;

-- try everyone again
begin;
ROLLBACK ;
UPDATE qm_pm_med_adh_metrics m
SET
    patient_measure_id = pm.id, updated_at = NOW()
FROM
    qm_patient_measures pm
WHERE
      m.patient_measure_id ISNULL
  AND pm.patient_id = m.patient_id
  AND pm.measure_key = m.measure_key
  AND pm.operational_year = m.measure_year
  AND m.measure_year = 2025;
-- still blocks. lets try getting all of these at once

SELECT *
FROM
    qm_pm_med_adh_metrics m
    JOIN patients p ON p.id = m.patient_id
WHERE
      m.measure_year = 2025
  AND p.status = 'hard_delete'
;
UPDATE
    qm_pm_med_adh_metrics m
SET
    patient_measure_id = NULL, updated_at = NOW()
FROM
    patients p
WHERE
      m.measure_year = 2025
  AND p.status = 'hard_delete'
  AND p.id = m.patient_id
;

-- try everyone again
begin;
ROLLBACK ;
UPDATE qm_pm_med_adh_metrics m
SET
    patient_measure_id = pm.id, updated_at = NOW()
FROM
    qm_patient_measures pm
WHERE
      m.patient_measure_id ISNULL
  AND pm.patient_id = m.patient_id
  AND pm.measure_key = m.measure_key
  AND pm.operational_year = m.measure_year
  AND m.measure_year = 2025;



------------------------------------------------------------------------------------------------------------------------
/* back to original problem */
------------------------------------------------------------------------------------------------------------------------

SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , m.priority_status
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
    pm.operational_year = 2025
    and pm.patient_id = 1575921
-- and    pt.id = 3347026
and pm.id = 3568465
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
-- nfd 3/25
-- ccd = 4/15
-- task 4/9

SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 1575921
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#'
;
-- no ss

SELECT pdc, is_prev_year_fail, next_fill_date,
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 1575921
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';

SELECT * FROM patient_sure_scripts_panels WHERE patient_id = 1575921;
-- 3/13
SELECT *
FROM
    qm_pm_med_adh_handoffs WHERE patient_id = 1575921 order by id;


------------------------------------------------------------------------------------------------------------------------
/* concurrent meds */
------------------------------------------------------------------------------------------------------------------------

--     select * from
UPDATE
    qm_pm_concurrent_med_metrics m
SET
    patient_measure_id = NULL, updated_at = NOW()
FROM
    patients p
WHERE
      m.measure_year = 2025
  AND p.status = 'hard_delete'
  AND p.id = m.patient_id
;
------------------------------------------------------------------------------------------------------------------------
/* ` */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    qm_pm_med_adh_potential_fills where patient_task_id = 3287223
SELECT *
FROM
    qm_patient_measures where id = 3152008;