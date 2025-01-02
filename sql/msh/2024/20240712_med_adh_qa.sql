------------------------------------------------------------------------------------------------------------------------
/*
@Brendon Pierson hey could you please look into these three med adherence items before 3pm tomorrow?

Patient was late to pick up on 5/12 but task didn't generate until 6/21.
We get SS data from the pharmacy where this pt is picking up meds, but source is MCO for MAD measure. (522318)

Source MCO when we get SS data for this patient. The meds on hand update from 6/26 is missing in the meds list.
Overlap with the Ozempic fills that would give extra days. Why the large delta in Plan file PCD and MCO file PDC? (516682)

patient ID 7442 - MAD - ID - 1507031 - between 5/8 and 4/11 there was no task and it seems stuck in compliance loop but not sure why

As some background, we are digging into all 2000 patients who are failing med adherence or who are close to failing to see if we can figure out if there were things we could have done to prevent the failure (we are not doing better enough on med adherence this year to be confident)
 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
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
     , pf.meds_on_hand_date
  , pf.meds_on_hand_days_supply
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
    JOIN      fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 522318
and pm.measure_key = 'med_adherence_diabetes'
    -- pt.id =
ORDER BY pm.measure_key, pf.id ;
-- Patient was late to pick up on 5/12 but task didn't generate until 6/21.
-- We get SS data from the pharmacy where this pt is picking up meds, but source is MCO for MAD measure. (522318)
-- SS
+----------+----------+
|start_date|end_date  |
+----------+----------+
|2024-03-11|2024-04-09|
|2024-04-12|2024-05-11|
+----------+----------+
-- should have had task on 5/16
SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_synth_periods
WHERE
      patient_id = 522318
  AND measure_key = 'med_adherence_diabetes' ;

-- mco shows two fills, second one would have prevented task from getting generated on 5/16
-- should get task on 6/13
+--------------+--------------+
|last_fill_date|next_fill_date|
+--------------+--------------+
|2024-04-08    |2024-05-08    |
|2024-05-09    |2024-06-08    |
+--------------+--------------+
SELECT *
FROM
    fdw_member_doc_stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 522318
  AND measure_key = 'med_adherence_diabetes' order by inserted_at ;

-- 6/17 got a task -> order status = has_supply date 6/19, 15 day supply
-- SELECT '2024-06-19'::date + 15 = 7/4
    ;
-- got reopened task on 7/4 -> order status attempts exhausted,
-- %{
--           status: "unable_to_reach",
--           why: "Unable to reach, attempts exhausted second time",
--           task_status: "completed",
--           wf_is_closed: true,
--           wf_is_active: false
--         }

-- measure switched from ss to mco on 6/4
SELECT
    *
FROM
   fdw_member_doc.qm_pm_med_adh_handoffs
WHERE
    patient_id = 522318
AND measure_key = 'med_adherence_diabetes' order by inserted_at ;

-- 6/6 to 6/17 was the ss outage problem
SELECT inserted_at::date, count(*)
FROM
   fdw_member_doc.qm_pm_med_adh_handoffs GROUP BY 1 order by 1;

------------------------------------------------------------------------------------------------------------------------
/* 2
Source MCO when we get SS data for this patient. The meds on hand update from 6/26 is missing in the meds list.
Overlap with the Ozempic fills that would give extra days. Why the large delta in Plan file PCD and MCO file PDC? (516682)
*/
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
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
     , pf.meds_on_hand_date
  , pf.meds_on_hand_days_supply
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
    JOIN      fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 516682
and pm.measure_key = 'med_adherence_diabetes'
    -- pt.id =
ORDER BY pm.measure_key, pf.id ;
-- Source MCO when we get SS data for this patient. The meds on hand update from 6/26 is missing in the meds list.
-- Overlap with the Ozempic fills that would give extra days.
-- not sure what this means "Why the large delta in Plan file PCD and MCO file PDC?" (516682)
-- SS
+----------+----------+
|start_date|end_date  |
+----------+----------+
|2024-01-09|2024-03-04|
|2024-03-05|2024-04-01|
|2024-04-02|2024-04-29|
|2024-05-06|2024-06-02|
+----------+----------+
-- should have had task on 5/16
SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_synth_periods
WHERE
      patient_id = 516682
  AND measure_key = 'med_adherence_diabetes' ;

-- mco shows
+--------------+--------------+--------------------------+--------------------------+
|last_fill_date|next_fill_date|inserted_at               |processed_at              |
+--------------+--------------+--------------------------+--------------------------+
|2024-02-13    |2024-03-12    |2024-02-29 20:54:43.828959|2024-05-01 14:06:39.612775|
|2024-03-18    |2024-04-15    |2024-03-26 00:22:23.519837|2024-06-17 15:23:44.105251|
|2024-01-08    |2024-03-04    |2024-04-19 20:47:43.153439|2024-04-20 12:59:02.558295|
|2024-05-06    |2024-06-03    |2024-05-11 01:00:06.851729|2024-07-11 10:08:45.915827|
|2024-06-10    |2024-07-08    |2024-06-13 22:39:24.591958|2024-06-26 09:34:00.236071|
+--------------+--------------+--------------------------+--------------------------+
SELECT last_fill_date, next_fill_date, inserted_at, processed_at
FROM
    fdw_member_doc_stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 516682
  AND measure_key = 'med_adherence_diabetes' order by inserted_at ;


SELECT
    *
FROM
   fdw_member_doc.qm_pm_med_adh_handoffs
WHERE
    patient_id = 516682
AND measure_key = 'med_adherence_diabetes' order by inserted_at ;

------------------------------------------------------------------------------------------------------------------------
/* 3
   patient ID 7442 - MAD - ID - 1507031 - between 5/8 and 4/11 there was no task and it seems stuck in compliance loop but not sure why
   compliant 2024-04-12 to 2024-05-12 fill according to mco
 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
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
     , pf.meds_on_hand_date
  , pf.meds_on_hand_days_supply
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
    JOIN      fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 7442
and pm.measure_key = 'med_adherence_diabetes'
    -- pt.id =
ORDER BY pm.measure_key, pf.id ;
-- SS
+----------+----------+
|start_date|end_date  |
+----------+----------+
|2024-03-12|2024-04-10|
|2024-05-23|2024-06-21|
+----------+----------+
-- should have had task on 4/15 and 6/26
SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_synth_periods
WHERE
      patient_id = 7442
  AND measure_key = 'med_adherence_diabetes' ;

-- mco shows
+--------------+--------------+--------------------------+--------------------------+
|last_fill_date|next_fill_date|inserted_at               |processed_at              |
+--------------+--------------+--------------------------+--------------------------+
|2024-03-12    |2024-04-11    |2024-04-09 03:22:45.763311|2024-05-08 09:32:36.289927|
|2024-04-12    |2024-05-12    |2024-04-19 20:48:17.583596|2024-05-30 11:19:06.677098|
|2024-04-12    |2024-05-12    |2024-04-23 20:17:02.425064|2024-06-17 15:23:44.105251|
|2024-05-23    |2024-06-22    |2024-05-30 02:48:04.222466|2024-07-02 12:28:46.244867|
+--------------+--------------+--------------------------+--------------------------+
-- should have had task on 5/17, 6/27
SELECT *, last_fill_date, next_fill_date, inserted_at, processed_at
FROM
    fdw_member_doc_stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 7442
  AND measure_key = 'med_adherence_diabetes' order by inserted_at ;


SELECT
    *
FROM
   fdw_member_doc.qm_pm_med_adh_handoffs
WHERE
    patient_id = 7442
AND measure_key = 'med_adherence_diabetes' order by inserted_at ;