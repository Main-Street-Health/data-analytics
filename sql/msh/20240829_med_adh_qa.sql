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
    pm.patient_id = 917236
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
      patient_id = 917236
  AND measure_key = 'med_adherence_diabetes';
SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 917236
  AND measure_key = 'med_adherence_diabetes'
order by start_date
;

SELECT * FROM patients WHERE id = 917236;
SELECT is_medication_adherence FROM supreme_pizza WHERE patient_id = 917236;

SELECT *
FROM
    qm_pm_med_adh_handoffs
where
      patient_id = 917236
  AND measure_key = 'med_adherence_diabetes'
order by id
;

SELECT *, patient_id, measure_key, pdc, adr, last_fill_date, days_supply, next_fill_date, inserted_at
FROM
    stage.qm_pm_med_adh_mco_measures
where
      patient_id = 917236
  AND measure_key = 'med_adherence_diabetes'
order by inserted_at
;

SELECT *
FROM
    qm_pm_activities where patient_measure_id = 419223 order by id;

update qm_pm_med_adh_handoffs
set processed_at = NULL
where id = 8264901;

------------------------------------------------------------------------------------------------------------------------
/*  banu lost from clinic msg 8/30 - fixed
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
    pm.patient_id = 553974
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    patients WHERE id = 553974 ;
SELECT *, inserted_at AT TIME ZONE 'utc' AT TIME ZONE 'america/chicago'
FROM
    qm_pm_med_adh_handoffs WHERE patient_id = 553974  and measure_key = 'med_adherence_diabetes' order by id;
UPDATE qm_pm_med_adh_handoffs
SET
    processed_at = NULL
WHERE
    id = 8270148;

------------------------------------------------------------------------------------------------------------------------
/* https://github.com/orgs/Main-Street-Health/projects/2/views/15?filterQuery=-status%3ADone+assignee%3ABrendonPierson&pane=issue&itemId=76203345 */
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
     , m.fill_count
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
    pm.patient_id = 970060
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 970060
  AND measure_key = 'med_adherence_cholesterol';

SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 970060
  AND measure_key = 'med_adherence_cholesterol';

------------------------------------------------------------------------------------------------------------------------
/* https://github.com/orgs/Main-Street-Health/projects/2/views/15?filterQuery=-status%3ADone+assignee%3ABrendonPierson&pane=issue&itemId=76203968 */
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
    pm.patient_id = 626115
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;

SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 626115
  AND measure_key = 'med_adherence_cholesterol'
;

SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      patient_id = 626115
  AND measure_key = 'med_adherence_cholesterol'
order by id
;


------------------------------------------------------------------------------------------------------------------------
/* https://github.com/orgs/Main-Street-Health/projects/2/views/15?filterQuery=-status%3ADone+assignee%3ABrendonPierson&pane=issue&itemId=76215081
 1347423 */
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
    pm.patient_id = 1347423
    -- pt.id =
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    patient_medication_fills
WHERE
    patient_id = 1347423
and drug_description ~* 'entresto'
;

-- SELECT *
-- FROM
--     analytics.ref.med_adherence_value_sets vs
--     JOIN ref.med_adherence_measures m ON vs.value_set_id = m.value_set_id
-- WHERE
--       code = '00078065920'
--   AND m.measure_id = 'PDC-RASA'
-- and m.measure_version = '2024'
    ;