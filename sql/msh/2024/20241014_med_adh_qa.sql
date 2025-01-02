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
    pm.patient_id = 970572
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM qm_pm_med_adh_potential_fills WHERE id = 267547;

SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 970572
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';


-- switch to analytics db
SELECT *
FROM
    analytics.prd.patient_medications
WHERE
      patient_id = 970572
  AND ndc LIKE '60505257808';
SELECT *
FROM
    sure_scripts_panel_patients WHERE  patient_id = 970572 order by id desc;
SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 15709
  AND patient_id = '970572'
  AND sure_scripts_med_history_details.product_code LIKE '60505257808'
;

-- add another ndc to the lookup
ndc,drug_description
70710177500,ATORVASTATIN 20MG TAB

SELECT *
FROM
    analytics.ref.med_adherence_value_sets vs
WHERE
    description ~* 'ATORVASTATIN 20 MG TAB' order by from_date desc;

INSERT
INTO
    ref.med_adherence_value_sets (value_set_id, value_set_subgroup, value_set_item, code_type, code, description,
                                  route, dosage_form, ingredient, strength, units, is_recycled, from_date, thru_date,
                                  attribute_type, attribute_value, inserted_at, updated_at)
VALUES
    ('STATINS', 'STATINS', 'ATORVASTATIN', 'NDC', '70710177500', 'ATORVASTATIN 20 MG TABLET', 'ORAL', 'TABLET',
     NULL, NULL, NULL, 'N', '2024-06-12', '2099-12-31', NULL, NULL, now(), now());


update
    fdw_member_doc.patient_medication_fills pf
set measure_key = 'med_adherence_cholesterol', updated_at = now()
where ndc = '70710177500'
;
------------------------------------------------------------------------------------------------------------------------
/*  #14060 */
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
     , pf.last_filled_date
     , pf.next_fill_date
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
--     pm.patient_id =
    pt.id = 2024672
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 1236213
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#'
order by signal_date desc
;

------------------------------------------------------------------------------------------------------------------------
/*  */
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
    pm.patient_id = 936599
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;o;
SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 936599
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#' order by signal_date desc;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.fill_count
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
    pm.patient_id = 1115809
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

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
    pm.patient_id = 750594
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM patient_medication_fills where patient_id = 750594 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';
SELECT * FROM stage.qm_pm_med_adh_mco_measures where patient_id = 750594 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';
