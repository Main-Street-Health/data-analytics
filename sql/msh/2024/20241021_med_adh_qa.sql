SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
     , m.fill_count
     , m.is_excluded
     , pm.is_active
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
    pm.patient_id = 448113
    -- pt.id = 
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      patient_id = 448113
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
order by id desc
;
SELECT * FROM qm_pm_activities where patient_measure_id = 1252419 order by id;
SELECT *
FROM
    patient_medication_fills

WHERE
      patient_id = 448113
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
      patient_id = 448113
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
;

SELECT *
FROM
    patient_task_activities WHERE patient_task_id = 1822816;

SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
     , pm.is_active
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
    pm.patient_id = 501276
    -- pt.id =
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

SELECT *
FROM
    qm_patient_config where patient_id = 501276 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';

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
    pm.patient_id = 1211357
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
      patient_id = 1211357
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';

SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 1211357
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';

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
    pm.patient_id = 776471
    -- pt.id = 
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
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
     , m.absolute_fail_date
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
    pm.patient_id = 669139
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_status_periods
WHERE
    patient_measure_id = 443955
ORDER BY
    id;

SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      patient_id = 669139
  AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#';


;
SELECT
    signal_date
  , payer_id
  , drug_name
  , last_fill_date
  , next_fill_date
  , days_supply
  , adr
  , absolute_fail_date
  , pdc
  , fill_count
  , unique_key
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 669139
  AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
ORDER BY
    signal_date;



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
    pm.patient_id = 730689
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 730689
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
    pm.patient_id = 95941
    -- pt.id =
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    s.key
  , s.name
  , s.send_to_reveleer
  , pm.measure_key
  , COUNT(*)
FROM
    qm_ref_patient_measure_statuses s
    JOIN qm_patient_measures pm ON s.key = pm.measure_status_key
GROUP BY
    s.key, s.name, s.send_to_reveleer, s.is_deleted, pm.measure_key

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
    pm.patient_id = 620618
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 620618
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
order by signal_date
;

------------------------------------------------------------------------------------------------------------------------
/* 1259144 */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 1259144
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#'
order by signal_date
;

------------------------------------------------------------------------------------------------------------------------
/* cody */
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
    pm.patient_id = 1280443
    -- pt.id = 
    and pm.measure_key = 'med_adherence_diabetes'
ORDER BY pm.measure_key, pf.id
;

1280443 / MAD


Members - Detail - 669139 - Quality Timeline - 443955
Members - Detail - 869914 - Tasks - Detail - 2004549
Members - Detail - 869914 - Tasks - Detail - 2004517


------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
     , pm.is_active
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
    pm.patient_id = 349683
    -- pt.id =
    and pm.measure_key = 'med_adherence_diabetes'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_patient_config
WHERE
      patient_id = 349683
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';

SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      patient_id = 349683
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';

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
      pm.patient_id = 193384
      -- pt.id =
  AND pm.measure_key = 'med_adherence_diabetes'
ORDER BY
    pm.measure_key, pf.id
;

SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 193384
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#'
ORDER BY
    signal_date
;


SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      patient_id = 193384
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#'
order by id
;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------

-- 10/7
-- SELECT inserted_at, sure_scripts_panel_id
-- FROM
--     sure_scripts_panel_patients
-- WHERE
--     patient_id = 1103130;
-- 
-- SELECT *
-- FROM
--     sure_scripts_med_history_details
-- WHERE
--       sure_scripts_panel_id = 15610
--   AND patient_id = '1103130';


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
    pm.patient_id = 1103130
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
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
    pm.patient_id = 846743
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
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
    pm.patient_id = 879344
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
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
    pm.patient_id = 1491852
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;