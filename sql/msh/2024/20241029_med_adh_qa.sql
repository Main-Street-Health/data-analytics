------------------------------------------------------------------------------------------------------------------------
/* banu 736334 */
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
    pm.patient_id = 736334
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    patient_medication_fills
where patient_id = 736334 and drug_description ~* 'atorv'

;
-- analytics

SELECT *
FROM
    ref.med_adherence_value_sets WHERE code = '43598010105';

DROP TABLE IF EXISTS _atorvas;
CREATE TEMP TABLE _atorvas AS
SELECT distinct ndc
FROM
    analytics.prd.patient_medications pm
where pm.drug_description ~* 'ATORVASTATIN'
;
CREATE INDEX on _atorvas(ndc);


SELECT *
FROM
    _atorvas pm
WHERE
    NOT EXISTS( SELECT 1 FROM ref.med_adherence_value_sets vs WHERE vs.code = pm.ndc );

------------------------------------------------------------------------------------------------------------------------
/* mdp codes that should be deletedc */
------------------------------------------------------------------------------------------------------------------------
-- dx's sent on 6/13
-- wdx on 8/13
-- got history on 9/26
-- wds on 10/7
SELECT
    wdx.*
-- xdx.*
FROM
    msh_external_emr_diagnoses xdx
    JOIN icd10s i ON xdx.icd10_id = i.id
    JOIN msh_cca_worksheet_dxs wdx ON wdx.external_emr_diagnosis_id = xdx.id
WHERE
      xdx.patient_id = 1380502
  AND i.code_formatted = 'N18.6'
;
SELECT *
FROM
    member_doc.stage.msh_md_portal_suspects_history
WHERE golgi_patient_id = 1380502
--   AND icd_10_code = 'N18.6'
    ;
------------------------------------------------------------------------------------------------------------------------
/* analytics  */
SELECT *
FROM
    prd.patient_medication_deletions WHERE patient_id = 368229;
start_date,days_supply,end_date
2024-07-14,90,2024-10-11
2024-07-14,90,2024-10-11

SELECT inserted_at, reason_for_query
FROM
    sure_scripts_panel_patients WHERE patient_id = 368229;

inserted_at,reason_for_query
2024-01-31 20:50:13.752505,30 day refresh for non med adherence patients
2024-04-27 11:37:00.626526,"Pull 5 days after expected next fill date: med_adherence_cholesterol, med_adherence_hypertension, med_adherence_cholesterol, med_adherence_hypertension"
2024-05-06 11:37:00.115594,"90day fill 14 days in: med_adherence_hypertension, med_adherence_hypertension"
2024-07-26 05:00:02.385649,"Pull 5 days after expected next fill date: med_adherence_hypertension, med_adherence_hypertension"
2024-10-29 03:01:01.609821,"Pull 10 days after expected next fill date: med_adherence_hypertension, med_adherence_hypertension"

-- md
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
        pm.patient_id = 368229
        -- pt.id = 
        and pm.measure_key = 'med_adherence_hypertension'
    ORDER BY pm.measure_key, pf.id
    ;
SELECT *
FROM
    qm_pm_activities WHERE patient_measure_id = 432464;

SELECT *
FROM
    patient_medication_fills where patient_id = 368229 AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#' and start_date >= '2024-01-01';
    
------------------------------------------------------------------------------------------------------------------------
/* #1 from doc
   adr = 0 from mco

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
--     pm.patient_id =
    pt.id = 1461597
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT
    adr, signal_date
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 810070
  AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#';

------------------------------------------------------------------------------------------------------------------------
/* adr and abs fail date are null */
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
--     pm.patient_id =
    pt.id = 2018079
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

------------------------------------------------------------------------------------------------------------------------
/* #3  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
     , m.fill_count
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
--     pm.patient_id =
    pt.id = 2034336
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

SELECT patient_id, measure_key, fill_count, adr, is_reversal, signal_date
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures where patient_id = 838472 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';