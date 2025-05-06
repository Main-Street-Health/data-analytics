------------------------------------------------------------------------------------------------------------------------
/* https://member-doc.prd.mainstreetruralhealth.com/members/detail/1645481/med-adh-overview/3017247
   Looks like it would be fixed if run through
   Need to check tomorrow 5/6
*/
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , m.calc_to_date
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
     , m.priority_pdc
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
    and pm.patient_id = 1645481
    -- pt.id = 
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id;
;
SELECT pdc, signal_date, inserted_at, updated_at
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures where patient_id = 1645481 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#' order by signal_date;
SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 1645481
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
ORDER BY
    last_filled_date desc;;
------------------------------------------------------------------------------------------------------------------------
/* https://member-doc.prd.mainstreetruralhealth.com/members/detail/1661168/med-adh-overview/3567371
  also looks like it would be fixed with todays running
   check 5/6
   */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
    , m.is_excluded
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , m.priority_status
     , m.fill_count
     , m.is_excluded
     , m.calc_to_date
     , pm.is_active
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
    and pm.patient_id = 1661168    -- pt.id =
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_status_periods WHERE patient_measure_id = 3567371;
select *
from
    qm_pm_med_adh_handoffs
where
      patient_id = 1661168 -- pt.id =
  and measure_key = 'med_adherence_cholesterol'
;

SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
patient_id = 1661168 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'

;
select *
from
    qm_patient_config
where
      patient_id = 1661168 -- pt.id =
  and measure_key = 'med_adherence_cholesterol'
;
------------------------------------------------------------------------------------------------------------------------
/*  */
--el---------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    patient_sure_scripts_panels order by panel_sent_at desc;