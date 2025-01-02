
SELECT
--     pm.measure_status_key
    pm.inserted_at ::date
    , count(*)
--     pm.inserted_at, count(*)
-- *
FROM
     fdw_member_doc.qm_pm_med_adh_metrics m
join fdw_member_doc.qm_patient_measures pm  on m.patient_measure_id = pm.id
where pm.is_active
and m.ipsd >= '2024-10-01'::date
GROUP BY  1
order by 1
-- order by pm.id desc

SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_handoffs h
where h.patient_id = 300730
order by h.id desc
;
SELECT *
FROM
    sure_scripts_panel_patients pp
where pp.patient_id = 300730
order by pp.id desc
;
SELECT
    '2024-01-01'::date + interval '9 months';



SELECT DISTINCT ON (patient_id)
    patient_id
  , message_id
FROM
    sure_scripts_med_history_details
WHERE
    patient_id IN
    ('850960', '922338', '921364', '370177', '657985', '502793', '172551', '239608', '466646', '64143', '927123',
     '880399', '889233', '687376', '646105', '527344', '645872', '1076961', '555926', '320333', '944568', '1285406',
     '1285406', '191231', '50300')
ORDER BY
    patient_id, sure_scripts_panel_id DESC
;

SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;

------------------------------------------------------------------------------------------------------------------------
/* 327677 */
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
    pm.patient_id = 327677
    -- pt.id =
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT filename
FROM
    documents;

------------------------------------------------------------------------------------------------------------------------
/* 
392470 - mco data says adr = 0
79481 - mco data adr = 0
392477 - ss adr = -15
1485249 - is past due pending navigator, not lost
1485249 - mco data adr = 0
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
    pm.patient_id = 1444165
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------