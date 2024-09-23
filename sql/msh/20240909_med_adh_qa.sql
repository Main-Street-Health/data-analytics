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
      pm.patient_id = 76644
  AND pm.measure_key = 'med_adherence_diabetes'
-- pt.id =
ORDER BY
    pm.measure_key, pf.id
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
--     pm.patient_id =
    pt.id = 1093616
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
    pm.patient_id = 16465
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;
------------------------------------------------------------------------------------------------------------------------
/* 
 Only counts for the same wf/task
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
    pm.patient_id = 611838
and pm.measure_key = 'med_adherence_cholesterol'
    -- pt.id = 
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
    pm.patient_id = 743236
    -- pt.id = 
    and pm.measure_key = 'med_adherence_hypertension'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures pm where
pm.patient_id = 743236
    -- pt.id = 
    and pm.measure_key = 'med_adherence_hypertension'
------------------------------------------------------------------------------------------------------------------------
/* banu check in visits */
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
        pm.patient_id = 673858
--         pt.id = 1840142
        and pm.measure_key = 'med_adherence_cholesterol'
    ORDER BY pm.measure_key, pf.id
    ;
    673858/tasks/detail/1840142
SELECT *
FROM
    patient_tasks where id = 1840142;
SELECT *
FROM
    users where id = 10013;
SELECT * FROM qm_pm_med_adh_wfs WHERE  id = 14532;
SELECT * FROM qm_pm_med_adh_potential_fills WHERE  id = 14532;
SELECT * FROM qm_pm_med_adh_90_day_conversions WHERE  patient_task_id =1840142;

------------------------------------------------------------------------------------------------------------------------
/* lost for the year but compliant from nelson */
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
    pm.patient_id in (54741,97407,97893,99053,99816,100291,100478,108228,109730,128568,144812)
    -- pt.id =
    -- and pm.measure_key = 'med_adherence_cholesterol'
and measure_status_key != 'meds_on_hand'
ORDER BY pm.measure_key, pf.id
;
    ;


SELECT patient_id, measure_key, measure_source_key, ipsd, next_fill_date, adr, pdc_to_date, calc_to_date
FROM
    qm_pm_med_adh_metrics m
where m.patient_id = 54741 AND m.measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
   OR m.patient_id = 97407 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
   OR m.patient_id = 97893 AND m.measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
   OR m.patient_id = 99053 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
   OR m.patient_id = 99816 AND m.measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
   OR m.patient_id = 100291 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
   OR m.patient_id = 100478 AND m.measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
   OR m.patient_id = 108228 AND m.measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#'
   OR m.patient_id = 109730 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
   OR m.patient_id = 144812 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
;
------------------------------------------------------------------------------------------------------------------------
/* MCO */
------------------------------------------------------------------------------------------------------------------------
SELECT distinct on (patient_id, measure_key) patient_id, measure_key, adr, pdc, next_fill_date
FROM
    stage.qm_pm_med_adh_mco_measures m
where m.patient_id = 54741 AND m.measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
   OR m.patient_id = 97407 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
   OR m.patient_id = 97893 AND m.measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
   OR m.patient_id = 99053 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
   OR m.patient_id = 99816 AND m.measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
   OR m.patient_id = 100291 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
   OR m.patient_id = 100478 AND m.measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
   OR m.patient_id = 108228 AND m.measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#'
   OR m.patient_id = 109730 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
   OR m.patient_id = 144812 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
order by patient_id, measure_key, inserted_at desc
;

SELECT DISTINCT ON (patient_id, measure_key)
    patient_id
  , measure_key
  , payer_id
  , adr
  , pdc
  , next_fill_date
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      next_fill_date >= '2025-01-01'
  AND adr < 0
  AND pdc > .8
ORDER BY
    patient_id, measure_key, inserted_at DESC;

------------------------------------------------------------------------------------------------------------------------
/* SS */
------------------------------------------------------------------------------------------------------------------------
SELECT distinct on (    m.patient_id , m.measure_key )
    m.patient_id
  , m.measure_key
  , m.measure_source_key
  , m.ipsd
  , m.next_fill_date
  , mco.next_fill_date
  , m.adr
  , mco.adr
  , m.pdc_to_date
  , mco.pdc
  , m.calc_to_date
FROM
    qm_pm_med_adh_metrics m
    JOIN stage.qm_pm_med_adh_mco_measures mco ON m.patient_id = mco.patient_id AND m.measure_key = mco.measure_key
WHERE
      measure_source_key = 'sure_scripts'
  AND m.patient_id = 97407
  AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
    OR m.patient_id = 144812 AND m.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
order by     m.patient_id , m.measure_key , mco.inserted_at desc


;
;
------------------------------------------------------------------------------------------------------------------------
/* https://github.com/Main-Street-Health/member-doc/issues/13117

   1098849: NFD 9/4, pdc .96

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
    pm.patient_id = 1098849
    -- pt.id =
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

------------------------------------------------------------------------------------------------------------------------
/* 901574 */
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
    pm.patient_id = 901574
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
--
-- SELECT
--     COUNT(DISTINCT patient_id)                                               nd_patients_total
--   , COUNT(*)                                                                 n_total_meds
--   , COUNT(DISTINCT patient_id) FILTER (WHERE dow_sun = 1)                    nd_patients_sunday
--   , COUNT(*) FILTER (WHERE dow_sun = 1)                                      n_sunday
--   , (COUNT(DISTINCT patient_id) FILTER (WHERE dow_sun = 1) * 100.0 /
--      COUNT(DISTINCT patient_id))::DECIMAL(16, 2)                             pct_patients_sunday
--   , (COUNT(*) FILTER (WHERE dow_sun = 1) * 100.0 / COUNT(*))::DECIMAL(16, 2) pct_meds_sunday
--   , (100.0 / 7.0)::DECIMAL(16, 2)                                            one_seventh_for_comparison
-- FROM
--     sure_scripts_med_history_details mhd
--     JOIN ref.dates d ON d.day = mhd.last_filled_date
-- WHERE
--       mhd.sure_scripts_med_history_id IN (15676, 15643, 15610, 15577, 15544)
--   AND mhd.sold_date IS NULL
--   AND mhd.last_filled_date::DATE BETWEEN '2024-07-01' AND '2024-07-01'::DATE + 55
-- ;

SELECT m.*
FROM
    qm_pm_med_adh_metrics m
-- join qm_patient_measures qpm ON m.patient_measure_id = qpm.id
WHERE
    m.patient_id = 693038;

SELECT * FROM qm_pm_med_adh_synth_periods WHERE patient_id = 693038;
SELECT * FROM qm_pm_med_adh_synth_periods WHERE patient_id = 693038;
SELECT is_medication_adherence FROM supreme_pizza WHERE patient_id = 693038;
SELECT * FROM qm_patient_config WHERE patient_id = 693038;


-- one off revert
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
--     pm.patient_id = 1702975
    pt.id = 1702975
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;;

------------------------------------------------------------------------------------------------------------------------
/* from matt */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.inserted_at
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
    pm.patient_id = 1461473--1195526
-- pt.id =
-- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY
    pm.measure_key, pf.id
;
