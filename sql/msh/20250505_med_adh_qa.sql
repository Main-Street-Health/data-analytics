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

------------------------------------------------------------------------------------------------------------------------
/* job errored out */
------------------------------------------------------------------------------------------------------------------------
SELECT
    COUNT(*)
FROM
    qm_pm_med_adh_handoffs
WHERE
    processed_at ISNULL
and reason = 'compliant with unverified wf'
;

-- SELECT *
-- FROM
-- update
--     qm_pm_med_adh_handoffs h
-- set processed_at = now()
-- WHERE
--     h.id = 15120060;

SELECT
    m.next_fill_date, m.priority_pdc
                    , qpmapf.*
FROM
    qm_pm_med_adh_handoffs h
    join qm_pm_med_adh_metrics m on m.id = h.qm_pm_med_adh_metric_id
    join qm_pm_med_adh_wfs wf on m.id = wf.qm_pm_med_adh_metric_id
        and wf.is_active
    join qm_pm_med_adh_potential_fills qpmapf ON wf.id = qpmapf.qm_pm_med_adh_wf_id
WHERE
    h.id = 15120060;


SELECT *
FROM qm_pm_med_adh_handoffs WHERE processed_at ISNULL
and reason ~* 'compliant'
;

-- 43240
SELECT *
FROM
    oban_jobs
WHERE
      queue ~* 'med_ad'
  AND worker = 'MD.QualityMeasures2.Workflows.MedAdhWorker'
order by id
;
-- UPDATE oban_jobs
-- SET
--     state        = 'available'
--   , scheduled_at = NOW()
--   , discarded_at = NULL
--   , max_attempts = max_attempts + 1
-- WHERE
--     id = 13107758;
SELECT *
FROM
    oban_jobs
WHERE
    id = 13107758;
;


SELECT h.reason, m.priority_pdc, m.priority_status, m.measure_source_key, m.next_fill_date
FROM
    qm_pm_med_adh_handoffs h
    join qm_pm_med_adh_metrics m ON h.qm_pm_med_adh_metric_id = m.id
WHERE
    h.id IN (
             15120103, 15120117, 15120133, 15120156, 15120165, 15120279, 15120327, 15120368, 15120543, 15120590, 15120643,
             15120728, 15120733, 15120745, 15120773, 15120785, 15120809, 15120894, 15120984, 15121033, 15121047, 15121066,
             15121149, 15121189, 15121235, 15121305, 15121317, 15121362, 15121375, 15121377, 15121382, 15121397, 15121421,
             15121424, 15121443, 15121444, 15121461, 15121564, 15121619, 15121687, 15121704, 15121761, 15121786, 15121788,
             15121789, 15121799, 15121852, 15121877, 15121882, 15121912, 15121980, 15122049, 15122069, 15122124, 15122145,
             15122163, 15122204, 15122205, 15122222, 15122268, 15122275, 15122306, 15122307, 15122344, 15122403, 15122404,
             15122406, 15122429, 15122448, 15122511, 15122684, 15122685, 15122742, 15122767, 15122768, 15122788, 15122789,
             15122834, 15122684, 15122685, 15122742, 15122767, 15122768, 15122788, 15122789, 15122834, 15122846, 15122684,
             15122685, 15122742, 15122767, 15122768, 15122788, 15122789, 15122834, 15122846, 15122875, 15122879, 15122957,
             15122975, 15123016, 15123029, 15123033, 15123167, 15123206, 15123274, 15123284, 15123314, 15123384
        );


------------------------------------------------------------------------------------------------------------------------
/* again */
------------------------------------------------------------------------------------------------------------------------
SELECT
    h.reason
  , m.priority_status
  , m.priority_pdc
  , m.measure_source_key
  , m.next_fill_date
  , qpm.measure_status_key
    , m.id
-- , qpm.id
  , wf.*
  , pf.*
FROM
    qm_pm_med_adh_handoffs h
    JOIN qm_pm_med_adh_metrics m ON h.qm_pm_med_adh_metric_id = m.id
    JOIN qm_patient_measures qpm ON m.patient_measure_id = qpm.id
    JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    JOIN public.qm_pm_med_adh_potential_fills pf ON qpm.id = pf.patient_measure_id
WHERE
    h.id = 15164045
;

SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
    qm_pm_med_adh_metric_id = 83872942
ORDER BY id
;
SELECT *
FROM
    qm_pm_med_adh_priority_histories
WHERE
    med_adh_metric_id = 83872942
ORDER BY
    id;

------------------------------------------------------------------------------------------------------------------------
/* failed */
------------------------------------------------------------------------------------------------------------------------
SELECT
    h.reason
  , pm.measure_status_key
  , m.*
  , wf.*
FROM
    qm_pm_med_adh_handoffs h
    JOIN qm_pm_med_adh_metrics m ON h.qm_pm_med_adh_metric_id = m.id
    JOIN qm_patient_measures pm ON pm.id = m.patient_measure_id
    left JOIN qm_pm_med_adh_wfs wf ON wf.patient_measure_id = pm.id
WHERE
    h.id IN (
--              15163057, 15163772, 15164028, 15164045, 15164130, 15165170, 15165194, 15165368, 15165373, 15165497,
--              15165585, 15165587,
             15174155, 15176317
        )
;
SELECT patient_status, start_at, end_at, is_medication_adherence
FROM
    supreme_pizza_history WHERE patient_id = 1628035 order by start_at;

SELECT *
FROM
    patient_sure_scripts_panels where patient_id = 1628035;

SELECT state, *
FROM
    oban_jobs
WHERE
      queue ~* 'med_ad'
  AND worker = 'MD.QualityMeasures2.Workflows.MedAdhWorker'
order by id;
SELECT
    COUNT(*)
FROM
    qm_pm_med_adh_handoffs
WHERE
    processed_at ISNULL
-- and reason = 'compliant with unverified wf'
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.next_fill_date
  , m.priority_pdc
  , m.adr
  , pm.measure_status_key
  , pm.measure_source_key
  , wf.*
  , pf.*
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
    JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
WHERE
    m.id IN (83967326, 83969574)
ORDER BY
    m.id, pf.id
;



SELECT * FROM patient_sure_scripts_panels where patient_id = 86472;
