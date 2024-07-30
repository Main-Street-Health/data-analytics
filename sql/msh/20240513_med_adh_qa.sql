------------------------------------------------------------------------------------------------------------------------
/* pt 118485
   mco measure with nfd of 4/23
     - originally received on 3/1, but updated on 5/11 and processed at 5/13?
   querried ss but synth period did not make it into coop?
*/
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
--      , pm.is_active
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
, pf.*
--   , pf.id pf_id
--      , pf.next_fill_date
--   , pf.drug_description
--   , pf.order_status
--   , pf.medication_status
--   , pf.pharmacy_verified_fill_date
--   , pf.pharmacy_verified_days_supply
--   , pf.system_verified_closed_at
--   , pf.inserted_at
--   , pf.updated_at
--   , pt.status
--   , pt.id
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
    pm.patient_id = 118485
and m.measure_key = 'med_adherence_diabetes'
--     pt.id = 1201535
ORDER BY pm.measure_key, pf.id
;

SELECT * FROM fdw_member_doc.qm_pm_med_adh_synth_periods WHERE patient_id = 118485 ;
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 118485 ;
SELECT is_medication_adherence FROM fdw_member_doc.supreme_pizza WHERE patient_id = 118485 ;
SELECT * FROM fdw_member_doc_stage.qm_pm_med_adh_mco_measures WHERE patient_id = 118485 and measure_key = 'med_adherence_diabetes';

SELECT
    sp.*,
    b.*
FROM
    prd.patient_med_adherence_synth_periods sp
join prd.patient_med_adherence_synth_period_batches b on b.id = sp.batch_id
WHERE
      patient_id = 118485
  AND batch_id = 14587
and sp.measure_id = 'PDC-DR'
;
SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_synth_periods WHERE batch_id = 14587
;

SELECT *
FROM
    prd.sure_scripts_patient_hit
WHERE
    patient_id = 118485 ;


SELECT * FROM sure_scripts_panel_patients WHERE patient_id = 118485 order by id;

SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 12277
  and sure_scripts_med_history_id = 12970
  AND patient_id = '118485'
and drug_description ~* 'Jardiance'
and last_filled_date::date >= '2024-01-01'::date
ORDER BY
    last_filled_date;;

SELECT *
FROM
    analytics.ref.med_adherence_value_sets WHERE code = '00597015330';
------------------------------------------------------------------------------------------------------------------------
/* pt 678541
   PDC .85
   NFD 5/6
   panel on 5/9
   task 5/13
   should close on query on 5/18

   working as designed, just missed the med by a day with our query
*/
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
--      , pm.is_active
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
     , pf.next_fill_date
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , wf.system_verified_closed_at
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
    pm.patient_id = 678541
-- and m.measure_key = 'med_adherence_hypertension'
--     pt.id = 1201535
ORDER BY pm.measure_key, pf.id
;

SELECT * FROM fdw_member_doc.qm_pm_med_adh_synth_periods WHERE patient_id = 678541 and measure_key = 'med_adherence_hypertension';
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 678541 ;

SELECT
    sp.*,
    b.*
FROM
    prd.patient_med_adherence_synth_periods sp
join prd.patient_med_adherence_synth_period_batches b on b.id = sp.batch_id
WHERE
      patient_id = 678541
order by b.id desc
--   AND batch_id = 14587;



SELECT * FROM sure_scripts_panel_patients WHERE patient_id = 678541 order by id;
sure_scripts_panel_id,inserted_at
11089,2024-04-04 11:37:00.156453
11947,2024-04-30 11:37:00.205508
12211,2024-05-09 06:00:00.370014

------------------------------------------------------------------------------------------------------------------------
/* pt 251288
    Unclear why no panel prior to 5/13 task

    4/17 SS Panel for previous WF compliance check
    PDC < .9

    NFD 5/12

    Task 5/13

-- should have
panel on 5/9, task on 5/10

-- issue was the +2 day vs -2 day
*/
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
--      , pm.is_active
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
     , m.days_covered_to_period_end
     , m.days_not_covered
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
     , pf.next_fill_date
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , wf.system_verified_closed_at
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
    pm.patient_id = 251288
-- and m.measure_key = 'med_adherence_hypertension'
--     pt.id = 1201535
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    fdw_member_doc.qm_pm_activities WHERE patient_measure_id = 429206;

SELECT * FROM fdw_member_doc.qm_pm_med_adh_synth_periods WHERE patient_id = 251288  and measure_key = 'med_adherence_hypertension';
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 251288  ;




SELECT sure_scripts_panel_id, inserted_at, reason_for_query FROM sure_scripts_panel_patients WHERE patient_id = 251288  order by id;

sure_scripts_panel_id,inserted_at,reason_for_query
10231,2024-03-10 11:37:00.607311,"Pull every 30 days for : lost_adr_gt_zero, lost_adr_gt_zero, lost_adr_gt_zero, lost_adr_gt_zero - med_adherence_hypertension, med_adherence_cholesterol, med_adherence_hypertension, med_adherence_cholesterol"
10693,2024-03-24 11:37:00.432105,"Pull every 30 days for : lost_adr_gt_zero, lost_adr_gt_zero, lost_adr_gt_zero, lost_adr_gt_zero - med_adherence_hypertension, med_adherence_cholesterol, med_adherence_hypertension, med_adherence_cholesterol"
11518,2024-04-17 11:37:00.597715,"WF compliance date check: med_adherence_cholesterol, med_adherence_cholesterol, med_adherence_hypertension, med_adherence_hypertension"


------------------------------------------------------------------------------------------------------------------------
/* pt 251344
Panel 4/18 for prev workflow
NFD 5/12
PDC .73
issue was the +2 day vs -2 day
*/
------------------------------------------------------------------------------------------------------------------------
-- today        ~D[2024-05-13]
-- two days ago ~D[2024-05-11]
-- Date.before?(two_days_ago, next_fill_date) would be true if NFD = 05-10

-- pp.next_fill_date + '2 days'::INTERVAL <= NOW()
-- 5/12 + 2 days = 5/14 WRONG
--
select '2024-05-12'::date + '2 days'::INTERVAL ;
select '2024-05-12'::date + 2 ;
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
--      , pm.is_active
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
--      , m.days_covered_to_period_end
--      , m.days_not_covered
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
     , pf.next_fill_date
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , wf.system_verified_closed_at
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
    pm.patient_id = 251344
-- and m.measure_key = 'med_adherence_hypertension'
--     pt.id = 1201535
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    fdw_member_doc.qm_pm_activities WHERE patient_measure_id = 429211;

SELECT * FROM fdw_member_doc.qm_pm_med_adh_synth_periods WHERE patient_id = 251344  and measure_key = 'med_adherence_hypertension';
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 251344  ;




SELECT sure_scripts_panel_id, inserted_at, reason_for_query FROM sure_scripts_panel_patients WHERE patient_id = 251344  order by id;

sure_scripts_panel_id,inserted_at,reason_for_query
10726,2024-03-25 11:37:00.743476,"WF compliance date check: med_adherence_cholesterol, med_adherence_cholesterol"
11551,2024-04-18 11:37:00.758344,Pull 5 days after expected next fill date: med_adherence_cholesterol


------------------------------------------------------------------------------------------------------------------------
/* pt 347366
NFD 5/5
panel on 5/10
ss did not find the patient
mco generated task
FAD
*/
------------------------------------------------------------------------------------------------------------------------

SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
--      , pm.is_active
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
--      , m.days_covered_to_period_end
--      , m.days_not_covered
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
     , pf.next_fill_date
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , wf.system_verified_closed_at
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
    pm.patient_id = 347366
-- and m.measure_key = 'med_adherence_hypertension'
--     pt.id = 1201535
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    fdw_member_doc.qm_pm_activities WHERE patient_measure_id = 429211;

SELECT * FROM fdw_member_doc.qm_pm_med_adh_synth_periods WHERE patient_id = 347366  and measure_key = 'med_adherence_hypertension';
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 347366  ;




SELECT sure_scripts_panel_id, inserted_at, reason_for_query FROM sure_scripts_panel_patients WHERE patient_id = 347366  order by id;


sure_scripts_panel_id,inserted_at,reason_for_query
10033,2024-03-04 12:37:00.473835,30 day refresh for non med adherence patients
12244,2024-05-10 06:00:01.471846,"Pull 5 days after expected next fill date: med_adherence_hypertension, med_adherence_diabetes, med_adherence_diabetes, med_adherence_hypertension"


SELECT *
FROM
    prd.sure_scripts_patient_hit
WHERE
    patient_id = 347366 ;

------------------------------------------------------------------------------------------------------------------------
/* fix potentially missing synths */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _coop_synths;
CREATE TEMP TABLE _coop_synths AS
SELECT distinct patient_id, batch_id
FROM
    fdw_member_doc.qm_pm_med_adh_synth_periods;
CREATE INDEX on _coop_synths(patient_id, batch_id);

SELECT
    patient_id
  , COUNT(*)
FROM
    _coop_synths c
GROUP BY
    patient_id
HAVING
    COUNT(*) > 1
;



DROP TABLE IF EXISTS _a_synths;
CREATE TEMP TABLE _a_synths AS
SELECT DISTINCT ON (patient_id)
    patient_id
  , batch_id
FROM
    prd.patient_med_adherence_synth_periods sp
where date_part('year', sp.start_date) = 2024
ORDER BY
    patient_id, batch_id DESC
    ;
CREATE INDEX on _a_synths(patient_id, batch_id);



SELECT * FROM analytics.prd.patient_med_adherence_synth_periods WHERE  patient_id = 85 and batch_id = 14488
SELECT * FROM fdw_member_doc.qm_pm_med_adh_synth_periods WHERE  patient_id = 85 and batch_id = 13135

INSERT
INTO
    fdw_member_doc.qm_pm_med_adh_synth_periods (analytics_id, patient_id, measure_key,
                                                batch_id, fn_iteration, is_moved, join_key, days_supply, rn,
                                                start_date, end_date, overlap_id, overlap_start_date,
                                                overlap_end_date, value_set_item, og_start_date, og_end_date,
                                                prev_start_date, prev_days_supply, patient_medication_ids, ndcs,
                                                inserted_at, updated_at, yr)
SELECT
    sp.id               analytics_id
  , sp.patient_id
  , coop_measure_key measure_key
  , sp.batch_id
  , fn_iteration
  , is_moved
  , join_key
  , days_supply
  , rn
  , start_date
  , end_date
  , overlap_id
  , overlap_start_date
  , overlap_end_date
  , value_set_item
  , og_start_date
  , og_end_date
  , prev_start_date
  , prev_days_supply
  , patient_medication_ids
  , ndcs
  , sp.inserted_at
  , NOW()
  , DATE_PART('year', sp.start_date)
FROM
    _a_synths a
    LEFT JOIN _coop_synths cs ON a.patient_id = cs.patient_id --and a.batch_id = cs.batch_id
    JOIN prd.patient_med_adherence_synth_periods sp ON sp.patient_id = a.patient_id AND sp.batch_id = a.batch_id
    JOIN ref.med_adherence_measure_names mamm ON mamm.analytics_measure_id = sp.measure_id
WHERE
    cs.patient_id ISNULL
;
------------------------------------------------------------------------------------------------------------------------
/* courtney 4/22 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
--      , pm.is_active
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
--      , m.days_covered_to_period_end
--      , m.days_not_covered
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
     , pf.next_fill_date
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , wf.system_verified_closed_at
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
    pm.patient_id = 368684
-- and m.measure_key = 'med_adherence_hypertension'
--     pt.id = 1201535
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    fdw_member_doc.qm_pm_activities WHERE patient_measure_id = 432489;
;
SELECT
    sure_scripts_panel_id
  , reason_for_query
  , inserted_at
FROM
    sure_scripts_panel_patients
WHERE
    patient_id = 368684
ORDER BY
    id;

SELECT last_filled_date, sold_date, days_supply, drug_description, message_id
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 12442
  AND patient_id = '368684'
and drug_description ~* 'losart'
;
