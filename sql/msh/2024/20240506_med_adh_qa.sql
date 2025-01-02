------------------------------------------------------------------------------------------------------------------------
/* #1 issue: panel wasn't generated
    why: panel logic wasn't taking the .9 pdc logic into account
   solution: update the panel logic (complete)
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
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
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
    pm.patient_id = 118831
and pm.measure_key = 'med_adherence_cholesterol'
    -- pt.id =
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods where patient_id = 118831 and measure_key = 'med_adherence_cholesterol';


2024-04-29
-- panels
-- SELECT * FROM sure_scripts_panel_patients WHERE patient_id = 118831 order by id;

| inserted\_at |
| :--- |
| 2024-01-31 20:48:57.344758 |
| 2024-02-25 12:37:00.255407 |
| 2024-03-20 11:37:01.054662 |

------------------------------------------------------------------------------------------------------------------------
/* #4 nfd=4/23, panel=4/28, task=4/29 completed=4/29 (pending appt) visit_date = 5/1, compliance check date 5/6
(elation fill 5/1)

panel created on 5/5 (sunday pm ) wouldn't have patient
5/5 panel is returned ~9pm ET but 1:22 UTC (what the calculations go off of
logic runs with 5/6 date but without the 4/6 compliance checks. we reopen if compliance check date is <= today

potential solutions:
1. don't do the evening rosters
2. only reopen when the compliance check date is < today
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
  , pf.order_status
  , pf.medication_status
     , pf.drug_description
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
     , pf.visit_date
  , pf.updated_at
  , pt.status
  , pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
         JOIN fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 118256
    -- pt.id =
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    fdw_member_doc.patient_task_activities WHERE patient_task_id in ( 1169098, 1198484 ) order by id;


SELECT * FROM sure_scripts_panel_patients where patient_id = 118256 order by id desc ;
| inserted\_at |
| :--- |
| 2024-04-28 11:37:00.650061 |
| 2024-01-31 20:48:57.344758 |
------------------------------------------------------------------------------------------------------------------------
/* #5
NFD=4/25
panel=4/30
task=5/1
reopened=5/6
same situation as above
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
     , pf.drug_description
  , pf.id pf_id
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
              fdw_member_doc.qm_patient_measures pm
        JOIN  fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 118295
and pm.measure_key = 'med_adherence_hypertension'
    -- pt.id =
ORDER BY pm.measure_key, pf.id
;


SELECT * FROM sure_scripts_panel_patients where patient_id = 118295 order by id desc ;
inserted_at
2024-04-30 11:37:00.205508
2024-03-24 11:37:00.432105
2024-03-22 11:37:00.622285
2024-01-31 20:48:57.344758

SELECT *
FROM
    fdw_member_doc.patient_task_activities WHERE patient_task_id in (1184272, 1201281 ) order by id;

------------------------------------------------------------------------------------------------------------------------
/* #6-7
NFD=4/18
panel=4/23
task=4/24
visit_date=5/1
reopened=5/6
same situation as above
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
     , pf.drug_description
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
     , pf.visit_date
  , pt.status
, pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
        JOIN  fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 118081
-- and pm.measure_key = 'med_adherence_hypertension'
    -- pt.id =
ORDER BY pm.measure_key, pf.id
;


SELECT * FROM sure_scripts_panel_patients where patient_id = 118081 order by id desc ;
inserted_at
2024-04-23 11:37:00.832882
2024-01-31 20:48:57.344758



SELECT *
FROM
    fdw_member_doc.patient_task_activities WHERE patient_task_id in (1150054, 1200395) order by id;


------------------------------------------------------------------------------------------------------------------------
/* #8 MCO task
NFD=4/28
(elation says picked up 4/23)
panel=5/3
patient not found in ss
   task should have been created on 5/4 but no sat panel
task=5/6 (1:22)
   
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
  , pf.drug_description
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pf.visit_date
  , pt.status
  , pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
        JOIN  fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 380211
and pm.measure_key = 'med_adherence_diabetes'
    -- pt.id =
ORDER BY pm.measure_key, pf.id
;

SELECT *
FROM
    sure_scripts_med_history_details WHERE patient_id = '380211' order by id desc;

SELECT * FROM sure_scripts_panel_patients where patient_id = 380211 order by id desc ;

inserted_at
2024-05-03 11:37:00.546920
2024-04-10 11:37:01.465021
2024-04-09 11:37:00.958619
2024-03-04 12:37:00.473835


SELECT *
FROM
    fdw_member_doc.patient_task_activities WHERE patient_task_id in (1200596) order by id;

-- patient not found in 4/3 panel
SELECT * FROM analytics.prd.sure_scripts_patient_hit WHERE patient_id = 380211;

SELECT * FROM analytics.prd.patient_medication_deletions WHERE patient_id = 380211;;
------------------------------------------------------------------------------------------------------------------------
/* #9
NFD=4/28
panel=5/2 (for cholesterol)
    (elation says picked up 5/3
    (would have done a panel on 5/3 if we hadn't done one on 5/2.
     We've got logic that will not do ss panel's that close together
     Still likely would not have picked up as fill was on the same day
     )
(would have created a task on the 4th but no sat panel)
task=5/6 (1:22)
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
  , pf.drug_description
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pf.visit_date
  , pt.status
  , pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
        JOIN  fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 118338
and pm.measure_key = 'med_adherence_hypertension'
    -- pt.id =
ORDER BY pm.measure_key, pf.id
;


SELECT * FROM sure_scripts_panel_patients where patient_id = 118338 order by id desc ;

inserted_at
2024-05-02 11:37:00.850219
2024-01-31 20:48:57.344758



SELECT *
FROM
    fdw_member_doc.patient_task_activities WHERE patient_task_id in (1201282) order by id;

------------------------------------------------------------------------------------------------------------------------
/* last point */
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
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
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
    pt.id = 1199649
ORDER BY pm.measure_key, pf.id
;
------------------------------------------------------------------------------------------------------------------------
/* from 5/7 tuesday round 2
  pt 1200103
   ss panel on 5/1 didn't pick up atorva 4/24 fill that is in elation
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
--     pm.patient_id =
    pt.id = 1200103
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    sure_scripts_panel_patients WHERE patient_id = 919869 order by id;
inserted_at
2024-02-25 12:37:00.255407
2024-04-21 11:37:00.530132
2024-04-30 11:37:00.205508
SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 11947
  AND patient_id = '919869'
and drug_description ~* 'atorva'
ORDER BY
    last_filled_date;;

------------------------------------------------------------------------------------------------------------------------
/* from amanda mobley */
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
/* pt 1200494
   mco task
   ss does not find any meds for this patient
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
--     pm.patient_id =
    pt.id = 1200494
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    sure_scripts_panel_patients WHERE patient_id = 249898 order by id;
inserted_at
2024-01-31 20:48:57.344758
2024-03-08 12:37:00.897066
2024-04-06 11:37:00.967459

SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 11155
  AND patient_id = '249898'
-- and drug_description ~* 'januvia'
ORDER BY
    last_filled_date;;

------------------------------------------------------------------------------------------------------------------------
/* 1201532
   missed fill by a day on first one
   sund/sat straddle situation on second task
   */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
     , m.inserted_at
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
    pm.patient_id = 249920
--     pt.id = 1201532
ORDER BY pm.measure_key, pf.id
;

SELECT *
FROM
    sure_scripts_panel_patients WHERE patient_id = 249920 order by id;

inserted_at
2024-01-31 20:48:57.344758
2024-04-30 11:37:00.205508


SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 11947
  AND patient_id = '249920'
and last_filled_date >= '2024-01-01'
and drug_description ~* 'RAMIPRIL'
ORDER BY
    last_filled_date;;

------------------------------------------------------------------------------------------------------------------------
/* pt 1200220
   missed fill by a day on first one
   sund/sat straddle situation on second task
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
    pm.patient_id = 970748
--     pt.id = 1200220
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    sure_scripts_panel_patients WHERE patient_id = 970748 order by id;

sure_scripts_panel_id,inserted_at
11947,2024-04-30 11:37:00.205508


SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 11947
  AND patient_id = '970748'
-- and drug_description ~* 'januvia'
ORDER BY
    last_filled_date;;
------------------------------------------------------------------------------------------------------------------------
/* pt 1201531
--    missed fill by a day on first one
   sund/sat straddle situation on second task
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
--     pm.patient_id = 970748
    pt.id = 1201531
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    sure_scripts_panel_patients WHERE patient_id = 249827 order by id;


sure_scripts_panel_id,inserted_at
11221,2024-04-08 11:37:00.395705
11815,2024-04-26 11:37:00.462784
12145,2024-05-07 06:00:00.745959



SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 11947
  AND patient_id = '970748'
-- and drug_description ~* 'januvia'
ORDER BY
    last_filled_date;;
------------------------------------------------------------------------------------------------------------------------
/* pt 1200188
   sund/sat straddle situation
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
--     pm.patient_id = 970748
    pt.id = 1200188
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    sure_scripts_panel_patients WHERE patient_id = 967873 order by id;


sure_scripts_panel_id,inserted_at
11221,2024-04-08 11:37:00.395705
11815,2024-04-26 11:37:00.462784
12145,2024-05-07 06:00:00.745959



SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 11947
  AND patient_id = '970748'
-- and drug_description ~* 'januvia'
ORDER BY
    last_filled_date;;

------------------------------------------------------------------------------------------------------------------------
/* pt 1190943
   pdc < .9 issue but wouldn't have fixed it as med wasn't picked up early
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
    pm.patient_id = 196386
--     pt.id = 1190943
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    sure_scripts_panel_patients WHERE patient_id = 196386 order by id;

sure_scripts_panel_id,inserted_at
10363,2024-03-14 11:37:01.887809
11254,2024-04-09 11:37:00.958619
11980,2024-05-01 11:37:00.819086



SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 11980
  AND patient_id = '196386'
and drug_description ~* 'statin'
ORDER BY
    last_filled_date;;

------------------------------------------------------------------------------------------------------------------------
/* pt 1201535
   5/3 query (for other measure) did not have the drug
   would have querried again on 5/4 if querried every day and maybe gotten it
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
    pm.patient_id = 250466
--     pt.id = 1201535
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    sure_scripts_panel_patients WHERE patient_id = 250466 order by id;

sure_scripts_panel_id,inserted_at
9769,2024-02-25 12:37:00.255407
12046,2024-05-03 11:37:00.546920

SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 12046
  AND patient_id = '250466'
and drug_description ~* 'losartan'
ORDER BY
    last_filled_date;;

Mobley summary
pt 1200494 - MCO task, ss did not find any meds for this patient
pt 1201532 - Missed fill by one day on the first task, second task was the sunday night issue
pt 1200220 - Missed fill by one day on the first task, second task was the sunday night issue
pt 1201531 - sunday night issue
pt 1200188 - sunday night issue
pt 1190943 - pdc < .9 issue but wouldn't have fixed it as med wasn't picked up early
pt 1201535 - 5/3 query (for other measure) did not have the drug would have querried again on 5/4 if querried every day and maybe gotten it

------------------------------------------------------------------------------------------------------------------------
/* eric towne */
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
     , m.is_excluded
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
--     pm.patient_id =
    pt.id = 1184799
ORDER BY pm.measure_key, pf.id
;
SELECT is_medication_adherence FROM fdw_member_doc.supreme_pizza where patient_id = 442515;
SELECT * from fdw_member_doc.qm_pm_med_adh_synth_periods where patient_id = 442515;
SELECT * from fdw_member_doc.patient_medication_fills where patient_id = 442515 and measure_key = 'med_adherence_hypertension';
SELECT * from fdw_member_doc_stage.qm_pm_med_adh_mco_measures where patient_id = 442515 and measure_key = 'med_adherence_hypertension';
