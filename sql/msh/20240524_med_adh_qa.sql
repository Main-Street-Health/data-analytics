------------------------------------------------------------------------------------------------------------------------
/* Banu
   https://github.com/Main-Street-Health/member-doc/issues/10818
   pt 1267964

   reopened due to different data
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
    JOIN fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
--     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
--     pm.patient_id = 251288
-- and m.measure_key = 'med_adherence_hypertension'
-- pt.id = 1267964
pm.id = 539522
ORDER BY
    pm.measure_key, pf.id
;

SELECT *
FROM
    fdw_member_doc.qm_pm_activities WHERE patient_measure_id = 539522;

SELECT * FROM fdw_member_doc.qm_pm_med_adh_synth_periods WHERE patient_id = 931186  and measure_key = 'med_adherence_hypertension';
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 931186  ;


SELECT
    sure_scripts_panel_id
  , inserted_at
  , reason_for_query
FROM
    sure_scripts_panel_patients
WHERE
    patient_id = 931186
ORDER BY
    id;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT m.*
FROM
    fdw_member_doc.qm_pm_med_adh_potential_fills pf
join fdw_member_doc.patient_tasks pt on pf.patient_task_id = pt.id
join fdw_member_doc.qm_patient_measures pm on pm.id = pf.patient_measure_id
join fdw_member_doc.qm_pm_med_adh_metrics m on m.patient_measure_id = pm.id
WHERE
    pf.drug_description ~* 'insulin' and pt.status in ('new', 'in_progress');

SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_handoffs
WHERE
    patient_id IN (198316, 153430)
order by patient_id , id desc
;
select now();

------------------------------------------------------------------------------------------------------------------------
/* 6/4 */
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
    , pf.moved_to_ninety_day_status
  , pt.status
  , pt.id
, pt.task_type
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
    pm.patient_id = 669004
--     pt.id = 1251455
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM patient_task_activities WHERE patient_task_id in ( 1203243, 1251455 ) order by id;
SELECT * FROM qm_pm_med_adh_potential_fills WHERE patient_measure_id = 953023;
SELECT * FROM qm_pm_med_adh_wfs WHERE patient_measure_id = 953023;
SELECT * FROM qm_pm_activities WHERE patient_measure_id = 953023;
SELECT * FROM qm_pm_med_adh_metrics WHERE patient_measure_id = 953023;
SELECT * FROM qm_pm_status_periods WHERE patient_measure_id = 953023;
SELECT * FROM qm_patient_measures WHERE id = 953023;
SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
      patient_id = 669004
  AND measure_key = 'med_adherence_hypertension';


SELECT *
FROM
    stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 669004
  AND measure_key = 'med_adherence_hypertension';
SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      patient_id = 669004
  AND measure_key = 'med_adherence_hypertension' order by id;

------------------------------------------------------------------------------------------------------------------------
/*  */
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
    pm.patient_id = 532220
    -- pt.id =
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM qm_pm_med_adh_handoffs WHERE patient_id = 532220 order by id;
SELECT * FROM qm_pm_med_adh_metrics WHERE patient_id = 532220 order by id;
SELECT * FROM qm_pm_med_adh_potential_fills WHERE patient_measure_id = 636912 order by id;
SELECT * FROM qm_pm_activities WHERE patient_measure_id = 636912 order by id;
SELECT * FROM qm_patient_measures WHERE id = 636912 order by id;

SELECT pf.order_status, pf.inserted_at, pf.updated_at, pt.status, pt.inserted_at, pt.updated_at, pt.id, pt.modified_by_id
FROM
    qm_pm_med_adh_potential_fills pf
join patient_tasks pt on pt.id = pf.patient_task_id
where pf.id = 67305
;
SELECT *
FROM
    patient_task_activities where patient_task_id = 1199367;


------------------------------------------------------------------------------------------------------------------------
/* find and fix issues where patient went active - inactive - active and wf didn't open */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _potential_issues;
CREATE TEMP TABLE _potential_issues AS
SELECT DISTINCT
    h.patient_id
  , h.measure_key
FROM
    qm_pm_med_adh_handoffs h
    JOIN supreme_pizza sp ON h.patient_id = sp.patient_id
        AND sp.is_medication_adherence
    JOIN qm_patient_measures pm ON pm.patient_id = h.patient_id
        AND pm.measure_key = h.measure_key
        and pm.is_active
    JOIN qm_pm_med_adh_metrics m ON m.patient_measure_id = pm.id
        AND m.next_fill_date < NOW()::DATE - 5
        AND NOT m.is_excluded
        AND coalesce(adr, 1) > 0
WHERE
      NOT is_active_patient_measure
  AND EXISTS( SELECT
                  1
              FROM
                  qm_pm_med_adh_handoffs h2
              WHERE
                    h2.patient_id = h.patient_id
                AND h2.measure_key = h.measure_key
                AND h2.is_active_patient_measure
                AND h2.id > h.id )
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      qm_patient_measures pm
                      JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
                  WHERE
                        pm.patient_id = h.patient_id
                    AND pm.measure_key = h.measure_key
                    AND pf.inserted_at > h.inserted_at )
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      qm_pm_med_adh_wfs wf
                  WHERE
                        wf.patient_measure_id = m.patient_measure_id
                    AND wf.is_active )
;
SELECT *
FROM _potential_issues
    ;



INSERT
INTO
    qm_pm_med_adh_handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year,
                            is_active_patient_measure, measure_source_key, reason, inserted_at)
SELECT DISTINCT
--     count(*)
    m.measure_key
  , m.patient_id
  , m.id  qm_pm_med_adh_metric_id
  , measure_year
  , TRUE  is_active_patient_measure
  , m.measure_source_key
  , 'reactivating measure'
  , NOW() inserted_at
FROM
    _potential_issues pi
    JOIN qm_patient_measures pm ON pm.patient_id = pi.patient_id
        AND pm.measure_key = pi.measure_key
    JOIN qm_pm_med_adh_metrics m ON m.patient_measure_id = pm.id
;


-- join qm_pm_med_adh_metrics m ON pi.patient_id = m.patient_id and pi.measure_key = m.measure_key

SELECT * FROM qm_pm_med_adh_handoffs WHERE patient_id = 532220 order by id;
SELECT * FROM qm_pm_med_adh_handoffs WHERE patient_id = 532220 and measure_key ~* 'diabetes' order by id;
SELECT * FROM qm_pm_med_adh_metrics WHERE patient_id = 532220 and measure_key ~* 'diabetes' order by id;
SELECT *
FROM
    qm_pm_med_adh_exclusions WHERE patient_id = 532220;

SELECT count(distinct patient_task_id)
FROM
    qm_pm_med_adh_potential_fills
where inserted_at > now() - '2 hours'::interval
;

------------------------------------------------------------------------------------------------------------------------
/* issue with open task and no wf */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    qm_pm_med_adh_potential_fills pf
    JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    JOIN qm_pm_med_adh_wfs wf ON pf.qm_pm_med_adh_wf_id = wf.id
WHERE
      pt.status IN ('new', 'in_progress')
  AND NOT wf.is_active
;
SELECT *
FROM
    qm_pm_med_adh_potential_fills pf
join patient_tasks pt ON pf.patient_task_id = pt.id
where qm_pm_med_adh_wf_id in ( 50245,12018,22058,58155,45686 )
and
        pt.status not IN ('new', 'in_progress')
;


delete FROM qm_pm_med_adh_potential_fills WHERE id in (83341,17755,64287,93589,64285 );
delete FROM patient_task_activities WHERE patient_task_id in ( 1251455,1044739,1187906,1326959,1187904 );
delete FROM qm_pm_tasks WHERE qm_pm_tasks.patient_task_id in ( 1251455,1044739,1187906,1326959,1187904 );
delete FROM md_prioritized_tasks where task_id in ( 1251455,1044739,1187906,1326959,1187904 );
DELETE FROM patient_task_call_cadence_off_track WHERE patient_task_id in ( 1251455,1044739,1187906,1326959,1187904 );
delete FROM patient_tasks WHERE id in ( 1251455,1044739,1187906,1326959,1187904 );