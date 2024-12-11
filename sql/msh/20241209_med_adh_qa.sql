SELECT
    pm.measure_status_key, m.*
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
WHERE
    m.measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#' AND m.patient_id = 268930;

SELECT * FROM qm_pm_med_adh_handoffs where measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#' AND patient_id = 268930 order by id desc;
SELECT * FROM stage.qm_pm_med_adh_mco_measures where measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#' AND patient_id = 268930 order by signal_date desc;

-- TODO: why is this flipping
SELECT * FROM qm_pm_med_adh_handoffs where patient_id = 1285427 order by id desc;
-- SELECT * FROM qm_pm_med_adh_synth_periods where measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#' AND patient_id = 268930 order by start_date desc;

-- abs_fail_date_in_past_mco_20241209_2
SELECT
    m.patient_id
  , m.measure_key
  , m.measure_source_key
  , m.adr
  , m.absolute_fail_date
  , m.pdc_to_date
  , m.ipsd
  , m.next_fill_date
  , m.fill_count
  , m.calc_to_date
  , pm.measure_status_key
  , p.name
  , p.id payer_id
  , sp.attribution_status
  , sp.attribution_substatus
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
    JOIN supreme_pizza sp ON sp.patient_id = m.patient_id
    JOIN payers p ON p.id = sp.patient_payer_id
WHERE
      pm.is_active
  AND NOT m.is_excluded
  AND m.measure_source_key = 'mco'
  AND m.adr > 0
  AND m.absolute_fail_date <= NOW()::DATE
;


-- what it might look like
DROP TABLE IF EXISTS _new_logic;
CREATE TEMP TABLE _new_logic AS 
WITH
    calced AS ( SELECT
                    m.patient_id
                  , m.measure_key
                  , m.measure_source_key
                  , m.adr
                  , m.calc_to_date
                  , m.next_fill_date
                  , m.absolute_fail_date
                  , m.adr + GREATEST(calc_to_date, next_fill_date)                               new_abs_fail_date
                  , CASE WHEN GREATEST(calc_to_date, next_fill_date) > NOW()::DATE THEN m.adr
                         ELSE m.adr - (NOW()::DATE - GREATEST(calc_to_date, next_fill_date)) END new_adr
                  , m.pdc_to_date
                  , m.ipsd
                  , m.fill_count
                  , pm.measure_status_key
                  , NOW()::DATE                                                                  today
                  , '2024-12-31'::DATE                                                           eoy
                FROM
                    qm_pm_med_adh_metrics m
                    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
                WHERE
                      pm.is_active
                  AND NOT m.is_excluded
                  AND m.measure_source_key = 'mco'
                  AND pm.measure_status_key = 'compliant_for_year'
                  AND NOW()::DATE + m.adr >= '2024-12-31'::DATE
--   AND m.calc_to_date + m.adr < '2024-12-31'::DATE
--   AND m.next_fill_date < '2024-12-31'::DATE
--   AND m.absolute_fail_date > NOW()::DATE
    )
SELECT
    patient_id
  , measure_key
  , measure_source_key
  , adr
  , calc_to_date
  , next_fill_date
  , absolute_fail_date
  , pdc_to_date
  , ipsd
  , fill_count
  , measure_status_key
  , new_abs_fail_date
  , new_adr
  , CASE
        WHEN fill_count = 1                   THEN '1 fill'
        WHEN new_abs_fail_date > '2024-12-31' THEN 'compliant_for_year'
        WHEN new_abs_fail_date <= today       THEN 'failed_for_year'
        WHEN next_fill_date >= today          THEN 'meds_on_hand'
        WHEN next_fill_date < today           THEN 'past_due_pending_nav' END potential_new_status
FROM
    calced
WHERE
    new_abs_fail_date <= eoy
;

SELECT *
FROM
    _new_logic;


SELECT
    potential_new_status
  , COUNT(*)
FROM
    _new_logic
GROUP BY
    1
;


SELECT
    u.full_name
  , COUNT(*)
FROM
    _new_logic nl
    JOIN patients p ON p.id = nl.patient_id
    JOIN care_team_members ctm ON ctm.care_team_id = p.care_team_id AND ctm.role = 'health_navigator'
    JOIN users u ON ctm.user_id = u.id
WHERE
    potential_new_status = 'past_due_pending_nav'
GROUP BY
    1
ORDER BY
    2 DESC
;

-- one fill onlys
DROP TABLE IF EXISTS _one_fills;
CREATE TEMP TABLE _one_fills AS
SELECT distinct on (pm.id) pm.id pm_id , sp.*
FROM
    _new_logic nl
join qm_patient_measures pm on pm.patient_id = nl.patient_id and pm.measure_key = nl.measure_key
join qm_pm_status_periods sp on pm.id = sp.patient_measure_id
WHERE
    potential_new_status = '1 fill'
and sp.measure_status_key != 'compliant_for_year'
order by pm.id, sp.id desc
;

SELECT measure_status_key, count(*)
FROM
    _one_fills
GROUP BY measure_status_key order by 2 desc
;

measure_status_key
one_fill_only_inactive
pending_compliance_check
one_fill_only
unable_to_reach
pharmacy_verified_pharmacy_found
past_due_pending_navigator
provider_refused
patient_refused
meds_on_hand
excluded


lost_for_year
pending_discharge
past_due_pending_provider

;
SELECT now()
    ;
;

-- TODO: why a mismatch in sources?
SELECT
    m.measure_source_key  m_source
  , pm.measure_source_key pm_source
  , COUNT(*)
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
WHERE
      pm.is_active
  AND NOT m.is_excluded
  AND pm.measure_source_key <> m.measure_source_key
GROUP BY
    1, 2
    ;


------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT errors[attempt - 1], *
FROM
    oban.oban_jobs
WHERE
    args ->> 'sql' ~* 'sp_med_adherence_load_surescripts_to_coop'
ORDER BY
    id DESC;


update oban.oban_jobs set state = 'discarded' where id = 195606107;

-- call public.qm_pm_med_adh_process();
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
    pm.patient_id = 687934
    -- pt.id =
    and pm.measure_key = 'med_adherence_diabetes'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_status_periods
WHERE
    patient_measure_id = 413785
ORDER BY
    id;

SELECT
    signal_date
  , pdc
  , adr
  , absolute_fail_date
  , last_fill_date
  , next_fill_date
  , is_reversal
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 687934
  AND measure_key = 'med_adherence_diabetes'
ORDER BY
    signal_date
    ;
SELECT *
FROM
qm_patient_config
WHERE
    patient_id = 687934
;
SELECT *
FROM
    supreme_pizza WHERE
    patient_id = 687934
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods
where
    patient_id = 687934
  AND measure_key = 'med_adherence_diabetes'
order by start_date
;


SELECT *
FROM
    patient_medication_fills
where
      patient_id = 687934
  AND measure_key = 'med_adherence_diabetes'
order by start_date;

SELECT *
FROM
    patient_medication_fills
where
      patient_id = 687934
--   AND measure_key = 'med_adherence_diabetes'
order by start_date;

SELECT *
FROM
    qm_pm_med_adh_metrics
where
    patient_id = 687934
  AND measure_key = 'med_adherence_diabetes'
;

SELECT *
FROM
    qm_pm_activities
WHERE
    patient_measure_id = 413785
ORDER BY
    id;


SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      patient_id = 687934
  AND measure_key = 'med_adherence_diabetes'
ORDER BY
    id
;
-- analytics
SELECT *
FROM
    analytics.prd.patient_medications
WHERE patient_id = 687934
and drug_description ~* 'jardiance'
order by start_date
;
SELECT patient_id, sure_scripts_panel_id, inserted_at
FROM
    sure_scripts_panel_patients
WHERE patient_id = 687934
order by inserted_at;
-- and drug_description ~* 'jardiance'
-- order by start_date

patient_id,sure_scripts_panel_id,inserted_at
687934,15808,2024-10-15 05:01:01.070020
687934,16270,2024-11-04 04:01:01.194376
687934,16435,2024-11-11 04:01:00.702670
687934,16600,2024-11-18 04:01:00.971383
687934,16765,2024-11-25 04:01:00.911620
687934,16930,2024-12-02 04:01:00.525217
687934,17095,2024-12-09 04:01:00.881205

   AND patient_id = 687934 AND sure_scripts_panel_id = 15445 -- AND inserted_at = '2024-09-30 05:01:01.320340'
AND patient_id = '687934' AND sure_scripts_panel_id = 15808 -- AND inserted_at = '2024-10-15 05:01:01.070020'
AND patient_id = '687934' AND sure_scripts_panel_id = 16270 -- AND inserted_at = '2024-11-04 04:01:01.194376'
AND patient_id = '687934' AND sure_scripts_panel_id = 16435 -- AND inserted_at = '2024-11-11 04:01:00.702670'
AND patient_id = '687934' AND sure_scripts_panel_id = 16600 -- AND inserted_at = '2024-11-18 04:01:00.971383'
AND patient_id = '687934' AND sure_scripts_panel_id = 16765 -- AND inserted_at = '2024-11-25 04:01:00.911620'
AND patient_id = '687934' AND sure_scripts_panel_id = 16930 -- AND inserted_at = '2024-12-02 04:01:00.525217'
AND patient_id = '687934' AND sure_scripts_panel_id = 17095 -- AND inserted_at = '2024-12-09 04:01:00.881205'

SELECT drug_description, last_filled_date, days_supply, inserted_at
FROM
    sure_scripts_med_history_details
where
    drug_description ~* 'jardiance'
--     and patient_id = '687934' AND sure_scripts_panel_id = 15445 -- AND inserted_at = '2024-09-30 05:01:01.320340'
-- AND patient_id = '687934' AND sure_scripts_panel_id = 15808 -- AND inserted_at = '2024-10-15 05:01:01.070020'
AND patient_id = '687934' AND sure_scripts_panel_id = 16270 -- AND inserted_at = '2024-11-04 04:01:01.194376'
-- AND patient_id = '687934' AND sure_scripts_panel_id = 16435 -- AND inserted_at = '2024-11-11 04:01:00.702670'
-- AND patient_id = '687934' AND sure_scripts_panel_id = 16600 -- AND inserted_at = '2024-11-18 04:01:00.971383'
-- AND patient_id = '687934' AND sure_scripts_panel_id = 16765 -- AND inserted_at = '2024-11-25 04:01:00.911620'
-- AND patient_id = '687934' AND sure_scripts_panel_id = 16930 -- AND inserted_at = '2024-12-02 04:01:00.525217'
-- AND patient_id = '687934' AND sure_scripts_panel_id = 17095 -- AND inserted_at = '2024-12-09 04:01:00.881205'
order by last_filled_date
;
