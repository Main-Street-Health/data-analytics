
-- the prd red list means they failed in year - 1
SELECT
    id
  , patient_id
  , mco_id
  , measure_id
  , year
  , is_mco_red_list
  , is_sure_scripts_red_list
  , patient_medication_ids
  , patient_med_adherence_synth_period_ids
  , days_supply
  , inserted_at
  , updated_at
  , measure_key
FROM
    analytics.prd.patient_med_adherence_red_list
WHERE
    patient_id = 716883 ;

-- stage the failed_year is the failed_year
DROP TABLE IF EXISTS _stg_rl;
CREATE TEMP TABLE _stg_rl AS 
SELECT *
FROM
    fdw_member_doc_stage.qm_pm_med_adh_red_list
WHERE
    failed_year = 2024
;
create UNIQUE INDEX  on _stg_rl(patient_id, measure_key);

DROP TABLE IF EXISTS _to_remove;
CREATE TEMP TABLE _to_remove AS 
SELECT *
FROM
    _stg_rl s
WHERE
    NOT EXISTS( SELECT
                    1
                FROM
                    prd.patient_med_adherence_red_list p
                WHERE
                      p.patient_id = s.patient_id
                  AND s.measure_key = p.measure_key
                  AND p.year = 2025 )
;

create UNIQUE INDEX  on _to_remove(patient_id, measure_key);

SELECT count(*)
FROM
    fdw_member_doc_stage.qm_pm_med_adh_red_list
WHERE
    failed_year = 2024


-- remove ones that are real
    delete
FROM
    _to_remove tr
using fdw_member_doc_stage.qm_pm_med_adh_mco_measures m
where tr.patient_id = m.patient_id
and tr.measure_key = m.measure_key
and m.measure_year = 2025
and m.is_prev_year_fail
;

SELECT count(*)
FROM
    _to_remove;
-- backupstage
    create table junk.med_adh_rl_20250319 as SELECT * from fdw_member_doc_stage.qm_pm_med_adh_red_list
;
-- delete out from stage
    delete from fdw_member_doc_stage.qm_pm_med_adh_red_list rl
    using _to_remove tr
    where tr.analytics_id = rl.analytics_id;


-- update metrics where stage rl no longer existse

    update fdw_member_doc.qm_pm_med_adh_metrics m
    set failed_last_year = false, updated_at = now()
where measure_year = 2025
and failed_last_year
and not exists(
    select 1
    from fdw_member_doc_stage.qm_pm_med_adh_red_list rl
    where rl.patient_id = m.patient_id
    and rl.measure_key = m.measure_key
    and rl.failed_year = 2024
)
;

------------------------------------------------------------------------------------------------------------------------
/* make sure priority updates */
------------------------------------------------------------------------------------------------------------------------
SELECT failed_last_year, priority_status
FROM
    fdw_member_doc.qm_pm_med_adh_metrics
WHERE
    patient_id = 716883
and measure_year = 2025
and measure_key = 'med_adherence_cholesterol'
;
