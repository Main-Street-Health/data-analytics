------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
-- create index on analytics.junk.reveleer_life_of_chase_20240726(clientchasekey);
SELECT count(*) FROM analytics.junk.reveleer_life_of_chase_20240726;
SELECT * FROM analytics.junk.reveleer_life_of_chase_20240726;
SELECT *
FROM
    analytics.junk.reveleer_life_of_chase_20240726
WHERE
    clientchasekey ISNULL ;



-- in reveleer, never sent
SELECT r.*
FROM
    analytics.junk.reveleer_life_of_chase_20240726 r
    LEFT JOIN reveleer_chase_file_details cfd ON cfd.chase_id = r.clientchasekey AND cfd.yr = 2024
WHERE
    cfd.id ISNULL
;

DROP TABLE IF EXISTS _sent_but_not_returned;
CREATE TEMP TABLE _sent_but_not_returned AS
SELECT cfd.*
FROM
    reveleer_chase_file_details cfd
    LEFT JOIN analytics.junk.reveleer_life_of_chase_20240726 r ON cfd.chase_id = r.clientchasekey
WHERE
    r.chaseid ISNULL
AND cfd.yr = 2024
;

SELECT count(*), count(distinct chase_id)
FROM
    _sent_but_not_returned ;

CREATE TABLE junk.reveleer_missing_measures_resend_pqms_20240801 AS
SELECT DISTINCT
    UNNEST(rc.qm_patient_measure_ids) pqm_id
  , rc.reveleer_project_id
  , rc.id                             chase_id
, rc.patient_id
, rc.measure_code
FROM
    _sent_but_not_returned nr
    JOIN reveleer_chases rc ON nr.chase_id = rc.id
WHERE
    rc.yr = 2024
;
SELECT  rp.name, count(*)
FROM
    junk.reveleer_missing_measures_resend_pqms_20240801 m
join reveleer_projects rp on m.reveleer_project_id = rp.id
GROUP BY rp.name
;



-- SELECT *
-- FROM
UPDATE
    reveleer_chases c
SET
    external_chase_id = r.chaseid,
    is_confirmed_in_reveleer_system = TRUE,
    confirmed_in_reveleer_system_at = NOW(),
    updated_at = NOW()
FROM
    analytics.junk.reveleer_life_of_chase_20240726 r
WHERE
    c.yr = 2024
and c.id = r.clientchasekey
and not c.is_confirmed_in_reveleer_system
;

SELECT
    is_confirmed_in_reveleer_system
  , COUNT(*)
FROM
    reveleer_chases rc
where rc.yr = 2024
and rc.is_active

GROUP BY
    1;

SELECT *
FROM
    reveleer_chase_file_details cfd
join reveleer_chases rc on cfd.patient_id = rc.patient_id and cfd.measure_id = rc.measure_code
WHERE
    chase_id ISNULL ;

SELECT * FROM reveleer_chase_file_details WHERE patient_id = 245003 order by id desc;

select count(*) from raw.milliman_pro_20240802;
select count(*) from raw.milliman_ret_20240802;

/*
### Current state
    #### sql land
    - `sp_populate_sure_scripts_panel_patients` submits anyone pizza.is_med_adh=true to SS monthly or more freq
    - `qm_pm_med_adh_process` takes latest ss+mco data, create/updates metric, generates handoffs
      - if no data for measure we create a handoff with is_active_patient_measure=false
      - if patient is no longer pizza.is_med_adh we create a handoff with is_active_patient_measure=false
      - if patient is pizza.is_med_adh, has mco|ss data and an inactive pqm we create a handoff with is_active_patient_measure=true
      - if patient is pizza.is_med_adh, has mco|ss data and an pqm doesn't exist we create a handoff with is_active_patient_measure=true

    #### elixir land
    if handoff.is_active_patient_measure=false
        - updates pqm is_active_false
        - updates (if active wf exists) wf.is_active: false
        - closes tasks if exists, ( should probably change but always cancel_reason: "sure_scripts_data_removed")
        - creates activity with activity_key: "is_active_false", description: "Patient Measure was inactivated from an external source #{handoff.measure_source_key}"
    if handoff.is_active_patient_measure=true
       - creates measure if not exists
       - makes measure active if exists
       - calcs status, progress wf based on result

### Future state
    #### sql land
    - `sp_populate_sure_scripts_panel_patients` submits anyone pizza.is_med_adh=true to SS monthly or more freq
    - `qm_pm_med_adh_process` takes latest ss+mco data, create/updates metric, generates handoffs
      - if no data for measure we create a handoff with is_active_patient_measure=false
      - (DIFF) if patient is no longer qm_patient_config.is_active for measure we create a handoff with is_active_patient_measure=false
      - (DIFF) if patient is qm_patient_config.is_active for specific measure, has mco|ss data and an inactive pqm we create a handoff with is_active_patient_measure=true
      - (DIFF) if patient is qm_patient_config.is_active for specific measure, has mco|ss data and an pqm doesn't exist we create a handoff with is_active_patient_measure=true

    #### elixir land
    if handoff.is_active_patient_measure=false
        - updates pqm is_active_false
        - (DIFF) updates pqm fall_off_status_key to "fall_off"
        - updates (if active wf exists) wf.is_active: false
        - closes tasks if exists, ( should probably change but always cancel_reason: "sure_scripts_data_removed")
        - creates activity with activity_key: "is_active_false", description: "Patient Measure was inactivated from an external source #{handoff.measure_source_key}"
    if handoff.is_active_patient_measure=true
       - creates measure if not exists
       - makes measure active if exists
       - (DIFF) updates pqm fall_off_status_key to null
       - calcs status, progress wf based on result
*/