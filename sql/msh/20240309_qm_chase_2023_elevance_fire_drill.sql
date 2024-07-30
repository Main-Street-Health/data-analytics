
SELECT *
FROM
    junk.qm_chase_2023_elevance_last_batch j
-- join fdw_member_doc.patient_quality_measures pqm on pqm.patient_id = j.patient_id and pqm.measure_id = j.measure_id
-- join fdw_member_doc.patients p on p.first_name = j.pat_fn and p.last_name = j.pat_ln
-- join fdw_member_doc.patient_quality_measures pqm on pqm.patient_id = p.id
WHERE
    j.pqm_id ISNULL
;


-- INSERT
-- INTO
--     fdw_member_doc_stage.qm_chase_2023_elevance_brendon_adds_20240309 (patient_id, measure_id, yearr, status,
--                                                                        modified_by_id, inserted_at, updated_at, source,
--                                                                        impact_date, fall_off_status,
--                                                                        mco_source_state_payer_id)
-- SELECT
--     patient_id
--   , measure_id
--   , 2023                                    yearr
--   , 'open'                                  status
--   , 2                                       modified_by_id
--   , NOW()                                   inserted_at
--   , NOW()                                   updated_at
--   , 'mco'                                   source
--   , COALESCE(date_of_service, '2023-01-01') impact_date
--   , 'on_latest_file'                        fall_off_status
--   , x.state_payer_id                        mco_source_state_payer_id
-- FROM
--     junk.qm_chase_2023_elevance_last_batch x
-- WHERE
--     x.pqm_id ISNULL;


SELECT *
FROM
    junk.qm_chase_2023_elevance_brendon_adds_20240309;
call cb.x_util_create_fdw_member_doc();
SELECT *
FROM
    junk.qm_chase_2023_elevance_last_batch ;
alter table     junk.qm_chase_2023_elevance_last_batch  add state_payer_id bigint;
update junk.qm_chase_2023_elevance_last_batch set state_payer_id = case when state = 'KY' then 281 when state = 'TN' then 91 end
where true;

------------------------------------------------------------------------------------------------------------------------
/* member doc */
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE stage.qm_chase_2023_elevance_brendon_adds_20240309 (
    patient_id                BIGINT,
    measure_id                BIGINT,
    yearr                     INTEGER,
    status                    TEXT,
    modified_by_id            INTEGER,
    inserted_at               TIMESTAMP WITH TIME ZONE,
    updated_at                TIMESTAMP WITH TIME ZONE,
    source                    TEXT,
    impact_date               DATE,
    fall_off_status           TEXT,
    mco_source_state_payer_id BIGINT
);

DROP TABLE IF EXISTS _created_pqms;
CREATE TEMP TABLE _created_pqms AS
WITH
    ins AS (
        INSERT INTO patient_quality_measures(patient_id, measure_id, year, status,
                                             modified_by_id, inserted_at, updated_at,
                                             source, impact_date,
                                             fall_off_status, mco_source_state_payer_id)
            SELECT
                patient_id
              , measure_id
              , yearr
              , status
              , modified_by_id
              , inserted_at
              , updated_at
              , source
              , impact_date
              , fall_off_status
              , mco_source_state_payer_id
            FROM
                stage.qm_chase_2023_elevance_brendon_adds_20240309
            where patient_id is not null
            RETURNING id )
SELECT
    id pqm_id
FROM
    ins;

SELECT *
FROM
    _created_pqms;

;
alter table stage.qm_chase_2023_elevance_brendon_adds_20240309 add pqm_id bigint;
update
    stage.qm_chase_2023_elevance_brendon_adds_20240309 j
    set pqm_id = pqm.id
    from patient_quality_measures pqm
    JOIN _created_pqms cp ON cp.pqm_id = pqm.id
    where j.patient_id = pqm.patient_id AND j.measure_id = pqm.measure_id AND pqm.year = 2023
;

------------------------------------------------------------------------------------------------------------------------
/* ready to run reveleer data stager */
------------------------------------------------------------------------------------------------------------------------

    ;

-- call cb.x_util_create_fdw_member_doc();
-- SELECT *
update junk.qm_chase_2023_elevance_last_batch j
set pqm_id = s.pqm_id
FROM fdw_member_doc_stage.qm_chase_2023_elevance_brendon_adds_20240309 s
where   s.patient_id = j.patient_id and s.measure_id = j.measure_id
and j.pqm_id ISNULL
;
SELECT * FROM junk.qm_chase_2023_elevance_last_batch j WHERE pqm_id is not null;
SELECT * FROM junk.qm_chase_2023_elevance_last_batch j WHERE pqm_id is null;
SELECT *
FROM
    reveleer_projects WHERE state_payer_id in (281, 91);

SELECT *
FROM
    fdw_member_doc.patients p
    JOIN junk.qm_chase_2023_elevance_last_batch j ON p.first_name = j.pat_fn AND p.last_name = j.pat_ln
and j.patient_id ISNULL
;
DROP TABLE IF EXISTS _results;
CREATE TEMP TABLE _results AS
SELECT *
FROM
    ( SELECT
          j.*
        , MAX(cfd.inserted_at)      last_sent_at
        , MAX(cfd.reveleer_file_id) file_id
      FROM
          junk.qm_chase_2023_elevance_last_batch j
          LEFT JOIN reveleer_chase_file_details cfd ON cfd.chase_id = j.pqm_id
      GROUP BY
          doc_name, state, pat_fn, pat_ln, j.patient_id, gap, date_of_service, pqm_id, j.measure_id
                  , j.state_payer_id ) x
;

SELECT *
FROM
    _results r
left join reveleer_files rf on rf.id = r.file_id
-- where r.pqm_id = 315174
;


-- update fdw_member_doc.patient_quality_measures pqm
-- set mco_source_state_payer_id = j.state_payer_id
-- -- SELECT pqm.mco_source_state_payer_id
-- FROM
--     junk.qm_chase_2023_elevance_last_batch j
-- where j.pqm_id = pqm.id
-- and mco_source_state_payer_id ISNULL
-- -- join fdw_member_doc.patient_quality_measures pqm on j.pqm_id = pqm.id
-- -- WHERE
-- --       j.pqm_id IS NOT NULL
--   AND NOT EXISTS( SELECT
--                       1
--                   FROM
--                       reveleer_chase_file_details cfd
--                   WHERE
--                       cfd.chase_id = j.pqm_id )
-- ;

