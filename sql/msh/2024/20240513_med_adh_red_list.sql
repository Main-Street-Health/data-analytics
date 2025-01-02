
------------------------------------------------------------------------------------------------------------------------
/* red_list */
------------------------------------------------------------------------------------------------------------------------
-- populating red list
SELECT
    payer_id
  , COUNT(DISTINCT (a.member_id, a.measure_key))                                    nd_pat_measures
  , COUNT(DISTINCT (a.member_id, a.measure_key)) FILTER (WHERE a.is_prev_year_fail) nd_pat_measures_failed
  , (COUNT(DISTINCT (a.member_id, a.measure_key)) FILTER (WHERE a.is_prev_year_fail) * 100.0) /
    COUNT(DISTINCT (a.member_id, a.measure_key))                                    pct_pat_measures_failed
FROM
    prd.member_rx_adherence a
GROUP BY
    payer_id;

SELECT * from raw.cigna_rx_adherence rx;
SELECT * FROM fdw_member_doc.payers where id in (49, 147);

create UNIQUE INDEX on prd.patient_med_adherence_red_list (patient_id, measure_key, year);
-- one time
INSERT
INTO
    prd.patient_med_adherence_red_list AS rl (patient_id, mco_id, measure_key, year, is_sure_scripts_red_list)
select
    y.patient_id, sp.patient_payer_id mco_id, mn.coop_measure_key, 2023 , true is_sure_scripts_red_list
FROM
    prd.patient_med_adherence_year_measures y
join fdw_member_doc.supreme_pizza sp on sp.patient_id = y.patient_id
join ref.med_adherence_measure_names mn on y.measure_id = mn.analytics_measure_id
WHERE
      pdc_to_date < .8
  AND y.year = date_part('year', now()) - 1
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      prd.patient_med_adherence_exclusions e
                  WHERE
                        y.patient_id = e.patient_id
                    AND y.measure_id = e.measure_id
                    AND e.year = 2023 )
ON CONFLICT (patient_id, measure_key, year)
    DO UPDATE
    SET
        is_sure_scripts_red_list = TRUE, updated_at = NOW()
WHERE
    NOT rl.is_sure_scripts_red_list;
;


-- ongoing
INSERT
INTO
    prd.patient_med_adherence_red_list AS rl (patient_id, mco_id, measure_key, year, is_mco_red_list)
SELECT DISTINCT ON (gm.patient_id, measure_key)
    gm.patient_id
  , rx.payer_id
  , measure_key
  , measure_year
  , TRUE
FROM
    prd.member_rx_adherence rx
    JOIN gmm.global_mco_member_mappings gmmm ON gmmm.member_id = rx.member_id AND gmmm.payer_id = rx.payer_id
    JOIN gmm.global_members gm ON gmmm.global_member_id = gm.id AND gm.is_duplicate IS FALSE
WHERE
      is_prev_year_fail
  AND gm.patient_id IS NOT NULL
  AND measure_year = DATE_PART('year', NOW())
  AND rx.payer_id != 40 -- exclude cigna for now, seems very high
ORDER BY
    gm.patient_id, measure_key, rx.inserted_at DESC
ON CONFLICT (patient_id, measure_key, year)
    DO UPDATE
    SET
        is_mco_red_list = TRUE, updated_at = NOW()
WHERE
    NOT rl.is_mco_red_list;




-- push to coop

-- CREATE TABLE stage.qm_pm_med_adh_red_list (
--     analytics_id             BIGINT PRIMARY KEY NOT NULL,
--     patient_id               BIGINT             NOT NULL,
--     measure_key              TEXT               NOT NULL,
--     failed_year              INT                NOT NULL,
--     is_sure_scripts_red_list BOOLEAN            NOT NULL DEFAULT FALSE,
--     is_mco_red_list          BOOLEAN            NOT NULL DEFAULT FALSE,
--     inserted_at              TIMESTAMP          NOT NULL DEFAULT NOW(),
--     updated_at               TIMESTAMP          NOT NULL DEFAULT NOW()
-- );
-- CREATE unique index on stage.qm_pm_med_adh_red_list (patient_id, measure_key, failed_year);
INSERT
INTO
    fdw_member_doc_stage.qm_pm_med_adh_red_list (analytics_id, patient_id, measure_key, failed_year, is_mco_red_list,
                                                 is_sure_scripts_red_list, inserted_at, updated_at)
SELECT
    id
  , patient_id
  , measure_key
  , 2023
  , is_mco_red_list
  , is_sure_scripts_red_list
  , NOW()
  , NOW()
FROM
    analytics.prd.patient_med_adherence_red_list
WHERE
    year = 2023;





------------------------------------------------------------------------------------------------------------------------
/* red_list  end*/
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    fdw_member_doc_stage.qm_pm_med_adh_red_list
    ;
