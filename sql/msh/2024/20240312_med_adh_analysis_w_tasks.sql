
-- contracted fails from Cheena
DROP TABLE IF EXISTS _contracted_fails;
CREATE TEMP TABLE _contracted_fails AS
SELECT distinct patient_id, measure_id, qm.measure_code, mamn.*
FROM
    fdw_member_doc_ent.patient_quality_measures pqm
join fdw_member_doc.quality_measures qm on qm.id = pqm.measure_id
join ref.med_adherence_measure_names mamn on qm.measure_code = mamn.coop_measure_id
WHERE
      quality_measure_group = 'Medication Adherence'
  AND is_plan_gap
  AND plan_gap_status = 'open';

select count(*) from _contracted_fails;

DROP TABLE IF EXISTS _fails;
CREATE TEMP TABLE _fails AS
SELECT
    pm.patient_id
  , pm.measure_id
  , pdc_to_date
  , ipsd
  , fill_count
  , days_not_covered
  , days_covered_to_period_end
  , ROUND(('2023-12-31'::DATE - ipsd) * .2) initial_adr
FROM
    prd.patient_med_adherence_year_measures pm
join _contracted_fails cf on cf.analytics_measure_id = pm.measure_id and cf.patient_id = pm.patient_id
WHERE
      pdc_to_date < .80
  AND fill_count > 1
  AND ipsd <= '2023-12-31'::DATE - 91
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      prd.patient_med_adherence_exclusions e
                  WHERE
                        e.patient_id = pm.patient_id
                    AND e.year = 2023
                    AND e.measure_id = pm.measure_id )
;


CREATE UNIQUE INDEX ON _fails(patient_id, measure_id);
SELECT * FROM _fails;



-- all meds
DROP TABLE IF EXISTS _sure_scripts_fills;
CREATE TEMP TABLE _sure_scripts_fills AS
SELECT
    pm.patient_id
  , m.measure_id
  , CONCAT_WS('::', pm.patient_id, m.measure_id)                            join_key
  , pm.start_date
  , LEAST(pm.start_date + MAX(pm.days_supply)::INT - 1, '2023-12-31'::DATE) end_date
  , MAX(pm.days_supply)::INT                                                days_supply
  , ARRAY_AGG(DISTINCT pm.id)                                               patient_medication_ids
  , ARRAY_AGG(pm.ndc)                                                       ndcs
FROM
    _fails f
    JOIN prd.patient_medications pm ON f.patient_id = pm.patient_id
        AND DATE_PART('year', pm.start_date) = 2023
    JOIN ref.med_adherence_value_sets vs ON vs.code = pm.ndc
        AND pm.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
        AND vs.code_type = 'NDC'
    JOIN ref.med_adherence_measures m
         ON m.value_set_id = vs.value_set_id AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
             AND m.is_med = 'Y'
             AND m.is_exclusion = 'N'
             AND f.measure_id = m.measure_id
             AND m.measure_version = '2024'
GROUP BY
    1, 2, 3, 4
;


------------------------------------------------------------------------------------------------
-- tricky bookend thing
-- this creates contiguous unique periods for the join_key(pat+measure)
DROP TABLE IF EXISTS _ss_beginning;
CREATE TEMPORARY TABLE _ss_beginning AS
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY join_key ORDER BY start_date) rn
FROM
    ( SELECT DISTINCT
          a.join_key
        , a.start_date
        , a.patient_id
        , a.measure_id
      FROM
          _sure_scripts_fills a
      WHERE
          NOT EXISTS( SELECT
                          1
                      FROM
                          _sure_scripts_fills b
                      WHERE
                            a.join_key = b.join_key
                        AND a.start_date - 1 BETWEEN b.start_date AND b.end_date ) ) x
;

DROP TABLE IF EXISTS _ss_ending;
CREATE TEMPORARY TABLE _ss_ending AS
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY join_key ORDER BY end_date) rn
FROM
    ( SELECT DISTINCT
          a.join_key
        , a.end_date end_date
        , a.patient_id
        , a.measure_id
      FROM
          _sure_scripts_fills a
      WHERE
          NOT EXISTS( SELECT
                          1
                      FROM
                          _sure_scripts_fills b
                      WHERE
                            a.join_key = b.join_key
                        AND a.end_date + 1 BETWEEN b.start_date AND b.end_date ) ) x
;

------------------------------------------------------------------------------------------------------------------------
/* JAKE monkey patch */
------------------------------------------------------------------------------------------------------------------------
INSERT
INTO
    _ss_beginning (join_key, start_date, patient_id, measure_id, rn)
SELECT join_key, '2024-01-01'::date, patient_id, measure_id, max(rn) + 1
FROM
    _ss_beginning
GROUP BY 1,2,3,4
;

INSERT
INTO
    _ss_ending (join_key, end_date, patient_id, measure_id, rn)
SELECT join_key, '2024-01-03'::date, patient_id, measure_id, max(rn) + 1
FROM
    _ss_ending
GROUP BY 1,2,3,4
;


DROP TABLE IF EXISTS _sure_scripts_periods;
CREATE TEMP TABLE _sure_scripts_periods AS
SELECT
    bg.join_key
  , bg.patient_id
  , bg.measure_id
  , ROW_NUMBER() OVER (PARTITION BY bg.join_key ORDER BY bg.start_date)        block
  , DATERANGE(LAG(e.end_date, 1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date), bg.start_date - 1, '[)')  prev_gap_range -- [ inclusive,  ) exclusive
  , bg.start_date - 1 - LAG(e.end_date, 1)
                        OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) prev_gap_days_missed
  , DATERANGE(bg.start_date, e.end_date, '[]')                                 has_meds_range
  , bg.start_date                                                              has_meds_start_date
  , e.end_date                                                                 has_meds_end_date

--   , LAG(start_date, -1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) - 1             next_block_start


  , DATERANGE(e.end_date, LAG(start_date, -1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) - 1, '(]') upcoming_gap_range
  , LAG(bg.start_date, -1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) - e.end_date - 1                                                                          days_missed_to_next_block
--   , bg.start_date -
--     LAG(end_date, 1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date)                    days_since_last_block
FROM
    _ss_beginning bg
    JOIN _ss_ending e ON bg.join_key = e.join_key
        AND bg.rn = e.rn
ORDER BY
    bg.patient_id, bg.measure_id, e.end_date
;
SELECT *
FROM
    _sure_scripts_periods;
------------------------------------------------------------------------------------------------------------------------
/* build mco ranges */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _mco_fills;
CREATE TEMP TABLE _mco_fills AS
SELECT
    m.*
  , CONCAT_WS('::', m.patient_id, m.measure_id) join_key
  , last_fill_date                              start_date
  , last_fill_date + days_supply                end_date
FROM
    analytics.prd.mco_patient_measure_rx_fills m
    JOIN _fails f ON m.patient_id = f.patient_id AND m.measure_id = f.measure_id
;



-- tricky bookend thing
-- this creates contiguous unique periods for the join_key(pat+measure)
DROP TABLE IF EXISTS _mco_beginning;
CREATE TEMPORARY TABLE _mco_beginning AS
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY join_key ORDER BY start_date) rn
FROM
    ( SELECT DISTINCT
          a.join_key
        , a.start_date
        , a.patient_id
        , a.measure_id
      FROM
          _mco_fills a
      WHERE
          NOT EXISTS( SELECT
                          1
                      FROM
                          _mco_fills b
                      WHERE
                            a.join_key = b.join_key
                        AND a.start_date - 1 BETWEEN b.start_date AND b.end_date ) ) x
;

DROP TABLE IF EXISTS _mco_ending;
CREATE TEMPORARY TABLE _mco_ending AS
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY join_key ORDER BY end_date) rn
FROM
    ( SELECT DISTINCT
          a.join_key
        , a.end_date end_date
        , a.patient_id
        , a.measure_id
      FROM
          _mco_fills a
      WHERE
          NOT EXISTS( SELECT
                          1
                      FROM
                          _mco_fills b
                      WHERE
                            a.join_key = b.join_key
                        AND a.end_date + 1 BETWEEN b.start_date AND b.end_date ) ) x
;

DROP TABLE IF EXISTS _mco_periods;
CREATE TEMP TABLE _mco_periods AS
SELECT
    bg.join_key
  , bg.patient_id
  , bg.measure_id
  , ROW_NUMBER() OVER (PARTITION BY bg.join_key ORDER BY bg.start_date)        block
  , DATERANGE(LAG(e.end_date, 1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date), bg.start_date - 1, '[)')  prev_gap_range -- [ inclusive,  ) exclusive
  , bg.start_date - 1 - LAG(e.end_date, 1)
                        OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) prev_gap_days_missed
  , DATERANGE(bg.start_date, e.end_date, '[]')                                 has_meds_range
  , bg.start_date                                                              has_meds_start_date
  , e.end_date                                                                 has_meds_end_date

--   , LAG(start_date, -1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) - 1             next_block_start


  , DATERANGE(e.end_date, LAG(start_date, -1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) - 1, '(]') upcoming_gap_range
  , LAG(bg.start_date, -1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) - e.end_date - 1                                                                          days_missed_to_next_block
--   , bg.start_date -
--     LAG(end_date, 1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date)                    days_since_last_block
FROM
    _mco_beginning bg
    JOIN _mco_ending e ON bg.join_key = e.join_key
        AND bg.rn = e.rn
ORDER BY
    bg.patient_id, bg.measure_id, e.end_date
;

SELECT * FROM _mco_periods ;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM _sure_scripts_periods o; where prev_gap_days_missed >= 25;

------------------------------------------------------------------------------------------------------------------------
/* Maybe something like this? */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _failure_event_gaps;
CREATE TEMP TABLE _failure_event_gaps AS
SELECT DISTINCT
    patient_id
  , measure_id
  , CASE
        WHEN measure_id = 'PDC-RASA' THEN 'med_adherence_hypertension_legacy'
        WHEN measure_id = 'PDC-DR'   THEN 'med_adherence_diabetes_legacy'
        WHEN measure_id = 'PDC-STA'  THEN 'med_adherence_cholesterol_legacy'
        END task_type
  , o.block
  , o.prev_gap_range
  , o.prev_gap_days_missed
  , o.has_meds_range
  , o.has_meds_start_date
  , o.has_meds_end_date
  , o.upcoming_gap_range
  , o.days_missed_to_next_block
FROM
    _sure_scripts_periods o
WHERE
    prev_gap_days_missed >= 25
;

create index on _failure_event_gaps(patient_id, task_type);
create index on _failure_event_gaps(patient_id, measure_id);
CREATE INDEX ON _failure_event_gaps USING GIST (prev_gap_range);
CREATE INDEX ON _failure_event_gaps USING GIST (has_meds_range);


-- nd failed
select count(*) from _fails;
-- nd with 25d gap
SELECT count(distinct (patient_id, measure_id)), count(*) FROM _failure_event_gaps;

-- nd gaps with more fills through eoy
SELECT
--     *
    count(distinct (patient_id, measure_id))
FROM
    _failure_event_gaps g
WHERE
      g.has_meds_start_date < '2024-01-01'

;
-- nd gaps with no more fills through eoy
SELECT
--     *
    count(distinct (patient_id, measure_id))
FROM
    _failure_event_gaps g
WHERE
      g.has_meds_start_date = '2024-01-01'
;



;

------------------------------------------------------------------------------------------------------------------------
/* PICKUP HERE

   */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _failure_event_tasks;
CREATE TEMP TABLE _failure_event_tasks AS
SELECT
    p.patient_id
  , p.measure_id
  , p.task_type
  , p.block
  , p.prev_gap_range
  , pt.inserted_at                                           task_inserted_at
  , mapt.system_verified_closed_at
  , p.has_meds_range
  , mapt.order_status
  , mapt.medication_status
  , mapt.is_system_verified_closed
  , mapt.system_verified_closed_at::DATE <@ p.has_meds_range is_verified_during_has_meds_range
  , mapt.is_task_reopened
  , p.prev_gap_days_missed
  , p.upcoming_gap_range
  , p.days_missed_to_next_block
  , p.has_meds_start_date
  , p.has_meds_end_date
  , pt.id                                                    task_id
  , pt.assigned_to_id
  , pt.due_at
  , pt.instructions
  , pt.priority
  , pt.status
  , pt.notes
  , pt.created_by_id
  , pt.modified_by_id
  , pt.updated_at
  , pt.start_date
  , pt.focus
  , pt.snooze_date
FROM
    _failure_event_gaps p
    JOIN fdw_member_doc.patient_tasks pt ON p.patient_id = pt.patient_id AND p.task_type = pt.task_type
        AND (pt.inserted_at::DATE <@ p.prev_gap_range or pt.inserted_at::DATE + 3 <@ p.prev_gap_range)
    JOIN fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
WHERE
    pt.task_type IN
    ('med_adherence_hypertension_legacy', 'med_adherence_cholesterol_legacy', 'med_adherence_diabetes_legacy')
ORDER BY
    pt.patient_id;



--
SELECT
--     *
    COUNT(DISTINCT (patient_id, measure_id))               nd_pm
  , COUNT(DISTINCT task_id)                                nd_tasks
  , COUNT(DISTINCT (patient_id, measure_id)) FILTER ( WHERE is_verified_during_has_meds_range ) nd_verified_during_med_range
  , COUNT(DISTINCT (patient_id, measure_id)) FILTER ( WHERE NOT is_verified_during_has_meds_range ) nd_not_verified_during_med_range
FROM
    _failure_event_tasks fet
;


-- sys verified outside of range and no mco data
SELECT
--     *
COUNT(DISTINCT (patient_id, measure_id)) nd_pm
FROM
    _failure_event_tasks f
WHERE
      NOT is_verified_during_has_meds_range
  AND NOT EXISTS ( SELECT 1
                   FROM _mco_periods mp
                   where mp.patient_id =  f.patient_id
                   and mp.measure_id = f.measure_id
                   and mp.has_meds_range @> f.system_verified_closed_at::date
                   )
;
-- sys verified outside of range and no mco data and no synth period
DROP TABLE IF EXISTS _bad_verfied;
CREATE TEMP TABLE _bad_verfied AS
SELECT
    *
FROM
    _failure_event_tasks f
WHERE
      NOT is_verified_during_has_meds_range
  AND NOT EXISTS ( SELECT 1
                   FROM _mco_periods mp
                   where mp.patient_id =  f.patient_id
                   and mp.measure_id = f.measure_id
                   and mp.has_meds_range @> f.system_verified_closed_at::date
                   );

-- ND sys ver, no mco data, no synth
WITH
    max_batch AS ( SELECT
                       bv.patient_id
                     , MAX(sp.batch_id) batch_id
                   FROM
                       _bad_verfied bv
                       JOIN prd.patient_med_adherence_synth_periods sp ON sp.patient_id = bv.patient_id
                           AND DATE_PART('year', sp.start_date) = 2023
                   GROUP BY 1 )
SELECT count(distinct (bv.patient_id, measure_id))
FROM
    _bad_verfied bv
    JOIN max_batch mb ON mb.patient_id = bv.patient_id
WHERE
    not EXISTS( SELECT
                1
            FROM
                prd.patient_med_adherence_synth_periods sp
            WHERE
                  sp.patient_id = bv.patient_id
              AND sp.measure_id = bv.measure_id
              AND DATE_PART('year', sp.start_date) = 2023
              AND bv.system_verified_closed_at BETWEEN sp.start_date AND sp.end_date
              AND sp.batch_id = mb.batch_id )
;


-- order, med status breakdown

SELECT
    order_status
  , medication_status
  , COUNT(DISTINCT task_id)                  nd_tasks
  , COUNT(DISTINCT (patient_id, measure_id)) nd_pm

FROM
    _failure_event_tasks f
GROUP BY
    1, 2
ORDER BY
    nd_tasks DESC


    ;

SELECT *
FROM
        _failure_event_tasks f
;



-- kind of weird with multiple mapts in different states, not sure how to think about these. only 459
SELECT *
FROM
    _failure_event_tasks fet
    JOIN ( SELECT
               task_id
             , COUNT(*)
           --     COUNT(*)
--   , COUNT(DISTINCT task_id)
           FROM
               _failure_event_tasks
           GROUP BY 1
           HAVING
               COUNT(*) > 1 ) x ON x.task_id = fet.task_id
ORDER BY
    fet.task_id
;

SELECT *
FROM
    fdw_member_doc.medication_adherence_patient_task
WHERE
    patient_task_id = 609088;
