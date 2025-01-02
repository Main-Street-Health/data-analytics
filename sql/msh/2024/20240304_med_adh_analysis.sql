DROP TABLE IF EXISTS _fails;
CREATE TEMP TABLE _fails AS
SELECT
    patient_id
  , measure_id
  , pdc_to_date
  , ipsd
  , fill_count
  , days_not_covered
  , days_covered_to_period_end
  , ROUND(('2023-12-31'::DATE - ipsd) * .2) initial_adr
FROM
    prd.patient_med_adherence_year_measures pm
WHERE
      pdc_to_date < .85
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



-- all meds
DROP TABLE IF EXISTS _patient_med_periods;
CREATE TEMP TABLE _patient_med_periods AS
SELECT
    pm.patient_id
  , m.measure_id
  , CONCAT_WS('::', pm.patient_id, m.measure_id)                               join_key
  , pm.start_date
  , least(pm.start_date + MAX(pm.days_supply)::INT - 1, '2023-12-31'::DATE)    end_date
  , MAX(pm.days_supply)::INT                                                   days_supply
  , ARRAY_AGG(DISTINCT pm.id)                                                  patient_medication_ids
  , ARRAY_AGG(pm.ndc)                                                          ndcs
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
GROUP BY
    1, 2, 3, 4
;




------------------------------------------------------------------------------------------------
-- tricky bookend thing
-- this creates contiguous unique periods for the join_key(pat+measure)
DROP TABLE IF EXISTS _beginning;
CREATE TEMPORARY TABLE _beginning AS
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY join_key ORDER BY start_date) rn
FROM
    ( SELECT DISTINCT
          a.join_key
        , a.start_date
        , a.patient_id
        , a.measure_id
      FROM
          _patient_med_periods a
      WHERE
          NOT EXISTS( SELECT
                          1
                      FROM
                          _patient_med_periods b
                      WHERE
                            a.join_key = b.join_key
                        AND a.start_date - 1 BETWEEN b.start_date AND b.end_date ) ) x
;

DROP TABLE IF EXISTS _ending;
CREATE TEMPORARY TABLE _ending AS
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY join_key ORDER BY end_date) rn
FROM
    ( SELECT DISTINCT
          a.join_key
        , a.end_date end_date
        , a.patient_id
        , a.measure_id
      FROM
          _patient_med_periods a
      WHERE
          NOT EXISTS( SELECT
                          1
                      FROM
                          _patient_med_periods b
                      WHERE
                            a.join_key = b.join_key
                        AND a.end_date + 1 BETWEEN b.start_date AND b.end_date ) ) x
;

------------------------------------------------------------------------------------------------------------------------
/* JAKE monkey patch */
------------------------------------------------------------------------------------------------------------------------
INSERT
INTO
    _beginning (join_key, start_date, patient_id, measure_id, rn)
SELECT join_key, '2024-01-01'::date, patient_id, measure_id, max(rn) + 1
FROM
    _beginning
GROUP BY 1,2,3,4
;

INSERT
INTO
    _ending (join_key, end_date, patient_id, measure_id, rn)
SELECT join_key, '2024-01-03'::date, patient_id, measure_id, max(rn) + 1
FROM
    _ending
GROUP BY 1,2,3,4
;


DROP TABLE IF EXISTS _output;
CREATE TEMP TABLE _output AS
SELECT
    bg.join_key
  , bg.patient_id
  , bg.measure_id
  , ROW_NUMBER() OVER (PARTITION BY bg.join_key ORDER BY bg.start_date)                        block
  , bg.start_date
  , e.end_date
  , LAG(start_date, -1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) - 1             next_block_start
  , DATERANGE(e.end_date, LAG(start_date, -1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) - 1,
              '(]')                                                                            upcoming_gap_range -- ( start inclusive, ] end exclusive
  , LAG(bg.start_date, -1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) - e.end_date days_missed_to_next_block
--   , bg.start_date -
--     LAG(end_date, 1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date)                    days_since_last_block
FROM
    _beginning bg
    JOIN _ending e ON bg.join_key = e.join_key
        AND bg.rn = e.rn
ORDER BY
    bg.patient_id, bg.measure_id, e.end_date
;

SELECT * FROM _output where patient_id = 7548  and measure_id = 'PDC-STA'; -- task: 7/13, autoclosed 8/11
SELECT * FROM _output where patient_id = 8892 and measure_id = 'PDC-DR';   -- 9/16, 9/22
SELECT * FROM _output where patient_id = 6552 and measure_id = 'PDC-RASA';  -- 8/27, 9/6
SELECT * FROM _output where patient_id = 118427 and measure_id = 'PDC-STA'; -- 3/28, 10/6

SELECT * FROM _output where patient_id = 69012 and measure_id = 'PDC-DR'; -- 8/1, 8/21 -- mco data
SELECT * FROM _output where patient_id = 20221 and measure_id = 'PDC-RASA'; -- 5/19, 6/12 -- mco data

SELECT *
FROM
    prd.patient_med_adherence_synth_periods
WHERE
      patient_id = 69012
  AND measure_id = 'PDC-DR'
ORDER BY
    id DESC;

SELECT *
FROM
    analytics.prd.mco_patient_measure_rx_fills
WHERE
    patient_id = 20221 and measure_id= 'PDC-RASA'
--       patient_id = 69012 AND measure_id = 'PDC-DR'
;


SELECT pt.patient_id, pt.inserted_at, pt.start_date, system_verified_closed_at, pt.patient_id, pt.task_type
FROM
     fdw_member_doc.patient_tasks pt
join fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
WHERE
    pt.id in (658904, 724235, 708915, 609175, 695344, 634658)
order by patient_id
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM _output where patient_id = 2575 and measure_id = 'PDC-STA'; -- 8/7, 8/24; 9/7 10/15

SELECT pt.patient_id, pt.inserted_at, pt.start_date, system_verified_closed_at, pt.patient_id, pt.task_type
FROM
     fdw_member_doc.patient_tasks pt
join fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
WHERE
--     pt.id = 698005
    pt.id = 716677
order by patient_id
;
--
SELECT * FROM _output where patient_id = 3477 and measure_id = 'PDC-STA'; -- 10/22, 10/26

SELECT pt.patient_id, pt.inserted_at, pt.start_date, system_verified_closed_at, pt.patient_id, pt.task_type
FROM
     fdw_member_doc.patient_tasks pt
join fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
WHERE
    pt.id = 775824
order by pt.patient_id
;
SELECT *
FROM
    analytics.prd.mco_patient_measure_rx_fills rx
WHERE
      patient_id = 3477
  AND measure_id = 'PDC-STA';;




-- from david
SELECT * FROM _output where patient_id = 19818 and measure_id = 'PDC-RASA'; -- 3/28, 3/31

SELECT pt.patient_id, pt.inserted_at, pt.start_date, system_verified_closed_at, pt.patient_id, pt.task_type
FROM
     fdw_member_doc.patient_tasks pt
join fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
WHERE
    pt.id = 609031
order by pt.patient_id
;
--

SELECT pt.patient_id, pt.inserted_at, pt.start_date, system_verified_closed_at, pt.patient_id, pt.task_type
FROM
     fdw_member_doc.patient_tasks pt
join fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
WHERE
    pt.id = 624526
order by pt.patient_id

SELECT * FROM _output where patient_id = 3688 and measure_id = 'PDC-STA'; -- 4/27, 5/11

SELECT pt.patient_id, pt.inserted_at, pt.start_date, system_verified_closed_at, pt.patient_id, pt.task_type
FROM
     fdw_member_doc.patient_tasks pt
join fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
WHERE
    pt.id = 624527
order by pt.patient_id
SELECT * FROM _output where patient_id = 3688 and measure_id = 'PDC-RASA'; -- 4/27, 5/11
;

------------------------------------------------------------------------------------------------------------------------
/* Maybe something like this? */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _pats;
CREATE TEMP TABLE _pats AS
    select distinct patient_id, measure_id, case
        when measure_id = 'PDC-RASA' then 'med_adherence_hypertension_legacy'
        when measure_id = 'PDC-DR' then 'med_adherence_diabetes_legacy'
        when measure_id = 'PDC-STA' then 'med_adherence_cholesterol_legacy'
        end task_type
    from _output;
create index on _pats(patient_id, task_type);
create index on _pats(patient_id, measure_id);

DROP TABLE IF EXISTS _out_of_wack;
CREATE TEMP TABLE _out_of_wack AS
SELECT
    p.patient_id, p.measure_id, pt.id task_id
FROM
    _pats p
    JOIN fdw_member_doc.patient_tasks pt ON p.patient_id = pt.patient_id AND p.task_type = pt.task_type
    JOIN fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
WHERE
      pt.task_type IN
      ('med_adherence_hypertension_legacy', 'med_adherence_cholesterol_legacy', 'med_adherence_diabetes_legacy')
  AND (
          NOT EXISTS( SELECT
                          1
                      FROM
                          _output o
                      WHERE
                            o.patient_id = p.patient_id
                        AND o.measure_id = p.measure_id
                        AND mapt.system_verified_closed_at BETWEEN o.start_date AND o.end_date )
              OR
          NOT EXISTS( SELECT
                          1
                      FROM
                          prd.mco_patient_measure_rx_fills m
                      WHERE
                            m.patient_id = p.patient_id
                        AND m.measure_id = p.measure_id
                        AND mapt.system_verified_closed_at BETWEEN m.last_fill_date AND m.last_fill_date + m.days_supply )
          )
ORDER BY
    pt.patient_id;

SELECT *
FROM
    _out_of_wack ;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------


SELECT count(distinct (patient_id, measure_id)) nd_patient_measures
     , count(distinct patient_id) nd_patients
FROM
    _output;


SELECT round(n_gt_25 * 100.0 / n) pct_gt_25
FROM
    ( SELECT
          COUNT(*)                                  n
        , COUNT(*) FILTER ( WHERE max_block >= 25 ) n_gt_25
      FROM
          ( SELECT
                patient_id
              , measure_id
              , MAX(days_missed_to_next_block) max_block
            FROM
                _output
            GROUP BY 1, 2 ) x ) y
;




date_range @> date

SELECT *
FROM
    _period_overlap
ORDER BY patient_id, measure_id, block;



-- select 952.0 / 55708;
SELECT
    COUNT(DISTINCT (f.patient_id, f.measure_id)) -- 337404
  , COUNT(DISTINCT (po.patient_id, po.measure_id)) -- 36525
FROM
    _fails f
    LEFT JOIN _period_overlap po ON po.patient_id = f.patient_id;

SELECT
    f.patient_id
  , f.measure_id
  , mn.coop_measure_key
  , f.pdc_to_date
  , f.ipsd
  , f.fill_count
  , f.days_not_covered
  , f.days_covered_to_period_end
  , f.initial_adr
  , '<------------'                                    year_info
  , '------------>'                                    block_info
  , po.start_date
  , po.end_date
  , po.block
  , po.gap_range
  , po.days_since_last_block
  , ROUND(days_since_last_block * 100.0 / initial_adr) pct_adr_used_in_gap
  , '------------>'                                    task_info
  , pt.id                                              task_id
  , pt.status                                          task_status
  , pt.notes                                           task_notes
  , mapt.medication_status
  , mapt.can_script_move
  , mapt.order_placed
  , mapt.contacted_pharmacy_to_cancel
  , mapt.order_status
  , mapt.patient_refused_reason
  , mapt.updated_by_id
  , mapt.inserted_at
  , mapt.updated_at
  , mapt.is_system_verified_closed
  , mapt.system_verified_closed_at
  , mapt.order_substatus
  , mapt.visit_date
  , mapt.is_task_reopened
  , mapt.expected_discharge_date
  , mapt.unknown_discharge_date
  , mapt.pharmacy_not_found
  , mapt.alt_pharmacy_id
  , mapt.previous_order_status
  , mapt.delivery_refused_reason
  , mapt.part_d_covered
  , mapt.duration
  , mapt.fill_date
FROM
    _fails f
    JOIN _period_overlap po ON po.patient_id = f.patient_id AND po.measure_id = f.measure_id
    JOIN ref.med_adherence_measure_names mn ON mn.analytics_measure_id = po.measure_id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pt.patient_id = po.patient_id
        AND pt.task_type ~* mn.coop_measure_key
        AND po.gap_range @> pt.inserted_at::DATE
    LEFT JOIN fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
ORDER BY
    f.patient_id, f.measure_id, start_date;


------------------------------------------------------------------------------------------------------------------------
/* END measure level */
------------------------------------------------------------------------------------------------------------------------




------------------------------------------------------------------------------------------------------------------------
/* Cut it by pat med ingredient instead */
------------------------------------------------------------------------------------------------------------------------

-- all meds
DROP TABLE IF EXISTS _patient_med_ingredient_periods;
CREATE TEMP TABLE _patient_med_ingredient_periods AS
SELECT
    pm.patient_id
  , m.measure_id
  , vs.value_set_item
  , CONCAT_WS('::', pm.patient_id, m.measure_id, vs.value_set_item)                               join_key
  , pm.start_date
  , least(pm.start_date + MAX(pm.days_supply)::INT - 1, '2023-12-31'::DATE) end_date
  , MAX(pm.days_supply)::INT                                                   days_supply
  , ARRAY_AGG(DISTINCT pm.id)                                                  patient_medication_ids
  , ARRAY_AGG(pm.ndc)                                                          ndcs
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
GROUP BY
    1, 2, 3, 4, 5
;



SELECT count(distinct(patient_id, measure_id, value_set_item))
FROM
    _patient_med_ingredient_periods --
;
SELECT *
FROM
    _patient_med_ingredient_periods
ORDER BY
    patient_id, measure_id, value_set_item, start_date
;


------------------------------------------------------------------------------------------------
-- tricky bookend thing
-- this creates contiguous unique periods for the join_key(pat+measure)
DROP TABLE IF EXISTS _beginning_med_ing;
CREATE TEMPORARY TABLE _beginning_med_ing AS
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY join_key ORDER BY start_date) rn
FROM
    ( SELECT DISTINCT
          a.join_key
        , a.start_date
        , a.patient_id
        , a.measure_id
        , a.value_set_item
      FROM
          _patient_med_ingredient_periods a
      WHERE
          NOT EXISTS( SELECT
                          1
                      FROM
                          _patient_med_ingredient_periods  b
                      WHERE
                            a.join_key = b.join_key
                        AND a.start_date - 1 BETWEEN b.start_date AND b.end_date ) ) x
;

DROP TABLE IF EXISTS _ending_med_ing;
CREATE TEMPORARY TABLE _ending_med_ing AS
SELECT *
     , ROW_NUMBER() OVER (PARTITION BY join_key ORDER BY end_date) rn
FROM
    ( SELECT DISTINCT
          a.join_key
        , a.end_date end_date
        , a.patient_id
        , a.measure_id
        , a.value_set_item
      FROM
          _patient_med_ingredient_periods a
      WHERE
          NOT EXISTS( SELECT
                          1
                      FROM
                          _patient_med_ingredient_periods  b
                      WHERE
                            a.join_key = b.join_key
                        AND a.end_date + 1 BETWEEN b.start_date AND b.end_date ) ) x
;

DROP TABLE IF EXISTS _period_overlap_med_ing;
CREATE TABLE _period_overlap_med_ing (
    id                    BIGSERIAL PRIMARY KEY,
    patient_id            BIGINT,
    measure_id            TEXT,
    value_set_item        TEXT,
    join_key              TEXT,
    start_date            DATE,
    end_date              DATE,
    block                 INT,
    gap_range             daterange,
    days_since_last_block INT
);



INSERT
INTO
    _period_overlap_med_ing(join_key, start_date, end_date, patient_id, measure_id, value_set_item, block, gap_range, days_since_last_block)
SELECT
    bg.join_key
  , bg.start_date
  , e.end_date
  , bg.patient_id
  , bg.measure_id
  , bg.value_set_item
  , ROW_NUMBER() OVER (PARTITION BY bg.join_key ORDER BY bg.start_date)     block
  , DATERANGE(LAG(end_date, 1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date),
              bg.start_date - 1,
              '(]')                                                                          gap_range -- ( start inclusive, ] end exclusive
  , bg.start_date -
    LAG(end_date, 1) OVER (PARTITION BY bg.join_key ORDER BY bg.start_date) days_since_last_block
FROM
    _beginning_med_ing bg
    JOIN _ending_med_ing e ON bg.join_key = e.join_key
        AND bg.rn = e.rn
;





SELECT
    f.patient_id
  , f.measure_id
  , mn.coop_measure_key
  , f.pdc_to_date
  , f.ipsd
  , f.fill_count
  , f.days_not_covered
  , f.days_covered_to_period_end
  , f.initial_adr
  , '<------------'                                    year_info
  , '------------>'                                    block_info
  , po.value_set_item
  , po.start_date
  , po.end_date
  , po.block
  , po.gap_range
  , po.days_since_last_block
  , ROUND(days_since_last_block * 100.0 / initial_adr) pct_adr_used_in_gap
  , '------------>'                                    task_info
  , pt.id                                              task_id
  , pt.status                                          task_status
  , pt.notes                                           task_notes
  , mapt.medication_status
  , mapt.can_script_move
  , mapt.order_placed
  , mapt.contacted_pharmacy_to_cancel
  , mapt.order_status
  , mapt.patient_refused_reason
  , mapt.updated_by_id
  , mapt.inserted_at
  , mapt.updated_at
  , mapt.is_system_verified_closed
  , mapt.system_verified_closed_at
  , mapt.order_substatus
  , mapt.visit_date
  , mapt.is_task_reopened
  , mapt.expected_discharge_date
  , mapt.unknown_discharge_date
  , mapt.pharmacy_not_found
  , mapt.alt_pharmacy_id
  , mapt.previous_order_status
  , mapt.delivery_refused_reason
  , mapt.part_d_covered
  , mapt.duration
  , mapt.fill_date
FROM
    _fails f
    JOIN _period_overlap_med_ing po ON po.patient_id = f.patient_id AND po.measure_id = f.measure_id
    JOIN ref.med_adherence_measure_names mn ON mn.analytics_measure_id = po.measure_id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pt.patient_id = po.patient_id
        AND pt.task_type ~* mn.coop_measure_key
        AND po.gap_range @> pt.inserted_at::DATE
    LEFT JOIN fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
ORDER BY
    f.patient_id, f.measure_id, start_date;




