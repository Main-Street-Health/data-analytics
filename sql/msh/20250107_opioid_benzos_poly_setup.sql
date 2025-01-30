CREATE FUNCTION fn_build_med_overlap_measures(_sure_scripts_med_history_id bigint, _patient_ids bigint[]) RETURNS bigint
    LANGUAGE plpgsql
AS
$$ BEGIN
    DROP TABLE IF EXISTS _controls;
    CREATE TEMP TABLE _controls AS
    SELECT _sure_scripts_med_history_id sure_scripts_med_history_id
    , _patient_ids patient_ids
    ;
    create UNIQUE INDEX  on _controls(patient_ids)

 DROP TABLE IF EXISTS _opioids;
CREATE TEMP TABLE _opioids AS
SELECT DISTINCT ON (pm.patient_id, pm.start_date)
    pm.patient_id
  , start_date
  , end_date - start_date + 1           days_supply
  , LEAST(end_date, '2024-12-31'::DATE) end_date -- cap it to eoy
  , end_date                            real_end_date
  , days_supply                         real_days_supply
FROM
    _controls c
    prd.patient_medications pm
    JOIN ref.med_adherence_value_sets vs
         ON vs.code = pm.ndc AND pm.last_filled_date BETWEEN vs.from_date AND vs.thru_date
    JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
WHERE
      pm.start_date BETWEEN '2024-01-01'::DATE AND '2024-12-31'::DATE
  AND m.measure_id = 'COB'
  AND m.measure_version = '2024'
  AND m.table_id = 'COB-A'
--   AND table_id IN ('COB-A', 'COB-B')
ORDER BY
  -- take max days supply when same start date
  pm.patient_id, pm.start_date, pm.days_supply DESC
;


    return (select ctx.batch_id from _controls_fn_build_med_adherence_synthetics ctx);
END; $$;



DROP TABLE IF EXISTS _opioids;
CREATE TEMP TABLE _opioids AS
SELECT DISTINCT ON (pm.patient_id, pm.start_date)
    pm.patient_id
  , start_date
  , end_date - start_date + 1           days_supply
  , LEAST(end_date, '2024-12-31'::DATE) end_date -- cap it to eoy
  , end_date                            real_end_date
  , days_supply                         real_days_supply
FROM
    prd.patient_medications pm
    JOIN ref.med_adherence_value_sets vs
         ON vs.code = pm.ndc AND pm.last_filled_date BETWEEN vs.from_date AND vs.thru_date
    JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
WHERE
      pm.start_date BETWEEN '2024-01-01'::DATE AND '2024-12-31'::DATE
  AND m.measure_id = 'COB'
  AND m.measure_version = '2024'
  AND m.table_id = 'COB-A'
--   AND table_id IN ('COB-A', 'COB-B')
ORDER BY
  -- take max days supply when same start date
  pm.patient_id, pm.start_date, pm.days_supply DESC
;


DROP TABLE IF EXISTS _benzos;
CREATE TEMP TABLE _benzos AS
SELECT DISTINCT ON (pm.patient_id, pm.start_date)
    pm.patient_id
  , start_date
  , end_date - start_date + 1           days_supply
  , LEAST(end_date, '2024-12-31'::DATE) end_date
  , end_date                            real_end_date
  , days_supply                         real_days_supply
FROM
    prd.patient_medications pm
    JOIN ref.med_adherence_value_sets vs
         ON vs.code = pm.ndc AND pm.last_filled_date BETWEEN vs.from_date AND vs.thru_date
    JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
WHERE
      pm.start_date BETWEEN '2024-01-01'::DATE AND '2024-12-31'::DATE
  AND m.measure_id = 'COB'
  AND m.measure_version = '2024'
  AND m.table_id = 'COB-B'
ORDER BY
  -- take max days supply when same start date
  pm.patient_id, pm.start_date, pm.days_supply DESC
;


DROP TABLE IF EXISTS _denom;
CREATE TEMP TABLE _denom AS
SELECT
    patient_id
  , MIN(start_date)                                  ipsd
  , MAX(end_date)                                    max_end_date
  , COUNT(DISTINCT start_date)                       n_fills
  , SUM(days_supply)                                 total_days_supply
  , range_agg(DATERANGE(start_date, end_date, '[]')) segments
FROM
    _opioids
GROUP BY
    1
;


SELECT * FROM _denom;
SELECT count(*), count(distinct patient_id) FROM _denom;



DROP TABLE IF EXISTS _num;
CREATE TEMP TABLE _num AS
SELECT
    patient_id
  , MIN(start_date)                                  ipsd
  , MAX(end_date)                                    max_end_date
  , COUNT(DISTINCT start_date)                       n_fills
  , SUM(days_supply)                                 total_days_supply
  , range_agg(DATERANGE(start_date, end_date, '[]')) segments
FROM
    _benzos b
where exists(select patient_id from _denom d where d.patient_id = b.patient_id)
GROUP BY
    1
-- HAVING
--       COUNT(DISTINCT start_date) > 1
--   AND MIN(start_date) <= '2024-12-02'
--   AND SUM(days_supply) >= 15
;

SELECT * FROM _num;

SELECT count(*), count(distinct patient_id) FROM _num;
;

DROP TABLE IF EXISTS _cob;
CREATE TEMP TABLE _cob AS
SELECT
    denom.patient_id
  , denom.ipsd
  , denom.max_end_date                                              opiod_max_end_date
  , denom.n_fills                                                   n_opiod_fills
  , denom.total_days_supply                                         total_opiod_days_supply
  , denom.segments                                                  opioid_date_ranges
  , n.segments                                                      benzo_date_ranges
  , n.n_fills > 1                                                   is_numerator_elg
  , COUNT(DISTINCT d.day) FILTER ( WHERE b.patient_id IS NOT NULL ) n_days_overlap
FROM
    _denom denom
    JOIN ref.dates d ON d.day BETWEEN denom.ipsd AND denom.max_end_date
    JOIN _opioids o ON denom.patient_id = o.patient_id and d.day BETWEEN o.start_date and o.end_date
    LEFT JOIN _num n ON denom.patient_id = n.patient_id
    LEFT JOIN _benzos b ON denom.patient_id = b.patient_id AND d.day BETWEEN b.start_date AND b.end_date
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8
;

DROP TABLE IF EXISTS _cob_output;
CREATE TEMP TABLE _cob_output AS
SELECT
    c.patient_id
  , c.ipsd
  , c.opiod_max_end_date
  , c.n_opiod_fills
  , c.total_opiod_days_supply
  , c.opioid_date_ranges
  , c.benzo_date_ranges
  , c.n_days_overlap
  , is_numerator_elg AND n_days_overlap >= 30                                                is_in_numerator
  , c.n_opiod_fills > 1 AND c.ipsd <= '2024-12-02'::DATE AND c.total_opiod_days_supply >= 15 is_in_denominator
FROM
    _cob c
;
CREATE UNIQUE INDEX  on _cob_output(patient_id);

-- cob_from_ss_2024_20241218
SELECT
    co.patient_id
  , pay.name
  , state_pay.state
  , co.ipsd
  , co.opiod_max_end_date
  , co.n_opiod_fills
  , co.total_opiod_days_supply
  , co.opioid_date_ranges
  , co.benzo_date_ranges
  , co.n_days_overlap
  , co.is_in_numerator
  , co.is_in_denominator
FROM
    _cob_output co
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = co.patient_id
    JOIN fdw_member_doc.payers pay ON pay.id = sp.patient_payer_id
    JOIN fdw_member_doc.msh_state_payers state_pay ON state_pay.id = sp.patient_state_payer_id
;





SELECT
    COUNT(*)                                    n
  , COUNT(DISTINCT patient_id)                  n
  , COUNT(*) FILTER ( WHERE is_in_denominator ) n_denom
  , COUNT(*) FILTER ( WHERE is_in_denominator ) n_denom
  , COUNT(*) FILTER ( WHERE is_in_numerator )   n_num
  , COUNT(*) FILTER ( WHERE is_in_numerator )   n_num
  , (COUNT(*) FILTER ( WHERE is_in_numerator ) * 100.0 / COUNT(*) FILTER ( WHERE is_in_denominator )) ::DECIMAL(5, 2)
FROM
    _cob_output
    ;


------------------------------------------------------------------------------------------------------------------------
/* POLY-ACH */
------------------------------------------------------------------------------------------------------------------------
SELECT distinct value_set_item
FROM
    ref.med_adherence_measures m
join ref.med_adherence_value_sets vs on vs.value_set_id = m.value_set_id
WHERE
      m.measure_id = 'POLY-ACH'
  AND m.measure_version = '2024'
  AND m.table_id = 'POLY-ACH-A'
and m.is_med = 'Y'
and m.is_exclusion = 'N'
;

DROP TABLE IF EXISTS _elders;
CREATE TEMP TABLE _elders AS
SELECT p.id patient_id
FROM
    fdw_member_doc.patients p
WHERE AGE(DATE '2024-01-01', dob) >= INTERVAL '65 years';
;
create unique INDEX  on _elders(patient_id);

-- Anticholinergic
DROP TABLE IF EXISTS _aches;
CREATE TEMP TABLE _aches AS
SELECT DISTINCT ON (pm.patient_id, pm.start_date, vs.value_set_item)
    pm.patient_id
  , pm.start_date
  , LEAST(pm.end_date, '2024-12-31'::DATE) end_date
  , pm.drug_description
  , pm.days_supply
  , pm.ndc
  , vs.value_set_id
  , vs.value_set_item
FROM
    _elders e
    JOIN prd.patient_medications pm ON pm.patient_id = e.patient_id AND DATE_PART('year', start_date) = 2024
    JOIN ref.med_adherence_value_sets vs ON vs.code = pm.ndc AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
    JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
WHERE
      m.measure_id = 'POLY-ACH'
  AND m.measure_version = '2024'
  AND m.table_id = 'POLY-ACH-A'
ORDER BY
    pm.patient_id, pm.start_date, vs.value_set_item, pm.days_supply DESC
;
SELECT *
FROM
    _aches;

-- to be elg for the measure you need to dos's for the same ingredient (value_set_item)
DROP TABLE IF EXISTS _ache_ing_groups;
CREATE TEMP TABLE _ache_ing_groups AS
SELECT
    patient_id
  , value_set_item
  , min(start_date) ipsd
  , count(distinct start_date) n_fills
  , max(end_date) max_end_date
  , sum(days_supply) sum_days_supply
, range_agg(daterange(start_date, end_date)) segments
FROM
    _aches
GROUP BY patient_id, value_set_item
;

CREATE unique INDEX on _ache_ing_groups(patient_id, value_set_item);

DROP TABLE IF EXISTS _overlap;
CREATE TEMP TABLE _overlap AS
SELECT
    aig.patient_id
  , MIN(aig.ipsd)                                                                                      first_ipsd
  , COUNT(DISTINCT d.day)                                                                              days_supply
  , COUNT(DISTINCT d.day) FILTER (WHERE a_other.patient_id IS NOT NULL)                                n_overlap
  , COUNT(DISTINCT aig_other.value_set_item)                                                           n_other_ingredients
  , ARRAY_AGG(DISTINCT aig_other.value_set_item) FILTER ( WHERE aig_other.value_set_item IS NOT NULL ) other_ingredients
FROM
    _ache_ing_groups aig
    JOIN _aches a ON a.patient_id = aig.patient_id AND a.value_set_item = aig.value_set_item
    JOIN ref.dates d ON d.day BETWEEN a.start_date AND a.end_date
    LEFT JOIN _ache_ing_groups aig_other
              ON aig_other.patient_id = aig.patient_id
                  AND aig_other.value_set_item != aig.value_set_item
                  AND aig_other.n_fills >= 2
    LEFT JOIN _aches a_other
              ON a_other.patient_id = aig_other.patient_id
                  AND a_other.value_set_item = aig_other.value_set_item
                  AND d.day BETWEEN a_other.start_date AND a_other.end_date

WHERE
      aig.n_fills >= 2
  AND aig.ipsd <= '2024-12-02'
GROUP BY
    1
;





-- poly_ach_first_cut_20241218
SELECT
    patient_id
  , first_ipsd
  , days_supply
  , n_overlap
  , n_other_ingredients
  , other_ingredients
  , n_overlap >= 30 is_in_numerator
FROM
    _overlap o
;
SELECT count(*), count(distinct patient_id)
FROM
    _overlap;

SELECT count(distinct patient_id) FROM _aches; -- 51,063
SELECT count(distinct patient_id) FROM _ache_ing_groups where n_fills >= 2 and ipsd <= '2024-12-02'; -- 28,893
SELECT count(distinct patient_id) FROM _overlap where n_overlap >= 30; -- 3,016

SELECT 3016.0 / 28893; -- 0.10438514519087668293





