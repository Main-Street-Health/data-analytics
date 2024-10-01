
SELECT *
FROM
    analytics.junk.s_pqa_meas_yr_2023_value_sets20240612;
select * from junk.s_pqa_meas_yr_2023_value_sets_meds20240612;
SELECT *
FROM
    junk.s_pqa_meas_yr_2023_value_sets20240612 m
join junk.s_pqa_meas_yr_2023_value_sets_meds20240612 vs on vs.value_set_id = m.value_set_id
;
SELECT *
FROM
    analytics.ref.med_adherence_measures;
SELECT * FROM analytics.ref.notes where 'ref.med_adherence_measures' = any(tables)

------------------------------------------------------------------------------------------------------------------------
/* check for measure level differences */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.id
  , m.measure_id
  , m.measure_name
  , m.measure_version
  , m.table_id
  , m.table_name
  , m.value_set_id
  , m.is_med
  , m.is_exclusion
  , m.inserted_at
  , m.updated_at
FROM
    analytics.ref.med_adherence_measures m
    JOIN junk.s_pqa_meas_yr_2023_value_sets20240612 n ON m.measure_id = n.measure_id
        AND m.value_set_id = n.value_set_id
WHERE
      m.measure_version = '2024'
  AND (
          m.measure_id IS DISTINCT FROM n.measure_id OR
          m.measure_name IS DISTINCT FROM n.measure_name OR
          m.measure_version IS DISTINCT FROM n.measure_version::TEXT OR
          m.table_id IS DISTINCT FROM trim(n.table_id) OR
          m.table_name IS DISTINCT FROM n.table_name OR
          m.value_set_id IS DISTINCT FROM n.value_set_id OR
          m.is_med IS DISTINCT FROM n.is_med OR
          m.is_exclusion IS DISTINCT FROM n.is_exclusion
          )
    ;
-- no results means no differences

------------------------------------------------------------------------------------------------------------------------
/* check for measure med level differences */
------------------------------------------------------------------------------------------------------------------------

SELECT
    value_set_id
  , value_set_item
  , value_set_subgroup
  , code
  , COUNT(*)
FROM
    ref.med_adherence_value_sets
GROUP BY
    value_set_id, value_set_item, value_set_subgroup, code
HAVING
    COUNT(*) > 1
ORDER BY
    5 DESC
;
-- unique grain appears to be
-- create unique index on junk.s_pqa_meas_yr_2023_value_sets_meds20240612(value_set_id , value_set_item , value_set_subgroup , code);
-- update junk.s_pqa_meas_yr_2023_value_sets_meds20240612 j set thru_date = '2099-12-31' WHERE thru_date ISNULL ;


-- differences
SELECT
    vs.code_type
  , j.code_type
  , vs.description
  , j.description
  , vs.route
  , j.route
  , vs.dosage_form
  , j.dosage_form
  , vs.ingredient
  , j.ingredient
  , vs.strength
  , j.strength
  , vs.units
  , j.units
  , vs.is_recycled
  , j.is_recycled
  , vs.from_date
  , j.from_date
  , vs.thru_date
  , j.thru_date
  , vs.attribute_type
  , j.attribute_type
  , vs.attribute_value
  , j.attribute_value
FROM
    analytics.ref.med_adherence_value_sets vs
    JOIN junk.s_pqa_meas_yr_2023_value_sets_meds20240612 j ON j.value_set_id = vs.value_set_id
        AND j.value_set_item = vs.value_set_item
        AND j.value_set_subgroup = vs.value_set_subgroup
        AND j.code = vs.code
WHERE
     vs.code_type IS DISTINCT FROM TRIM(j.code_type)
  OR vs.description IS DISTINCT FROM TRIM(j.description)
  OR vs.route IS DISTINCT FROM TRIM(j.route)
  OR vs.dosage_form IS DISTINCT FROM TRIM(j.dosage_form)
  OR vs.ingredient IS DISTINCT FROM TRIM(j.ingredient)
  OR vs.strength IS DISTINCT FROM TRIM(REGEXP_REPLACE(j.strength, '\.0', ''))
  OR vs.units IS DISTINCT FROM TRIM(j.units)
  OR vs.is_recycled IS DISTINCT FROM TRIM(j.is_recycled)
  OR vs.from_date IS DISTINCT FROM TRIM(j.from_date)::DATE
  OR vs.thru_date IS DISTINCT FROM TRIM(j.thru_date)::DATE
  OR vs.attribute_type IS DISTINCT FROM TRIM(j.attribute_type)
  OR vs.attribute_value IS DISTINCT FROM TRIM(REGEXP_REPLACE(j.attribute_value, '\.0', ''))
ORDER BY
    vs.code
;

-- updates

UPDATE analytics.ref.med_adherence_value_sets vs
SET
    code_type = TRIM(j.code_type), description = TRIM(j.description), route = TRIM(j.route)
                                 , dosage_form = TRIM(j.dosage_form), ingredient = TRIM(j.ingredient)
                                 , strength = TRIM(j.strength), units = TRIM(j.units)
                                 , is_recycled = TRIM(j.is_recycled), from_date = TRIM(j.from_date)::DATE
                                 , thru_date = TRIM(j.thru_date)::DATE, attribute_type = TRIM(j.attribute_type)
                                 , attribute_value = TRIM(j.attribute_value)
                                 , updated_at = NOW()
FROM
    junk.s_pqa_meas_yr_2023_value_sets_meds20240612 j
WHERE
      (
          j.value_set_id = vs.value_set_id
              AND j.value_set_item = vs.value_set_item
              AND j.value_set_subgroup = vs.value_set_subgroup
              AND j.code = vs.code
          )
  AND (
          vs.code_type IS DISTINCT FROM TRIM(j.code_type)
              OR vs.description IS DISTINCT FROM TRIM(j.description)
              OR vs.route IS DISTINCT FROM TRIM(j.route)
              OR vs.dosage_form IS DISTINCT FROM TRIM(j.dosage_form)
              OR vs.ingredient IS DISTINCT FROM TRIM(j.ingredient)
              OR vs.strength IS DISTINCT FROM TRIM(j.strength)
              OR vs.units IS DISTINCT FROM TRIM(j.units)
              OR vs.is_recycled IS DISTINCT FROM TRIM(j.is_recycled)
              OR vs.from_date IS DISTINCT FROM TRIM(j.from_date)::DATE
              OR vs.thru_date IS DISTINCT FROM TRIM(j.thru_date)::DATE
              OR vs.attribute_type IS DISTINCT FROM TRIM(j.attribute_type)
              OR vs.attribute_value IS DISTINCT FROM TRIM(j.attribute_value)
          );

-- soft deletes, set thru date
UPDATE analytics.ref.med_adherence_value_sets vs
SET
    thru_date = '2024-06-12'::DATE, updated_at = now()
WHERE
      thru_date > '2024-06-12'::DATE
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      junk.s_pqa_meas_yr_2023_value_sets_meds20240612 j
                  WHERE
                        j.value_set_id = vs.value_set_id
                    AND j.value_set_item = vs.value_set_item
                    AND j.value_set_subgroup = vs.value_set_subgroup
                    AND j.code = vs.code );

-- inserts
INSERT
INTO
    analytics.ref.med_adherence_value_sets (value_set_id, value_set_subgroup, value_set_item, code_type, code,
                                            description, route, dosage_form, ingredient, strength, units, is_recycled,
                                            from_date, thru_date, attribute_type, attribute_value)
SELECT
    trim(value_set_id)
  , trim(value_set_subgroup)
  , trim(value_set_item)
  , trim(code_type)
  , trim(code)
  , trim(description)
  , trim(route)
  , trim(dosage_form)
  , trim(ingredient)
  , trim(strength)
  , trim(units)
  , trim(is_recycled)
  , trim(from_date)::date
  , trim(thru_date)::date
  , trim(attribute_type)
  , trim(attribute_value)
FROM
    junk.s_pqa_meas_yr_2023_value_sets_meds20240612 j
WHERE
    NOT EXISTS( SELECT
                    1
                FROM
                    analytics.ref.med_adherence_value_sets vs
                WHERE
                      j.value_set_id = vs.value_set_id
                  AND j.value_set_item = vs.value_set_item
                  AND j.value_set_subgroup = vs.value_set_subgroup
                  AND j.code = vs.code )
;

