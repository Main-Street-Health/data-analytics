select * from junk.s_pqa_meas_yr_2024_value_sets_20250117
SELECT *
FROM
    junk.s_pqa_meas_yr_2024_value_sets_meds20250117;
------------------------------------------------------------------------------------------------------------------------
/*
 these tables are weird
 ref.med_adherence_measures  has a version on it ie 2024, 2025 etc
 ref.med_adherence_value_sets has a valid from_date and thru date

 previously we've inserted new rows for the new ref.med_adherence_measures but then updated the value sets

 should we add a version to the value set?
   - probably
   - its an extra join clause but makes it easy to be unique
 */
------------------------------------------------------------------------------------------------------------------------

SELECT
    measure_version
  , COUNT(*)
  , COUNT(DISTINCT value_set_id)
FROM
    analytics.ref.med_adherence_measures m
GROUP BY
    1
ORDER BY
    1;


SELECT value_set_id, value_set_item, count(*), count(distinct code)
FROM
    analytics.ref.med_adherence_value_sets
GROUP BY value_set_id, value_set_item
having count(*) <> count(distinct code)
;

SELECT *
FROM
    analytics.ref.med_adherence_measures m
join ref.med_adherence_value_sets vs on m.value_set_id = vs.value_set_id
;

------------------------------------------------------------------------------------------------------------------------
/* modify table to add in version */
------------------------------------------------------------------------------------------------------------------------
alter table ref.med_adherence_value_sets add measure_version TEXT;
------------------------------------------------------------------------------------------------------------------------
/* insert new version */
------------------------------------------------------------------------------------------------------------------------
INSERT
INTO
    analytics.ref.med_adherence_measures (measure_id, measure_name, measure_version, table_id, table_name, value_set_id,
                                          is_med, is_exclusion)
SELECT measure_id, measure_name, measure_version, table_id, table_name, value_set_id,
                                          is_med, is_exclusion
FROM
    junk.s_pqa_meas_yr_2024_value_sets_20250117;

INSERT
INTO
    analytics.ref.med_adherence_value_sets (value_set_id, value_set_subgroup, value_set_item, code_type, code,
                                            description, route, dosage_form, ingredient, strength, units, is_recycled,
                                            from_date, thru_date, attribute_type, attribute_value, measure_version)
SELECT
    value_set_id
  , value_set_subgroup
  , value_set_item
  , code_type
  , code
  , description
  , route
  , dosage_form
  , ingredient
  , strength
  , units
  , is_recycled
  , '1900-01-01'::date
  , '2099-12-31'::date
  , attribute_type
  , attribute_value
  , '2025' measure_version
FROM
    junk.s_pqa_meas_yr_2024_value_sets_meds20250117;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
call etl.sp_med_adherence_load_surescripts_to_coop(19867);

DROP TABLE IF EXISTS _mismatched_measure_keys;
CREATE TEMP TABLE _mismatched_measure_keys AS
SELECT pm.analytics_id, pm.measure_key old_key, mamm.coop_measure_key new_key
FROM
    fdw_member_doc.patient_medication_fills pm
    LEFT JOIN ref.med_adherence_measures m
    JOIN ref.med_adherence_measure_names mamm ON mamm.analytics_measure_id = m.measure_id
    JOIN ref.med_adherence_value_sets vs ON m.value_set_id = vs.value_set_id
         ON vs.code = pm.ndc
             AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
             AND m.is_med = 'Y'
             AND m.is_exclusion = 'N'
             AND m.measure_version = '2025'
             AND vs.measure_version = '2025'
where
pm.last_filled_date  >= '2025-01-01'::date
AND pm.measure_key is distinct from mamm.coop_measure_key
;

SELECT
    old_key
  , new_key
  , COUNT(*)
FROM
    _mismatched_measure_keys
GROUP BY
    1, 2;

INSERT
INTO
    fdw_member_doc_stage.pat_med_fills_upd_20241007 (analytics_id, new_measure_key)
    select analytics_id, new_key
from    _mismatched_measure_keys m
;
-- back to member doc

UPDATE fdw_member_doc.patient_medication_fills f
SET
    measure_key = m.new_measure_key, updated_at = NOW()
FROM
    fdw_member_doc_stage.pat_med_fills_upd_20241007 m
WHERE
    m.analytics_id = f.analytics_id
;