SELECT * FROM junk.pqa_measures_2024;
SELECT * FROM junk.pqa_value_set_meds_2024;
SELECT * FROM junk.pq;

------------------------------------------------------------------------------------------------------------------------
/* measure level */
------------------------------------------------------------------------------------------------------------------------
-- missing one
SELECT
    j.measure_id
  , j.value_set_id
  , j.measure_name
  , r.measure_name
  , j.measure_version
  , r.measure_version
  , j.table_id
  , r.table_id
  , j.table_name
  , r.table_name
  , j.is_med
  , r.is_med
  , j.is_exclusion
  , r.is_exclusion
FROM
    junk.pqa_measures_2024 j
    LEFT JOIN analytics.ref.med_adherence_measures r ON j.measure_id = r.measure_id
        AND r.value_set_id = j.value_set_id
and r.measure_version = '2023'
where r.id ISNULL
ORDER BY
    j.measure_id, j.value_set_id
;
-- same but insert all to get 2024 values

INSERT
INTO
    analytics.ref.med_adherence_measures (measure_id, measure_name, measure_version, table_id, table_name, value_set_id, is_med, is_exclusion)

SELECT
    measure_id
  , measure_name
  , measure_version
  , table_id
  , table_name
  , value_set_id
  , is_med
  , is_exclusion
FROM
    junk.pqa_measures_2024 j
-- WHERE
--     NOT EXISTS( SELECT
--                     1
--                 FROM
--                     analytics.ref.med_adherence_measures r
--                 WHERE
--                       j.measure_id = r.measure_id
--                   AND r.value_set_id = j.value_set_id )
;

-- need to update synth generation and etl.sp_med_adherence_load_surescripts_to_coop to new measure version

-- total overlap
SELECT
    j.measure_id
  , j.value_set_id
  , j.measure_name
  , r.measure_name
  , j.measure_version
  , r.measure_version
  , j.table_id
  , r.table_id
  , j.table_name
  , r.table_name
  , j.is_med
  , r.is_med
  , j.is_exclusion
  , r.is_exclusion
FROM
        ref.med_adherence_measures r
    LEFT JOIN  junk.pqa_measures_2024 j ON j.measure_id = r.measure_id
        AND r.value_set_id = j.value_set_id
where j.measure_id in ('PDC-DR', 'PDC-RASA', 'PDC-STA', 'SUPD', 'SPC')
and j.value_set_id ISNULL
ORDER BY
    j.measure_id, j.value_set_id
;
------------------------------------------------------------------------------------------------------------------------
/* value set med level */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM ref.med_adherence_value_sets;
SELECT * FROM junk.pqa_value_set_meds_2024 ;

-- new, 25k?
SELECT
    COUNT(*)
FROM
    junk.pqa_value_set_meds_2024 j
WHERE
    NOT EXISTS( SELECT
                    1
                FROM
                    ref.med_adherence_value_sets vs
                WHERE
                      vs.value_set_id = j.value_set_id
                  AND vs.code = j.code )
;
-- add new
INSERT
INTO
    analytics.ref.med_adherence_value_sets (value_set_id, value_set_subgroup, value_set_item, code_type, code,
                                            description, route, dosage_form, ingredient, strength, units, is_recycled,
                                            from_date, thru_date, attribute_type, attribute_value)

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
  , from_date::date
  , coalesce(thru_date::date, '2099-12-31'::date)
  , attribute_type
  , attribute_value
FROM
    junk.pqa_value_set_meds_2024 j
WHERE
    NOT EXISTS( SELECT
                    1
                FROM
                    ref.med_adherence_value_sets vs
                WHERE
                      vs.value_set_id = j.value_set_id
                  AND vs.code = j.code )
;



-- retired, 13k
UPDATE
    ref.med_adherence_value_sets vs
SET
    thru_date = '2023-12-31', updated_at = NOW()
WHERE
      vs.thru_date >= NOW()
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      junk.pqa_value_set_meds_2024 j
                  WHERE
                        vs.value_set_id = j.value_set_id
                    AND vs.code = j.code )
;

SELECT *
FROM
    analytics.ref.notes order by id desc;

-- INSERT
-- INTO
--     analytics.ref.notes (author, note, sql, tables)
-- VALUES
--     ('Brendon', 'imported PQA med adherence value sets', 'SELECT * FROM junk.pqa_measures_2024;
-- SELECT * FROM junk.pqa_value_set_meds_2024;
--
-- ------------------------------------------------------------------------------------------------------------------------
-- /* measure level */
-- ------------------------------------------------------------------------------------------------------------------------
-- -- missing one
-- SELECT
--     j.measure_id
--   , j.value_set_id
--   , j.measure_name
--   , r.measure_name
--   , j.measure_version
--   , r.measure_version
--   , j.table_id
--   , r.table_id
--   , j.table_name
--   , r.table_name
--   , j.is_med
--   , r.is_med
--   , j.is_exclusion
--   , r.is_exclusion
-- FROM
--     junk.pqa_measures_2024 j
--     LEFT JOIN analytics.ref.med_adherence_measures r ON j.measure_id = r.measure_id
--         AND r.value_set_id = j.value_set_id
-- and r.measure_version = ''2023''
-- where r.id ISNULL
-- ORDER BY
--     j.measure_id, j.value_set_id
-- ;
-- -- same but insert all to get 2024 values
--
-- INSERT
-- INTO
--     analytics.ref.med_adherence_measures (measure_id, measure_name, measure_version, table_id, table_name, value_set_id, is_med, is_exclusion)
--
-- SELECT
--     measure_id
--   , measure_name
--   , measure_version
--   , table_id
--   , table_name
--   , value_set_id
--   , is_med
--   , is_exclusion
-- FROM
--     junk.pqa_measures_2024 j
-- -- WHERE
-- --     NOT EXISTS( SELECT
-- --                     1
-- --                 FROM
-- --                     analytics.ref.med_adherence_measures r
-- --                 WHERE
-- --                       j.measure_id = r.measure_id
-- --                   AND r.value_set_id = j.value_set_id )
-- ;
--
-- -- need to update synth generation and etl.sp_med_adherence_load_surescripts_to_coop to new measure version
--
-- -- total overlap
-- SELECT
--     j.measure_id
--   , j.value_set_id
--   , j.measure_name
--   , r.measure_name
--   , j.measure_version
--   , r.measure_version
--   , j.table_id
--   , r.table_id
--   , j.table_name
--   , r.table_name
--   , j.is_med
--   , r.is_med
--   , j.is_exclusion
--   , r.is_exclusion
-- FROM
--         ref.med_adherence_measures r
--     LEFT JOIN  junk.pqa_measures_2024 j ON j.measure_id = r.measure_id
--         AND r.value_set_id = j.value_set_id
-- where j.measure_id in (''PDC-DR'', ''PDC-RASA'', ''PDC-STA'', ''SUPD'', ''SPC'')
-- and j.value_set_id ISNULL
-- ORDER BY
--     j.measure_id, j.value_set_id
-- ;
-- ------------------------------------------------------------------------------------------------------------------------
-- /* value set med level */
-- ------------------------------------------------------------------------------------------------------------------------
-- SELECT * FROM ref.med_adherence_value_sets;
-- SELECT * FROM junk.pqa_value_set_meds_2024 ;
--
-- -- new, 25k?
-- SELECT
--     COUNT(*)
-- FROM
--     junk.pqa_value_set_meds_2024 j
-- WHERE
--     NOT EXISTS( SELECT
--                     1
--                 FROM
--                     ref.med_adherence_value_sets vs
--                 WHERE
--                       vs.value_set_id = j.value_set_id
--                   AND vs.code = j.code )
-- ;
-- -- add new
-- INSERT
-- INTO
--     analytics.ref.med_adherence_value_sets (value_set_id, value_set_subgroup, value_set_item, code_type, code,
--                                             description, route, dosage_form, ingredient, strength, units, is_recycled,
--                                             from_date, thru_date, attribute_type, attribute_value)
--
-- SELECT
--     value_set_id
--   , value_set_subgroup
--   , value_set_item
--   , code_type
--   , code
--   , description
--   , route
--   , dosage_form
--   , ingredient
--   , strength
--   , units
--   , is_recycled
--   , from_date::date
--   , coalesce(thru_date::date, ''2099-12-31''::date)
--   , attribute_type
--   , attribute_value
-- FROM
--     junk.pqa_value_set_meds_2024 j
-- WHERE
--     NOT EXISTS( SELECT
--                     1
--                 FROM
--                     ref.med_adherence_value_sets vs
--                 WHERE
--                       vs.value_set_id = j.value_set_id
--                   AND vs.code = j.code )
-- ;
--
--
--
-- -- retired, 13k
-- UPDATE
--     ref.med_adherence_value_sets vs
-- SET
--     thru_date = ''2023-12-31'', updated_at = NOW()
-- WHERE
--       vs.thru_date >= NOW()
--   AND NOT EXISTS( SELECT
--                       1
--                   FROM
--                       junk.pqa_value_set_meds_2024 j
--                   WHERE
--                         vs.value_set_id = j.value_set_id
--                     AND vs.code = j.code )
-- ;', '{ref.med_adherence_value_sets,ref.med_adherence_measures}');
--
--
--
--
