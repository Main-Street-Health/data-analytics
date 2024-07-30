-- need to
-- 1. de dupe base on last_filled_date. keep the latest version of each record
-- 2. update unique keys to new version
-- 3. rebuild with existing machinery

-- 1. de dupe base on last_filled_date. keep the latest version of each record
DROP TABLE IF EXISTS _dupes_to_clean_up;
CREATE TEMP TABLE _dupes_to_clean_up AS
SELECT
    new_unique_key
  , COUNT(*) n
  , MAX(id)  most_recent_record_id
  , MIN(id)  older_record_id
FROM
    ( SELECT
          id
        , CONCAT_WS('::', patient_id::TEXT, ndc::TEXT, last_filled_date::TEXT, days_supply::TEXT) new_unique_key
      FROM
          prd.patient_medications ) x
GROUP BY
    1
HAVING
    COUNT(*) > 1
    ;
-- needs to be 0
SELECT count(*) FROM _dupes_to_clean_up WHERE n > 2;

-- 44502
SELECT count(*) FROM _dupes_to_clean_up;

CREATE TABLE bk_up.patient_medication_dupes_20231107 AS
SELECT
    pm.*
FROM
    _dupes_to_clean_up d
    JOIN prd.patient_medications pm ON pm.id = d.older_record_id
;
BEGIN;
DELETE
FROM
    prd.patient_medications pm
    USING bk_up.patient_medication_dupes_20231107 d
WHERE
    d.id = pm.id
;
END;

-- need to update existing unique_keys
-- DROP TABLE IF EXISTS _to_upd;
-- CREATE TEMP TABLE _to_upd AS
-- drop TABLE if exists bk_up.patient_medication_new_unique_keys_20231107;
CREATE TABLE bk_up.patient_medication_new_unique_keys_20231107 AS
SELECT *
FROM
    ( SELECT
          CONCAT_WS('::', patient_id::TEXT, ndc::TEXT, last_filled_date::TEXT, days_supply::TEXT, 'new') new_unique_key
        , m.unique_key                                                                                   old_unique_key
        , m.id                                                                                           patient_medication_id
      FROM
          prd.patient_medications m ) x
WHERE
    x.old_unique_key != x.new_unique_key
ORDER BY
    x.new_unique_key
;

SELECT * FROM prd.patient_medications WHERE unique_key = '199067::00527328046::2022-06-06::30';
SELECT * FROM prd.patient_medications WHERE patient_id = 199067 ORDER BY patient_id, ndc, last_filled_date, sold_date;
SELECT * FROM bk_up.patient_medication_new_unique_keys_20231107 WHERE new_unique_key = '199067::00527328046::2022-06-06::30';

SELECT count(*), count(distinct new_unique_key), count(distinct old_unique_key) FROM bk_up.patient_medication_new_unique_keys_20231107;
begin;
UPDATE prd.patient_medications pm
SET
    unique_key = u.new_unique_key || '::new'
  , updated_at = NOW()
FROM
    bk_up.patient_medication_new_unique_keys_20231107 u
WHERE
    u.patient_medication_id = pm.id;

SELECT
    unique_key
  , REGEXP_REPLACE(unique_key, '\:\:new', '', 'g') nn
FROM
    prd.patient_medications
WHERE
    RIGHT(unique_key, 5) = '::new';

UPDATE prd.patient_medications pm
SET
    unique_key = REGEXP_REPLACE(unique_key, '\:\:new', '', 'g')
  , updated_at = NOW()
WHERE
        RIGHT(unique_key, 5) = '::new';
end;


end;
ROLLBACK ;
-- regen synth periods
-- pickup at STEP #2 in sp_med_adherence_load_surescripts_to_coop
  -- need to pass in updated patient ids and -1 for sure_scripts_med_history_id (unused but stored on the batch
SELECT array_agg(patient_id) patient_ids FROM bk_up.patient_medication_dupes_20231107;
    ;
-- ignore step 3.6, 4, 5
-- VERIFY before 6 (send to coop)

-- create table bk_up.patient_medication_dupes_cleanup_created_tasks as
-- SELECT id patient_task_id
-- FROM
--     fdw_member_doc.patient_tasks
-- WHERE
--     task_type ~* 'adherence'
-- and inserted_at > now() - '30 minutes'::interval

SELECT array_agg(patient_id) patient_ids FROM bk_up.patient_medication_dupes_20231107;

