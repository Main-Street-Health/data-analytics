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

select * from raw.milliman_pro_20240802;
select * from raw.milliman_ret_20240802;

SELECT *
FROM
        analytics.junk.chases_sent_not_in_reveleer_20240807 j
join junk.reveleer_life_of_chase_20240726 j2 on j2.clientchasekey = j.reveleer_chase_id
;
SELECT
--     j.reveleer_chase_id
--   , COUNT(*)
--   , COUNT(DISTINCT rcfd.id)
rcfd.*
FROM
    analytics.junk.chases_sent_not_in_reveleer_20240807 j
    JOIN reveleer_chase_file_details rcfd ON j.reveleer_chase_id = rcfd.reveleer_chase_id
;
-- GROUP BY
--     1
-- HAVING
--     COUNT(DISTINCT rcfd.id) < COUNT(*);

-- ORDER BY
--     j.reveleer_chase_id, rcfd.id
;
SELECT
    reveleer_chase_id
  , COUNT(*)
FROM
    analytics.junk.chases_sent_not_in_reveleer_20240807 j
GROUP BY
    1
HAVING
    COUNT(*) > 1;

SELECT count(reveleer_chase_id), count(distinct reveleer_chase_id) FROM analytics.junk.chases_sent_not_in_reveleer_20240807 j



------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM reveleer_chases WHERE id in ( 24036850, 23385169 );
SELECT *
FROM
    reveleer_chases rc
join reveleer_compliance_file_details rcfd ON rc.id = rcfd.reveleer_chase_id
WHERE
    external_chase_id IN ('24036850', '23385169');

call sp_reveleer_data_stager();
SELECT * FROM reveleer_chases;
SELECT * FROM reveleer_chase_file_details;
SELECT * FROM reveleer_attribute_file_details;
SELECT * FROM reveleer_compliance_file_details;
SELECT * FROM reveleer_files;

