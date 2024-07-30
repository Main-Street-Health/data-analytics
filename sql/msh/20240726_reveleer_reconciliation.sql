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

SELECT *
FROM
    _sent_but_not_returned;

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

SELECT * from fdw_file_router.ftp_servers where name ~* 'bcbsar';