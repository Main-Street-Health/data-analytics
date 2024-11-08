SELECT *
FROM
    pg_stat_activity;
 /*
INSERT
INTO
    oban.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at, attempted_at,
                    completed_at, attempted_by, discarded_at, priority, tags, meta, cancelled_at, state)
VALUES
    ('dpoc_worker', 'Deus.DPOC.DPOCWorker', '{
      "type": "incremental"
    }', '{}', 0, 1, now(), now(), NULL, NULL, NULL, NULL, 0, '{}', null, NULL, 'available')
returning *
;
 */

select * from deus_dpc_stage_monthly_incremental_fn();
-- select * from deus_dpc_stage_monthly_new_fn();
-- UPDATE oban.oban_jobs
-- SET
--     max_attempts = max_attempts + 1,
--     scheduled_at = NOW(),
--     state = 'available',
--     discarded_at = NULL
-- WHERE
--     id = 187326528;


SELECT -- errors[oban_jobs.attempt], *
*
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_worker'
  AND id >= 186796340
ORDER BY
    id ;


-- 2024-11-08T04:13:12.909Z
-- 04:13:12.909 [info] 91072 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T05:16:24.463Z
-- 05:16:24.462 [info] 91028 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T06:19:12.052Z
-- 06:19:12.051 [info] 91025 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T07:24:20.262Z
-- 07:24:20.261 [info] 91025 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T08:27:28.851Z
-- 08:27:28.850 [info] 91084 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T09:30:07.879Z
-- 09:30:07.879 [info] 91080 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T10:33:47.695Z
-- 10:33:47.694 [info] 91080 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T11:37:36.984Z
-- 11:37:36.983 [info] 91079 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T12:40:11.048Z
-- 12:40:11.048 [info] 91081 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T13:42:59.508Z
-- 13:42:59.508 [info] 91075 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868
-- 2024-11-08T14:45:53.270Z
-- 14:45:53.270 [info] 91055 were excluded
-- msh-deus/msh-deus/4dd709787c2f41c993f9599c3f3ac868


   -- hard break out of incremental runs stuck at around 91055

-- update oban.oban_jobs set state = 'discarded', discarded_at = now() where id = 187645225;



INSERT
INTO
    oban.oban_jobs (queue, worker, args, max_attempts, inserted_at, scheduled_at, state)
VALUES
    ('dpoc_worker', 'Deus.DPOC.DPOCWorker', '{
      "type": "new"
    }', 5, NOW(), NOW(), 'available')
returning id
;


select * from oban.oban_jobs where id = 187650126;

SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2024-11-01'
GROUP BY
    1;
SELECT
    *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  and state = 'discarded'
  AND inserted_at >= '2024-11-01'
    ;

------------------------------------------------------------------------------------------------------------------------
/* bump */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM dpoc_patients where source_id = 1514564;
SELECT * FROM dpoc_patients where '9A51XK5CA94' = any(mbis);
-- looks like a dupe
SELECT *
FROM
    fdw_member_doc.patients
WHERE
    id IN ( 1514564, 322793);
-- made one inactive
UPDATE public.dpoc_patients SET is_active = false::boolean WHERE id = 2228367::bigint;
------------------------------------------------------------------------------------------------------------------------
/* bump */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    dpoc_patients
WHERE
    '4DQ8JM8AJ47' = ANY (mbis);

SELECT *
FROM
    fdw_member_doc.patients
WHERE
    id IN (1529457, 145427);

UPDATE dpoc_patients
SET
    is_active = FALSE
WHERE
      '4DQ8JM8AJ47' = ANY (mbis)
  AND NOT is_registered
    ;
------------------------------------------------------------------------------------------------------------------------
/* 2FE4AJ8FR74 */
------------------------------------------------------------------------------------------------------------------------
SELECT dp.source_id, dp.dpoc_id, dp.mbi, dp.is_registered, dp.is_active, p.full_name, p.dob, p.status, p.substatus
FROM
    dpoc_patients dp
join fdw_member_doc.patients p on p.id = dp.source_id
WHERE
    '2FE4AJ8FR74' = ANY (mbis);


UPDATE dpoc_patients
SET
    is_active = FALSE
WHERE
      '2FE4AJ8FR74' = ANY (mbis)
  AND NOT is_registered
    ;




SELECT DISTINCT
    p.*
FROM
    fdw_member_doc.msh_physicians p
WHERE
    LENGTH(p.npi::TEXT) > 10
;

SELECT *
FROM
    dpoc_bulk_export_jobs WHERE inserted_at::date = now()::date;


-- UPDATE oban.oban_jobs
-- SET
--     max_attempts = max_attempts + 1, state = 'available', discarded_at = NULL, scheduled_at = NOW()
-- WHERE
--     id = 178869389;

SELECT unnest(errors) e, *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_worker'
  AND id = 178869389
ORDER BY
    id ;



WITH
    job_ouput_ids AS ( SELECT
                           (args -> 'dpoc_bulk_export_job_output_id')::BIGINT dpoc_bulk_export_job_output_id
                       FROM
                           oban.oban_jobs
                       WHERE
                             queue = 'dpoc_bulk_export_worker'
                         AND state = 'discarded'
                         AND inserted_at >= '2024-08-01' )
SELECT *
FROM
    job_ouput_ids joi
    JOIN dpoc_bulk_export_job_outputs jo ON jo.id = joi.dpoc_bulk_export_job_output_id
where not jo.is_processed
;


    ;


SELECT *
FROM
    dpoc_patients where '5F15VU2WH56' = any(mbis);

SELECT * FROM dpoc_patients where '1Y50YH3WU38' = any(mbis);
SELECT *
FROM
    fdw_member_doc.patients p
where p.id in ( 926682, 971228 )
;
SELECT *
FROM
    fdw_member_doc.supreme_pizza p
where p.patient_id in (804295, 877579 )
;


DROP TABLE IF EXISTS _dupes;
CREATE TEMP TABLE _dupes AS
SELECT dp_new.id new_id, dp_old.id old_id, dp_new.source_id new_source_id, dp_old.source_id old_source_id
FROM
    dpoc_patients dp_new
    JOIN dpoc_patients dp_old ON dp_new.source_id != dp_old.source_id
        AND dp_new.mbi = ANY (dp_old.mbis)
        AND dp_new.is_registered = FALSE
        AND dp_new.is_active = TRUE
        AND dp_old.is_registered = TRUE
        AND dp_old.is_active = FALSE
;

DELETE
FROM
    dpoc_patients
WHERE
    id IN ( SELECT new_id FROM _dupes );

UPDATE dpoc_patients p
SET
    source_id = d.new_source_id, is_active = TRUE
FROM
    _dupes d
WHERE
    d.old_id = p.id;

call sp_dpc_refresh_dpc_patients()