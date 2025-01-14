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


-- select * from deus_dpc_stage_monthly_new_fn();
-- UPDATE oban.oban_jobs
-- SET
--     max_attempts = max_attempts + 1,
--     scheduled_at = NOW(),
--     state = 'available',
--     discarded_at = NULL
-- WHERE
--     id = 204503685;


SELECT -- errors[oban_jobs.attempt], *
state, *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_worker'
  AND id >= 202932875
-- 188891932
ORDER BY
    id desc;


-- SELECT * FROM dpoc_practitioner_group_patients where npi = '1518758855' ;
-- update dpoc_practitioner_group_patients  set npi = '1518578855' where npi = '1518758855';
-- update dpoc_practitioner_group_patients  set npi = '1720461916' where npi = '1720431916';
-- update dpoc_practitioners  set npi = '1518578855' where npi = '1518758855';
-- update dpoc_practitioners  set npi = '1720461916' where npi = '1720431916';
--

    update
    msh_physicians set npi = '1417586819', updated_at = now() WHERE npi = '147586819';

-- update oban.oban_jobs set state = 'discarded' where id = 194677239

SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2025-01-01'
GROUP BY
    1;
SELECT * FROM oban.oban_queues where name = 'dpoc_bulk_export_worker';
UPDATE oban.oban_queues
SET
    opts = '{
      "paused": null,
      "rate_limit": null,
      "local_limit": 2,
      "global_limit": {
        "allowed": 2,
        "tracked": {},
        "partition": null
      },
      "retry_backoff": 1000,
      "retry_attempts": 5
    }'::jsonb
, updated_at = now()
where name = 'dpoc_bulk_export_worker';




SELECT
   completed_at - oban_jobs.attempted_at, *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2024-12-09'
and state = 'discarded'
order BY
    id desc;

    update oban.oban_jobs
    set state = 'available', discarded_at = null, max_attempts = max_attempts + 1, scheduled_at = now()
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2024-12-09'
and state = 'discarded';

SELECT npi
FROM
    dpoc_practitioners WHERE is_registered;

SELECT
    *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
--   and state = 'discarded'
  AND inserted_at >= '2024-12-08'
order by id
    ;

SELECT *
FROM
    dpoc_bulk_export_jobs ORDER BY id desc;


SELECT is_active, is_registered, count(*)
FROM
    dpoc_practitioners
GROUP BY is_active, is_registered
-- where is_active
--   and is_registered

;


-- select * from deus_dpc_stage_monthly_incremental_fn();
SELECT *
FROM
    dpoc_practitioners WHERE is_active;



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

------------------------------------------------------------------------------------------------------------------------
/* 2FE4AJ8FR74 */
------------------------------------------------------------------------------------------------------------------------
SELECT dp.source_id, dp.dpoc_id, dp.mbi, dp.mbis, dp.is_registered, dp.bene_id, dp.is_active, p.full_name, p.dob, p.status, p.substatus
FROM
    dpoc_patients dp
join fdw_member_doc.patients p on p.id = dp.source_id
WHERE
    '2FE4AJ8FR74' = ANY (mbis);

delete from dpoc_patients where source_id = 1532326
update dpoc_patients set source_id = 1532326 where source_id = 336086
SELECT is_dpc
FROM
    fdw_member_doc.supreme_pizza where patient_id = 1532326;

UPDATE dpoc_patients
SET
    is_active = FALSE
WHERE
      '2FE4AJ8FR74' = ANY (mbis)
  AND NOT is_registered
    ;

DELETE
FROM

WHERE;
select patient_id, mbi from fdw_member_doc.patient_mbi_and_medicare_dates where patient_id in (1532326,336086);
select patient_id, mbi from gmm.global_members where patient_id in (1532326,336086);
4FK5VY4FK94
| source\_id |
| :--- |
| 1532326 |
| 336086 |


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
