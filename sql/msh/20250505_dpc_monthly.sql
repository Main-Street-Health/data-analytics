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
 CALL public.sp_dpc_refresh_dpc_patients();
-- select * from deus_dpc_stage_monthly_incremental_fn();

-- select * from deus_dpc_stage_monthly_new_fn();
-- UPDATE oban.oban_jobs
-- SET
--     max_attempts = max_attempts + 1,
--     scheduled_at = NOW(),
--     state = 'available',
--     discarded_at = NULL
-- WHERE
--     id = 231128342;


SELECT
    errors[attempt]
  , state
  , *
-- state, *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_worker'
  AND id >= 231128342
-- 188891932
ORDER BY
    id DESC;

SELECT *
FROM
    dpoc_practitioners where npi = '1003594318';

SELECT *
FROM
    dpoc_practitioners
WHERE
      is_active
  AND is_registered
    ;


select * from deus_dpc_stage_monthly_incremental_fn();

SELECT * FROM msh_physicians where npi = '1902479754' ;
-- update dpoc_practitioner_group_patients  set npi = '1518578855' where npi = '1518758855';
-- update dpoc_practitioner_group_patients  set npi = '1720461916' where npi = '1720431916';
-- update dpoc_practitioners  set npi = '1518578855' where npi = '1518758855';
-- update dpoc_practitioners  set npi = '1720461916' where npi = '1720431916';
--

update msh_physicians set npi = '1902473754', updated_at = now() WHERE npi = '1902479754';
update msh_physicians set npi = '1417586819', updated_at = now() WHERE npi = '147586819';


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
SELECT *
FROM
    dpoc_practitioner_group_patients ;
SELECT *
FROM
    dpoc_practitioners dp
WHERE
      is_registered
  AND is_active
and exists (
    select 1 from dpoc_bulk_export_jobs j
             where j.npi = dp.npi
             and j.inserted_at > now() - '1 day'::interval
)
id
783,598,548,741,477,136,242,728,223,479,743,466,331,672,809,379,704,373,711,405,715,212,401,269,229,555,799,605,224,636,183,496,343,366,214,154,810,629,558,137,503,675,662,204,578,185,580,769,650,750,733,623,793,772,504,619,287,572,707,178,290,621,585,536,193,602,191,716,335,575,376

;
    ;


