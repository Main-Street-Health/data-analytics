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
    }', '{}', 0, 5, now(), now(), NULL, NULL, NULL, NULL, 0, '{}', null, NULL, 'available')
returning *
;
 */
--  CALL public.sp_dpc_refresh_dpc_patients();
-- select * from deus_dpc_stage_monthly_incremental_fn();
-- select * from deus_dpc_stage_monthly_new_fn();

 /*
UPDATE oban.oban_jobs
SET
    max_attempts = max_attempts + 1,
    scheduled_at = NOW(),
    state = 'available',
    discarded_at = NULL
WHERE
    id = 239932259;
  */

-- SELECT state, * from oban_jobs where id = 13133868
--  INSERT
-- INTO
--     oban.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at, attempted_at,
--                     completed_at, attempted_by, discarded_at, priority, tags, meta, cancelled_at, state)
-- VALUES
--     ('dpoc_worker', 'Deus.DPOC.DPOCWorker', '{
--       "type": "new"
--     }', '{}', 0, 5, now(), now(), NULL, NULL, NULL, NULL, 0, '{}', null, NULL, 'available')
-- returning *;

SELECT
    errors[attempt]
  , state
  , *
-- state, *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_worker'
  AND id >= 239932259
-- 188891932
ORDER BY
    id DESC;

1376991927
SELECT * FROM dpoc_practitioners WHERE npi = '1053316596';
SELECT * FROM dpoc_practitioner_group_patients WHERE npi = '1053316596';
delete FROM dpoc_practitioner_group_patients WHERE npi = '1053316596';
delete FROM dpoc_practitioners WHERE npi = '1053316596';

SELECT
    mbi
  , npi
  , min_appt_date
  , max_appt_date
  , appt_count
  , min_claim_date
  , max_claim_date
  , claim_count
  , provider_rank
  , last_sent_to_dpc_at
  , last_returned_by_dpc_at
  , inserted_at
  , updated_at
FROM
    dpc_patient_physicians;




SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
--   AND inserted_at >= '2025-05-01'
and id > 239932259
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


-- fix one bad npi

UPDATE dpoc_practitioners
SET
    npi = '1275361230', updated_at = now()
WHERE
    npi = '1275631230';


UPDATE dpoc_practitioner_group_patients
SET
    npi = '1275361230', updated_at = now()
WHERE
    npi = '1275631230';

update
    fdw_member_doc.msh_physicians
SET
    npi = '1275361230', updated_at = now()
WHERE npi = '1275631230';
