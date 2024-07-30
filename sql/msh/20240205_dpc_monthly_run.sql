
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



SELECT *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_worker'
  AND id >= 147991552
ORDER BY
    id ;

-- update oban.oban_jobs set state = 'completed' where id = 147863924

-- select * from deus_dpc_stage_monthly_incremental_fn();
-- select * from deus_dpc_stage_monthly_new_fn();

SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2024-06-01'
GROUP BY
    1;
SELECT * FROM dpoc_patients where id = 1382742;
SELECT distinct dpoc_practitioner_id
FROM
    dpoc_bulk_export_jobs WHERE inserted_at::date = now()::date;
-- delete FROM dpoc_patients where '2JA8MY3NG34' = any(mbis);

SELECT
    *
--     state
--   , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2024-06-01'
and state = 'discarded'
and id > 133641287
order by id
-- GROUP BY 1
    ;
SELECT length('6KW1KC2AA996');

DROP TABLE IF EXISTS _mismatched_mbi;
CREATE TEMP TABLE _mismatched_mbi AS
SELECT dp.mbi, sp.patient_mbi, id, dpoc_id, is_registered, data, last_refreshed_at, address_line_1, address_line_2, bene_id, city, dob, dod, first_name, gender, last_name, state, zip, dp.inserted_at, dp.updated_at, source_id, dpoc_provider_org_id, dpoc_bulk_export_job_output_id, mbis, dpoc_last_updated, is_active, source,  patient_status, patient_substatus
FROM
    dpoc_patients dp
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = dp.source_id
WHERE
      dp.bene_id ISNULL
  AND dp.mbi != sp.patient_mbi
;
SELECT string_agg('"' || dpoc_id ||'"', ',')
-- SELECT array_agg(dpoc_id)
FROM
    _mismatched_mbi
WHERE
    is_active;
DELETE
FROM
    dpoc_patients dp
WHERE
    EXISTS( SELECT
                1
            FROM
                _mismatched_mbi m
            WHERE
                  is_active
              AND m.dpoc_id = dp.dpoc_id );

SELECT is_dpc, patient_mbi, patient_mbi = 'C8C65C20FR88'
FROM
    fdw_member_doc.supreme_pizza WHERE patient_id = 422693;

DROP TABLE IF EXISTS _mismatched_mbi;
CREATE TEMP TABLE _mismatched_mbi AS
SELECT dp.dpoc_id
FROM
    dpoc_patients dp
    left JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = dp.source_id
WHERE
      dp.bene_id ISNULL
  AND dp.mbi is DISTINCT FROM sp.patient_mbi
;
SELECT *
FROM
    _mismatched_mbi;

SELECT * FROM dpoc_practitioners WHERE is_registered;
SELECT * FROM dpoc_practitioners WHERE is_active;


-- INSERT
-- INTO
--     oban.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at, attempted_at,
--                     completed_at, attempted_by, discarded_at, priority, tags, meta, cancelled_at, state)
-- VALUES
--     ('dpoc_worker', 'Deus.DPOC.DPOCWorker', '{
--       "type": "spin_cycle"
--     }', '{}', 0, 5, now(), now(), NULL, NULL, NULL, NULL, 0, '{}', null, NULL, 'available')
-- returning *
-- ;



SELECT
--    errors[12]
*
FROM
-- update
    oban.oban_jobs
-- set max_attempts = attempt + 1, state = 'available', discarded_at = null, scheduled_at = now()
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2024-02-01'
--   and id = 96420256
-- and state in ('retryable', 'discarded')
    ;
select etl.dpc_load_prd_dpc_eligibility();
select etl.dpc_eligibility_to_coop_current_coverage();

------------------------------------------------------------------------------------------------------------------------
/* patient issues */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    dpoc_patients p
WHERE
    dpoc_id = '0e93c9c0-bb92-4e20-9f57-7c659c1b49b1'
--  last_name = 'Kehl'
--     '2JA8MY3NG34' = ANY (p.mbis)
;







