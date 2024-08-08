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



SELECT *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_worker'
  AND id >= 163981570
ORDER BY
    id ;

SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2024-08-01'
GROUP BY
    1;

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