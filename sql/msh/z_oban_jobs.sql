INSERT
INTO
    public.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at, attempted_at,
                      completed_at, attempted_by, discarded_at, priority, tags, meta, cancelled_at, state)
VALUES
    ('md_portals_rosters', 'MD.MDPortals.MDPortalsRosterWorker', '{
      "type": "full",
      "conf_name": "mdp_api_msh"
    }', '{}', 0, 5, NOW(), NOW(), NULL,
     NULL, NULL,
     NULL, 0, '{}', '{}', NULL, 'available');


INSERT
INTO
    public.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at, attempted_at,
                      completed_at, attempted_by, discarded_at, priority, tags, meta, cancelled_at, state)
VALUES
    ('md_portals_nlp_issues', 'MD.MDPortals.MDPortalsNLPIssuesWorker', '{}', '{}', 1, 1,
     NOW(), NOW(), NULL,
     NULL, NULL,
     NULL, 0, '{}', NULL, NULL, 'available')
RETURNING id;


SELECT *
FROM
    oban.oban_jobs WHERE queue = 'dpoc_worker' order by id desc;

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


INSERT
INTO
    oban.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at, attempted_at,
                    completed_at, attempted_by, discarded_at, priority, tags, meta, cancelled_at, state)
VALUES
    ('dpoc_worker', 'Deus.DPOC.DPOCWorker', '{
      "type": "spin_cycle"
    }', '{}', 0, 5, now(), now(), NULL, NULL, NULL, NULL, 0, '{}', null, NULL, 'available')
returning *
;

SELECT
    *
--     state
--   , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2024-01-01'
and id = 98962119
order by id
-- GROUP BY 1
    ;


SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2024-01-29'
GROUP BY
    1;

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
--   AND inserted_at >= '2024-01-01'
--   and id = 96420256
-- and state in ('retryable', 'discarded')
    ;
SELECT *
FROM
    quality_measures WHERe name ~* 'adhere' and id != 15;


INSERT
INTO
    oban.oban_jobs (queue, worker, args, max_attempts, inserted_at, scheduled_at, state)
VALUES
    ('sure_scripts', 'Deus.SureScripts.SureScriptsWorker', '{
      "action": "generate_panel"
    }', 1, NOW(), NOW(), 'available')
returning *
;


UPDATE public.oban_jobs
SET
    max_attempts = max_attempts + 1,
    scheduled_at = NOW(),
    discarded_at = NULL,
    state = 'available'
WHERE
    id = 2784938;



SELECT *
FROM
    oban_jobs
WHERE
    id = 2784938;

