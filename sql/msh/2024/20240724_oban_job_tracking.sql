CREATE TABLE audit.oban_jobs (
    id                  BIGINT,
    source              TEXT,
    queue               TEXT,
    worker              TEXT,
    args                jsonb,
    errors              jsonb[],
    attempt             INTEGER,
    max_attempts        INTEGER,
    inserted_at         TIMESTAMP,
    scheduled_at        TIMESTAMP,
    attempted_at        TIMESTAMP,
    completed_at        TIMESTAMP,
    attempted_by        TEXT[],
    discarded_at        TIMESTAMP,
    priority            INTEGER,
    tags                TEXT,
    meta                jsonb,
    cancelled_at        TIMESTAMP,
    state               TEXT,
    archived_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    archived_updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, source)
);
SELECT * FROM audit.oban_jobs;

CREATE PROCEDURE audit.oban_jobs_archive()
    LANGUAGE plpgsql
AS
$$
DECLARE
    message_text      TEXT;
    exception_detail  TEXT;
    exception_hint    TEXT;
    stack             TEXT;
    exception_context TEXT;
    error_text        TEXT;
BEGIN


    INSERT
    INTO
        audit.oban_jobs AS o (id, source, queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at,
                              attempted_at, completed_at, attempted_by, discarded_at, priority, tags, meta,
                              cancelled_at, state)

    SELECT
        id
      , 'oban.oban_jobs' source
      , queue
      , worker
      , args
      , errors
      , attempt
      , max_attempts
      , inserted_at
      , scheduled_at
      , attempted_at
      , completed_at
      , attempted_by
      , discarded_at
      , priority
      , tags
      , meta
      , cancelled_at
      , state
    FROM
        oban.oban_jobs j
    ORDER BY id
    ON CONFLICT (id, source) DO UPDATE
        SET
            queue        = excluded.queue
          , worker       = excluded.worker
          , args         = excluded.args
          , errors       = excluded.errors
          , attempt      = excluded.attempt
          , max_attempts = excluded.max_attempts
          , inserted_at  = excluded.inserted_at
          , scheduled_at = excluded.scheduled_at
          , attempted_at = excluded.attempted_at
          , completed_at = excluded.completed_at
          , attempted_by = excluded.attempted_by
          , discarded_at = excluded.discarded_at
          , priority     = excluded.priority
          , tags         = excluded.tags
          , meta         = excluded.meta
          , cancelled_at = excluded.cancelled_at
          , state        = excluded.state
    WHERE
         o.queue IS DISTINCT FROM excluded.queue
      OR o.worker IS DISTINCT FROM excluded.worker
      OR o.args IS DISTINCT FROM excluded.args
      OR o.errors IS DISTINCT FROM excluded.errors
      OR o.attempt IS DISTINCT FROM excluded.attempt
      OR o.max_attempts IS DISTINCT FROM excluded.max_attempts
      OR o.inserted_at IS DISTINCT FROM excluded.inserted_at
      OR o.scheduled_at IS DISTINCT FROM excluded.scheduled_at
      OR o.attempted_at IS DISTINCT FROM excluded.attempted_at
      OR o.completed_at IS DISTINCT FROM excluded.completed_at
      OR o.attempted_by IS DISTINCT FROM excluded.attempted_by
      OR o.discarded_at IS DISTINCT FROM excluded.discarded_at
      OR o.priority IS DISTINCT FROM excluded.priority
      OR o.tags IS DISTINCT FROM excluded.tags
      OR o.meta IS DISTINCT FROM excluded.meta
      OR o.cancelled_at IS DISTINCT FROM excluded.cancelled_at
      OR o.state IS DISTINCT FROM excluded.state;


    INSERT
    INTO
        audit.oban_jobs AS o (id, source, queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at,
                              attempted_at, completed_at, attempted_by, discarded_at, priority, tags, meta,
                              cancelled_at,
                              state)

    SELECT
        id
      , 'public.oban_jobs' source
      , queue
      , worker
      , args
      , errors
      , attempt
      , max_attempts
      , inserted_at
      , scheduled_at
      , attempted_at
      , completed_at
      , attempted_by
      , discarded_at
      , priority
      , tags
      , meta
      , cancelled_at
      , state
    FROM
        fdw_member_doc.oban_jobs
    ORDER BY
        id
    ON CONFLICT (id, source) DO UPDATE
        SET
            queue        = excluded.queue
          , worker       = excluded.worker
          , args         = excluded.args
          , errors       = excluded.errors
          , attempt      = excluded.attempt
          , max_attempts = excluded.max_attempts
          , inserted_at  = excluded.inserted_at
          , scheduled_at = excluded.scheduled_at
          , attempted_at = excluded.attempted_at
          , completed_at = excluded.completed_at
          , attempted_by = excluded.attempted_by
          , discarded_at = excluded.discarded_at
          , priority     = excluded.priority
          , tags         = excluded.tags
          , meta         = excluded.meta
          , cancelled_at = excluded.cancelled_at
          , state        = excluded.state
    WHERE
         o.queue IS DISTINCT FROM excluded.queue
      OR o.worker IS DISTINCT FROM excluded.worker
      OR o.args IS DISTINCT FROM excluded.args
      OR o.errors IS DISTINCT FROM excluded.errors
      OR o.attempt IS DISTINCT FROM excluded.attempt
      OR o.max_attempts IS DISTINCT FROM excluded.max_attempts
      OR o.inserted_at IS DISTINCT FROM excluded.inserted_at
      OR o.scheduled_at IS DISTINCT FROM excluded.scheduled_at
      OR o.attempted_at IS DISTINCT FROM excluded.attempted_at
      OR o.completed_at IS DISTINCT FROM excluded.completed_at
      OR o.attempted_by IS DISTINCT FROM excluded.attempted_by
      OR o.discarded_at IS DISTINCT FROM excluded.discarded_at
      OR o.priority IS DISTINCT FROM excluded.priority
      OR o.tags IS DISTINCT FROM excluded.tags
      OR o.meta IS DISTINCT FROM excluded.meta
      OR o.cancelled_at IS DISTINCT FROM excluded.cancelled_at
      OR o.state IS DISTINCT FROM excluded.state;

-- EXCEPTION
--     WHEN OTHERS THEN
--         GET DIAGNOSTICS stack = PG_CONTEXT;
--         GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT, exception_detail = PG_EXCEPTION_DETAIL, exception_hint = PG_EXCEPTION_HINT, exception_context = PG_EXCEPTION_CONTEXT;
--         ROLLBACK;
--         error_text = '(1) Message_Text( ' || COALESCE(message_text, '') || E' ) \nstack (' ||
--                      COALESCE(exception_context, '') || ' ) ';
--         PERFORM *
--         FROM
--             audit.fn_create_sms_alerts(ARRAY ['de-analytics-etl','ae-analytics-etl'], 'audit.run_oban_jobs_audit',
--                                        error_text::TEXT);
--         COMMIT;
--         RAISE EXCEPTION 'audit.run_oban_jobs_audit', error_text;

END;
$$;



SELECT *
FROM
    analytics.oban.oban_jobs;
call audit.oban_jobs_archive();