-- select    pg_terminate_backend(3754)
SELECT
    NOW() - query_start run_time
--      , pg_terminate_backend(pid)
  , query
  , application_name
  , usename
  , state
  , wait_event_type
  , wait_event
  , pid
FROM
    pg_stat_activity
WHERE
      state != 'idle'
  AND backend_type = 'client backend'
--   and query  ~* 'with T as'
--   and usename = 'abigail_asper'
-- and wait_event_type = 'Lock'
ORDER BY
    1 DESC
;
--blocked pids
SELECT *
FROM
    ( SELECT
          blocked_locks.pid         AS      blocked_pid
        , blocked_activity.usename  AS      blocked_user
        , blocking_locks.pid        AS      blocking_pid
        , blocking_activity.usename AS      blocking_user
        , blocked_activity.query    AS      blocked_statement
        , blocked_activity.application_name blocked_app
        , blocking_activity.query   AS      current_statement_in_blocking_process
      FROM
          pg_catalog.pg_locks blocked_locks
          JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
          JOIN pg_catalog.pg_locks blocking_locks
               ON blocking_locks.locktype = blocked_locks.locktype
                   AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
                   AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                   AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                   AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                   AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                   AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                   AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                   AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                   AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                   AND blocking_locks.pid != blocked_locks.pid
          JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
      WHERE
          NOT blocked_locks.GRANTED ) blbablba;



DROP TABLE IF EXISTS _req;
CREATE TEMP TABLE _req AS
SELECT
    path
  , method
  , COUNT(*)                                                                              n_reqs
  , COUNT(distinct user_id)                                                               nd_users
  , COUNT(*) FILTER ( WHERE status < 300 )                                                n_200s
  , (COUNT(*) FILTER ( WHERE status < 300 ) * 100. / NULLIF(COUNT(*), 0))::DECIMAL(16, 2) pct_success
  , (AVG(duration) / 1000000.) ::DECIMAL(16, 3)                                           mean_resp_time_s
  , (MIN(duration) / 1000000.) ::DECIMAL(16, 3)                                           min_resp_time_s
  , (MAX(duration) / 1000000.) ::DECIMAL(16, 3)                                           max_resp_time_s
  , (sum(duration) / 1000000.) ::DECIMAL(16, 3)                                           total_resp_time_s
FROM
    request_logs rl
WHERE
    rl.inserted_at > NOW() - '2 weeks'::INTERVAL
GROUP BY
    1, 2
ORDER BY
    3 DESC;

SELECT (total_resp_time_s / 60 / 60)::decimal(16,3), *
FROM
    _req
order by total_resp_time_s desc
;

SELECT *
FROM
    pg_stat_statements;

SELECT *
FROM
    ;