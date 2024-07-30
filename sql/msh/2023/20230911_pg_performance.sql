
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
  , COUNT(*)                                                                              n
  , COUNT(*) FILTER ( WHERE status < 300 )                                                n_200s
  , (COUNT(*) FILTER ( WHERE status < 300 ) * 100. / NULLIF(COUNT(*), 0))::DECIMAL(16, 2) pct_success
  , (AVG(duration) / 1000000.) ::DECIMAL(16, 3)                                           mean_resp_time_s
  , (MIN(duration) / 1000000.) ::DECIMAL(16, 3)                                           min_resp_time_s
  , (MAX(duration) / 1000000.) ::DECIMAL(16, 3)                                           max_resp_time_s
FROM
    request_logs rl
WHERE
    rl.inserted_at > NOW() - '2 weeks'::INTERVAL
GROUP BY
    1, 2
ORDER BY
    3 DESC;

SELECT *
FROM
    _req r
where r.n_200s > 1000
ORDER BY r.mean_resp_time_s DESC
;

SELECT *
FROM
    pg_stat_statements WHERE queryid = -1591281282486597953;

SELECT
    COUNT(*)
FROM
    "public"."patients" AS p0
    INNER JOIN "supreme_pizza" AS s1 ON s1."patient_id" = p0."id"
    INNER JOIN "patient_programs" AS p2 ON (p2."patient_id" = p0."id") AND p2."end_date" IS NULL
    INNER JOIN "care_teams" AS c3 ON c3."id" = p0."care_team_id"
    LEFT OUTER JOIN "msh_patient_insurances" AS m4 ON (m4."patient_id" = p0."id") AND (m4."is_deleted" IS NULL)
    LEFT OUTER JOIN "msh_insurance_plans" AS m5 ON (m4."insurance_plan_id" = m5."id") AND NOT (m4."is_deleted")
    LEFT OUTER JOIN "patient_contacts" AS p6
                    ON ((p6."patient_id" = p0."id") AND (p6."is_primary" = $1)) AND (p6."relationship" = $2)
    LEFT OUTER JOIN "contacts" AS c7 ON c7."id" = p6."contact_id"
    LEFT OUTER JOIN "patient_referring_partners" AS p11 ON (p0."id" = p11."patient_id") AND (p11."primary" = $3)
    LEFT OUTER JOIN "referring_partners" AS r8 ON p11."referring_partner_id" = r8."id"
    LEFT OUTER JOIN ( SELECT DISTINCT ON (sa0."patient_id")
                          sa0."id"                             AS "id"
                        , sa0."start"                          AS "start"
                        , sa0."end"                            AS "end"
                        , sa0."notes"                          AS "notes"
                        , sa0."title"                          AS "title"
                        , sa0."created_at"                     AS "created_at"
                        , sa0."status"                         AS "status"
                        , sa0."recurrence_edited"              AS "recurrence_edited"
                        , sa0."completed_at"                   AS "completed_at"
                        , sa0."deleted_at"                     AS "deleted_at"
                        , sa0."location_identifier"            AS "location_identifier"
                        , sa0."location_name"                  AS "location_name"
                        , sa0."location_type"                  AS "location_type"
                        , sa0."all_day"                        AS "all_day"
                        , sa0."start_date"                     AS "start_date"
                        , sa0."end_date"                       AS "end_date"
                        , sa0."states"                         AS "states"
                        , sa0."reason_for_home_visit"          AS "reason_for_home_visit"
                        , sa0."checkin_priority_level"         AS "checkin_priority_level"
                        , sa0."source"                         AS "source"
                        , sa0."external_uuid"                  AS "external_uuid"
                        , sa0."needs_voluntary_attribution"    AS "needs_voluntary_attribution"
                        , sa0."is_patient_experience_eligible" AS "is_patient_experience_eligible"
                        , sa0."type_id"                        AS "type_id"
                        , sa0."recurrence_id"                  AS "recurrence_id"
                        , sa0."visit_type_id"                  AS "visit_type_id"
                        , sa0."patient_id"                     AS "patient_id"
                        , sa0."created_by_id"                  AS "created_by_id"
                        , sa0."completed_by_id"                AS "completed_by_id"
                        , sa0."completed_by_login_id"          AS "completed_by_login_id"
                        , sa0."deleted_by_id"                  AS "deleted_by_id"
                        , sa0."visit_contact_phone_id"         AS "visit_contact_phone_id"
                      FROM
                          "appointments" AS sa0
                      WHERE
                            (sa0."visit_type_id" = $6)
                        AND (sa0."status" = $7)
                        AND (sa0."deleted_at" IS NULL)
                        AND (sa0."start" > $4)
                      ORDER BY sa0."patient_id", sa0."start" ) AS s9 ON s9."patient_id" = p0."id"
    LEFT OUTER JOIN "msh_patient_unsuccessful_research" AS m10 ON m10."patient_id" = p0."id"
WHERE
      (NOT (p0."status" IN ($8, $9, $10)))
  AND (p2."program" = $11)
  AND (EXISTS(( SELECT
                    sc0."id"
                  , sc0."name"
                  , sc0."emergency_sms"
                  , sc0."microsoft_teams_channel_id"
                  , sc0."deleted"
                  , sc0."state_codes"
                  , sc0."cb_enable_autoassignment"
                  , sc0."group_id"
                  , sc0."emergency_user_id"
                  , sc0."created_by_id"
                  , sc0."modified_by_id"
                  , sc0."type"
                  , sc0."inserted_at"
                  , sc0."updated_at"
                FROM
                    "care_teams" AS sc0
                    INNER JOIN "care_team_members" AS sc1 ON sc1."care_team_id" = sc0."id"
                WHERE
                      (sc0."id" = c3."id")
                  AND (sc1."user_id" = $5) ) ))
  AND (m10."researched_at" IS NULL);
