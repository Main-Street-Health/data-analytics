
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
, *
FROM
    pg_stat_activity
WHERE
    state != 'idle'
ORDER BY
    1 DESC
;
SELECT *
FROM
    pg_stat_statements
WHERE
    pg_stat_statements.queryid = -8723378216912078511;

SELECT
    c0."id"
  , c0."phone_number"
  , c0."type"
  , c0."nickname"
  , c0."extension"
  , c0."status"
  , c0."contact_id"
  , c0."created_by_id"
  , c0."updated_by_id"
  , c0."inserted_at"
  , c0."updated_at"
  , s1."call_count"
  , s2."can_start"
  , NOT (s3."contact_phone_id" IS NULL)
  , c0."contact_id"
FROM
    "contact_phones" AS c0
    LEFT OUTER JOIN ( SELECT
                          sc0."contact_phone_id" AS "contact_phone_id"
                        , COUNT(*)               AS "call_count"
                      FROM
                          "calls" AS sc0
                      GROUP BY sc0."contact_phone_id" ) AS s1 ON c0."id" = s1."contact_phone_id"
    INNER JOIN ( SELECT
                     sp0."patient_id"                        AS "patient_id"
                   , sp0."id"                                AS "patient_contact_id"
                   , sc2."id"                                AS "contact_phone_id"
                   , sc3."id"                                AS "conversation_id"
                   , ss4."phi_approved" OR (sc1."type" = $8) AS "can_start"
                 FROM
                     "patient_contacts" AS sp0
                     INNER JOIN "contacts" AS sc1 ON sc1."id" = sp0."contact_id"
                     INNER JOIN "contact_phones" AS sc2 ON (sc2."contact_id" = sc1."id") AND (sc2."status" = $1)
                     LEFT OUTER JOIN "conversations" AS sc3 ON ((sc3."patient_id" = sp0."patient_id") AND
                                                                (sc3."patient_contact_id" = sp0."id")) AND
                                                               (sc3."contact_phone_id" = sc2."id")
                     LEFT OUTER JOIN ( SELECT
                                           ssp0."id"                        AS "id"
                                         , ssp0."patient_id"                AS "patient_id"
                                         , ssp0."contact_id"                AS "contact_id"
                                         , ssp0."relationship"              AS "relationship"
                                         , ssp0."relationship_other"        AS "relationship_other"
                                         , ssp0."phi_approval_date"         AS "phi_approval_date"
                                         , ssp0."captured_as"               AS "captured_as"
                                         , ssp0."status"                    AS "status"
                                         , ssp0."notes"                     AS "notes"
                                         , ssp0."emergency_contact"         AS "emergency_contact"
                                         , ssp0."is_primary"                AS "is_primary"
                                         , ssp0."created_by_id"             AS "created_by_id"
                                         , ssp0."updated_by_id"             AS "updated_by_id"
                                         , ssp0."inserted_at"               AS "inserted_at"
                                         , ssp0."updated_at"                AS "updated_at"
                                         , NOT (ssp0."phi_approval_date" IS NULL) AND
                                           (ssp0."phi_approval_date" >= $2) AS "phi_approved"
                                       FROM
                                           "patient_contacts" AS ssp0 ) AS ss4 ON ss4."id" = sp0."id"
                 WHERE
                     (sp0."status" = $9) ) AS s2 ON (s2."patient_id" = $3) AND (s2."contact_phone_id" = c0."id")
    LEFT OUTER JOIN ( SELECT DISTINCT
                          sc0."contact_phone_id" AS "contact_phone_id"
                      FROM
                          "calls" AS sc0
                          INNER JOIN "call_dispositions" AS sc1 ON sc1."id" = sc0."disposition_id"
                      WHERE
                            (sc1."successful")
                        AND (sc0."started_at" > $4) ) AS s3 ON s3."contact_phone_id" = c0."id"
    LEFT OUTER JOIN "patient_contact_phones" AS p4 ON (p4."contact_phone_id" = c0."id") AND (p4."patient_id" = $5)
WHERE
    (c0."contact_id" = ANY ($6) AND (c0."status" = $7))
ORDER BY
    c0."contact_id", p4."primary" OR p4."primary_sms" DESC;


------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    pg_stat_statements where queryid =3551550469020419045;

SELECT
    c0."id"
  , c0."id"
  , c3."id"
  , c4."name"
  , c4."id"
  , e5."display_value"
  , c0."ended_at" IS NULL
  , c8."label"
  , c8."successful"
  , COALESCE(c3."phone_number", c0."to_phone_number")
  , u1."id"
  , u1."first_name"
  , u1."last_name"
  , c0."started_at"
  , c0."ended_at"
  , c0."dispositioned_at"
  , c0."call_type"
  , e7."display_value"
  , c0."purpose"
  , e6."display_value"
  , c0."call_uuid"
  , c0."notes"
  , c0."patient_id"
  , p2."first_name"
  , p2."last_name"
  , s9."task_ids"
FROM
    "calls" AS c0
    INNER JOIN "users" AS u1 ON u1."id" = c0."user_id"
    LEFT OUTER JOIN "public"."patients" AS p2 ON p2."id" = c0."patient_id"
    LEFT OUTER JOIN "contact_phones" AS c3 ON c3."id" = c0."contact_phone_id"
    LEFT OUTER JOIN "contact_phones" AS c10 ON c0."contact_phone_id" = c10."id"
    LEFT OUTER JOIN "contacts" AS c4 ON c10."contact_id" = c4."id"
    INNER JOIN "enums" AS e5 ON (e5."name" = $2) AND (e5."value" = c0."direction")
    LEFT OUTER JOIN "enums" AS e6 ON (e6."name" = $3) AND (e6."value" = c0."purpose")
    LEFT OUTER JOIN "enums" AS e7 ON (e7."name" = $4) AND (e7."value" = c0."call_type")
    LEFT OUTER JOIN "call_dispositions" AS c8 ON c8."id" = c0."disposition_id"
    LEFT OUTER JOIN ( SELECT
                          sm0."call_id"                    AS "call_id"
                        , ARRAY_AGG(sm0."patient_task_id") AS "task_ids"
                      FROM
                          "msh_patient_tasks_calls" AS sm0
                      GROUP BY sm0."call_id" ) AS s9 ON s9."call_id" = c0."id"
WHERE
      (p2."id" = $1)
  AND (c8."successful" = $5)
ORDER BY
    c0."started_at" DESC NULLS LAST
LIMIT $6;


------------------------------------------------------------------------------------------------------------------------
/*  */ 
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    pg_stat_database;

SELECT query, temp_blks_read, temp_blks_written
FROM pg_stat_statements
ORDER BY temp_blks_written DESC
LIMIT 10;

call public.bake_the_supreme_pizza()