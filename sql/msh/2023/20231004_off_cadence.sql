-- drop PROCEDURE sp_patient_task_off_call_cadence_add()
CREATE or replace PROCEDURE sp_patient_task_off_call_cadence_add()
    LANGUAGE plpgsql
AS
$$
    DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN
    BEGIN
        -- New med adh + quality tasks inserted 2+ days ago without a call
        INSERT
        INTO
            patient_task_call_cadence_off_track (patient_task_id, assigned_to_user_id, is_current,
                                                 started_at, look_back_period, look_back_period_units,
                                                 inserted_at, updated_at)
        SELECT
            pt.id
          , pt.assigned_to_id

          , TRUE   is_current
          , pt.inserted_at
          , 2      look_back_period
          , 'days' look_back_period_units
          , NOW()  inserted_at
          , NOW()  updated_at
        FROM
            patient_tasks pt
            JOIN tasks t ON pt.task_type = t.type
        WHERE
                t.task_group_config IN ('medication_adherence', 'quality_gaps', 'recon_followup')
          AND   pt.status = 'new'
          AND   pt.inserted_at < NOW() - '2 days'::INTERVAL
          AND   NOT EXISTS( SELECT
                                1
                            FROM
                                calls c
                            WHERE
                                  c.patient_id = pt.patient_id
                              AND c.started_at > pt.inserted_at )
          AND   NOT EXISTS( SELECT
                                1
                            FROM
                                patient_task_call_cadence_off_track ptccot
                            WHERE
                                  ptccot.patient_task_id = pt.id
                              AND ptccot.is_current
                               )
        ;


        -- in progress med adh with order status in progress still calling
        INSERT
        INTO
            patient_task_call_cadence_off_track (patient_task_id, assigned_to_user_id, is_current,
                                                 started_at, look_back_period, look_back_period_units,
                                                 inserted_at, updated_at)
        SELECT
            x.patient_task_id
          , x.assigned_to_id

          , TRUE   is_current
          , x.started_at
          , 2      look_back_period
          , 'days' look_back_period_units
          , NOW()  inserted_at
          , NOW()  updated_at
        FROM
            ( SELECT
                  pt.id                  patient_task_id
                , pt.assigned_to_id
                , MAX(mapth.inserted_at) started_at
              FROM
                  patient_tasks pt
                  JOIN tasks t ON pt.task_type = t.type
                  JOIN medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
                  JOIN medication_adherence_order_status_history mapth ON mapt.id = mapth.medication_adherence_task_id
                  LEFT JOIN medication_adherence_order_status_history mapth2 -- more recent order status
                            ON mapt.id = mapth2.medication_adherence_task_id
                                AND mapth2.inserted_at > mapth.inserted_at
              WHERE
                    t.task_group_config = 'medication_adherence'
                AND pt.status = 'in_progress'
                AND mapth2.id ISNULL -- exclude where there has been a more recent order status
                AND mapt.order_status = 'in_progress_calling'
                AND mapth.current_order_status = 'in_progress_calling'
                AND mapth.inserted_at < NOW() - '2 days'::INTERVAL
                AND NOT EXISTS( SELECT
                                    1
                                FROM
                                    calls c
                                WHERE
                                      c.patient_id = pt.patient_id
                                  AND c.started_at > pt.inserted_at )
                AND NOT EXISTS( SELECT
                                    1
                                FROM
                                    patient_task_call_cadence_off_track ptccot
                                WHERE
                                      ptccot.patient_task_id = pt.id
                                  AND ptccot.is_current
                                   )
              GROUP BY 1, 2 ) x
        ;

        -- quality task_status = in_progress and task substatus = pending scheduling
        INSERT
        INTO
            patient_task_call_cadence_off_track (patient_task_id, assigned_to_user_id, is_current,
                                                 started_at, look_back_period, look_back_period_units,
                                                 inserted_at, updated_at)
        SELECT
            x.patient_task_id
          , x.assigned_to_id

          , TRUE   is_current
          , x.started_at
          , 2      look_back_period
          , 'days' look_back_period_units
          , NOW()  inserted_at
          , NOW()  updated_at
        FROM
            ( SELECT
                  pt.id                 patient_task_id
                , pt.assigned_to_id
                , MAX(mpmsh.changed_at) started_at
              FROM
                  tasks t
                  JOIN patient_tasks pt ON pt.task_type = t.type
                  JOIN patient_quality_measures_tasks pqmt ON pqmt.patient_task_id = pt.id
                  JOIN patient_quality_measures pqm ON pqmt.patient_measure_id = pqm.id
                  JOIN msh_patient_quality_measures mpqm ON pqm.id = mpqm.patient_quality_measure_id
                  JOIN msh_patient_measure_substatus_history mpmsh ON mpqm.id = mpmsh.msh_patient_quality_measure_id
                  LEFT JOIN msh_patient_measure_substatus_history mpmsh2
                            ON mpqm.id = mpmsh2.msh_patient_quality_measure_id
                                AND mpmsh2.changed_at > mpmsh.changed_at
              WHERE
                    t.task_group_config IN ('quality_gaps', 'recon_followup')
                AND pt.status = 'in_progress'
                AND mpqm.substatus = 'pending_scheduling'
                AND mpmsh.substatus = 'pending_scheduling'
                AND mpmsh.changed_at < NOW() - '2 days'::INTERVAL
                AND mpmsh2.id ISNULL
                AND NOT EXISTS( SELECT
                                    1
                                FROM
                                    calls c
                                WHERE
                                      c.patient_id = pt.patient_id
                                  AND c.started_at > pt.inserted_at )
                AND NOT EXISTS( SELECT
                                    1
                                FROM
                                    patient_task_call_cadence_off_track ptccot
                                WHERE
                                      ptccot.patient_task_id = pt.id
                                  AND ptccot.is_current
                                   )
              GROUP BY 1, 2 ) x
        ;

        -- pat exp
        INSERT
        INTO
            patient_task_call_cadence_off_track (patient_task_id, assigned_to_user_id, is_current,
                                                 started_at, look_back_period, look_back_period_units,
                                                 inserted_at, updated_at)
        SELECT
            pt.id
          , pt.assigned_to_id

          , TRUE   is_current
          , pt.inserted_at
          , 2      look_back_period
          , 'days' look_back_period_units
          , NOW()  inserted_at
          , NOW()  updated_at
        FROM
            patient_tasks pt
            JOIN tasks t ON pt.task_type = t.type
        WHERE
           t.type = 'patient_experience_follow_up'
          AND pt.status in ('new', 'in_progress')
          AND pt.inserted_at < NOW() - '2 days'::INTERVAL
          AND not exists(
            SELECT 1 from calls c
            WHERE c.patient_id = pt.patient_id
              AND c.started_at > pt.inserted_at
          )
          AND NOT exists(
            SELECT 1 from patient_task_call_cadence_off_track ptccot
            WHERE ptccot.patient_task_id = pt.id
              AND ptccot.is_current

        );

        EXCEPTION WHEN OTHERS THEN
         /**/     -- raise notice 'x % %', SQLERRM, SQLSTATE;
         /**/     GET DIAGNOSTICS stack = PG_CONTEXT;
         /**/     --  RAISE NOTICE E'--- Call Stack ---\n%', stack;
         /**/     GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT,
         /**/                             exception_detail = PG_EXCEPTION_DETAIL,
         /**/                             exception_hint = PG_EXCEPTION_HINT,
         /**/                             exception_context = PG_EXCEPTION_CONTEXT;
         /**/     -- raise notice '--> sqlerrm(%) sqlstate(%) mt(%)  ed(%)  eh(%)  stack(%) ec(%)', SQLERRM, SQLSTATE, message_text, exception_detail, exception_hint, stack, exception_context;
         /**/     raise notice '-----';
         /**/     --raise notice ' stck(%)', exception_context;
         /**/     raise notice ' exception_context(%), message_text(%)', exception_context, message_text;
         /**/     raise notice '-----';
         /**/     -------
         /**/     -- GET EXCEPTION INFO
         /**/     error_text = 'Message_Text( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
         /**/     insert into rpt.error_log(location, error_note)
         /**/     select 'public.sp_patient_task_off_call_cadence_add()', error_text;
         /**/     INSERT INTO public.sms_alerts (body, recipient_phone_numbers, inserted_at, updated_at) VALUES
         /**/      (
         /**/          E'Issue creating patient task call cadence: public.sp_patient_task_off_call_cadence_add() threw an exception. :: \n ' || left(error_text, 1000),
         /**/          '{+19084894555}',
         /**/          now(),
         /**/          now()
         /**/      );
         /**/   commit;
         /**/   -------
         /**/   RAISE EXCEPTION 'Error in stage.sp_stp_process_med_adherence_tasks() :: %', error_text;
    END;
END
$$;

-- drop PROCEDURE sp_patient_task_off_call_cadence_close();
CREATE or replace PROCEDURE sp_patient_task_off_call_cadence_close()
    LANGUAGE plpgsql
AS
$$
    DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN
    BEGIN

        UPDATE patient_task_call_cadence_off_track ot
        SET
            is_current = FALSE, back_on_track_at = c.started_at, updated_at = now()
        FROM
            calls c
            JOIN patient_tasks pt ON pt.patient_id = c.patient_id
        WHERE
              pt.id = ot.patient_task_id
          AND ot.is_current
          AND c.started_at > ot.started_at;

        EXCEPTION WHEN OTHERS THEN
         /**/     -- raise notice 'x % %', SQLERRM, SQLSTATE;
         /**/     GET DIAGNOSTICS stack = PG_CONTEXT;
         /**/     --  RAISE NOTICE E'--- Call Stack ---\n%', stack;
         /**/     GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT,
         /**/                             exception_detail = PG_EXCEPTION_DETAIL,
         /**/                             exception_hint = PG_EXCEPTION_HINT,
         /**/                             exception_context = PG_EXCEPTION_CONTEXT;
         /**/     -- raise notice '--> sqlerrm(%) sqlstate(%) mt(%)  ed(%)  eh(%)  stack(%) ec(%)', SQLERRM, SQLSTATE, message_text, exception_detail, exception_hint, stack, exception_context;
         /**/     raise notice '-----';
         /**/     --raise notice ' stck(%)', exception_context;
         /**/     raise notice ' exception_context(%), message_text(%)', exception_context, message_text;
         /**/     raise notice '-----';
         /**/     -------
         /**/     -- GET EXCEPTION INFO
         /**/     error_text = 'Message_Text( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
         /**/     insert into rpt.error_log(location, error_note)
         /**/     select 'public.sp_patient_task_off_call_cadence_close()', error_text;
         /**/     INSERT INTO public.sms_alerts (body, recipient_phone_numbers, inserted_at, updated_at) VALUES
         /**/      (
         /**/          E'Issue closing out patient task call cadence off track: public.sp_patient_task_off_call_cadence_close()() threw an exception. :: \n ' || left(error_text, 1000),
         /**/          '{+19084894555}',
         /**/          now(),
         /**/          now()
         /**/      );
         /**/   commit;
         /**/   -------
         /**/   RAISE EXCEPTION 'Error in stage.sp_stp_process_med_adherence_tasks() :: %', error_text;
    END;
END
$$
;
call sp_patient_task_off_call_cadence_add();
call sp_patient_task_off_call_cadence_close();
SELECT * FROM patient_task_call_cadence_off_track ;


SELECT *
FROM
    schema_migrations WHERE version = 20231003170315;



CREATE TABLE patient_task_call_cadence_off_track (
    id                     BIGSERIAL PRIMARY KEY,
    patient_task_id        BIGINT REFERENCES patient_tasks,
    assigned_to_user_id    BIGINT REFERENCES users,
    is_current             BOOLEAN,
    started_at             TIMESTAMP,
    back_on_track_at       TIMESTAMP,
    look_back_period       INTEGER,
    look_back_period_units TEXT,
    inserted_at            TIMESTAMP(0) NOT NULL,
    updated_at             TIMESTAMP(0) NOT NULL
);

INSERT
INTO
    oban.oban_crons (name, expression, worker, opts, paused, lock_version, inserted_at, updated_at)
VALUES
    ('patient_task_off_call_cadence_add', '18 3 * * *', 'Deus.SQLRunner', '{
      "args": {
        "sql": "call public.sp_patient_task_off_call_cadence_add();",
        "params": [],
        "database_connection_id": 34
      },
      "queue": "deus_sql_runner_async",
      "max_attempts": 1
    }', FALSE, 1, now(), now());
INSERT
INTO
    oban.oban_crons (name, expression, worker, opts, paused, lock_version, inserted_at, updated_at)
VALUES
    ('patient_task_off_call_cadence_close', '* * * * *', 'Deus.SQLRunner', '{
      "args": {
        "sql": "call public.sp_patient_task_off_call_cadence_close();",
        "params": [],
        "database_connection_id": 34
      },
      "queue": "deus_sql_runner_async",
      "max_attempts": 1
    }', FALSE, 1, now(), now());

SELECT * FROM oban.oban_jobs WHERE queue = 'deus_sql_runner_async' and args->>'sql' ~* 'off_';
SELECT *
FROM
    patient_task_call_cadence_off_track where not is_current;

-- task detail cancel btn
INSERT
INTO
    "patient_task_activities" ("action", "patient_task_id", "user_id", "value", "inserted_at", "updated_at")
VALUES
    ($1, $2, $3, $4, $5, $6)
RETURNING "id" ["cancelled", 714, 2, "moved_out_of_area", ~N[2023-10-06 14:03:43], ~N[2023-10-06 14:03:43]];;

-- bulk cancel
INSERT
INTO
    "patient_task_activities" ("action", "patient_task_id", "user_id", "value", "inserted_at", "updated_at")
VALUES
    ($1, $2, $3, $4, $5, $6)
RETURNING "id" ["update_status", 1287, 2, "cancelled", ~N[2023-10-06 14:05:10], ~N[2023-10-06 14:05:10]];

-- status drop down
INSERT
INTO
    "patient_task_activities" ("action", "patient_task_id", "user_id", "value", "inserted_at", "updated_at")
VALUES
    ($1, $2, $3, $4, $5, $6)
RETURNING "id" ["cancelled", 125, 2, "bullshit", ~N[2023-10-06 14:06:52], ~N[2023-10-06 14:06:52]];

-- med adh sproc
INSERT
INTO
    public.patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at,
                                    updated_at)
SELECT
    ttc.id
  , 2
  , 'cancelled'
  , CASE WHEN ttc.adr <= 0 THEN 'Auto-Cancelled-ADR <=0'
         ELSE 'Auto-Cancelled: 1 Fill and Minimal Days Remaining.' END
  , NULL
  , NOW()
  , NOW()
FROM
    _cancel_adr_zeros ttc;
