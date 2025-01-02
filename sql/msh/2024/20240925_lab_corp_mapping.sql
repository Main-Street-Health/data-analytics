SELECT
    subscriber_id
  , date_of_service
  , test_order_name
  , performed_test_name
  , test_result
  , abnormal_code
  , reference_range
, result_loinc
FROM
    analytics.raw.lab_corp_response ;


select * from etl.rtcoop_patient_loinc_codes lk ;
SELECT count(*) FROM analytics.etl.rtcoop_patient_loinc_codes ;
SELECT count(*) FROM analytics.raw.lab_corp_response
;
SELECT *
FROM
    analytics.raw.lab_corp_response lc
;
-- call etl.sp_rtcoop_loinc_patient_codes();


INSERT
INTO
    etl.rtcoop_patient_loinc_codes(patient_id, loinc_code, value, order_date, result_date, order_test_name, range,
                                   units, analytic_inserted_at, patient_external_id, integrations_id, raw_id,
                                   raw_table_name);
SELECT
    patient_id, loinc_code, value, order_date, result_date, order_test_name, range,
    units, analytic_inserted_at, patient_external_id, integrations_id, raw_id,
    raw_table_name
FROM
    ( SELECT DISTINCT ON (patient_id, loinc_code, order_date)
          lcr.subscriber_id       patient_id
        , lcr.result_loinc        loinc_code
        , lcr.test_result         value
        , lcr.date_of_service     order_date
        , lcr.date_of_service     result_date
        , lcr.test_order_name     order_test_name
        , lcr.reference_range     range
        , lcr.test_units          units
        , lcr.inserted_at         analytic_inserted_at
        , NULL                    patient_external_id
        , NULL                    integrations_id
        , lcr.id                  raw_id
        , 'raw.lab_corp_response' raw_table_name
      FROM
          raw.lab_corp_response lcr
      WHERE
          NOT EXISTS ( SELECT
                           1
                       FROM
                           etl.rtcoop_patient_loinc_codes i
                       WHERE
                             i.patient_id = lcr.patient_id
                         AND i.loinc_code = lcr.result_loinc
                         AND i.order_date = to_date(lcr.date_of_service, 'MM/DD/YYYY'))
      ORDER BY patient_id, loinc_code, order_date, lcr.id DESC ) x
;

alter table raw.lab_corp_response add column patient_id bigint GENERATED ALWAYS AS (subscriber_id::bigint) STORED;
-- alter table raw.lab_corp_response add column dos date GENERATED ALWAYS AS (to_date(date_of_service, 'MM/DD/YYYY')) STORED;

create INDEX  on raw.lab_corp_response(patient_id, result_loinc, to_date(date_of_service, 'MM/DD/YYYY'));

DROP TABLE IF EXISTS _duped_lab_corp;
CREATE TEMP TABLE _duped_lab_corp AS 
SELECT
    r.patient_id
  , r.date_of_service
  , r.performed_test_name
  , r.result_loinc
  , COUNT(*) n
  , ARRAY_AGG(DISTINCT r.id)          ids
  , ARRAY_AGG(DISTINCT r.inserted_at) ins
  , ARRAY_AGG(DISTINCT r.file_creation_date) file_creation_dates
  , ARRAY_AGG(DISTINCT r.inbound_file_id) ib_file_ids
  , ARRAY_AGG(DISTINCT r.specimen_number) spec_ids
FROM
    raw.lab_corp_response r
GROUP BY  1,2,3,4
having count(*) > 1
    ;
SELECT * FROM analytics.raw.lab_corp_response WHERE id in (17140210,17250314);
SELECT * FROM _duped_lab_corp where array_length(spec_ids, 1) > 1
SELECT
    specimen_number
  , COUNT(DISTINCT lab_corp_response.patient_id)
FROM
    analytics.raw.lab_corp_response
GROUP BY specimen_number
having  COUNT(DISTINCT lab_corp_response.date_of_service) > 1
;

SELECT *
FROM
    analytics.raw.lab_corp_response;
WHERE
    specimen_number IN (

'330548823850',
'332129874900',
'334969600760',
'405370629280',
'405907902360',
'405917006650',
'406407508190',
'406538206640',
'406601815630',
'406767000810',
'406870328630',
'407174419920',
'408071436500',
'414458908050',
'415114604670',
'416662634740',
'416965201510',
'417968712410',
'418407854220',
'419312906861',
'422776105070',
'425617442540'

        )
order by specimen_number
;
patient_id, loinc_code, value, order_date, result_date, order_test_name, range,
                                   units, analytic_inserted_at, patient_external_id, integrations_id, raw_id,
                                   raw_table_name
drop TABLE staging.lab_corp_response;
CREATE TABLE staging.lab_corp_response (
    id                           BIGSERIAL PRIMARY KEY,
    patient_id                   bigint,
    date_of_service              date,
    test_order_name              TEXT,
    performed_test_name          TEXT,
    result_loinc                 TEXT,
    test_result                  TEXT,
    abnormal_code                TEXT,
    reference_range              TEXT,
    test_units                   TEXT,
    inserted_at                  TIMESTAMP DEFAULT NOW() NOT NULL,
    updated_at                  TIMESTAMP DEFAULT NOW() NOT NULL,
    latest_raw_id                BIGINT
);
create UNIQUE INDEX on staging.lab_corp_response(patient_id, date_of_service, result_loinc);
create INDEX on staging.lab_corp_response(latest_raw_id);

WITH
    latest_results AS ( SELECT DISTINCT ON (patient_id, date_of_service, result_loinc) *
                        FROM
                            analytics.raw.lab_corp_response r
                        ORDER BY
                            patient_id, date_of_service, result_loinc, file_creation_date DESC, inserted_at DESC, id desc)
INSERT
INTO
    staging.lab_corp_response (patient_id, date_of_service, test_order_name, performed_test_name,
                               result_loinc, test_result, abnormal_code, reference_range, test_units, latest_raw_id)
SELECT
    patient_id, date_of_service, test_order_name, performed_test_name,
    result_loinc, test_result, abnormal_code, reference_range, test_units, latest_raw_id
FROM
    ( SELECT
          patient_id
        , date_of_service::date date_of_service
        , test_order_name
        , performed_test_name
        , result_loinc
        , test_result
        , abnormal_code
        , reference_range
        , test_units
        , id latest_raw_id
      FROM
          latest_results ) x
;



------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------

INSERT
INTO
    etl.rtcoop_patient_loinc_codes(patient_id, loinc_code, value, order_date, result_date, order_test_name, range,
                                   units, analytic_inserted_at, patient_external_id, integrations_id, raw_id,
                                   raw_table_name);
SELECT
    patient_id, loinc_code, value, order_date, result_date, order_test_name, range,
    units, analytic_inserted_at, patient_external_id, integrations_id, raw_id,
    raw_table_name
FROM
    ( SELECT
          lcr.patient_id              patient_id
        , lcr.result_loinc            loinc_code
        , lcr.test_result             value
        , lcr.date_of_service         order_date
        , lcr.date_of_service         result_date
        , lcr.test_order_name         order_test_name
        , lcr.reference_range         range
        , lcr.test_units              units
        , lcr.inserted_at             analytic_inserted_at
        , NULL                        patient_external_id
        , NULL                        integrations_id
        , lcr.id                      raw_id
        , 'staging.lab_corp_response' raw_table_name
      FROM
          staging.lab_corp_response lcr
      WHERE
          NOT EXISTS ( SELECT
                           1
                       FROM
                           etl.rtcoop_patient_loinc_codes i
                       WHERE
                             i.patient_id = lcr.patient_id
                         AND i.loinc_code = lcr.result_loinc
                         AND i.order_date = lcr.date_of_service)
      ) x;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------

CREATE PROCEDURE staging.sp_rts_lab_corp_data()
    LANGUAGE plpgsql
AS
$$
DECLARE
    _greatest_processed_raw_id BIGINT;
    message_text               TEXT;
    exception_detail           TEXT;
    exception_hint             TEXT;
    stack                      TEXT;
    exception_context          TEXT;
    error_text                 TEXT;
BEGIN
    /*
    ------------------------------------------------------------------------------------------------------------
    -- Author : Brendon 20240925

    Change Log
    --------------------------------------
    DATE        AUTHOR         DESCRIPTION
    ----------------------------------------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------
    */

    -- check to see if we are processing the med history in the correct order
    _greatest_processed_raw_id = ( SELECT
                                       MAX(latest_raw_id)
                                   FROM
                                       staging.lab_corp_response );

    DROP TABLE IF EXISTS _controls;
    CREATE TEMPORARY TABLE _controls AS
    SELECT DISTINCT _greatest_processed_raw_id greatest_processed_raw_id;

    WITH
        latest_results AS ( SELECT DISTINCT ON (patient_id, date_of_service, result_loinc) *
                            FROM
                                analytics.raw.lab_corp_response r
                            WHERE
                                id > ( SELECT greatest_processed_raw_id FROM _controls )
                            ORDER BY
                                patient_id, date_of_service, result_loinc, file_creation_date DESC, inserted_at DESC
                                          , id DESC )
    INSERT
    INTO
        staging.lab_corp_response AS s (patient_id, date_of_service, test_order_name, performed_test_name,
                                        result_loinc, test_result, abnormal_code, reference_range, test_units,
                                        latest_raw_id)
    SELECT
        patient_id
      , date_of_service
      , test_order_name
      , performed_test_name
      , result_loinc
      , test_result
      , abnormal_code
      , reference_range
      , test_units
      , latest_raw_id
    FROM
        ( SELECT
              patient_id
            , date_of_service::DATE date_of_service
            , test_order_name
            , performed_test_name
            , result_loinc
            , test_result
            , abnormal_code
            , reference_range
            , test_units
            , id                    latest_raw_id
          FROM
              latest_results ) x
    ON CONFLICT (patient_id, date_of_service, result_loinc)
        DO UPDATE SET
                      test_order_name     = excluded.test_order_name
                    , performed_test_name = excluded.performed_test_name
                    , test_result         = excluded.test_result
                    , abnormal_code       = excluded.abnormal_code
                    , reference_range     = excluded.reference_range
                    , test_units          = excluded.test_units
                    , latest_raw_id       = excluded.latest_raw_id
                    , updated_at          = NOW()
    WHERE
         s.test_order_name IS DISTINCT FROM excluded.test_order_name
      OR s.performed_test_name IS DISTINCT FROM excluded.performed_test_name
      OR s.test_result IS DISTINCT FROM excluded.test_result
      OR s.abnormal_code IS DISTINCT FROM excluded.abnormal_code
      OR s.reference_range IS DISTINCT FROM excluded.reference_range
      OR s.test_units IS DISTINCT FROM excluded.test_units
      OR s.latest_raw_id IS DISTINCT FROM excluded.latest_raw_id;


    ---------------------------
    ---------------------------
EXCEPTION
    WHEN OTHERS THEN
        GET DIAGNOSTICS stack = PG_CONTEXT;
        GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT, exception_detail = PG_EXCEPTION_DETAIL, exception_hint = PG_EXCEPTION_HINT, exception_context = PG_EXCEPTION_CONTEXT;
        ROLLBACK;
        error_text = '(1) Message_Text( ' || COALESCE(message_text, '') || E' ) \nstack (' ||
                     COALESCE(exception_context, '') || ' ) ';
        PERFORM *
        FROM
            audit.fn_create_sms_alerts(ARRAY ['de-analytics-etl','ae-analytics-etl'],
                                       'staging.sp_rts_lab_corp_data', error_text::TEXT);
        COMMIT;
        RAISE EXCEPTION 'staging.sp_rts_lab_corp_data :: %', error_text;

END;
$$;

call staging.sp_rts_lab_corp_data();
SELECT *
FROM
    inbound_file_config;
UPDATE public.inbound_file_config
SET
    post_success_oban_job_opts = '{
      "queue": "deus_sql_runner_2",
      "worker": "Deus.SQLRunner"
    }', post_success_oban_job_args = '{
  "sql": "call staging.sp_rts_lab_corp_data();",
  "params": [],
  "database_connection_id": 1
}'
WHERE
    id = 22606;
SELECT post_success_oban_job_opts
FROM
    inbound_file_config WHERE post_success_oban_job_opts is not null

