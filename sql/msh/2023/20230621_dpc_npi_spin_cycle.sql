------------------------------------------------------------------------------------------------------------------------
/*
DPC NPI Spin Cycle
   1) Find 100 practitioners that aren't currently registered that have the most patients that don't have coverage pulled from dpc
      - Start with all is_dpc patients that don't have coverage
      - Find all their possible providers
        - first use primary provider
        - fall back to hier rank
      - Remove already attempted providers that errored
      - group by npi, count distinct mbi
   2) Delete out the 100 practitioners with the fewest associated patients by the practitioner_group_patients table
   3) create (if not exists) and register practitioners in step 1
   4) register practitioner group patients
   5) Run patients export on step 1 practitioners
   6) Run coverage export on step 1 practitioners
*/
------------------------------------------------------------------------------------------------------------------------
call public.sp_dpc_refresh_dpc_patients();


-- gather patients that don't have any coverage
-- select count(*) from _patients_to_run; -- 76301
                                     -- 6/27 55800
                                     --      47714
                                     --      43998

DROP TABLE IF EXISTS _patients_to_run;
CREATE TEMP TABLE _patients_to_run AS
SELECT
    dp.source_id patient_id
  , dp.mbi
FROM
    dpoc_patients dp
WHERE
    NOT EXISTS( SELECT 1 FROM dpoc_coverage dc WHERE dc.patient = dp.bene_id )
;

-- get latest export result for every npi-mbi pair
DROP TABLE IF EXISTS _last_export_results;
CREATE TEMP TABLE _last_export_results AS
SELECT DISTINCT ON (x.npi, x.mbi)
    x.npi
  , x.mbi
  , ptr.patient_id
  , x.job_id
  , x.inserted_at                     last_exported_at
  , x.export_type
  , je.id IS NOT NULL                 has_export_error
  , je.error
  , COALESCE(dp.is_registered, FALSE) provider_is_registered
FROM
    ( SELECT
          j.npi
        , UNNEST(mbis) mbi
        , j.inserted_at
        , j.id         job_id
        , j.export_type
      FROM
          dpoc_bulk_export_jobs j
      WHERE
          j.npi IS NOT NULL ) x
    JOIN _patients_to_run ptr ON ptr.mbi = x.mbi
    LEFT JOIN dpoc_bulk_export_job_errors je ON x.job_id = je.dpoc_bulk_export_job_id AND je.mbi = x.mbi
    LEFT JOIN dpoc_practitioners dp ON dp.npi = x.npi
ORDER BY
    x.npi, x.mbi, x.inserted_at DESC;

CREATE INDEX ON _last_export_results(patient_id);
CREATE INDEX ON _last_export_results(mbi);
CREATE INDEX ON _last_export_results(npi);



-- pull all pats primary phys (filtering out failed is wicked slow, delete them out below)
    DROP TABLE IF EXISTS _possible_phys;
    CREATE TEMP TABLE _possible_phys AS
    SELECT
        sp.patient_id
      , mp.npi::TEXT   npi
      , sp.patient_mbi mbi
      , 0              rank -- stage phys rank starts at 1, 0 for primary phys to always come first
    FROM
        fdw_member_doc.supreme_pizza sp
        JOIN fdw_member_doc.msh_physicians mp ON mp.id = sp.primary_physician_id
    WHERE
        sp.patient_mbi is not null
    AND EXISTS( SELECT 1 FROM _patients_to_run ptr WHERE ptr.patient_id = sp.patient_id );

    CREATE UNIQUE INDEX ON _possible_phys(patient_id, npi);

    -- add hierarchy phys
    INSERT
    INTO
        _possible_phys (patient_id, npi, mbi, rank)
    SELECT
        sp.patient_id
      , mp.npi
      , sp.patient_mbi
      , ph.appts_physician_rank
    FROM
        fdw_member_doc_stage.patient_physician_location_hierarchy ph
        JOIN fdw_member_doc.msh_physicians mp ON mp.id = ph.msh_physician_id
        JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = ph.patient_id
    WHERE
        EXISTS( SELECT 1 FROM _patients_to_run ptr WHERE ptr.patient_id = sp.patient_id )
    ON CONFLICT DO NOTHING;

-- remove  phys that have previously failed
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        ppp.npi is null
        or mbi ISNULL
        or EXISTS ( SELECT 1 FROM _last_export_results ler WHERE ler.mbi = ppp.mbi AND ler.npi = ppp.npi and ler.has_export_error);

-- Only part of initial spin cycle, remove providers that have been registered previously
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        EXISTS ( SELECT 1 FROM dpoc_bulk_export_jobs j where j.npi = ppp.npi);

-- take top 100
DROP TABLE IF EXISTS _practitioners_to_register;
CREATE TEMP TABLE _practitioners_to_register AS
SELECT
    pp.npi
  , COUNT(DISTINCT pp.patient_id)
FROM
    _possible_phys pp
    LEFT JOIN dpoc_practitioners dp ON dp.npi = pp.npi AND dp.is_registered
WHERE
    dp.id ISNULL
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 100
;


-- inactivate the bottom 100 providers
DROP TABLE IF EXISTS _to_deregister;
CREATE TEMP TABLE _to_deregister AS
SELECT
    dp.npi
  , COUNT(DISTINCT gp.mbi)
FROM
    dpoc_practitioners dp
    JOIN dpoc_practitioner_group_patients gp ON gp.npi = dp.npi
WHERE
    dp.is_registered
GROUP BY
    1
ORDER BY
    2
LIMIT 100
;
UPDATE dpoc_practitioners dp
SET
    is_active = FALSE, updated_at = NOW()
FROM
    _to_deregister td
WHERE
    td.npi = dp.npi;


-- add new providers
INSERT
INTO
    dpoc_practitioners (npi, first_name, last_name, inserted_at, updated_at, source_id, dpoc_provider_org_id, is_active)
SELECT
    ptr.npi
  , first_name
  , last_name
  , NOW()
  , NOW()
  , id
  , 1
  , TRUE
FROM
    _practitioners_to_register ptr
    JOIN fdw_member_doc.msh_physicians mp ON ptr.npi = mp.npi::TEXT
WHERE
    NOT EXISTS( SELECT 1 FROM dpoc_practitioners dp WHERE dp.npi = ptr.npi )
;

-- flip other providers to active
UPDATE dpoc_practitioners dp
SET
    is_active = TRUE, updated_at = NOW()
FROM
    _practitioners_to_register ptr
WHERE
    ptr.npi = dp.npi;


-- new providers shouldn't have any yet
SELECT *
FROM
    dpoc_practitioner_group_patients gp
join _practitioners_to_register ptr ON gp.npi = ptr.npi and gp.is_registered
;

------------------------------------------------------------------------------------------------------------------------
/* Need to reallocate patients to providers when 1200 patient cap is hit */
------------------------------------------------------------------------------------------------------------------------

    -- Create a temporary table to store the assigned patients and their ranks
    DROP TABLE IF EXISTS _assigned_patients;
    CREATE TEMP TABLE _assigned_patients (
        mbi text,
        npi text,
        rank INTEGER
    );
    create UNIQUE INDEX on _assigned_patients(mbi);

    -- Assign patients to providers based on rank, ensuring provider limits
    WITH ranked_patients AS (
        SELECT mbi, npi, rank,
               ROW_NUMBER() OVER (PARTITION BY npi ORDER BY rank) AS row_num
        FROM _possible_phys
    )
    INSERT INTO _assigned_patients (mbi, npi, rank)
    SELECT distinct on(mbi) mbi, npi, rank
    FROM ranked_patients
    WHERE row_num <= 1200
    order by mbi, rank
    ;

    -- Assign remaining patients to providers based on rank, without exceeding limits
    WITH remaining_patients AS (
        SELECT mbi, npi, rank,
               ROW_NUMBER() OVER (PARTITION BY npi ORDER BY rank) AS row_num
        FROM _possible_phys
        WHERE mbi NOT IN (SELECT mbi FROM _assigned_patients)
    )
    INSERT INTO _assigned_patients (mbi, npi, rank)
    SELECT distinct on (mbi) mbi, npi, rank
    FROM remaining_patients
    WHERE row_num <= 1200 - (
        SELECT COUNT(*) FROM _assigned_patients WHERE npi = remaining_patients.npi
    )
    order by mbi, rank
    ;

--     select npi, count(mbi) from _assigned_patients GROUP BY 1 ORDER BY 2 desc;
--     CREATE TABLE junk.dpoc_pat_pract_20230523 AS
--     SELECT * FROM _final_pat_pract;

-- DROP TABLE IF EXISTS junk.dpoc_npis_to_run_20230523;
--     CREATE TABLE junk.dpoc_npis_to_run_20230523 AS
    INSERT
    INTO
        dpoc_practitioner_group_patients(npi, mbi, inserted_at, updated_at, dpoc_provider_org_id)
    SELECT
        ap.npi
      , ap.mbi
      , NOW()
      , NOW()
      , 1
    FROM
        _assigned_patients ap
    ON CONFLICT DO NOTHING
    ;

CREATE TABLE junk.dpoc_providers_to_cycle_20230627_3 AS
SELECT *
FROM
    _practitioners_to_register;
------------------------------------------------------------------------------------------------------------------------
/* run in elixir */
------------------------------------------------------------------------------------------------------------------------
-- failing on this one for some reason
SELECT * FROM dpoc_practitioner_group_patients where npi = '1285385641';

SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
  AND worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at::DATE >= '2023-06-26'
--       AND inserted_at::DATE >= now()::date
GROUP BY
    1;

select * from oban.oban_jobs where id = 47928531;



SELECT
 (errors[1])['error']
, *
FROM
    oban.oban_jobs
WHERE
    queue = 'dpoc_bulk_export_worker'
--     and state != 'discarded'
    and state != 'completed'
    and state != 'cancelled'
and state = 'discarded'
-- and state = 'retryable'
order by id desc



UPDATE oban.oban_jobs
SET
    state        = 'available'
  , scheduled_at = NOW()
  , discarded_at = NULL
  , max_attempts = attempt + 1
WHERE
    id IN ( 47947783, 47947766, 47944389)
;


-- deal with mbi dupe issues


WITH
    mbis AS ( SELECT *
              FROM
                  ( VALUES

-- ('4X46YN3WF67'),
-- ('1WP6TR1KW87')
('7E84DC9CP88'),
('1VD5T13CW78'),
('3C94NW4TN78')
                    ) x(mbi) )
  , upd  AS (
    UPDATE dpoc_patients ours
        SET
            updated_at = NOW()
            , mbi = dpc.mbi || '_remove_me'
            , bene_id = dpc.bene_id
            , mbis = ARRAY_APPEND(dpc.mbis, ours.mbi)
            , dpoc_last_updated = dpc.dpoc_last_updated
            , dpoc_bulk_export_job_output_id = dpc.dpoc_bulk_export_job_output_id
        FROM
            dpoc_patients dpc
        WHERE
                    ours.source = 'member_doc'
                AND dpc.source = 'dpc'
                AND ours.mbi IN ( SELECT mbi FROM mbis )
                AND dpc.mbi IN ( SELECT mbi FROM mbis )
        RETURNING ours.* )
DELETE
FROM
    dpoc_patients dp
    USING upd u
WHERE
      dp.source = 'dpc'
  AND dp.mbi || '_remove_me' = u.mbi
;

CREATE OR REPLACE PROCEDURE sp_dpc_patients_merge(_dpoc_patient_one_id bigint, _dpoc_patient_two_id bigint)
    LANGUAGE plpgsql
AS
$$
BEGIN

    with upd as (
        UPDATE dpoc_patients ours
            SET
                updated_at = NOW()
                , mbi = dpc.mbi || '_remove_me'
                , bene_id = dpc.bene_id
                , mbis = ARRAY_APPEND(dpc.mbis, ours.mbi)
                , dpoc_last_updated = dpc.dpoc_last_updated
                , dpoc_bulk_export_job_output_id = dpc.dpoc_bulk_export_job_output_id
            FROM
                dpoc_patients dpc
            WHERE
                        ours.source = 'member_doc'
                    AND dpc.source = 'dpc'
                    AND ours.mbi = _dpoc_patient_one_id
                    AND dpc.mbi = _dpoc_patient_two_id
               RETURNING ours.*
               )
     DELETE
        FROM
            dpoc_patients dp
            USING upd u
        WHERE
              dp.source = 'dpc'
          AND dp.mbi || '_remove_me' = u.mbi

    ;

END;
$$;

SELECT mbi, replace(mbi, '_remove_me', '') FROM dpoc_patients WHERE   mbi ~* '_remove_me'
update dpoc_patients set mbi = replace(mbi, '_remove_me', '')  WHERE   mbi ~* '_remove_me';
;
SELECT * FROM dpoc_patients WHERE mbi IN ('9YX4YE2HP88', '5R74HW5HQ68');
SELECT * FROM dpoc_patients WHERE mbi IN ( '3QA3RG8UW06', '8U94Q47NK06')
SELECT * FROM dpoc_patients WHERE mbi IN ( '4DU8QT3GJ61', '9RK1DJ2YG31', '5WW6AX9QU51' );
SELECT * FROM dpoc_patients WHERE mbi IN ('9QX8WH8GM36', '1WN9TY3WW36', '8A15QD7EH56', '9TN7MK2CJ46');
SELECT * FROM dpoc_patients WHERE mbi IN ('9W18EV8AM29', '6Q69D34JU29', '2U32CQ3EJ19'    );

SELECT * FROM dpoc_patients WHERE mbi IN (
   '4X46YN3WF67',
'1WP6TR1KW87'
    );

SELECT mbi, mbis, * FROM dpoc_patients WHERE mbi IN  (
                                           ('7C93NM0WG12'),
                                           ('9WV5HD4EW82'),
                                           ('7G21U87PP02')
                                           );
SELECT * FROM dpoc_patients WHERE mbi IN  ('8TQ6NK3MQ63'),
                                           ('7MJ3ER0HW73'),
                                           ('8PN8A48FF63');
SELECT * FROM dpoc_patients WHERE mbi IN  ('1UA2T10XG84'),
                                           ('2XU9NK9FN54'),
                                           ('5C97QD6YH64');

('4X46YN3WF67'),
('1WP6TR1KW87')
('7E84DC9CP88'),
('1VD5T13CW78'),
('3C94NW4TN78'),
--
-- INSERT
-- INTO
--     dpoc_practitioners (npi, first_name, last_name, inserted_at, updated_at, source_id, dpoc_provider_org_id)
--
-- SELECT a.npi, mp.first_name, mp.last_name, now(), now(), mp.id, 1
-- FROM
--     _new_providers_to_add a
-- join fdw_member_doc.msh_physicians mp on mp.npi::text = a.npi
-- where not exists(select 1 from dpoc_practitioners dp where dp.npi = a.npi)
-- ;
--
-- UPDATE
--     dpoc_practitioners dp
-- SET
--     is_active  = TRUE
--   , updated_at = NOW()
-- WHERE
-- exists(select 1 from _new_providers_to_add a where dp.npi = a.npi)
--       ;

-- NOTE: Need to run DPOC.register_new_practioners() in elixir before going through the code below
-- because the code below depends on the is_registered flag to know what providers are possible to assign pats to

CREATE OR REPLACE PROCEDURE sp_refresh_dpc_roster_and_attribution()
    LANGUAGE plpgsql
AS
$$
BEGIN

    ------------------------------------------------------------------------------------------------------------------------
    /* Attribution */
    ------------------------------------------------------------------------------------------------------------------------
    -- can only use registered providers due to cap
    DROP TABLE IF EXISTS _registered_providers;
    CREATE TEMP TABLE _registered_providers AS
    SELECT
        dp.npi
      , COUNT(pgp.mbi) n
    FROM
        dpoc_practitioners dp
        LEFT JOIN dpoc_practitioner_group_patients pgp ON pgp.npi = dp.npi
    WHERE
        dp.is_registered
    GROUP BY 1;
    CREATE INDEX ON _registered_providers(npi);

    -- get latest export result for every npi-mbi pair
    DROP TABLE IF EXISTS _last_export_results;
    CREATE TEMP TABLE _last_export_results AS
    SELECT DISTINCT ON (x.npi, x.mbi)
        x.npi
      , x.mbi
      , x.job_id
      , x.inserted_at     last_exported_at
      , x.export_type
      , je.id IS NOT NULL has_export_error
      , je.error
      , coalesce(dp.is_registered, false) provider_is_registered
    FROM
        ( SELECT
              j.npi
            , UNNEST(mbis) mbi
            , j.inserted_at
            , j.id         job_id
            , j.export_type
          FROM
              dpoc_bulk_export_jobs j
          WHERE
              j.npi IS NOT NULL ) x
        LEFT JOIN dpoc_bulk_export_job_errors je ON x.job_id = je.dpoc_bulk_export_job_id AND je.mbi = x.mbi
        Left join dpoc_practitioners dp on dp.npi = x.npi
    ORDER BY
        x.npi, x.mbi, x.inserted_at DESC;
    CREATE INDEX ON _last_export_results(mbi);
    CREATE INDEX ON _last_export_results(npi);

    -- patients that have always failed or are new
    DROP TABLE IF EXISTS _patients_to_retry;
    CREATE TEMP TABLE _patients_to_retry AS
    SELECT
        id        dpoc_patient_id
      , source_id patient_id
      , mbi
    FROM
        dpoc_patients dp
    WHERE
          dp.is_active
      AND NOT EXISTS ( SELECT 1 FROM _last_export_results r2
                                WHERE r2.mbi = dp.mbi AND NOT r2.has_export_error and r2.provider_is_registered)
    ;

    CREATE unique INDEX ON _patients_to_retry(patient_id);
    CREATE unique INDEX ON _patients_to_retry(mbi);
    -- SELECT count(*) FROM _patients_to_retry; --23964
--     select r.patient_id isnull as match_found, count(*) from junk.dpoc_pat_pract_20230523 j
--     left join _patients_to_retry r ON j.patient_id = r.patient_id
--     GROUP BY 1


-- pull all pats primary phys (filtering out failed is wicked slow, delete them out below)
    DROP TABLE IF EXISTS _possible_phys;
    CREATE TEMP TABLE _possible_phys AS
    SELECT
        sp.patient_id
      , mp.npi::TEXT   npi
      , sp.patient_mbi mbi
      , 0              rank -- stage phys rank starts at 1, 0 for primary phys to always come first
    FROM
        fdw_member_doc.supreme_pizza sp
        JOIN fdw_member_doc.msh_physicians mp ON mp.id = sp.primary_physician_id
    WHERE
        EXISTS( SELECT 1 FROM _patients_to_retry ptr WHERE ptr.patient_id = sp.patient_id );

    CREATE UNIQUE INDEX ON _possible_phys(patient_id, npi);

    -- add hierarchy phys
    INSERT
    INTO
        _possible_phys (patient_id, npi, mbi, rank)
    SELECT
        sp.patient_id
      , mp.npi
      , sp.patient_mbi
      , ph.appts_physician_rank
    FROM
        fdw_member_doc_stage.patient_physician_location_hierarchy ph
        JOIN fdw_member_doc.msh_physicians mp ON mp.id = ph.msh_physician_id
        JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = ph.patient_id
    WHERE
        EXISTS( SELECT 1 FROM _patients_to_retry ptr WHERE ptr.patient_id = sp.patient_id )
    ON CONFLICT DO NOTHING;

-- remove  phys that have previously failed
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        EXISTS ( SELECT 1 FROM _last_export_results ler WHERE ler.mbi = ppp.mbi AND ler.npi = ppp.npi and ler.has_export_error);

-- remove  phys that have not been registered
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        NOT EXISTS ( SELECT 1 FROM dpoc_practitioners dp WHERE dp.is_registered AND dp.npi = ppp.npi );

-- remove  phys that have exceed patient count
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        EXISTS ( SELECT 1 FROM _registered_providers rp WHERE rp.npi = ppp.npi AND rp.n >= 1200 );

--     SELECT COUNT(*) FROM _possible_phys;

    DROP TABLE IF EXISTS _final_pat_pract;
    CREATE TEMP TABLE _final_pat_pract AS
    SELECT DISTINCT ON (patient_id)
        patient_id
      , npi
      , dp.mbi
      , rank
    FROM
        _possible_phys pp
        JOIN dpoc_patients dp ON dp.source_id = pp.patient_id and dp.is_active
    ORDER BY
        patient_id, rank;

    -- add in previously successful runs
    -- rank -1 to make it even better than pcp
    INSERT
    INTO
        _final_pat_pract (patient_id, npi, mbi, rank)
    SELECT DISTINCT ON (ler.mbi)
        p.source_id
      , ler.npi
      , ler.mbi
      , -1
    FROM
        _last_export_results ler
        JOIN dpoc_patients p ON p.mbi = ler.mbi AND p.is_active
    WHERE
        NOT ler.has_export_error
    and ler.provider_is_registered
    ORDER BY ler.mbi, ler.last_exported_at DESC;

--     SELECT COUNT(*) FROM _final_pat_pract;
------------------------------------------------------------------------------------------------------------------------
/* Populate practitioner group patients */
------------------------------------------------------------------------------------------------------------------------
-- clean out any not pract group pats that weren't successful
    WITH
        archived AS (
            INSERT
                INTO
                    dpoc_practitioner_group_patients_history(id, npi, mbi, is_registered, last_refreshed_at,
                                                             dpoc_provider_org_id, archived_at, inserted_at, updated_at)
                    SELECT
                        id
                      , npi
                      , mbi
                      , is_registered
                      , last_refreshed_at
                      , dpoc_provider_org_id
                      , NOW() archived_at
                      , inserted_at
                      , updated_at
                    FROM
                        dpoc_practitioner_group_patients gp
                    WHERE
                        not EXISTS( SELECT
                                    1
                                FROM
                                    _last_export_results ler
                                WHERE
                                      ler.npi = gp.npi
                                  AND ler.mbi = gp.mbi
                                  AND NOT ler.has_export_error
                                  AND ler.provider_is_registered
                                )
                    RETURNING * )
    DELETE
    FROM
        dpoc_practitioner_group_patients pgp
    WHERE
        EXISTS( SELECT 1 FROM archived a WHERE a.id = pgp.id );


    ------------------------------------------------------------------------------------------------------------------------
    /* Need to reallocate patients to providers when 1200 patient cap is hit */
    ------------------------------------------------------------------------------------------------------------------------

    -- Create a temporary table to store the assigned patients and their ranks
    CREATE TEMP TABLE _assigned_patients (
        mbi text,
        npi text,
        rank INTEGER
    );
    create UNIQUE INDEX on _assigned_patients(mbi);

    -- Assign patients to providers based on rank, ensuring provider limits
    WITH ranked_patients AS (
        SELECT mbi, npi, rank,
               ROW_NUMBER() OVER (PARTITION BY npi ORDER BY rank) AS row_num
        FROM _final_pat_pract
    )
    INSERT INTO _assigned_patients (mbi, npi, rank)
    SELECT mbi, npi, rank
    FROM ranked_patients
    WHERE row_num <= 1200;

    -- Assign remaining patients to providers based on rank, without exceeding limits
    WITH remaining_patients AS (
        SELECT mbi, npi, rank,
               ROW_NUMBER() OVER (PARTITION BY npi ORDER BY rank) AS row_num
        FROM _final_pat_pract
        WHERE mbi NOT IN (SELECT mbi FROM _assigned_patients)
    )
    INSERT INTO _assigned_patients (mbi, npi, rank)
    SELECT mbi, npi, rank
    FROM remaining_patients
    WHERE row_num <= 1200 - (
        SELECT COUNT(*) FROM _assigned_patients WHERE npi = remaining_patients.npi
    );

--     select npi, count(mbi) from _assigned_patients GROUP BY 1 ORDER BY 2 desc;

--     CREATE TABLE junk.dpoc_pat_pract_20230523 AS
--     SELECT * FROM _final_pat_pract;

-- DROP TABLE IF EXISTS junk.dpoc_npis_to_run_20230523;
--     CREATE TABLE junk.dpoc_npis_to_run_20230523 AS
    INSERT
    INTO
        dpoc_practitioner_group_patients(npi, mbi, inserted_at, updated_at, dpoc_provider_org_id)
    SELECT
        ap.npi
      , ap.mbi
      , NOW()
      , NOW()
      , 1
    FROM
        _assigned_patients ap
    ON CONFLICT DO NOTHING
    ;
END;

$$;



SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
  AND worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at::DATE >= '2023-06-05'
--       AND inserted_at::DATE >= now()::date
GROUP BY
    1;


SELECT *
FROM
    oban.oban_jobs
WHERE
      worker ~* 'DPOC'
  AND inserted_at::DATE = NOW()::DATE;


SELECT run_end_time - md_portals_batches.run_start_time, *
FROM
    md_portals.md_portals_batches ORDER BY id desc;

SELECT *
FROM
    pg_stat_activity WHERE state != 'idle';
    -- ORDER BY
--     id DESC;
;
-- call sp_refresh_dpc_roster_and_attribution();
