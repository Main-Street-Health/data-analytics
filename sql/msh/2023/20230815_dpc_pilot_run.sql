------------------------------------------------------------------------------------------------------------------------
/* run dpc just at these rpls
  218,221,225,227
*/
------------------------------------------------------------------------------------------------------------------------
-- refresh patients
call public.sp_dpc_refresh_dpc_patients();


-- get pilot pats
DROP TABLE IF EXISTS _pilot_pats;
CREATE TEMP TABLE _pilot_pats AS
SELECT sp.patient_id, sp.patient_mbi
FROM
    fdw_member_doc.supreme_pizza sp
where sp.primary_referring_partner_id in (218,221,225,227)
and sp.is_dpc
;
UPDATE _pilot_pats pp
SET patient_mbi = dp.mbi
FROM dpoc_patients dp
WHERE dp.source_id = pp.patient_id
  AND dp.mbi != pp.patient_mbi;


-- look at patient to provider roster counts
-- stage providers to add remove
-- add remove in elxir
-- create attribution
-- run attribution
-- run exports

-- NOTE: Need to run DPOC.register_new_practioners() in elixir before going through the code below
-- because the code below depends on the is_registered flag to know what providers are possible to assign pats to

CREATE OR REPLACE PROCEDURE sp_refresh_dpc_roster_and_attribution()
    LANGUAGE plpgsql
AS
$$
BEGIN


--     select count(*) from dpoc_patients WHERE is_active;
--     select count(*) from fdw_member_doc.supreme_pizza sp join fdw_member_doc_ent.patients p on p.patient_id = sp.patient_id and p.mbi is not null  WHERE sp.is_dpc;

    ------------------------------------------------------------------------------------------------------------------------
    /* Attribution */
    ------------------------------------------------------------------------------------------------------------------------
    -- can only use registered providers due to cap
--     DROP TABLE IF EXISTS _registered_providers;
--     CREATE TEMP TABLE _registered_providers AS
--     SELECT
--         dp.npi
--       , COUNT(pgp.mbi) n
--     FROM
--         dpoc_practitioners dp
--         LEFT JOIN dpoc_practitioner_group_patients pgp ON pgp.npi = dp.npi
--     WHERE
--         dp.is_registered
--     GROUP BY 1;
--     CREATE INDEX ON _registered_providers(npi);

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
        join _pilot_pats pp on pp.patient_mbi = x.mbi
        LEFT JOIN dpoc_bulk_export_job_errors je ON x.job_id = je.dpoc_bulk_export_job_id AND je.mbi = x.mbi
        Left join dpoc_practitioners dp on dp.npi = x.npi
    ORDER BY
        x.npi, x.mbi, x.inserted_at DESC;
    CREATE INDEX ON _last_export_results(mbi);
    CREATE INDEX ON _last_export_results(npi);

    -- patients that have always failed or are new
--     DROP TABLE IF EXISTS _patients_to_retry;
--     CREATE TEMP TABLE _patients_to_retry AS
--     SELECT
--         id        dpoc_patient_id
--       , source_id patient_id
--       , mbi
--     FROM
--         dpoc_patients dp
--     WHERE
--           dp.is_active
--       AND NOT EXISTS ( SELECT 1 FROM _last_export_results r2
--                                 WHERE r2.mbi = dp.mbi AND NOT r2.has_export_error and r2.provider_is_registered)
--     ;
--
--     CREATE unique INDEX ON _patients_to_retry(patient_id);
--     CREATE unique INDEX ON _patients_to_retry(mbi);



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
        join _pilot_pats pp on pp.patient_id = sp.patient_id
        JOIN fdw_member_doc.msh_physicians mp ON mp.id = sp.primary_physician_id
    where sp.is_dpc and sp.patient_mbi is not null;

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
        join _pilot_pats pp on pp.patient_id = ph.patient_id
        JOIN fdw_member_doc.msh_physicians mp ON mp.id = ph.msh_physician_id
        JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = ph.patient_id
    where sp.is_dpc and sp.patient_mbi is not null
    ON CONFLICT DO NOTHING;

-- Add in previously successful providers
    INSERT INTO _possible_phys (patient_id, npi, mbi, rank)
    SELECT DISTINCT ON (p.source_id)
        p.source_id
      , ler.npi
      , ler.mbi
      , -1
    FROM
        _last_export_results ler
        JOIN dpoc_patients p ON p.mbi = ler.mbi AND p.is_active
    WHERE
        NOT ler.has_export_error
    ORDER BY p.source_id, ler.last_exported_at DESC
    ON CONFLICT DO NOTHING
;

-- remove  phys that have previously failed
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        EXISTS ( SELECT 1 FROM _last_export_results ler WHERE ler.mbi = ppp.mbi AND ler.npi = ppp.npi and ler.has_export_error);

-- remove  phys that don't have mbi in dpoc patients
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        NOT EXISTS ( SELECT 1 FROM dpoc_patients dp WHERE dp.is_active and dp.mbi = ppp.mbi);

    delete FROM _possible_phys WHERE npi ISNULL;

-- -- remove  phys that have not been registered
--     DELETE
--     FROM
--         _possible_phys ppp
--     WHERE
--         NOT EXISTS ( SELECT 1 FROM dpoc_practitioners dp WHERE dp.is_registered AND dp.npi = ppp.npi );

-- -- remove  phys that have exceed patient count
--     DELETE
--     FROM
--         _possible_phys ppp
--     WHERE
--         EXISTS ( SELECT 1 FROM _registered_providers rp WHERE rp.npi = ppp.npi AND rp.n >= 1200 );

--     SELECT COUNT(*) FROM _possible_phys;



--     SELECT COUNT(*) FROM _final_pat_pract;

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
    WITH
       ranked_patients AS (
        SELECT mbi, npi, rank,
               ROW_NUMBER() OVER (PARTITION BY npi ORDER BY rank) AS npi_row_num
        FROM _possible_phys pp
    )
    INSERT INTO _assigned_patients (mbi, npi, rank)
    SELECT distinct on(mbi) mbi, npi, rank
    FROM ranked_patients rp
    WHERE npi_row_num <= 1200
    ORDER BY mbi, rank, npi_row_num desc;

--     SELECT * FROM _assigned_patients ;
--     SELECT npi, count(*) FROM _assigned_patients GROUP BY 1 ORDER BY  2 desc;


    -- Assign remaining patients to providers based on rank, without exceeding limits
    WITH prov_below_cnt as (
            select npi, count(*) n
            from _assigned_patients
            GROUP BY 1
            having count(*) < 1200
        )
      , remaining_patients AS (
        SELECT mbi, npi, rank,
               ROW_NUMBER() OVER (PARTITION BY npi ORDER BY rank) AS row_num
        FROM _possible_phys pp
        WHERE mbi NOT IN (SELECT mbi FROM _assigned_patients)
        and exists(select 1 from prov_below_cnt pbc where pbc.npi = pp.npi)
    )
    INSERT INTO _assigned_patients (mbi, npi, rank)
    SELECT distinct on(mbi) mbi, npi, rank
    FROM remaining_patients
    WHERE row_num <= 1200 - (
        SELECT COUNT(*) FROM _assigned_patients WHERE npi = remaining_patients.npi
    )
    ORDER BY mbi, rank, row_num;

-- SELECT * FROM _possible_phys pp where not exists(select 1 from _assigned_patients ap where ap.mbi = pp.mbi);
select count(distinct npi) from _assigned_patients; -- 29
------------------------------------------------------------------------------------------------------------------------
/* Update practitioners */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _providers_to_run;
CREATE TEMP TABLE _providers_to_run AS
SELECT
    npi
  , COUNT(*) n
FROM
    _assigned_patients
GROUP BY 1
ORDER BY 2 DESC
LIMIT 750 ;

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
    _providers_to_run ptr
    JOIN fdw_member_doc.msh_physicians mp ON ptr.npi = mp.npi::TEXT
WHERE
    NOT EXISTS( SELECT 1 FROM dpoc_practitioners dp WHERE dp.npi = ptr.npi )
;

SELECT * FROM _providers_to_run ptr join dpoc_practitioners dp on dp.npi = ptr.npi;

-- make inactive practitioners active
update dpoc_practitioners dp
set updated_at = now(), is_active = true
from _providers_to_run ptr
where ptr.npi = dp.npi
and not dp.is_active;

-- make active practitioners inactive
update dpoc_practitioners dp
set updated_at = now(), is_active = false
where dp.is_active
and not exists(select 1 from _providers_to_run ptr where ptr.npi = dp.npi);

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
                    NOT EXISTS( SELECT
                                    1
                                FROM
                                    _providers_to_run ptr
                                WHERE
                                    ptr.npi = gp.npi )
                RETURNING * )
DELETE
FROM
    dpoc_practitioner_group_patients pgp
WHERE
    EXISTS( SELECT 1 FROM archived a WHERE a.id = pgp.id );



------------------------------------------------------------------------------------------------------------------------
/* Populate practitioner group patients */
------------------------------------------------------------------------------------------------------------------------
-- clean out any not pract group pats that weren't successful


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

-- create index on dpoc_bulk_export_job_outputs(dpoc_bulk_export_job_id)
SELECT count(*) FROM dpoc_bulk_export_job_outputs;
SELECT count(*)
FROM
    dpoc_practitioners where is_registered;

SELECT *
FROM
    dpoc_practitioners
-- WHERE attribution_group_id = 'b92600c9-30f3-4a1f-a5d4-d41a85debc41';

SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
  AND worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at::DATE >= '2023-08-15'
--       AND inserted_at::DATE >= now()::date
GROUP BY
    1;

SELECT *
FROM
    oban.oban_jobs
WHERE
      worker ~* 'DPOC'
  AND inserted_at::DATE >= '2023-08-08'
--   AND state = 'discarded'
--   and state = 'retryable'
--   and state not in  ('completed')
ORDER BY
    id DESC;

SELECT
    sp.primary_referring_partner_id
  , COUNT(DISTINCT sp.patient_id) nd_patients
  , COUNT(DISTINCT dp.source_id)  nd_dpoc_patients
  , COUNT(DISTINCT dc.patient)    nd_coverage
FROM
    fdw_member_doc.supreme_pizza sp
    LEFT JOIN dpoc_patients dp ON dp.source_id = sp.patient_id
    LEFT JOIN dpoc_coverage dc ON dc.patient = dp.bene_id
WHERE
      sp.is_dpc
  AND sp.primary_referring_partner_id IN (221, 225, 227, 218)
GROUP BY
    1
;

select etl.dpc_load_prd_dpc_eligibility();

SELECT
    'from pizza ---------------->' from_pizza
  , sp.patient_id
  , sp.primary_referring_partner_id
  , sp.program
  , 'dpc elg ---------------->'    from_dpc_elg
  , e.*
FROM
    fdw_member_doc.supreme_pizza sp
        --     LEFT JOIN dpoc_patients dp ON dp.source_id = sp.patient_id
--     LEFT JOIN dpoc_coverage dc ON dc.patient = dp.bene_id
    LEFT JOIN prd.dpc_eligibility e ON e.patient_id = sp.patient_id AND e.most_recent_coverage
WHERE
      sp.is_dpc
  AND sp.primary_referring_partner_id IN (221, 225, 227, 218)
;



SELECT *
FROM
    pg_stat_activity where state != 'idle';
SELECT max(inserted_at)
FROM
    prd.dpc_eligibility;

SELECT *
FROM
    dpoc_patients;
SELECT *
FROM
    dpoc_practitioners;
SELECT *
FROM
    dpoc_practitioner_group_patients;

SELECT *
FROM
    dpoc_bulk_export_jobs order by id desc;

SELECT *
FROM
    dpoc_bulk_export
_job_outputs;

SELECT * FROM dpoc_claims;
SELECT * FROM dpoc_claim_dx;
SELECT * FROM dpoc_claim_lines;
SELECT * FROM dpoc_coverage;

;