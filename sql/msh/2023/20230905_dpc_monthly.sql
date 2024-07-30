CREATE OR REPLACE PROCEDURE sp_dpc_stage_monthly()
    LANGUAGE plpgsql
AS
$$
BEGIN
    ------------------------------------------------------------------------------------------------------------------------
    /* Monthly export of full data for all dpc patients  */
    ------------------------------------------------------------------------------------------------------------------------
    -- refresh patients
    call public.sp_dpc_refresh_dpc_patients();

    DROP TABLE IF EXISTS _pats;
    CREATE TEMP TABLE _pats AS
    SELECT distinct dp.source_id patient_id, dp.mbi
    FROM
        public.dpoc_patients dp
    WHERE
          dp.is_active
    ;
--     select count(*) from _pats;
    create UNIQUE INDEX on _pats(patient_id);
    create UNIQUE INDEX on _pats(mbi);

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
              public.dpoc_bulk_export_jobs j
          WHERE
              j.npi IS NOT NULL ) x
        JOIN _pats p on p.mbi = x.mbi
        LEFT JOIN public.dpoc_bulk_export_job_errors je ON x.job_id = je.dpoc_bulk_export_job_id AND je.mbi = x.mbi
        Left join public.dpoc_practitioners dp on dp.npi = x.npi
    ORDER BY
        x.npi, x.mbi, x.inserted_at DESC;

    CREATE INDEX ON _last_export_results(mbi);
    CREATE INDEX ON _last_export_results(npi);


    -- remove folks that have been exported EOB recently. Due to provider limit we have to do multiple monthly runs
    -- only delete successfully exported, likely have room to attempt failures again with different npi
    DELETE FROM _pats p WHERE exists(select 1 from _last_export_results ler where ler.mbi = p.mbi and not ler.has_export_error and ler.export_type like '%ExplanationOfBenefit%' and ler.last_exported_at >= now() - '1 week'::interval);

--     select * from _pats p
--     join _last_export_results ler on ler.mbi = p.mbi and not ler.has_export_error
--     SELECT * FROM dpoc_patients where mbi = '1ER0FF6CW09';
--     select count(*) from dpoc_patients where mbis isnull;



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

-- remove patients that don't need spin cycle
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        not EXISTS ( SELECT 1 FROM _pats p WHERE p.mbi = ppp.mbi);


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

    DELETE FROM _possible_phys WHERE npi ISNULL;


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

-- SELECT count(distinct mbi) FROM _possible_phys pp where not exists(select 1 from _assigned_patients ap where ap.mbi = pp.mbi);
-- 1111

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

INSERT
INTO
    public.dpoc_practitioners (npi, first_name, last_name, inserted_at, updated_at, source_id, dpoc_provider_org_id, is_active)
SELECT
    ptr.npi
  , mp.first_name
  , mp.last_name
  , NOW()
  , NOW()
  , mp.id
  , 1
  , TRUE
FROM
    _providers_to_run ptr
    JOIN fdw_member_doc.msh_physicians mp ON ptr.npi = mp.npi::TEXT
WHERE
    NOT EXISTS( SELECT 1 FROM dpoc_practitioners dp WHERE dp.npi = ptr.npi )
;

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

-- No need to selectively add or remove since the roster is stored on the job
TRUNCATE dpoc_practitioner_group_patients;
-- WITH
--     archived AS (
--         INSERT
--             INTO
--                 dpoc_practitioner_group_patients_history(id, npi, mbi, is_registered, last_refreshed_at,
--                                                          dpoc_provider_org_id, archived_at, inserted_at, updated_at)
--                 SELECT
--                     id
--                   , npi
--                   , mbi
--                   , is_registered
--                   , last_refreshed_at
--                   , dpoc_provider_org_id
--                   , NOW() archived_at
--                   , inserted_at
--                   , updated_at
--                 FROM
--                     dpoc_practitioner_group_patients gp
--                 WHERE
--                     NOT EXISTS( SELECT
--                                     1
--                                 FROM
--                                     _providers_to_run ptr
--                                 WHERE
--                                     ptr.npi = gp.npi )
--                 RETURNING * )
-- DELETE
-- FROM
--     dpoc_practitioner_group_patients pgp
-- WHERE
--     EXISTS( SELECT 1 FROM archived a WHERE a.id = pgp.id );


------------------------------------------------------------------------------------------------------------------------
/* Populate practitioner group patients */
------------------------------------------------------------------------------------------------------------------------
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
call sp_dpc_stage_spin_cycle();

create table junk.still_need_to_run_dpc_20230905 as
SELECT ap.*
FROM
    _assigned_patients ap
    left join _providers_to_run r on r.npi = ap.npi
where r.npi isnull
;
SELECT count(*)
FROM
    junk.still_need_to_run_dpc_20230905;

SELECT count(*)
FROM
    _pats; -- 42522
SELECT count(*) -- 67165
FROM
    dpoc_practitioner_group_patients;

SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
  AND worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at::DATE >= '2023-09-05'
--       AND inserted_at::DATE >= now()::date
GROUP BY
    1;


SELECT *
FROM
    oban.oban_jobs
WHERE
      worker ~* 'DPOC'
  AND inserted_at::DATE >= '2023-09-05'
  AND state = 'discarded'
--   and state = 'retryable'
--   and state not in  ('completed')
ORDER BY
    id DESC;

select etl.dpc_load_prd_dpc_eligibility();
select etl.dpc_eligibility_to_coop_current_coverage();
SELECT *
FROM
    pg_stat_activity where state != 'idle';