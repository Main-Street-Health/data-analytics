-- select * from deus_fn_dpc_stage_monthly_incremental();
CREATE or replace FUNCTION deus_dpc_stage_monthly_incremental_fn() RETURNS INTEGER
    LANGUAGE plpgsql AS
    -- returns the count of excess patients that still need to be run
$$
BEGIN
    ------------------------------------------------------------------------------------------------------------------------
    /* Monthly export of new data for previously exported dpc patients */
    ------------------------------------------------------------------------------------------------------------------------
    -- refresh patients
    CALL public.sp_dpc_refresh_dpc_patients();

    DROP TABLE IF EXISTS _pats;
    CREATE TEMP TABLE _pats AS
    SELECT DISTINCT
        dp.source_id patient_id
      , dp.mbi
    FROM
        public.dpoc_patients dp
    WHERE
        dp.is_active;
--     select count(*) from _pats;
    CREATE UNIQUE INDEX ON _pats(patient_id);
    CREATE UNIQUE INDEX ON _pats(mbi);

    -- get latest export result for every npi-mbi pair
    DROP TABLE IF EXISTS _last_export_results;
    CREATE TEMP TABLE _last_export_results AS
    SELECT DISTINCT ON (x.npi, x.mbi)
        x.npi
      , x.mbi
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
              public.dpoc_bulk_export_jobs j
          WHERE
              j.npi IS NOT NULL ) x
        JOIN _pats p ON p.mbi = x.mbi
        LEFT JOIN public.dpoc_bulk_export_job_errors je ON x.job_id = je.dpoc_bulk_export_job_id AND je.mbi = x.mbi
        LEFT JOIN public.dpoc_practitioners dp ON dp.npi = x.npi
    ORDER BY
        x.npi, x.mbi, x.inserted_at DESC;

    CREATE INDEX ON _last_export_results(mbi);
    CREATE INDEX ON _last_export_results(npi);


    -- remove folks that have been exported EOB recently. Due to provider limit we have to do multiple monthly runs
    -- only delete successfully exported, likely have room to attempt failures again with different npi
    DELETE
    FROM
        _pats p
    WHERE
        EXISTS( SELECT
                    1
                FROM
                    _last_export_results ler
                WHERE
                      ler.mbi = p.mbi
                  AND NOT ler.has_export_error
                  AND ler.export_type LIKE '%ExplanationOfBenefit%'
                  AND ler.last_exported_at >= NOW() - '1 week'::INTERVAL );


    -- remove folks without a successful eob export in the last 3 months
    DELETE
    FROM
        _pats p
    WHERE
        NOT EXISTS( SELECT
                        1
                    FROM
                        _last_export_results ler
                    WHERE
                          ler.mbi = p.mbi
                      AND NOT ler.has_export_error
                      AND ler.export_type LIKE '%ExplanationOfBenefit%'
                      AND ler.last_exported_at >= NOW() - '3 months'::INTERVAL );


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
    WHERE
          sp.is_dpc
      AND sp.patient_mbi IS NOT NULL;

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
          sp.is_dpc
      AND sp.patient_mbi IS NOT NULL
    ON CONFLICT DO NOTHING;

-- Add in previously successful providers
    INSERT
    INTO
        _possible_phys (patient_id, npi, mbi, rank)
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
    ON CONFLICT DO NOTHING;

-- remove phys without elg pats
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        NOT EXISTS ( SELECT 1 FROM _pats p WHERE p.mbi = ppp.mbi );


-- remove  phys that have previously failed
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        EXISTS ( SELECT 1
                 FROM _last_export_results ler
                 WHERE ler.mbi = ppp.mbi AND ler.npi = ppp.npi AND ler.has_export_error );

-- remove  phys that don't have mbi in dpoc patients
    DELETE
    FROM
        _possible_phys ppp
    WHERE
        NOT EXISTS ( SELECT 1 FROM dpoc_patients dp WHERE dp.is_active AND dp.mbi = ppp.mbi );

    DELETE FROM _possible_phys WHERE npi ISNULL;


    ------------------------------------------------------------------------------------------------------------------------
    /* Need to reallocate patients to providers when 1200 patient cap is hit */
    ------------------------------------------------------------------------------------------------------------------------

    -- Create a temporary table to store the assigned patients and their ranks
    DROP TABLE IF EXISTS _assigned_patients;
    CREATE TEMP TABLE _assigned_patients (
        mbi  TEXT,
        npi  TEXT,
        rank INTEGER
    );
    CREATE UNIQUE INDEX ON _assigned_patients(mbi);

    -- Assign patients to providers based on rank, ensuring provider limits
    WITH
        ranked_patients AS ( SELECT
                                 mbi
                               , npi
                               , rank
                               , ROW_NUMBER() OVER (PARTITION BY npi ORDER BY rank) AS npi_row_num
                             FROM
                                 _possible_phys pp )
    INSERT
    INTO
        _assigned_patients (mbi, npi, rank)
    SELECT DISTINCT ON (mbi)
        mbi
      , npi
      , rank
    FROM
        ranked_patients rp
    WHERE
        npi_row_num <= 1200
    ORDER BY mbi, rank, npi_row_num DESC;

    --     SELECT * FROM _assigned_patients ;
--     SELECT npi, count(*) FROM _assigned_patients GROUP BY 1 ORDER BY  2 desc;


    -- Assign remaining patients to providers based on rank, without exceeding limits
    WITH
        prov_below_cnt     AS ( SELECT
                                    npi
                                  , COUNT(*) n
                                FROM
                                    _assigned_patients
                                GROUP BY 1
                                HAVING
                                    COUNT(*) < 1200 )
      , remaining_patients AS ( SELECT
                                    mbi
                                  , npi
                                  , rank
                                  , ROW_NUMBER() OVER (PARTITION BY npi ORDER BY rank) AS row_num
                                FROM
                                    _possible_phys pp
                                WHERE
                                      mbi NOT IN ( SELECT mbi FROM _assigned_patients )
                                  AND EXISTS( SELECT 1 FROM prov_below_cnt pbc WHERE pbc.npi = pp.npi ) )
    INSERT
    INTO
        _assigned_patients (mbi, npi, rank)
    SELECT DISTINCT ON (mbi)
        mbi
      , npi
      , rank
    FROM
        remaining_patients
    WHERE
            row_num <= 1200 - ( SELECT COUNT(*) FROM _assigned_patients WHERE npi = remaining_patients.npi )
    ORDER BY mbi, rank, row_num;

    -- SELECT count(distinct mbi) FROM _possible_phys pp where not exists(select 1 from _assigned_patients ap where ap.mbi = pp.mbi);
-- 1111

------------------------------------------------------------------------------------------------------------------------
/* Update practitioners */
------------------------------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS _providers_to_run;
    CREATE TEMP TABLE _providers_to_run AS
    SELECT
        ap.npi
      , COUNT(*)                                       n
      , MIN(ler.last_exported_at) - '1 week'::INTERVAL oldest_export -- export can take a while, bake in a week buffer
    FROM
        _assigned_patients ap
        JOIN _last_export_results ler ON ler.mbi = ap.mbi
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 750;

------------------------------------------------------------------------------------------------------------------------
/* MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE */
------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        public.dpoc_practitioners (npi, first_name, last_name, inserted_at, updated_at, source_id, dpoc_provider_org_id,
                                   is_active)
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
        NOT EXISTS( SELECT 1 FROM dpoc_practitioners dp WHERE dp.npi = ptr.npi );

-- key bit for incremental
    UPDATE dpoc_practitioners dp
    SET last_exported_at = ptr.oldest_export, updated_at = NOW()
    FROM
        _providers_to_run ptr
    WHERE
          ptr.npi = dp.npi
      AND NOT dp.is_active;

-- make inactive practitioners active
    UPDATE dpoc_practitioners dp
    SET updated_at = NOW(), is_active = TRUE
    FROM
        _providers_to_run ptr
    WHERE
          ptr.npi = dp.npi
      AND NOT dp.is_active;

-- make active practitioners inactive
    UPDATE dpoc_practitioners dp
    SET updated_at = NOW(), is_active = FALSE
    WHERE
          dp.is_active
      AND NOT EXISTS( SELECT 1 FROM _providers_to_run ptr WHERE ptr.npi = dp.npi );


-- No need to selectively add or remove since the roster is stored on the job
    TRUNCATE dpoc_practitioner_group_patients;

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
    ON CONFLICT DO NOTHING;

    -- return how many patients didn't make it due to the 750 provider count limit
    RETURN (SELECT
                     COUNT(DISTINCT mbi)
                 FROM
                     _assigned_patients ap
                 WHERE
                     NOT EXISTS( SELECT 1 FROM _providers_to_run ptr WHERE ptr.npi = ap.npi ));

END;
$$;


CREATE PROCEDURE deus_dpc_stage_monthly_new()
    LANGUAGE plpgsql
AS
$$
BEGIN
    ------------------------------------------------------------------------------------------------------------------------
    /* Monthly export of new data for previously exported dpc patients */
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



    -- remove folks with a successful eob export in the last 3 months. Opposite of the incremental
    DELETE FROM _pats p WHERE exists(select 1 from _last_export_results ler where ler.mbi = p.mbi and not ler.has_export_error and ler.export_type like '%ExplanationOfBenefit%' and ler.last_exported_at >= now() - '3 months'::interval);


--     select count(*) from _pats p
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

-- remove phys without elg pats
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
    ap.npi
  , COUNT(*) n
FROM
    _assigned_patients ap
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

-- key bit for new
update dpoc_practitioners dp
set last_exported_at = null, updated_at = now()
from _providers_to_run ptr
where ptr.npi = dp.npi
and not dp.is_active;

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
