CREATE OR REPLACE FUNCTION dpc_stage_spin_cycle_fn() RETURNS integer
    LANGUAGE plpgsql
AS
$$
BEGIN
    ------------------------------------------------------------------------------------------------------------------------
    /* Focussed on pulling coverage for patients that have not previously had a successful dpc hit  */
    ------------------------------------------------------------------------------------------------------------------------

    -- refresh patients
    call public.sp_dpc_refresh_dpc_patients();
--     SELECT * FROM dpoc_patients where is_active;

    DROP TABLE IF EXISTS _pats;
    CREATE TEMP TABLE _pats AS
    SELECT distinct dp.source_id patient_id, dp.mbi
    FROM
        public.dpoc_patients dp
    WHERE
          dp.is_active
      AND (
                  dp.bene_id IS NULL
                  OR NOT EXISTS( SELECT 1 FROM public.dpoc_coverage dc WHERE dc.patient = dp.bene_id )
              )
    ;
--     CREATE TEMP TABLE _pats AS
--     select j.patient_id, j.patient_mbi mbi from junk.dpc_pats_to_run_for_sean20240125 j -- junk.dpc_pats_to_run_for_sean20240116 j
--     join dpoc_patients dp on dp.source_id = j.patient_id ;
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

    DELETE FROM _pats p
    where exists(select 1 from _last_export_results ler where ler.mbi = p.mbi and ler.last_exported_at > now() - '1 week'::interval)
    ;

    DELETE FROM _last_export_results ler
    where exists(select 1 from _pats p where ler.mbi = p.mbi)
    ;

    UPDATE fdw_member_doc.dpc_patient_physicians p
    SET
        last_sent_to_dpc_at     = ler.last_exported_at
      , last_returned_by_dpc_at = CASE WHEN ler.has_export_error THEN p.last_returned_by_dpc_at
                                       ELSE ler.last_exported_at END
      , updated_at              = NOW()
    FROM
        _last_export_results ler
    WHERE
          ler.mbi = p.mbi
      AND ler.npi = p.npi
      AND ler.last_exported_at > p.last_sent_to_dpc_at;


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
      , ph.npi
      , sp.patient_mbi
      , ph.provider_rank
    FROM
        fdw_member_doc.dpc_patient_physicians ph
        JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_mbi = ph.mbi
    where sp.is_dpc
    and exists(select 1
               from fdw_member_doc.msh_physicians mp2
               where mp2.npi::text = ph.npi
               and mp2.first_name is not null
               and mp2.last_name is not null
               )
    ON CONFLICT DO NOTHING;

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

-- SELECT * FROM _possible_phys pp where not exists(select 1 from _assigned_patients ap where ap.mbi = pp.mbi);

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

update dpoc_practitioners dp
set last_exported_at = null, updated_at = now()
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
        -- return how many patients didn't make it due to the 750 provider count limit
    RETURN (SELECT
                     COUNT(DISTINCT mbi)
                 FROM
                     _assigned_patients ap
                 WHERE
                     NOT EXISTS( SELECT 1 FROM _providers_to_run ptr WHERE ptr.npi = ap.npi ));


END;

$$;
