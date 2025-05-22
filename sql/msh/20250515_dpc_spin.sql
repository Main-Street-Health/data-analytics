
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
              );

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


-- remove patients never exported
    DELETE
    FROM
        _pats p
    WHERE
        NOT EXISTS( SELECT 1 FROM _last_export_results ler WHERE ler.mbi = p.mbi )
    ;


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


    drop table junk.dpc_out_of_options;
    create table junk.dpc_out_of_options as
    SELECT p.*
    FROM
        _pats p
    where not exists(select 1 from _possible_phys pp where pp.mbi = p.mbi)
    ;
    SELECT *
    FROM
        junk.dpc_out_of_options;

    create table junk.dpc_out_of_options as
    SELECT
        p.patient_id, p.mbi, array_agg(distinct ler.npi) FILTER ( WHERE ler.npi is not null ) npis_tried
    FROM
        _pats p
        join _last_export_results ler  on ler.mbi = p.mbi
    GROUP BY 1,2

    ;
create UNIQUE INDEX on junk.dpc_out_of_options(patient_id);
create UNIQUE INDEX on junk.dpc_out_of_options(mbi);

    SELECT *
    FROM
        junk.dpc_out_of_options;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
INSERT
INTO
    oban.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at, attempted_at,
                    completed_at, attempted_by, discarded_at, priority, tags, meta, cancelled_at, state)
VALUES
    ('dpoc_worker', 'Deus.DPOC.DPOCWorker', '{
      "type": "spin_cycle"
    }', '{}', 0, 5, now(), now(), NULL, NULL, NULL, NULL, 0, '{}', null, NULL, 'available')
returning *
;

SELECT
    errors[attempt]
  , state
  , *
-- state, *
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_worker'
  AND id >= 231128342
-- 188891932
ORDER BY
    id DESC;


SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
-- worker = 'Deus.DPOC.BulkExportAPIWorker'
  AND inserted_at >= '2025-05-01'
GROUP BY
    1;


