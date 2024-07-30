CREATE OR REPLACE PROCEDURE sp_refresh_dpc_roster_and_attribution()
    LANGUAGE plpgsql
AS
$$
BEGIN


    ------------------------------------------------------------------------------------------------------------------------
    /* Update DPOC patient rosters */
    ------------------------------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS _dpc_coop_roster;
    CREATE TEMP TABLE _dpc_coop_roster AS
    SELECT DISTINCT ON (p.patient_id)
        p.patient_id
      , p.patient_first_name
      , p.patient_last_name
      , p.gender              AS patient_gender
      , p.date_of_birth       AS patient_dob
      , p.date_of_death       AS patient_dod
      , p.address_line1       AS patient_address_1
      , p.address_line2       AS patient_address_2
      , p.address_city        AS patient_city
      , p.address_state       AS patient_state
      , p.address_postal_code AS patient_zip
      , p.mbi
    FROM
        fdw_member_doc_ent.patients p
        JOIN fdw_member_doc.supreme_pizza sp on sp.patient_id = p.patient_id and sp.is_dpc
    WHERE
        p.mbi IS NOT NULL
    order by p.patient_id, sp.latest_cca_date desc
    ;
    create UNIQUE INDEX on _dpc_coop_roster(patient_id);
--     select count(*) from _dpc_coop_roster;

    -- No good solution for dupes, just delete out, 168 on 5/23/23
--     SELECT *
--     FROM
--         _dpc_coop_roster r
--         JOIN ( SELECT mbi, COUNT(*) n FROM _dpc_coop_roster GROUP BY 1 HAVING COUNT(*) > 1 ) dupes
--              ON dupes.mbi = r.mbi
--     ORDER BY r.mbi;
    WITH dupes AS ( SELECT mbi, COUNT(*) n FROM _dpc_coop_roster GROUP BY 1 HAVING COUNT(*) > 1 )
    DELETE
    FROM
        _dpc_coop_roster r3
    WHERE
        EXISTS( SELECT 1 FROM dupes d WHERE d.mbi = r3.mbi );
    create UNIQUE INDEX on _dpc_coop_roster(mbi);

    -- updated dpoc patients based on current roster
    UPDATE dpoc_patients dp
    SET
        is_active = FALSE, updated_at = now()
    WHERE
        dp.is_active
        and NOT EXISTS(SELECT 1 FROM _dpc_coop_roster cr WHERE cr.patient_id = dp.source_id);

    -- new: insert patients
    INSERT
    INTO
        public.dpoc_patients(mbi, address_line_1, address_line_2, city, dob,
                             dod, first_name, gender, last_name, state, zip, inserted_at, updated_at,
                             source_id,
                             dpoc_provider_org_id)
    SELECT
        mbi
      , patient_address_1
      , patient_address_2
      , patient_city
      , patient_dob
      , patient_dod
      , patient_first_name
      , LOWER(patient_gender) gender
      , patient_last_name
      , patient_state
      , patient_zip
      , NOW()                 inserted_at
      , NOW()                 updated_at
      , patient_id
      , 1                     dpoc_org_id -- msh org = 1
    FROM
        _dpc_coop_roster pats
    WHERE
        NOT EXISTS( SELECT 1 FROM dpoc_patients dp WHERE dp.source_id = pats.patient_id )
    ON CONFLICT (mbi) DO NOTHING;


    -- reactivate
    UPDATE dpoc_patients dp
    SET
        is_active = true, updated_at = now()
    WHERE
        not dp.is_active
        and EXISTS(SELECT 1 FROM _dpc_coop_roster cr WHERE cr.patient_id = dp.source_id);
--     select count(*) from dpoc_patients WHERE is_active;
--     select count(*) from fdw_member_doc.supreme_pizza sp join fdw_member_doc_ent.patients p on p.patient_id = sp.patient_id and p.mbi is not null  WHERE sp.is_dpc;

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
      AND NOT EXISTS ( SELECT 1 FROM _last_export_results r2 WHERE r2.mbi = dp.mbi AND NOT r2.has_export_error )
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
        EXISTS ( SELECT 1 FROM _last_export_results ler WHERE ler.mbi = ppp.mbi AND ler.npi = ppp.npi );

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
        EXISTS ( SELECT 1 FROM _registered_providers rp WHERE rp.npi = ppp.npi AND rp.n >= 1000 );

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
    INSERT
    INTO
        _final_pat_pract (patient_id, npi, mbi, rank)
    SELECT DISTINCT ON (ler.mbi)
        p.source_id
      , ler.npi
      , ler.mbi
      , 0
    FROM
        _last_export_results ler
        JOIN dpoc_patients p ON p.mbi = ler.mbi AND p.is_active
    WHERE
        NOT ler.has_export_error
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
                                )
                    RETURNING * )
    DELETE
    FROM
        dpoc_practitioner_group_patients pgp
    WHERE
        EXISTS( SELECT 1 FROM archived a WHERE a.id = pgp.id );

-- make sure no providers above 1k
--     IF EXISTS( SELECT
--                    fpp.npi
--                  , rp.n
--                  , COUNT(DISTINCT fpp.patient_id)        new_pats
--                  , rp.n + COUNT(DISTINCT fpp.patient_id) new_total
--                FROM
--                    _final_pat_pract fpp
--                    JOIN _registered_providers rp ON fpp.npi = rp.npi
--                GROUP BY
--                    fpp.npi, rp.n
--                HAVING
--                    rp.n + COUNT(DISTINCT fpp.patient_id) > 1000
--                ORDER BY
--                    4 DESC )
--     THEN
--         RAISE EXCEPTION
--             USING MESSAGE = 'Providers over 1000 patient limit',
--                 HINT =
--                         'SELECT fpp.npi , rp.n , COUNT(DISTINCT fpp.patient_id)        new_pats , rp.n + COUNT(DISTINCT fpp.patient_id) new_total FROM _final_pat_pract fpp JOIN _registered_providers rp ON fpp.npi = rp.npi GROUP BY fpp.npi, rp.n HAVING rp.n + COUNT(DISTINCT fpp.patient_id) > 1000 ORDER BY 4 DESC';
--     END IF;

--     CREATE TABLE junk.dpoc_pat_pract_20230523 AS
--     SELECT * FROM _final_pat_pract;

-- DROP TABLE IF EXISTS junk.dpoc_npis_to_run_20230523;
--     CREATE TABLE junk.dpoc_npis_to_run_20230523 AS
    INSERT
    INTO
        dpoc_practitioner_group_patients(npi, mbi, inserted_at, updated_at, dpoc_provider_org_id)
    SELECT
        fpp.npi
      , fpp.mbi
      , NOW()
      , NOW()
      , 1
    FROM
        _final_pat_pract fpp
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
          worker ~* 'DPOC'
      AND inserted_at::DATE >= now() - '4 days'::interval
--       AND inserted_at::DATE >= now()::date
    GROUP BY 1;

    SELECT
        *
    FROM
        oban.oban_jobs
    WHERE
          worker ~* 'DPOC'
      AND inserted_at::DATE = NOW()::DATE


    -- ORDER BY
--     id DESC;
;
call sp_refresh_dpc_roster_and_attribution();
