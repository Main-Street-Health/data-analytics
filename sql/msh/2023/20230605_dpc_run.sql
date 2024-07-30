--     deleted these providers
-- update dpoc_practitioners set is_active = false, is_registered = false, updated_at = now() where npi  in ( '1417949983', '1306415237', '1437107901', '1245907005', '1710360060', '1144872177', '1013095603', '1316977812', '1306415237', '1417949983', '1437107901', '1043266752', '1871582007', '1366997652', '1952865842', '1558887653', '1093086233', '1215331160', '1396332375', '1073047189', '1225400484', '1316189004', '1730602087', '1144621756', '1528040672', '1033673769', '1952953051', '1275196362', '1295265023', '1265851398', '1356359053', '1245271642', '1104059484', '1376198564', '1174572473', '1407472020', '1710557079', '1710378849', '1760824007', '1013490853', '1144256157', '1184929523', '1275922908', '1699137240', '1053549709', '1184181216', '1346707981', '1336780881', '1164872917', '1124432455', '1326607490', '1447769062', '1699721092', '1861451338', '1558740415', '1710945829', '1033701057', '1982798674', '1740419688', '1407275225', '1174547269', '1134623259', '1366465858', '1023279510', '1720548282', '1154097160', '1003457409', '1467555839', '1548927619', '1114099025', '1518162981', '1437749397', '1639538473', '1336640127', '1326678897', '1124174081', '1801567862', '1417953696', '1265407241', '1558348169', '1295736189', '1316404296', '1265475826', '1083126106', '1528000940', '1467567255', '1710579388', '1891797924', '1477026730', '1508259508', '1144870007', '1114201068', '1316421662', '1649207416', '1043532419', '1144513276', '1316557705', '1497231054', '1104810704', '1598014201' )
-- add these providers
-- DROP TABLE IF EXISTS _new_providers_to_add;
-- CREATE TEMP TABLE _new_providers_to_add AS
-- SELECT *
-- FROM
--     ( VALUES
--
--           ('1508853938'),
--           ('1922304369'),
--           ('1225117088'),
--           ('1396824165'),
--           ('1730101965'),
--           ('1154385151'),
--           ('1043234214'),
--           ('1275702672'),
--           ('1447329313'),
--           ('1902078587'),
--           ('1811195290'),
--           ('1942274394'),
--           ('1689667826'),
--           ('1750686283'),
--           ('1518155761'),
--           ('1760674022'),
--           ('1164041513'),
--           ('1871589309'),
--           ('1336321363'),
--           ('1265559421'),
--           ('1770583742'),
--           ('1396079042'),
--           ('1831163013'),
--           ('1972087724'),
--           ('1497805725'),
--           ('1013109354'),
--           ('1982604948'),
--           ('1790741312'),
--           ('1992322069'),
--           ('1437686474'),
--           ('1639179609'),
--           ('1265621973'),
--           ('1235121963'),
--           ('1457336026'),
--           ('1871536714'),
--           ('1366618373'),
--           ('1275023020'),
--           ('1649262999'),
--           ('1225187164'),
--           ('1609096643'),
--           ('1578806105'),
--           ('1598383861'),
--           ('1619302262'),
--           ('1770908998'),
--           ('1831147289'),
--           ('1467463299'),
--           ('1790891208'),
--           ('1780060913'),
--           ('1295086783'),
--           ('1932410776'),
--           ('1891769212'),
--           ('1558808626'),
--           ('1710546296'),
--           ('1942290358'),
--           ('1851663595'),
--           ('1861487399'),
--           ('1811937303'),
--           ('1114924867'),
--           ('1568045292'),
--           ('1689167736'),
--           ('1700320173'),
--           ('1174930747'),
--           ('1285788521'),
--           ('1184909657'),
--           ('1396179560'),
--           ('1164745261'),
--           ('1275694747'),
--           ('1720091770'),
--           ('1740275031'),
--           ('1386661221'),
--           ('1326565607'),
--           ('1427616903'),
--           ('1518051788'),
--           ('1912326158'),
--           ('1396761474'),
--           ('1407325731'),
--           ('1710537428'),
--           ('1235546839'),
--           ('1942861794'),
--           ('1588698906'),
--           ('1548670532'),
--           ('1902156524'),
--           ('1225050404'),
--           ('1922041409'),
--           ('1578823696'),
--           ('1679901243'),
--           ('1679507149'),
--           ('1154885531'),
--           ('1265815740'),
--           ('1619976024'),
--           ('1669402715'),
--           ('1912992843'),
--           ('1790258721'),
--           ('1659682649'),
--           ('1073735304'),
--           ('1962796466'),
--           ('1053452839'),
--           ('1730182841'),
--           ('1386674711'),
--           ('1740541606'),
--           ('1093796658'),
--           ('1396980009'),
--           ('1790958148'),
--           ('1891749065'),
--           ('1992775878'),
--           ('1851727762'),
--           ('1588183156'),
--           ('1396858882'),
--           ('1568475598'),
--           ('1841586492'),
--           ('1629062807'),
--           ('1659387181'),
--           ('1710935234'),
--           ('1972502789'),
--           ('1932630761'),
--           ('1073153417'),
--           ('1770781841'),
--           ('1669575981'),
--           ('1962054106'),
--           ('1942967500'),
--           ('1154855237'),
--           ('1972537512'),
--           ('1952506230'),
--           ('1598766768'),
--           ('1124061700'),
--           ('1952382384'),
--           ('1043988413'),
--           ('1871972588'),
--           ('1770500365'),
--           ('1841234614'),
--           ('1679927990'),
--           ('1881857498'),
--           ('1629004015'),
--           ('1801851498'),
--           ('1679699375'),
--           ('1164405494'),
--           ('1114299484'),
--           ('1740214873'),
--           ('1821437302'),
--           ('1659033215'),
--           ('1902838527'),
--           ('1689616849'),
--           ('1912134693'),
--           ('1821079260'),
--           ('1487207973'),
--           ('1194203786'),
--           ('1760155121'),
--           ('1942974829') ) x(npi)
--
-- ;
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

    -- updated dpoc patients no longer active based on current roster
    UPDATE dpoc_patients dp
    SET
        is_active = FALSE, updated_at = now()
    WHERE
        dp.is_active
        and NOT EXISTS(SELECT 1 FROM _dpc_coop_roster cr WHERE cr.patient_id = dp.source_id);

    -- updated dpoc patients no activebased on current roster
    UPDATE dpoc_patients dp
    SET
        is_active = true, updated_at = now()
    WHERE
        not dp.is_active
        and  EXISTS(SELECT 1 FROM _dpc_coop_roster cr WHERE cr.patient_id = dp.source_id);

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
