-- copy ref to stage and run directly off of patient medications;
-- member doc
-- CREATE TABLE stage.ref_concurrent_meds (
--     id              BIGSERIAL PRIMARY KEY NOT NULL,
--     measure_id      TEXT,
--     measure_version TEXT,
--     table_id        TEXT,
--     value_set_id    TEXT,
--     value_set_item  TEXT,
--     ndc             TEXT,
--     description     TEXT,
--     from_date       DATE,
--     thru_date       DATE,
--     inserted_at     TIMESTAMP             NOT NULL DEFAULT NOW(),
--     updated_at      TIMESTAMP             NOT NULL DEFAULT NOW()
-- );
-- create index on stage.ref_concurrent_meds(ndc);

-- analytics
-- call cb.x_util_rebuild_fdw_stage();
--
-- INSERT
-- INTO
--     fdw_member_doc_stage.ref_concurrent_meds (id, measure_id, measure_version, table_id, value_set_id, value_set_item, ndc, description, from_date, thru_date, inserted_at, updated_at)
-- SELECT
--     vs.id,m.measure_id, m.measure_version, m.table_id, vs.value_set_id, vs.value_set_item, vs.code ndc, vs.description,  vs.from_date, vs.thru_date, now(), now()
-- FROM
--     ref.med_adherence_value_sets vs
--     JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
-- WHERE
--       m.measure_id = 'COB'
--   AND m.measure_version = '2024'
--   AND m.table_id IN ('COB-A', 'COB-B')
--   AND m.is_med = 'Y'
--   AND m.is_exclusion = 'N'
-- UNION
-- SELECT
--     vs.id,m.measure_id, m.measure_version, m.table_id, vs.value_set_id, vs.value_set_item, vs.code ndc, vs.description,  vs.from_date, vs.thru_date, now(), now()
-- FROM
--     ref.med_adherence_value_sets vs
--     JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
-- WHERE
--       m.measure_id = 'POLY-ACH'
--   AND m.measure_version = '2024'
--   AND m.table_id = 'POLY-ACH-A'
--   AND m.is_med = 'Y'
--   AND m.is_exclusion = 'N'
-- ;

-- member doc
-- SELECT * FROM stage.ref_concurrent_meds;


-- drop PROCEDURE sp_build_concurrent_meds(IN _yr integer );
CREATE or replace PROCEDURE stage.sp_build_concurrent_meds(IN _yr INTEGER DEFAULT DATE_PART('year'::TEXT, NOW()))
    LANGUAGE plpgsql
AS
$$
BEGIN
    BEGIN

        DROP TABLE IF EXISTS _cntrls;
        CREATE TEMPORARY TABLE _cntrls AS
--         SELECT
--             (2025 || '-01-01')::DATE boy
--           , (2025 || '-12-31')::DATE eoy
--           , (2025 || '-12-02')::DATE new_measure_cutoff
--           , (2025)                   yr;
          SELECT
            (_yr || '-01-01')::DATE boy
          , (_yr || '-12-31')::DATE eoy
          , (_yr || '-12-02')::DATE new_measure_cutoff
          , _yr                     yr
          ;
------------------------------------------------------------------------------------------------------------------------
/* COB */
------------------------------------------------------------------------------------------------------------------------
        DROP TABLE IF EXISTS _opioids;
        CREATE TEMP TABLE _opioids AS
        SELECT DISTINCT ON (pm.patient_id, pm.start_date)
            pm.patient_id
          , pm.start_date
          , pm.analytics_id
          , LEAST(pm.end_date, ( SELECT eoy FROM _cntrls )) end_date -- cap it to eoy
        FROM
            public.patient_medication_fills pm
            JOIN stage.ref_concurrent_meds vs
                 ON vs.ndc = pm.ndc AND pm.last_filled_date BETWEEN vs.from_date AND vs.thru_date
            JOIN qm_patient_config qpc ON pm.patient_id = qpc.patient_id AND qpc.measure_key = 'cob_concurrent_opioid_benzo'
        WHERE
              pm.last_filled_date BETWEEN ( SELECT boy FROM _cntrls ) AND ( SELECT eoy FROM _cntrls )
          AND vs.measure_id = 'COB'
          AND vs.table_id = 'COB-A' -- opioids
        ORDER BY
          -- take max days supply when same start date
          pm.patient_id, pm.start_date, pm.days_supply DESC;

        CREATE UNIQUE INDEX ON _opioids(analytics_id);
        CREATE INDEX ON _opioids(patient_id);

        DROP TABLE IF EXISTS _benzos;
        CREATE TEMP TABLE _benzos AS
        SELECT DISTINCT ON (pm.patient_id, pm.start_date)
            pm.patient_id
          , pm.analytics_id
          , pm.start_date
          , LEAST(end_date, ( SELECT eoy FROM _cntrls )) end_date
        FROM
            public.patient_medication_fills pm
            JOIN stage.ref_concurrent_meds vs
                 ON vs.ndc = pm.ndc AND pm.last_filled_date BETWEEN vs.from_date AND vs.thru_date
        WHERE
              pm.last_filled_date BETWEEN ( SELECT boy FROM _cntrls ) AND ( SELECT eoy FROM _cntrls )
          AND vs.measure_id = 'COB'
          AND vs.table_id = 'COB-B' -- benzos
        ORDER BY
          -- take max days supply when same start date
          pm.patient_id, pm.start_date, pm.days_supply DESC;

        CREATE UNIQUE INDEX ON _benzos(analytics_id);
        CREATE INDEX ON _benzos(patient_id);


        DROP TABLE IF EXISTS _opioid_pat_groups;
        CREATE TEMP TABLE _opioid_pat_groups AS
        SELECT
            patient_id
          , MIN(start_date)                ipsd
          , COUNT(DISTINCT start_date)     opioid_fill_count
          , SUM(end_date - start_date + 1) opioid_total_days_supply
        -- from PQA: "For multiple opioid claims with different dates of service,
        -- sum the days’ supply for all the prescription claims, regardless of overlapping days’ supply."
        FROM
            _opioids o
        GROUP BY patient_id
        ;
        CREATE UNIQUE INDEX ON _opioid_pat_groups(patient_id);

        DROP TABLE IF EXISTS _most_recent_opioid;
        CREATE TEMP TABLE _most_recent_opioid AS
        SELECT DISTINCT ON (o.patient_id)
            o.patient_id
          , o.start_date                  opioid_last_fill_date
          , o.end_date - o.start_date + 1 opioid_last_days_supply
          , pm.prescriber_name            opioid_last_prescriber
          , pm.prescriber_npi             opioid_last_prescriber_npi
          , pm.ndc                        opioid_last_ndc
        FROM
            _opioids o
            JOIN public.patient_medication_fills pm ON pm.analytics_id = o.analytics_id
        ORDER BY o.patient_id, o.start_date DESC;
        CREATE UNIQUE INDEX ON _most_recent_opioid(patient_id);


        DROP TABLE IF EXISTS _most_recent_benzo;
        CREATE TEMP TABLE _most_recent_benzo AS
        SELECT DISTINCT ON (b.patient_id)
            b.patient_id
          , b.start_date                  benzo_last_fill_date
          , b.end_date - b.start_date + 1 benzo_last_days_supply
          , pm.prescriber_name            benzo_last_prescriber
          , pm.prescriber_npi             benzo_last_prescriber_npi
          , pm.ndc                        benzo_last_ndc
        FROM
            _benzos b
            JOIN public.patient_medication_fills pm ON pm.analytics_id = b.analytics_id
        ORDER BY b.patient_id, b.start_date DESC;
        CREATE UNIQUE INDEX ON _most_recent_benzo(patient_id);


        DROP TABLE IF EXISTS _cob_final;
        CREATE TEMP TABLE _cob_final AS
        SELECT
            opg.patient_id
          , opg.ipsd
          , opg.opioid_fill_count
          , opg.opioid_total_days_supply
          , mro.opioid_last_fill_date
          , mro.opioid_last_days_supply
          , mro.opioid_last_prescriber
          , mro.opioid_last_prescriber_npi
          , mro.opioid_last_ndc
          , mrb.benzo_last_fill_date
          , mrb.benzo_last_days_supply
          , mrb.benzo_last_prescriber
          , mrb.benzo_last_prescriber_npi
          , mrb.benzo_last_ndc
          , opg.opioid_fill_count >= 2
                AND opg.opioid_total_days_supply >= 15
                AND opg.ipsd <= ( SELECT new_measure_cutoff FROM _cntrls )   is_in_measure
          , TRUE                                                             is_active
          , 'cob_concurrent_opioid_benzo'                                    measure_key
          , 'sure_scripts'                                                   measure_source_key
          , ( SELECT yr FROM _cntrls )                                       measure_year
          , NOW()                                                            inserted_at
          , NOW()                                                            updated_at
          , COUNT(DISTINCT b.start_date)                                     benzo_fill_count
          , COUNT(DISTINCT d.date) FILTER ( WHERE b.patient_id IS NOT NULL ) days_overlap
        FROM
            _opioid_pat_groups opg
            JOIN _most_recent_opioid mro ON mro.patient_id = opg.patient_id
            LEFT JOIN _most_recent_benzo mrb ON mrb.patient_id = opg.patient_id
            JOIN _opioids o ON opg.patient_id = o.patient_id
            JOIN public.dates d ON d.date BETWEEN o.start_date AND o.end_date
            LEFT JOIN _benzos b ON opg.patient_id = b.patient_id AND d.date BETWEEN b.start_date AND b.end_date
            JOIN qm_patient_config qpc
                 ON opg.patient_id = qpc.patient_id AND qpc.measure_key = 'cob_concurrent_opioid_benzo'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14;


------------------------------------------------------------------------------------------------------------------------
/* MUTATE */
------------------------------------------------------------------------------------------------------------------------
        INSERT
        INTO
            qm_pm_concurrent_med_metrics (patient_id, measure_key, measure_status_key, measure_year, measure_source_key,
                                          is_active, is_in_measure, is_failed, ipsd, days_overlap,
                                          opioid_total_days_supply, opioid_fill_count, opioid_last_fill_date,
                                          opioid_last_days_supply, opioid_last_prescriber, opioid_last_prescriber_npi,
                                          opioid_last_ndc, benzo_fill_count, benzo_last_fill_date,
                                          benzo_last_days_supply, benzo_last_prescriber, benzo_last_prescriber_npi,
                                          benzo_last_ndc, inserted_at, updated_at)
        SELECT
            patient_id, measure_key, measure_status_key, measure_year, measure_source_key,
            is_active, is_in_measure, is_failed, ipsd, days_overlap,
            opioid_total_days_supply, opioid_fill_count, opioid_last_fill_date,
            opioid_last_days_supply, opioid_last_prescriber, opioid_last_prescriber_npi,
            opioid_last_ndc, benzo_fill_count, benzo_last_fill_date,
            benzo_last_days_supply, benzo_last_prescriber, benzo_last_prescriber_npi,
            benzo_last_ndc, inserted_at, updated_at
        FROM
            ( SELECT *
                   , CASE
                    WHEN is_failed     THEN 'lost'
                    WHEN is_in_measure THEN 'open'
                    END measure_status_key
              FROM
                  ( SELECT
                        f.*
                      , is_in_measure AND benzo_fill_count >= 2 AND days_overlap >= 30 is_failed
                    FROM
                        _cob_final f ) x ) y
        ON CONFLICT (patient_id, measure_key, measure_year)
        DO UPDATE
        SET
            measure_status_key = excluded.measure_status_key,
            measure_source_key = excluded.measure_source_key,
            is_active = excluded.is_active,
            is_in_measure = excluded.is_in_measure,
            is_failed = excluded.is_failed,
            ipsd = excluded.ipsd,
            days_overlap = excluded.days_overlap,
            opioid_total_days_supply = excluded.opioid_total_days_supply,
            opioid_fill_count = excluded.opioid_fill_count,
            opioid_last_fill_date = excluded.opioid_last_fill_date,
            opioid_last_days_supply = excluded.opioid_last_days_supply,
            opioid_last_prescriber = excluded.opioid_last_prescriber,
            opioid_last_prescriber_npi = excluded.opioid_last_prescriber_npi,
            opioid_last_ndc = excluded.opioid_last_ndc,
            benzo_fill_count = excluded.benzo_fill_count,
            benzo_last_fill_date = excluded.benzo_last_fill_date,
            benzo_last_days_supply = excluded.benzo_last_days_supply,
            benzo_last_prescriber = excluded.benzo_last_prescriber,
            benzo_last_prescriber_npi = excluded.benzo_last_prescriber_npi,
            benzo_last_ndc = excluded.benzo_last_ndc,
            updated_at = excluded.updated_at
--         TODO: will need to handle mixing in MCO data at somepoint
        ;
        ------------------------------------------------------------------------------------------------------------------------
        /* END COB */
        ------------------------------------------------------------------------------------------------------------------------

        ------------------------------------------------------------------------------------------------------------------------
        /* POLY
           For the metrics use the furthest progressed drug class
           - order by >= 2 fills, days overlap
           For the meds use the two most recent
        */
        ------------------------------------------------------------------------------------------------------------------------
        DROP TABLE IF EXISTS _aches;
        CREATE TEMP TABLE _aches AS
        SELECT DISTINCT ON (pm.patient_id, pm.start_date, vs.value_set_item)
            pm.patient_id
          , pm.start_date
          , pm.analytics_id
          , LEAST(pm.end_date, ( SELECT eoy FROM _cntrls ))                     end_date
          , LEAST(pm.end_date, ( SELECT eoy FROM _cntrls )) - pm.start_date + 1 days_supply -- used for overlap
          , pm.days_supply                                                      real_days_supply -- used for display downstream
          , pm.drug_description
          , pm.ndc
          , pm.prescriber_name
          , pm.prescriber_npi
          , vs.value_set_id
          , vs.value_set_item
        FROM
            public.patient_medication_fills pm
            JOIN stage.ref_concurrent_meds vs
                 ON vs.ndc = pm.ndc AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
            JOIN public.qm_patient_config qpc
                 ON qpc.patient_id = pm.patient_id AND qpc.measure_key = 'poly_ach_multi_anticholinergic_meds'
        WHERE
              vs.measure_id = 'POLY-ACH'
          AND vs.table_id = 'POLY-ACH-A'
          AND DATE_PART('year', last_filled_date) = ( SELECT yr FROM _cntrls )
        ORDER BY
            pm.patient_id, pm.start_date, vs.value_set_item, pm.days_supply DESC;



        CREATE INDEX ON _aches(patient_id);


        -- check overlap
        DROP TABLE IF EXISTS _poly_overlaps;
        CREATE TEMP TABLE _poly_overlaps AS
        SELECT
            a1.patient_id
          , a1.value_set_item AS                                              ingredient1
          , a2.value_set_item AS                                              ingredient2
          , COUNT(DISTINCT d.date) FILTER ( WHERE a2.patient_id IS NOT NULL ) days_overlap
          , MIN(a1.start_date)                                                med_one_ipsd
          , COUNT(DISTINCT a1.start_date)                                     poly_ach_one_fill_count
          , COUNT(DISTINCT a1.start_date) >= 2                                med_one_two_plus_fills
          , MIN(a2.start_date)                                                med_two_ipsd
          , COUNT(DISTINCT a2.start_date)                                     poly_ach_two_fill_count
        FROM
            _aches a1
            JOIN public.dates d ON d.date BETWEEN a1.start_date AND a1.end_date
            LEFT JOIN _aches a2
                      ON a1.patient_id = a2.patient_id
                          AND a1.value_set_item <> a2.value_set_item
                          AND d.date BETWEEN a2.start_date AND a2.end_date
        GROUP BY 1, 2, 3
        ORDER BY 1;


        -- group by ingredient, to find the two mos recent ingredients
        DROP TABLE IF EXISTS _ache_ing_groups;
        CREATE TEMP TABLE _ache_ing_groups AS
        WITH
            ing_groups AS ( SELECT
                                patient_id
                              , value_set_item
                              , MAX(start_date)            last_filled_date
                              , COUNT(DISTINCT start_date) n_fills
                            FROM
                                _aches
                            GROUP BY patient_id, value_set_item )
          , ordered    AS ( SELECT
                                aig.patient_id
                              , aig.value_set_item
                              , aig.last_filled_date
                              , aig.n_fills
                              , ROW_NUMBER()
                                OVER (PARTITION BY aig.patient_id ORDER BY last_filled_date DESC, n_fills DESC, value_set_item) rn
                            FROM
                                ing_groups aig )
        SELECT
            patient_id
          , value_set_item
          , last_filled_date
          , n_fills
          , rn
        FROM
            ordered
        WHERE
            rn <= 2;

        CREATE UNIQUE INDEX ON _ache_ing_groups(patient_id, value_set_item);

        DROP TABLE IF EXISTS _latest_meds;
        CREATE TEMP TABLE _latest_meds AS
        SELECT
            aig1.patient_id
--           , aig1.n_fills
--           , aig1.value_set_item
--           , aig2.n_fills
--           , aig2.value_set_item
          , a1.start_date       poly_ach_one_last_fill_date
          , a1.real_days_supply poly_ach_one_last_days_supply
          , a1.prescriber_name  poly_ach_one_last_prescriber
          , a1.prescriber_npi   poly_ach_one_last_prescriber_npi
          , a1.ndc              poly_ach_one_last_ndc
          , a2.start_date       poly_ach_two_last_fill_date
          , a2.real_days_supply poly_ach_two_last_days_supply
          , a2.prescriber_name  poly_ach_two_last_prescriber
          , a2.prescriber_npi   poly_ach_two_last_prescriber_npi
          , a2.ndc              poly_ach_two_last_ndc
        FROM
            _ache_ing_groups aig1
            JOIN _aches a1 ON a1.patient_id = aig1.patient_id
                AND a1.value_set_item = aig1.value_set_item
                AND a1.start_date = aig1.last_filled_date
            LEFT JOIN _ache_ing_groups aig2 ON aig1.patient_id = aig2.patient_id AND aig2.rn = 2
            LEFT JOIN _aches a2 ON a2.patient_id = aig2.patient_id
                AND a2.value_set_item = aig2.value_set_item
                AND a2.start_date = aig2.last_filled_date
        WHERE
            aig1.rn = 1;


        -- for metrics, bring it to patient level, note poly med 1 and two are potentially not the meds that
        -- will be on the metric since they might not be the most recent poly drugs
        DROP TABLE IF EXISTS _poly_metrics;
        CREATE TEMP TABLE _poly_metrics AS
        SELECT DISTINCT ON (po.patient_id)
            po.patient_id
          , po.ingredient1
          , po.ingredient2
          , po.days_overlap
          , po.med_one_ipsd                                               ipsd
          , po.poly_ach_one_fill_count
          , po.poly_ach_two_fill_count
          , po.poly_ach_one_fill_count >= 2 AND
            po.med_one_ipsd <= ( SELECT new_measure_cutoff FROM _cntrls ) is_in_measure

          , lm.poly_ach_one_last_fill_date
          , lm.poly_ach_one_last_days_supply
          , lm.poly_ach_one_last_prescriber
          , lm.poly_ach_one_last_prescriber_npi
          , lm.poly_ach_one_last_ndc
          , lm.poly_ach_two_last_fill_date
          , lm.poly_ach_two_last_days_supply
          , lm.poly_ach_two_last_prescriber
          , lm.poly_ach_two_last_prescriber_npi
          , lm.poly_ach_two_last_ndc
          , 'poly_ach_multi_anticholinergic_meds'                         measure_key
          , ( SELECT yr FROM _cntrls )                                    measure_year
          , 'sure_scripts'                                                measure_source_key
          , TRUE                                                          is_active
          , NOW()                                                         inserted_at
          , NOW()                                                         updated_at
        FROM
            _poly_overlaps po
            JOIN _latest_meds lm ON po.patient_id = lm.patient_id
        ORDER BY po.patient_id, po.med_one_two_plus_fills DESC, po.days_overlap DESC, po.med_one_ipsd;

------------------------------------------------------------------------------------------------------------------------
/* MUTATE */
------------------------------------------------------------------------------------------------------------------------
        INSERT
        INTO
            qm_pm_concurrent_med_metrics (patient_id, measure_key, measure_status_key, measure_year,
                                          measure_source_key, is_active, is_in_measure, is_failed,
                                          ipsd, days_overlap, poly_ach_one_fill_count,
                                          poly_ach_one_last_fill_date, poly_ach_one_last_days_supply,
                                          poly_ach_one_last_prescriber, poly_ach_one_last_prescriber_npi,
                                          poly_ach_one_last_ndc, poly_ach_two_fill_count, poly_ach_two_last_fill_date,
                                          poly_ach_two_last_days_supply, poly_ach_two_last_prescriber,
                                          poly_ach_two_last_prescriber_npi, poly_ach_two_last_ndc, inserted_at,
                                          updated_at)
        SELECT
          patient_id, measure_key, measure_status_key, measure_year,
          measure_source_key, is_active, is_in_measure, is_failed,
          ipsd, days_overlap, poly_ach_one_fill_count,
          poly_ach_one_last_fill_date, poly_ach_one_last_days_supply,
          poly_ach_one_last_prescriber, poly_ach_one_last_prescriber_npi,
          poly_ach_one_last_ndc, poly_ach_two_fill_count, poly_ach_two_last_fill_date,
          poly_ach_two_last_days_supply, poly_ach_two_last_prescriber,
          poly_ach_two_last_prescriber_npi, poly_ach_two_last_ndc, inserted_at,
          updated_at
        FROM
            ( SELECT *
                   , CASE WHEN is_failed     THEN 'lost'
                          WHEN is_in_measure THEN 'open'
                    END measure_status_key
              FROM
                  ( SELECT
                        is_in_measure AND poly_ach_two_fill_count >= 2 AND days_overlap >= 30 is_failed
                      , pm.*
                    FROM
                        _poly_metrics pm ) x ) y
        ON CONFLICT (patient_id, measure_key, measure_year)
        DO UPDATE
        SET
          measure_status_key = excluded.measure_status_key,
          measure_source_key = excluded.measure_source_key,
          is_active = excluded.is_active,
          is_in_measure = excluded.is_in_measure,
          is_failed = excluded.is_failed,
          ipsd = excluded.ipsd,
          days_overlap = excluded.days_overlap,
          poly_ach_one_fill_count = excluded.poly_ach_one_fill_count,
          poly_ach_one_last_fill_date = excluded.poly_ach_one_last_fill_date,
          poly_ach_one_last_days_supply = excluded.poly_ach_one_last_days_supply,
          poly_ach_one_last_prescriber = excluded.poly_ach_one_last_prescriber,
          poly_ach_one_last_prescriber_npi = excluded.poly_ach_one_last_prescriber_npi,
          poly_ach_one_last_ndc = excluded.poly_ach_one_last_ndc,
          poly_ach_two_fill_count = excluded.poly_ach_two_fill_count,
          poly_ach_two_last_fill_date = excluded.poly_ach_two_last_fill_date,
          poly_ach_two_last_days_supply = excluded.poly_ach_two_last_days_supply,
          poly_ach_two_last_prescriber = excluded.poly_ach_two_last_prescriber,
          poly_ach_two_last_prescriber_npi = excluded.poly_ach_two_last_prescriber_npi,
          poly_ach_two_last_ndc = excluded.poly_ach_two_last_ndc,
          updated_at = excluded.updated_at
        ;

        INSERT
        INTO
            public.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at,
                              priority, tags, state)
        VALUES
            ('qm_pm_med_adherence', 'MD.QualityMeasures2.Workflows.ConcurrentMedsWorker', '{}', '{}', 0, 2, NOW(), NOW(),
             0, '{}', 'available')
       ;
    END;
END;
$$;



