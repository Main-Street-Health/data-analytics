CREATE PROCEDURE qm_pm_med_adh_process(IN _yr integer DEFAULT date_part('year'::text, now()))
    LANGUAGE plpgsql
AS
$$
    DECLARE unprocess_handoff_id bigint; message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN
    BEGIN
        /*
        ------------------------------------------------------------------------------------------------------------
        -- Author : Brendon 2024-01-31
        -- Description: Take staged analytics data and drive coop workflows
            -- STEP #1 Calculate metrics for everyone
            -- STEP #2 Upsert metrics
            -- STEP #3 Flag all measures that need to get staged in the handoff table
              -- any next fill date in the future (compliant) and we've got open tasks/wf
              -- next fill date <= today + 5
              -- any active/inactive measures
            -- STEP #4 insert oban job for coop to process handoffs

        Change Log
        --------------------------------------
        DATE        AUTHOR         DESCRIPTION
        ----------------------------------------------------------------------------------------------------------------
        4/1/2024    BP             Changed exclusions to not make pqm inactive. Now they are tracked with a pqm status

        ----------------------------------------------------------------------------------------------------------------
        */

        -- check to see if downstream coop processing failed to process all of the handoffs. if it did alert and cleanup
        if exists(select 1 from public.qm_pm_med_adh_handoffs where processed_at ISNULL) then
            unprocess_handoff_id  = (select min(id) from public.qm_pm_med_adh_handoffs where processed_at ISNULL);
            PERFORM * FROM audit.fn_create_sms_alerts(array['ae-coop-stage','med-adherence-tasks'],'public.qm_pm_med_adherence_process()','Error: unprocessed med adh handoffs id: ' || unprocess_handoff_id::text);
            update public.qm_pm_med_adh_handoffs set processed_at = now() where processed_at ISNULL;
        end if;

        drop table if exists _controls_qm_pm_med_adh_process;
        create temporary table _controls_qm_pm_med_adh_process as
--         select (2024 || '-01-01')::date boy, (2024 || '-12-31')::date eoy, 2024 yr;
        select (_yr || '-01-01')::date boy, (_yr || '-12-31')::date eoy, _yr yr;

        -- set high cost meds flag
        with max_hc_true as (
            select max(inserted_at) max_ins_at
            from public.patient_medication_fills f
            where f.is_high_cost
        )
        update public.patient_medication_fills pmf
        set is_high_cost = true
        from qm_ref_high_cost_meds hcm
        cross join max_hc_true mht
        where
            not pmf.is_high_cost
          and pmf.inserted_at >= mht.max_ins_at
          and hcm.measure_key = pmf.measure_key
          and pmf.drug_description ~* hcm.med_name;

        update stage.qm_pm_med_adh_mco_measures m
        set is_high_cost = true
        from qm_ref_high_cost_meds hcm
        where
            not m.is_high_cost
          and hcm.measure_key = m.measure_key
          and m.drug_name ~* hcm.med_name;

        -- set is msh physician
        update public.patient_medication_fills pmf
        set is_msh_provider = true
        from public.msh_physicians phys
        where not pmf.is_msh_provider
          and pmf.prescriber_npi = phys.npi::text;

        update stage.qm_pm_med_adh_mco_measures m
        set is_msh_provider = true
            from public.msh_physicians phys
        where not m.is_msh_provider
          and m.prescriber_npi = phys.npi::text;

        -- upd exclusions, removed
        UPDATE qm_pm_med_adh_metrics m
        SET
            is_excluded = FALSE, updated_at = NOW()
        WHERE
              m.is_excluded
          AND NOT EXISTS( SELECT
                              1
                          FROM
                              public.qm_pm_med_adh_exclusions ex
                          WHERE
                                ex.patient_id = m.patient_id
                            AND ex.measure_key = m.measure_key
                            AND ex.year = m.measure_year );

        -- upd exclusions, added
        UPDATE qm_pm_med_adh_metrics m
        SET
            is_excluded = TRUE, updated_at = NOW()
        FROM
            public.qm_pm_med_adh_exclusions ex
        WHERE
              ex.patient_id = m.patient_id
          AND ex.measure_key = m.measure_key
          AND ex.year = m.measure_year
          AND NOT m.is_excluded;

        -- STEP #1 Calculate metrics for everyone
        drop table if exists _our_current_measures;
        create temporary table _our_current_measures as
        select
            patient_id
          , measure_key
          , fill_count
          , ipsd
          , days_covered_to_period_end
          , days_not_covered
          , absolute_fail_date
          , pdc_to_date
          , is_excluded
          , (select ctx.yr from _controls_qm_pm_med_adh_process ctx)                           measure_year
          , 'sure_scripts'                                                                     measure_source_key
          , last_covered_date + 1                                                              next_fill_date
          , now()::date                                                                        calc_to_date
          , now()                                                                              inserted_at
          , now()                                                                              updated_at
          , greatest((absolute_fail_date - greatest(last_covered_date, current_date))::int, 0) adr --  allowable days remaining   ----  --|--(-)[.....]|
          , coalesce(max_days_supply, 0) >= 90                                                 is_on_90_day_supply
        from
            (
                select
                    x.patient_id,
                    x.measure_key,
                    array_length(patient_medication_ids, 1) fill_count,
                    ipsd,
                    days_to_cover_to_date,
                    days_covered_to_date,
                    days_to_cover_to_date - days_covered_to_date  days_not_covered,
                    days_covered_to_period_end,
                    --
                    last_covered_date,
                    (days_covered_to_date       * 1.0 / nullif(days_to_cover_to_date, 0)  )::decimal(16,2) pdc_to_date,  -- proportion of days covered
                    (select ctx.eoy from _controls_qm_pm_med_adh_process ctx)  - (days_needed_thru_eoy - days_covered_to_period_end)::int absolute_fail_date,
                    days_needed_thru_eoy,
                    max_days_supply,
                    ex.analytics_id IS NOT NULL is_excluded
--                     (days_needed_thru_eoy - days_covered_to_period_end)  days_must_cover,
--                     patient_medication_ids,
--                     patient_med_adherence_synth_period_ids
                from (
                    select
                        sp.patient_id,
                        sp.measure_key,
                        min(sp.start_date)                                                                                                                  ipsd,
                        max(end_date)                                                                                                                       last_covered_date,
--                         count(distinct sp.start_date )                                                                                                      fill_count,
--                         array_agg(distinct sp.patient_medication_ids)                                                                                       patient_med_adherence_synth_period_ids,
                        --
                        current_date - min(sp.start_date) + 1                                                                                               days_to_cover_to_date,
                        (((select ctx.eoy from _controls_qm_pm_med_adh_process ctx) - min(sp.start_date)) * .8)::int                                        days_needed_thru_eoy,
                        --
                        count(distinct d.date         ) filter ( where d.date between sp.start_date and least(current_date, sp.end_date))                   days_covered_to_date,
                        count(distinct d.date         ) filter ( where d.date between sp.start_date and sp.end_date                    )                    days_covered_to_period_end,
                        --
                        max(days_supply)                                                                                                                    max_days_supply,
                        ('{' || replace(replace(replace(array_agg(distinct sp.patient_medication_ids::text)::text,'}',''),'{',''),'"','') || '}')::bigint[] patient_medication_ids
                    from
                        public.qm_pm_med_adh_synth_periods sp
                        join public.dates d on d.date between (select ctx.boy from _controls_qm_pm_med_adh_process ctx) and sp.end_date
                        join public.supreme_pizza za on za.patient_id = sp.patient_id and za.is_medication_adherence
                    where
                        sp.start_date between (select ctx.boy from _controls_qm_pm_med_adh_process ctx) and (select ctx.eoy from _controls_qm_pm_med_adh_process ctx)
                    group by 1,2
                ) x
                left join public.qm_pm_med_adh_exclusions ex on ex.patient_id = x.patient_id
                                                            and ex.measure_key = x.measure_key
                                                            and ex.year = (select ctx.yr from _controls_qm_pm_med_adh_process ctx)
        ) y

        ;
        create UNIQUE INDEX _our_current_measures_uidx on _our_current_measures(patient_id, measure_key);


        -- STEP #2 upsert metrics
        INSERT
        INTO
            public.qm_pm_med_adh_metrics (patient_id, measure_key, measure_year, measure_source_key,
                                          fill_count, ipsd, next_fill_date, days_covered_to_period_end,
                                          days_not_covered, absolute_fail_date,
                                          calc_to_date, pdc_to_date, adr, inserted_at, updated_at,
                                          is_on_90_day_supply, is_excluded)
        SELECT
                                          patient_id, measure_key, measure_year, measure_source_key,
                                          fill_count, ipsd, next_fill_date, days_covered_to_period_end,
                                          days_not_covered, absolute_fail_date,
                                          calc_to_date, pdc_to_date, adr, inserted_at, updated_at,
                                          is_on_90_day_supply, is_excluded
        FROM
            _our_current_measures
        ON CONFLICT (patient_id, measure_key, measure_year)
        DO UPDATE
        SET
          measure_source_key = excluded.measure_source_key,
          fill_count = excluded.fill_count,
          ipsd = excluded.ipsd,
          next_fill_date = excluded.next_fill_date,
          days_covered_to_period_end = excluded.days_covered_to_period_end,
          days_not_covered = excluded.days_not_covered,
          absolute_fail_date = excluded.absolute_fail_date,
          calc_to_date = excluded.calc_to_date,
          pdc_to_date = excluded.pdc_to_date,
          adr = excluded.adr,
          failed_last_year = excluded.failed_last_year,
          updated_at = excluded.updated_at,
          is_excluded = excluded.is_excluded,
          is_on_90_day_supply = excluded.is_on_90_day_supply;

        -- turn ss measures to mco if ss data no longer exists but mco does
        -- edge case when ss next fill date is still >= mco next fill date
        update qm_pm_med_adh_metrics m
        set measure_source_key = 'mco', updated_at = now()
        where
            m.measure_source_key = 'sure_scripts'
        and not exists(select 1 from qm_pm_med_adh_synth_periods sp where sp.patient_id = m.patient_id and sp.measure_key = m.measure_key and sp.yr = m.measure_year)
        and exists(select 1 from stage.qm_pm_med_adh_mco_measures mco where mco.patient_id = m.patient_id and mco.measure_key = m.measure_key and mco.measure_year = m.measure_year);


        -- STEP #3 use MCO data to upsert metrics
        -- get latest unprocessed mco data for patients with no SS data that is more recent
        DROP TABLE IF EXISTS _latest_mco_data;
        CREATE TEMP TABLE _latest_mco_data AS
        SELECT DISTINCT ON (m.patient_id, m.measure_key, m.measure_year) m.*
        FROM
            stage.qm_pm_med_adh_mco_measures m
        join public.supreme_pizza sp on sp.patient_id = m.patient_id and sp.is_medication_adherence
        WHERE
              m.measure_year = DATE_PART('year', NOW())
          -- only override ss if the ss next fill is less than the mco next fill
          AND NOT EXISTS(SELECT 1
                         FROM public.qm_pm_med_adh_metrics mam
                         WHERE mam.patient_id = m.patient_id
                           and mam.measure_key = m.measure_key
                           and mam.measure_year = m.measure_year
                           and mam.measure_source_key = 'sure_scripts'
                           and mam.next_fill_date >= m.next_fill_date
                         )
        -- 2024-03-19 BP removed the is processed flag. We ended up in a bad state when an earlier nfd record was not processed but latest one was
--           AND NOT m.is_processed
        ORDER BY
            m.patient_id, m.measure_key, m.measure_year, m.next_fill_date DESC, m.raw_inserted_at DESC
        ;

        -- crupdate metrics with mco data
        INSERT
        INTO
            qm_pm_med_adh_metrics (patient_id, measure_key, measure_year, measure_source_key,
                                   fill_count, ipsd, next_fill_date, absolute_fail_date, calc_to_date, pdc_to_date, adr,
                                   failed_last_year, inserted_at,
                                   updated_at, is_on_90_day_supply)
        SELECT patient_id, measure_key, measure_year, measure_source_key,
               fill_count, ipsd, next_fill_date, absolute_fail_date, calc_to_date, pdc_to_date, adr,
               failed_last_year, inserted_at,
               updated_at, is_on_90_day_supply
        FROM
            ( SELECT
                  patient_id
                , measure_key
                , measure_year
                , 'mco'                                                 measure_source_key
                , fill_count
                , ipsd
                , next_fill_date
                , absolute_fail_date
                , raw_inserted_at::DATE                                 calc_to_date
                , pdc                                                   pdc_to_date
                , case when adr is not null then greatest(adr, 0) end   adr
                , coalesce(is_prev_year_fail, false)                    failed_last_year
                , NOW()                                                 inserted_at
                , NOW()                                                 updated_at
                , coalesce(days_supply,0) >= 90                         is_on_90_day_supply
              FROM
                  _latest_mco_data) x
        ON CONFLICT (patient_id, measure_key, measure_year)
                DO UPDATE
                SET
                  measure_source_key = excluded.measure_source_key,
                  fill_count = excluded.fill_count,
                  ipsd = excluded.ipsd,
                  next_fill_date = excluded.next_fill_date,
                  days_covered_to_period_end = excluded.days_covered_to_period_end,
                  days_not_covered = excluded.days_not_covered,
                  absolute_fail_date = excluded.absolute_fail_date,
                  calc_to_date = excluded.calc_to_date,
                  pdc_to_date = excluded.pdc_to_date,
                  adr = excluded.adr,
                  failed_last_year = excluded.failed_last_year,
                  updated_at = excluded.updated_at,
                  is_on_90_day_supply = excluded.is_on_90_day_supply;


        UPDATE stage.qm_pm_med_adh_mco_measures m
        SET is_processed = TRUE, processed_at = now()
        WHERE NOT m.is_processed;

        -- STEP #4 Flag all measures that need to get staged in the handoff table
        DROP TABLE IF EXISTS _handoffs;
        CREATE TEMP TABLE _handoffs (
            measure_key               TEXT,
            patient_id                BIGINT,
            qm_pm_med_adh_metric_id   BIGINT,
            measure_year              INTEGER,
            is_active_patient_measure BOOLEAN               DEFAULT TRUE NOT NULL,
            is_excluded               BOOLEAN               DEFAULT false NOT NULL,
            measure_source_key        TEXT,
            reason                    TEXT,
            inserted_at               TIMESTAMP(0) NOT NULL DEFAULT NOW()
        );

        create UNIQUE INDEX _ho_temp_uidx on _handoffs(measure_year, measure_key, patient_id);

        -- look for active pqm where synth periods no long exist, set to inactive
        -- need to only run for SS measures where mco measure doesn't exist
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason, is_active_patient_measure )
        SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'Need to inactivate measure due to data removed by SS' reason, false
        FROM
            public.qm_pm_med_adh_metrics m
        join public.supreme_pizza za on za.patient_id = m.patient_id and za.is_medication_adherence
        join public.qm_patient_measures pm on pm.patient_id = m.patient_id
                                          and pm.measure_key = m.measure_key
                                          and pm.operational_year = m.measure_year
                                          and pm.is_active
                                          and pm.measure_source_key = 'sure_scripts'
        where not exists(
            select 1
            from public.qm_pm_med_adh_synth_periods sp
            where sp.patient_id = m.patient_id
              and sp.measure_key = m.measure_key
              and sp.yr = m.measure_year
        )
        and m.measure_source_key = 'sure_scripts'
        ON CONFLICT DO NOTHING
        ;

        -- no longer med adherence in pizza, make the active pqm inactive
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason, is_active_patient_measure)
        SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'is_active_update' reason, false
        FROM
            public.qm_pm_med_adh_metrics m
        join public.supreme_pizza za on za.patient_id = m.patient_id
        join public.qm_patient_measures pm on pm.patient_id = m.patient_id
                                          and pm.measure_key = m.measure_key
                                          and pm.operational_year = m.measure_year
        where
            (NOT za.is_medication_adherence AND pm.is_active) --> inactivate measure, no longer med adh
        ON CONFLICT DO NOTHING
        ;

        -- need to change pqm status to excluded due to new exclusion
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason, is_excluded)
        SELECT DISTINCT
            m.measure_key
          , m.patient_id
          , m.id
          , m.measure_year
          , m.measure_source_key
          , 'Need to make measure excluded due to exclusion' reason
          , true
        FROM
            public.qm_pm_med_adh_metrics m
            left JOIN public.qm_patient_measures pm ON pm.id = m.patient_measure_id
        where m.is_excluded
          and pm.measure_status_key IS DISTINCT FROM 'excluded'
          -- not sure if we want to exclude when we get measure from mco
          and m.measure_source_key = 'sure_scripts'
        ON CONFLICT DO NOTHING;

        -- ADR = 0
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason,
                       is_active_patient_measure)
        SELECT DISTINCT m.measure_key , m.patient_id , m.id , m.measure_year , m.measure_source_key , 'ADR=0' reason
          , TRUE
        FROM
            public.qm_pm_med_adh_metrics m
            JOIN public.supreme_pizza za ON za.patient_id = m.patient_id AND za.is_medication_adherence
            JOIN public.qm_patient_measures pm ON pm.patient_id = m.patient_id
                AND pm.measure_key = m.measure_key
                AND pm.operational_year = m.measure_year
                AND pm.is_active
        WHERE
              m.adr = 0
          AND pm.measure_status_key not in ('one_fill_only_inactive', 'lost_for_year')
          AND NOT m.is_excluded
        ON CONFLICT DO NOTHING
            ;

        -- any next fill date in the future (compliant) and we've got open tasks/wf
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason)
        SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'compliant with unverified wf' reason
        FROM
            public.qm_pm_med_adh_metrics m
        join public.qm_pm_med_adh_wfs wf on m.id = wf.qm_pm_med_adh_metric_id
        JOIN public.qm_patient_measures pm on m.patient_measure_id = pm.id
                                          and pm.is_active
        where not m.is_excluded
          and m.next_fill_date >= now()::date
          and not wf.is_system_verified_closed
        ON CONFLICT DO NOTHING
        ;

        -- next fill date <= today + 5 (non compliant or close or maybe need to check side effects
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason)
        SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'close to fill date' reason
        FROM
            public.qm_pm_med_adh_metrics m
        JOIN public.qm_patient_measures pm on m.patient_measure_id = pm.id
                                          and pm.is_active
        where not m.is_excluded
          and m.next_fill_date < now()::date + '5 days'::interval
        ON CONFLICT DO NOTHING
        ;


        -- make the inactive pqm active
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason, is_active_patient_measure)
        SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'is_active_update' reason, true
        FROM
            public.qm_pm_med_adh_metrics m
        join public.supreme_pizza za on za.patient_id = m.patient_id
        join public.qm_patient_measures pm on pm.patient_id = m.patient_id
                                          and pm.measure_key = m.measure_key
                                          and pm.operational_year = m.measure_year
        where
             NOT pm.is_active --> activate measure
             AND za.is_medication_adherence
             AND not m.is_excluded
             AND (
                 -- sure scripts data
                  exists(select 1
                         from public.qm_pm_med_adh_synth_periods sp
                         where sp.patient_id = m.patient_id
                           and sp.measure_key = m.measure_key
                           and sp.yr = m.measure_year)
               or
                  -- mco data
                  exists(select 1
                         from stage.qm_pm_med_adh_mco_measures mco
                         where mco.patient_id = m.patient_id
                           and mco.measure_key = m.measure_key
                           and mco.measure_year = m.measure_year
                         )
                 )
        ON CONFLICT DO NOTHING
        ;

        -- need to create the measure
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason)
        SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'need to create measure' reason
        FROM
            public.qm_pm_med_adh_metrics m
        join public.supreme_pizza za on za.patient_id = m.patient_id and za.is_medication_adherence
        left join public.qm_patient_measures pm on pm.patient_id = m.patient_id
                                          and pm.measure_key = m.measure_key
                                          and pm.operational_year = m.measure_year
--                                           and pm.is_active
        where not m.is_excluded
          and pm.id isnull
        AND (
                 -- sure scripts data
                  exists(select 1
                         from public.qm_pm_med_adh_synth_periods sp
                         where sp.patient_id = m.patient_id
                           and sp.measure_key = m.measure_key
                           and sp.yr = m.measure_year)
               or
                  -- mco data
                  exists(select 1
                         from stage.qm_pm_med_adh_mco_measures mco
                         where mco.patient_id = m.patient_id
                           and mco.measure_key = m.measure_key
                           and mco.measure_year = m.measure_year
                         )
                 )
        ON CONFLICT DO NOTHING
        ;
--         select * FROM _handoffs;


------------------------------------------------------------------------------------------------------------------------
/* MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE */
------------------------------------------------------------------------------------------------------------------------

        INSERT
        INTO
            qm_pm_med_adh_handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year,
                                    is_active_patient_measure, measure_source_key, reason, inserted_at)
        SELECT
            h.measure_key, h.patient_id, h.qm_pm_med_adh_metric_id, h.measure_year, h.is_active_patient_measure, h.measure_source_key, h.reason, h.inserted_at
        FROM
            _handoffs h
        -- limit it to pilot
        join supreme_pizza sp on sp.patient_id = h.patient_id and (sp.is_medication_adherence or not h.is_active_patient_measure or h.is_excluded)
--         where
--             sp.primary_referring_partner_id in (135,345,328,133,133,266,302,67,159, -- pilot 2/7 v2
-- --             1390,1391,3292,464,1541,1538,463,404,411,238,999,1487,1439,1167,1521,1648,343,343,1389,1351,1069,134,233,306,263,1536,245,346,344,130,1486,889,1284,1337,45,1443,312,310,  -- pilot 2/12 v3
--             1390,1391,3292,464,1541,1538,463,404,411,238,999,1487,1439,1167,1521,1648,343,343,1389,1351,1069,134,233,263,1536,245,346,344,130,1486,889,1284,1337,45,1443,  -- pilot 2/12 v4
--             169                                    -- pilot 2/13 v4
--                                                )
--             sp.primary_referring_partner_id in (135,345,328,133,133,266,302,67,159, -- pilot 2/7 v2
--                                                   1390,1391,3292,464,1541,1538,463,404,411,238,999,1487,1439,1167,1521,1648,343,343,1389,1351,1069,134,233,306,263,1536,245,346,344,130,1486,889,1284,1337,45,1443,312,310)  -- pilot 2/12 v3
        ;
--         SELECT * FROM qm_pm_med_adh_handoffs where processed_at ISNULL ;

        -- STEP #5 insert oban job for coop to process handoffs
        -- signal to coop to run worker
        INSERT
        INTO
            public.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at,
                              priority, tags, state)
        VALUES
            ('qm_pm_med_adherence', 'MD.QualityMeasures2.Workflows.MedAdhWorker', '{}', '{}', 0, 2, NOW(), NOW(),
             0, '{}', 'available')
--         returning *
       ;


        DROP TABLE IF EXISTS _handoffs;

    EXCEPTION WHEN OTHERS THEN
         /**/     -- raise notice 'x % %', SQLERRM, SQLSTATE;
         /**/     GET DIAGNOSTICS stack = PG_CONTEXT;
         /**/     --  RAISE NOTICE E'--- Call Stack ---\n%', stack;
         /**/     GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT,
         /**/                             exception_detail = PG_EXCEPTION_DETAIL,
         /**/                             exception_hint = PG_EXCEPTION_HINT,
         /**/                             exception_context = PG_EXCEPTION_CONTEXT;
         /**/     -- raise notice '--> sqlerrm(%) sqlstate(%) mt(%)  ed(%)  eh(%)  stack(%) ec(%)', SQLERRM, SQLSTATE, message_text, exception_detail, exception_hint, stack, exception_context;
         /**/     raise notice '-----';
         /**/     --raise notice ' stck(%)', exception_context;
         /**/     raise notice ' exception_context(%), message_text(%)', exception_context, message_text;
         /**/     raise notice '-----';
         /**/     -------
         /**/     -- GET EXCEPTION INFO
         /**/     error_text = 'Issue building the COOP Med Adherence ( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
         /**/     insert into rpt.error_log(location, error_note)
         /**/     select 'public.qm_pm_med_adherence_process()', error_text;
         /**/     PERFORM * FROM audit.fn_create_sms_alerts(array['ae-coop-stage','med-adherence-tasks'],'public.qm_pm_med_adherence_process()',error_text::text);
                  commit;
         /**/   -------
         /**/   RAISE EXCEPTION 'Error in stage.sp_stp_process_med_adherence_tasks() :: %', error_text;
    end;

END; $$;

ALTER PROCEDURE qm_pm_med_adh_process(INTEGER) OWNER TO postgres;

