CREATE PROCEDURE qm_pm_med_adh_process(_yr int default date_part('year', now()))
    LANGUAGE plpgsql
AS
$$
    DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
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

        Change Log
        --------------------------------------
        DATE        AUTHOR         DESCRIPTION
        ----------------------------------------------------------------------------------------------------------------

        ----------------------------------------------------------------------------------------------------------------
        */
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





        -- set is msh physician
        with max_msh_prov_true as (
            select max(inserted_at) max_ins_at
            from public.patient_medication_fills f
            where f.is_msh_provider
        )
        update public.patient_medication_fills pmf
        set is_msh_provider = true
        from public.msh_physicians phys
         cross join max_msh_prov_true mpt
        where not pmf.is_msh_provider
          and pmf.inserted_at >= mpt.max_ins_at
          and pmf.prescriber_npi = phys.npi::text;


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
          , (select ctx.yr from _controls_qm_pm_med_adh_process ctx)              measure_year
          , 'sure_scripts'                                                        measure_source_key
          , last_covered_date + 1                                                 next_fill_date
          , now()::date                                                           calc_to_date
          , now()                                                                 inserted_at
          , now()                                                                 updated_at
          , (absolute_fail_date - greatest(last_covered_date, current_date))::int adr --  allowable days remaining   ----  --|--(-)[.....]|
          , coalesce(max_days_supply, 0) >= 90                                    is_on_90_day_supply
        from
            (
                select
                    patient_id,
                    measure_key,
                    fill_count,
                    ipsd,
                    days_to_cover_to_date,
                    days_covered_to_date,
                    days_to_cover_to_date - days_covered_to_date  days_not_covered,
                    days_covered_to_period_end,
                    --
                    last_covered_date,
                    (days_covered_to_date       * 1.0 / days_to_cover_to_date  )::decimal(16,2) pdc_to_date,  -- proportion of days covered
                    (select ctx.eoy from _controls_qm_pm_med_adh_process ctx)  - (days_needed_thru_eoy - days_covered_to_period_end)::int absolute_fail_date,
                    days_needed_thru_eoy,
                    max_days_supply
--                     (days_needed_thru_eoy - days_covered_to_period_end)  days_must_cover,
--                     patient_medication_ids,
--                     patient_med_adherence_synth_period_ids
                from (
                    select
                        sp.patient_id,
                        sp.measure_key,
                        min(sp.start_date)                                                                                                                  ipsd,
                        max(end_date)                                                                                                                       last_covered_date,
                        count(distinct sp.start_date )                                                                                                      fill_count,
--                         array_agg(distinct sp.patient_medication_ids)                                                                                       patient_med_adherence_synth_period_ids,
                        --
                        current_date - min(sp.start_date) + 1                                                                                               days_to_cover_to_date,
                        (((select ctx.eoy from _controls_qm_pm_med_adh_process ctx) - min(sp.start_date)) * .8)::int                                        days_needed_thru_eoy,
                        --
                        count(distinct d.date         ) filter ( where d.date between sp.start_date and least(current_date, sp.end_date))                   days_covered_to_date,
                        count(distinct d.date         ) filter ( where d.date between sp.start_date and sp.end_date                    )                    days_covered_to_period_end,
                        --
                        max(days_supply)                                                                                                                    max_days_supply
--                         ('{' || replace(replace(replace(array_agg(distinct sp.patient_medication_ids::text)::text,'}',''),'{',''),'"','') || '}')::bigint[] patient_medication_ids
                    from
                        public.qm_pm_med_adh_synth_periods sp
                        join public.dates d on d.date between (select ctx.boy from _controls_qm_pm_med_adh_process ctx) and sp.end_date
                        join public.supreme_pizza za on za.patient_id = sp.patient_id and za.is_medication_adherence
                    where
                        sp.start_date between (select ctx.boy from _controls_qm_pm_med_adh_process ctx) and current_date
                        and not exists(select 1
                                       FROM public.qm_pm_med_adh_exclusions ex
                                       where ex.patient_id = sp.patient_id
                                       and ex.measure_key = sp.measure_key
                                       and ex.year = sp.yr
                       )
                    group by 1,2
                ) x
        ) y
        ;
        create UNIQUE INDEX _our_current_measures_uidx on _our_current_measures(patient_id, measure_key);

--         SELECT * FROM _our_current_measures WHERE next_fill_date < now() - '5 days'::interval;

        -- STEP #2 upsert metrics
        INSERT
        INTO
            public.qm_pm_med_adh_metrics (patient_id, measure_key, measure_year, measure_source_key,
                                          fill_count, ipsd, next_fill_date, days_covered_to_period_end,
                                          days_not_covered, absolute_fail_date,
                                          calc_to_date, pdc_to_date, adr, inserted_at, updated_at,
                                          is_on_90_day_supply)
        SELECT
                                          patient_id, measure_key, measure_year, measure_source_key,
                                          fill_count, ipsd, next_fill_date, days_covered_to_period_end,
                                          days_not_covered, absolute_fail_date,
                                          calc_to_date, pdc_to_date, adr, inserted_at, updated_at,
                                          is_on_90_day_supply
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
          is_on_90_day_supply = excluded.is_on_90_day_supply;


        -- STEP #3 Flag all measures that need to get staged in the handoff table
        DROP TABLE IF EXISTS _handoffs;
        CREATE TEMP TABLE _handoffs (
            measure_key               TEXT,
            patient_id                BIGINT,
            qm_pm_med_adh_metric_id   BIGINT,
            measure_year              INTEGER,
            is_active_patient_measure BOOLEAN               DEFAULT TRUE NOT NULL,
            measure_source_key        TEXT,
            reason                    TEXT,
            inserted_at               TIMESTAMP(0) NOT NULL DEFAULT NOW()
        );

        create UNIQUE INDEX _ho_temp_uidx on _handoffs(measure_year, measure_key, patient_id);

        -- any next fill date in the future (compliant) and we've got open tasks/wf
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason)
        SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'compliant with unverified wf' reason
        FROM
            public.qm_pm_med_adh_metrics m
        join public.qm_pm_med_adh_wfs wf on m.id = wf.qm_pm_med_adh_metric_id
        where m.next_fill_date >= now()::date
          and not wf.is_system_verified_closed
        ;

        -- next fill date <= today + 5 (non compliant or close or maybe need to check side effects
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason)
        SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'close to fill date' reason
        FROM
            public.qm_pm_med_adh_metrics m
        where
            m.next_fill_date < now()::date + '5 days'::interval
        ;

        -- no longer med adherence, make the active pqm inactive or vice versa
        INSERT
        INTO
            _handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, measure_source_key, reason)
        SELECT distinct
            m.measure_key, m.patient_id, m.id, m.measure_year, m.measure_source_key, 'is_active_update' reason
        FROM
            public.qm_pm_med_adh_metrics m
        join public.supreme_pizza za on za.patient_id = m.patient_id
        join public.qm_patient_measures pm on pm.patient_id = m.patient_id
                                          and pm.measure_key = m.measure_key
                                          and pm.operational_year = m.measure_year
        where (not za.is_medication_adherence and pm.is_active) --> inactivate measure
           or (za.is_medication_adherence and not pm.is_active) --> activate measure
        ON CONFLICT DO NOTHING -- may already be active record from above, don't want two handoffs
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
        where pm.id isnull
        ON CONFLICT DO NOTHING -- may already be active record from above, don't want two handoffs
        ;

--         SELECT * FROM _handoffs h
--                              join supreme_pizza sp on sp.patient_id = h.patient_id and sp.is_medication_adherence
--         where sp.primary_referring_partner_id in (135,345,328,133,133,266,302,67,159) -- pilot 2/7 v2
--         ;

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
        join supreme_pizza sp on sp.patient_id = h.patient_id and sp.is_medication_adherence
        where sp.primary_referring_partner_id in (135,345,328,133,133,266,302,67,159) -- pilot 2/7 v2
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
