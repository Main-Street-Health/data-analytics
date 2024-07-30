CREATE PROCEDURE sp_med_adherence_load_surescripts_to_coop(IN _sure_scripts_med_history_id bigint)
    LANGUAGE plpgsql
AS
$$
DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;  _latest_md_portals_roster_ts timestamp; _latest_md_portals_file_ts timestamp;
BEGIN

        /*
        ------------------------------------------------------------------------------------------------------------
        -- Author : Brendon & Alan : 2023-03-28
        -- Description: Take surescripts data, build synth periods, build patient_med_adherences
            -- STEP #1 : Build prd.patient_medications
            -- STEP #2 Build Synth batch here
            -- STEP #3 med_adherence
                -- STEP #3.5 CR-UPDATE : prd.patient_med_adherence_year_measures
                -- STEP #3.6 Exclusions
            -- STEP #4 RX fill measures
            -- STEP #5 Update pharmacies table
            -- STEP #6 publish to COOP

        Change Log
        --------------------------------------
        DATE        AUTHOR         DESCRIPTION
        ----------------------------------------------------------------------------------------------------------------
        2023-05-05  BP             Added Pharmacy table population

        ----------------------------------------------------------------------------------------------------------------
        */

        drop table if exists _controls_pts_load_patient_medications_from_surscripts;
        create temporary table _controls_pts_load_patient_medications_from_surscripts as
        select distinct _sure_scripts_med_history_id sure_scripts_med_history_id;


        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        BEGIN

        -- STEP #1 : Build prd.patient_medications ---------------------------------------------------------------------

            --------------------------------------------------------------------------------------------------------------
            -- Store whether a patient got a hit or not, start_date is null when no meds but patient could've been matched
            drop table if exists _pat_hit_forensics;
            create temporary table _pat_hit_forensics as
            select
                x.patient_id,
                case when not x.is_not_found then timestmp end found_at,
                case when x.is_not_found then timestmp end not_found_at
            from (
                select
                    mdh.patient_id::bigint patient_id,
                    bool_and(mdh.note is not distinct from 'Patient Not Found.') is_not_found,
                    coalesce(max(mdh.inserted_at),now()) timestmp
                from
                    public.sure_scripts_med_history_details mdh
                where
                    mdh.sure_scripts_med_history_id = (select ctx.sure_scripts_med_history_id from _controls_pts_load_patient_medications_from_surscripts ctx)
                group by 1
            ) x
            ;

            update prd.sure_scripts_patient_hit u
                set not_found_at = coalesce(phf.not_found_at, u.not_found_at),
                    found_at     = coalesce(phf.found_at    , u.found_at    )
            from
                _pat_hit_forensics phf
            where
                phf.patient_id = u.patient_id
            ;

            insert into prd.sure_scripts_patient_hit(patient_id, found_at, not_found_at)
            select phf.patient_id, found_at, not_found_at
            from _pat_hit_forensics phf
            where not exists (select 1 from prd.sure_scripts_patient_hit ssph where ssph.patient_id = phf.patient_id)
            ;
            ----------------------------------



            -- JAMAL strategy of creating Franken-what-the-fuck prescriptions.. not awesome but effective :) ADH 2023-03-27
            drop table if exists _ss_data;
            create temporary table _ss_data as
            select
                'sure_scripts' src,
                concat_ws('::', patient_id::text, ndc::text, start_date::text, days_supply::text) unique_key,
                patient_id::bigint patient_id,
                ndc,
                start_date,
                days_supply::int                    days_supply,
                --
                max(drug_description)                    drug_description,
                max(start_date + x.days_supply::int - 1) end_date,
                max(quantity_prescribed::numeric)        quantity,
                max(refills_value::int)                  refills_remaining,
                max(prescriber_name_use)                 prescriber_name,
                max(prescriber_npi_id)                   prescriber_npi,
                max(prescriber_phone_number)             prescriber_phone,
                'pharmacy'                               dispenser_type,
                max(pharmacy_name)                       dispenser_name,
                max(pharmacy_npi)                        dispenser_npi,
                max(pharmacy_phone_number)               dispenser_phone,
                max(sold_date)                           sold_date,
                max(last_filled_date)                    last_filled_date,
                max(written_date)                        written_date,
                max(id)                                  last_src_id,
                array_agg(distinct jsonb_build_object('src', 'sure_scripts', 'id', id)) sources
            from (
                select
                    coalesce(sold_date, last_filled_date, written_date) start_date,
                    trim(product_code) ndc,
                    concat_ws(' ', prescriber_first_name, prescriber_last_name) prescriber_name_use,
                    *
                from
                    public.sure_scripts_med_history_details mdh
                where
                    mdh.sure_scripts_med_history_id = (select ctx.sure_scripts_med_history_id from _controls_pts_load_patient_medications_from_surscripts ctx)
                    AND mdh.product_code_qualifier = 'ND'
            ) x
            where
                start_date is not null
                and ndc is not null
                and days_supply is not null
            group by 1,2,3,4,5,6
            ;
            create index idx_ss_data on _ss_data(unique_key);


            update prd.patient_medications pmu
                set
                    src               = sd.src,
                    drug_description  = sd.drug_description,
                    end_date          = sd.end_date,
                    quantity          = sd.quantity,
                    refills_remaining = sd.refills_remaining,
                    prescriber_name   = sd.prescriber_name,
                    prescriber_npi    = sd.prescriber_npi,
                    prescriber_phone  = sd.prescriber_phone,
                    dispenser_type    = sd.dispenser_type,
                    dispenser_name    = sd.dispenser_name,
                    dispenser_npi     = sd.dispenser_npi,
                    dispenser_phone   = sd.dispenser_phone,
                    sold_date         = sd.sold_date,
                    last_filled_date  = sd.last_filled_date,
                    written_date      = sd.written_date,
                    last_src_id       = sd.last_src_id,
                    sources           = pmu.sources || sd.sources,
                    updated_at        = now()
            from
                _ss_data sd
            where
                sd.unique_key = pmu.unique_key
                and (
                       pmu.drug_description  is distinct from sd.drug_description
                    or pmu.end_date          is distinct from sd.end_date
                    or pmu.quantity          is distinct from sd.quantity
                    or pmu.refills_remaining is distinct from sd.refills_remaining
                    or pmu.prescriber_name   is distinct from sd.prescriber_name
                    or pmu.prescriber_npi    is distinct from sd.prescriber_npi
                    or pmu.prescriber_phone  is distinct from sd.prescriber_phone
                    or pmu.dispenser_type    is distinct from sd.dispenser_type
                    or pmu.dispenser_name    is distinct from sd.dispenser_name
                    or pmu.dispenser_npi     is distinct from sd.dispenser_npi
                    or pmu.dispenser_phone   is distinct from sd.dispenser_phone
                    or pmu.sold_date         is distinct from sd.sold_date
                    or pmu.last_filled_date  is distinct from sd.last_filled_date
                    or pmu.written_date      is distinct from sd.written_date
                )
            ;

            insert into prd.patient_medications(
                src, unique_key, patient_id, ndc, drug_description, start_date,
                days_supply, end_date, quantity, refills_remaining, prescriber_name,
                prescriber_npi, prescriber_phone, dispenser_type, dispenser_name, dispenser_phone,
                dispenser_npi, sold_date, last_filled_date, written_date, last_src_id, sources
            )
            select
                src, unique_key, patient_id, ndc, drug_description, start_date,
                days_supply, end_date, quantity, refills_remaining, prescriber_name,
                prescriber_npi, prescriber_phone, dispenser_type, dispenser_name, dispenser_phone,
                dispenser_npi, sold_date, last_filled_date, written_date, last_src_id,
                sources
            from
                _ss_data sd
            where
                not exists(
                             select 1
                             from prd.patient_medications pm
                             where pm.unique_key = sd.unique_key
            )
            ;


    ----------------------------------------------------------------------------------------------------------------
    -- STEP #2 Build Synth batch here ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------

        drop table if exists _btch;
        create temporary table _btch as
        with pats as (
            select
                (select boy from ref.dates d where d.day = current_date) boy,
                (select eoy from ref.dates d where d.day = current_date) eoy,
                (select ctx.sure_scripts_med_history_id from _controls_pts_load_patient_medications_from_surscripts ctx) sure_scripts_med_history_id,
                array_agg(distinct sd.patient_id) patient_ids
            from _ss_data sd
        )
        select
            extract(year from pats.boy) yr,
            fn synth_batch_id
        from
            pats
            cross join prd.fn_build_med_adherence_synthetics(pats.boy, pats.eoy, pats.patient_ids, sure_scripts_med_history_id) fn
        ;


    ----------------------------------------------------------------------------------------------------------------
    -- STEP #3 med_adherence --------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------
        drop table if exists _controls_ptcs_med_adherence_measures;
        create temporary table _controls_ptcs_med_adherence_measures as
        select (yr || '-01-01')::date boy, (yr || '-12-31')::date eoy, b.yr, b.synth_batch_id from _btch b;
--         select ('2023-01-01')::date boy, ('2023-12-31')::date eoy, 2023, 331 synth_batch_id;

        ------------------------------------------------------------------------------------------------------------------------
        /* current non compliant list */
        ------------------------------------------------------------------------------------------------------------------------
        drop table if exists _our_current_measures;
        create temporary table _our_current_measures as
        select
            *,
            (absolute_fail_date - greatest(last_covered_date, current_date))::int adr --  allowable days remaining   ----  --|--(-)[.....]|
        from
            (
                select
                    patient_id,
                    measure_id,
                    nd_fills,
                    ipsd,
                    days_to_cover_to_date,
                    days_covered_to_date,
                    days_covered_to_period_end,
                    --
                    last_covered_date,
                    (days_covered_to_date       * 1.0 / days_to_cover_to_date  )::decimal(16,2) pdc_to_date,  -- proportion of days covered
                    (select ctx.eoy from _controls_ptcs_med_adherence_measures ctx)  - (days_needed_thru_eoy - days_covered_to_period_end)::int absolute_fail_date,
                    days_needed_thru_eoy,
                    (days_needed_thru_eoy - days_covered_to_period_end)  days_must_cover,
                    patient_medication_ids,
                    patient_med_adherence_synth_period_ids
                from (
                    select
                        mp.patient_id,
                        mp.measure_id,
                        min(mp.start_date)                    ipsd,
                        max(end_date)                         last_covered_date,
                        count(distinct mp.start_date )        nd_fills,
                        array_agg(distinct mp.id)             patient_med_adherence_synth_period_ids,
                        --
                        current_date - min(mp.start_date) + 1                                                              days_to_cover_to_date,
                        (((select ctx.eoy from _controls_ptcs_med_adherence_measures ctx) - min(mp.start_date)) * .8)::int days_needed_thru_eoy,
                        --
                        count(distinct d.day         ) filter ( where d.day between mp.start_date and least(current_date, mp.end_date)) days_covered_to_date,
                        count(distinct d.day         ) filter ( where d.day between mp.start_date and mp.end_date                    )  days_covered_to_period_end,
                        --
                        ('{' || replace(replace(replace(array_agg(distinct mp.patient_medication_ids::text)::text,'}',''),'{',''),'"','') || '}')::bigint[] patient_medication_ids
                    from
                        prd.patient_med_adherence_synth_periods mp
                        join ref.dates d on d.day between (select ctx.boy from _controls_ptcs_med_adherence_measures ctx) and mp.end_date
                    where
                        mp.start_date between (select ctx.boy from _controls_ptcs_med_adherence_measures ctx) and current_date
                        and mp.batch_id = (select ctx.synth_batch_id from _controls_ptcs_med_adherence_measures ctx)
                        and exists (select 1 from _ss_data sd where sd.patient_id = mp.patient_id)
                    group by 1,2
                ) x
        ) y
        ;

        /*

            SELECT * FROM _our_current_measures;

            SELECT count(*)
            FROM
                _our_current_measures
            where current_date - last_covered_date  >= 5
            ;

        */

        -- delete out all old records
        delete from prd.patient_med_adherence_measures x
        where
            x.sent_to_coop is false
            and x.year = (select ctx.yr from _controls_ptcs_med_adherence_measures ctx)
            and exists (select 1 from _pat_hit_forensics phf where phf.patient_id = x.patient_id and phf.found_at is not null)
        ;

        -- layer in our measures first as there is more data available
        INSERT INTO prd.patient_med_adherence_measures(
            patient_id, measure_id, year, fill_count, ipsd, next_fill_date,
            absolute_fail_date, is_sure_scripts_measure, patient_medication_ids,
            patient_med_adherence_synth_period_ids, patient_med_adherence_synth_period_batch_id,
            days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date, calc_to_date, days_needed_thru_eoy, pdc_to_date, adr
        )
        select
            patient_id, measure_id, year, fill_count, ipsd, next_fill_date,
            absolute_fail_date, is_sure_scripts_measure, patient_medication_ids,
            patient_med_adherence_synth_period_ids, patient_med_adherence_synth_period_batch_id,
            days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date, calc_to_date, days_needed_thru_eoy, pdc_to_date, adr
        from (
            SELECT
                cm.patient_id
              , cm.measure_id
              , (select ctx.yr from _controls_ptcs_med_adherence_measures ctx) "year"
              , (select ctx.synth_batch_id from _controls_ptcs_med_adherence_measures ctx) patient_med_adherence_synth_period_batch_id
              , cm.nd_fills fill_count
              , cm.ipsd
              , cm.last_covered_date + 1 next_fill_date
              , cm.absolute_fail_date
              , TRUE is_sure_scripts_measure
              , patient_medication_ids
              , patient_med_adherence_synth_period_ids
              , days_covered_to_date
              , days_covered_to_period_end
              , days_to_cover_to_date
              , current_date calc_to_date
              , days_needed_thru_eoy
              , pdc_to_date
              , adr
            FROM
                _our_current_measures cm
        ) x
        where
            -- GUARD: DO NOT create new tasks if cur task created
            -- if the current pat_measure (presumably already sent to coop)
            -- has a next_fill_date in the future of the inbound
            not exists (
                select 1
                from
                    prd.patient_med_adherence_measures cur
                where
                    cur.patient_id         = x.patient_id
                    and cur.measure_id     = x.measure_id
                    and cur.year           = x.year
                    and cur.next_fill_date >= x.next_fill_date
            )
        ;

    --------------------------------------------------------------------------------------------------------------------
    -- STEP #3.5 CR-UPDATE : prd.patient_med_adherence_year_measures
    --------------------------------------------------------------------------------------------------------------------
        drop table if exists _crupdate_pmay;
        create temporary table _crupdate_pmay as
        select
            distinct
            case when pmay.id is not null then 'update'
                                          else 'create'
            end do_action,
            pmam.patient_id,
            pmam.measure_id,
            pmam.year "year",
            pmam.fill_count,
            pmam.ipsd,
            pmam.next_fill_date,
            pmam.days_covered_to_period_end,
            0 days_not_covered,
            pmam.absolute_fail_date,
            pmam.id patient_med_adherence_measure_id,
            pmam.is_sure_scripts_measure,
            pmam.calc_to_date,
            pmam.pdc_to_date,
            pmam.adr,
            false sent_to_coop,
            null::timestamp sent_to_coop_at,
            now() inserted_at,
            now() updated_at
        from
            prd.patient_med_adherence_measures pmam
            left join prd.patient_med_adherence_year_measures pmay on pmay.patient_id     = pmam.patient_id
                                                                      and pmay.measure_id = pmam.measure_id
                                                                      and pmay.year       = pmam.year
        where
            pmam.patient_med_adherence_synth_period_batch_id = (select ctx.synth_batch_id from _controls_ptcs_med_adherence_measures ctx)
        ;

        update prd.patient_med_adherence_year_measures pmay
            set
                fill_count                        = x.fill_count,
                ipsd                              = x.ipsd,
                next_fill_date                    = x.next_fill_date,
                days_covered_to_period_end        = x.days_covered_to_period_end,
                days_not_covered                  = x.days_not_covered,
                absolute_fail_date                = x.absolute_fail_date,
                patient_med_adherence_measure_id  = x.patient_med_adherence_measure_id,
                is_sure_scripts_measure           = x.is_sure_scripts_measure,
                calc_to_date                      = x.calc_to_date,
                pdc_to_date                       = x.pdc_to_date,
                adr                               = x.adr,
                sent_to_coop                      = x.sent_to_coop,
                sent_to_coop_at                   = x.sent_to_coop_at,
                updated_at                        = x.updated_at
        from _crupdate_pmay x
        where x.do_action = 'update'
            and pmay.patient_id = x.patient_id
            and pmay.measure_id = x.measure_id
            and pmay.year = x.year
        ;

        insert into prd.patient_med_adherence_year_measures(
            patient_id, measure_id, year, fill_count, ipsd, next_fill_date, days_covered_to_period_end, days_not_covered,
            absolute_fail_date, patient_med_adherence_measure_id, is_sure_scripts_measure, calc_to_date, pdc_to_date, adr,
            sent_to_coop, sent_to_coop_at, inserted_at, updated_at
        )
        select
            distinct
            patient_id, measure_id, year, fill_count, ipsd, next_fill_date, days_covered_to_period_end, days_not_covered,
            absolute_fail_date, patient_med_adherence_measure_id, is_sure_scripts_measure, calc_to_date, pdc_to_date, adr,
            sent_to_coop, sent_to_coop_at, inserted_at, updated_at
        from _crupdate_pmay x
        where x.do_action = 'create'
        ;
    --------------------------------------------------------------------------------------------------------------------
    -- STEP #3.6 Exclusions --------------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------

        INSERT
        INTO
            prd.patient_med_adherence_exclusions AS ex (patient_id, year, measure_id, exclusion_reasons, exclusion_src)
        SELECT DISTINCT
            pmay.patient_id
          , pmay.year
          , pmay.measure_id
          , '{Insulin Exclusion}'::text[]
          , 'sure_scripts'
        FROM
            prd.patient_med_adherence_year_measures pmay
            JOIN prd.patient_medications mhd
                 ON mhd.patient_id::BIGINT = pmay.patient_id AND DATE_PART('year', mhd.start_date) = pmay.year
            JOIN ref.med_adherence_value_sets vs
                 ON vs.code = mhd.ndc AND mhd.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
            JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
                AND m.measure_id = 'PDC-DR'
                AND m.table_name = 'Insulin Exclusion'
                AND m.is_exclusion = 'Y'
        where pmay.measure_id = 'PDC-DR'
        ON CONFLICT (patient_id, measure_id, year) DO UPDATE
            SET exclusion_reasons = ex.exclusion_reasons || excluded.exclusion_reasons
        WHERE
            'Insulin Exclusion' != ANY (ex.exclusion_reasons)
        ;

    --------------------------------------------------------------------------------------------------------------------
    -- STEP #4 RX fill measures  ----------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------
        INSERT
        INTO
            prd.patient_rx_fill_measures (patient_id, measure_id, year, is_closed, patient_medication_ids, inserted_at, updated_at)
        SELECT
            patient_id
          , mtml.measure_id
          , mtml.yr
          , TRUE
          , ARRAY_AGG(DISTINCT pm.id)
        , now()
        , now()
        FROM
            prd.patient_medications pm
            JOIN ref.hedis_med_list_to_codes mltc ON mltc.code = pm.ndc
                AND mltc.code_system = 'NDC'
                AND mltc.yr = ( SELECT ctx.yr FROM _controls_ptcs_med_adherence_measures ctx )
            JOIN ref.hedis_measure_to_med_list mtml
                 ON mtml.medication_list_name = mltc.medication_list_name AND mtml.yr = mltc.yr
        WHERE
              mtml.measure_id IN ('SPC', 'SPD', 'OMW')
          AND pm.start_date BETWEEN ( SELECT ctx.boy FROM _controls_ptcs_med_adherence_measures ctx ) AND ( SELECT ctx.eoy FROM _controls_ptcs_med_adherence_measures ctx )
        GROUP BY 1, 2, 3, 4
        ON CONFLICT (patient_id, measure_id, year) DO NOTHING
        ;
    ------------------------------------------------------------------------------------------------------------------------
    /* STEP #5 update phamacy table */
    ------------------------------------------------------------------------------------------------------------------------
        DROP TABLE IF EXISTS _pharmacies;
        CREATE TEMP TABLE _pharmacies AS
        SELECT
            ncpdpid
          , pharmacy_npi                           npi
          , LOWER(pharmacy_name)                   name
          , LOWER(pharmacy_address_line_1)         address_line_1
          , LOWER(pharmacy_address_line_2)         address_line_2
          , LOWER(pharmacy_city)                   city
          , LOWER(pharmacy_state)                  state
          , pharmacy_zip                           zip
          , pharmacy_phone_number                  phone_number
          , pharmacy_fax_number                    fax_number
          , MAX(inserted_at)                       max_ins_at
          , MAX(id)                                max_id
          , ARRAY_AGG(DISTINCT source_description) sources
        FROM
            public.sure_scripts_med_history_details mdh
        WHERE
                mdh.sure_scripts_med_history_id = ( SELECT ctx.sure_scripts_med_history_id
                                                    FROM _controls_pts_load_patient_medications_from_surscripts ctx )
          AND   ncpdpid IS NOT NULL
          AND   pharmacy_npi IS NOT NULL
          AND   pharmacy_phone_number IS NOT NULL
        GROUP BY
            ncpdpid
          , pharmacy_npi
          , LOWER(pharmacy_name)
          , LOWER(pharmacy_address_line_1)
          , LOWER(pharmacy_address_line_2)
          , LOWER(pharmacy_city)
          , LOWER(pharmacy_state)
          , pharmacy_zip
          , pharmacy_phone_number
          , pharmacy_fax_number
    ;

        INSERT
        INTO
            prd.sure_scripts_pharmacies as p (ncpdpid, npi, phone_number, name, address_line_1, address_line_2, city, state,
                                         zip, fax_number, sources)
        WITH
            ordered AS ( SELECT *
                              , ROW_NUMBER()
                                OVER (PARTITION BY ncpdpid, npi, phone_number ORDER BY max_ins_at desc, max_id DESC) rn
                         FROM
                             _pharmacies )
        SELECT
            o.ncpdpid
          , o.npi
          , o.phone_number
          , o.name
          , o.address_line_1
          , o.address_line_2
          , o.city
          , o.state
          , o.zip
          , o.fax_number
          , o.sources
        FROM
            ordered o
        WHERE
            o.rn = 1
        ON CONFLICT (ncpdpid, npi, phone_number) DO UPDATE SET
            name = excluded.name
          , address_line_1 = excluded.address_line_1
          , address_line_2 = excluded.address_line_2
          , city = excluded.city
          , state = excluded.state
          , zip = excluded.zip
          , fax_number = excluded.fax_number
          , sources = array(select distinct s from  unnest(excluded.sources || p.sources) s)
          , updated_at = now()
        WHERE
            p.name is distinct from excluded.name OR
            p.address_line_1 is distinct from excluded.address_line_1 OR
            p.address_line_2 is distinct from excluded.address_line_2 OR
            p.city is distinct from excluded.city OR
            p.state is distinct from excluded.state OR
            p.zip is distinct from excluded.zip OR
            p.fax_number is distinct from excluded.fax_number OR
            p.sources is distinct from excluded.sources
        ;


    --------------------------------------------------------------------------------------------------------------------
    -- STEP #6 publish to COOP  ----------------------------------------------------------------------------------------
    --------------------------------------------------------------------------------------------------------------------
        drop table if exists _trasy_is_fun;
        create temporary table _trasy_is_fun as
        with pats as (
            select
                (select year from ref.dates d where d.day = current_date) yr,
                array_agg(distinct sd.patient_id) patient_ids
            from
                _ss_data sd
            where
                -- Hard exclude basssett
                exists (
                    select 1
                    from
                        fdw_member_doc.supreme_pizza sp
                    join fdw_member_doc.referring_partners rp on sp.primary_referring_partner_id = rp.id
                    where
                        sp.patient_id = sd.patient_id
                    and rp.organization_id != 7 -- hard bassett exclusion
                    and sp.is_medication_adherence
                )
        )
        select
            pats.*
        from
            pats
            cross join prd.fn_ptcs_patient_med_adherence(pats.yr, pats.patient_ids)
        ;

    ---------------------------
    ---------------------------
    EXCEPTION WHEN OTHERS THEN

        GET DIAGNOSTICS stack = PG_CONTEXT; GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT, exception_detail = PG_EXCEPTION_DETAIL, exception_hint = PG_EXCEPTION_HINT, exception_context = PG_EXCEPTION_CONTEXT;
        rollback;
        error_text = '(1) Message_Text( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
        PERFORM * FROM audit.fn_create_sms_alert('["de-analytics-etl"]'::jsonb,'etl.sp_med_adherence_load_surescripts_to_coop',error_text::text);
        commit;
        RAISE EXCEPTION 'etl.pts_load_patient_meds_from_sure_scripts :: %', error_text;

    END;
    COMMIT;
    -------

    -- Do this outside of the transaction to ensure staging sproc has access to the data
    drop table if exists _trashy99;
    create temporary table _trashy99 as
    select 1 from dblink_exec('cb_member_doc', 'call stage.sp_stp_process_med_adherence_tasks()');


END;
$$;

ALTER PROCEDURE sp_med_adherence_load_surescripts_to_coop(BIGINT) OWNER TO postgres;

