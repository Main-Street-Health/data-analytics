------------------------------------------------------------------------------------------------------------------------
/* NEXT_SPROC */
------------------------------------------------------------------------------------------------------------------------
CREATE PROCEDURE sp_populate_sure_scripts_panel_patients()
    LANGUAGE plpgsql
AS
$$
DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN
    BEGIN
        DROP TABLE IF EXISTS _potential_patients;
        CREATE TEMP TABLE _potential_patients AS
        SELECT
            sp.patient_id
          , m.id IS NOT NULL  has_measure
          , m.measure_key
          , pm.measure_status_key
          , coalesce(pm.measure_status_key, '') in ('unable_to_reach', 'lost_adr_gt_zero', 'pharmacy_not_found') is_weird_status
          , m.next_fill_date
          , m.adr
          , wf.id IS NOT NULL has_workflow
          , wf.compliance_check_date
          , '2023-01-01'::date        last_panel_created_at
        FROM -- all fdws
             fdw_member_doc.supreme_pizza sp
             JOIN fdw_member_doc.referring_partners rp
                  ON rp.id = sp.primary_referring_partner_id AND rp.organization_id != 7
             LEFT JOIN fdw_member_doc.qm_pm_med_adh_metrics m
                       ON m.patient_id = sp.patient_id AND m.measure_year = DATE_PART('year', NOW())
             LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON wf.qm_pm_med_adh_metric_id = m.id AND wf.is_active
             LEFT JOIN fdw_member_doc.qm_patient_measures pm ON pm.patient_id = sp.patient_id
                 AND pm.measure_key IN
                     ('med_adherence_diabetes', 'med_adherence_hypertension', 'med_adherence_cholesterol')
                 AND pm.operational_year = DATE_PART('year', NOW())
        WHERE
            sp.is_medication_adherence
        and sp.primary_referring_partner_id in (135,345,829,133,266,302,67,162) -- pilot 2/7
        ;
                -- update with analytics data
        UPDATE _potential_patients pp
        SET last_panel_created_at = sspp.inserted_at::DATE
        FROM
            public.sure_scripts_panel_patients sspp
        WHERE
              pp.patient_id = sspp.patient_id
          AND DATE_PART('year', sspp.inserted_at) = DATE_PART('year', NOW());

--         SELECT * FROM _potential_patients;

    -- pilot qa
--         SELECT
--             u.full_name
--           , u.id  user_id
--           , rp.name
--           , rp.id rp_id
--         FROM
--             fdw_member_doc.care_team_members ctm
--             JOIN fdw_member_doc.msh_care_team_referring_partners ctrp ON ctrp.care_team_id = ctm.care_team_id
--             JOIN fdw_member_doc.users u ON ctm.user_id = u.id
--             JOIN fdw_member_doc.referring_partners rp ON ctrp.referring_partner_id = rp.id
--         WHERE
--               role = 'health_navigator'
--           AND ctrp.referring_partner_id IN (135, 345, 829, 133, 266, 302, 67, 162) -- pilot 2/7
--           AND u.deleted_at ISNULL


        DROP TABLE IF EXISTS _patients_to_pull;
        CREATE TEMP TABLE _patients_to_pull (
            patient_id BIGINT NOT NULL,
            reason     TEXT   NOT NULL,
            UNIQUE (patient_id)
        );

    --   - Patients not qualifying for any measure who haven't been pulled in {date range} days
        WITH
            date_ranges AS ( SELECT *
                             FROM
                                 ( VALUES
                                       (1, 3, 30),
                                       (4, 5, 60),
                                       (6, 12, 90) ) x(mnth_start, mnth_end, days) )
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , dr.days || ' day refresh for non med adherence patients'
        FROM
            _potential_patients pp
            JOIN date_ranges dr ON DATE_PART('month', NOW()) BETWEEN dr.mnth_start AND dr.mnth_end
        WHERE
              NOT EXISTS( SELECT 1
                          FROM _potential_patients pp2
                          WHERE pp2.patient_id = pp.patient_id AND pp2.has_measure )
          AND (
                  pp.last_panel_created_at IS NULL
                      OR
                  pp.last_panel_created_at < NOW()::DATE - dr.days
          );


--     --   - Patients with measure med and no open task and 20 <= adr < 30 and max refill date is 2 days from now
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 2 days before expected next fill date when adr < 20: ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.adr < 20
          AND pp.next_fill_date + '2 days'::INTERVAL <= NOW()
          AND NOT has_workflow
          AND NOT is_weird_status
          AND last_panel_created_at < NOW() - '5 days'::INTERVAL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

    --   - Patients with measure med and no open task and 20 <= adr < 30 and max refill date was 2+ days ago
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 2 days after expected next fill date when 20 <= adr < 30: ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND pp.adr <= 29
          AND next_fill_date <= NOW() - '2 days'::INTERVAL
          AND NOT has_workflow
          AND NOT is_weird_status
          AND last_panel_created_at < NOW() - '5 days'::INTERVAL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

    --   - Patients with measure med and no open task and max refill date was 5+ days ago
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 5 days after expected next fill date: ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND pp.adr >= 30
          AND next_fill_date <= NOW() - '5 days'::INTERVAL
          AND NOT has_workflow
          AND NOT is_weird_status
          AND last_panel_created_at < NOW() - '5 days'::INTERVAL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

     --   - Patients with measure in unable to reach - query every ten days
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull every 10 days for unable to reach: ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND next_fill_date <= NOW() - '5 days'::INTERVAL
          AND NOT has_workflow
          AND is_weird_status
          and measure_status_key = 'unable_to_reach'
          AND last_panel_created_at <= NOW() - '10 days'::INTERVAL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

     --   - Patients with measure in lost to reach - query every ten days
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull every 30 days for : ' || String_agg(pp.measure_status_key, ', ') || ' - ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND next_fill_date <= NOW() - '5 days'::INTERVAL
          AND NOT has_workflow
          AND is_weird_status
          and measure_status_key in ('lost_adr_gt_zero', 'pharmacy_not_found')
          AND last_panel_created_at <= NOW() - '30 days'::INTERVAL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

        -- wf exists use compliance check date
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'WF compliance date check: ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND has_workflow
          AND compliance_check_date <= NOW() - '5 days'::INTERVAL
          AND last_panel_created_at < NOW() - '5 days'::INTERVAL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

--         SELECT * FROM _potential_patients pp
--         left join _patients_to_pull ptp on ptp.patient_id = pp.patient_id
--         order by pp.next_fill_date
--         ;
--         SELECT reason, count(*) FROM _patients_to_pull GROUP BY 1

------------------------------------------------------------------------------------------------------------------------
/* MUTATE */
------------------------------------------------------------------------------------------------------------------------

        INSERT
        INTO
            public.sure_scripts_panel_patients (patient_id, sequence, last_name, first_name, middle_name, prefix,
                                                suffix, address_line_1, address_line_2, city, state, zip, dob, gender, npi,
                                                updated_at, inserted_at, reason_for_query)
        SELECT DISTINCT
            ptp.patient_id
          , ROW_NUMBER() OVER (ORDER BY ptp.patient_id)           sequence
          , REGEXP_REPLACE(p.last_name, E'[\\n\\r]+', '', 'g')    last_name
          , REGEXP_REPLACE(p.first_name, E'[\\n\\r]+', '', 'g')   first_name
          , NULL                                                  middle_name
          , NULL                                                  prefix
          , NULL                                                  suffix
          , REGEXP_REPLACE(pa.line1, E'[\\n\\r]+', '', 'g')       address_line_1
          , REGEXP_REPLACE(pa.line2, E'[\\n\\r]+', '', 'g')       address_line_2
          , REGEXP_REPLACE(pa.city, E'[\\n\\r]+', '', 'g')        city
          , REGEXP_REPLACE(pa.state, E'[\\n\\r]+', '', 'g')       state
          , REGEXP_REPLACE(pa.postal_code, E'[\\n\\r]+', '', 'g') zip
          , p.dob
          , LEFT(p.gender, 1)                                     gender
          , coalesce(mp.npi::TEXT, '1023087954')                  npi
          , NOW()                                                 updated_at
          , NOW()                                                 inserted_at
          , ptp.reason
        FROM
            _patients_to_pull ptp
            JOIN fdw_member_doc.patients p ON ptp.patient_id = p.id
            JOIN fdw_member_doc.patient_addresses pa ON p.id = pa.patient_id
            LEFT JOIN fdw_member_doc.patient_contacts pc
                      ON p.id = pc.patient_id AND pc.relationship = 'physician' AND pc.is_primary
            LEFT JOIN fdw_member_doc.msh_physicians mp ON mp.contact_id = pc.contact_id AND mp.npi IS NOT NULL
        WHERE
              -- don't add if patient already exists
              NOT EXISTS(SELECT
                             1
                         FROM
                             public.sure_scripts_panel_patients sspp
                         WHERE
                               sspp.sure_scripts_panel_id ISNULL
                           AND sspp.patient_id = ptp.patient_id)
              and length(p.first_name) >= 2 -- SS requires Two of a person's names (Last Name, First Name, Middle Name) must have 2 or more characters.
              and length(p.last_name) >= 2
        ;
    ---------------------------
    EXCEPTION WHEN OTHERS THEN
        GET DIAGNOSTICS stack = PG_CONTEXT; GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT, exception_detail = PG_EXCEPTION_DETAIL, exception_hint = PG_EXCEPTION_HINT, exception_context = PG_EXCEPTION_CONTEXT;
        rollback;
        error_text = 'MSH Analytics Message_Text( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
        PERFORM * FROM audit.fn_create_sms_alerts(array['ae-analytics-public','de-analytics-public'],'public.sp_populate_sure_scripts_panel_patients',error_text::text);
        commit;
        RAISE EXCEPTION 'public.sp_populate_sure_scripts_panel_patients :: %', error_text;

    END;
    -------

END;
$$;

ALTER PROCEDURE sp_populate_sure_scripts_panel_patients() OWNER TO postgres;

------------------------------------------------------------------------------------------------------------------------
/* NEXT_SPROC */
------------------------------------------------------------------------------------------------------------------------

CREATE PROCEDURE sp_med_adherence_load_surescripts_to_coop(IN _sure_scripts_med_history_id bigint)
    LANGUAGE plpgsql
AS
$$
DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;  _latest_md_portals_roster_ts timestamp; _latest_md_portals_file_ts timestamp;
BEGIN


        /*
        ------------------------------------------------------------------------------------------------------------
        -- Author : Brendon & Alan : 2023-03-28
        -- Rewrite: 2024-01-30 BP
        -- Description: Take surescripts data, build synth periods, build patient_med_adherences
            -- STEP #1 : Build prd.patient_medications
            -- STEP #2 Build Synth batch here
            -- STEP #3 sync patient medications to coop
            -- STEP #4 sync synth periods to coop

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
--         create temporary table _controls_pts_load_patient_medications_from_surscripts as
--         select distinct _sure_scripts_med_history_id sure_scripts_med_history_id;
        create temporary table _controls_pts_load_patient_medications_from_surscripts as
        select 9472::bigint sure_scripts_med_history_id;
--         select * from sure_scripts_med_histories order by id desc;
--     9439
--     9472


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
                min_effective_date,
                case when not x.is_not_found then timestmp end found_at,
                case when x.is_not_found then timestmp end not_found_at
            from (
                select
                    mdh.patient_id::bigint patient_id,
                    bool_and(mdh.note is not distinct from 'Patient Not Found.') is_not_found,
                    coalesce(max(mdh.inserted_at),now())                         timestmp,
                    min(effective_date)                                          min_effective_date -- ss returns 12m of data, last_filled_date >= this date
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
                concat_ws('::', patient_id::text, ndc::text, last_filled_date::text, days_supply::text) unique_key,
                patient_id::bigint patient_id,
                ndc,
                fill_d                                   last_filled_date,
                days_supply::int                         days_supply,
                --
                max(start_date)                          start_date,
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
                max(sold_d)                              sold_date,
                max(written_date)                        written_date,
                max(id)                                  last_src_id,
                array_agg(distinct jsonb_build_object('src', 'sure_scripts', 'id', id)) sources
            from (
                select
--                  NEW (2024-01-08 BP + AH)
                    coalesce(
                        case when sold_d ISNULL then fill_d
                             when sold_d < fill_d then fill_d
                             when sold_d - fill_d <= 10 then sold_d
                             when sold_d - fill_d > 10 then fill_d end,
                        written_date)                                           start_date,
                    concat_ws(' ', prescriber_first_name, prescriber_last_name) prescriber_name_use,
                    trim(product_code)                                          ndc,
                    *
                from
                    (select
                            last_filled_date::date fill_d,
                            sold_date::date        sold_d,
                            *
                     from
                        PUBLIC.sure_scripts_med_history_details mdh
                    WHERE
                        mdh.sure_scripts_med_history_id = (SELECT ctx.sure_scripts_med_history_id FROM _controls_pts_load_patient_medications_from_surscripts ctx)
                        AND mdh.product_code_qualifier = 'ND'
                    ) y
            ) x
            where
                start_date is not null
                and ndc is not null
                and days_supply is not null
            group by 1,2,3,4,5,6
            ;
            create index idx_ss_data on _ss_data(unique_key);

            -- NEW (2024-01-08 BP): Dedupe from https://mainstreetruralhealth.sharepoint.com/:p:/s/MainStreetProduct/EeqwbMOuR9JBlp4e9s2AcIcBj_VEKhoMq7AqOrOuoLrU1w?e=bhAhye
            -- When a SureScripts query occurs and a previously returned fill is no longer there, we will remove that previous fill from our list and ignore it.
                -- (e.g. Metformin 11/1 for 30 days is returned on 11/2. On 12/7, the query has Metformin 11/18 for 30 days and does not return the 11/1 Metformin, we will only use the 11/18 Metformin and will ignore the previously used value for the 11/1).
            -- If two fills happen within the below specified period, we will ignore.
                -- Days supply <= 30, 50%
                -- Days supply >30, use least of 50% multiplied by days supply or 30 days
                -- Examples:
                    -- if a 7 day script for the same NDC is <3.5 days from the previous script, we will ignore.
                    -- if a 30 day script for the same NDC is <15 days from the previous script, we will ignore.
                    -- If a 90 day script for the same NDC is >30 days from the previous script, we will ignore.

        -- existing records that should be covered by the 12m trailing history returned by ss
        DROP TABLE IF EXISTS _existing_patient_medications;
        CREATE TEMP TABLE _existing_patient_medications AS
        SELECT
            pm.id         patient_medication_id
          , pm.patient_id
          , pm.unique_key
          , pm.ndc
          , pm.last_filled_date
          , pm.updated_at
          , 'no_match' ss_match
          , NULL       ss_unique_key
        FROM
            prd.patient_medications pm
        join _pat_hit_forensics phf on phf.patient_id = pm.patient_id
                                   and phf.found_at is not null
                                   and pm.last_filled_date >= phf.min_effective_date
        ;

        -- prep updates
        UPDATE _existing_patient_medications pm
        SET ss_match = 'hard_match', ss_unique_key = sd.unique_key
        FROM
            _ss_data sd
        WHERE
            sd.unique_key = pm.unique_key;


        -- update based on hard or soft match
        DROP TABLE IF EXISTS _crupdated_patient_meds;
        CREATE TEMP TABLE _crupdated_patient_meds AS
        with upd as (
             update prd.patient_medications pmu
                set
                    unique_key        = sd.unique_key,
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
                _existing_patient_medications epm
            join _ss_data sd on sd.unique_key = epm.ss_unique_key and epm.ss_match = 'hard_match'
            where
                epm.patient_medication_id = pmu.id
                and (
                       pmu.drug_description  is distinct from sd.drug_description
                    or pmu.unique_key        is distinct from sd.unique_key
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
            returning id
        )
        select id patient_medication_id, 'update' op
        from upd
        ;


        -- bkup and delete from prd.patient_medications
        WITH
            ins AS (
                INSERT INTO prd.patient_medication_deletions(
                    deleted_record_id, src, unique_key, patient_id, ndc,
                    drug_description, start_date, days_supply, end_date, quantity,
                    refills_remaining, prescriber_name, prescriber_npi,
                    prescriber_phone, dispenser_type, dispenser_name,
                    dispenser_npi, sold_date, last_filled_date, written_date,
                    sources, last_src_id, dispenser_phone)
                SELECT
                    id deleted_record_id, src, unique_key, patient_id, ndc,
                    drug_description, start_date, days_supply, end_date, quantity,
                    refills_remaining, prescriber_name, prescriber_npi,
                    prescriber_phone, dispenser_type, dispenser_name,
                    dispenser_npi, sold_date, last_filled_date, written_date,
                    sources, last_src_id, dispenser_phone
                FROM
                    prd.patient_medications pm
                WHERE
                    EXISTS( SELECT
                                1
                            FROM
                                _existing_patient_medications em
                            WHERE
                                em.ss_match != 'hard_match'
                            and em.patient_medication_id = pm.id
                            )
                RETURNING deleted_record_id )
        , ins2 as (
            insert into _crupdated_patient_meds (patient_medication_id, op)
            select deleted_record_id, 'delete'
            from ins
            returning patient_medication_id
        )
        DELETE
        FROM
            prd.patient_medications pm
        WHERE
            EXISTS( SELECT 1 FROM ins2 i WHERE i.patient_medication_id = pm.id )
        ;
--         SELECT * FROM analytics.prd.patient_medication_deletions;


        with ins as (
            INSERT INTO prd.patient_medications(
                src, unique_key, patient_id, ndc, drug_description, start_date,
                days_supply, end_date, quantity, refills_remaining, prescriber_name,
                prescriber_npi, prescriber_phone, dispenser_type, dispenser_name,
                dispenser_phone,
                dispenser_npi, sold_date, last_filled_date, written_date, last_src_id,
                sources
                )
                SELECT
                   src, unique_key, patient_id, ndc, drug_description, start_date,
                    days_supply, end_date, quantity, refills_remaining, prescriber_name,
                    prescriber_npi, prescriber_phone, dispenser_type, dispenser_name,
                    dispenser_phone,
                    dispenser_npi, sold_date, last_filled_date, written_date, last_src_id,
                    sources
                FROM
                    _ss_data sd
                WHERE
                    NOT EXISTS( SELECT
                                    1
                                FROM
                                    prd.patient_medications pm
                                WHERE
                                    pm.unique_key = sd.unique_key )
                   returning id
                   )
        insert into _crupdated_patient_meds (patient_medication_id, op)
        select id, 'create'
        from ins
        ;


    ----------------------------------------------------------------------------------------------------------------
    -- STEP #2 Build Synth batch here ------------------------------------------------------------------------------
    ----------------------------------------------------------------------------------------------------------------

        -- TODO: Need to run this one final time for 2023, then use the same data and run for 2024
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
    -- STEP #3 sync patient medications to coop
    ----------------------------------------------------------------------------------------------------------------
        DELETE
        FROM
            fdw_member_doc.patient_medication_fills f
            USING _crupdated_patient_meds cpm
        WHERE
              cpm.patient_medication_id = f.analytics_id
          AND cpm.op = 'delete';

        update fdw_member_doc.patient_medication_fills f
        set
--             src               = pm.src,
            drug_description  = pm.drug_description,
            end_date          = pm.end_date,
            quantity          = pm.quantity,
            refills_remaining = pm.refills_remaining,
            prescriber_name   = pm.prescriber_name,
            prescriber_npi    = pm.prescriber_npi,
            prescriber_phone  = pm.prescriber_phone,
            dispenser_type    = pm.dispenser_type,
            dispenser_name    = pm.dispenser_name,
            dispenser_npi     = pm.dispenser_npi,
            dispenser_phone   = pm.dispenser_phone,
            sold_date         = pm.sold_date,
            last_filled_date  = pm.last_filled_date,
            written_date      = pm.written_date,
            src               = 'sure_scripts',
            updated_at        = now()
        from _crupdated_patient_meds cpm
        join prd.patient_medications pm on cpm.patient_medication_id = pm.id
        WHERE
              cpm.patient_medication_id = f.analytics_id
          AND cpm.op = 'update';


        INSERT
        INTO
            fdw_member_doc.patient_medication_fills (analytics_id, patient_id, ndc, drug_description,
                                                     start_date, days_supply, end_date, quantity, refills_remaining,
                                                     prescriber_name, prescriber_npi, prescriber_phone, dispenser_type,
                                                     dispenser_name, dispenser_npi, dispenser_phone, sold_date,
                                                     last_filled_date, written_date, src, received_at, inserted_at,
                                                     updated_at, measure_key)
        SELECT
            pm.id analytics_id
          , patient_id
          , ndc
          , drug_description
          , start_date
          , days_supply
          , end_date
          , quantity
          , refills_remaining
          , prescriber_name
          , prescriber_npi
          , prescriber_phone
          , dispenser_type
          , dispenser_name
          , dispenser_npi
          , dispenser_phone
          , sold_date
          , last_filled_date
          , written_date
          , src
          , pm.updated_at
          , now() inserted_at
          , now() updated_at
          , max(mamm.coop_measure_key) measure_key -- multiple value sets can tie to one ndc but they only tie to one measure. hack to dedupe
        FROM
            _crupdated_patient_meds cpm
            JOIN prd.patient_medications pm ON cpm.patient_medication_id = pm.id
            LEFT JOIN ref.med_adherence_measures m
                JOIN ref.med_adherence_measure_names mamm ON mamm.analytics_measure_id = m.measure_id
                JOIN ref.med_adherence_value_sets vs ON m.value_set_id = vs.value_set_id
                     ON vs.code = pm.ndc
                         AND pm.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
                         AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
                         AND m.is_med = 'Y'
                         AND m.is_exclusion = 'N'
        WHERE
            cpm.op = 'create'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
        ;

    ----------------------------------------------------------------------------------------------------------------
    -- STEP #4 sync synth batches to coop
    ----------------------------------------------------------------------------------------------------------------
    -- not sure how we want to do this one, delete all for the year and replace if patient in _crupdated_patient_meds?
    -- would need to add year concept to synth periods

        DELETE
        FROM
            fdw_member_doc.qm_pm_med_adh_synth_periods sp
        WHERE
              EXISTS( SELECT
                          1
                      FROM
                          _crupdated_patient_meds cpm
                          JOIN prd.patient_medications pm ON cpm.patient_medication_id = pm.id
                      WHERE
                          pm.patient_id = sp.patient_id )
          AND date_part('year', sp.start_date) = ( SELECT yr FROM _btch );

        INSERT
        INTO
            fdw_member_doc.qm_pm_med_adh_synth_periods (analytics_id, patient_id, measure_key,
                                                        batch_id, fn_iteration, is_moved, join_key, days_supply, rn,
                                                        start_date, end_date, overlap_id, overlap_start_date,
                                                        overlap_end_date, value_set_item, og_start_date, og_end_date,
                                                        prev_start_date, prev_days_supply, patient_medication_ids, ndcs,
                                                        inserted_at, updated_at)
        select
            id analytics_id,  patient_id, coop_measure_key measure_key,
            batch_id, fn_iteration, is_moved, join_key, days_supply, rn,
            start_date, end_date, overlap_id, overlap_start_date,
            overlap_end_date, value_set_item, og_start_date, og_end_date,
            prev_start_date, prev_days_supply, patient_medication_ids, ndcs,
            sp.inserted_at, now()
        from prd.patient_med_adherence_synth_periods sp
        join ref.med_adherence_measure_names mamm on mamm.analytics_measure_id = sp.measure_id
        where
        EXISTS( SELECT
                          1
                      FROM
                          _crupdated_patient_meds cpm
                          JOIN prd.patient_medications pm ON cpm.patient_medication_id = pm.id
                      WHERE
                          pm.patient_id = sp.patient_id )
          AND date_part('year', sp.start_date) = ( SELECT yr FROM _btch );
        ;


    -- save rest of measure calcs for the coop side

        INSERT
        INTO
            fdw_member_doc.qm_pm_med_adh_exclusions (analytics_id, patient_id, year, measure_key, patient_measure_id,
                                                     exclusion_reasons, exclusion_src, inserted_at, updated_at)
        SELECT
            pm.id                            analytics_id
          , pm.patient_id
          , ( SELECT yr FROM _btch )
          , mamm.coop_measure_key            measure_key
          , NULL                             patient_measure_id
          , '{Insulin Exclusion}'::TEXT[]
          , 'sure_scripts'
          , NOW()                            inserted_at
          , NOW()                            updated_at
        FROM
            prd.patient_medications pm
            JOIN ref.med_adherence_value_sets vs ON vs.code = pm.ndc
                AND pm.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
            JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
                AND m.measure_id = 'PDC-DR'
                AND m.table_name = 'Insulin Exclusion'
                AND m.is_exclusion = 'Y'
                AND DATE_PART('year', pm.start_date) = ( SELECT yr FROM _btch )
            JOIN ref.med_adherence_measure_names mamm ON mamm.analytics_measure_id = m.measure_id
        where not exists(select 1 from fdw_member_doc.qm_pm_med_adh_exclusions ex where ex.patient_id = pm.patient_id and ex.year = ( SELECT yr FROM _btch ) and ex.measure_key = mamm.coop_measure_key)
        ;



        ------------------------------------------------------------------------------------------------------------------------
        /* END END END
           OLD OLD OLD
           BELOW
       */
        ------------------------------------------------------------------------------------------------------------------------

--     ----------------------------------------------------------------------------------------------------------------
--     -- STEP #3 med_adherence --------------------------------------------------------------------------------------------
--     ------------------------------------------------------------------------------------------------------------------------
--         drop table if exists _controls_ptcs_med_adherence_measures;
--         create temporary table _controls_ptcs_med_adherence_measures as
--         select (yr || '-01-01')::date boy, (yr || '-12-31')::date eoy, b.yr, b.synth_batch_id from _btch b;
-- --         select ('2023-01-01')::date boy, ('2023-12-31')::date eoy, 2023, 331 synth_batch_id;
--
--         ------------------------------------------------------------------------------------------------------------------------
--         /* Measures */
--         ------------------------------------------------------------------------------------------------------------------------
--         drop table if exists _our_current_measures;
--         create temporary table _our_current_measures as
--         select
--             *,
--             (absolute_fail_date - greatest(last_covered_date, current_date))::int adr --  allowable days remaining   ----  --|--(-)[.....]|
--         from
--             (
--                 select
--                     patient_id,
--                     measure_id,
--                     nd_fills,
--                     ipsd,
--                     days_to_cover_to_date,
--                     days_covered_to_date,
--                     days_covered_to_period_end,
--                     --
--                     last_covered_date,
--                     (days_covered_to_date       * 1.0 / days_to_cover_to_date  )::decimal(16,2) pdc_to_date,  -- proportion of days covered
--                     (select ctx.eoy from _controls_ptcs_med_adherence_measures ctx)  - (days_needed_thru_eoy - days_covered_to_period_end)::int absolute_fail_date,
--                     days_needed_thru_eoy,
--                     (days_needed_thru_eoy - days_covered_to_period_end)  days_must_cover,
--                     patient_medication_ids,
--                     patient_med_adherence_synth_period_ids
--                 from (
--                     select
--                         mp.patient_id,
--                         mp.measure_id,
--                         min(mp.start_date)                    ipsd,
--                         max(end_date)                         last_covered_date,
--                         count(distinct mp.start_date )        nd_fills,
--                         array_agg(distinct mp.id)             patient_med_adherence_synth_period_ids,
--                         --
--                         current_date - min(mp.start_date) + 1                                                              days_to_cover_to_date,
--                         (((select ctx.eoy from _controls_ptcs_med_adherence_measures ctx) - min(mp.start_date)) * .8)::int days_needed_thru_eoy,
--                         --
--                         count(distinct d.day         ) filter ( where d.day between mp.start_date and least(current_date, mp.end_date)) days_covered_to_date,
--                         count(distinct d.day         ) filter ( where d.day between mp.start_date and mp.end_date                    )  days_covered_to_period_end,
--                         --
--                         ('{' || replace(replace(replace(array_agg(distinct mp.patient_medication_ids::text)::text,'}',''),'{',''),'"','') || '}')::bigint[] patient_medication_ids
--                     from
--                         prd.patient_med_adherence_synth_periods mp
--                         join ref.dates d on d.day between (select ctx.boy from _controls_ptcs_med_adherence_measures ctx) and mp.end_date
--                     where
--                         mp.start_date between (select ctx.boy from _controls_ptcs_med_adherence_measures ctx) and current_date
--                         and mp.batch_id = (select ctx.synth_batch_id from _controls_ptcs_med_adherence_measures ctx)
--                         and exists (select 1 from _ss_data sd where sd.patient_id = mp.patient_id)
--                     group by 1,2
--                 ) x
--         ) y
--         ;
--
--         /*
--
--             SELECT * FROM _our_current_measures;
--
--             SELECT count(*)
--             FROM
--                 _our_current_measures
--             where current_date - last_covered_date  >= 5
--             ;
--
--         */
--
--         -- delete out all old records
--         delete from prd.patient_med_adherence_measures x
--         where
--             x.sent_to_coop is false
--             and x.year = (select ctx.yr from _controls_ptcs_med_adherence_measures ctx)
--             and exists (select 1 from _pat_hit_forensics phf where phf.patient_id = x.patient_id and phf.found_at is not null)
--         ;
--
--         -- layer in our measures first as there is more data available
--         INSERT INTO prd.patient_med_adherence_measures(
--             patient_id, measure_id, year, fill_count, ipsd, next_fill_date,
--             absolute_fail_date, is_sure_scripts_measure, patient_medication_ids,
--             patient_med_adherence_synth_period_ids, patient_med_adherence_synth_period_batch_id,
--             days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date, calc_to_date, days_needed_thru_eoy, pdc_to_date, adr
--         )
--         select
--             patient_id, measure_id, year, fill_count, ipsd, next_fill_date,
--             absolute_fail_date, is_sure_scripts_measure, patient_medication_ids,
--             patient_med_adherence_synth_period_ids, patient_med_adherence_synth_period_batch_id,
--             days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date, calc_to_date, days_needed_thru_eoy, pdc_to_date, adr
--         from (
--             SELECT
--                 cm.patient_id
--               , cm.measure_id
--               , (select ctx.yr from _controls_ptcs_med_adherence_measures ctx) "year"
--               , (select ctx.synth_batch_id from _controls_ptcs_med_adherence_measures ctx) patient_med_adherence_synth_period_batch_id
--               , cm.nd_fills fill_count
--               , cm.ipsd
--               , cm.last_covered_date + 1 next_fill_date
--               , cm.absolute_fail_date
--               , TRUE is_sure_scripts_measure
--               , patient_medication_ids
--               , patient_med_adherence_synth_period_ids
--               , days_covered_to_date
--               , days_covered_to_period_end
--               , days_to_cover_to_date
--               , current_date calc_to_date
--               , days_needed_thru_eoy
--               , pdc_to_date
--               , adr
--             FROM
--                 _our_current_measures cm
--         ) x
--         where
--             -- GUARD: DO NOT create new tasks if cur task created
--             -- if the current pat_measure (presumably already sent to coop)
--             -- has a next_fill_date in the future of the inbound
--             not exists (
--                 select 1
--                 from
--                     prd.patient_med_adherence_measures cur
--                 where
--                     cur.patient_id         = x.patient_id
--                     and cur.measure_id     = x.measure_id
--                     and cur.year           = x.year
--                     and cur.next_fill_date >= x.next_fill_date
--             )
--         ;
--
--     --------------------------------------------------------------------------------------------------------------------
--     -- STEP #3.5 CR-UPDATE : prd.patient_med_adherence_year_measures
--     --------------------------------------------------------------------------------------------------------------------
--         drop table if exists _crupdate_pmay;
--         create temporary table _crupdate_pmay as
--         select
--             distinct
--             case when pmay.id is not null then 'update'
--                                           else 'create'
--             end do_action,
--             pmam.patient_id,
--             pmam.measure_id,
--             pmam.year "year",
--             pmam.fill_count,
--             pmam.ipsd,
--             pmam.next_fill_date,
--             pmam.days_covered_to_period_end,
--             0 days_not_covered,
--             pmam.absolute_fail_date,
--             pmam.id patient_med_adherence_measure_id,
--             pmam.is_sure_scripts_measure,
--             pmam.calc_to_date,
--             pmam.pdc_to_date,
--             pmam.adr,
--             false sent_to_coop,
--             null::timestamp sent_to_coop_at,
--             now() inserted_at,
--             now() updated_at
--         from
--             prd.patient_med_adherence_measures pmam
--             left join prd.patient_med_adherence_year_measures pmay on pmay.patient_id     = pmam.patient_id
--                                                                       and pmay.measure_id = pmam.measure_id
--                                                                       and pmay.year       = pmam.year
--         where
--             pmam.patient_med_adherence_synth_period_batch_id = (select ctx.synth_batch_id from _controls_ptcs_med_adherence_measures ctx)
--         ;
--
--         update prd.patient_med_adherence_year_measures pmay
--             set
--                 fill_count                        = x.fill_count,
--                 ipsd                              = x.ipsd,
--                 next_fill_date                    = x.next_fill_date,
--                 days_covered_to_period_end        = x.days_covered_to_period_end,
--                 days_not_covered                  = x.days_not_covered,
--                 absolute_fail_date                = x.absolute_fail_date,
--                 patient_med_adherence_measure_id  = x.patient_med_adherence_measure_id,
--                 is_sure_scripts_measure           = x.is_sure_scripts_measure,
--                 calc_to_date                      = x.calc_to_date,
--                 pdc_to_date                       = x.pdc_to_date,
--                 adr                               = x.adr,
--                 sent_to_coop                      = x.sent_to_coop,
--                 sent_to_coop_at                   = x.sent_to_coop_at,
--                 updated_at                        = x.updated_at
--         from _crupdate_pmay x
--         where x.do_action = 'update'
--             and pmay.patient_id = x.patient_id
--             and pmay.measure_id = x.measure_id
--             and pmay.year = x.year
--         ;
--
--         insert into prd.patient_med_adherence_year_measures(
--             patient_id, measure_id, year, fill_count, ipsd, next_fill_date, days_covered_to_period_end, days_not_covered,
--             absolute_fail_date, patient_med_adherence_measure_id, is_sure_scripts_measure, calc_to_date, pdc_to_date, adr,
--             sent_to_coop, sent_to_coop_at, inserted_at, updated_at
--         )
--         select
--             distinct
--             patient_id, measure_id, year, fill_count, ipsd, next_fill_date, days_covered_to_period_end, days_not_covered,
--             absolute_fail_date, patient_med_adherence_measure_id, is_sure_scripts_measure, calc_to_date, pdc_to_date, adr,
--             sent_to_coop, sent_to_coop_at, inserted_at, updated_at
--         from _crupdate_pmay x
--         where x.do_action = 'create'
--         ;
--     --------------------------------------------------------------------------------------------------------------------
--     -- STEP #3.6 Exclusions --------------------------------------------------------------------------------------------
--     --------------------------------------------------------------------------------------------------------------------
--
--         INSERT
--         INTO
--             prd.patient_med_adherence_exclusions AS ex (patient_id, year, measure_id, exclusion_reasons, exclusion_src)
--         SELECT DISTINCT
--             pmay.patient_id
--           , pmay.year
--           , pmay.measure_id
--           , '{Insulin Exclusion}'::text[]
--           , 'sure_scripts'
--         FROM
--             prd.patient_med_adherence_year_measures pmay
--             JOIN prd.patient_medications mhd
--                  ON mhd.patient_id::BIGINT = pmay.patient_id AND DATE_PART('year', mhd.start_date) = pmay.year
--             JOIN ref.med_adherence_value_sets vs
--                  ON vs.code = mhd.ndc AND mhd.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
--             JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
--                 AND m.measure_id = 'PDC-DR'
--                 AND m.table_name = 'Insulin Exclusion'
--                 AND m.is_exclusion = 'Y'
--         where pmay.measure_id = 'PDC-DR'
--         ON CONFLICT (patient_id, measure_id, year) DO UPDATE
--             SET exclusion_reasons = ex.exclusion_reasons || excluded.exclusion_reasons
--         WHERE
--             'Insulin Exclusion' != ANY (ex.exclusion_reasons)
--         ;
--
--     --------------------------------------------------------------------------------------------------------------------
--     -- STEP #4 RX fill measures  ----------------------------------------------------------------------------------------
--     --------------------------------------------------------------------------------------------------------------------
--         INSERT
--         INTO
--             prd.patient_rx_fill_measures (patient_id, measure_id, year, is_closed, patient_medication_ids, inserted_at, updated_at)
--         SELECT
--             patient_id
--           , mtml.measure_id
--           , mtml.yr
--           , TRUE
--           , ARRAY_AGG(DISTINCT pm.id)
--         , now()
--         , now()
--         FROM
--             prd.patient_medications pm
--             JOIN ref.hedis_med_list_to_codes mltc ON mltc.code = pm.ndc
--                 AND mltc.code_system = 'NDC'
--                 AND mltc.yr = ( SELECT ctx.yr FROM _controls_ptcs_med_adherence_measures ctx )
--             JOIN ref.hedis_measure_to_med_list mtml
--                  ON mtml.medication_list_name = mltc.medication_list_name AND mtml.yr = mltc.yr
--         WHERE
--               mtml.measure_id IN ('SPC', 'SPD', 'OMW')
--           and mtml.medication_list_name not in ( 'Dementia Medications', 'Diabetes Medications', 'Estrogen Agonists Medications')
--           AND pm.start_date BETWEEN ( SELECT ctx.boy FROM _controls_ptcs_med_adherence_measures ctx ) AND ( SELECT ctx.eoy FROM _controls_ptcs_med_adherence_measures ctx )
--         GROUP BY 1, 2, 3, 4
--         ON CONFLICT (patient_id, measure_id, year) DO NOTHING
--         ;
--     ------------------------------------------------------------------------------------------------------------------------
--     /* STEP #5 update phamacy table */
--     ------------------------------------------------------------------------------------------------------------------------
--         DROP TABLE IF EXISTS _pharmacies;
--         CREATE TEMP TABLE _pharmacies AS
--         SELECT
--             ncpdpid
--           , pharmacy_npi                           npi
--           , LOWER(pharmacy_name)                   name
--           , LOWER(pharmacy_address_line_1)         address_line_1
--           , LOWER(pharmacy_address_line_2)         address_line_2
--           , LOWER(pharmacy_city)                   city
--           , LOWER(pharmacy_state)                  state
--           , pharmacy_zip                           zip
--           , pharmacy_phone_number                  phone_number
--           , pharmacy_fax_number                    fax_number
--           , MAX(inserted_at)                       max_ins_at
--           , MAX(id)                                max_id
--           , ARRAY_AGG(DISTINCT source_description) sources
--         FROM
--             public.sure_scripts_med_history_details mdh
--         WHERE
--                 mdh.sure_scripts_med_history_id = ( SELECT ctx.sure_scripts_med_history_id
--                                                     FROM _controls_pts_load_patient_medications_from_surscripts ctx )
--           AND   ncpdpid IS NOT NULL
--           AND   pharmacy_npi IS NOT NULL
--           AND   pharmacy_phone_number IS NOT NULL
--         GROUP BY
--             ncpdpid
--           , pharmacy_npi
--           , LOWER(pharmacy_name)
--           , LOWER(pharmacy_address_line_1)
--           , LOWER(pharmacy_address_line_2)
--           , LOWER(pharmacy_city)
--           , LOWER(pharmacy_state)
--           , pharmacy_zip
--           , pharmacy_phone_number
--           , pharmacy_fax_number
--     ;
--
--         INSERT
--         INTO
--             prd.sure_scripts_pharmacies as p (ncpdpid, npi, phone_number, name, address_line_1, address_line_2, city, state,
--                                          zip, fax_number, sources)
--         WITH
--             ordered AS ( SELECT *
--                               , ROW_NUMBER()
--                                 OVER (PARTITION BY ncpdpid, npi, phone_number ORDER BY max_ins_at desc, max_id DESC) rn
--                          FROM
--                              _pharmacies )
--         SELECT
--             o.ncpdpid
--           , o.npi
--           , o.phone_number
--           , o.name
--           , o.address_line_1
--           , o.address_line_2
--           , o.city
--           , o.state
--           , o.zip
--           , o.fax_number
--           , o.sources
--         FROM
--             ordered o
--         WHERE
--             o.rn = 1
--         ON CONFLICT (ncpdpid, npi, phone_number) DO UPDATE SET
--             name = excluded.name
--           , address_line_1 = excluded.address_line_1
--           , address_line_2 = excluded.address_line_2
--           , city = excluded.city
--           , state = excluded.state
--           , zip = excluded.zip
--           , fax_number = excluded.fax_number
--           , sources = array(select distinct s from  unnest(excluded.sources || p.sources) s)
--           , updated_at = now()
--         WHERE
--             p.name is distinct from excluded.name OR
--             p.address_line_1 is distinct from excluded.address_line_1 OR
--             p.address_line_2 is distinct from excluded.address_line_2 OR
--             p.city is distinct from excluded.city OR
--             p.state is distinct from excluded.state OR
--             p.zip is distinct from excluded.zip OR
--             p.fax_number is distinct from excluded.fax_number OR
--             p.sources is distinct from excluded.sources
--         ;
--
--
--     --------------------------------------------------------------------------------------------------------------------
--     -- STEP #6 publish to COOP  ----------------------------------------------------------------------------------------
--     --------------------------------------------------------------------------------------------------------------------
--         drop table if exists _trasy_is_fun;
--         create temporary table _trasy_is_fun as
--         with pats as (
--             select
--                 (select year from ref.dates d where d.day = current_date) yr,
--                 array_agg(distinct sd.patient_id) patient_ids
--             from
--                 _ss_data sd
--             where
--                 -- Hard exclude basssett
--                 exists (
--                     select 1
--                     from
--                         fdw_member_doc.supreme_pizza sp
--                     join fdw_member_doc.referring_partners rp on sp.primary_referring_partner_id = rp.id
--                     where
--                         sp.patient_id = sd.patient_id
--                     and rp.organization_id != 7 -- hard bassett exclusion
--                     and sp.is_medication_adherence
--                 )
--         )
--         select
--             pats.*
--         from
--             pats
--             cross join prd.fn_ptcs_patient_med_adherence(pats.yr, pats.patient_ids)
--         ;

    ---------------------------
    ---------------------------
    EXCEPTION WHEN OTHERS THEN

        GET DIAGNOSTICS stack = PG_CONTEXT; GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT, exception_detail = PG_EXCEPTION_DETAIL, exception_hint = PG_EXCEPTION_HINT, exception_context = PG_EXCEPTION_CONTEXT;
        rollback;
        error_text = '(1) Message_Text( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
        PERFORM * FROM audit.fn_create_sms_alerts(array['de-analytics-etl','ae-analytics-etl'],'etl.sp_med_adherence_load_surescripts_to_coop',error_text::text);
        commit;
        RAISE EXCEPTION 'etl.pts_load_patient_meds_from_sure_scripts :: %', error_text;

    END;
    COMMIT;
    -------

--     -- Do this outside of the transaction to ensure staging sproc has access to the data
--     drop table if exists _trashy99;
--     create temporary table _trashy99 as
--     select 1 from dblink_exec('cb_member_doc', 'call stage.sp_stp_process_med_adherence_tasks()');


END;
$$;

ALTER PROCEDURE zzzzzz_sp_med_adherence_load_surescripts_to_coop_zon20241028(BIGINT) OWNER TO postgres;

