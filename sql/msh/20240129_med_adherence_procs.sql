------------------------------------------------------------------------------------------------------------------------
/* GO_TO_NEXT_SPROC */
------------------------------------------------------------------------------------------------------------------------

CREATE PROCEDURE sp_populate_sure_scripts_panel_patients()
    LANGUAGE plpgsql
AS
$$
DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN
    BEGIN
        -- temporarily prevent from running
        if (select now() between '2024-01-01' and '2024-01-31') then
            raise exception 'Med adh paused for January';
        END IF;

        DROP TABLE IF EXISTS _potential_patients;
        CREATE TEMP TABLE _potential_patients AS
        SELECT
            p.id                                                                                patient_id
          , pmay.measure_id                                                                     measure_id
          , pmay.id IS NOT NULL                                                                 has_measure
          , pmay.next_fill_date                                                                 next_fill_date
          , pmay.adr                                                                            adr
          , MAX(pp.inserted_at) FILTER ( WHERE ssr.hdr_load_status_description !=
                                               'Failed to load file.  No data was processed.' ) last_panel_created_at
          , MAX(pt.id) FILTER (WHERE pt.status IN ('new', 'in_progress')) IS NOT NULL           has_open_task
          , min(pt.inserted_at) FILTER (WHERE pt.status IN ('new', 'in_progress'))              open_task_inserted_at
          , MAX(med_ad_pat_task.visit_date) FILTER (WHERE pt.status IN ('new', 'in_progress'))  open_task_visit_date
          , MAX(med_ad_pat_task.expected_discharge_date)
            FILTER (WHERE pt.status = 'completed'
                AND NOT med_ad_pat_task.is_system_verified_closed)                      closed_task_discharge_date
          , MAX(pt.updated_at) FILTER (WHERE pt.status = 'completed'
            AND NOT med_ad_pat_task.is_system_verified_closed)   closed_non_verified_task_updated_at
          , MAX(mhd.id) IS NOT NULL                                                             pulled_but_no_ss_hit_previously
          , ARRAY_AGG(DISTINCT mhd.note)                                                        previous_ss_notes
            -- prescription fill tasks fields below
          , min(pqmt.inserted_at) FILTER (WHERE pqmt.status IN ('new', 'in_progress'))          open_rx_fill_task_inserted_at
          , MAX(pqmt.updated_at) FILTER (WHERE pqmt.status = 'completed'
            AND NOT pfpt.is_system_verified_closed)         closed_non_verified_rx_fill_task_updated_at
          , string_agg(distinct pqmt.task_type, ',')
            FILTER ( WHERE pqmt.status in ('new', 'in_progress') ) open_rx_fill_task_types
          , string_agg(distinct pqmt.task_type, ',') FILTER ( WHERE pqmt.status = 'completed' ) closed_rx_fill_task_types
            -- external osteo order
          , max(pt_ext_osteo.updated_at)                                                        max_upd_ext_osteo_complete_at
        FROM
            --         junk.med_adherence_pilot_care_teams_20230327 pct
            --         join fdw_member_doc.patients p  on pct.care_team_id = p.care_team_id
            fdw_member_doc.patients p
            JOIN fdw_member_doc.supreme_pizza sp on sp.patient_id = p.id and sp.is_medication_adherence
            JOIN fdw_member_doc.referring_partners rp on rp.id = sp.primary_referring_partner_id and rp.organization_id != 7
            LEFT JOIN fdw_member_doc.patient_tasks pt ON pt.patient_id = p.id
                AND pt.task_type IN ('med_adherence_cholesterol', 'med_adherence_diabetes', 'med_adherence_hypertension')
            LEFT JOIN fdw_member_doc.medication_adherence_patient_task med_ad_pat_task
                      ON pt.id = med_ad_pat_task.patient_task_id
            LEFT JOIN prd.patient_med_adherence_year_measures pmay ON pmay.patient_id = p.id
                AND pmay.year = EXTRACT('year' FROM NOW())
            LEFT JOIN public.sure_scripts_panel_patients pp ON pp.patient_id = p.id
            LEFT JOIN public.sure_scripts_responses ssr ON ssr.sure_scripts_panel_id = pp.sure_scripts_panel_id
            LEFT JOIN public.sure_scripts_med_history_details mhd ON p.id = mhd.patient_id::BIGINT
                AND mhd.note IS NOT NULL
            LEFT JOIN fdw_member_doc.patient_tasks pqmt on pqmt.patient_id = p.id
                AND pqmt.task_type IN ('prescription_fill_osteoporosis', 'prescription_fill_statin_diabetes', 'prescription_fill_statin_cvd') --prescription_fill_osteoporosis -- 12 prescription_fill_statin_diabetes -- 18 prescription_fill_statin_cvd -- 17
            left JOIN fdw_member_doc.prescription_fill_patient_task pfpt on pfpt.patient_task_id = pqmt.id
            left join fdw_member_doc.patient_tasks pt_ext_osteo on pt_ext_osteo.patient_id = p.id
                and pt_ext_osteo.task_type = 'recon_osteoporosis_external_order'
                and pt_ext_osteo.status = 'completed'
        GROUP BY
            1,2,3,4,5;


        DROP TABLE IF EXISTS _patients_to_pull;
        CREATE TEMP TABLE _patients_to_pull (
            patient_id BIGINT NOT NULL,
            reason     TEXT   NOT NULL,
            UNIQUE (patient_id)
        );

        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , '30 day refresh for non med adherence patients'
        FROM
            _potential_patients pp
        WHERE
              NOT pp.has_measure
              --   and pulled_but_no_ss_hit_previously -- not sure if we want to pull these folks
          AND (
                  pp.last_panel_created_at IS NULL
                      OR
                  pp.last_panel_created_at < NOW() - '30 days'::INTERVAL
                  );


--     --   - Patients with measure med and no open task and 20 <= adr < 30 and max refill date is 2 days from now
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 2 days before expected next fill date when adr < 20: ' || STRING_AGG(pp.measure_id, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND pp.adr < 20
          AND next_fill_date + '2 days'::INTERVAL <= NOW()
          AND NOT has_open_task
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
          , 'Pull 2 days after expected next fill date when 20 <= adr < 30: ' || STRING_AGG(pp.measure_id, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
--           AND pp.adr between 20 and 29
          AND pp.adr <= 29
          AND next_fill_date <= NOW() - '2 days'::INTERVAL
          AND NOT has_open_task
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
          , 'Pull 5 days after expected next fill date: ' || STRING_AGG(pp.measure_id, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND pp.adr >= 30
          AND next_fill_date <= NOW() - '5 days'::INTERVAL
          AND NOT has_open_task
          AND last_panel_created_at < NOW() - '5 days'::INTERVAL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

        -- Pull five days after visit date when task open with populated visit date
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 5 days from visit date on open task: ' || STRING_AGG(pp.measure_id, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND has_open_task
          AND open_task_visit_date <= NOW() - '5 days'::INTERVAL
          AND last_panel_created_at < NOW() - '5 days'::INTERVAL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

        -- Pull five days after discharge date on closed task
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 5 days from discharge date on closed task: ' || STRING_AGG(pp.measure_id, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND closed_task_discharge_date IS NOT NULL
          AND closed_task_discharge_date <= NOW() - '5 days'::INTERVAL
          AND last_panel_created_at < NOW() - '5 days'::INTERVAL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;


        --   - Patients with measure med and max refill date was 5+ days ago and an open task and it's been ten days since their last pull
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 10 days from last query if task is still open after ten: ' || STRING_AGG(pp.measure_id, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND has_open_task
          AND open_task_inserted_at <= NOW() - '10 days'::INTERVAL
          AND last_panel_created_at < NOW() - '10 days'::INTERVAL
          AND open_task_visit_date ISNULL
        GROUP BY 1
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;


        -- confirm task closed 5 days after closure
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Closed task needs confirmation'
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND closed_non_verified_task_updated_at < now() - '5 days'::INTERVAL
              -- only check if we haven't checked since closed
          AND last_panel_created_at < closed_non_verified_task_updated_at
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

        -- RX Fill quality measure Logic
        -- OLD logic: depended on CCA. BP removed 20230830 per conversation with Banu/John/Moon
        -- Pull for patients that have pending_rx_fill completed cca and no tasks
--         DROP TABLE IF EXISTS _pending_rx_fills;
--         CREATE TEMP TABLE _pending_rx_fills AS
--             SELECT DISTINCT
--                     pqm.patient_id, pqm.measure_id, pqm.id patient_quality_measure_id
--                 FROM
--                     fdw_member_doc.msh_cca_worksheet_patient_quality_measures wpm
--                     JOIN fdw_member_doc.patient_quality_measures pqm ON wpm.patient_quality_measure_id = pqm.id
--                     JOIN fdw_member_doc.supreme_pizza sp ON pqm.patient_id = sp.patient_id and sp.is_quality_measures
--                     JOIN fdw_member_doc.msh_patient_quality_measures mpqm ON pqm.id = mpqm.patient_quality_measure_id
--                     JOIN fdw_member_doc.msh_cca_worksheets cw ON cw.id = wpm.msh_cca_worksheet_id
--                     JOIN fdw_member_doc.visits v ON cw.visit_id = v.id
--                 WHERE
--                       wpm.substatus_outcome = 'pending_rx_fill'
--                   AND mpqm.substatus = 'pending_rx_fill'
--                   AND pqm.measure_id IN (12, 17, 18)
--                   AND cw.status = 'completed'
--                   AND v.type_id = 'cca_recon'
--                   AND v.completed_at + '5 days'::INTERVAL <= NOW()
--                   AND v.deleted_at ISNULL
--         ;
        -- for new bulk without cca, query ss 5 days after substatus change to pending_rx_fill
        DROP TABLE IF EXISTS _pending_rx_fills;
        CREATE TEMP TABLE _pending_rx_fills AS
        SELECT DISTINCT
            pqm.patient_id, pqm.measure_id, pqm.id patient_quality_measure_id
        FROM
            fdw_member_doc.patient_quality_measures pqm
            JOIN fdw_member_doc.supreme_pizza sp ON pqm.patient_id = sp.patient_id and sp.is_quality_measures
            JOIN fdw_member_doc.msh_patient_quality_measures mpqm ON pqm.id = mpqm.patient_quality_measure_id
            JOIN fdw_member_doc.msh_patient_measure_substatus_history ssh on mpqm.id = ssh.msh_patient_quality_measure_id and ssh.substatus = 'pending_rx_fill'
        WHERE
              mpqm.substatus = 'pending_rx_fill'
          AND pqm.measure_id IN (12, 17, 18)
          AND ssh.changed_at + '5 days'::INTERVAL <= NOW()
        ;

        INSERT
        INTO
            _patients_to_pull(patient_id, reason)
        SELECT DISTINCT
            prf.patient_id
          , 'CCA pending rx fill: ' || STRING_AGG(DISTINCT qm.code, ',')
        FROM
            _pending_rx_fills prf
            JOIN fdw_member_doc.quality_measures qm ON prf.measure_id = qm.id
            LEFT JOIN fdw_member_doc.patient_quality_measures_tasks pqmt
                      ON pqmt.patient_measure_id = prf.patient_quality_measure_id
            LEFT JOIN fdw_member_doc.patient_tasks pt ON pqmt.patient_task_id = pt.id
        WHERE
              NOT EXISTS(
                  SELECT
                      1
                  FROM
                      public.sure_scripts_panel_patients pp
                  WHERE
                        pp.patient_id = prf.patient_id
                    AND pp.inserted_at + '5 days'::INTERVAL > NOW()
              )
          AND pt.id ISNULL
        GROUP BY 1
        ON CONFLICT (patient_id) DO UPDATE SET reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

        --   - Patients with prescription fill task open for ten days, query again
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 10 days from last query if rx fill task is still open after ten: ' || open_rx_fill_task_types
        FROM
            _potential_patients pp
        WHERE
              pp.open_rx_fill_task_inserted_at is not null
          and pp.open_rx_fill_task_inserted_at <= NOW() - '10 days'::INTERVAL
          AND pp.last_panel_created_at < NOW() - '10 days'::INTERVAL
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

        -- confirm task closed 5 days after closure
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Closed rx fill task needs confirmation'
        FROM
            _potential_patients pp
        WHERE
              closed_non_verified_rx_fill_task_updated_at < now() - '5 days'::INTERVAL
              -- only check if we haven't checked since closed
          AND last_panel_created_at < closed_non_verified_rx_fill_task_updated_at
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;
        ------------------------------------------------------------------------------------------------------------------------
        /* One off for completed  recon_osteoporosis_external_order tasks
           Check 5 days after task is completed and has a quality measure that's still open
        */
        ------------------------------------------------------------------------------------------------------------------------
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT pp.patient_id, 'Completed osteo order follow 5 days ago'
        FROM
            _potential_patients pp
        where coalesce(max_upd_ext_osteo_complete_at, now()) + '5 days'::interval <= now()
          and pp.last_panel_created_at + '5 days'::interval <= now()
              -- ignore if already closed ie present in this table
          and not exists(
            select 1 from prd.patient_rx_fill_measures prfm
            where prfm.patient_id = pp.patient_id
              and prfm.measure_id = 'OMW'
              and prfm.year = date_part('year', now())
        )
        ON CONFLICT (patient_id) do UPDATE set reason = _patients_to_pull.reason || '; ' || excluded.reason
        ;

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
/* GO_TO_NEXT_SPROC */
------------------------------------------------------------------------------------------------------------------------
create procedure etl.sp_med_adherence_load_surescripts_to_coop(IN _sure_scripts_med_history_id bigint)
    language plpgsql
as
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
                concat_ws('::', patient_id::text, ndc::text, last_filled_date::text, days_supply::text) unique_key,
                patient_id::bigint patient_id,
                ndc,
                last_filled_date,
                days_supply::int                    days_supply,
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
                max(sold_date)                           sold_date,
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
          and mtml.medication_list_name not in ( 'Dementia Medications', 'Diabetes Medications', 'Estrogen Agonists Medications')
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
        PERFORM * FROM audit.fn_create_sms_alerts(array['de-analytics-etl','ae-analytics-etl'],'etl.sp_med_adherence_load_surescripts_to_coop',error_text::text);
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

alter procedure etl.sp_med_adherence_load_surescripts_to_coop(bigint) owner to postgres;

------------------------------------------------------------------------------------------------------------------------
/* GO_TO_NEXT_SPROC  */
------------------------------------------------------------------------------------------------------------------------
CREATE FUNCTION fn_build_med_adherence_synthetics(_start_date date, _end_date date, _patient_ids bigint[], _sure_scripts_med_history_id bigint) RETURNS bigint
    LANGUAGE plpgsql
AS
$$ BEGIN

    drop table if exists _controls_fn_build_med_adherence_synthetics;
    create temporary table _controls_fn_build_med_adherence_synthetics as
    with insrt as (
        insert into prd.patient_med_adherence_synth_period_batches(start_date, end_date, patient_ids, sure_scripts_med_history_id)
        select _start_date start_date, _end_date end_date, _patient_ids patient_ids, _sure_scripts_med_history_id sure_scripts_med_history_id
        returning id batch_id, start_date, end_date, patient_ids, sure_scripts_med_history_id
    )
    select * from insrt;

    drop table if exists _patients_to_run;
    create temporary table _patients_to_run(patient_id bigint primary key);
    insert into _patients_to_run(patient_id)
    select distinct unnest(ctx.patient_ids) patient_id from _controls_fn_build_med_adherence_synthetics ctx;

    -- determine patients to run this for
    DROP TABLE IF EXISTS _patients;
    CREATE TEMP TABLE _patients AS
    SELECT DISTINCT
        prp.patient_id
      , prp.referring_partner_id
    FROM
        fdw_member_doc.patient_referring_partners prp
        JOIN prd.patient_medications mhd ON mhd.patient_id::BIGINT = prp.patient_id
    WHERE
        prp."primary"
        and exists (select 1 from _patients_to_run ptr where ptr.patient_id = mhd.patient_id)
    ;


    -- pre_process the data
    DROP TABLE IF EXISTS _patient_meds;
    CREATE TEMP TABLE _patient_meds AS
    select
        *
    from (
        SELECT
            p.patient_id
          , p.referring_partner_id
          , mhd.drug_description
          , mhd.ndc
          , mhd.days_supply
          , vs.value_set_item
          , mhd.refills_remaining
          , mhd.start_date
          , mhd.written_date
          , mhd.last_filled_date
          , mhd.sold_date
          , mhd.start_date + days_supply::INT - 1 last_day_of_meds
          , mhd.start_date + days_supply::INT     next_fill_date
          , array_agg(distinct mhd.id)            patient_medication_ids
        FROM
            _patients p
            join prd.patient_medications mhd          on mhd.patient_id::bigint = p.patient_id
            JOIN ref.med_adherence_value_sets vs      ON vs.code = mhd.ndc  and mhd.start_date between vs.from_date and vs.thru_date -- only have ndc's
            JOIN ref.med_adherence_measures m         ON m.value_set_id = vs.value_set_id AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
        WHERE
            -- 20231213 BP added is_med+not exclusion
            m.is_med = 'Y'
        and m.is_exclusion = 'N'
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13
    ) x
    where  x.start_date between (select ctx.start_date from _controls_fn_build_med_adherence_synthetics ctx)
                                and
                                (select ctx.end_date from _controls_fn_build_med_adherence_synthetics ctx)
    ;

    -- define the tables to be used below
    DROP TABLE IF EXISTS _patient_med_periods;
    create temporary table _patient_med_periods(
        id                      bigserial primary key,
        fn_iteration            bigint,
        is_moved                bool,
        patient_id              bigint,
        join_key                text,
        value_set_item          text,
        days_supply             int,
        rn                      int,
        start_date              date,
        end_date                date,
        og_start_date           date,
        og_end_date             date,
        prev_start_date         date,
        prev_days_supply        int,
        overlap_id              bigint,
        overlap_start_date      date,
        overlap_end_date        date,
        measure_id              text,
        measure_table_id        text,
        patient_medication_ids  bigint[],
        ndcs                    text[]
    );

    DROP TABLE IF EXISTS _processed_patient_med_periods;
    create temporary table _processed_patient_med_periods(
        fn_iteration           bigint,
        id                     bigint,
        is_moved               bool,
        patient_id             bigint,
        join_key               text,
        value_set_item         text,
        days_supply            int,
        rn                     int,
        start_date             date,
        end_date               date,
        og_start_date          date,
        og_end_date            date,
        prev_start_date        date,
        prev_days_supply       int,
        overlap_id             bigint,
        overlap_start_date     date,
        overlap_end_date       date,
        measure_id             text,
        measure_table_id       text,
        patient_medication_ids bigint[],
        ndcs                   text[]
    );


    -- build initial dataset
    insert into _patient_med_periods(
        fn_iteration, is_moved,
        patient_id, join_key, days_supply, rn, start_date, end_date,
        og_start_date, og_end_date, prev_start_date, prev_days_supply, overlap_id, overlap_start_date, overlap_end_date,
        measure_id, value_set_item, patient_medication_ids, ndcs
    )
    select
        0 fn_iteration, true is_moved,
        patient_id, join_key, days_supply, rn, start_date, end_date,
        null::date og_start_date, null::date og_end_date, null::date prev_start_date, null::int prev_days_supply,
        null::bigint overlap_id, null::date overlap_start_date, null::date overlap_end_date,
        measure_id, value_set_item, patient_medication_ids, ndcs
    from (
        select
            *,
            row_number() over (partition by join_key order by start_date asc) rn
        from (
            select
                pm.patient_id,
                m.measure_id,
                vs.value_set_item,
                concat_ws('::', patient_id, measure_id, vs.value_set_item) join_key,
                pm.start_date,
                pm.start_date + max(pm.days_supply)::int - 1 end_date,
                max(pm.days_supply)::int days_supply,
                ('{' || replace(replace(replace(array_agg(distinct pm.patient_medication_ids::text)::text,'}',''),'{',''),'"','') || '}')::bigint[] patient_medication_ids,
                array_agg(pm.ndc) ndcs
            FROM
                ref.med_adherence_measures m
                JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
                JOIN _patient_meds pm                ON pm.ndc = vs.code AND vs.code_type = 'NDC'
                                                        AND pm.value_set_item = vs.value_set_item
                                                        AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
            WHERE
                m.is_med = 'Y'
                and m.is_exclusion <> 'Y'
                and m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
            group by 1,2,3,4,5
        ) xx
    ) x
    ;


    -- run initial serializer
    drop table if exists _trsh;
    create temporary table _trsh as
    select * from prd.fn_med_adherence_iterate_internal(1);

    -- determine max # of iterations required
    drop table if exists _max_iterations;
    create temporary table _max_iterations as
    select
        max(nd) do_n_iterations
    from
        (
           select
                ts1.join_key, count(distinct ts1.overlap_id) nd
            from
                _patient_med_periods ts1
            group by 1
        ) x
    ;

    -- run that max # of iterations
    drop table if exists _trsh2;
    create temporary table _trsh2 as
    select
        i, fn.*
    from
        _max_iterations mi
        cross join generate_series(2, mi.do_n_iterations + 2) i
        cross join prd.fn_med_adherence_iterate_internal(i::int) fn
    ;

    insert into prd.patient_med_adherence_synth_periods(
        batch_id, fn_iteration, is_moved, patient_id, join_key, days_supply, rn,
        start_date, end_date, overlap_id, overlap_start_date, overlap_end_date,
        measure_id, value_set_item, og_start_date, og_end_date, prev_start_date,
        prev_days_supply, patient_medication_ids, ndcs
    )
    select
        batch_id, fn_iteration, is_moved, patient_id, join_key, days_supply, rn,
        start_date, end_date, overlap_id, overlap_start_date, overlap_end_date,
        measure_id, value_set_item, og_start_date, og_end_date, prev_start_date,
        prev_days_supply, patient_medication_ids, ndcs
    from (
        select
            *,
            (select ctx.batch_id from _controls_fn_build_med_adherence_synthetics ctx) batch_id
        from _processed_patient_med_periods pm order by join_key, start_date
    ) xx;


    return (select ctx.batch_id from _controls_fn_build_med_adherence_synthetics ctx);

END; $$;

ALTER FUNCTION fn_build_med_adherence_synthetics(DATE, DATE, BIGINT[], BIGINT) OWNER TO postgres;


------------------------------------------------------------------------------------------------------------------------
/* GO_TO_NEXT_SPROC  */
------------------------------------------------------------------------------------------------------------------------
CREATE FUNCTION fn_ptcs_patient_med_adherence(_year integer, _patient_ids bigint[]) RETURNS void
    LANGUAGE plpgsql
AS
$$ BEGIN
    ---------

    drop table if exists _controls_master_fn_ptcs_patient_med_adherence;
    create temporary table _controls_master_fn_ptcs_patient_med_adherence as
    select _year yr;

    drop table if exists _controls_patients;
    create temporary table _controls_patients(patient_id bigint primary key);

    insert into _controls_patients(patient_id)
    select distinct unnest(_patient_ids) patient_id;

    ------------------------------------------------------------------------------------------------------------------------
    /*  */
    ------------------------------------------------------------------------------------------------------------------------
    drop table if exists _records_to_use;
    create temporary table _records_to_use(patient_id bigint primary key, batch_id bigint);
    insert into _records_to_use(patient_id, batch_id)
    select
        patient_id, batch_id
    from (
        select
            pmam.patient_id,
            max(patient_med_adherence_synth_period_batch_id) batch_id,
            max(patient_med_adherence_synth_period_batch_id) filter ( where pmam.sent_to_coop ) last_sent_batch_id
        from
            prd.patient_med_adherence_measures pmam
            join _controls_patients ctx on ctx.patient_id = pmam.patient_id
        group by 1
    ) x
    where x.batch_id is distinct from last_sent_batch_id -- if batch already sent, do not do anything for that patient
    ;


    -- last fill meds for measure
    drop table if exists _last_fill_med_measures;
    CREATE TEMPORARY TABLE _last_fill_med_measures AS
    with pre_last_fills as (
        SELECT
            DISTINCT ON (mhd.patient_id, pmam.measure_id, mhd.ndc)
            pmam.measure_id,
            mhd.*,
            pms.end_date + 1 adjusted_date
        FROM
            prd.patient_med_adherence_measures pmam
            join _records_to_use rtu on rtu.patient_id = pmam.patient_id and rtu.batch_id = pmam.patient_med_adherence_synth_period_batch_id
            JOIN prd.patient_med_adherence_synth_periods pms ON pms.id = ANY (pmam.patient_med_adherence_synth_period_ids)
            JOIN prd.patient_medications mhd ON mhd.id = ANY (pms.patient_medication_ids)
        where
            pmam.year = (select ctx.yr from _controls_master_fn_ptcs_patient_med_adherence ctx)
        ORDER BY
            mhd.patient_id, pmam.measure_id, mhd.ndc, mhd.start_date DESC
    ), last_fill_limiter as (
        select
            patient_id,
            measure_id,
            max(adjusted_date) max_adjusted_date
        from
            pre_last_fills
        group by 1,2
    )
    select
        *
    from
        pre_last_fills plf
    where
        exists (
                select 1
                from last_fill_limiter lfl
                where lfl.patient_id = plf.patient_id
                      and lfl.measure_id = plf.measure_id
                      and lfl.max_adjusted_date = plf.adjusted_date
        )
    ;

    drop table if exists _previous_patient_ndc_fills;
    create temporary table _previous_patient_ndc_fills as
    select
        lfm.id,
        array_remove(array_agg(pm.start_date order by pm.start_date),null) previous_fills
    from
        _last_fill_med_measures lfm
        left join prd.patient_medications pm on pm.patient_id = lfm.patient_id
                                                and pm.ndc = lfm.ndc
                                                and pm.start_date < lfm.start_date
    group by 1
    ;

    drop table if exists _pizza_med_adherence;
    create temporary table _pizza_med_adherence(patient_id bigint primary key);
    insert into _pizza_med_adherence(patient_id)
    select distinct patient_id from fdw_member_doc.supreme_pizza sp where sp.is_medication_adherence;

    drop table if exists _final_publish;
    create temporary table _final_publish as
        SELECT
            pmam.patient_id,
            pmam.measure_id,
            (select ctx.yr from _controls_master_fn_ptcs_patient_med_adherence ctx) yr,
            f.drug_description  drug_name,
            f.ndc               ndc,
            f.days_supply::int  days_supply,
            f.start_date        last_fill_date,
            f.adjusted_date     adjusted_next_fill_date,
            f.refills_remaining remaining_refills,
            f.dispenser_name    pharmacy_name,
            f.dispenser_npi     pharmacy_npi,
            f.dispenser_phone   pharmacy_phone,
            f.prescriber_phone,
            f.prescriber_name,
            f.prescriber_npi,
            pmam.days_covered_to_date,
            pmam.days_covered_to_period_end,
            pmam.days_to_cover_to_date,
            pmam.calc_to_date,
            pmam.days_needed_thru_eoy,
            pmam.pdc_to_date,
            pmam.adr,
            pmam.ipsd, -- index period start date
            pmam.absolute_fail_date,
            f.start_date + f.days_supply::int next_fill_date,
            rl.id is not null                 failed_last_year,
            pmam.id                           analytics_id,
            lfp.previous_fills                prev_fill_dates,
            'sure_scripts'                    source,
            gen_random_uuid()                 uuid,
            false                             is_processed,
            false                             is_ignored,
            now()                             inserted_at,
            now()                             updated_at
        FROM
            prd.patient_med_adherence_measures pmam
            join _pizza_med_adherence pz on pz.patient_id = pmam.patient_id
            join _records_to_use rtu on rtu.patient_id = pmam.patient_id and rtu.batch_id = pmam.patient_med_adherence_synth_period_batch_id
            join _last_fill_med_measures f on f.patient_id::bigint = pmam.patient_id
                                              and f.measure_id = pmam.measure_id
            join _previous_patient_ndc_fills lfp on lfp.id = f.id
            left join prd.patient_med_adherence_red_list rl on rl.patient_id = pmam.patient_id
                                                               and rl.measure_id = pmam.measure_id
                                                               and rl.year = pmam.year - 1
            left join prd.patient_med_adherence_exclusions ex on ex.patient_id = pmam.patient_id
                                                             and ex.measure_id = pmam.measure_id
                                                             and ex.year = pmam.year
        where
            pmam.year = (select ctx.yr from _controls_master_fn_ptcs_patient_med_adherence ctx)
            and (
                   pmam.next_fill_date + 5 <= CURRENT_DATE
                OR (pmam.next_fill_date + 2 <= CURRENT_DATE and pmam.adr < 30)
                OR (pmam.next_fill_date - 2 <= CURRENT_DATE and pmam.adr < 20)
                )
            and pmam.is_sure_scripts_measure
            and ex.id ISNULL
    ;
    create index idx__final_publish on _final_publish(analytics_id);

    -- mark as sent to coop.stage
    update prd.patient_med_adherence_measures pmam
    set sent_to_coop = true
    where exists(select 1 from _final_publish fp where fp.analytics_id = pmam.id)
    ;

    -- send to coop.stage
    INSERT INTO fdw_member_doc_stage.patient_medication_adherences (
        patient_id, measure_id, drug_name, ndc, days_supply, next_fill_date, last_fill_date,
        adjusted_next_fill_date, remaining_refills, prescriber_name, prescriber_npi, prescriber_phone,
        pharmacy_name, pharmacy_npi, pharmacy_phone, failed_last_year, analytics_id,
        inserted_at, updated_at, prev_fill_dates, yr, uuid, is_processed, is_ignored,
        days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date, calc_to_date,
        days_needed_thru_eoy, pdc_to_date, adr, absolute_fail_date, source
    )
    select
        patient_id, measure_id, drug_name, ndc, days_supply, next_fill_date, last_fill_date,
        adjusted_next_fill_date, remaining_refills, prescriber_name, prescriber_npi, prescriber_phone,
        pharmacy_name, pharmacy_npi, pharmacy_phone, failed_last_year, analytics_id,
        inserted_at, updated_at, prev_fill_dates, yr, uuid, is_processed, is_ignored,
        days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date, calc_to_date,
        days_needed_thru_eoy, pdc_to_date, adr, absolute_fail_date, source
    from _final_publish
    ;

    -- CLOSE TASKS THAT ARE OPEN -----------------------------------------------------
    drop table if exists _measure_to_task_type;
    create temporary table _measure_to_task_type as
    select * from fdw_member_doc_stage.medication_adherence_measure_id_to_task_types;

    drop table if exists _non_verified_tasks;
    create temporary table _non_verified_tasks as
    select distinct
        pt.id patient_task_id,
        pt.patient_id,
        pt.task_type
    from
        fdw_member_doc.patient_tasks pt
    join fdw_member_doc.medication_adherence_patient_task mapt ON mapt.patient_task_id = pt.id
    where
        pt.status in ('new', 'in_progress')
    or (pt.status in ('cancelled', 'completed') and not mapt.is_system_verified_closed)
    ;

    insert into fdw_member_doc_stage.patient_medication_adherence_compliances(
        patient_id, measure_id, is_processed, inserted_at, source, yr
    )
    select
        distinct
        pmam.patient_id,
        pmam.measure_id,
        false is_processed,
        now() inserted_at,
        'sure_scripts' source,
        pmam.year yr
    FROM
        prd.patient_med_adherence_measures pmam
        -- ADH & BP 2023-03-31 :: send the compliance data down regardless of layer_cake/pizza is_med_adherence feature toggle to ensure tasks are closed & confirmed
            -- join _pizza_med_adherence pz on pz.patient_id = pmam.patient_id
        join _records_to_use rtu on rtu.patient_id = pmam.patient_id and rtu.batch_id = pmam.patient_med_adherence_synth_period_batch_id
        join _non_verified_tasks ot on pmam.patient_id = ot.patient_id
        join _measure_to_task_type mtt on mtt.measure_id = pmam.measure_id and ot.task_type = mtt.task_type
    where
        pmam.year = (select ctx.yr from _controls_master_fn_ptcs_patient_med_adherence ctx)
        and pmam.next_fill_date > current_date
        and pmam.is_sure_scripts_measure
    ;

    -- insert closed rx fill measures
    INSERT
    INTO
        fdw_member_doc_stage.patient_rx_fill_measures (analytics_id, patient_id, measure_id, year, is_closed, patient_medication_ids, inserted_at, updated_at)
    select id, patient_id, measure_id, year, is_closed, patient_medication_ids, now(), now()
    from prd.patient_rx_fill_measures
    ON CONFLICT DO NOTHING ;

    INSERT
    INTO
        fdw_member_doc.sure_scripts_pharmacies AS p (analytics_id, ncpdpid, npi, phone_number, name, address_line_1,
                                                     address_line_2, city, state, zip, fax_number, sources, inserted_at,
                                                     updated_at)
    SELECT id, ncpdpid, npi, phone_number, name, address_line_1,
           address_line_2, city, state, zip, fax_number, sources, inserted_at,
           updated_at
    FROM
        prd.sure_scripts_pharmacies ssp
    ON CONFLICT DO NOTHING;

    UPDATE fdw_member_doc.sure_scripts_pharmacies c
    SET
        name           = p.name
      , address_line_1 = p.address_line_1
      , address_line_2 = p.address_line_2
      , city           = p.city
      , state          = p.state
      , zip            = p.zip
      , fax_number     = p.fax_number
      , sources        = p.sources
      , updated_at     = NOW()
    FROM
        prd.sure_scripts_pharmacies p
    WHERE
        c.analytics_id = p.id
      AND (p.name IS DISTINCT FROM c.name
      OR p.address_line_1 IS DISTINCT FROM c.address_line_1
      OR p.address_line_2 IS DISTINCT FROM c.address_line_2
      OR p.city IS DISTINCT FROM c.city
      OR p.state IS DISTINCT FROM c.state
      OR p.zip IS DISTINCT FROM c.zip
      OR p.fax_number IS DISTINCT FROM c.fax_number
      OR p.sources IS DISTINCT FROM c.sources)
        ;

    -- exclusions
    INSERT
    INTO
        fdw_member_doc_stage.patient_med_adherence_exclusions (analytics_id, patient_id, year, measure_id,
                                                               exclusion_reason, exclusion_src, inserted_at,
                                                               updated_at)
    SELECT distinct id , patient_id , year , measure_id
      , unnest(exclusion_reasons) ex_reason, exclusion_src , inserted_at
      , updated_at
    FROM
        prd.patient_med_adherence_exclusions ax
    ON CONFLICT DO NOTHING;


END; $$;

ALTER FUNCTION fn_ptcs_patient_med_adherence(INTEGER, BIGINT[]) OWNER TO postgres;


------------------------------------------------------------------------------------------------------------------------
/* GO_TO_NEXT_SPROC  */
------------------------------------------------------------------------------------------------------------------------

create procedure etl.sp_patient_med_adherence_year_measures_update_for_today_to_coop()
    language plpgsql
as
$$
DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;  _latest_md_portals_roster_ts timestamp; _latest_md_portals_file_ts timestamp;
BEGIN

    BEGIN

        drop table if exists _to_crupdate;
        create temporary table _to_crupdate as
        select
            1.0 - (days_not_covered * 1.0 / nullif(treatment_period, 0))::decimal(16,3) new_pdc_to_date,
            *
        from
            (
                select
                    case when cpmay.analytics_pmay_id is not null then 'update'
                         else 'create'
                        end do_action,
                    (pmay.next_fill_date - pmay.ipsd - pmay.days_covered_to_period_end) + greatest(0, current_date - pmay.next_fill_date) new_days_not_covered,
                    greatest(0, pmay.absolute_fail_date - greatest(current_date, pmay.next_fill_date) - 1) new_adr,
                    current_date - pmay.ipsd treatment_period,
                    pmay.id analytics_pmay_id,
                    pmay.*
                from
                    prd.patient_med_adherence_year_measures pmay
                    left join fdw_member_doc.patient_med_adherence_year_measures cpmay on cpmay.analytics_pmay_id = pmay.id
                where
                    pmay.year = extract(year from current_date)
            ) x
        ;


        update prd.patient_med_adherence_year_measures pmay
        set adr              = tu.new_adr,
            pdc_to_date      = tu.new_pdc_to_date,
            days_not_covered = tu.new_days_not_covered,
            calc_to_date     = current_date,
            updated_at       = now(),
            sent_to_coop     = true,
            sent_to_coop_at  = now()
        from
            _to_crupdate tu
        where
              tu.patient_id = pmay.patient_id
          and tu.measure_id = pmay.measure_id
          and tu.year       = pmay.year
        ;

        update fdw_member_doc.patient_med_adherence_year_measures u
        set
            adr                        = pmay.adr,
            pdc_to_date                = pmay.pdc_to_date,
            days_not_covered           = pmay.days_not_covered,
            calc_to_date               = pmay.calc_to_date,
            updated_at                 = pmay.updated_at,
            failed_last_year           = rl.id IS NOT NULL,
            fill_count                 = pmay.fill_count,
            next_fill_date             = pmay.next_fill_date,
            days_covered_to_period_end = pmay.days_covered_to_period_end,
            absolute_fail_date         = pmay.absolute_fail_date
        from
            _to_crupdate tu
            join prd.patient_med_adherence_year_measures pmay on pmay.id = tu.id
            left join prd.patient_med_adherence_red_list rl on rl.patient_id = pmay.patient_id
                and rl.measure_id = pmay.measure_id
                and rl.year = pmay.year - 1

        where
              tu.do_action = 'update'
          and tu.id = u.analytics_pmay_id
        ;

        insert into fdw_member_doc.patient_med_adherence_year_measures(
            analytics_pmay_id, patient_id, measure_id, year, fill_count, ipsd,
            next_fill_date, days_covered_to_period_end, days_not_covered, absolute_fail_date,
            patient_med_adherence_measure_id, is_sure_scripts_measure, calc_to_date,
            pdc_to_date, adr, inserted_at, updated_at, quality_measure_id, failed_last_year
        )
        select
            analytics_pmay_id, patient_id, measure_id, year, fill_count, ipsd,
            next_fill_date, days_covered_to_period_end, days_not_covered, absolute_fail_date,
            patient_med_adherence_measure_id, is_sure_scripts_measure, calc_to_date,
            pdc_to_date, adr, inserted_at, updated_at, quality_measure_id, failed_last_year
        from (
                 select
                     tu.analytics_pmay_id,
                     tt.quality_measure_id,
                     rl.id is not null failed_last_year,
                     pmay.*
                 from
                     _to_crupdate tu
                     join prd.patient_med_adherence_year_measures pmay on pmay.id = tu.id
                     left join prd.patient_med_adherence_red_list rl on rl.patient_id = pmay.patient_id
                         and rl.measure_id = pmay.measure_id
                         and rl.year = pmay.year - 1

                     join fdw_member_doc_stage.medication_adherence_measure_id_to_task_types tt on tt.measure_id = pmay.measure_id
                 where
                     tu.do_action = 'create'
             ) x
        ;

        ---------------------------
        ---------------------------
    EXCEPTION WHEN OTHERS THEN

        GET DIAGNOSTICS stack = PG_CONTEXT; GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT, exception_detail = PG_EXCEPTION_DETAIL, exception_hint = PG_EXCEPTION_HINT, exception_context = PG_EXCEPTION_CONTEXT;
        rollback;
        error_text = '(1) Message_Text( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
        PERFORM * FROM audit.fn_create_sms_alerts(array['de-analytics-etl'],'etl.sp_patient_med_adherence_year_measures_update_for_today_to_coop',error_text::text);
        commit;
        RAISE EXCEPTION 'etl.sp_patient_med_adherence_year_measures_update_for_today_to_coop :: %', error_text;

    END;
    COMMIT;
    -------

END; $$;

alter procedure etl.sp_patient_med_adherence_year_measures_update_for_today_to_coop() owner to postgres;

------------------------------------------------------------------------------------------------------------------------
/* GO_TO_NEXT_SPROC  */
------------------------------------------------------------------------------------------------------------------------
create procedure etl.sp_patient_med_measures_mco_gaps(IN _payer_id bigint, IN _inbound_file_id bigint DEFAULT NULL::bigint)
    language plpgsql
as
$$
    ------------------------------------------------------------------------------------------------------------------------
/*
 Load MCO med adherence measures from raw tables to prd.mco_patient_measure_rx_fills
 Revision History :
    --------------------------------------------------------------------------------------------
    Date            Author                  Comment
    --------------------------------------------------------------------------------------------
    2023-04-28      Brendon Pierson         Added wellcare and Humana Processing
    2023-05-08      Brendon Pierson         Added Elevance Processing
    2023-06-15      Brendon + Austin        Update to new wellcare File + (updated inbound_file_id logic on 06-30)
    2023-07-17      Brendon Pierson         Switched bcbstn from raw.bcbstn_medication_adherence to raw.bcbstn_medication_adherence_data
    2023-07-29      Austin                  Fixed bcbstn prd.mco_patients join on new table
    2023-10-26      Matt Zaloba             Added in Topic Based SMS_Alerting.
    2023-12-29      Austin                  Added control table _payer_id filter to ensure 2023 files don't process as 2024
    2024-01-03      Austin                  Commented out step 3 to prevent task sproc from being fired, turn back on at end of Jan
*/
------------------------------------------------------------------------------------------------------------------------
DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN

    BEGIN

        --check new files loaded after 12/29/23
        /*SELECT * FROM public.rtp_mco_file_tracker
                 WHERE table_name IN (
                                      'elevance_pharmacy_report' --2
                                     ,'bcbstn_medication_adherence_data' --38
                                     ,'humana_rx_opportunity_detail_report' --44
                                     ,'patient_rx_adherence_roster_uhc' --47
                                     ,'wellcare_medadherence' --49
                                     )
        ORDER BY payer_id, inbound_file_id DESC
        ;*/

        drop table if exists _controls_rtp_patient_mco_med_measures;
        create temporary table _controls_rtp_patient_mco_med_measures as
        select _payer_id payer_id, _inbound_file_id inbound_file_id
        WHERE _payer_id NOT IN (
                                 2  --elevance/anthem
                                ,38 --bcbstn
                                ,44 --humana
                                ,47 --uhc
                                ,49 --wellcare
                                );
         --select 44 payer_id, 12765983 inbound_file_id;

        drop table if exists _mco_patient_measure_fills;
        create temporary table _mco_patient_measure_fills
        (
            unique_key             text    not null primary key,
            payer_id               bigint  not null,
            latest_raw_id          bigint  not null,
            latest_inbound_file_id bigint  not null,
            measure_id             text    not null,
            measure_year           integer not null,
            is_new_to_measure      boolean,
            is_prev_year_fail      boolean,
            is_first_fill          boolean,
            pdc                    numeric,
            adr                    integer,
            days_missed            integer,
            absolute_fail_date     date,
            risk_strat             text,
            patient_id             bigint not null,
            mco_member_id          text,
            drug_name              text not null,
            ndc                    text,
            quantity               numeric,
            days_supply            integer,
            last_fill_date         date,
            next_fill_date         date not null,
            max_refill_due         date not null,
            pharmacy_name          text,
            pharmacy_phone         text,
            prescriber_npi         text,
            prescribing_provider   text
        );


        --------------------------------------------------------------------------------------------------------------------
        -- UHC
        if( exists(select 1 from _controls_rtp_patient_mco_med_measures ctx where ctx.payer_id = 47) ) then

            drop table if exists _uhc_attr;
            create temporary table _uhc_attr(patient_id bigint not null primary key, mco_member_id text);
            insert into _uhc_attr(patient_id, mco_member_id)
            select
                 distinct on (mp.patient_id)
                 mp.patient_id,
                 mp.mco_member_id
            from
                prd.mco_patients mp
            where
                mp.payer_id = 47
                and mp.patient_id is not null
            order by mp.patient_id, mp.on_most_recent_file desc /*true first*/, mp.id desc /* tie break */
            ;

            insert into _mco_patient_measure_fills(
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, is_first_fill
            )
            select
                distinct on (unique_key)
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, is_first_fill
            from (
                select
                    concat_ws('::', payer_id, measure_id, measure_year, patient_id, drug_name, last_fill_date) unique_key,
                    *
                from (
                    select
                        distinct
                        ux.payer_id,
                        ux.id              latest_raw_id,
                        ux.inbound_file_id latest_inbound_file_id,
                        -- measure ------------------------------------------------------------------------
                        case when trim(ux.rx_category) = 'MAD' then 'PDC-DR'
                             when trim(ux.rx_category) = 'MAH' then 'PDC-RASA'
                             when trim(ux.rx_category) = 'MAC' then 'PDC-STA'
                        end                                                                    measure_id,
                        coalesce(ux.measure_year::int, extract(year from ux.inserted_at)::int) measure_year,
                        case when new_to_measure ~* 'Y' then true else false end               is_new_to_measure,
                        case when ux.previous_year_failure ~* 'Y' then true else false end     is_prev_year_fail,
                        is_1x_fill ~ 'Yes'                                                     is_first_fill,
                        replace(nullif(ux.pdc_measure_level,''), '%', '')::decimal/100         pdc,
                        nullif(adr_measure_level,'')::int                                      adr,
                        nullif(days_missed_measure_level,'')::int                              days_missed,
                        nullif(absolute_fail_date,'')::date                                    absolute_fail_date,
                        case when risk = 'R' then 'high'
                             when risk = 'Y' then 'medium'
                             when risk = 'G' then 'low'
                        end risk_strat,
                        -- member  ------------------------------------------------------------------------
                        ua.patient_id,
                        ua.mco_member_id,
                        -- drug/rx  -----------------------------------------------------------------------
                        trim(ux.drug_name) drug_name,
                        null::text         ndc,
                        case when ux.quantity_ds = '' then null else nullif(split_part(ux.quantity_ds, '/', 1),'')::numeric end quantity,
                        case when ux.quantity_ds = '' then null else nullif(split_part(ux.quantity_ds, '/', 2),'')::int     end days_supply,
                        nullif(ux.date_of_last_refill,'')::date                                            last_fill_date,
                        nullif(ux.next_refill_due    ,'')::date                                            next_fill_date,
                        greatest(nullif(ux.next_refill_due,'')::date, nullif(ux.max_refill_due ,'')::date) max_refill_due, -- should always be greatest
                        -- pharmacy  -----------------------------------------------------------------------
                        split_part(nullif(ux.pharmacy_name_phone,'')                                            , '/', 1 ) pharmacy_name,
                        staging._cleanup_phone_number(split_part(nullif(ux.pharmacy_name_phone,''), '/', 2)) pharmacy_phone,
                        -- prescriber  ---------------------------------------------------------------------
                        trim(ux.prescriber_npi      ) prescriber_npi,
                        trim(ux.prescribing_provider) prescribing_provider
                    from
                        raw.patient_rx_adherence_roster_uhc ux
                        join _uhc_attr ua on lower(trim(ua.mco_member_id)) = lower(trim(ux.patient_card_id))
                        join fdw_member_doc.payers py on py.id = ux.payer_id
                    where
                        ux.rx_category in ('MAC', 'MAH', 'MAD') --, 'SUPD')
                        and quantity_ds ~* '/'
--                         and inbound_file_id = (select ctx.inbound_file_id from _controls_rtp_patient_mco_med_measures ctx)
                        and (
                            (
                                inbound_file_id = (select max(inbound_file_id) from raw.patient_rx_adherence_roster_uhc)
                                and not exists (select 1 from _controls_rtp_patient_mco_med_measures ctx where inbound_file_id is not null)
                            )
                            or
                            (inbound_file_id = (select ctx.inbound_file_id from _controls_rtp_patient_mco_med_measures ctx))
                        )
                ) x
            ) y
            order by unique_key, latest_inbound_file_id desc, latest_raw_id desc
            ;

        end if;
        -- END UHC
        -- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


        --------------------------------------------------------------------------------------------------------------------
        -- BCBSTN
        if( exists(select 1 from _controls_rtp_patient_mco_med_measures ctx where ctx.payer_id = 38) ) then

            insert into _mco_patient_measure_fills(
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, is_first_fill
            )
            select
                distinct on (unique_key)
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, is_first_fill
            from (
                select
                    concat_ws('::', payer_id, measure_id, measure_year, patient_id, drug_name, last_fill_date) unique_key,

     --  best case adr - approx days missed if started on jan 1
--                     73 - (best_case_treatment_period - (pdc * best_case_treatment_period))::numeric            adr,
                    *
                from (
                    select
                        bx.payer_id,
                        bx.id              latest_raw_id,
                        bx.inbound_file_id latest_inbound_file_id,
                        -- measure ------------------------------------------------------------------------
                        case when bx.measure_name = 'MAD' THEN 'PDC-DR'
                             when bx.measure_name = 'MAH' THEN 'PDC-RASA'
                             when bx.measure_name = 'MAC' THEN 'PDC-STA' end              measure_id,
--                         cb._eoy(current_date) - TO_DATE(nullif(bx.first_fill_date,''), 'MM/DD/YYYY')   treatment_period,
                        extract(year from TO_DATE(bx.absolute_fail_date, 'MM/DD/YYYY'))   measure_year,
                        null::bool                                                        is_new_to_measure,
                        nullif(bx.prior_year_compliance,'') = 'Non-Compliant'             is_prev_year_fail,
                        replace(nullif(bx.pdc_rate,''), '%', '')::decimal/100.0           pdc,
                        null::int                                                         days_missed,
                        TO_DATE(bx.absolute_fail_date, 'MM/DD/YYYY')                      absolute_fail_date,
                        null::text                                                        risk_strat,
                        -- member  ------------------------------------------------------------------------
                        mp.patient_id,
                        coalesce(mp.mco_member_id, bx.member_id)                mco_member_id,
                        -- drug/rx  -----------------------------------------------------------------------
                        trim(bx.drug_name)                                      drug_name,
                        null                                                    ndc,
                        COALESCE(nullif(bx.quantity, '')::numeric, 0)           quantity,
                        COALESCE(nullif(nullif(bx.days_supply,''), '')::numeric::int, 0) days_supply,
                        TO_DATE(nullif(bx.recent_fill_date,''), 'MM/DD/YYYY')   last_fill_date,
                        TO_DATE(nullif(bx.next_fill_date,''), 'MM/DD/YYYY')     next_fill_date,
                        TO_DATE(nullif(bx.next_fill_date,''), 'MM/DD/YYYY')     max_refill_due,
                        bx.recent_fill_date = bx.first_fill_date                is_first_fill,
                        null::numeric                                           adr,
                        -- pharmacy  -----------------------------------------------------------------------
                        nullif(bx.pharmacy_name,'')                             pharmacy_name,
                        nullif(bx.pharmacy_phone_number,'')                     pharmacy_phone,
                        -- prescriber  ---------------------------------------------------------------------
                        null::text                                              prescriber_npi,
                        trim(bx.prescriber_name)                                prescribing_provider
                    from
                        raw.bcbstn_medication_adherence_data bx
                        join prd.mco_patients mp on mp.mco_member_id = LEFT(bx.member_id,9) and mp.payer_id = bx.payer_id
                    where
                        (
                            (
                                inbound_file_id = (select max(inbound_file_id) from raw.bcbstn_medication_adherence_data)
                                and
                                not exists (select 1 from _controls_rtp_patient_mco_med_measures ctx where inbound_file_id is not null)
                            )
                            or
                            (inbound_file_id = (select ctx.inbound_file_id from _controls_rtp_patient_mco_med_measures ctx))
                        )
                ) x
            ) y
            order by unique_key, latest_inbound_file_id desc, latest_raw_id desc
            ;

        end if;
        -- END BCBSTN
        -- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    ------------------------------------------------------------------------------------------------------------------------
    /* Elevance  2 */
        IF (EXISTS( SELECT 1 FROM _controls_rtp_patient_mco_med_measures ctx WHERE ctx.payer_id = 2 ))
        THEN

            INSERT
            INTO
                _mco_patient_measure_fills(unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id,
                                           measure_year,
                                           is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed,
                                           absolute_fail_date, risk_strat,
                                           patient_id, mco_member_id, drug_name, ndc, quantity, days_supply,
                                           last_fill_date, next_fill_date,
                                           max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi,
                                           prescribing_provider, is_first_fill)
            SELECT DISTINCT ON (unique_key)
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id,
                measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed,
                absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply,
                last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi,
                prescribing_provider, is_first_fill
            FROM
                ( SELECT
                      CONCAT_WS('::', payer_id, measure_id, measure_year, patient_id, drug_name,
                                last_fill_date) unique_key
                    , *
                  FROM
                      (
                      SELECT
                            2                                                                                              payer_id
                          , epr.id                                                                                         latest_raw_id
                          , epr.inbound_file_id                                                                            latest_inbound_file_id
                            -- measure ------------------------------------------------------------------------
                          , 'PDC-DR'                                                                                       measure_id
                          , EXTRACT(YEAR FROM epr.inserted_at)                                                             measure_year
                          , NULLIF(TRIM(epr.diabetes_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_new_to_measure
                          , NULLIF(TRIM(epr.diabetes_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_first_fill
                          , epr.diabetes_adherent_prior_year IS NOT DISTINCT FROM 'Y'                                      is_prev_year_fail
                          , REPLACE(
                                    NULLIF(NULLIF(NULLIF(TRIM(epr.diabetes_proportion_of_days_covered_pdc), ''),
                                                  'FIRSTFILL'), '.00%')
                                , '%', '')::DECIMAL / 100                                                                  pdc
                          , NULLIF(TRIM(epr.diabetes_allowable_days), '')::NUMERIC                                         adr
                          , NULL::INT                                                                                      days_missed
                          , NULLIF(TRIM(epr.last_day_to_fail_diabetes), '')::DATE                                          absolute_fail_date
                          , NULL::TEXT                                                                                     risk_strat
                            -- member  ------------------------------------------------------------------------
                          , mp.patient_id
                          , COALESCE(mp.mco_member_id, epr.sbscrbr_id)                                                     mco_member_id
                            -- drug/rx  -----------------------------------------------------------------------
                          , TRIM(epr.diabetes_drug_name_based_on_last_paid_claim)                                          drug_name
                          , NULL                                                                                           ndc
                          , NULL::NUMERIC                                                                                  quantity
                          , NULLIF(TRIM(epr.diabetes_drug_last_days_supply), '')::NUMERIC::INT                             days_supply
                          , NULLIF(TRIM(epr.diabetes_last_fill_date), '')::DATE                                            last_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_diabetes), '')::DATE                                          next_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_diabetes), '')::DATE                                          max_refill_due
                            -- pharmacy  -----------------------------------------------------------------------
                          , NULLIF(TRIM(epr.pharmacy_name), '')                                                            pharmacy_name
                          , NULLIF(TRIM(epr.pharmacy_phone_number), '')                                                    pharmacy_phone
                            -- prescriber  ---------------------------------------------------------------------
                          , NULLIF(TRIM(epr.prescriber_npi_for_diabetes), '')                                              prescriber_npi
                          , NULLIF(TRIM(epr.prescriber_for_diabetes), '')                                                  prescribing_provider
                        FROM
                            raw.elevance_pharmacy_report epr
                            JOIN prd.mco_patients mp ON mp.mco_member_id = epr.sbscrbr_id AND mp.payer_id = 2
                        WHERE
                              NULLIF(TRIM(epr.diabetes_drug_name_based_on_last_paid_claim), '') IS NOT NULL
                          AND (
                                  ( inbound_file_id = ( SELECT MAX(inbound_file_id) FROM raw.elevance_pharmacy_report )
                                      AND
                                      NOT EXISTS ( SELECT 1 FROM
                                                       _controls_rtp_patient_mco_med_measures ctx
                                                   WHERE
                                                       inbound_file_id IS NOT NULL )
                                  )
                                  OR
                                 (inbound_file_id = ( SELECT ctx.inbound_file_id FROM _controls_rtp_patient_mco_med_measures ctx ))
                                  )
                        UNION
                       SELECT
                            2                                                                                              payer_id
                          , epr.id                                                                                         latest_raw_id
                          , epr.inbound_file_id                                                                            latest_inbound_file_id
                            -- measure ------------------------------------------------------------------------
                          , 'PDC-STA'                                                                                      measure_id
                          , EXTRACT(YEAR FROM epr.inserted_at)                                                             measure_year
                          , NULLIF(TRIM(epr.cholesterol_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_new_to_measure
                          , NULLIF(TRIM(epr.cholesterol_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_first_fill
                          , epr.cholesterol_adherent_prior_year IS NOT DISTINCT FROM 'Y'                                   is_prev_year_fail
                          , REPLACE(
                                    NULLIF(NULLIF(NULLIF(TRIM(epr.cholesterol_proportion_of_days_covered_pdc), ''),
                                                  'FIRSTFILL'), '.00%')
                                , '%', '')::DECIMAL / 100                                                                  pdc
                          , NULLIF(TRIM(epr.cholesterol_allowable_days), '')::NUMERIC                                      adr
                          , NULL::INT                                                                                      days_missed
                          , NULLIF(TRIM(epr.last_day_to_fail_cholesterol), '')::DATE                                       absolute_fail_date
                          , NULL::TEXT                                                                                     risk_strat
                            -- member  ------------------------------------------------------------------------
                          , mp.patient_id
                          , COALESCE(mp.mco_member_id, epr.sbscrbr_id)                                                     mco_member_id
                            -- drug/rx  -----------------------------------------------------------------------
                          , TRIM(epr.cholesterol_drug_name_based_on_last_paid_claim)                                       drug_name
                          , NULL                                                                                           ndc
                          , NULL::NUMERIC                                                                                  quantity
                          , NULLIF(TRIM(epr.cholesterol_drug_last_days_supply), '')::NUMERIC::INT                          days_supply
                          , NULLIF(TRIM(epr.cholesterol_last_fill_date), '')::DATE                                         last_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_cholesterol), '')::DATE                                       next_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_cholesterol), '')::DATE                                       max_refill_due
                            -- pharmacy  -----------------------------------------------------------------------
                          , NULLIF(TRIM(epr.pharmacy_name), '')                                                            pharmacy_name
                          , NULLIF(TRIM(epr.pharmacy_phone_number), '')                                                    pharmacy_phone
                            -- prescriber  ---------------------------------------------------------------------
                          , NULLIF(TRIM(epr.prescriber_npi_for_cholesterol), '')                                           prescriber_npi
                          , NULLIF(TRIM(epr.prescriber_for_cholesterol), '')                                               prescribing_provider
                        FROM
                            raw.elevance_pharmacy_report epr
                            JOIN prd.mco_patients mp ON mp.mco_member_id = epr.sbscrbr_id AND mp.payer_id = 2
                        WHERE
                              NULLIF(TRIM(epr.cholesterol_drug_name_based_on_last_paid_claim), '') IS NOT NULL
                          AND (
                                  ( inbound_file_id = ( SELECT MAX(inbound_file_id) FROM raw.elevance_pharmacy_report )
                                      AND
                                      NOT EXISTS ( SELECT 1 FROM
                                                       _controls_rtp_patient_mco_med_measures ctx
                                                   WHERE
                                                       inbound_file_id IS NOT NULL )
                                  )
                                  OR
                                 (inbound_file_id = ( SELECT ctx.inbound_file_id FROM _controls_rtp_patient_mco_med_measures ctx ))
                                  )
                        UNION
                        SELECT
                            2                                                                                              payer_id
                          , epr.id                                                                                         latest_raw_id
                          , epr.inbound_file_id                                                                            latest_inbound_file_id
                            -- measure ------------------------------------------------------------------------
                          , 'PDC-RASA'                                                                                     measure_id
                          , EXTRACT(YEAR FROM epr.inserted_at)                                                             measure_year
                          , NULLIF(TRIM(epr.hypertension_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_new_to_measure
                          , NULLIF(TRIM(epr.hypertension_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_first_fill
                          , epr.hypertension_adherent_prior_year IS NOT DISTINCT FROM 'Y'                                      is_prev_year_fail
                          , REPLACE(
                                    NULLIF(NULLIF(NULLIF(TRIM(epr.hypertension_proportion_of_days_covered_pdc), ''),
                                                  'FIRSTFILL'), '.00%')
                                , '%', '')::DECIMAL / 100                                                                  pdc
                          , NULLIF(TRIM(epr.hypertension_allowable_days), '')::NUMERIC                                     adr
                          , NULL::INT                                                                                      days_missed
                          , NULLIF(TRIM(epr.last_day_to_fail_hypertension), '')::DATE                                      absolute_fail_date
                          , NULL::TEXT                                                                                     risk_strat
                            -- member  ------------------------------------------------------------------------
                          , mp.patient_id
                          , COALESCE(mp.mco_member_id, epr.sbscrbr_id)                                                     mco_member_id
                            -- drug/rx  -----------------------------------------------------------------------
                          , TRIM(epr.hypertension_drug_name_based_on_last_paid_claim)                                      drug_name
                          , NULL                                                                                           ndc
                          , NULL::NUMERIC                                                                                  quantity
                          , NULLIF(TRIM(epr.hypertension_drug_last_days_supply), '')::NUMERIC::INT                         days_supply
                          , NULLIF(TRIM(epr.hypertension_last_fill_date), '')::DATE                                        last_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_hypertension), '')::DATE                                      next_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_hypertension), '')::DATE                                      max_refill_due
                            -- pharmacy  -----------------------------------------------------------------------
                          , NULLIF(TRIM(epr.pharmacy_name), '')                                                            pharmacy_name
                          , NULLIF(TRIM(epr.pharmacy_phone_number), '')                                                    pharmacy_phone
                            -- prescriber  ---------------------------------------------------------------------
                          , NULLIF(TRIM(epr.prescriber_npi_for_hypertension), '')                                          prescriber_npi
                          , NULLIF(TRIM(epr.prescriber_for_hypertension), '')                                              prescribing_provider
                        FROM
                            raw.elevance_pharmacy_report epr
                            JOIN prd.mco_patients mp ON mp.mco_member_id = epr.sbscrbr_id AND mp.payer_id = 2
                        WHERE
                              NULLIF(TRIM(epr.hypertension_drug_name_based_on_last_paid_claim), '') IS NOT NULL
                          AND (
                                  ( inbound_file_id = ( SELECT MAX(inbound_file_id) FROM raw.elevance_pharmacy_report )
                                      AND
                                      NOT EXISTS ( SELECT 1 FROM
                                                       _controls_rtp_patient_mco_med_measures ctx
                                                   WHERE
                                                       inbound_file_id IS NOT NULL )
                                  )
                                  OR
                                 (inbound_file_id = ( SELECT ctx.inbound_file_id FROM _controls_rtp_patient_mco_med_measures ctx ))
                                  )

                        ) x
                  ) y
            where y.patient_id is not null
            ORDER BY unique_key, latest_inbound_file_id DESC, latest_raw_id DESC;

        END IF;
    -- END Elevance
    ------------------------------------------------------------------------------------------------------------------------

    ------------------------------------------------------------------------------------------------------------------------

    /* wellcare 49*/
        if( exists(select 1 from _controls_rtp_patient_mco_med_measures ctx where ctx.payer_id = 49) ) then

            INSERT
            INTO
                _mco_patient_measure_fills(unique_key, payer_id, latest_raw_id, latest_inbound_file_id,
                                           measure_id,
                                           measure_year,
                                           is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed,
                                           absolute_fail_date, risk_strat,
                                           patient_id, mco_member_id, drug_name, ndc, quantity, days_supply,
                                           last_fill_date, next_fill_date,
                                           max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi,
                                           prescribing_provider)
            SELECT DISTINCT ON (payer_id, measure_id, measure_year, patient_id, drug_name)
                -- NOTE: All other plans use the last fill date in unqiue key. We don't have it for Cenetene so we use next_fill_date
                CONCAT_WS('::', payer_id, measure_id, measure_year, patient_id, drug_name,
                          next_fill_date) unique_key
              , payer_id
              , latest_raw_id
              , latest_inbound_file_id
              , measure_id
              , measure_year
              , is_new_to_measure
              , is_prev_year_fail
              , pdc
              , adr
              , days_missed
              , calc_date + adr::INT      absolute_fail_date
              , risk_strat
              , patient_id
              , mco_member_id
              , drug_name
              , ndc
              , quantity
              , days_supply
              , last_fill_date
              , next_fill_date
              , max_refill_due
              , pharmacy_name
              , pharmacy_phone
              , prescriber_npi
              , prescribing_provider
            FROM
                ( SELECT
                      ctn.payer_id
                    , ctn.id                                                          latest_raw_id
                    , ctn.inbound_file_id                                             latest_inbound_file_id
                    , CASE WHEN ctn.measure_key = 'CHOL' THEN 'PDC-STA'
                           WHEN ctn.measure_key = 'DIAB' THEN 'PDC-DR'
                           WHEN ctn.measure_key = 'RASA' THEN 'PDC-RASA' END          measure_id
                    , DATE_PART('year', ctn.next_fill_due_date::DATE)                 measure_year
                    , NULL::BOOLEAN                                                   is_new_to_measure
                    , rl.id IS NOT NULL                                               is_prev_year_fail
                    , ctn.p_dayscovered::NUMERIC                                      pdc
                    , ctn.days_to_nonadh::NUMERIC                                     adr
                    , ctn.days_missed_ytd::INT                                        days_missed
--                  , null::DATE                                                                              absolute_fail_date
                    , (SUBSTRING((meta -> 'extra_args' ->> 'original_name') FROM
                                 '([0-9]{8})\.csv$'))::DATE                           calc_date
                    , NULL                                                            risk_strat
                    , mp.patient_id
                    , mp.mco_member_id
                    , NULLIF(TRIM(ctn.label_name), '')                                drug_name
                    , NULLIF(TRIM(REGEXP_REPLACE(ctn.last_fill_quantity, '\[|\]', '', 'g')),
                             '')::NUMERIC                                             quantity
                    , NULLIF(TRIM(REGEXP_REPLACE(ctn.last_fill_days_supply, '\[|\]', '', 'g')),
                             '')::INT                                                 days_supply
                    , NULLIF(TRIM(ctn.next_fill_due_date), '')::DATE                  next_fill_date
                    , NULLIF(TRIM(ctn.pharmacy_name), '')                             pharmacy_name
                    , NULLIF(TRIM(ctn.pharmacy_phone_number), '')                     pharmacy_phone
                    , NULLIF(TRIM(ctn.prescriber_npi), '')                            prescriber_npi
                    , NULLIF(TRIM(ctn.next_fill_due_date), '')::DATE                  max_refill_due
                    , ctn.last_fill_refill_date::DATE - ctn.last_fill_quantity::INT   last_fill_date
                    , ctn.last_fill_ndc                                               ndc
                    , CONCAT_WS(' ', NULLIF(TRIM(ctn.last_fill_prescriber_first_name), ''),
                                NULLIF(TRIM(ctn.last_fill_prescriber_last_name), '')) prescribing_provider
                  FROM
                      raw.wellcare_medadherence ctn
                      JOIN prd.mco_patients mp ON mp.mco_member_id = ctn.subscriber_id
                      LEFT JOIN prd.patient_med_adherence_red_list rl
                                ON rl.patient_id = mp.patient_id
                                    AND
                                   rl.measure_id = CASE WHEN ctn.measure_key = 'CHOL' THEN 'PDC-STA'
                                                        WHEN ctn.measure_key = 'DIAB' THEN 'PDC-DR'
                                                        WHEN ctn.measure_key = 'RASA'
                                                                                      THEN 'PDC-RASA' END
                                    AND rl.year = DATE_PART('year', ctn.next_fill_due_date::DATE) - 1
                  WHERE
                      NOT EXISTS( SELECT
                                      1
                                  FROM
                                      etl.processed_inbound_files pif
                                  WHERE
                                      pif.inbound_file_id = ctn.inbound_file_id ) ) x
            ORDER BY payer_id, measure_id, measure_year, patient_id, drug_name, next_fill_date DESC;


            INSERT
            INTO
                etl.processed_inbound_files (inbound_file_id, payer_id, processed_to)
            SELECT DISTINCT
                inbound_file_id
              , 49                                 payer_id
              , 'prd.mco_patient_measure_rx_fills' processed_to
            FROM
                raw.wellcare_medadherence ctn
            WHERE
                NOT EXISTS (SELECT 1
                            FROM etl.processed_inbound_files pif
                            WHERE pif.inbound_file_id = ctn.inbound_file_id);



        END IF;
    -- END wellcare
    ------------------------------------------------------------------------------------------------------------------------

    ------------------------------------------------------------------------------------------------------------------------
    /* Humana */
        if( exists(select 1 from _controls_rtp_patient_mco_med_measures ctx where ctx.payer_id = 44) ) then
            INSERT
            INTO
                _mco_patient_measure_fills(unique_key,
                                           payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                                           is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed,
                                           absolute_fail_date, risk_strat,
                                           patient_id, mco_member_id, drug_name, ndc, quantity, days_supply,
                                           last_fill_date, next_fill_date,
                                           max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi,
                                           prescribing_provider, is_first_fill)
            SELECT
                CONCAT_WS('::', payer_id, measure_id, measure_year, patient_id, drug_name, last_fill_date) unique_key
              , payer_id
              , latest_raw_id
              , latest_inbound_file_id
              , measure_id
              , measure_year
              , is_new_to_measure
              , is_prev_year_fail
              , pdc / 100.0 -- 2023-06-19 BP Added div by 100
              , adr
              , days_missed
              , absolute_fail_date
              , risk_strat
              , patient_id
              , mco_member_id
              , drug_name
              , ndc
              , quantity
              , days_supply
              , last_fill_date
              , next_fill_date
              , max_refill_due
              , pharmacy_name
              , pharmacy_phone
              , prescriber_npi
              , prescribing_provider
              , is_first_fill
            FROM
                ( SELECT
                      h.payer_id
                    , h.id                                                                      latest_raw_id
                    , h.inbound_file_id                                                         latest_inbound_file_id
                    , CASE WHEN h.measure = 'ADH-DIABETES' THEN 'PDC-DR'
                           WHEN h.measure = 'ADH-STATINS'  THEN 'PDC-STA'
                           WHEN h.measure = 'ADH-ACE/ARB'  THEN 'PDC-RASA'
                          END                                                                   measure_id
                    , DATE_PART('year', NULLIF(NULLIF(TRIM(h.refill_due), ''), '-')::DATE) measure_year
                    , h.is_new = 'Y'                                                            is_new_to_measure
                    , h.prev_yr_fail = 'Y'                                                      is_prev_year_fail
                    , h.first_fill = 'Y'                                                        is_first_fill
                    , NULLIF(NULLIF(TRIM(h.current_yr_adh_perc), ''), '-')::NUMERIC             pdc
                    , NULL::INT                                                                 adr
                    , NULL::INT                                                                 days_missed
                    , NULLIF(NULLIF(TRIM(h.last_impactable), ''), '-')::DATE                    absolute_fail_date
                    , NULL                                                                      risk_strat
                    , mp.patient_id
                    , mp.mco_member_id
                    , NULLIF(TRIM(h.medication), '')                                            drug_name
                    , NULL                                                                      ndc
                    , NULL::NUMERIC                                                             quantity
                    , NULLIF(NULLIF(TRIM(h.day_supply), ''), '-')::NUMERIC::INT                 days_supply
                    , NULLIF(NULLIF(TRIM(h.last_fill), ''), '-')::DATE                          last_fill_date
                    , NULLIF(NULLIF(TRIM(h.refill_due), ''), '-')::DATE                         next_fill_date
                    , NULLIF(NULLIF(TRIM(h.refill_due), ''), '-')::DATE                         max_refill_due
                    , NULLIF(TRIM(h.rx_name), '')                                               pharmacy_name
                    , NULLIF(TRIM(h.rx_phone), '')                                              pharmacy_phone
                    , NULLIF(TRIM(h.prescriber_npi), '')                                        prescriber_npi
                    , NULLIF(TRIM(h.prescriber), '')                                            prescribing_provider
                  FROM
                      raw.humana_rx_opportunity_detail_report h
                      JOIN prd.mco_patients mp ON mp.mco_member_id = h.humana_patient_id AND mp.patient_id IS NOT NULL
                  WHERE
                        h.measure IN ('ADH-DIABETES', 'ADH-STATINS', 'ADH-ACE/ARB')
                    AND ((inbound_file_id =
                          ( SELECT MAX(inbound_file_id) FROM raw.humana_rx_opportunity_detail_report )
                      AND NOT EXISTS ( SELECT
                                           1
                                       FROM
                                           _controls_rtp_patient_mco_med_measures ctx
                                       WHERE
                                           inbound_file_id IS NOT NULL ))
                      OR
                         (inbound_file_id =
                          ( SELECT ctx.inbound_file_id FROM _controls_rtp_patient_mco_med_measures ctx ))
                            ) ) x
            ; -- 44
        END IF;
    -- END Humana
    ------------------------------------------------------------------------------------------------------------------------

        update prd.mco_patient_measure_rx_fills pxu
            set
                is_new_to_measure      = ux.is_new_to_measure ,
                is_prev_year_fail      = ux.is_prev_year_fail ,
                is_first_fill          = ux.is_first_fill,
                pdc                    = ux.pdc ,
                adr                    = ux.adr ,
                days_missed            = ux.days_missed ,
                absolute_fail_date     = ux.absolute_fail_date ,
                risk_strat             = ux.risk_strat ,
                mco_member_id          = ux.mco_member_id ,
                ndc                    = coalesce(ux.ndc        , pxu.ndc),
                quantity               = coalesce(ux.quantity   , pxu.quantity),
                days_supply            = coalesce(ux.days_supply, pxu.days_supply),
                last_fill_date         = ux.last_fill_date ,
                next_fill_date         = ux.next_fill_date ,
                max_refill_due         = ux.max_refill_due ,
                pharmacy_name          = coalesce(ux.pharmacy_name       , pxu.pharmacy_name       ),
                pharmacy_phone         = coalesce(ux.pharmacy_phone      , pxu.pharmacy_phone      ),
                prescriber_npi         = coalesce(ux.prescriber_npi      , pxu.prescriber_npi      ),
                prescribing_provider   = coalesce(ux.prescribing_provider, pxu.prescribing_provider),
                latest_inbound_file_id = ux.latest_inbound_file_id ,
                latest_raw_id          = ux.latest_raw_id ,
                inbound_file_ids       = pxu.inbound_file_ids || array[ux.latest_inbound_file_id],
                raw_ids                = pxu.inbound_file_ids || array[ux.latest_raw_id],
                updated_at             = now()
        from
            _mco_patient_measure_fills ux
        where
            pxu.unique_key = ux.unique_key
            and (
                       pxu.is_new_to_measure    is distinct from ux.is_new_to_measure
                    or pxu.is_prev_year_fail    is distinct from ux.is_prev_year_fail
                    or pxu.is_first_fill        is distinct from ux.is_first_fill
                    or pxu.pdc                  is distinct from ux.pdc
                    or pxu.adr                  is distinct from ux.adr
                    or pxu.days_missed          is distinct from ux.days_missed
                    or pxu.absolute_fail_date   is distinct from ux.absolute_fail_date
                    or pxu.risk_strat           is distinct from ux.risk_strat
                    or pxu.mco_member_id        is distinct from ux.mco_member_id
                    or pxu.ndc                  is distinct from ux.ndc
                    or pxu.quantity             is distinct from ux.quantity
                    or pxu.days_supply          is distinct from ux.days_supply
                    or pxu.last_fill_date       is distinct from ux.last_fill_date
                    or pxu.next_fill_date       is distinct from ux.next_fill_date
                    or pxu.max_refill_due       is distinct from ux.max_refill_due
                    or pxu.pharmacy_name        is distinct from ux.pharmacy_name
                    or pxu.pharmacy_phone       is distinct from ux.pharmacy_phone
                    or pxu.prescriber_npi       is distinct from ux.prescriber_npi
                    or pxu.prescribing_provider is distinct from ux.prescribing_provider
            )
        ;

        insert into prd.mco_patient_measure_rx_fills (
            unique_key, patient_id, payer_id, measure_id, measure_year,
            is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat, mco_member_id,
            drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date, max_refill_due,
            pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider,
            latest_inbound_file_id, latest_raw_id, is_sent_to_coop_med_adherence, is_sent_to_coop_med_adherence_at,
            inbound_file_ids, raw_ids, is_first_fill, inserted_at, updated_at
        )
        select
            unique_key, patient_id, payer_id, measure_id, measure_year,
            is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat, mco_member_id,
            drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date, max_refill_due,
            pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider,
            latest_inbound_file_id, latest_raw_id, false is_sent_to_coop_med_adherence, null::timestamp is_sent_to_coop_med_adherence_at,
            array[latest_inbound_file_id] inbound_file_ids, array[latest_raw_id] raw_ids, is_first_fill, now() inserted_at, now() updated_at
        from
            _mco_patient_measure_fills ux
        where
            not exists (
                select 1
                from prd.mco_patient_measure_rx_fills rx
                where
                    rx.unique_key = ux.unique_key
            )
        ;
        ----------------------------------------------------------------------------------------------------------------


        --------------------------------------------------------------------------------------------------------------------
        -- STEP 2 ::  LOAD TO COOP
        --------------------------------------------------------------------------------------------------------------------
        drop table if exists _to_process;
        create temporary table _to_process as
        select
            *
        from (
            select
                distinct on (payer_id, measure_id, patient_id)
                mcm.*
            from
                prd.mco_patient_measure_rx_fills mcm
            where
                mcm.measure_year = extract(year from current_date)
                and exists (select 1 from _mco_patient_measure_fills mcp where mcp.patient_id = mcm.patient_id and mcp.measure_id = mcm.measure_id)
            order by mcm.payer_id, mcm.measure_id, mcm.patient_id, greatest(mcm.max_refill_due, mcm.next_fill_date) desc
        ) tp
        where
--             -- ignore if sure_scripts has ever had a measure for that patient, year, measure_id
--             not exists (
--                 select 1
--                 from prd.patient_med_adherence_measures pmam
--                 where
--                     pmam.patient_id = tp.patient_id
--                     and pmam.measure_id = tp.measure_id
--                     and pmam.year = tp.measure_year
--             )
            exists (
                select 1
                from fdw_member_doc.supreme_pizza sp
                where sp.patient_id = tp.patient_id
                  and sp.is_medication_adherence
            )
        ;

        insert into fdw_member_doc_stage.patient_medication_adherence_compliances(patient_id, measure_id, is_processed, closed_patient_task_id, inserted_at, source, yr)
        select
            patient_id, measure_id, false is_processed, null::bigint closed_patient_task_id, now() inserted_at, 'mco' source, tp.measure_year
        from
            _to_process tp
        where
            greatest(tp.next_fill_date, tp.max_refill_due) >= current_date -- intentionally not using >= given the staleness of this data
        ;

        -- non-compliant part
        drop table if exists _to_create_potential_tasks;
        create temporary table _to_create_potential_tasks as
        with potential_tasks as (
            select
                *
            from
                _to_process tp
            where
                tp.is_sent_to_coop_med_adherence is false
                and greatest(tp.next_fill_date, tp.max_refill_due) + 5 < current_date
                and not exists(select 1 -- 2023-06-13 BP: don't create if SS exists for same patient measure
                               from prd.patient_med_adherence_year_measures pmay
                               where pmay.patient_id = tp.patient_id
                                 and pmay.measure_id = tp.measure_id
                                 and pmay.year = tp.measure_year
                               )
            -- BP removed 20230626, not sure why this was here
--                 and not exists (
--                 select 1
--                 from prd.patient_med_adherence_measures pmam
--                 where
--                     pmam.patient_id = tp.patient_id
--                     and pmam.measure_id = tp.measure_id
--                     and pmam.year = tp.measure_year
--             )
        ),
        prev_fills as (
            select
                pt.id,
                array_remove(array_agg(distinct mpr.last_fill_date),null) prev_fill_dates
            from
                potential_tasks pt
                left join prd.mco_patient_measure_rx_fills mpr on mpr.patient_id = pt.patient_id
                                                          and mpr.payer_id = pt.payer_id
                                                          and mpr.measure_year = pt.measure_year
                                                          and mpr.last_fill_date < pt.last_fill_date
                                                          and mpr.measure_id = pt.measure_id
                                                          and coalesce(mpr.ndc,mpr.drug_name) = coalesce(pt.ndc,mpr.drug_name)
                                                          and mpr.id <> pt.id
            group by 1
        )
        select
            pt.*,
            pf.prev_fill_dates
        from
            potential_tasks pt
            join prev_fills pf on pf.id = pt.id
        ;

        update prd.mco_patient_measure_rx_fills mx
            set is_sent_to_coop_med_adherence = true,
                is_sent_to_coop_med_adherence_at = now()
        from
            _to_create_potential_tasks tpt
        where
            tpt.id = mx.id
        ;

        insert into fdw_member_doc_stage.patient_medication_adherences(
            patient_id, measure_id, drug_name, ndc, days_supply, next_fill_date, last_fill_date,
            adjusted_next_fill_date, remaining_refills, prescriber_name, prescriber_npi, pharmacy_name,
            pharmacy_npi, pharmacy_phone, failed_last_year, analytics_id, inserted_at, updated_at,
            prescriber_phone, prev_fill_dates, yr, is_processed, medication_adherence_patient_task_id,
            patient_task_id, uuid, pdc_to_date, adr, absolute_fail_date, source, is_ignored
        )
        select
            tx.patient_id, tx.measure_id, tx.drug_name, tx.ndc, tx.days_supply, next_fill_date, last_fill_date,
            next_fill_date adjusted_next_fill_date, null::int remaining_refills, prescribing_provider prescriber_name,
            prescriber_npi, pharmacy_name, null::text pharmacy_npi, pharmacy_phone,
            rl.patient_id is not null failed_last_year, tx.id analytics_id, now() inserted_at, now() updated_at,
            null::text prescriber_phone, prev_fill_dates, measure_year yr, false is_processed, null::bigint medication_adherence_patient_task_id,
            null::bigint patient_task_id, gen_random_uuid() uuid, pdc pdc_to_date,
            tx.adr, tx.absolute_fail_date, 'mco' source, false is_ignored
        from
            _to_create_potential_tasks tx
            left join prd.patient_med_adherence_red_list rl on rl.patient_id = tx.patient_id
                                                               and rl.measure_id = tx.measure_id
                                                               and rl.year = tx.measure_year - 1
        ;

        -- copy over everything to stage for reporting BP added 2023-06-28
        INSERT INTO
            fdw_member_doc_stage.mco_patient_measure_rx_fills (analytics_id, unique_key, patient_id, payer_id, measure_id, measure_year, is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date, max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, latest_inbound_file_id, latest_raw_id, is_sent_to_coop_med_adherence, is_sent_to_coop_med_adherence_at, inbound_file_ids, raw_ids, inserted_at, updated_at, is_first_fill)
        SELECT
            id, unique_key, patient_id, payer_id, measure_id, measure_year, is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date, max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, latest_inbound_file_id, latest_raw_id, is_sent_to_coop_med_adherence, is_sent_to_coop_med_adherence_at, inbound_file_ids, raw_ids, inserted_at, updated_at, is_first_fill
        FROM prd.mco_patient_measure_rx_fills f
        where not exists(select 1 from fdw_member_doc_stage.mco_patient_measure_rx_fills s where s.analytics_id = f.id);

    ---------------------------
    ---------------------------
    EXCEPTION WHEN OTHERS THEN
        GET DIAGNOSTICS stack = PG_CONTEXT; GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT, exception_detail = PG_EXCEPTION_DETAIL, exception_hint = PG_EXCEPTION_HINT, exception_context = PG_EXCEPTION_CONTEXT;
        rollback;
        error_text = '(1) Message_Text( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
        PERFORM * FROM audit.fn_create_sms_alerts(array['de-analytics-etl'],'etl.sp_patient_med_measures_mco_gaps',error_text::text);
        commit;

        RAISE EXCEPTION 'etl.sp_patient_med_measures_mco_gaps :: %', error_text;

    END;
    COMMIT;
    -------

    --------------------------------------------------------------------------------------------------------------------
    -- STEP 3 ::  Call COOP.stage sproc to process the signal
    --------------------------------------------------------------------------------------------------------------------

    --turn back on this task processing at the end of January 2024 - AH 1/3/24
    /*
    drop table if exists _trashy99;
    create temporary table _trashy99 as
    select 1 from dblink_exec('cb_member_doc', 'call stage.sp_stp_process_med_adherence_tasks()');
    */

END;
$$;

alter procedure etl.sp_patient_med_measures_mco_gaps(bigint, bigint) owner to postgres;

------------------------------------------------------------------------------------------------------------------------
/* GO_TO_NEXT_SPROC  */
------------------------------------------------------------------------------------------------------------------------

CREATE PROCEDURE sp_stp_process_med_adherence_tasks()
    LANGUAGE plpgsql
AS
$$
    DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN

    BEGIN
            -------------------------------------------------------------------------------------
            -- LOAD SCRIPT
            -- temporarily prevent from running
            if (select now() between '2024-01-01' and '2024-01-31') then
                raise exception 'Med adh paused for January';
            END IF;

            -- CLOSE TASKS that the patient is compliant for
            DROP TABLE IF EXISTS _closed_tasks;
            CREATE TEMP TABLE _closed_tasks AS
            with to_close_tasks as (
                update stage.patient_medication_adherence_compliances pmc
                    set closed_patient_task_id = pt.id,
                        is_processed = true
                from
                    public.patient_tasks pt
                    join stage.medication_adherence_measure_id_to_task_types tm on pt.task_type = tm.task_type
                    join public.medication_adherence_patient_task mapt on pt.id = mapt.patient_task_id
                where
                    pmc.is_processed is false
                    and tm.measure_id = pmc.measure_id
                    and pmc.patient_id = pt.patient_id
                    and (
                            pt.status in ('in_progress','new')
                            or
                            mapt.is_system_verified_closed is false -- this will close tasks that are human closed only
                        )
                returning closed_patient_task_id, source
            ),
            pat_med_tasks as (
                UPDATE public.medication_adherence_patient_task mapt
                    set order_status = 'auto_close_picked_up',
                        is_system_verified_closed = true,
                        system_verified_closed_at = now()
                where
                    exists (select 1 from to_close_tasks tct where tct.closed_patient_task_id = mapt.patient_task_id)
            )
            , pat_tasks as (
                 update public.patient_tasks pt
                     set status = 'completed'
                 where
                     pt.status in ('in_progress','new') -- only mark completed if in these status
                     and exists (select 1 from to_close_tasks tct where tct.closed_patient_task_id = pt.id)
                 returning pt.id, pt.assigned_to_id, pt.patient_id, pt.task_type
            ), pat_task_activity as (
                INSERT
                INTO
                    public.patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at,
                                                    updated_at)
                SELECT distinct
                    pt.id
                  , 2
                  , 'completed'
                  , null
                  , 'system_auto_closed'
                  , NOW()
                  , NOW()
                FROM
                    pat_tasks pt
            )
            select * from pat_tasks;

            -- Notify on closed task
            INSERT
            INTO
                public.oban_jobs (queue, worker, max_attempts, args)
            SELECT DISTINCT
                'default'                           queue
              , 'MD.Notifications.SendNotification' worker
              , 3                                   max_attempts
              , JSONB_BUILD_OBJECT(
                        'type_id', 'medication_adherence_task',
                        'transports', ARRAY ['web'],
                        'user_id', pt.assigned_to_id,
                        'opts', JSONB_BUILD_OBJECT(
                                'task_id', pt.id,
                                'type', 'closed' -- 're opened' or 'closed'
                            )
                    )                               args
            FROM
                _closed_tasks pt
           ;

            -- close call cadence off track when task is closed
            update public.patient_task_call_cadence_off_track ot
            set is_current = false, updated_at = now()
            from _closed_tasks ct
            where ot.patient_task_id = ct.id
            and ot.is_current
            ;


--             Create follow up tasks for tasks closed where patient wants to move to mail order
            WITH
                follow_up_tasks AS (
                    INSERT
                        INTO
                            public.patient_tasks(patient_id, assigned_to_id, task_type, due_at, priority, status,
                                                 created_by_id, inserted_at, updated_at)
                            SELECT
                                ct.patient_id
                              , ct.assigned_to_id
                              , ct.task_type || '_followup'
                              , current_date
                              , 'normal'
                              , 'new'
                              , 2
                              , NOW()
                              , NOW()
                            FROM
                                _closed_tasks ct
                                JOIN public.medication_adherence_patient_task mapt ON mapt.patient_task_id = ct.id
                            WHERE
                                mapt.order_placed = 'yes_mail_order'
                            RETURNING id, patient_id, task_type)
            INSERT
            INTO
                public.medication_adherence_followup_patient_task (patient_task_id, associated_patient_task_id)
            SELECT
                fut.id
              , ct.id
            FROM
                follow_up_tasks fut
            join _closed_tasks ct on ct.patient_id = fut.patient_id and (ct.task_type || '_followup') = fut.task_type;


            -- once processed, anything left is old
            update stage.patient_medication_adherence_compliances pmc
                    set is_processed = true
            where is_processed is false
            ;

            --------------------------------------------------------------------------------
            -- always update per AG 4/25/23
            -- IGNORE IF MCO Source & Patient has SURE_SCRIPTS Measure DATA ----------------
--             update stage.patient_medication_adherences f
--                 set is_processed = true,
--                     is_ignored   = true,
--                     updated_at   = now()
--             where
--                 f.is_processed is false
--                 and f.source = 'mco'
--                 and (
--                         exists (
--                             select
--                                 1
--                             from
--                                 stage.patient_medication_adherences mx
--                             where
--                                 mx.source = 'sure_scripts'
--                                 and mx.yr = f.yr
--                                 and mx.patient_id = f.patient_id
--                                 and mx.measure_id = f.measure_id
--                         )
--                         or
--                         exists (
--                             select
--                                 1
--                             from
--                                 stage.patient_medication_adherence_compliances mx
--                             where
--                                 mx.source = 'sure_scripts'
--                                 and mx.yr = f.yr
--                                 and mx.patient_id = f.patient_id
--                                 and mx.measure_id = f.measure_id
--                         )
--             )
--             ;


            drop table if exists _to_do;
            create temporary table _to_do as
            select
                patient_id, quality_measure_id, year, drug_name, last_fill_date,
                next_fill_date, pharmacy, prescribing_provider_name, created_by_id,
                updated_by_id, inserted_at, updated_at, rx_days_supply, prescriber_npi, ndc,
                analytics_id, adjusted_next_fill_date, measure_id raw_measure_id, failed_last_year,
                stage_uuid,
                days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date, calc_to_date, days_needed_thru_eoy,
                pdc_to_date, adr, source, absolute_fail_date, prev_fill_dates,
                null:: bigint wip_patient_task_id,
                null::bigint  wip_medication_adherence_patient_task_id,
                null::bigint  wip_patient_medication_adherence_id,
                pharmacy_phone
            from (
                select
                    patient_id, tm.quality_measure_id, yr "year", drug_name, last_fill_date,
                    next_fill_date, f.pharmacy_name pharmacy, f.prescriber_name prescribing_provider_name, 2 created_by_id,
                    2 updated_by_id, now() inserted_at, now() updated_at, f.days_supply rx_days_supply, prescriber_npi, ndc,
                    analytics_id, adjusted_next_fill_date, f.measure_id, failed_last_year, f.uuid stage_uuid,
                    days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date, calc_to_date, days_needed_thru_eoy, pdc_to_date, adr,
                    prev_fill_dates, absolute_fail_date,
                    source, f.pharmacy_phone
                from
                    stage.patient_medication_adherences f
                    join stage.medication_adherence_measure_id_to_task_types tm on f.measure_id = tm.measure_id
                where
                    f.is_processed is false
            ) x
            ;


            drop table if exists _to_crupdate_task;
            create temporary table _to_crupdate_task as
            select
                *, gen_random_uuid() grouper,
                case when patient_task_id is not null then 'update'
                                                      else 'create'
                end do_action
            from (
                select
                    td.patient_id,
                    ctm.user_id assigned_to_id,
                    tm.task_type,
                    current_date due_at,
                    null::text instructions,
                    'new' status,
                    null::text notes,
                    null::text subtask_type,
                    '{}'::int[] subscribers,
                    2 created_by_id,
                    2 modified_by_id,
                    now() inserted_at,
                    now() updated_at,
                    td.raw_measure_id,
                    td.days_covered_to_date,
                    td.days_covered_to_period_end,
                    td.days_to_cover_to_date,
                    td.calc_to_date,
                    td.days_needed_thru_eoy,
                    td.pdc_to_date,
                    td.adr,
                    td.absolute_fail_date,
--                     td.prev_fill_dates, -- BP 2023-07-21 moved to agg below, was breaking group by as different drugs for same measure had diff prev fills
                    -- aggregates below
                    'critical' priority, -- 2023-12-06 BP made all med adh tasks critical per banu
--                     case when bool_or(td.failed_last_year) then 'high' else 'normal' end priority,
                    max(array_length(td.prev_fill_dates, 1)) n_max_prev_fill_dates,
                    max(pt.id) patient_task_id,
                    array_agg(td.analytics_id) analytics_ids,
                    array_agg(stage_uuid) stage_uuids
                from
                    _to_do td
                    join stage.medication_adherence_measure_id_to_task_types tm on td.raw_measure_id = tm.measure_id
                    join public.patients p on td.patient_id = p.id
                    join public.care_teams ct on p.care_team_id = ct.id
                    join public.care_team_members ctm on ct.id = ctm.care_team_id and ctm.role = 'health_navigator'
                    left join public.patient_tasks pt on pt.patient_id = td.patient_id
                                                         and pt.task_type = tm.task_type
                                                         and pt.status in ('in_progress','new')
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22 --,23
            ) x
            ;

            -- task already exists, update
            update _to_do td
                set wip_patient_task_id = tct.patient_task_id
            from
                _to_crupdate_task tct
            where
                tct.do_action = 'update'
                and td.stage_uuid = any(tct.stage_uuids)
            ;

            -- create new tasks
            with insrt as (
                insert into public.patient_tasks(
                    patient_id, assigned_to_id, task_type, due_at, instructions, status, notes,
                    subtask_type, subscribers, created_by_id, modified_by_id, inserted_at,
                    updated_at, priority, grouper
                )
                select
                    patient_id, assigned_to_id, task_type, due_at, instructions, status, notes,
                    subtask_type, subscribers, created_by_id, modified_by_id, inserted_at,
                    updated_at, priority, grouper
                from _to_crupdate_task tct
                where tct.do_action = 'create'
                  and (tct.adr > 0 -- BP Added 2023-04-20 per Banu issue #1100
                      OR tct.absolute_fail_date > current_date)
                  and (tct.adr > 5 or n_max_prev_fill_dates >= 1) -- BP Added 2023-06-27 per issue #2539
                returning id patient_task_id, grouper
            )
            update _to_do td
                set wip_patient_task_id = i.patient_task_id
            from
                insrt i
                join _to_crupdate_task tct on tct.grouper = i.grouper
            where
                td.stage_uuid = any(tct.stage_uuids)
            ;

            drop table if exists _ids;
            create temporary table _ids as
            with ids as (
                insert into public.patient_medication_adherences(
                    patient_id, quality_measure_id, year, drug_name, last_fill_date,
                    next_fill_date, pharmacy, prescribing_provider_name, created_by_id,
                    updated_by_id, inserted_at, updated_at, rx_days_supply, prescriber_npi, ndc,
                    analytics_id, adjusted_next_fill_date, failed_last_year,
                    days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date,
                    calc_to_date, days_needed_thru_eoy, pdc_to_date, adr, source,
                    absolute_fail_date, prev_fill_dates, measure_id, pharmacy_phone
                )
                select
                    patient_id, quality_measure_id, year, drug_name, last_fill_date,
                    next_fill_date, pharmacy, prescribing_provider_name, created_by_id,
                    updated_by_id, inserted_at, updated_at, rx_days_supply, prescriber_npi, ndc,
                    analytics_id, adjusted_next_fill_date, failed_last_year,
                    days_covered_to_date, days_covered_to_period_end, days_to_cover_to_date,
                    calc_to_date, days_needed_thru_eoy, pdc_to_date, adr, source,
                    absolute_fail_date, prev_fill_dates, raw_measure_id, pharmacy_phone
                from (
                    select
                        ma.*
                    from
                        _to_do ma
                    where ma.wip_patient_task_id is not null
                ) x
                returning id patient_medication_adherence_id
            )
            select * from ids
            ;

            update _to_do td
                set wip_patient_medication_adherence_id =  pma.id
            from
                public.patient_medication_adherences pma
            where
                pma.analytics_id = td.analytics_id
                and pma.source = td.source
                and coalesce(pma.ndc, pma.drug_name) = coalesce(td.ndc, td.drug_name)
                and exists (select 1 from _ids i where i.patient_medication_adherence_id = pma.id)
            ;


            insert into public.medication_adherence_patient_task(
                patient_task_id, patient_medication_adherence_id, medication_status,
                updated_by_id, inserted_at, updated_at
            )
            select
                distinct
                ma.wip_patient_task_id, ma.wip_patient_medication_adherence_id patient_medication_adherence_id,
                null::text medication_status,
                2 updated_by_id, now() inserted_at, now() updated_at
            from
                _to_do ma
            where ma.wip_patient_medication_adherence_id is not null
            ;

            update _to_do td
                set wip_medication_adherence_patient_task_id = map.id
            from
                public.medication_adherence_patient_task map
            where
                map.patient_task_id = td.wip_patient_task_id
                and map.patient_medication_adherence_id = td.wip_patient_medication_adherence_id
            ;

            update stage.patient_medication_adherences pu
                set
                    patient_medication_adherence_id      = td.wip_patient_medication_adherence_id,
                    patient_task_id                      = td.wip_patient_task_id,
                    medication_adherence_patient_task_id = td.wip_medication_adherence_patient_task_id,
                    is_processed                         = true
            from
                _to_do td
            where
                td.stage_uuid = pu.uuid
            ;
            /*
            Reopen closed tasks
            BP 2023-04-07
            */
            WITH pt_upd   AS (
                    update public.patient_tasks pt
                        set status = 'in_progress',
                            priority = 'critical',
                            updated_at = NOW(),
                            -- BP 2023-05-19 https://github.com/Main-Street-Health/member-doc/issues/1715
                            start_date = case when mapt.order_status = 'attempts_exhausted'
                                -- BP 2023-08-22 https://github.com/Main-Street-Health/member-doc/issues/2848
                                              then (date_trunc('month', now()) + '1 month'::interval)::date
                                              else current_date end,
                            due_at = case when mapt.order_status = 'attempts_exhausted'
                                -- BP 2023-09-29 https://github.com/Main-Street-Health/member-doc/issues/5052
                                              then (date_trunc('month', now()) + '1 month'::interval)::date
                                              else current_date end
                    from
                        public.medication_adherence_patient_task mapt
                        JOIN public.patient_medication_adherences pma ON pma.id = mapt.patient_medication_adherence_id
                        JOIN public.patient_med_adherence_year_measures pmay on pmay.patient_id = pma.patient_id and pmay.measure_id = pma.measure_id and pmay.year = pma.year
                        JOIN public.supreme_pizza sp on sp.is_medication_adherence
                        JOIN public.referring_partners rp on sp.primary_referring_partner_id = rp.id and rp.organization_id != 7 -- hard bassett exclusion
                    where
                        sp.patient_id = pt.patient_id
                        and pt.id = mapt.patient_task_id
                        and pt.status = 'completed'
                        and pma.source = 'sure_scripts'
                        and pmay.adr > 0
                        and NOT mapt.is_system_verified_closed
                        and mapt.pharmacy_not_found is not true
                        and mapt.order_status      IS DISTINCT FROM 'not_paying_through_insurance'
                        and mapt.order_status      IS DISTINCT FROM 'prn'
                        and mapt.order_status      IS DISTINCT FROM 'patient_refused'
                        and mapt.order_status      IS DISTINCT FROM 'provider_no_more_outreach'
                        and mapt.order_substatus   IS DISTINCT FROM 'provider_no_more_outreach'
                        and mapt.medication_status IS DISTINCT FROM 'discontinued'
                        and (
                                -- BP added 2023-09-25 per Jake/Amy/Cody meeting
                                -- if pharmacy_verified_fill we only care if part d and the manually entered fill date has populated
                                mapt.order_status IS DISTINCT FROM 'pharmacy_verified_fill'
                                OR (
                                    mapt.part_d_covered = 'covered'
                                    and mapt.fill_date + mapt.duration + 5 < now()::date
                                )
                        )
                        AND NOT ( -- exclude ones that have had attempts_exhausted order status more than once
                                 mapt.order_status = 'attempts_exhausted'
                                 AND (
                                   SELECT count(*) FROM
                                        public.medication_adherence_order_status_history h
                                   WHERE h.medication_adherence_task_id = mapt.id
                                     AND h.current_order_status = 'attempts_exhausted'
                                ) > 1
                        )
                        and coalesce(mapt.expected_discharge_date, mapt.visit_date, pt.updated_at) + '5 days'::INTERVAL < NOW()
                        AND not exists(select 1
                                           from public.patient_tasks pt2
                                           where pt2.patient_id = pt.patient_id
                                              and pt2.task_type = pt.task_type
                                              and pt2.id > pt.id
                        )
                    returning pt.id , pt.assigned_to_id
            ), mapt_upd AS (
                update public.medication_adherence_patient_task mapt
                    set is_task_reopened = TRUE,
                        updated_at = NOW(),
                        previous_order_status = mapt.order_status,
                        order_status = null
                where EXISTS(SELECT 1 FROM pt_upd pu WHERE pu.id = mapt.patient_task_id)
                returning mapt.patient_task_id
            ),
                pat_task_activity_upd as (
                INSERT
                INTO
                    public.patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at,
                                                    updated_at)
                SELECT distinct
                    pu.id
                  , 2
                  , 'update_status'
                  , 'in_progress'
                  , 'reopened'
                  , NOW()
                  , NOW()
                FROM
                    pt_upd pu
            )
            INSERT
            INTO
                public.oban_jobs (queue, worker, max_attempts, args)
            SELECT DISTINCT
                'default'                           queue
              , 'MD.Notifications.SendNotification' worker
              , 3                                   max_attempts
              , JSONB_BUILD_OBJECT(
                        'type_id', 'medication_adherence_task',
                        'transports', ARRAY ['web'],
                        'user_id', pu.assigned_to_id,
                        'opts', JSONB_BUILD_OBJECT(
                                'task_id', mu.patient_task_id,
                                'type', 'reopened' -- 'reopened' or 'closed'
                            )
                    )                               args
            FROM
                mapt_upd mu
                JOIN pt_upd pu ON pu.id = mu.patient_task_id
           ;

            UPDATE public.patient_medication_adherences pma
            SET patient_med_adherence_year_measure_id = pmay.analytics_pmay_id
            FROM
                public.patient_med_adherence_year_measures pmay
            WHERE
                  pma.patient_med_adherence_year_measure_id ISNULL
              AND pma.analytics_id IS NOT NULL
              AND pmay.patient_id = pma.patient_id
              AND pmay.measure_id = pma.measure_id
              AND pmay.year = pma.year
            ;


            -- Cancel tasks with adr <=0 or adr <=5 when fill count is 1
            -- https://github.com/Main-Street-Health/member-doc/issues/1680
            -- https://github.com/Main-Street-Health/member-doc/issues/2539
            DROP TABLE IF EXISTS _cancel_adr_zeros;
            CREATE TEMP TABLE _cancel_adr_zeros AS
            SELECT distinct
                pt.id, pmay.adr
            FROM
                public.patient_tasks pt
                JOIN public.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
                JOIN public.patient_medication_adherences pma ON mapt.patient_medication_adherence_id = pma.id
                JOIN public.patient_med_adherence_year_measures pmay ON pmay.patient_id = pma.patient_id
                    AND pmay.measure_id = pma.measure_id
                    AND pmay.year = pma.year
            WHERE
                  pt.status IN ('new', 'in_progress')
              AND (
                  pmay.adr <= 0
                  or (pmay.adr <= 5 and pmay.fill_count < 2) -- BP Added 2023-06-27
              )
              AND NOT EXISTS(
                  select 1 from _to_do td where td.wip_patient_task_id = pt.id
            )
            ;

            INSERT
            INTO
                public.patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at,
                                                updated_at)
            SELECT
                ttc.id
              , 2
              , 'closed'
              , CASE WHEN ttc.adr <= 0 THEN 'Auto-Closed-ADR <=0'
                     ELSE 'Auto-Closed: 1 Fill and Minimal Days Remaining.' END
              , NULL
              , NOW()
              , NOW()
            FROM
                _cancel_adr_zeros ttc;

            UPDATE public.patient_tasks pt
            SET status = 'closed', updated_at = NOW()
            FROM
                _cancel_adr_zeros ttc
            WHERE
                pt.id = ttc.id;

            -- cancel tasks due to exclusion
            DROP TABLE IF EXISTS _cancel_tasks_for_exclusions;
            CREATE TEMP TABLE _cancel_tasks_for_exclusions AS
            SELECT distinct pt.id, string_agg(ex.exclusion_reason, ',') reason
            FROM
                stage.patient_med_adherence_exclusions ex
                JOIN public.patient_medication_adherences pma
                     ON pma.patient_id = ex.patient_id AND pma.measure_id = ex.measure_id AND pma.year = ex.year
                JOIN public.medication_adherence_patient_task mapt ON pma.id = mapt.patient_medication_adherence_id
                JOIN public.patient_tasks pt ON pt.id = mapt.patient_task_id
            WHERE
                pt.status IN ('new', 'in_progress')
            GROUP BY 1
            ;

            INSERT
            INTO
                public.patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at, updated_at)
            SELECT ttc.id, 2, 'cancelled', ttc.reason, null, NOW(), NOW()
            FROM
                _cancel_tasks_for_exclusions ttc;

            UPDATE public.patient_tasks pt
            SET status = 'cancelled', updated_at = NOW()
            FROM
                _cancel_tasks_for_exclusions ttc
            WHERE
                pt.id = ttc.id;


------------------------------------------------------------------------------------------------------------------------
/*  RX Fill Tasks
    These are one hitters ie one script closes the measure per year
    Considered quality measures, not med adherence but they are still based on sure scripts data
*/
------------------------------------------------------------------------------------------------------------------------
            -- Completed: have received SS data
            DROP TABLE IF EXISTS _rx_fill_tasks_to_complete;
            CREATE TEMP TABLE _rx_fill_tasks_to_complete AS
                SELECT
                    pt.id patient_task_id
                  , pqm.id pqm_id
                  , mpqm.id mpqm_id
                FROM
                    public.patient_tasks pt
                    JOIN public.patient_quality_measures_tasks pqmt ON pt.id = pqmt.patient_task_id
                    JOIN public.patient_quality_measures pqm ON pqm.id = pqmt.patient_measure_id
                    JOIN stage.medication_adherence_measure_id_to_task_types m_to_tt
                         ON m_to_tt.task_type = pt.task_type
                    JOIN public.prescription_fill_patient_task pfpt ON pt.id = pfpt.patient_task_id
                    JOIN stage.patient_rx_fill_measures fm ON fm.patient_id = pt.patient_id
                        AND m_to_tt.measure_id = fm.measure_id
                        AND fm.year = pqm.year
                    LEFT JOIN public.msh_patient_quality_measures mpqm on pqm.id = mpqm.patient_quality_measure_id
                WHERE
                     pt.status IN ('new', 'in_progress')
                  OR (pt.status = 'completed' AND NOT pfpt.is_system_verified_closed);

            -- pqm
            WITH
                upd  AS (
                    UPDATE public.patient_quality_measures pqm
                        SET status = 'closed_pending', updated_at = NOW()
                        FROM _rx_fill_tasks_to_complete ttc
                        WHERE ttc.pqm_id = pqm.id
                            AND pqm.status IN ('open', 'in_progress')
                        RETURNING pqm.id )
              , ins  AS (
                INSERT
                    INTO
                        public.patient_measure_status_history(patient_measure_id, status, changed_at, changed_by_id)
                        SELECT
                            id
                          , 'closed_pending'
                          , NOW()
                          , 2
                        FROM
                            upd
                        RETURNING patient_measure_id )
                -- mpqm
              , upd2 AS (
                UPDATE public.msh_patient_quality_measures mpqm
                    SET substatus = NULL, updated_at = NOW()
                    FROM ins ttc
                    WHERE ttc.patient_measure_id = mpqm.patient_quality_measure_id
                        AND mpqm.substatus IS NOT NULL
                    RETURNING mpqm.id )
            INSERT
            INTO
                public.msh_patient_measure_substatus_history (msh_patient_quality_measure_id, substatus, changed_at, changed_by_id)
            SELECT
                id
              , NULL
              , NOW()
              , 2
            FROM
                upd2
            ;

            -- Complete patient tasks
            WITH
                pqmt           AS (
                UPDATE public.prescription_fill_patient_task pqmt
                    SET is_system_verified_closed = TRUE, system_verified_closed_at = NOW(), updated_at = NOW()
                    FROM _rx_fill_tasks_to_complete ttc
                    WHERE ttc.patient_task_id = pqmt.patient_task_id
                    AND NOT is_system_verified_closed
                    RETURNING pqmt.patient_task_id )
            , comp AS (
                UPDATE public.patient_tasks pt
                SET status = 'completed' , updated_at = now()
                FROM
                    pqmt
                WHERE
                    pqmt.patient_task_id = pt.id
                and pt.status != 'completed'
                returning pt.id
            )
                INSERT INTO
                    PUBLIC.patient_task_activities (patient_task_id, user_id, ACTION, VALUE, reason, inserted_at,
                                                    updated_at)
                SELECT DISTINCT
                    c.id
                  , 2
                  , 'completed'
                  , null
                  , 'system_auto_closed'
                  , NOW()
                  , NOW()
                FROM
                    comp c
            ;

            -- Closed: when patient refused and provider no more outreach added 2023-11-01 per banu mm
            DROP TABLE IF EXISTS _closed_pat_refuse_rx_fill;
            CREATE TEMP TABLE _closed_pat_refuse_rx_fill AS
            SELECT
                pt.id                   patient_task_id
              , pqmt.patient_measure_id pqm_id
              , mpqm.id                 mpqm_id
            FROM
                public.patient_tasks pt
                JOIN public.prescription_fill_patient_task pfpt ON pt.id = pfpt.patient_task_id
                JOIN public.patient_quality_measures_tasks pqmt ON pt.id = pqmt.patient_task_id
                JOIN public.msh_patient_quality_measures mpqm
                     ON mpqm.patient_quality_measure_id = pqmt.patient_measure_id
            WHERE
                  pt.status IN ('new', 'in_progress', 'completed')
              AND pfpt.order_status = 'patient_refused'
              AND pfpt.order_substatus = 'provider_no_more_outreach'
              AND not pfpt.is_system_verified_closed
            ;

            INSERT
            INTO
                PUBLIC.patient_task_activities (patient_task_id, user_id, ACTION, VALUE, reason, inserted_at,
                                                updated_at)
            SELECT DISTINCT
                ttc.patient_task_id
              , 2
              , 'closed'
              , NULL
              , 'patient_refused'
              , NOW()
              , NOW()
            FROM
                _closed_pat_refuse_rx_fill ttc;

            UPDATE public.patient_tasks pt
            SET status = 'closed'
            FROM
                _closed_pat_refuse_rx_fill ttc
            WHERE
                 ttc.patient_task_id = pt.id;

            -- qm statuses
            UPDATE public.patient_quality_measures pqm
            SET refused_at = NOW(), updated_at = NOW()
            FROM
                _closed_pat_refuse_rx_fill ttc
            WHERE
                ttc.pqm_id = pqm.id;

            WITH
                upd  AS (
                    UPDATE public.patient_quality_measures pqm
                        SET status = 'open', updated_at = NOW(), refused_at = now()
                        FROM _closed_pat_refuse_rx_fill ttc
                        WHERE ttc.pqm_id = pqm.id
                        and pqm.status not in ('closed_pending', 'closed_system', 'closed_reveleer', 'lost', 'open')
                        RETURNING pqm.id )
                INSERT
                    INTO
                        public.patient_measure_status_history(patient_measure_id, status, changed_at, changed_by_id)
                        SELECT
                            id
                          , 'open'
                          , NOW()
                          , 2
                        FROM
                            upd ;

                -- mpqm
            WITH
                upd2 AS (
                    UPDATE public.msh_patient_quality_measures mpqm
                        SET substatus = 'refused_no_more_outreach', updated_at = NOW()
                        FROM _closed_pat_refuse_rx_fill ttc
                        WHERE ttc.mpqm_id = mpqm.id
                            and mpqm.substatus is DISTINCT FROM 'refused_no_more_outreach'
                        RETURNING mpqm.id )
            INSERT
            INTO
                public.msh_patient_measure_substatus_history (msh_patient_quality_measure_id, substatus, changed_at, changed_by_id)
            SELECT
                id
              , 'refused_no_more_outreach'
              , NOW()
              , 2
            FROM
                upd2
            ;


            -- Reopen tasks (needs to come after closing of tasks)
            WITH
                tasks_to_reopen AS ( SELECT
                                         pt.id patient_task_id
                                     FROM
                                         public.patient_tasks pt
                                         JOIN public.prescription_fill_patient_task pfpt ON pt.id = pfpt.patient_task_id
                                     WHERE
                                           pt.status = 'completed'
                                       AND pt.updated_at + '5 days' < NOW()
                                       AND NOT pfpt.is_system_verified_closed
                                       and pfpt.pharmacy_not_found is not true
                                       and pfpt.order_status IS DISTINCT FROM 'not_paying_through_insurance'
                                       and (
                                            -- BP added 2023-10-19
                                            -- if pharmacy_verified_fill we only care if part d and the manually entered fill date has populated
                                            pfpt.order_status IS DISTINCT FROM 'pharmacy_verified_fill'
                                            OR (
                                                pfpt.part_d_covered = 'covered'
                                                and pfpt.fill_date + pfpt.duration + 5 < now()::date
                                            )
                                    )
              )
              , pqmt            AS (
                UPDATE public.prescription_fill_patient_task pqmt
                    SET is_task_reopened = TRUE, updated_at = NOW()
                    FROM tasks_to_reopen ttr
                    WHERE ttr.patient_task_id = pqmt.patient_task_id
                    RETURNING pqmt.patient_task_id )
            UPDATE public.patient_tasks pt
            SET status = 'in_progress'
            FROM
                pqmt
            WHERE
                pqmt.patient_task_id = pt.id;

            -- Create New Tasks
            -- BP update 20230830: No longer require cca visit, instead look at substatus history for anchor date
            -- BP update 20230830: Update to EMoon query to avoid dupe cca's
            DROP TABLE IF EXISTS _patient_med_fill_tasks_to_create;
            CREATE TEMP TABLE _patient_med_fill_tasks_to_create AS
            SELECT DISTINCT pqm.patient_id,
                 m_to_tt.task_type,
                 mpqm.patient_quality_measure_id,
                 last_recon.cca_worksheet_id
            FROM public.patient_quality_measures pqm
                 JOIN public.msh_patient_quality_measures mpqm ON pqm.id = mpqm.patient_quality_measure_id
                 JOIN public.msh_patient_measure_substatus_history ssh ON mpqm.id = ssh.msh_patient_quality_measure_id AND ssh.substatus = 'pending_rx_fill'
                 JOIN public.supreme_pizza sp ON sp.patient_id = pqm.patient_id AND sp.is_quality_measures
                 JOIN public.msh_referring_partner_feature_config rpfc ON rpfc.referring_partner_id = sp.primary_referring_partner_id AND rpfc.recon_gaps_followup_tasks_start_date <= current_date
                 JOIN public.quality_measures qm ON pqm.measure_id = qm.id
                 JOIN stage.medication_adherence_measure_id_to_task_types m_to_tt ON m_to_tt.quality_measure_id = qm.id
                 LEFT JOIN LATERAL (
                      SELECT DISTINCT ON (wpm.patient_quality_measure_id) wpm.patient_quality_measure_id,
                           cca.id cca_worksheet_id
                      FROM public.msh_cca_worksheet_patient_quality_measures wpm
                           JOIN public.msh_cca_worksheets cca ON cca.id = wpm.msh_cca_worksheet_id AND cca.status = 'completed'
                           JOIN public.visits v ON v.id = cca.visit_id
                      WHERE wpm.patient_quality_measure_id = pqm.id
                      ORDER BY wpm.patient_quality_measure_id,
                           v.date DESC
                      LIMIT 1
                 ) AS last_recon ON TRUE
            WHERE mpqm.substatus = 'pending_rx_fill'
                 AND pqm.is_active
                 AND pqm.measure_id IN (12, 17, 18)
                 AND pqm.year = date_part('year', now())
                 AND ssh.changed_at + '5 days'::INTERVAL < NOW()
                 AND NOT exists(
                     select 1
                     from public.patient_quality_measures_tasks pqmt
                     JOIN public.patient_tasks pt ON pqmt.patient_measure_id = pqm.id
                     where pqmt.patient_task_id = pt.id
                       and pt.task_type in ('prescription_fill_statin_cvd', 'prescription_fill_statin_diabetes', 'prescription_fill_osteoporosis')
                )

            ;

            WITH
                ins  AS (
                    INSERT
                        INTO
                            public.patient_tasks(patient_id, assigned_to_id, task_type, due_at, priority, status,
                                                 created_by_id, inserted_at, updated_at)
                            SELECT DISTINCT
                                ttc.patient_id
                              , ctm.user_id
                              , ttc.task_type
                              , current_date
                              , 'normal'
                              , 'new'
                              , 2
                              , NOW()
                              , NOW()
                            FROM
                                _patient_med_fill_tasks_to_create ttc
                                JOIN public.patients p ON p.id = ttc.patient_id
                                JOIN public.care_teams ct ON p.care_team_id = ct.id and not ct.deleted
                                JOIN public.care_team_members ctm
                                     ON ctm.care_team_id = ct.id AND ctm.role = 'health_navigator'
                            RETURNING id, patient_id, task_type )
              , pqmt AS (
                INSERT INTO public.patient_quality_measures_tasks(patient_task_id, patient_measure_id, inserted_at,
                                                                  updated_at, cca_worksheet_id)
                    SELECT
                        i.id
                      , ttc.patient_quality_measure_id
                      , NOW()
                      , NOW()
                      , ttc.cca_worksheet_id
                    FROM
                        ins i
                        JOIN _patient_med_fill_tasks_to_create ttc
                             ON i.patient_id = ttc.patient_id AND i.task_type = ttc.task_type
                       RETURNING patient_task_id, patient_measure_id, cca_worksheet_id )
            INSERT
            INTO
                public.prescription_fill_patient_task (patient_task_id, inserted_at, updated_at, cca_worksheet_id)
            SELECT
                patient_task_id
              , NOW()
              , NOW()
              , cca_worksheet_id
            FROM
                pqmt;

-- ------------------------------------------------------------------------------------------------------------------------
-- /*  External osteo order
--     Reopen if completed more than 5 days ago and measure isn't closed
-- */
-- ------------------------------------------------------------------------------------------------------------------------
            UPDATE public.patient_tasks pt
            SET status = 'in_progress', updated_at = NOW()
            FROM
                stage.medication_adherence_measure_id_to_task_types tt
                LEFT JOIN stage.patient_rx_fill_measures rfm ON rfm.measure_id = tt.measure_id
            WHERE
                  pt.task_type = 'recon_osteoporosis_external_order'
              AND rfm.patient_id = pt.patient_id
              AND rfm.year = DATE_PART('year', pt.inserted_at)
              AND tt.task_type = pt.task_type
              AND pt.status = 'completed'
              AND pt.updated_at + '5 days'::INTERVAL < NOW()
              AND rfm.analytics_id ISNULL
            ;


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
         /**/     select 'stg.sp_stp_process_med_adherence_tasks()', error_text;
         /**/     PERFORM * FROM audit.fn_create_sms_alerts(array['ae-coop-stage','med-adherence-tasks'],'stage.sp_stp_process_med_adherence_tasks',error_text::text);
                  commit;
         /**/   -------
         /**/   RAISE EXCEPTION 'Error in stage.sp_stp_process_med_adherence_tasks() :: %', error_text;
    end;

END; $$;

ALTER PROCEDURE sp_stp_process_med_adherence_tasks() OWNER TO postgres;

