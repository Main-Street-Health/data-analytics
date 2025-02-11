create procedure public.sp_populate_sure_scripts_panel_patients()
    language plpgsql
as
$$
DECLARE max_panel bigint; max_panel_hist bigint; message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN
    BEGIN

        -- check to see if we haven't received a med hist for last panel
        max_panel = ( SELECT MAX(id) FROM public.sure_scripts_panels );
        max_panel_hist = ( SELECT MAX(sure_scripts_panel_id) FROM sure_scripts_med_histories );
        IF (max_panel != max_panel_hist)
        THEN
            PERFORM *
            FROM
                audit.fn_create_sms_alerts(ARRAY ['ae-analytics-public','de-analytics-public'],
                                           'public.sp_populate_sure_scripts_panel_patients',
                                           'Surescripts panel is generating before previous panel was fully processed.');
        END IF;

        DROP TABLE IF EXISTS _controls;
        CREATE TEMP TABLE _controls AS
        with base as (
            select now() as dt
            -- the one day ahead isn't needed since the compliance checks occur using utc date
            -- sending the roster at 10:01pm CT equates to the next day utc
            -- send file the day before so it will be processed now() + 1
--             select now() + '1 day'::interval as dt
        )
        SELECT
            DATE_PART('year', b.dt)    _yr
          , DATE_PART('month', b.dt)   _month
          , b.dt::DATE                 _process_date
          , b.dt - '3 days'::INTERVAL  _three_days_ago
          , b.dt - '5 days'::INTERVAL  _five_days_ago
          , b.dt - '7 days'::INTERVAL  _seven_days_ago
          , b.dt - '10 days'::INTERVAL _ten_days_ago
          , b.dt - '14 days'::INTERVAL _forteen_days_ago
          , b.dt - '30 days'::INTERVAL _thirty_days_ago
          , d.day_text = 'monday'       _is_monday
        FROM
            base b
            join ref.dates d on b.dt::date = d.day
        ;


        DROP TABLE IF EXISTS _potential_patients;
        CREATE TEMP TABLE _potential_patients AS
        SELECT
            sp.patient_id
          , m.id IS NOT NULL                                            has_measure
          , m.measure_key
          , m.priority_status
          , pm.measure_status_key
          , COALESCE(pm.measure_status_key, '') IN ('unable_to_reach', 'lost_adr_gt_zero', 'pharmacy_not_found',
                                                    'pharmacy_verified_pharmacy_found', 'patient_refused',
                                                    'provider_refused') is_weird_status
          , m.next_fill_date
          , m.adr
          , m.pdc_to_date
          , m.is_on_90_day_supply
          , wf.id IS NOT NULL                                           has_workflow
          , wf.compliance_check_date
          , '2023-01-01'::DATE                                          last_panel_created_at
          , day_text = 'monday'                                         is_monday
        FROM -- all fdws
             fdw_member_doc.supreme_pizza sp
             JOIN fdw_member_doc.referring_partners rp ON rp.id = sp.primary_referring_partner_id
                 AND rp.organization_id != 7
             JOIN ref.dates d on d.day = (select _process_date from _controls)
             LEFT JOIN fdw_member_doc.qm_pm_med_adh_metrics m ON m.patient_id = sp.patient_id
                 AND m.measure_year = (select _yr from _controls)
             LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON wf.qm_pm_med_adh_metric_id = m.id
                 AND wf.is_active
             LEFT JOIN fdw_member_doc.qm_patient_measures pm ON pm.patient_id = sp.patient_id
                 AND pm.measure_key IN
                     ('med_adherence_diabetes', 'med_adherence_hypertension', 'med_adherence_cholesterol')
                 AND pm.operational_year = (select _yr from _controls)
        WHERE
            sp.is_medication_adherence
        --         and sp.primary_referring_partner_id in (135,345,829,133,266,302,67,162) -- pilot 2/7
--         and sp.primary_referring_partner_id in (135,345,328,133,133,266,302,67,159) -- pilot 2/7 v2
--         and sp.primary_referring_partner_id in (135,345,328,133,133,266,302,67,159, -- pilot 2/7 v2
--             1390,1391,3292,464,1541,1538,463,404,411,238,999,1487,1439,1167,1521,1648,343,343,1389,1351,1069,134,233,306,263,1536,245,346,344,130,1486,889,1284,1337,45,1443,312,310)  -- pilot 2/12 v3
        ;
        -- update with analytics data
        WITH
            latest AS ( SELECT DISTINCT ON (sspp.patient_id)
                            sspp.patient_id
                          , sspp.inserted_at::DATE send_date
                        FROM
                            public.sure_scripts_panel_patients sspp
                        WHERE
                            DATE_PART('year', sspp.inserted_at) = (select _yr from _controls)
                        ORDER BY sspp.patient_id, sspp.inserted_at DESC )
        UPDATE _potential_patients pp
        SET last_panel_created_at = l.send_date
        FROM
            latest l
        WHERE
            pp.patient_id = l.patient_id;


        --         SELECT * FROM _potential_patients where patient_id = 239610 ;
--         SELECT is_medication_adherence, primary_referring_partner_id FROM fdw_member_doc.supreme_pizza where patient_id = 239610 ;

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
            patient_id BIGINT PRIMARY KEY NOT NULL,
            reason     TEXT   NOT NULL
        );
        /*
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        VALUES (295389, 'manual from cody');
         */

        --   - Patients not qualifying for any measure who haven't been pulled in {date range} days
        WITH
            date_ranges AS ( SELECT *
                             FROM
                                 ( VALUES
                                       (1, 3, 30),
                                       (4, 5, 60),
                                       (6, 12, 90) ) x(mnth_start, mnth_end, days)
                             )
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , dr.days || ' day refresh for non med adherence patients'
        FROM
            _potential_patients pp
            cross join _controls c
            JOIN date_ranges dr on c._month BETWEEN dr.mnth_start AND dr.mnth_end
        WHERE
            not c._is_monday -- reduce monday volume
            and c._process_date < (date_trunc('year', c._process_date) + interval '10 months') -- no need to send after 10/1, can't enter measure that late, leave a month buffer
            and NOT EXISTS( SELECT
                              1
                          FROM
                              _potential_patients pp2
                          WHERE
                                pp2.patient_id = pp.patient_id
                            AND pp2.has_measure )
          AND (
                  pp.last_panel_created_at IS NULL
                      OR
                      pp.last_panel_created_at < c._process_date - dr.days
                  );


--      - Patients with high priority measure and no open task and refill date is 5 days from now
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 5 days before expected next fill date when high priority: ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
            pp.has_measure
          AND pp.priority_status = 'high'
          AND pp.next_fill_date - 5 <= (select _process_date from _controls)
          AND NOT pp.has_workflow
          AND NOT pp.is_weird_status
--           AND pp.last_panel_created_at < (select _three_days_ago from _controls)
        GROUP BY 1
        ON CONFLICT (patient_id) DO UPDATE SET reason = _patients_to_pull.reason || '; ' || excluded.reason;

        --   - Patients with medium priority measure and no open task and refill date was 5+ days ago
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 5 days after expected next fill date when medium priority: ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND pp.priority_status = 'medium'
          AND pp.next_fill_date <= (select _five_days_ago from _controls)
          AND NOT pp.has_workflow
          AND NOT pp.is_weird_status
--           AND pp.last_panel_created_at < (select _five_days_ago from _controls)
        GROUP BY 1
        ON CONFLICT (patient_id) DO UPDATE SET reason = _patients_to_pull.reason || '; ' || excluded.reason;

        -- Patients with low priority measure, no open task, refill date was 14+ days ago
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull 14 days after expected next fill date when low priority: ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          AND pp.priority_status = 'low'
          AND pp.next_fill_date <= (select _forteen_days_ago from _controls)
          AND NOT pp.has_workflow
          AND NOT pp.is_weird_status
--           AND pp.last_panel_created_at < (select _five_days_ago from _controls)
        GROUP BY 1
        ON CONFLICT (patient_id) DO UPDATE SET reason = _patients_to_pull.reason || '; ' || excluded.reason;

        -- WEIRD ORDER STATUS EXCEPTIONS
        -- Patients with measure in unable to reach - query every ten days
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
          and not is_monday
          AND next_fill_date <= (select _five_days_ago from _controls)
          AND NOT has_workflow
          AND is_weird_status
          AND measure_status_key = 'unable_to_reach'
          AND last_panel_created_at <= (select _ten_days_ago from _controls)
        GROUP BY 1
        ON CONFLICT (patient_id) DO UPDATE SET reason = _patients_to_pull.reason || '; ' || excluded.reason;

        --   - Patients with measure in lost gt 0 and pharmacy not found - query every 30 days
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'Pull every 30 days for : ' || STRING_AGG(pp.measure_status_key, ', ') || ' - ' ||
            STRING_AGG(pp.measure_key, ', ')
        FROM
            _potential_patients pp
        WHERE
              pp.has_measure
          and not is_monday
          AND next_fill_date <= (select _five_days_ago from _controls)
          AND NOT has_workflow
          AND is_weird_status
          AND measure_status_key IN
              ('lost_adr_gt_zero', 'pharmacy_not_found', 'pharmacy_verified_pharmacy_found', 'patient_refused',
               'provider_refused')
          AND last_panel_created_at <= (select _thirty_days_ago from _controls)
        GROUP BY 1
        ON CONFLICT (patient_id) DO UPDATE SET reason = _patients_to_pull.reason || '; ' || excluded.reason;

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
          AND compliance_check_date <= (select _process_date from _controls)
          AND last_panel_created_at <= (select _five_days_ago from _controls)
        GROUP BY 1
        ON CONFLICT (patient_id) DO UPDATE SET reason = _patients_to_pull.reason || '; ' || excluded.reason;

        -- 60-90day fill pull 21 days in to double-check that the med is real
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        WITH
            _potential  AS ( SELECT *
                             FROM
                                 _potential_patients pp
                             WHERE
                                   pp.has_measure
                               and not is_monday
                               AND pp.next_fill_date - (select _process_date from _controls) BETWEEN 33 AND 69
                               AND last_panel_created_at < (select _seven_days_ago from _controls)
                               AND NOT EXISTS( SELECT 1
                                               FROM _patients_to_pull ptp
                                               WHERE ptp.patient_id = pp.patient_id ) )
          , _latest_med AS ( SELECT DISTINCT ON (pp.patient_id, pp.measure_key)
                                 pp.patient_id
                               , pp.measure_key
                               , pp.next_fill_date
                               , pm.days_supply
                             FROM
                                 _potential pp
                                 JOIN prd.patient_medications pm ON pp.patient_id = pm.patient_id
                                     AND pm.end_date + 1 = pp.next_fill_date
                                     AND DATE_PART('year', pm.last_filled_date) = (select _yr from _controls)
                                 JOIN ref.med_adherence_value_sets vs ON vs.code = pm.ndc
                                     AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
                                 JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
                                     AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
                                     AND m.is_med = 'Y'
                                     AND m.is_exclusion = 'N'
                                     AND m.measure_version = '2024'
                                 JOIN ref.med_adherence_measure_names mamm ON mamm.analytics_measure_id = m.measure_id
                                     AND mamm.coop_measure_key = pp.measure_key
                             ORDER BY pp.patient_id, pp.measure_key, pm.last_filled_date DESC )
        SELECT DISTINCT
            lm.patient_id
          , 'long fill 21 days in: ' || STRING_AGG(DISTINCT lm.measure_key, ', ')
        FROM
            _latest_med lm
        WHERE
              lm.days_supply >= 60
--           AND pp.is_on_90_day_supply
--           AND pp.next_fill_date - NOW()::DATE BETWEEN 70 AND 76 -- 90-14 = 76. Made it a 6 day range in the off chance it doesn't run on the exact date
--           AND pp.next_fill_date - NOW()::DATE BETWEEN 63 AND 69 -- upd[2024-07-18] 90-21 = 69. Made it a 6 day range in the off chance it doesn't run on the exact date
          AND lm.next_fill_date - (select _process_date from _controls) BETWEEN lm.days_supply - 27 AND lm.days_supply - 21 -- upd[2024-08-09] switch to 60day+ fills
        GROUP BY 1
        ON CONFLICT (patient_id) DO UPDATE SET reason = _patients_to_pull.reason || '; ' || excluded.reason;

        ------------------------------------------------------------------------------------------------------------------------
        /* PILOT 2025 */
        ------------------------------------------------------------------------------------------------------------------------
        DELETE
        FROM
            _patients_to_pull pp
        WHERE
            NOT EXISTS( SELECT
                            1
                        FROM
                            junk.med_adh_2025_pilot pilot
                            JOIN fdw_member_doc.care_team_members ctm ON pilot.user_id = ctm.user_id
                            JOIN fdw_member_doc.care_teams ct ON ct.id = ctm.care_team_id AND role = 'health_navigator'
                            JOIN fdw_member_doc.patients p ON p.care_team_id = ct.id
                            JOIN fdw_member_doc.msh_care_team_referring_partners ctrp
                                 ON ctrp.referring_partner_id = pilot.rpl_id AND ct.id = ctrp.care_team_id
                            WHERE
                                     pp.patient_id = p.id
--                         WHERE pilot.pilot = 'Pilot 1' -- switch to pilot 1+2
                        )
;
        ------------------------------------------------------------------------------------------------------------------------
        /* One time gaps: SPC, SPD, OMW */
        ------------------------------------------------------------------------------------------------------------------------
        DROP TABLE IF EXISTS _one_time_gaps_patients_to_pull;
        CREATE TEMP TABLE _one_time_gaps_patients_to_pull AS
        SELECT
            patient_id
          , measure_key
          , measure_status_key
          , compliance_check_date
          , '2024-01-01'::DATE last_panel_created_at
        FROM
            fdw_member_doc.qm_patient_measures qpm
            JOIN fdw_member_doc.qm_pm_spc_statin_wfs wf ON qpm.id = wf.patient_measure_id AND wf.is_active
        WHERE
              qpm.is_active
          AND measure_status_key NOT IN ('closed_system')
          AND wf.compliance_check_date <= (select _process_date from _controls);


        INSERT
        INTO
            _one_time_gaps_patients_to_pull (patient_id, measure_key, measure_status_key, compliance_check_date,
                                             last_panel_created_at)
        SELECT
            patient_id
          , measure_key
          , measure_status_key
          , compliance_check_date
          , '2024-01-01'::DATE last_panel_created_at
        FROM
            fdw_member_doc.qm_patient_measures qpm
            JOIN fdw_member_doc.qm_pm_spd_statin_wfs wf ON qpm.id = wf.patient_measure_id AND wf.is_active
          WHERE
              qpm.is_active
          AND measure_status_key NOT IN ('closed_system')
          AND wf.compliance_check_date <= (select _process_date from _controls);

        INSERT
        INTO
            _one_time_gaps_patients_to_pull (patient_id, measure_key, measure_status_key, compliance_check_date,
                                             last_panel_created_at)
        SELECT
            patient_id
          , measure_key
          , measure_status_key
          , compliance_check_date
          , '2024-01-01'::DATE last_panel_created_at
        FROM
            fdw_member_doc.qm_patient_measures qpm
            JOIN fdw_member_doc.qm_pm_osteoporosis_management_wfs wf ON qpm.id = wf.patient_measure_id AND wf.is_active
        WHERE
              qpm.is_active
          AND measure_status_key NOT IN ('closed_system')
          AND wf.order_type = 'prescribe'
          AND wf.compliance_check_date <= (select _process_date from _controls);

        WITH
            latest AS ( SELECT DISTINCT ON (sspp.patient_id)
                            sspp.patient_id
                          , sspp.inserted_at::DATE send_date
                        FROM
                            public.sure_scripts_panel_patients sspp
                        WHERE
                            DATE_PART('year', sspp.inserted_at) = (select _yr from _controls)
                        ORDER BY sspp.patient_id, sspp.inserted_at DESC )
        UPDATE _one_time_gaps_patients_to_pull pp
        SET
            last_panel_created_at = l.send_date
        FROM
            latest l
        WHERE
            pp.patient_id = l.patient_id;


        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT DISTINCT
            patient_id
          , 'prescribed med for: ' || STRING_AGG(pp.measure_key, ', ')
        FROM
            _one_time_gaps_patients_to_pull pp
        WHERE
            last_panel_created_at < (select _ten_days_ago from _controls) -- check every 10 days
        GROUP BY
            1
        ON CONFLICT (patient_id) DO UPDATE SET
            reason = _patients_to_pull.reason || '; ' || excluded.reason;

------------------------------------------------------------------------------------------------------------------------
/* Concurrent Meds Measures
   add all on 2/7*/
------------------------------------------------------------------------------------------------------------------------
        INSERT
        INTO
            _patients_to_pull (patient_id, reason)
        SELECT
            patient_id
          , 'concurrent meds monthly: ' || STRING_AGG(pc.measure_key, ', ')
        FROM
            fdw_member_doc.qm_patient_config pc
        WHERE
              pc.measure_key IN ('cob_concurrent_opioid_benzo', 'poly_ach_multi_anticholinergic_meds')
          AND NOT EXISTS( SELECT
                              1
                          FROM
                              public.sure_scripts_panel_patients pp
                          WHERE
                                pp.inserted_at > NOW() - '30 days'::INTERVAL
                            AND pp.patient_id = pc.patient_id )
        GROUP BY 1
        ;

------------------------------------------------------------------------------------------------------------------------
/* MUTATE */
------------------------------------------------------------------------------------------------------------------------

        INSERT
        INTO
            public.sure_scripts_panel_patients (patient_id, sequence, last_name, first_name, middle_name, prefix,
                                                suffix, address_line_1, address_line_2, city, state, zip, dob, gender,
                                                npi,
                                                updated_at, inserted_at, reason_for_query)
        SELECT DISTINCT
            p.id                                                  patient_id
          , ROW_NUMBER() OVER (ORDER BY p.id)                     sequence
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
          , COALESCE(mp.npi::TEXT, '1023087954')                  npi
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
              NOT EXISTS( SELECT
                              1
                          FROM
                              public.sure_scripts_panel_patients sspp
                          WHERE
                                sspp.sure_scripts_panel_id ISNULL
                            AND sspp.patient_id = p.id )
          AND LENGTH(p.first_name) >= 2 -- SS requires Two of a person's names (Last Name, First Name, Middle Name) must have 2 or more characters.
          AND LENGTH(p.last_name) >= 2;
        ---------------------------
    EXCEPTION
        WHEN OTHERS THEN
            GET DIAGNOSTICS stack = PG_CONTEXT;
            GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT, exception_detail = PG_EXCEPTION_DETAIL, exception_hint = PG_EXCEPTION_HINT, exception_context = PG_EXCEPTION_CONTEXT;
            ROLLBACK;
            error_text = 'MSH Analytics Message_Text( ' || COALESCE(message_text, '') || E' ) \nstack (' ||
                         COALESCE(exception_context, '') || ' ) ';
            PERFORM *
            FROM
                audit.fn_create_sms_alerts(ARRAY ['ae-analytics-public','de-analytics-public'],
                                           'public.sp_populate_sure_scripts_panel_patients', error_text::TEXT);
            COMMIT;
            RAISE EXCEPTION 'public.sp_populate_sure_scripts_panel_patients :: %', error_text;

    END;
    -------

END;
$$;

alter procedure public.sp_populate_sure_scripts_panel_patients() owner to postgres;

