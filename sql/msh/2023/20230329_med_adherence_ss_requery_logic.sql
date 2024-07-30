CREATE PROCEDURE sp_populate_sure_scripts_panel_patients()
    ------------------------------------------------------------------------------------------------------------------------
/*
  Sure Scripts requery logic
  - Patients not qualifying for any measure who haven't been pulled in 30 days
  - Patients with measure med (and no open task?) and max refill date was 5+ days ago
  - Patients with measure med and max refill date was 5+ days ago and an open task and it's been ten days since their last pull
  TODO: verify the 5 day closed tasks
*/
------------------------------------------------------------------------------------------------------------------------
    LANGUAGE plpgsql
AS
$$
BEGIN
    DROP TABLE IF EXISTS _potential_patients;
    CREATE TEMP TABLE _potential_patients AS
    SELECT
        lp.patient_id
      , pmam.measure_id                                                           measure_id
      , MAX(pmam.id) IS NOT NULL                                                  has_measure
      , MAX(pmam.next_fill_date)                                                  next_fill_date
      , MAX(pp.inserted_at)                                                       last_panel_created_at
      , MAX(pt.id) FILTER (WHERE pt.status IN ('new', 'in_progress')) IS NOT NULL has_open_task
      , MAX(pt.inserted_at) FILTER (WHERE pt.status IN ('new', 'in_progress'))    open_task_inserted_at
      , MAX(pt.updated_at) FILTER (WHERE pt.status IN ('completed', 'cancelled')
                              and not med_ad_pat_task.is_system_verified_closed)  closed_non_verified_task_updated_at
      , MAX(mhd.id) IS NOT NULL                                                   pulled_but_no_ss_hit_previously
      , ARRAY_AGG(DISTINCT mhd.note)                                              previous_ss_notes
    FROM
        junk.med_adherence_pilot_care_teams_20230327 pct
        JOIN fdw_member_doc.patients p ON pct.care_team_id = pct.care_team_id
        JOIN fdw_member_doc.layer_cake_patients lp ON lp.patient_id = p.id
        JOIN fdw_member_doc.msh_patient_integration_configs ic ON lp.patient_id = ic.patient_id
        LEFT JOIN fdw_member_doc.patient_tasks pt ON pt.patient_id = ic.patient_id
            AND pt.task_type IN ('med_adherence_cholesterol', 'med_adherence_diabetes', 'med_adherence_hypertension')
        left join fdw_member_doc.medication_adherence_patient_task med_ad_pat_task on pt.id = med_ad_pat_task.patient_task_id
        LEFT JOIN prd.patient_med_adherence_measures pmam ON pmam.patient_id = ic.patient_id
            AND pmam.year = EXTRACT('year' FROM NOW())
        LEFT JOIN public.sure_scripts_panel_patients pp ON pp.patient_id = ic.patient_id
        LEFT JOIN public.sure_scripts_med_history_details mhd ON p.id = mhd.patient_id::BIGINT
            AND mhd.note IS NOT NULL
    WHERE
        lp.is_medication_adherence
    GROUP BY
        1, 2;
--     SELECT * FROM _potential_patients WHERE patient_id = 68957;
--     SELECT COUNT(*) FROM _potential_patients;

    DROP TABLE IF EXISTS _patients_to_pull;
    CREATE TEMP TABLE _patients_to_pull (
        patient_id BIGINT NOT NULL,
        reason     TEXT   NOT NULL,
        UNIQUE (patient_id)
    );

--   - Patients not qualifying for any measure who haven't been pulled in 30 days
    INSERT
    INTO
        _patients_to_pull (patient_id, reason)
    SELECT
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
--     SELECT COUNT(*) FROM _patients_to_pull;


--   - Patients with measure med (and no open task?) and max refill date was 5+ days ago
    INSERT
    INTO
        _patients_to_pull (patient_id, reason)
    SELECT
        patient_id
      , 'Pull 5 days after expected next fill date: ' || STRING_AGG(pp.measure_id, ', ')
    FROM
        _potential_patients pp
    WHERE
          pp.has_measure
      AND next_fill_date <= NOW() - '5 days'::INTERVAL
          -- not sure what the logic should be to make sure we only pull this once the second may make more sense
      AND NOT has_open_task
      AND last_panel_created_at < NOW() - '5 days'::INTERVAL
    GROUP BY 1;

--     SELECT COUNT(*) FROM _patients_to_pull WHERE reason ~* 'Pull 5 days after expected next fill date';
--     SELECT * FROM _patients_to_pull WHERE reason ~* 'Pull 5 days after expected next fill date' and patient_id = 68957;

--   - Patients with measure med and max refill date was 5+ days ago and an open task and it's been ten days since their last pull
    INSERT
    INTO
        _patients_to_pull (patient_id, reason)
    SELECT
        patient_id
      , 'Pull 10 days from last query if task is still open after ten: ' || STRING_AGG(pp.measure_id, ', ')
    FROM
        _potential_patients pp
    WHERE
          pp.has_measure
      AND has_open_task
      AND open_task_inserted_at <= NOW() - '10 days'::INTERVAL
      AND last_panel_created_at < NOW() - '10 days'::INTERVAL
    GROUP BY 1;

--     SELECT COUNT(*) FROM _patients_to_pull;

    -- TODO: confirm closed task with ss data
--  Need a way to record that we verified it is closed
--  Need a way to know health nav manually closed
-- confirm task closed 5 days after closure

INSERT
INTO
    _patients_to_pull (patient_id, reason)
    SELECT
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
--     SELECT * FROM _patients_to_pull WHERE reason ~* 'Closed task needs confirmation';

------------------------------------------------------------------------------------------------------------------------
/* ONE time pilot pull */
------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _patients_to_pull (patient_id, reason)
    WITH
        qq AS ( SELECT DISTINCT
                    pt.patient_id
                FROM
                    fdw_member_doc.patient_tasks pt
                    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pt.patient_id
                    JOIN fdw_member_doc.referring_partners rp ON rp.id = sp.primary_referring_partner_id
                    LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rp.organization_id = rpo.id
                    JOIN fdw_member_doc.users u ON pt.assigned_to_id = u.id
                    JOIN fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
                    JOIN fdw_member_doc.patient_medication_adherences pma
                         ON pma.id = mapt.patient_medication_adherence_id
                    JOIN fdw_member_doc.patients p ON pt.patient_id = p.id
                    JOIN fdw_member_doc.care_teams ct ON ct.id = p.care_team_id
                WHERE
                        pt.task_type IN
                        ('med_adherence_cholesterol', 'med_adherence_diabetes', 'med_adherence_hypertension') )
    SELECT
        qq.patient_id
      , 'Pilot Open Task'
    FROM
        qq
        LEFT JOIN junk.sure_scripts_pids_to_refresh_20230327 j ON j.patient_id = qq.patient_id
    WHERE
        j.patient_id ISNULL
    ON CONFLICT (patient_id) do NOTHING
;
--
--     SELECT p.inserted_at::date, count(distinct ptp.patient_id) FROM _patients_to_pull ptp
--     left join sure_scripts_panel_patients s ON ptp.patient_id = s.patient_id
--     left join sure_scripts_panels p on s.sure_scripts_panel_id = p.id
--     GROUP BY 1
--     ;

    INSERT
    INTO
        public.sure_scripts_panel_patients (patient_id, sequence, last_name, first_name, middle_name, prefix,
                                            suffix, address_line_1, address_line_2, city, state, zip, dob, gender, npi,
                                            updated_at, inserted_at, reason)
    SELECT
        ptp.patient_id
      , ROW_NUMBER() OVER (ORDER BY ptp.patient_id) sequence
      , p.last_name
      , p.first_name
      , NULL                                        middle_name
      , NULL                                        prefix
      , NULL                                        suffix
      , pa.line1                                    address_line_1
      , pa.line2                                    address_line_2
      , pa.city
      , pa.state
      , pa.postal_code                              zip
      , p.dob
      , LEFT(p.gender, 1)                           gender
      , mp.npi::TEXT                                npi
      , NOW()                                       updated_at
      , NOW()                                       inserted_at
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
        NOT EXISTS(SELECT 1 FROM public.sure_scripts_panel_patients sspp WHERE sspp.sure_scripts_panel_id ISNULL)
    ;

--     SELECT * FROM _patients_to_pull WHERE patient_id = 119120;;


-- potential_ss_panel_detail_v2_20230329
--     SELECT
--         ptp.patient_id
--       , ptp.reason
--       , p.first_name
--       , p.last_name
--       , p.dob
--       , rp.name     rp
-- --       , u.full_name health_nav
-- --       , pp.measure_id
-- --       , pp.has_measure
-- --       , pp.next_fill_date
-- --       , pp.last_panel_created_at
-- --       , pp.has_open_task
-- --       , pp.open_task_inserted_at
-- --       , pp.closed_task_updated_at
-- --       , pp.pulled_but_no_ss_hit_previously
-- --       , pp.previous_ss_notes
--
--     FROM
--         _patients_to_pull ptp
-- --         JOIN _potential_patients pp ON pp.patient_id = ptp.patient_id
--         JOIN fdw_member_doc.patients p ON ptp.patient_id = p.id
--         left JOIN fdw_member_doc.patient_referring_partners prp ON prp.patient_id = p.id and prp."primary"
--         left JOIN fdw_member_doc.referring_partners rp ON prp.referring_partner_id = rp.id
-- --         JOIN fdw_member_doc.care_teams ct ON p.care_team_id = ct.id
-- --         JOIN fdw_member_doc.care_team_members ctm ON ct.id = ctm.care_team_id AND ctm.role = 'health_navigator'
-- --         JOIN fdw_member_doc.users u ON u.id = ctm.user_id
-- --     where ptp.patient_id = 68957
--     ;
END;
$$;



SELECT *
FROM
    sure_scripts_panel_patients
WHERE
    patient_id = 68975;
