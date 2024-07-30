CREATE or replace PROCEDURE sp_reveleer_data_stager()
    LANGUAGE plpgsql
AS
$$
BEGIN
    DROP TABLE IF EXISTS _controls;
    CREATE TEMP TABLE _controls AS
    SELECT
--         _is_new_only             is_new_only
        DATE_PART('year', NOW()) yr
      , _boy(NOW()::date)              boy;

------------------------------------------------------------------------------------------------------------------------
/* Compliance file */
------------------------------------------------------------------------------------------------------------------------
    CREATE TEMP TABLE _num_code_mappings AS
    SELECT *
    FROM
        ( VALUES
              ('BSC', 'bcs', 'bcs_breast_cancer_screening'),
              ('CBP', 'bp', 'cbp_controlling_high_blood_pressure'),
              ('COA', 'funcstat', 'coa_functional_assessment'),
              ('COA', 'medreview', 'coa_medication_review'),
              ('COA', 'painscreen', 'coa_pain_assessment'),
              ('COL', 'col', 'col_colorectal_screening'),
              ('EED', 'eyeexam', 'eed_eye_exam_for_patients_with_diabetes'),
              ('A1C9', 'hba1c_le_9', 'hbd_hemoglobin_a1c_control_for_patients_with_diabetes'),
              ('OMW', 'omw', 'omw_osteoporosis_management'),
              ('TRC', 'mrp', 'trc_mrp_medication_reconciliation_post_discharge'),
              ('TRC', 'pat_engage', 'trc_peid_patient_engagement_after_ip_discharge'),
              ('TRC', 'discharge_info', 'trc_rdi_receipt_of_discharge_information'),
              ('TRC', 'ipadmission', 'trc_nia_notification_of_ip_admission'),
              ('FMC', 'Follow_Up_Visit', 'fmc_follow_up_after_ed_visit_multiple_chronic_conditions')
          ) x(measurecode, numerator_code, measure_key);

        -- only populate compliance on refreshes
        DROP TABLE IF EXISTS _closed_measures;
        CREATE TEMP TABLE _closed_measures AS
        SELECT DISTINCT
            pqm.patient_id
          , CASE
                WHEN m.code = 'HBD' THEN 'A1C9'
                WHEN m.code in ('MRP', 'NIA', 'PEID', 'RDI') THEN 'TRC'
                ELSE m.code
                END measure_id
          , pqm.id
          , pqm.measure_key
        FROM
            fdw_member_doc.qm_patient_measures pqm
            JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
        WHERE
              pqm.measure_status_key = 'closed_system'
          AND pqm.operational_year = ( SELECT yr FROM _controls );

        CREATE INDEX ON _closed_measures(id);
        CREATE INDEX ON _closed_measures(measure_key);

        DROP TABLE IF EXISTS _comp;
        CREATE TEMP TABLE _comp AS
        SELECT DISTINCT
            cm.patient_id
          , cm.patient_id                member_id
          , rc.id                        sample_id
          , cm.measure_id
          , numerator_code
          , 'Y'                          compliance_flag
          , 'ADMIN'                      compliance_type
          , NOW()                        inserted_at
          , NOW()                        updated_at
          , ( SELECT yr FROM _controls ) yr
          , rc.reveleer_project_id
          , FALSE                        is_new
        FROM
            _closed_measures cm
            JOIN _num_code_mappings ncm ON ncm.measure_key = cm.measure_key
            JOIN reveleer_chases rc ON cm.id = ANY (rc.qm_patient_measure_ids)
        WHERE
--             rc.reveleer_project_id = 241 and
            -- only send for chases we've already sent
            EXISTS( SELECT
                        1
                    FROM
                        reveleer_chase_file_details cfd
                    WHERE
                          rc.id = cfd.reveleer_chase_id
                      AND cfd.reveleer_file_id IS NOT NULL )
    ;


    ------------------------------------------------------------------------------------------------------------------------
    /* COA compliance hack
       if a hp is not submitting all COA measures we auto send a compliance for the ones we're not on the hook for
       this way they don't look like they need to be worked
       super duper dumb but...what would you expect from reveleer
    */
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _comp (patient_id, member_id, sample_id, measure_id, numerator_code, compliance_flag, compliance_type,
               inserted_at, updated_at, yr, reveleer_project_id, is_new)
    SELECT DISTINCT
        rc.patient_id
      , rc.patient_id                member_id
      , rc.id                        sample_id
      , rc.measure_code              measure_id
      , ncm.numerator_code
      , 'Y'                          compliance_flag
      , 'ADMIN'                      compliance_type
      , NOW()                        inserted_at
      , NOW()                        updated_at
      , ( SELECT yr FROM _controls ) yr
      , rc.reveleer_project_id
      , true                         is_new
    FROM
        public.reveleer_chases rc
        JOIN reveleer_projects r ON rc.reveleer_project_id = r.id
        JOIN _num_code_mappings ncm ON ncm.measurecode = 'COA' AND ncm.measure_key != all(r.measures_to_send)
    WHERE
--         rc.reveleer_project_id = 241 and
          rc.measure_code = 'COA'
      AND rc.is_active
    ;
    ------------------------------------------------------------------------------------------------------------------------
    /* TRC Hack similar to COA */
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _comp (patient_id, member_id, sample_id, measure_id, numerator_code, compliance_flag, compliance_type,
               inserted_at, updated_at, yr, reveleer_project_id, is_new)
    SELECT DISTINCT
        rc.patient_id
      , rc.patient_id                member_id
      , rc.id                        sample_id
      , rc.measure_code              measure_id
      , ncm.numerator_code
      , 'Y'                          compliance_flag
      , 'ADMIN'                      compliance_type
      , NOW()                        inserted_at
      , NOW()                        updated_at
      , ( SELECT yr FROM _controls ) yr
      , rc.reveleer_project_id
      , true                         is_new
    FROM
        public.reveleer_chases rc
        JOIN reveleer_projects r ON rc.reveleer_project_id = r.id
        JOIN _num_code_mappings ncm ON ncm.measurecode = 'TRC' AND ncm.measure_key != all(r.measures_to_send)
    WHERE
--         rc.reveleer_project_id = 241 and
          rc.measure_code = 'TRC'
      AND rc.is_active
    ;

    -- not filtering and deleting in a second query is 1000x faster
    -- delete already sent comp records
    DELETE
    FROM
        _comp c
        USING public.reveleer_compliance_file_details cfd
    WHERE
          cfd.reveleer_chase_id = c.sample_id
      AND cfd.numerator_code = c.numerator_code
      AND cfd.yr = ( SELECT yr FROM _controls );

--     SELECT * FROM _comp;
------------------------------------------------------------------------------------------------------------------------
/* chase file */
------------------------------------------------------------------------------------------------------------------------
--     8:43
    DROP TABLE IF EXISTS _patient_measures;
    CREATE TEMP TABLE _patient_measures AS
    SELECT DISTINCT
        sp.patient_id
      , NULL::BIGINT chase_id
      , sp.patient_mbi
      , pay.name     payer_name
      , ptr.id       reveleer_project_id
      , CASE
            WHEN m.code = 'HBD' THEN 'A1C9'
            WHEN m.code in ('MRP', 'NIA', 'PEID', 'RDI') THEN 'TRC'
            ELSE m.code
            END      measure_code
      , pqm.measure_key
      , pqm.id       patient_quality_measure_id
      , pqm.operational_year
      , pqm.measure_source_key
      , pqm.must_close_by_date
      , mpm.subscriber_id
      , pqm.measure_status_key = 'closed_system' is_closed_system
    FROM
        fdw_member_doc.qm_patient_measures pqm
        JOIN fdw_member_doc.qm_mco_patient_measures mpm ON pqm.mco_patient_measure_id = mpm.id
        JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--         JOIN public.reveleer_projects ptr ON mpm.payer_id = ptr.payer_id
        JOIN (select id, unnest(measures_to_send) measures_to_send, payer_id from public.reveleer_projects) ptr ON mpm.payer_id = ptr.payer_id
        JOIN fdw_member_doc.payers pay ON pay.id = mpm.payer_id
        JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
        JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
    WHERE
          pqm.operational_year = ( SELECT yr FROM _controls )
      AND pqm.measure_source_key = 'mco'
      AND pqm.is_active
      AND sp.is_quality_measures
      -- need to include closed system for compliance file
      AND (st.send_to_reveleer or pqm.measure_status_key = 'closed_system')
      AND pqm.measure_key = ptr.measures_to_send
--       and mpm.payer_id = 2
--     and pqm.patient_id = 98173 and pqm.measure_key = 'trc_mrp_medication_reconciliation_post_discharge';
--       and ptr.id = 331
    ;

--     -- MSSP
--     INSERT
--     INTO
--         _patient_measures (patient_id, chase_id, patient_mbi, payer_name, reveleer_project_id, measure_code,
--                            measure_key, patient_quality_measure_id, operational_year, measure_source_key,
--                            must_close_by_date, subscriber_id, is_closed_system)
--     SELECT DISTINCT
--         sp.patient_id
--       , NULL::BIGINT                             chase_id
--       , sp.patient_mbi
--       , pay.name                                 payer_name
--       , ptr.id                                   reveleer_project_id
--       , CASE
--             WHEN m.code = 'HBD' THEN 'A1C9'
--             WHEN m.code in ('MRP', 'NIA', 'PEID', 'RDI') THEN 'TRC'
--             ELSE m.code
--             END                                  measure_code
--       , pqm.measure_key
--       , pqm.id                                   patient_quality_measure_id
--       , pqm.operational_year
--       , pqm.measure_source_key
--       , pqm.must_close_by_date
--       , sp.subscriber_id
--       , pqm.measure_status_key = 'closed_system' is_closed_system
--     FROM
--         fdw_member_doc.qm_patient_measures pqm
--         JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--         JOIN ( SELECT id, UNNEST(measures_to_send) measures_to_send, payer_id FROM public.reveleer_projects ) ptr
--              ON sp.patient_payer_id = ptr.payer_id
--         JOIN fdw_member_doc.payers pay ON pay.id = sp.patient_payer_id
--         JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
--         JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
--         JOIN raw.reveleer_mssp_cqm_aco_membrs_20240624 r ON r.patient_id = sp.patient_id
--     WHERE
--           pqm.operational_year = 2024-- ( SELECT yr FROM _controls )
--       AND pqm.measure_source_key = 'proxy'
--       AND pqm.is_active
--       AND sp.is_quality_measures
--           -- need to include closed system for compliance file
--       AND (st.send_to_reveleer OR pqm.measure_status_key = 'closed_system')
--       AND pqm.measure_key = ptr.measures_to_send
--       AND ptr.id = 298 -- mssp cqm aco
--           -- reveleer not ready to send dsf
--       AND pqm.measure_key != 'dsf_depression'
--     ;


    -- Stupid hack to send proxies for humana ga and sc. This is a one off. Can remove for 2025
    INSERT
    INTO
        _patient_measures (patient_id, chase_id, patient_mbi, payer_name, reveleer_project_id, measure_code,
                           measure_key, patient_quality_measure_id, operational_year, measure_source_key,
                           must_close_by_date, subscriber_id, is_closed_system)
    SELECT DISTINCT
        sp.patient_id
      , NULL::BIGINT chase_id
      , sp.patient_mbi
      , pay.name     payer_name
      , 265          reveleer_project_id
      , CASE
            WHEN m.code = 'HBD' THEN 'A1C9'
            WHEN m.code in ('MRP', 'NIA', 'PEID', 'RDI') THEN 'TRC'
            ELSE m.code
            END      measure_code
      , pqm.measure_key
      , pqm.id       patient_quality_measure_id
      , pqm.operational_year
      , pqm.measure_source_key
      , pqm.must_close_by_date
      , sp.subscriber_id
      , pqm.measure_status_key = 'closed_system' is_closed_system
    FROM
        fdw_member_doc.qm_patient_measures pqm
        JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
        JOIN fdw_member_doc.payers pay ON pay.id = sp.patient_payer_id
        JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
        JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
    WHERE
          pqm.operational_year = ( SELECT yr FROM _controls )
--       and sp.patient_state_payer_id in (453,57) -- Humana SC=453, GA=57
--       and sp.patient_state_payer_id = 453 -- Humana SC=453
      and sp.patient_state_payer_id = 57 -- Humana GA=57
      AND pqm.measure_source_key = 'proxy'
      AND pqm.is_active
      AND sp.is_quality_measures
      -- need to include closed system for compliance file
      AND (st.send_to_reveleer or pqm.measure_status_key = 'closed_system')
      AND pqm.measure_key IN
          ('col_colorectal_screening', 'eed_eye_exam_for_patients_with_diabetes', 'bcs_breast_cancer_screening');
    -- end one off, remove for 2025


    ------------------------------------------------------------------------------------------------------------------------
    /* TRC workaround to combine correct measures, group by discharge date */
    ------------------------------------------------------------------------------------------------------------------------
    UPDATE _patient_measures
    SET
        must_close_by_date = CASE
            WHEN measure_key = 'trc_peid_patient_engagement_after_ip_discharge'   THEN must_close_by_date - 30
            WHEN measure_key = 'trc_mrp_medication_reconciliation_post_discharge' THEN must_close_by_date - 30
            WHEN measure_key = 'trc_rdi_receipt_of_discharge_information'         THEN must_close_by_date - 2
            END
    WHERE
        measure_key IN (
                        'trc_peid_patient_engagement_after_ip_discharge',
                        'trc_rdi_receipt_of_discharge_information',
                        'trc_mrp_medication_reconciliation_post_discharge'
--                           'trc_nia_notification_of_ip_admission'
            );


    CREATE INDEX ON _patient_measures(patient_quality_measure_id);
    CREATE INDEX ON _patient_measures(patient_id);
    CREATE UNIQUE INDEX ON _patient_measures(patient_id, measure_key, must_close_by_date, operational_year, reveleer_project_id);



--     select * from _patient_measures ; --where is_closed_system;
--     select count(*) from _patient_measures;
    ------------------------------------------------------------------------------------------------------------------------
    /* clean up closed system */
    ------------------------------------------------------------------------------------------------------------------------
--     10min
    DELETE
    FROM
        _patient_measures pm
    WHERE
          is_closed_system
      AND NOT EXISTS( SELECT
                          1
                      FROM
                          _comp c
                          JOIN reveleer_chases rc ON rc.id = c.sample_id
                      WHERE
                          pm.patient_quality_measure_id = ANY (rc.qm_patient_measure_ids) );


    ------------------------------------------------------------------------------------------------------------------------
    /* TEMPORARY:
       Remove on hold closed pending
       Remove MRP and HBD
       will need to configure all of trc measures when ready
       */
    ------------------------------------------------------------------------------------------------------------------------
    DELETE
    FROM
        _patient_measures pm
        USING fdw_member_doc.qm_pm_status_periods sp
    WHERE
          sp.measure_status_key = 'closed_pending'
      AND sp.end_at ISNULL
      AND sp.start_why = 'New 2024 gaps (MCO and proxy) for which compliance will carry forward'
      AND sp.patient_measure_id = pm.patient_quality_measure_id
    ;

-- commented the mrp delete 6/28/2024 (NOTE: change to TRC if we want it back)
--     DELETE
--     FROM
--         _patient_measures pm
--     WHERE
--         pm.measure_code in ('MRP')
--     -- 2024-06-26 removed delete of  'A1C9' per MT
--     ;
    ------------------------------------------------------------------------------------------------------------------------
    /* END temporary work arounds to hold back measures */
    ------------------------------------------------------------------------------------------------------------------------


--     -- create/update reveleer_chases. Table maps 1to1 with chases in reveleer's system
--     -- these can link to multiple pqm's for TRC and COA
    INSERT
    INTO
        reveleer_chases  as rc (patient_id, measure_code, due_date, yr, reveleer_project_id, qm_patient_measure_ids)
    SELECT
        pm.patient_id
      , pm.measure_code
      , pm.must_close_by_date
      , pm.operational_year
      , pm.reveleer_project_id
--       , concat_ws('::', patient_id, measure_code, must_close_by_date, operational_year, reveleer_project_id) unqiue_key
      , ARRAY_AGG(distinct pm.patient_quality_measure_id) qm_patient_measure_ids
    FROM
        _patient_measures pm
    GROUP BY 1, 2, 3, 4, 5
    ON CONFLICT (patient_id, measure_code, due_date, yr, reveleer_project_id)
        DO UPDATE
        SET qm_patient_measure_ids = rc.qm_patient_measure_ids || excluded.qm_patient_measure_ids
    WHERE
        not rc.qm_patient_measure_ids @> excluded.qm_patient_measure_ids
    ;
--     create INDEX on reveleer_chases using gin (qm_patient_measure_ids);

    -- 12min
    UPDATE _patient_measures pm
    SET chase_id = rc.id
    FROM
        reveleer_chases rc
    WHERE
        pm.patient_id = rc.patient_id
        and pm.measure_code = rc.measure_code
        and pm.must_close_by_date = rc.due_date
        and pm.operational_year = rc.yr
        and pm.reveleer_project_id = rc.reveleer_project_id;
--     select * from _comp c where not exists(SELECT 1 FROM _patient_measures pm WHERE pm.chase_id = c.sample_id) ;


    -- 3min
    DROP TABLE IF EXISTS _reveleer_chase_file;
    CREATE TEMP TABLE _reveleer_chase_file AS
    SELECT DISTINCT ON (pm.chase_id)
        pm.patient_id
      , pm.reveleer_project_id
      , ROW_NUMBER() OVER ()                                     row_id
      , pm.payer_name                                            health_plan
      , msp.state                                                contract
      , pm.patient_id                                            member_id
      , 'Medicare'                                               line_of_business
      , 'Medicare'                                               product
      , pm.chase_id                                              sample_id
      , NULL                                                     sequence
      , pm.measure_code                                          measure_id
      , pm.chase_id                                              chase_id
      , pm.subscriber_id                                         enrollee_id
      , REPLACE(p.first_name, '.', '')                           member_fname
      , REPLACE(p.last_name, '.', '')                            member_lname
      , NULL                                                     member_mi
      , LEFT(p.gender, 1)                                        member_gender
      , TO_CHAR(p.dob, 'MM/DD/YYYY')                             member_dob
      , REGEXP_REPLACE(pa.line1, E'[\\n\\r]+', '', 'g')          member_address1
      , REGEXP_REPLACE(pa.line2, E'[\\n\\r]+', '', 'g')          member_address2
      , REGEXP_REPLACE(pa.city, E'[\\n\\r]+', '', 'g')           member_city
      , REGEXP_REPLACE(pa.state, E'[\\n\\r]+', '', 'g')          member_state
      , LEFT(REGEXP_REPLACE(pa.postal_code, '([^0-9])', ''), 5)  member_zip
      , NULL                                                     member_phone
      , NULL                                                     member_cellphone
      , NULL                                                     member_fax
      , NULL                                                     member_email
      , NULL                                                     member_last4
      , 'patient_mbi=' || pm.patient_mbi                         user_defined_values
      , 'Y'                                                      active
      , 'Reveleer'                                               retrieval_source
      , NULL                                                     chart_action
      , NULL                                                     third_party_vendor
      , sp.primary_physician_id                                  provider_id
      , coalesce(phys.first_name, phys.organization_name)        provider_firstname
      , coalesce(phys.last_name, phys.organization_name)         provider_lastname
      , phys.npi                                                 provider_npi
      , REGEXP_REPLACE(va.voluntary_alignment_tin, '-', '', 'g') tin
      , NULL                                                     provider_specialty
      , NULL                                                     provider_taxonomy
      , '4059999999'                                             chart_address_phone -- REGEXP_REPLACE(rp.admin_contact_phone, '^1', '') chart_address_phone
      , NULL                                                     chart_address_extension
      , NULL                                                     chart_address_fax
      , NULL                                                     chart_address_email
      , NULL                                                     chart_address_secondaryphone
      , rpo.name                                                 chart_address_grouping
      , rp.id                                                    chart_site_id
      , 'Rendering Location'                                     chart_address_type
      , rp.address1                                              chart_address1
      , rp.address2                                              chart_address2
      , rp.city                                                  chart_city
      , rp.state                                                 chart_state
      , rp.zip                                                   chart_zip_code
      , NULL                                                     comment
      , NULL                                                     alternate_address_phone
      , NULL                                                     alternate_address_extension
      , NULL                                                     alternate_address_fax
      , NULL                                                     alternate_address_email
      , NULL                                                     alternate_address_secondary_phone
      , NULL                                                     alternate_address_grouping
      , NULL                                                     alternate_site_id
      , NULL                                                     alternate_address_type
      , NULL                                                     alternate_address1
      , NULL                                                     alternate_address2
      , NULL                                                     alternate_city
      , NULL                                                     alternate_state
      , NULL                                                     alternate_zipcode
      , rp.name                                                  group_name
      , NULL                                                     contact_name
      , NULL                                                     dos_from
      , NULL                                                     dos_through
      , NULL                                                     chart_address_tag
      , NULL                                                     chase_tag
      , NULL                                                     chart_filename
      , false is_new
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patients p ON p.id = pm.patient_id
        JOIN fdw_member_doc.supreme_pizza sp on p.id = sp.patient_id
        JOIN fdw_member_doc.msh_state_payers msp on msp.id = sp.patient_state_payer_id
        LEFT JOIN fdw_member_doc.patient_addresses pa ON pa.patient_id = p.id
        LEFT JOIN fdw_member_doc.msh_physicians phys ON phys.id = sp.primary_physician_id
        LEFT JOIN fdw_member_doc.referring_partners rp ON rp.id = sp.primary_referring_partner_id
        LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id
        LEFT JOIN fdw_member_doc.va_physician_rpl_payer_assignment va ON va.rpl_id = sp.primary_referring_partner_id
    where
        -- 6/18/24  added where clause to remove null values per adam pritts/amy
          phys.last_name is not null
      and rp.id is not null
      and rp.address1 is not null
      and rp.city is not null
      and rp.state is not null
      and rp.zip is not null
    ORDER BY
        pm.chase_id, pm.must_close_by_date;

    UPDATE _reveleer_chase_file cf
    SET is_new = TRUE
    WHERE
        NOT EXISTS( SELECT
                        1
                    FROM
                        public.reveleer_chase_file_details cfd
                    WHERE
                          cfd.reveleer_chase_id = cf.chase_id
                      AND cfd.reveleer_file_id IS NOT NULL
                      AND cfd.yr = ( SELECT yr FROM _controls ) );


    ------------------------------------------------------------------------------------------------------------------------
    /* Build attributes table and populate */
    ------------------------------------------------------------------------------------------------------------------------

    DROP TABLE IF EXISTS _attributes;
    CREATE TEMP TABLE _attributes (
        chase_id bigint,
        attribute_group_name       TEXT,
        attribute_code             TEXT,
        attribute_value            TEXT,
        data_type_flag             TEXT DEFAULT 'DVA'
    );


    ------------------------------------------------------------------------------------------------------------------------
    /* CBP */
    ------------------------------------------------------------------------------------------------------------------------
    -- dx date
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.chase_id
      , 'Hypertension Diagnosis'    attribute_group_name
      , 'HypertensionDiagnosisDate' attribute_code
      , '12/30/2023'                attribute_value
      , 'Admin'                     data_type_flag
    FROM
        _patient_measures pm
    WHERE
        pm.measure_code IN ('CBP', 'BPD');

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.chase_id
      , 'Hypertension Diagnosis'    attribute_group_name
      , 'HypertensionDiagnosisDate' attribute_code
      , '12/31/2023'                attribute_value
      , 'Admin'                     data_type_flag
    FROM
        _patient_measures pm
    WHERE
        pm.measure_code IN ('CBP', 'BPD');


    -- test date
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Blood Preassure Test'                attribute_group_name
      , 'BloodPreassureTestDate'              attribute_code
      , TO_CHAR(pbp.encounter_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_blood_pressures pbp ON pbp.patient_id = pm.patient_id
            AND pbp.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
        pm.measure_code IN ('CBP', 'BPD')
    AND pbp.systolic between 40 and 300
    AND pbp.diastolic between 20 and 200
    ORDER BY pm.patient_id, pbp.encounter_date DESC;

    -- systolic
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Blood Preassure Test' attribute_group_name
      , 'Systolic'             attribute_code
      , pbp.systolic           attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_blood_pressures pbp ON pbp.patient_id = pm.patient_id
            AND pbp.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
        pm.measure_code IN ('CBP', 'BPD')
      AND pbp.systolic between 40 and 300
      AND pbp.diastolic between 20 and 200
    ORDER BY pm.patient_id, pbp.encounter_date DESC;
--     SELECT * FROM
--         _patient_measures pm
--         JOIN fdw_member_doc.patient_blood_pressures pbp ON pbp.patient_id = pm.patient_id
--         WHERE
--         pm.measure_code IN ('CBP', 'BPD')
--     ;


    -- diastolic
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Blood Preassure Test' attribute_group_name
      , 'Diastolic'            attribute_code
      , pbp.diastolic          attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_blood_pressures pbp ON pbp.patient_id = pm.patient_id
            AND pbp.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
        pm.measure_code IN ('CBP', 'BPD')
      AND pbp.systolic between 40 and 300
      AND pbp.diastolic between 20 and 200
    ORDER BY pm.patient_id, pbp.encounter_date DESC;

    ------------------------------------------------------------------------------------------------------------------------
    /* HBD */
    ------------------------------------------------------------------------------------------------------------------------
    -- hba1c test date
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Hba1c Test'                        attribute_group_name
      , 'Hba1cTestDate'                     attribute_code
      , TO_CHAR(hb.test_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_hba1cs hb ON hb.patient_id = pm.patient_id
            AND hb.test_date >= ( SELECT boy FROM _controls )
    WHERE
        pm.measure_code = 'A1C9'
    AND value::numeric between 0 and 20
    ORDER BY pm.patient_id, hb.test_date desc;


    -- hba1c test value
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Hba1c Test'     attribute_group_name
      , 'Hba1cTestValue' attribute_code
      , hb.value         attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_hba1cs hb ON hb.patient_id = pm.patient_id
            AND hb.test_date >= ( SELECT boy FROM _controls )
    WHERE
        pm.measure_code = 'A1C9'
    AND value::numeric between 0 and 20
    ORDER BY pm.patient_id, hb.test_date desc;


    ------------------------------------------------------------------------------------------------------------------------
    /* OMW */
    ------------------------------------------------------------------------------------------------------------------------
    -- fracture_date
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.chase_id
      , 'Osteoporosis Fracture'                            attribute_group_name
      , 'OsteoporosisFractureDate'                         attribute_code
      , TO_CHAR(pm.must_close_by_date - 180, 'MM/DD/YYYY') attribute_value
      , 'Admin'                                            data_type_flag
    FROM
        _patient_measures pm
    WHERE
          pm.measure_code = 'OMW'
      AND pm.must_close_by_date IS NOT NULL;


    ------------------------------------------------------------------------------------------------------------------------
    /* EED */
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Eye Exam'                               attribute_group_name
      , 'EyeExamDate'                            attribute_code
      , TO_CHAR(hb.encounter_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_eye_exams hb ON hb.patient_id = pm.patient_id
            AND hb.encounter_date >= ( SELECT boy FROM _controls )
            AND hb.encounter_date <= now()::date
    WHERE
        pm.measure_code = 'EED'
    ORDER BY pm.patient_id, hb.encounter_date DESC;


    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Eye Exam'            attribute_group_name
      , 'CorrectProviderType' attribute_code
      , 1                     attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_eye_exams hb ON hb.patient_id = pm.patient_id
            AND hb.encounter_date >= ( SELECT boy FROM _controls )
            AND hb.encounter_date <= now()::date
    WHERE
        pm.measure_code = 'EED'
    ORDER BY pm.patient_id, hb.encounter_date DESC;

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Eye Exam'      attribute_group_name
      , 'EyeExamResult' attribute_code
      , CASE
            WHEN hb.eye_exam_results_key = 'dilated_retinal_eye_exam_without_retinopathy' THEN 2
            WHEN hb.eye_exam_results_key = 'stereoscopic_photos_wi_retinopathy'           THEN 3
            WHEN hb.eye_exam_results_key = 'stereoscopic_photos_without_retinopathy'      THEN 2
            WHEN hb.eye_exam_results_key = 'eye_imaging_with_retinopathy'                 THEN 3
            WHEN hb.eye_exam_results_key = 'eye_imaging_without_retinopathy'              THEN 2
            WHEN hb.eye_exam_results_key = 'results_received_but_exam_type_unknown'       THEN 1
            WHEN hb.eye_exam_results_key = 'dilated_retinal_eye_exam_with_retinopathy'    THEN 3
            WHEN hb.eye_exam_results_key = 'no_evidence_of_retinopathy'                   THEN 2
            WHEN hb.eye_exam_results_key = 'had_evidence_of_retinopathy'                  THEN 3
            WHEN hb.eye_exam_results_key = 'result_unknown'                               THEN 1
            WHEN hb.eye_exam_results_key ISNULL                                           THEN 1
            END         attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_eye_exams hb ON hb.patient_id = pm.patient_id
            AND hb.encounter_date >= ( SELECT boy FROM _controls )
            AND hb.encounter_date <= now()::date
    WHERE
        pm.measure_code = 'EED'
    ORDER BY pm.patient_id, hb.encounter_date DESC;


    --     SELECT distinct eye_exam_results_key FROM fdw_member_doc.patient_eye_exams;
--     SELECT key, name, procedure_code FROM fdw_member_doc.qm_ref_eye_exam_results where is_deleted is DISTINCT FROM true;
    ------------------------------------------------------------------------------------------------------------------------
    /* BCS */
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Mammogram'                             attribute_group_name
      , 'MammogramDate'                         attribute_code
      , TO_CHAR(x.encounter_date, 'MM/DD/YYYY') attribute_value
    FROM
        ( SELECT
              bcs.encounter_date
            , pm.patient_quality_measure_id
          FROM
              _patient_measures pm
              JOIN fdw_member_doc.patient_breast_cancer_screenings bcs ON bcs.patient_id = pm.patient_id
                  AND bcs.encounter_date >= ( SELECT boy - '15 months'::INTERVAL FROM _controls )
          WHERE
              pm.measure_code = 'BCS'
          UNION
          SELECT
              se.scheduled_date encounter_date
            , pm.patient_quality_measure_id
          FROM
              _patient_measures pm
              -- clinical prof not populated until 3/22/24, need to get to values from this year prior to then
              JOIN fdw_member_doc.qm_pm_breast_cancer_screening_wfs wf
                   ON pm.patient_quality_measure_id = wf.patient_measure_id
              JOIN fdw_member_doc.qm_pm_scheduled_events se ON wf.pm_scheduled_event_id = se.id
          WHERE
                wf.is_results_in_emr
            AND se.scheduled_date <= NOW()::DATE
            AND se.scheduled_date >= ( SELECT boy - '15 months'::INTERVAL FROM _controls )
            AND pm.measure_code = 'BCS' ) x
        JOIN _patient_measures pm ON pm.patient_quality_measure_id = x.patient_quality_measure_id
    ORDER BY pm.patient_id, x.encounter_date DESC;


    ------------------------------------------------------------------------------------------------------------------------
    /* COA */
    ------------------------------------------------------------------------------------------------------------------------
    -- patient_functional_assessments: currently empty, will work on populating
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Functional Screening Tool'   attribute_group_name
      , 'FunctionalScreeningToolDate' attribute_code
      , TO_CHAR(v.date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
            -- TODO: can replace below once populated
            --         JOIN fdw_member_doc.patient_functional_assessments pfa ON pfa.patient_id = pm.patient_id
--             AND pfa.encounter_date >= ( SELECT boy FROM _controls )
        JOIN fdw_member_doc.qm_pm_functional_assessment_wfs wf ON wf.patient_measure_id = pm.patient_quality_measure_id
        JOIN fdw_member_doc.qm_pm_functional_assessment_provider_blocks pb ON pb.wf_id = wf.id
        JOIN fdw_member_doc.msh_cca_worksheets ws ON pb.ws_id = ws.id
        JOIN fdw_member_doc.visits v ON ws.visit_id = v.id
    --
    WHERE
          pm.measure_key = 'coa_pain_assessment'
      AND NOT pb.left_blank
    ORDER BY pm.patient_id, v.date DESC;



    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Pain Assessment'                     attribute_group_name
      , 'PainAssessmentDate'                  attribute_code
      , TO_CHAR(encounter_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_pain_assessments ppa ON ppa.patient_id = pm.patient_id
            AND ppa.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
        pm.measure_key = 'coa_pain_assessment'
    ORDER BY pm.patient_id, ppa.encounter_date DESC;


    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Medication Review'                       attribute_group_name
      , 'MedicationReviewDate'                    attribute_code
      , TO_CHAR(pmr.encounter_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_medication_reviews pmr ON pmr.patient_id = pm.patient_id
            AND pmr.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
        pm.measure_key = 'coa_medication_review'
    ORDER BY pm.patient_id, pmr.encounter_date DESC;

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Medication Review' attribute_group_name
      , 'HasMedicationList' attribute_code
      , 1                   attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_medication_reviews pmr ON pmr.patient_id = pm.patient_id
            AND pmr.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
        pm.measure_key = 'coa_medication_review'
    ORDER BY pm.patient_id, pmr.encounter_date DESC;

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Medication Review'   attribute_group_name
      , 'CorrectProviderType' attribute_code
      , 1                     attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_medication_reviews pmr ON pmr.patient_id = pm.patient_id
            AND pmr.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
        pm.measure_key = 'coa_medication_review'
    ORDER BY pm.patient_id, pmr.encounter_date DESC;

    ------------------------------------------------------------------------------------------------------------------------
    /* COL - default to 3*/
    ------------------------------------------------------------------------------------------------------------------------

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Fit-Dna Test'                          attribute_group_name
      , 'FitDnaTestDate'                        attribute_code
      , TO_CHAR(pcsr.encounter_date, 'MM/DD/YYYY') attribute_value
    FROM
      _patient_measures pm
      JOIN fdw_member_doc.patient_colorectal_screening_results pcsr ON pcsr.patient_id = pm.patient_id
          AND pcsr.encounter_date >= ( SELECT boy FROM _controls )
          AND pcsr.encounter_date <= NOW()::DATE
          AND pcsr.screening_type = 'fit_dna_test'
          AND pm.measure_code = 'COL'

    ORDER BY pm.patient_id, pcsr.encounter_date DESC;


    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Fit-Dna Test'     attribute_group_name
      , 'FitDnaTestResult' attribute_code
      , 3                  attribute_value
    FROM
        _attributes a
        JOIN _patient_measures pm ON pm.chase_id = a.chase_id
    WHERE
          a.attribute_group_name = 'Fit-Dna Test'
      AND a.attribute_code = 'FitDnaTestDate';


    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Colonoscopy Test'                         attribute_group_name
      , 'ColonoscopyTestDate'                      attribute_code
      , TO_CHAR(pcsr.encounter_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_colorectal_screening_results pcsr ON pcsr.patient_id = pm.patient_id
            AND pcsr.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
          pm.measure_code = 'COL'
      AND pcsr.screening_type = 'colonoscopy'
      AND pcsr.encounter_date <= NOW()::DATE
    ORDER BY pm.patient_id, pcsr.encounter_date DESC;

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Colonoscopy Test'      attribute_group_name
      , 'ColonoscopyTestResult' attribute_code
      , 3                       attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_colorectal_screening_results pcsr ON pcsr.patient_id = pm.patient_id
            AND pcsr.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
          pm.measure_code = 'COL'
      AND pcsr.screening_type = 'colonoscopy'
      AND pcsr.encounter_date <= NOW()::DATE
    ORDER BY pm.patient_id, pcsr.encounter_date DESC;

    -- only in clinical profile
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Sigmoidoscopy Test'                       attribute_group_name
      , 'SigmoidoscopyTestDate'                    attribute_code
      , TO_CHAR(pcsr.encounter_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_colorectal_screening_results pcsr ON pcsr.patient_id = pm.patient_id
            AND pcsr.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
          pm.measure_code = 'COL'
      AND pcsr.screening_type = 'flexible_sigmoidoscopy'
      AND pcsr.encounter_date <= NOW()::DATE
    ORDER BY pm.patient_id, pcsr.encounter_date DESC;

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Sigmoidoscopy Test'      attribute_group_name
      , 'SigmoidoscopyTestResult' attribute_code
      , 3                         attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_colorectal_screening_results pcsr ON pcsr.patient_id = pm.patient_id
            AND pcsr.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
          pm.measure_code = 'COL'
      AND pcsr.screening_type = 'flexible_sigmoidoscopy'
      AND pcsr.encounter_date <= NOW()::DATE
    ORDER BY pm.patient_id, pcsr.encounter_date DESC;

    -- only in clinical profile
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Gfobt Test'                               attribute_group_name
      , 'GfobtTestDate'                            attribute_code
      , TO_CHAR(pcsr.encounter_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_colorectal_screening_results pcsr ON pcsr.patient_id = pm.patient_id
            AND pcsr.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
          pm.measure_code = 'COL'
      AND pcsr.screening_type = 'fobt'
      AND pcsr.encounter_date <= NOW()::DATE
    ORDER BY pm.patient_id, pcsr.encounter_date DESC;

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value)
    SELECT DISTINCT ON (pm.patient_id)
        pm.chase_id
      , 'Gfobt Test'      attribute_group_name
      , 'GfobtTestResult' attribute_code
      , 3                 attribute_value
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.patient_colorectal_screening_results pcsr ON pcsr.patient_id = pm.patient_id
            AND pcsr.encounter_date >= ( SELECT boy FROM _controls )
    WHERE
          pm.measure_code = 'COL'
      AND pcsr.screening_type = 'fobt'
      AND pcsr.encounter_date <= NOW()::DATE
    ORDER BY pm.patient_id, pcsr.encounter_date DESC;

    ------------------------------------------------------------------------------------------------------------------------
    /* FMC */
    ------------------------------------------------------------------------------------------------------------------------
    -- Do we want discharge date or admit date
    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.chase_id
      , 'Emergency Department Visit	'            attribute_group_name
      , 'EmergencyDepartmentVisitDate'           attribute_code
      , TO_CHAR(wf.discharge_date, 'MM/DD/YYYY') attribute_value
      , 'ADMIN'
    FROM
        _patient_measures pm
    join fdw_member_doc.qm_pm_toc_er_followup_chronic_conditions_wfs wf on wf.patient_measure_id = pm.patient_quality_measure_id
    WHERE
        pm.measure_code = 'FMC';

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.chase_id
      , 'Inpatient Admission'                                                            attribute_group_name
      , 'InpatientAdmissionDate'                                                         attribute_code
      , TO_CHAR(COALESCE(mrp.admit_date, peid.admit_date, rdi.admit_date), 'MM/DD/YYYY') attribute_value
      , 'ADMIN'
    FROM
        _patient_measures pm
        LEFT JOIN fdw_member_doc.qm_pm_toc_med_rec_wfs mrp
                  ON pm.patient_quality_measure_id = mrp.patient_measure_id
                      AND mrp.admit_date IS NOT NULL
                      AND mrp.admit_date <= mrp.discharge_date
        LEFT JOIN fdw_member_doc.qm_pm_toc_engagement_after_discharge_wfs peid
                  ON pm.patient_quality_measure_id = peid.patient_measure_id
                      AND peid.admit_date IS NOT NULL
                      AND peid.admit_date <= peid.discharge_date
        LEFT JOIN fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs rdi
                  ON pm.patient_quality_measure_id = rdi.patient_measure_id
                      AND rdi.admit_date IS NOT NULL
                      AND rdi.admit_date <= rdi.discharge_date
    WHERE
        pm.measure_key IN (
                           'trc_peid_patient_engagement_after_ip_discharge',
                           'trc_rdi_receipt_of_discharge_information',
                           'trc_mrp_medication_reconciliation_post_discharge'
            )
    ;

    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.chase_id
      , 'Discharge From Hospital'                    attribute_group_name
      , 'DischargeFromHospitalDate'                  attribute_code
        -- monkey patched this to be discharge date for all trc except for nia
      , TO_CHAR(pm.must_close_by_date, 'MM/DD/YYYY') attribute_value
      , 'ADMIN'
    FROM
        _patient_measures pm
    WHERE
        pm.measure_key IN (
                           'trc_peid_patient_engagement_after_ip_discharge',
                           'trc_rdi_receipt_of_discharge_information',
                           'trc_mrp_medication_reconciliation_post_discharge'
            )
    ;


    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.chase_id
      , 'Medication Reconciliation'                                             attribute_group_name
      , 'MedicationReconciliationDate'                                          attribute_code
      , TO_CHAR(COALESCE(wf.encounter_date, d.inserted_at::DATE), 'MM/DD/YYYY') attribute_value
      , 'DVA'
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.qm_pm_toc_med_rec_wfs wf ON wf.patient_measure_id = pm.patient_quality_measure_id
        LEFT JOIN fdw_member_doc.documents d ON wf.worksheet_document_id = d.id AND d.deleted_at IS NULL
    WHERE
          pm.measure_key = 'trc_mrp_medication_reconciliation_post_discharge'
      AND COALESCE(wf.encounter_date, d.inserted_at::DATE) IS NOT NULL
      AND wf.worksheet_status IN ('completed_in_emr', 'completed_worksheet')
      AND COALESCE(wf.encounter_date, d.inserted_at::DATE) <= NOW()::DATE
    ;


    INSERT
    INTO
        _attributes (chase_id, attribute_group_name, attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.chase_id
      , 'Medication Reconciliation' attribute_group_name
      , 'HasMedicationList'         attribute_code
      , '1'                         attribute_value
      , 'DVA'
    FROM
        _patient_measures pm
        JOIN fdw_member_doc.qm_pm_toc_med_rec_wfs wf ON wf.patient_measure_id = pm.patient_quality_measure_id
        LEFT JOIN fdw_member_doc.documents d ON wf.worksheet_document_id = d.id AND d.deleted_at IS NULL
    WHERE
          pm.measure_key = 'trc_mrp_medication_reconciliation_post_discharge'
      AND COALESCE(wf.encounter_date, d.inserted_at::DATE) IS NOT NULL
      AND wf.worksheet_status IN ('completed_in_emr', 'completed_worksheet')
      AND COALESCE(wf.encounter_date, d.inserted_at::DATE) <= NOW()::DATE
    ;


    ------------------------------------------------------------------------------------------------------------------------
    /* END raw attributes */
    ------------------------------------------------------------------------------------------------------------------------

    ------------------------------------------------------------------------------------------------------------------------
    /* Delete out existing attributes already sent
       ONly need to do if we're doing a refresh but shouldn't hurt
       */
    ------------------------------------------------------------------------------------------------------------------------

    DELETE
    FROM
        _attributes a
    WHERE
        attribute_code not IN ('Systolic', 'Diastolic', 'Hba1cTestValue') -- only dedupe on BloodPreassureTestDate, these will get deleted out with the below
        and EXISTS ( SELECT
                     1
                 FROM
                     public.reveleer_attribute_file_details a2
                 WHERE
                       a.chase_id = a2.reveleer_chase_id
                   AND a.attribute_group_name = a2.attribute_group_name
                   AND a.attribute_code = a2.attribute_code
                   AND a.attribute_value = a2.attribute_value );

    -- remove null values
    DELETE
    FROM
        _attributes a
    WHERE
        a.attribute_value ISNULL;

    -- Delete out bp attributes that dont have date, sys and dias
    WITH
        bp_grps AS ( SELECT
                         chase_id
                       , COUNT(*) n
                     FROM
                         _attributes a
                     WHERE
                         attribute_code IN ('Systolic', 'Diastolic', 'BloodPreassureTestDate')
                     GROUP BY 1 )
--     select * from _attributes a1 join bp_grps bg on bg.chase_id = a1.chase_id and n % 3 != 0;
    DELETE
    FROM
        _attributes a1
        USING bp_grps bg
    WHERE
        bg.chase_id = a1.chase_id
    and n % 3 != 0
    ;

-- Delete out hbd attributes that dont have date and value
    WITH
        hbd_grps AS ( SELECT
                         chase_id
                       , COUNT(*) n
                     FROM
                         _attributes a
                     WHERE

                         attribute_code IN ('Hba1cTestDate', 'Hba1cTestValue')
                     GROUP BY 1 )
--     select * from _attributes a1 join hbd_grps bg on bg.chase_id = a1.chase_id and n % 2 != 0;
    DELETE
    FROM
        _attributes a1
        USING hbd_grps bg
    WHERE
        bg.chase_id = a1.chase_id
    and n % 2 != 0
    ;

    -- build attribute file format
    DROP TABLE IF EXISTS _attribute_file;
    CREATE TEMP TABLE _attribute_file AS
    SELECT
        rc.patient_id
      , rc.reveleer_project_id
      , ROW_NUMBER() OVER ()                                                              row_id
      , rc.patient_id                                                                     member_id
      , rc.id                                                                             sample_id
      , rc.measure_code                                                                   code
      , attr.attribute_group_name
      , attr.attribute_code
      , attr.attribute_value
      , ROW_NUMBER()
        OVER (PARTITION BY attr.chase_id, attr.attribute_group_name, attr.attribute_code) numerator_event_id
      , attr.data_type_flag
      , rc.id chase_id
      , false is_new
    FROM
        _attributes attr
        JOIN public.reveleer_chases rc ON rc.id = attr.chase_id
    ;


------------------------------------------------------------------------------------------------------------------------
/* Clean up data where requirements aren't met */
------------------------------------------------------------------------------------------------------------------------
     -- OMWW: need to have fracture date
    DELETE
    FROM
        _reveleer_chase_file cf
    WHERE
          measure_id = 'OMW'
      and is_new
      AND NOT EXISTS( SELECT 1
                      FROM _attributes a
                      WHERE a.chase_id = cf.chase_id);
    -- MRP: need to have discharge date
    DELETE
    FROM
        _reveleer_chase_file cf
    WHERE
          measure_id = 'MRP'
      and is_new
      AND NOT EXISTS( SELECT 1
                      FROM _attributes a
                      WHERE
                            a.chase_id = cf.chase_id);
    -- FMC:  need to have ER date
    DELETE
    FROM
        _reveleer_chase_file cf
    WHERE
          measure_id = 'FMC'
      and is_new
      AND NOT EXISTS( SELECT 1
                      FROM _attributes a
                      WHERE a.chase_id = cf.chase_id);

    -- BCS gender must be F. Update for measure and any other measures for that member, breaks reveleer otherwise
    UPDATE _reveleer_chase_file cf
    SET
        member_gender = 'F'
    WHERE
          member_gender != 'F'
      AND EXISTS( SELECT
                      1
                  FROM
                      _reveleer_chase_file cf2
                  WHERE
                        cf2.measure_id = 'BCS'
                    AND cf2.patient_id = cf.patient_id );

    UPDATE _reveleer_chase_file cf
    SET
        member_gender = 'F'
    WHERE
          member_gender != 'F'
      AND EXISTS( SELECT
                      1
                  FROM
                      public.reveleer_chase_file_details cf2
                  WHERE
                        cf2.measure_id = 'BCS'
                    AND cf2.patient_id = cf.patient_id );

    -- default gender to female when unknown
    UPDATE _reveleer_chase_file cf
    SET
        member_gender = 'F'
    WHERE
        member_gender NOT IN ('M', 'F');

    -- remove existing chases where attributes and compliance don't exist, no point in sending chase with no new info
    DELETE
    FROM
        _reveleer_chase_file cf
    WHERE
      not is_new
      and NOT EXISTS( SELECT 1 FROM _attribute_file af WHERE af.sample_id = cf.sample_id )
      and NOT EXISTS( SELECT 1 FROM _comp cp WHERE cp.sample_id = cf.sample_id )
    ;

    delete from _comp c where not exists(select 1 from _reveleer_chase_file cf where cf.sample_id = c.sample_id);
    delete from _attribute_file af where not exists(select 1 from _reveleer_chase_file cf where cf.sample_id = af.sample_id);
--  select * from _comp c where not exists(SELECT 1 FROM _reveleer_chase_file  pm WHERE pm.chase_id = c.sample_id) ;

    -- sync up is_new
    UPDATE _comp c
    SET
        is_new = cc.is_new
    FROM
        _reveleer_chase_file cc
    where cc.chase_id = c.sample_id::bigint
    ;
    UPDATE _attribute_file a
    SET
        is_new = cc.is_new
    FROM
        _reveleer_chase_file cc
    where cc.chase_id = a.sample_id::bigint
    ;
    ------------------------------------------------------------------------------------------------------------------------
    /* insert into permanent tables */
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        reveleer_compliance_file_details (patient_id, row_id, member_id, sample_id, measure_id,
                                          numerator_code, compliance_flag, compliance_type, inserted_at, updated_at,
                                          reveleer_project_id, yr, is_new)
    SELECT
        patient_id
      , ROW_NUMBER() OVER () row_id
--       , row_id
      , member_id
      , sample_id
      , measure_id
      , numerator_code
      , compliance_flag
      , compliance_type
      , inserted_at
      , updated_at
      , reveleer_project_id
      , yr
      , is_new
    FROM
        _comp c
    WHERE
        EXISTS( SELECT 1 FROM _reveleer_chase_file c WHERE c.sample_id = c.sample_id )
--     where reveleer_project_id IN (236, 238, 241)
    ;

    INSERT
    INTO
        reveleer_chase_file_details (patient_id, row_id, health_plan, contract, member_id,
                                     line_of_business, product, sample_id, sequence, measure_id, chase_id, enrollee_id,
                                     member_fname, member_lname, member_mi, member_gender, member_dob, member_address1,
                                     member_address2, member_city, member_state, member_zip, member_phone,
                                     member_cell_phone, member_fax, member_email, member_last4, user_defined_values,
                                     active,
                                     retrieval_source, chart_action, third_party_vendor, provider_id,
                                     provider_firstname,
                                     provider_lastname, provider_npi, tin, provider_specialty, provider_taxonomy,
                                     chart_address_phone, chart_address_extension, chart_address_fax,
                                     chart_address_email,
                                     chart_address_secondaryphone, chart_address_grouping, chart_site_id,
                                     chart_address_type, chart_address1, chart_address2, chart_city, chart_state,
                                     chart_zip_code, comment, alternate_address_phone, alternate_address_extension,
                                     alternate_address_fax, alternate_address_email, alternate_address_secondary_phone,
                                     alternate_address_grouping, alternate_site_id, alternate_address_type,
                                     alternate_address1, alternate_address2, alternate_city, alternate_state,
                                     alternate_zipcode, group_name, contact_name, dos_from, dos_through,
                                     chart_address_tag,
                                     chase_tag, chart_filename, inserted_at, updated_at, reveleer_project_id, yr,
                                     is_new, reveleer_chase_id)
    SELECT
        patient_id
      , row_id
      , health_plan
      , contract
      , member_id
      , line_of_business
      , product
      , sample_id
      , sequence
      , measure_id
      , chase_id
      , enrollee_id
      , member_fname
      , member_lname
      , member_mi
      , member_gender
      , member_dob
      , member_address1
      , member_address2
      , member_city
      , member_state
      , member_zip
      , member_phone
      , member_cellphone
      , member_fax
      , member_email
      , member_last4
      , user_defined_values
      , active
      , retrieval_source
      , chart_action
      , third_party_vendor
      , provider_id
      , provider_firstname
      , provider_lastname
      , provider_npi
      , tin
      , provider_specialty
      , provider_taxonomy
      , chart_address_phone
      , chart_address_extension
      , chart_address_fax
      , chart_address_email
      , chart_address_secondaryphone
      , chart_address_grouping
      , chart_site_id
      , chart_address_type
      , chart_address1
      , chart_address2
      , chart_city
      , chart_state
      , chart_zip_code
      , comment
      , alternate_address_phone
      , alternate_address_extension
      , alternate_address_fax
      , alternate_address_email
      , alternate_address_secondary_phone
      , alternate_address_grouping
      , alternate_site_id
      , alternate_address_type
      , alternate_address1
      , alternate_address2
      , alternate_city
      , alternate_state
      , alternate_zipcode
      , group_name
      , contact_name
      , dos_from
      , dos_through
      , chart_address_tag
      , chase_tag
      , chart_filename
      , NOW()
      , NOW()
      , reveleer_project_id
      , c.yr
      , cf.is_new
      , cf.chase_id
    FROM
        _reveleer_chase_file cf
        CROSS JOIN _controls c
--    where reveleer_project_id IN (236, 238, 241)
    ;

    INSERT
    INTO
        public.reveleer_attribute_file_details (patient_id, row_id, member_id, sample_id, measure_id,
                                                attribute_group_name,
                                                attribute_code, attribute_value, numerator_event_id, data_type_flag,
                                                inserted_at, updated_at, reveleer_project_id, is_new, reveleer_chase_id)
    SELECT
        patient_id
      , row_id
      , member_id
      , sample_id
      , code
      , attribute_group_name
      , attribute_code
      , attribute_value
      , numerator_event_id
      , data_type_flag
      , NOW()
      , NOW()
      , reveleer_project_id
      , a.is_new
      , a.chase_id
    FROM
        _attribute_file a
    WHERE
        EXISTS( SELECT 1 FROM _reveleer_chase_file c WHERE c.sample_id = a.sample_id )
--     and reveleer_project_id IN (236, 238, 241)
    ;
END;
$$;

ALTER PROCEDURE sp_reveleer_data_stager(BOOLEAN) OWNER TO postgres;

