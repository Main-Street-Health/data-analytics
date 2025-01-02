CREATE PROCEDURE sp_reveleer_data_stager(IN _is_new_only boolean)
    LANGUAGE plpgsql
AS
$$
BEGIN
    -- _is_new_only = true means chases aka patient quality measures that we previously not sent to reveleer
    -- _is_new_only = false means we've already sent, sending new attributes but need to include chase

    DROP TABLE IF EXISTS _controls;
    CREATE TEMP TABLE _controls AS
    select _is_new_only is_new_only;
--     select true is_new_only;

    DROP TABLE IF EXISTS _patient_measures;
    CREATE TEMP TABLE _patient_measures AS
    SELECT DISTINCT
        sp.patient_id
      , sp.patient_mbi
      , pay.name                                              payer_name
      , ptr.state_payer_id
      , sp.primary_referring_partner_id
      , sp.primary_physician_id
      , CASE
          WHEN qm.code = 'BSC' THEN 'BCS'
          WHEN qm.code = 'HBD' THEN 'A1C9'
          WHEN qm.code = 'TRC' THEN 'MRP'
          ELSE qm.code
        END                                                   code
      , qm.id                                                 quality_measure_id
      , pqm.id                                                patient_quality_measure_id
      , pqm.year                                              measure_year
      , pqm.source
      , pqm.impact_date
      , w.id                                                  cca_worksheet_id
      , w.msh_physician_id
      , v.date                                                date_of_visit
      , v.location_name                                       referring_partner_location
      , qma.name                                              measure_assessment_name
      , qm.name                                               measure_name
      , qmaa.label                                            measure_answer
      , qmaa.answer_group                                     measure_answer_group
      , qmaa.status_outcome                                   measure_status_outcome
      , qmaa.substatus_outcome                                measure_substatus_outcome
      , qmaa.key                                              measure_ans_key
      , paa.details_provided                                  measure_value
      , paa.procedure_date                                    ans_procedure_date
    FROM
        fdw_member_doc.patient_quality_measures pqm
        JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
--         JOIN junk.cigna_not_in_reveleer_1_30_24 j on j.patient_id::bigint = pqm.patient_id and j.measure_id = pqm.measure_id
--         join junk.open_gaps_for_rev_v2_20240206 j on j.pqm_id = pqm.id
--         join junk.reveleer_open_v3_20240212 j on j.pqm_id = pqm.id
--         join junk.open_gaps_for_rev_v4_20240213 j on j.quality_measure_id_3 = pqm.id
--         join junk.reveleer_open_gaps_20240304 j on j.patient_quality_measure_id = pqm.id
--         join junk.reveleer_open_chases_20240306 j on j.pqm_id = pqm.id
--         join junk.reveleer_open_chases_20240306_part2 j on j.pqm_id = pqm.id
--         join junk.reveleer_open_gaps_20240307 j on j.pqm_id = pqm.id
--         join junk.reveleer_open_gaps_202403308 j on j.pqm_id = pqm.id
--         join junk.qm_chase_2023_elevance_last_batch j on j.pqm_id = pqm.id
--         join junk.chases_2023_reveleer_20240311 j on j.pqm_id = pqm.id
--         join junk.qm_chase_2023_reveleer_gaps_humana_20240312 j on j.pqm_id = pqm.id
--         join junk.qm_chase_2023_reveleer_gaps_v2_20240312 j on j.pqm_id = pqm.id
--         join junk.qm_chase_2023_reveleer_gaps_20240313 j on j.id = pqm.id
--         join junk.qm_chase_2023_reveleer_gaps_the_purge_recreated_20240313  j on j.pqm_id = pqm.id
--         join junk.qm_chase_2023_reveleer_gaps_20240314  j on j.pqm_id = pqm.id
--         join junk.qm_chase_2023_reveleer_gaps_v2_20240314  j on j.pqm_id = pqm.id
--         join junk.qm_chase_2023_reveleer_gaps_20240315  j on j.pqm_id = pqm.id
--         join junk.reveleer_missing_humana_to_resend_20240318  j on j.pqm_id = pqm.id
--         join junk.reveleer_missing_wc_cig_uhc_to_resend_20240318 j on j.pqm_id = pqm.id
--         join junk.reveleer_missing_hum_20240318  j on j.pqm_id = pqm.id
--         join junk.reveleer_missing_hum_20240319  j on j.pqm_id = pqm.id
--         join junk.reveleer_missing_cigna_20240321  j on j.pqm_id = pqm.id

--         join junk.reveleer_missing_cigna_to_resend_20240322  j on j.pqm_id = pqm.id



        JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
            AND qmc.measure_id = pqm.measure_id
            AND qmc.measure_year = pqm.year
        JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
        JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
        JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
        JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
        -- don't need a cca for this pull - all left joins below
        LEFT JOIN fdw_member_doc.visits v ON v.patient_id = sp.patient_id
            AND v.date >= '2023-01-01'
            AND v.type_id = 'cca_recon'
        LEFT JOIN fdw_member_doc.msh_cca_worksheets w ON v.id = w.visit_id
            AND w.invalid_reason ISNULL
        LEFT JOIN fdw_member_doc.msh_cca_worksheet_patient_quality_measures mcwpqm ON w.id = mcwpqm.msh_cca_worksheet_id
            AND mcwpqm.deleted_at ISNULL
            AND mcwpqm.patient_quality_measure_id = pqm.id
        LEFT JOIN fdw_member_doc.patient_qm_assessment_answers paa
                  ON w.id = paa.msh_cca_worksheet_id AND mcwpqm.patient_qm_assessment_id = paa.patient_qm_assessment_id
        LEFT JOIN fdw_member_doc.msh_quality_measure_assessment_answers qmaa
                  ON qmaa.id = paa.qm_assessment_answer_id
        LEFT JOIN fdw_member_doc.msh_quality_measure_assessments qma ON qmaa.msh_quality_measure_assessment_id = qma.id
    WHERE
            pqm.year = 2023
    and pqm.id = 681270
-- --         pqm.year = DATE_PART('year', NOW())
--       AND   pqm.status IN ('closed_pending', 'in_progress', 'open', 'ordered', 'recommended', 'refused', 'lost', 'closed_billing') -- 20240104 BP: added 'lost', 'closed_billing' per MThackson
--       AND   pqm.source IN ('mco')
--       AND   qm.is_reveleer
--       AND   qm.name = ANY (ptr.measures_to_send)
--       AND   sp.is_quality_measures
--       AND   qmc.is_contracted -- contracted for the given measure/payer/year
-- --     pqm.id in (487489,487490,186015,186016,494730,226522,495637,230430,247944,273481,273482,487742,166675,597673,597216,234183,234184,246273,246274,603562,544507,432569,533268,237729,196880,178873,197684,544473,492751,238289,276893,413153,539053,602183,171653,219415,404409,245636,247152,221303,597668,155881,531448,159508,159958,160480,160824,164180,167706,168473,172148,544465,188241,403727,214130,403864,221302,222822,225311,229122,496056,232320,232492,597215,234886,234942,533343,247975,442141,272881,544513,404974,539704,153437,153600,153648,153697,403329,154732,155421,155713,413123,368507,156565,484074,403348,158685,158791,159013,159573,159860,546905,160044,160149,544458,486895,164943,539895,368986,606921,369074,168082,502628,539644,171647,369219,172108,172322,173827,177445,177993,178468,413133,179046,418188,490484,187541,379182,531566,188359,196003,491468,202982,539041,212990,213185,214264,219818,220808,493218,221235,221822,222669,222687,522416,223484,223920,403982,494411,225410,225687,228125,228361,228826,228894,229650,230844,533218,232050,232175,233775,233958,233971,558642,404394,539670,235106,235150,235270,235670,236199,606926,399995,272378,272764,396592,275480,404694,367952,499442,401911,534975,539052,543566,558578,602176,405082,602177,405113,547042,557237,539717,188123,403394,485697,170315,403517,178553,202132,403951,399644,404315,404716)
    ;

    -- Delete out previously sent chases because we are only doing new right now
    -- reveleer is a pos that can only do all new or all updates in a single set of files
    if _is_new_only then
        DELETE
        FROM
            _patient_measures pm
        WHERE
            EXISTS( SELECT
                        1
                    FROM
                        public.reveleer_chase_file_details cfd
                    WHERE
                        cfd.sample_id = pm.patient_quality_measure_id );
    else
        -- Delete out chases that don't already exist - opposite of sending new chases
        DELETE
        FROM
            _patient_measures pm
        WHERE
            not EXISTS( SELECT
                        1
                    FROM
                        public.reveleer_chase_file_details cfd
                    WHERE
                        cfd.sample_id = pm.patient_quality_measure_id );
    end if;


    DROP TABLE IF EXISTS _deduped_measure_codes;
    CREATE TEMP TABLE _deduped_measure_codes AS
    SELECT
        pm.patient_id
      , pm.patient_mbi
      , pm.code
      , pm.payer_name
      , pm.state_payer_id
      , pm.impact_date
      , coalesce(pm.msh_physician_id, pm.primary_physician_id) msh_physician_id
      , pm.primary_referring_partner_id
      , pm.date_of_visit visit_completed_at
      , MIN(pm.patient_quality_measure_id)    patient_quality_measure_id
      , ARRAY_AGG(DISTINCT pm.source) FILTER ( WHERE pm.source IS NOT NULL )
    FROM
        _patient_measures pm
    GROUP BY 1,2,3,4,5,6,7,8,9
    ;

    DROP TABLE IF EXISTS _reveleer_chase_file;
    CREATE TEMP TABLE _reveleer_chase_file AS
    SELECT distinct on (patient_quality_measure_id)
        d.patient_id
      , d.state_payer_id
      , ROW_NUMBER() OVER ()                                    row_id
      , d.payer_name                                            health_plan
      , NULL                                                    contract
      , d.patient_id                                            member_id
      , 'Medicare'                                              line_of_business
      , 'Medicare'                                              product
      , d.patient_quality_measure_id                            sample_id
      , NULL                                                    sequence
      , d.code                                                  measure_id
      , d.patient_quality_measure_id                            chase_id
      , mpqm.mco_member_id                                      enrollee_id
      , replace(p.first_name, '.', '')                          member_fname
      , replace(p.last_name, '.', '')                           member_lname
      , NULL                                                    member_mi
      , LEFT(p.gender, 1)                                       member_gender
      , TO_CHAR(p.dob, 'MM/DD/YYYY')                            member_dob
      , REGEXP_REPLACE(pa.line1, E'[\\n\\r]+', '', 'g')         member_address1
      , REGEXP_REPLACE(pa.line2, E'[\\n\\r]+', '', 'g')         member_address2
      , REGEXP_REPLACE(pa.city, E'[\\n\\r]+', '', 'g')          member_city
      , REGEXP_REPLACE(pa.state, E'[\\n\\r]+', '', 'g')         member_state
      , LEFT(REGEXP_REPLACE(pa.postal_code, '([^0-9])', ''), 5) member_zip
      , NULL                                                    member_phone
      , NULL                                                    member_cellphone
      , NULL                                                    member_fax
      , NULL                                                    member_email
      , NULL                                                    member_last4
      , 'patient_mbi=' || d.patient_mbi                         user_defined_values
      , 'Y'                                                     active
      , 'Reveleer'                                              retrieval_source
      , NULL                                                    chart_action
      , NULL                                                    third_party_vendor
      , d.msh_physician_id                                      provider_id
      , phys.first_name                                         provider_firstname
      , phys.last_name                                          provider_lastname
      , phys.npi                                                provider_npi
      , regexp_replace(va.voluntary_alignment_tin, '-', '', 'g') tin
      , NULL                                                    provider_specialty
      , NULL                                                    provider_taxonomy
      , '4059999999'                                            chart_address_phone -- REGEXP_REPLACE(rp.admin_contact_phone, '^1', '') chart_address_phone
      , NULL                                                    chart_address_extension
      , NULL                                                    chart_address_fax
      , NULL                                                    chart_address_email
      , NULL                                                    chart_address_secondaryphone
      , rpo.name                                                chart_address_grouping
      , rp.id                                                   chart_site_id
      , 'Rendering Location'                                    chart_address_type
      , rp.address1                                             chart_address1
      , rp.address2                                             chart_address2
      , rp.city                                                 chart_city
      , rp.state                                                chart_state
      , rp.zip                                                  chart_zip_code
      , NULL                                                    comment
      , NULL                                                    alternate_address_phone
      , NULL                                                    alternate_address_extension
      , NULL                                                    alternate_address_fax
      , NULL                                                    alternate_address_email
      , NULL                                                    alternate_address_secondary_phone
      , NULL                                                    alternate_address_grouping
      , NULL                                                    alternate_site_id
      , NULL                                                    alternate_address_type
      , NULL                                                    alternate_address1
      , NULL                                                    alternate_address2
      , NULL                                                    alternate_city
      , NULL                                                    alternate_state
      , NULL                                                    alternate_zipcode
      , rp.name                                                 group_name
      , NULL                                                    contact_name
      , NULL                                                    dos_from
      , NULL                                                    dos_through
      , NULL                                                    chart_address_tag
      , NULL                                                    chase_tag
      , NULL                                                    chart_filename
    FROM
        _deduped_measure_codes d
        JOIN fdw_member_doc.patients p ON p.id = d.patient_id
        --- made left joins on 1/30/24 for weird one offs
        left JOIN fdw_member_doc.patient_addresses pa ON pa.patient_id = p.id
        left JOIN fdw_member_doc.msh_physicians phys ON phys.id = d.msh_physician_id
        left JOIN fdw_member_doc.referring_partners rp ON rp.id = d.primary_referring_partner_id
        left JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id
        --- left joins below
        LEFT JOIN fdw_member_doc.patient_quality_measures pqm ON d.patient_quality_measure_id = pqm.id
        LEFT JOIN fdw_member_doc_stage.mco_patient_quality_measures mpqm
                  ON pqm.mco_source_state_payer_id = mpqm.state_payer_id
                      AND pqm.patient_id = mpqm.patient_id
                      AND pqm.measure_id = mpqm.measure_id
                      AND mpqm.measure_year = pqm.year
        LEFT JOIN fdw_member_doc.va_physician_rpl_payer_assignment va on va.rpl_id = d.primary_referring_partner_id
    ORDER BY
        d.patient_quality_measure_id, d.visit_completed_at
        ;
    ------------------------------------------------------------------------------------------------------------------------
    /* Build attributes table and populate */
    ------------------------------------------------------------------------------------------------------------------------

    DROP TABLE IF EXISTS _attributes;
    CREATE TEMP TABLE _attributes (
        patient_id                 BIGINT,
        state_payer_id             BIGINT,
        measure_id                 TEXT,
        patient_quality_measure_id BIGINT,
        attribute_group_name       TEXT,
        attribute_code             TEXT,
        attribute_value            TEXT,
        data_type_flag text default 'Supp'
    );


    ------------------------------------------------------------------------------------------------------------------------
    /* CBP */
    ------------------------------------------------------------------------------------------------------------------------
    -- dx date
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Hypertension Diagnosis'    attribute_group_name
      , 'HypertensionDiagnosisDate' attribute_code
      , '12/30/2022'                attribute_value
      , 'Admin'                     data_type_flag
    FROM
        _patient_measures pm
    WHERE
        pm.code IN ('CBP', 'BPD')
    ;

    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Hypertension Diagnosis'    attribute_group_name
      , 'HypertensionDiagnosisDate' attribute_code
      , '12/31/2022'                attribute_value
      , 'Admin'                     data_type_flag
    FROM
        _patient_measures pm
    WHERE
        pm.code IN ('CBP', 'BPD')
    ;


    -- test date
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Blood Preassure Test'               attribute_group_name
      , 'BloodPreassureTestDate'             attribute_code
      , TO_CHAR(date_of_visit, 'MM/DD/YYYY') attribute_value

    FROM
        _patient_measures pm
    WHERE
          pm.code IN ('CBP', 'BPD')
      AND measure_assessment_name = 'Blood Pressure'
      AND date_of_visit IS NOT NULL
    ;


    -- systolic
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Blood Preassure Test' attribute_group_name
      , 'Systolic'             attribute_code
      , pm.measure_value       attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code IN ('CBP', 'BPD')
      AND measure_assessment_name = 'Blood Pressure'
      AND measure_answer ~* 'systolic'
      AND measure_value IS NOT NULL
      AND measure_value ~* '^\d*$'
      AND measure_value::numeric between 40 and 300
      ;

    -- diastolic
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Blood Preassure Test' attribute_group_name
      , 'Diastolic'            attribute_code
      , pm.measure_value       attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code IN ('CBP', 'BPD')
      AND measure_answer ~* 'diastolic'
      AND measure_value ~* '^\d*$'
      AND measure_value::numeric between 20 and 200
      AND measure_value IS NOT NULL;


    ------------------------------------------------------------------------------------------------------------------------
    /* HBD */
    ------------------------------------------------------------------------------------------------------------------------
    -- hba1c test date
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Hba1c Test'                                 attribute_group_name
      , 'Hba1cTestDate'                              attribute_code
      , TO_CHAR(pm.ans_procedure_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'HBD'
      AND pm.measure_assessment_name ~* 'a1c'
      -- all sorts of junk in measure value, fairly strict regex on just #'s
      AND measure_value ~* '^\d*$|(^\d*\.\d*$)'
      AND pm.ans_procedure_date IS NOT NULL
      AND measure_value::numeric between 0 and 20
      AND pm.measure_ans_key = 'na_already_completed'
    ;

    -- hba1c test value
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Hba1c Test'                            attribute_group_name
      , 'Hba1cTestValue'                         attribute_code
      , measure_value                           attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'HBD'
      AND pm.measure_assessment_name ~* 'a1c'
      AND nullif(trim(measure_value),'') is not null
      -- all sorts of junk in measure value, fairly strict regex on just #'s
      AND measure_value ~* '^\d$|(^\d\.\d$)'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
    ;

    ------------------------------------------------------------------------------------------------------------------------
    /* OMW */
    ------------------------------------------------------------------------------------------------------------------------
    -- fracture_date
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value, data_type_flag)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Osteoporosis Fracture'                            attribute_group_name
      , 'OsteoporosisFractureDate'                         attribute_code
      , TO_CHAR(mpqm.measure_due_date - 180, 'MM/DD/YYYY') attribute_value
      , 'Admin'                                            data_type_flag
    FROM
        _patient_measures pm
        JOIN prd.mco_patient_quality_measures mpqm
             ON mpqm.patient_id = pm.patient_id
                 AND mpqm.measure_year = pm.measure_year
                 AND mpqm.measure_id = pm.quality_measure_id
    WHERE
          pm.code = 'OMW'
      AND mpqm.measure_due_date IS NOT NULL
    ;

    ------------------------------------------------------------------------------------------------------------------------
    /* EED */
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Eye Exam'                                         attribute_group_name
      , 'EyeExamDate'                                      attribute_code
      , pm.ans_procedure_date                              attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'EED'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
    ;


    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Eye Exam'            attribute_group_name
      , 'CorrectProviderType' attribute_code
      , 1                     attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'EED'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
    ;

    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Eye Exam'            attribute_group_name
      , 'EyeExamResult'       attribute_code
      , case
        when measure_answer = 'Seven standard field stereoscopic photos with retinopathy.' then 3
        when measure_answer = 'Eye imaging validated to match diagnosis from 7 standard field stereoscopic photos without retinopathy.' then 2
        when measure_answer = 'Dilated retinal eye exam with retinopathy.' then 3
        when measure_answer = 'Seven standard field stereoscopic photos without retinopathy.' then 2
        when measure_answer = 'Dilated retinal eye exam without retinopathy.' then 2
        when measure_answer = 'Eye imaging validated to match diagnosis from 7 standard field stereoscopic photos with retinopathy.' then 3
        end                     attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'EED'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
    ;

    ------------------------------------------------------------------------------------------------------------------------
    /* BCS */
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Mammogram'                                  attribute_group_name
      , 'MammogramDate'                              attribute_code
      , TO_CHAR(pm.ans_procedure_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'BCS'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
    ;


    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Mammogram'       attribute_group_name
      , 'MammogramResult' attribute_code
      , 1                 attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'BCS'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
    ;

    ------------------------------------------------------------------------------------------------------------------------
    /* COA */
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Functional Screening Tool'          attribute_group_name
      , 'FunctionalScreeningToolDate'        attribute_code
      , TO_CHAR(date_of_visit, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COA'
      AND pm.measure_name = 'Care for Older Adult: Functional Status Assessment'
      AND pm.date_of_visit IS NOT NULL
      AND pm.measure_ans_key in ('on_cca', 'in_emr')
    ;


    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Pain Assessment'                    attribute_group_name
      , 'PainAssessmentDate'                 attribute_code
      , TO_CHAR(date_of_visit, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COA'
      AND pm.measure_name = 'Care for Older Adult: Pain Assessment'
      AND pm.date_of_visit IS NOT NULL
      AND pm.measure_ans_key in ('no_pain', 'has_pain')
    ;

    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Medication Review'                  attribute_group_name
      , 'MedicationReviewDate'               attribute_code
      , TO_CHAR(date_of_visit, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COA'
      AND pm.measure_name = 'Care for Older Adult: Medication Review'
      AND pm.date_of_visit IS NOT NULL
      AND pm.measure_ans_key = 'is_documented_true'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_ans_key = 'is_reviewed_true' )
    ;

    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Medication Review' attribute_group_name
      , 'HasMedicationList' attribute_code
      , 1                   attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COA'
      AND pm.measure_name = 'Care for Older Adult: Medication Review'
      AND pm.date_of_visit IS NOT NULL
      AND pm.measure_ans_key = 'is_documented_true'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_ans_key = 'is_reviewed_true' )
    ;

    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Medication Review'   attribute_group_name
      , 'CorrectProviderType' attribute_code
      , 1                     attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COA'
      AND pm.measure_name = 'Care for Older Adult: Medication Review'
      AND pm.measure_answer IS NOT NULL
      AND pm.date_of_visit IS NOT NULL
      AND pm.measure_ans_key = 'is_documented_true'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_ans_key = 'is_reviewed_true' )
    ;

    ------------------------------------------------------------------------------------------------------------------------
    /* COL - default to 3*/
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Fit-Dna Test'                            attribute_group_name
      , 'FitDnaTestDate'                          attribute_code
      , TO_CHAR(pm.ans_procedure_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COL'
      AND pm.measure_answer_group = 'na_reason'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_answer = 'FIT-DNA test'
                    AND pm2.measure_answer_group = 'na_already_completed_type' )
    ;

    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Fit-Dna Test'     attribute_group_name
      , 'FitDnaTestResult' attribute_code
      , 3                  attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COL'
      AND pm.measure_answer_group = 'na_reason'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_answer = 'FIT-DNA test'
                    AND pm2.measure_answer_group = 'na_already_completed_type' )
    ;


    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Colonoscopy Test'                        attribute_group_name
      , 'ColonoscopyTestDate'                     attribute_code
      , TO_CHAR(pm.ans_procedure_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COL'
      AND pm.measure_answer_group = 'na_reason'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_answer = 'Colonoscopy'
                    AND pm2.measure_answer_group = 'na_already_completed_type' )
    ;


    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Colonoscopy Test'      attribute_group_name
      , 'ColonoscopyTestResult' attribute_code
      , 3                       attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COL'
      AND pm.measure_answer_group = 'na_reason'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_answer = 'Colonoscopy'
                    AND pm2.measure_answer_group = 'na_already_completed_type' );



    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Sigmoidoscopy Test'                         attribute_group_name
      , 'SigmoidoscopyTestDate'                      attribute_code
      , TO_CHAR(pm.ans_procedure_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COL'
      AND pm.measure_answer_group = 'na_reason'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_answer = 'Flexible Sigmoidoscopy'
                    AND pm2.measure_answer_group = 'na_already_completed_type' )
    ;


    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Sigmoidoscopy Test'      attribute_group_name
      , 'SigmoidoscopyTestResult' attribute_code
      , 3                         attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COL'
      AND pm.measure_answer_group = 'na_reason'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_answer = 'Flexible Sigmoidoscopy'
                    AND pm2.measure_answer_group = 'na_already_completed_type' );



    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Gfobt Test'                         attribute_group_name
      , 'GfobtTestDate'                      attribute_code
      , TO_CHAR(pm.ans_procedure_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COL'
      AND pm.measure_answer_group = 'na_reason'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_answer = 'Fecal Occult Blood Test'
                    AND pm2.measure_answer_group = 'na_already_completed_type' );


    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Gfobt Test'      attribute_group_name
      , 'GfobtTestResult' attribute_code
      , 3                 attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'COL'
      AND pm.measure_answer_group = 'na_reason'
      AND pm.ans_procedure_date IS NOT NULL
      AND pm.measure_ans_key = 'na_already_completed'
      AND EXISTS( SELECT
                      1
                  FROM
                      _patient_measures pm2
                  WHERE
                        pm2.cca_worksheet_id = pm.cca_worksheet_id
                    AND pm2.patient_quality_measure_id = pm.patient_quality_measure_id
                    AND pm2.measure_answer = 'Fecal Occult Blood Test'
                    AND pm2.measure_answer_group = 'na_already_completed_type' );

   ------------------------------------------------------------------------------------------------------------------------
   /* NEW 20240130 */
   ------------------------------------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------------------------------------
    /* FMC */
    ------------------------------------------------------------------------------------------------------------------------
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Emergency Department Visit	'            attribute_group_name
      , 'EmergencyDepartmentVisitDate'           attribute_code
      , TO_CHAR(pm.impact_date - 7, 'MM/DD/YYYY')      attribute_value
--       , TO_CHAR(pm.impact_date, 'MM/DD/YYYY')      attribute_value
--       , TO_CHAR(pm.ans_procedure_date, 'MM/DD/YYYY') attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'FMC'
    ;
    INSERT
    INTO
        _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                     attribute_code, attribute_value)
    SELECT DISTINCT
        pm.patient_id
      , pm.state_payer_id
      , pm.code
      , pm.patient_quality_measure_id
      , 'Discharge From Hospital'                  attribute_group_name
      , 'DischargeFromHospitalDate'                attribute_code
      , TO_CHAR(pm.impact_date - 30, 'MM/DD/YYYY')      attribute_value
--       , TO_CHAR(pm.impact_date, 'MM/DD/YYYY')      attribute_value
    FROM
        _patient_measures pm
    WHERE
          pm.code = 'MRP'
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
        EXISTS ( SELECT
                     1
                 FROM
                     public.reveleer_attribute_file_details a2
                 WHERE
                       a.patient_quality_measure_id::text = a2.sample_id
                   AND a.attribute_group_name = a2.attribute_group_name
                   AND a.attribute_code = a2.attribute_code
                   AND a.attribute_value = a2.attribute_value );

    -- remove null values
    DELETE
    FROM
        _attributes a
    WHERE
        a.attribute_value ISNULL
        ;


    -- build attribute file format
    DROP TABLE IF EXISTS _attribute_file;
    CREATE TEMP TABLE _attribute_file AS
    SELECT
        attr.patient_id
      , attr.state_payer_id
      , ROW_NUMBER() OVER ()                                                                                 row_id
      , attr.patient_id                                                                                      member_id
      , attr.patient_quality_measure_id                                                                      sample_id
      , attr.measure_id                                                                                      code
      , attr.attribute_group_name
      , attr.attribute_code
      , attr.attribute_value
      , ROW_NUMBER()
        OVER (PARTITION BY attr.patient_id, attr.measure_id, attr.attribute_group_name, attr.attribute_code) numerator_event_id
      , attr.data_type_flag
    FROM
        _attributes attr
    ;


   -- clean up chase file based on attributes
   if _is_new_only THEN
        -- if NEW, remove missing fracture date omw
        DELETE
        FROM
            _reveleer_chase_file cf
        WHERE
              measure_id = 'OMW'
          AND NOT EXISTS( SELECT 1 FROM _attributes a WHERE a.patient_id = cf.patient_id AND a.measure_id = 'OMW' ); -- need to have fracture date
       DELETE
        FROM
            _reveleer_chase_file cf
        WHERE
              measure_id = 'MRP'
          AND NOT EXISTS( SELECT 1 FROM _attributes a WHERE a.patient_id = cf.patient_id AND a.measure_id = 'MRP' ); -- need to have discharge date
       DELETE
        FROM
            _reveleer_chase_file cf
        WHERE
              measure_id = 'FMC'
          AND NOT EXISTS( SELECT 1 FROM _attributes a WHERE a.patient_id = cf.patient_id AND a.measure_id = 'FMC' ); -- need to have ER date
   ELSE
       -- if a REFRESH, remove chases where attributes don't exist
        DELETE
        FROM
            _reveleer_chase_file cf
        WHERE
            NOT EXISTS( SELECT 1 FROM _attribute_file af WHERE af.sample_id = cf.sample_id );
   END IF;

    -- BCS gender must be F. Update for measure and any other measures for that member
    UPDATE _reveleer_chase_file cf
    SET
        member_gender = 'F'
    WHERE
          member_gender != 'F'
      AND EXISTS( SELECT 1
                  FROM _reveleer_chase_file cf2
                  WHERE cf2.measure_id = 'BCS' AND cf2.patient_id = cf.patient_id );

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
          member_gender not in ('M', 'F');
    ------------------------------------------------------------------------------------------------------------------------
    /* Compliance file */
    ------------------------------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS _num_code_mappings;
    CREATE TEMP TABLE _num_code_mappings AS
    SELECT *
    FROM
        ( VALUES
              ('BSC', 'bcs', 'Breast Cancer Screening'),
              ('CBP', 'bp', 'Controlling High Blood Pressure < 140/90'),
              ('COA', 'funcstat', 'Care for Older Adult: Functional Status Assessment'),
              ('COA', 'medreview', 'Care for Older Adult: Medication Review'),
              ('COA', 'painscreen', 'Care for Older Adult: Pain Assessment'),
              ('COL', 'col', 'Colorectal Screening'),
              ('EED', 'eyeexam', 'Eye Exam for Patients with Diabetes'),
              -- ('HBD', 'hba1c_8', 'Hemoglobin A1c Control for Patients With Diabetes'),
              ('HBD', 'hba1c_9', 'Hemoglobin A1c Control for Patients With Diabetes'),
              ('OMW', 'omw', 'Osteoporosis Management') ) x(measurecode, numerator_code, numeratorname);

    -- only populate compliance on refreshes
    if NOT _is_new_only THEN
        DROP TABLE IF EXISTS _comp;
        CREATE TEMP TABLE _comp AS
        SELECT
            pqm.patient_id
          , ROW_NUMBER() OVER ()                                  row_id
          , pqm.patient_id                                        member_id
          , pqm.id                                                sample_id
          , CASE
              WHEN qm.code = 'BSC' THEN 'BCS'
              WHEN qm.code = 'HBD' THEN 'A1C9'
              ELSE qm.code
            END                                                   measure_id
          , numerator_code
          , 'Y'                                                   compliance_flag
          , 'Admin'                                               compliance_type
          , NOW()                                                 inserted_at
          , NOW()                                                 updated_at
          , pqm.mco_source_state_payer_id                         state_payer_id
--           , DATE_PART('year', NOW())                              yr
        , 2023 yr
        FROM
            fdw_member_doc.patient_quality_measures pqm
            JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
            JOIN _num_code_mappings ncm ON ncm.numeratorname = qm.name
        WHERE
              pqm.status = 'closed_system'
--           AND pqm.year = DATE_PART('year', NOW())
          AND pqm.year = 2023
          AND EXISTS( SELECT
                          1
                      FROM
                          reveleer_chase_file_details cfd
                      WHERE
                            pqm.id = cfd.chase_id
                        AND pqm.patient_id = cfd.patient_id
                        AND cfd.yr = pqm.year
                        AND cfd.reveleer_file_id IS NOT NULL );
    END IF;
    ------------------------------------------------------------------------------------------------------------------------
    /* insert into permanent tables */
    ------------------------------------------------------------------------------------------------------------------------
    if NOT _is_new_only THEN
        INSERT
        INTO
            reveleer_compliance_file_details (patient_id, row_id, member_id, sample_id, measure_id,
                                              numerator_code, compliance_flag, compliance_type, inserted_at, updated_at,
                                              state_payer_id, yr)
        select
          patient_id, row_id, member_id, sample_id, measure_id,
          numerator_code, compliance_flag, compliance_type, inserted_at, updated_at,
          state_payer_id, yr
        from _comp;
    END IF;

    INSERT
    INTO
        reveleer_chase_file_details (patient_id, row_id, health_plan, contract, member_id,
                                     line_of_business, product, sample_id, sequence, measure_id, chase_id, enrollee_id,
                                     member_fname, member_lname, member_mi, member_gender, member_dob, member_address1,
                                     member_address2, member_city, member_state, member_zip, member_phone,
                                     member_cell_phone, member_fax, member_email, member_last4, user_defined_values, active,
                                     retrieval_source, chart_action, third_party_vendor, provider_id, provider_firstname,
                                     provider_lastname, provider_npi, tin, provider_specialty, provider_taxonomy,
                                     chart_address_phone, chart_address_extension, chart_address_fax, chart_address_email,
                                     chart_address_secondaryphone, chart_address_grouping, chart_site_id,
                                     chart_address_type, chart_address1, chart_address2, chart_city, chart_state,
                                     chart_zip_code, comment, alternate_address_phone, alternate_address_extension,
                                     alternate_address_fax, alternate_address_email, alternate_address_secondary_phone,
                                     alternate_address_grouping, alternate_site_id, alternate_address_type,
                                     alternate_address1, alternate_address2, alternate_city, alternate_state,
                                     alternate_zipcode, group_name, contact_name, dos_from, dos_through, chart_address_tag,
                                     chase_tag, chart_filename, inserted_at, updated_at, state_payer_id, yr)
                              select patient_id, row_id, health_plan, contract, member_id,
                                     line_of_business, product, sample_id, sequence, measure_id, chase_id, enrollee_id,
                                     member_fname, member_lname, member_mi, member_gender, member_dob, member_address1,
                                     member_address2, member_city, member_state, member_zip, member_phone,
                                     member_cellphone, member_fax, member_email, member_last4, user_defined_values, active,
                                     retrieval_source, chart_action, third_party_vendor, provider_id, provider_firstname,
                                     provider_lastname, provider_npi, tin, provider_specialty, provider_taxonomy,
                                     chart_address_phone, chart_address_extension, chart_address_fax, chart_address_email,
                                     chart_address_secondaryphone, chart_address_grouping, chart_site_id,
                                     chart_address_type, chart_address1, chart_address2, chart_city, chart_state,
                                     chart_zip_code, comment, alternate_address_phone, alternate_address_extension,
                                     alternate_address_fax, alternate_address_email, alternate_address_secondary_phone,
                                     alternate_address_grouping, alternate_site_id, alternate_address_type,
                                     alternate_address1, alternate_address2, alternate_city, alternate_state,
                                     alternate_zipcode, group_name, contact_name, dos_from, dos_through, chart_address_tag,
                                     chase_tag, chart_filename, now(), now(), state_payer_id, 2023
                                         from _reveleer_chase_file cf
    ;

    INSERT
    INTO
        public.reveleer_attribute_file_details (patient_id, row_id, member_id, sample_id, measure_id, attribute_group_name,
                                         attribute_code,
                                         attribute_value, numerator_event_id, data_type_flag, inserted_at, updated_at, state_payer_id)
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
    , state_payer_id
    FROM
        _attribute_file a
    where exists(select 1 from _reveleer_chase_file c where c.sample_id = a.sample_id)
    ;
end;
$$;

ALTER PROCEDURE sp_reveleer_data_stager(BOOLEAN) OWNER TO postgres;

