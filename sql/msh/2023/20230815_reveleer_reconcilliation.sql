    ------------------------------------------------------------------------------------------------------------------------
    /* Need to determine who we've sent that should have been sent based on current methodology */
    ------------------------------------------------------------------------------------------------------------------------

    DROP TABLE IF EXISTS _patient_measures;
    CREATE TEMP TABLE _patient_measures AS
    SELECT DISTINCT
        sp.patient_id
      , pay.name                                              payer_name
      , ptr.state_payer_id
      , sp.primary_referring_partner_id
      , sp.primary_physician_id
      , CASE WHEN qm.code = 'BSC' THEN 'BCS' ELSE qm.code END code
      , qm.id                                                 quality_measure_id
      , pqm.id                                                patient_quality_measure_id
      , pqm.year                                              measure_year
      , pqm.source
      , qm.name                                               measure_name
    FROM
        fdw_member_doc.patient_quality_measures pqm
        JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
        JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
            AND qmc.measure_id = pqm.measure_id
            AND qmc.measure_year = pqm.year
            AND qmc.is_contracted -- this SHOULD be reflected in should_display already, but this is safer
        JOIN fdw_member_doc.quality_measures qm ON qm.id = qmc.measure_id AND qm.is_reveleer
        JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id and sp.is_quality_measures
        JOIN fdw_member_doc.layer_cake_patients lcp on lcp.patient_id = sp.patient_id and lcp.is_quality_measures
        JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id AND qm.name = ANY (ptr.measures_to_send)
        JOIN fdw_member_doc.payers pay ON pay.id = sp.patient_payer_id
    WHERE
            pqm.year = DATE_PART('year', NOW())
      AND   pqm.status IN ('closed_pending', 'in_progress', 'open', 'ordered', 'recommended', 'refused')
      AND   pqm.source IN ('mco', 'mco_fall_off')
      AND   pqm.should_display
      AND   qm.is_reveleer
      AND   sp.is_quality_measures
    ;

    DROP TABLE IF EXISTS _to_delete;
    CREATE TEMP TABLE _to_delete AS
    SELECT distinct
        cfd.chase_id, 'new 2023-08-14 logic' reason
    FROM
        reveleer_chase_file_details cfd
        LEFT JOIN _patient_measures pm ON cfd.chase_id = pm.patient_quality_measure_id
    where pm.patient_quality_measure_id ISNULL
    ;

    SELECT count(distinct chase_id)
    FROM
        reveleer_chase_file_details;

    SELECT count(distinct patient_quality_measure_id)
    FROM
        _patient_measures;


------------------------------------------------------------------------------------------------------------------------
/* Remove bcbst */
------------------------------------------------------------------------------------------------------------------------
--     INSERT INTO _to_delete (chase_id, reason)
    SELECT distinct cfd.chase_id, 'bcbst that should have been purged from reveleer'
    FROM
        junk.reveleer_bcbst_chases_20230815 cfd
        LEFT JOIN _patient_measures pm ON cfd.chase_id = pm.patient_quality_measure_id
    where pm.patient_quality_measure_id ISNULL
    and not exists(select 1 from _to_delete td where td.chase_id = cfd.chase_id)
    ;
    SELECT *
    FROM
        _to_delete;

    SELECT
        COUNT(DISTINCT chase_id)
    FROM
        reveleer_chase_file_details
    WHERE
        health_plan ~* 'bcbs';

    INSERT INTO _to_delete (chase_id, reason)
    SELECT distinct jcfd.chase_id, 'bcbst that should have been purged from reveleer'
    FROM
        junk.reveleer_bcbst_chases_20230815 jcfd
    left join reveleer_chase_file_details cfd on cfd.chase_id = jcfd.chase_id
    where cfd.chase_id ISNULL

    SELECT count(*)
    FROM
        _to_delete;

    SELECT rc.*, pqm.status, pqm.source
    FROM
        junk.reveleer_chases_20230815 rc
    left join _patient_measures pm on pm.patient_quality_measure_id = rc."Client Chase Key"
    left join fdw_member_doc.patient_quality_measures pqm on pqm.id = rc."Client Chase Key"
    where pm.patient_quality_measure_id ISNULL
    ;

    SELECT
        pqm.*
--     pay.name
--          , pay2.name
    FROM
        fdw_member_doc.supreme_pizza sp
    join fdw_member_doc.payers pay on pay.id = sp.patient_payer_id
    join fdw_member_doc.patient_quality_measures pqm on pqm.patient_id = sp.patient_id and pqm.year = 2023
--     JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
--         JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
--             AND qmc.measure_id = pqm.measure_id
--             AND qmc.measure_year = pqm.year
--             AND qmc.is_contracted -- this SHOULD be reflected in should_display already, but this is safer
--     join fdw_member_doc.payers pay2 on pay2.id = msp.payer_id
    where sp.patient_id = 66339
    ;

------------------------------------------------------------------------------------------------------------------------
/* backup chases we want to delete */
------------------------------------------------------------------------------------------------------------------------

--     select * from bk_up.reveleer_deleted_chases;
    CREATE TABLE bk_up.reveleer_deleted_chases (
        id                                BIGSERIAL PRIMARY KEY   NOT NULL,
        chase_id                          BIGINT                  NOT NULL,
        reason                            TEXT                    NOT NULL,
        deleted_at                        TIMESTAMP               NOT NULL DEFAULT NOW(),
        reveleer_file_id                  BIGINT,
        patient_id                        BIGINT,
        row_id                            INTEGER,
        health_plan                       TEXT,
        contract                          TEXT,
        member_id                         BIGINT,
        line_of_business                  TEXT,
        product                           TEXT,
        sample_id                         BIGINT,
        sequence                          TEXT,
        measure_id                        TEXT,
        enrollee_id                       TEXT,
        member_fname                      TEXT,
        member_lname                      TEXT,
        member_mi                         TEXT,
        member_gender                     TEXT,
        member_dob                        TEXT,
        member_address1                   TEXT,
        member_address2                   TEXT,
        member_city                       TEXT,
        member_state                      TEXT,
        member_zip                        TEXT,
        member_phone                      TEXT,
        member_cell_phone                 TEXT,
        member_fax                        TEXT,
        member_email                      TEXT,
        member_last4                      TEXT,
        user_defined_values               TEXT,
        active                            TEXT,
        retrieval_source                  TEXT,
        chart_action                      TEXT,
        third_party_vendor                TEXT,
        provider_id                       BIGINT,
        provider_firstname                TEXT,
        provider_lastname                 TEXT,
        provider_npi                      BIGINT,
        tin                               TEXT,
        provider_specialty                TEXT,
        provider_taxonomy                 TEXT,
        chart_address_phone               TEXT,
        chart_address_extension           TEXT,
        chart_address_fax                 TEXT,
        chart_address_email               TEXT,
        chart_address_secondaryphone      TEXT,
        chart_address_grouping            TEXT,
        chart_site_id                     BIGINT,
        chart_address_type                TEXT,
        chart_address1                    TEXT,
        chart_address2                    TEXT,
        chart_city                        TEXT,
        chart_state                       TEXT,
        chart_zip_code                    TEXT,
        comment                           TEXT,
        alternate_address_phone           TEXT,
        alternate_address_extension       TEXT,
        alternate_address_fax             TEXT,
        alternate_address_email           TEXT,
        alternate_address_secondary_phone TEXT,
        alternate_address_grouping        TEXT,
        alternate_site_id                 TEXT,
        alternate_address_type            TEXT,
        alternate_address1                TEXT,
        alternate_address2                TEXT,
        alternate_city                    TEXT,
        alternate_state                   TEXT,
        alternate_zipcode                 TEXT,
        group_name                        TEXT,
        contact_name                      TEXT,
        dos_from                          TEXT,
        dos_through                       TEXT,
        chart_address_tag                 TEXT,
        chase_tag                         TEXT,
        chart_filename                    TEXT,
        inserted_at                       TIMESTAMP,
        updated_at                        TIMESTAMP,
        state_payer_id                    BIGINT,
        yr                                INTEGER
    );


    INSERT
    INTO
        bk_up.reveleer_deleted_chases (chase_id, reason, reveleer_file_id, patient_id, row_id, health_plan, contract,
                                       member_id, line_of_business, product, sample_id, sequence, measure_id,
                                       enrollee_id, member_fname, member_lname, member_mi, member_gender, member_dob,
                                       member_address1, member_address2, member_city, member_state, member_zip,
                                       member_phone, member_cell_phone, member_fax, member_email, member_last4,
                                       user_defined_values, active, retrieval_source, chart_action, third_party_vendor,
                                       provider_id, provider_firstname, provider_lastname, provider_npi, tin,
                                       provider_specialty, provider_taxonomy, chart_address_phone,
                                       chart_address_extension, chart_address_fax, chart_address_email,
                                       chart_address_secondaryphone, chart_address_grouping, chart_site_id,
                                       chart_address_type, chart_address1, chart_address2, chart_city, chart_state,
                                       chart_zip_code, comment, alternate_address_phone, alternate_address_extension,
                                       alternate_address_fax, alternate_address_email,
                                       alternate_address_secondary_phone, alternate_address_grouping, alternate_site_id,
                                       alternate_address_type, alternate_address1, alternate_address2, alternate_city,
                                       alternate_state, alternate_zipcode, group_name, contact_name, dos_from,
                                       dos_through, chart_address_tag, chase_tag, chart_filename, inserted_at,
                                       updated_at, state_payer_id, yr)
    SELECT
        cfd.chase_id
      , 'Purge on 2023-08-21'
      , reveleer_file_id, patient_id, row_id, health_plan, contract,
        member_id, line_of_business, product, sample_id, sequence, measure_id,
        enrollee_id, member_fname, member_lname, member_mi, member_gender, member_dob,
        member_address1, member_address2, member_city, member_state, member_zip,
        member_phone, member_cell_phone, member_fax, member_email, member_last4,
        user_defined_values, active, retrieval_source, chart_action, third_party_vendor,
        provider_id, provider_firstname, provider_lastname, provider_npi, tin,
        provider_specialty, provider_taxonomy, chart_address_phone,
        chart_address_extension, chart_address_fax, chart_address_email,
        chart_address_secondaryphone, chart_address_grouping, chart_site_id,
        chart_address_type, chart_address1, chart_address2, chart_city, chart_state,
        chart_zip_code, comment, alternate_address_phone, alternate_address_extension,
        alternate_address_fax, alternate_address_email,
        alternate_address_secondary_phone, alternate_address_grouping, alternate_site_id,
        alternate_address_type, alternate_address1, alternate_address2, alternate_city,
        alternate_state, alternate_zipcode, group_name, contact_name, dos_from,
        dos_through, chart_address_tag, chase_tag, chart_filename, inserted_at,
        updated_at, state_payer_id, yr
    FROM
    junk.final_list_to_delete_from_reveleer_20230821 td
        JOIN reveleer_chase_file_details cfd ON cfd.chase_id = td."Client Chase Key"
    ;

select * from     junk.final_list_to_delete_from_reveleer_20230821 td
    DELETE
    FROM
        reveleer_chase_file_details cfd
        USING junk.final_list_to_delete_from_reveleer_20230821 j
    WHERE
        cfd.chase_id = j."Client Chase Key";


delete from reveleer_attribute_file_details cfd
    using
    junk.final_list_to_delete_from_reveleer_20230821 td
        where cfd.sample_id::integer = td."Client Chase Key";




