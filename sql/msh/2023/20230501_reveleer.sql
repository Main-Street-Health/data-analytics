CREATE or replace PROCEDURE sp_populate_reveleer_files()
    LANGUAGE plpgsql
AS
$$
BEGIN

DROP TABLE IF EXISTS _deduped_measure_codes;
CREATE TEMP TABLE _deduped_measure_codes AS
SELECT
    pqm.patient_id
  , qm.code
  , pay.name       payer_name
  , sp.primary_physician_id
  , sp.primary_referring_partner_id
  , v.completed_at visit_completed_at
  , MIN(pqm.id)    patient_quality_measure_id
  , ARRAY_AGG(DISTINCT pqm.source) FILTER ( WHERE pqm.source IS NOT NULL )
FROM
    fdw_member_doc.quality_measures qm
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.measure_id = qm.id
        AND qmc.is_contracted
        AND qmc.measure_year = 2023
    JOIN fdw_member_doc.patient_quality_measures pqm
         ON pqm.measure_id = qm.id AND pqm.year = 2023
    JOIN fdw_member_doc.payers pay ON qmc.payer_id = pay.id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
        AND sp.patient_payer_id = qmc.payer_id
        AND sp.is_quality_measures
        AND sp.patient_payer_id = 38
    JOIN fdw_member_doc.msh_cca_worksheet_patient_quality_measures wpm ON pqm.id = wpm.patient_quality_measure_id
    JOIN fdw_member_doc.msh_cca_worksheets cw ON cw.id = wpm.msh_cca_worksheet_id AND cw.status = 'completed'
    JOIN fdw_member_doc.visits v ON cw.visit_id = v.id AND v.type_id = 'cca_recon'
        AND v.deleted_at ISNULL
WHERE
    qm.is_reveleer
    and qm.code not in ('SPC', 'SPD', 'COA')
    and pqm.source = 'mco'
    AND v.completed_at BETWEEN '2023-01-01' AND '2023-02-14'
--                                 and pqm.source = 'mco'
GROUP BY
    1, 2, 3, 4, 5, 6
ORDER BY
    v.completed_at
;


DROP TABLE IF EXISTS _reveleer_chase_file;
CREATE TEMP TABLE _reveleer_chase_file AS
SELECT
    d.patient_id
  , ROW_NUMBER() OVER ()         row_id
  , d.payer_name                 health_plan
  , NULL                         contract
  , d.patient_id                 member_id
  , 'Medicare'                   line_of_business -- not sure if we have medicaid flag somewhere?
  , NULL                         product
  , d.patient_quality_measure_id sample_id
  , NULL                         sequence
  , d.code                       measure_id
  , d.patient_quality_measure_id chase_id
  , NULL                         enrollee_id
  , p.first_name                 member_fname
  , p.last_name                  member_lname
  , NULL                         member_mi
  , LEFT(p.gender, 1)            member_gender
  , p.dob                        member_dob
  , pa.line1                     member_address1
  , pa.line2                     member_address2
  , pa.city                      member_city
  , pa.state                     member_state
  , pa.postal_code               member_zip
  , NULL                         member_phone
  , NULL                         member_cellphone
  , NULL                         member_fax
  , NULL                         member_email
  , NULL                         member_last4
  , NULL                         user_defined_values
  , 'Y'                          active
  , 'Reveleer'                   retrieval_source
  , NULL                         chart_action
  , NULL                         third_party_vendor
  , d.primary_physician_id       provider_id
  , phys.first_name              provider_firstname
  , phys.last_name               provider_lastname
  , phys.npi                     provider_npi
  , NULL                         tin
  , NULL                         provider_specialty
  , NULL                         provider_taxonomy
  , rp.admin_contact_phone       chart_address_phone
  , NULL                         chart_address_extension
  , NULL                         chart_address_fax
  , NULL                         chart_address_email
  , NULL                         chart_address_secondaryphone
  , rpo.name                     chart_address_grouping
  , rp.id                        chart_site_id
  , 'Rendering Location'         chart_address_type
  , rp.address1                  chart_address1
  , rp.address2                  chart_address2
  , rp.city                      chart_city
  , rp.state                     chart_state
  , rp.zip                       chart_zip_code
  , NULL                         comment
  , NULL                         alternate_address_phone
  , NULL                         alternate_address_extension
  , NULL                         alternate_address_fax
  , NULL                         alternate_address_email
  , NULL                         alternate_address_secondary_phone
  , NULL                         alternate_address_grouping
  , NULL                         alternate_site_id
  , NULL                         alternate_address_type
  , NULL                         alternate_address1
  , NULL                         alternate_address2
  , NULL                         alternate_city
  , NULL                         alternate_state
  , NULL                         alternate_zipcode
  , rp.name                      group_name
  , NULL                         contact_name
  , NULL                         dos_from
  , NULL                         dos_through
  , NULL                         chart_address_tag
  , NULL                         chase_tag
  , NULL                         chart_filename
FROM
    _deduped_measure_codes d
    JOIN fdw_member_doc.patients p ON p.id = d.patient_id
    JOIN fdw_member_doc.patient_addresses pa ON pa.patient_id = p.id
    JOIN fdw_member_doc.msh_physicians phys ON phys.id = d.primary_physician_id
    JOIN fdw_member_doc.referring_partners rp ON rp.id = d.primary_referring_partner_id
    JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id
where d.code != 'OMW' -- TODO: figure out fractures lookup
order by d.visit_completed_at
    ;

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
                                 chase_tag, chart_filename, inserted_at, updated_at)
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
                                 chase_tag, chart_filename, now(), now()
                                     from _reveleer_chase_file;


INSERT
INTO
    reveleer_attribute_file_details (patient_id, row_id, member_id, sample_id, measure_id, attribute_group_name, attribute_code,
                                     attribute_value, numerator_event_id, data_type_flag, inserted_at, updated_at)

SELECT
    patient_id
  , ROW_NUMBER() OVER ()        row_id
  , patient_id                  member_id
  , rcf.chase_id || '_' || v.rn sample_id
  , rcf.measure_id              measure_id
  , 'Hypertension Diagnosis'    attribute_group_name
  , 'HypertensionDiagnosisDate' attribute_code
  , v.day                       attribute_value
  , v.rn                        numerator_event_id
  , 'ADMIN'                     data_type_flag
, now()
, now()
FROM
    _reveleer_chase_file rcf
    CROSS JOIN ( SELECT * FROM ( VALUES ('2022-12-30', 1), ('2022-12-31', 2) ) x(day, rn) ) v
WHERE
    measure_id = 'CBP'
;



-- -- WHERE
-- --     measure_id = 'OMW'
-- ;
-- SELECT fracture_date FROM raw.patient_quality_roster_uhc WHERE care_opportunity ~* 'osteo';
-- SELECT
--     event_date
--   , event_date_description
-- ,  *
-- --   , patient_id
-- FROM
--     raw.bcbstn_patient_quality_gaps bcbstn
-- -- join prd.mco_patients mp on mp.mco_member_id = bcbstn.member_id and mp.payer_id = 38
-- WHERE
--     measure_name ~* 'OMW'
-- and member_name ~* 'Ursery'
-- ;
--
-- SELECT *
-- FROM raw.elevance_pharmacy_report
-- WHERE inbound_file_id = '3347170'
-- -- group by 1,2;
--
-- SELECT * FROM raw.humana_quality_stars_report_measure_detail where measure = 'OMW';
--
-- SELECT * FROM integrations.mco_quality_measure_mapping;
-- SELECT * FROM prd.mco_patient_quality_measures where raw_measure_name ~* 'OMW|osteo';
-- SELECT * FROM prd.load_mco_patient_quality_measures();
--
-- -- OMW
-- -- 'Osteoporosis Fracture', 'OsteoporosisFractureDate'
--
--
-- SELECT reveleer_file_id, count(*)
-- FROM
--     reveleer_chase_file_details GROUP BY 1;
--
--
-- CBP
-- Hypertension Diagnosis
-- HypertensionDiagnosisDate
-- REQUIRED, 2 HypertensionDiagnosisDate records required for each CBP Sample Member in the Chase File
--
-- OMW
-- Osteoporosis Fracture
-- OsteoporosisFractureDate
-- REQUIRED, for each OMW SampleMember in the Chase file
end
$$