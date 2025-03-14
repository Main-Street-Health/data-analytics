ref.med_adherence_value_sets
69097079105 cholesterol

SELECT *
FROM
    ref.med_adherence_value_sets vs
WHERE
    description ~* 'PRAVASTATIN';
SELECT *
FROM
    analytics.ref.med_adherence_measures;
-- description ~* 'PRAVASTATIN SODIUM 40 MG TABLETS';


INSERT
INTO
    ref.med_adherence_value_sets (value_set_id, value_set_subgroup, value_set_item, code_type, code, description,
                                  route, dosage_form, ingredient, strength, units, is_recycled, from_date, thru_date,
                                  attribute_type, attribute_value, inserted_at, updated_at)
VALUES
    ('STATINS', 'STATINS', 'PRAVASTATIN', 'NDC', '69097079105', 'PRAVASTATIN 40 MG TABLET', 'ORAL', 'TABLET',
     NULL, NULL, NULL, 'N', '2024-06-01', '2099-12-31', NULL, NULL, NOW(),
     NOW());


DROP TABLE IF EXISTS _pats_to_resend;
CREATE TEMP TABLE _pats_to_resend AS
SELECT DISTINCT
    patient_id
FROM
    analytics.prd.patient_medications pm
WHERE
      ndc = '69097079105'
  AND last_filled_date >= '2025-01-01'
;
SELECT *
FROM
    _pats_to_resend;
DROP TABLE IF EXISTS _patients_to_pull;
        CREATE TEMP TABLE _patients_to_pull (
            patient_id BIGINT PRIMARY KEY NOT NULL,
            reason     TEXT   NOT NULL
        );
        CREATE UNIQUE INDEX on _patients_to_pull(patient_id);


INSERT
INTO
    _patients_to_pull (patient_id, reason)
select patient_id, 'Pull for new NDC mapping updates' from _pats_to_resend;


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

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    fdw_member_doc.patient_medication_fills WHERE drug_description ~* 'prava';
UPDATE fdw_member_doc.patient_medication_fills
SET
    measure_key = 'med_adherence_cholesterol'
  , updated_at  = NOW()
where ndc = '69097079105' and measure_key ISNULL ;
