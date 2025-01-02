
SELECT DISTINCT on (ndc)
   ndc
   , upper(drug_description)
FROM
    analytics.prd.patient_medications
WHERE
    ndc IN (
            '43598009805', '43598009905', '43598010105', '70710177000', '70710177509', '70710177700', '70710177709'
        )
order by ndc, inserted_at desc
;


INSERT
INTO
    ref.med_adherence_value_sets (value_set_id, value_set_subgroup, value_set_item, code_type, code, description,
                                  route, dosage_form, ingredient, strength, units, is_recycled, from_date, thru_date,
                                  attribute_type, attribute_value, inserted_at, updated_at)
VALUES
    ('STATINS', 'STATINS', 'ATORVASTATIN', 'NDC', '43598009805', 'ATORVASTATIN 10MG TABLETS', 'ORAL', 'TABLET', NULL, NULL, NULL, 'N', '1900-01-01', '2099-12-31', NULL, 'Manually added by BP 2024-10-30', now(), now()),
    ('STATINS', 'STATINS', 'ATORVASTATIN', 'NDC', '43598009905', 'ATORVASTATIN 20MG TABLETS', 'ORAL', 'TABLET', NULL, NULL, NULL, 'N', '1900-01-01', '2099-12-31', NULL, 'Manually added by BP 2024-10-30', now(), now()),
    ('STATINS', 'STATINS', 'ATORVASTATIN', 'NDC', '43598010105', 'ATORVASTATIN 40MG TABLETS', 'ORAL', 'TABLET', NULL, NULL, NULL, 'N', '1900-01-01', '2099-12-31', NULL, 'Manually added by BP 2024-10-30', now(), now()),
    ('STATINS', 'STATINS', 'ATORVASTATIN', 'NDC', '70710177000', 'ATORVASTATIN CALCIUM 80 MG TABS', 'ORAL', 'TABLET', NULL, NULL, NULL, 'N', '1900-01-01', '2099-12-31', NULL, 'Manually added by BP 2024-10-30', now(), now()),
    ('STATINS', 'STATINS', 'ATORVASTATIN', 'NDC', '70710177509', 'ATORVASTATIN 20MG TAB', 'ORAL', 'TABLET', NULL, NULL, NULL, 'N', '1900-01-01', '2099-12-31', NULL, 'Manually added by BP 2024-10-30', now(), now()),
    ('STATINS', 'STATINS', 'ATORVASTATIN', 'NDC', '70710177700', 'ATORVASTATIN 10MG TAB', 'ORAL', 'TABLET', NULL, NULL, NULL, 'N', '1900-01-01', '2099-12-31', NULL, 'Manually added by BP 2024-10-30', now(), now()),
    ('STATINS', 'STATINS', 'ATORVASTATIN', 'NDC', '70710177709', 'ATORVASTATIN 10MG TAB', 'ORAL', 'TABLET', NULL, NULL, NULL, 'N', '1900-01-01', '2099-12-31', NULL, 'Manually added by BP 2024-10-30', now(), now())
returning id
;

CREATE TABLE junk.patients_to_rerun_ss_20241030 AS
SELECT DISTINCT
    patient_id
FROM
    fdw_member_doc.patient_medication_fills pf
WHERE
    pf.ndc IN (
               '43598009805', '43598009905', '43598010105', '70710177000', '70710177509', '70710177700', '70710177709'
        );

SELECT *
FROM
    junk.patients_to_rerun_ss_20241030 j
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_metrics m
              ON m.patient_id = j.patient_id AND m.measure_key = 'med_adherence_cholesterol'


;

-- manual update fills table
UPDATE fdw_member_doc.patient_medication_fills pf
SET
    measure_key = 'med_adherence_cholesterol', updated_at = NOW()
WHERE
    pf.ndc IN (
               '43598009805', '43598009905', '43598010105', '70710177000', '70710177509', '70710177700', '70710177709'
        )
and measure_key is DISTINCT FROM 'med_adherence_cholesterol';
;
-- resend to ss to get latest and synth periods


DROP TABLE IF EXISTS _patients_to_pull;
CREATE TEMP TABLE _patients_to_pull (
    patient_id BIGINT PRIMARY KEY NOT NULL,
    reason     TEXT   NOT NULL
);

INSERT
INTO
    _patients_to_pull (patient_id, reason)
SELECT
    patient_id
  , 'Manual run after manual ndc update'
FROM
    junk.patients_to_rerun_ss_20241030 j;


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
