CREATE VIEW _dpc_roster
            ( patient_id, patient_first_name, patient_last_name, patient_gender, patient_dob, patient_dod
            , patient_address_1, patient_address_2, patient_city, patient_state, patient_zip, patient_mbi
            , provider_first_name, provider_last_name, provider_npi, provider_rank)
AS
WITH
    patient_docs AS ( SELECT
                          sp.patient_id
                        , mp.first_name
                        , mp.last_name
                        , mp.npi
                        , 0 AS rank
                      FROM
                          supreme_pizza sp
                          LEFT JOIN patient_contacts pc
                                    ON pc.patient_id = sp.patient_id AND pc.relationship = 'physician'::TEXT
                          LEFT JOIN msh_physicians mp
                                    ON pc.contact_id = mp.contact_id AND LENGTH(mp.npi::TEXT) = 10 AND NOT mp.deleted
                      WHERE
                            sp.is_dpc
                        AND pc.is_primary
                      UNION
                      SELECT DISTINCT ON (sp2.patient_id)
                          sp2.patient_id
                        , mp2.first_name
                        , mp2.last_name
                        , mp2.npi
                        , hier.appts_physician_rank
                      FROM
                          supreme_pizza sp2
                          LEFT JOIN stage.patient_physician_location_hierarchy hier ON hier.patient_id = sp2.patient_id
                          LEFT JOIN msh_physicians mp2
                                    ON hier.msh_physician_id = mp2.id AND LENGTH(mp2.npi::TEXT) = 10 AND NOT mp2.deleted
                      WHERE
                          sp2.is_dpc )
SELECT DISTINCT
    p.patient_id
  , p.patient_first_name
  , p.patient_last_name
  , p.gender              AS patient_gender
  , p.date_of_birth       AS patient_dob
  , p.date_of_death       AS patient_dod
  , p.address_line1       AS patient_address_1
  , p.address_line2       AS patient_address_2
  , p.address_city        AS patient_city
  , p.address_state       AS patient_state
  , p.address_postal_code AS patient_zip
  , p.mbi                 AS patient_mbi
  , doc.first_name        AS provider_first_name
  , doc.last_name         AS provider_last_name
  , doc.npi               AS provider_npi
  , doc.rank              AS provider_rank
FROM
    ent.patients p
    JOIN patient_docs doc ON doc.patient_id = p.patient_id
WHERE
    p.mbi IS NOT NULL;
