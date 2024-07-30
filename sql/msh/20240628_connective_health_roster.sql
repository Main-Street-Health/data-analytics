DROP TABLE IF EXISTS _pats;
CREATE TEMP TABLE _pats AS
SELECT DISTINCT on (p.first_name, p.id)
    mp.first_name          "ProviderFirstName"
  , mp.last_name           "ProviderLastName"
  , mp.npi                 "NPI"
  , rp.name                "ProviderClinicName"
  , rp.npi                 "ClinicNpi"
  , p.first_name           "FirstName"
  , p.last_name            "LastName"
  , p.dob                  "Date Of Birth"
  , LEFT(p.gender, 1)      "Gender"
  , pa.line1               "Address1"
  , pa.line2               "Address2"
  , pa.city                "City"
  , pa.state               "State"
  , pa.postal_code         "Zip"
  , cp_home.phone_number   "HOME"
  , cp_mobile.phone_number "MOBILE"
  , sp.patient_id
FROM
    qm_patient_measures qm
    JOIN supreme_pizza sp ON sp.patient_id = qm.patient_id
    JOIN patients p ON sp.patient_id = p.id
    JOIN msh_physicians mp ON sp.primary_physician_id = mp.id
    JOIN referring_partners rp ON rp.id = sp.primary_referring_partner_id
    JOIN patient_addresses pa ON p.id = pa.patient_id
    JOIN patient_contact_phones pcp_home ON p.id = pcp_home.patient_id
        AND pcp_home."primary"
    JOIN contact_phones cp_home ON cp_home.id = pcp_home.contact_phone_id
        AND cp_home.status = 'active'::TEXT
        AND cp_home.type = 'home'
    LEFT JOIN patient_contact_phones pcp_mobile ON p.id = pcp_mobile.patient_id
    LEFT JOIN contact_phones cp_mobile ON cp_mobile.id = pcp_mobile.contact_phone_id
        AND cp_mobile.status = 'active'::TEXT
        AND cp_mobile.type = 'mobile'
WHERE
      qm.is_active
  and sp.risk_program
  AND EXISTS( SELECT
                  1
              FROM
                  hospitalizations h
              WHERE
                  h.discharge_date > NOW() - '2 months'::INTERVAL
              and h.patient_id = sp.patient_id
              )
order by p.first_name, p.id
LIMIT 50
;

-- demographics
SELECT * FROM _pats;

-- diagnoses
SELECT
    p."FirstName"
  , p."LastName"
  , i.code_formatted                                             "ICD 10 Code"
  , i.short_description                                          "Display Value"
  , COALESCE(meed.plan_capture_date, meed.practice_capture_date) "Date"
  , p.patient_id
FROM
    _pats p
    JOIN msh_external_emr_diagnoses meed ON p.patient_id = meed.patient_id
    JOIN icd10s i ON meed.icd10_id = i.id
    LEFT JOIN visits v ON v.patient_id = meed.patient_id AND v.type_id = 'cca_recon'::TEXT AND
                          v.deleted_at IS NULL AND
                          DATE_PART('year'::TEXT, v.date) = meed.cms_contract_year::DOUBLE PRECISION
    LEFT JOIN msh_cca_worksheets mcw ON mcw.visit_id = v.id
    LEFT JOIN msh_cca_worksheet_dxs dx_1
              ON dx_1.msh_cca_worksheet_id = mcw.id AND dx_1.icd10_id = i.id
WHERE
     meed.plan_capture_date IS NOT NULL
  OR meed.practice_capture_date IS NOT NULL
         AND NOT meed.is_deleted
         AND (meed.diagnosis_type = 'recapture'::TEXT OR
              CASE
                  WHEN dx_1.code_status = 'checked_in_worksheet'::TEXT THEN 'checked_yes_on_worksheet'::TEXT
                  ELSE dx_1.code_status
                  END = 'checked_yes_on_worksheet'::TEXT)





;
