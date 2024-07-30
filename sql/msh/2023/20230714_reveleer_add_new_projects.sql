------------------------------------------------------------------------------------------------------------------------
/* Adding 5 new state payers */
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
/* Add new projects - only need to run once */
------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS _projects_to_add;
-- CREATE TEMP TABLE _projects_to_add AS
-- SELECT *
-- FROM
--     ( VALUES
--           ('centene_al','Wellcare (Centene)', 'AL', '1796'),
--           ('centene_ar','Wellcare (Centene)', 'AR', '1797'),
--           ('centene_ky','Wellcare (Centene)', 'KY', '1798'),
--           ('centene_ny','Wellcare (Centene)', 'NY', '1799'),
--           ('uhc_ky','United', 'KY', '1800') ) x(name, payer, state, project_id);
--
-- INSERT
-- INTO
--     reveleer_projects (name, payer_id, state_payer_id, reveleer_id)
-- SELECT pta.name, p.id, sp.id, pta.project_id
-- FROM
--     _projects_to_add pta
--     JOIN fdw_member_doc.payers p ON p.name = pta.payer
--     JOIN fdw_member_doc.msh_state_payers sp ON sp.payer_id = p.id AND sp.state = pta.state;

DROP TABLE IF EXISTS _patient_measures;
CREATE TEMP TABLE _patient_measures AS
SELECT DISTINCT
    sp.patient_id
  , pay.name               payer_name
  , ptr.state_payer_id
  , sp.primary_referring_partner_id
  , sp.primary_physician_id
  , case when qm.code = 'BSC' then 'BCS' else qm.code END code
  , qm.id                  quality_measure_id
  , pqm.id                 patient_quality_measure_id
  , pqm.year               measure_year
  , pqm.source
  , w.id                   cca_worksheet_id
  , w.msh_physician_id
  , v.date                 date_of_visit
  , v.location_name        referring_partner_location
  , qma.name               measure_assessment_name
  , qm.name                measure_name
  , qmaa.label             measure_answer
  , qmaa.answer_group      measure_answer_group
  , qmaa.status_outcome    measure_status_outcome
  , qmaa.substatus_outcome measure_substatus_outcome
  , paa.details_provided   measure_value
  , paa.procedure_date     ans_procedure_date
FROM
    fdw_member_doc.supreme_pizza sp
    JOIN fdw_member_doc.payers pay ON pay.id = sp.patient_payer_id
    JOIN fdw_member_doc.visits v ON v.patient_id = sp.patient_id
    JOIN fdw_member_doc.msh_cca_worksheets w ON v.id = w.visit_id
    JOIN fdw_member_doc.msh_cca_worksheet_patient_quality_measures mcwpqm ON w.id = mcwpqm.msh_cca_worksheet_id
                                                                         AND mcwpqm.deleted_at ISNULL
    JOIN fdw_member_doc.patient_quality_measures pqm ON mcwpqm.patient_quality_measure_id = pqm.id
    LEFT JOIN fdw_member_doc.msh_patient_quality_measures mpqm on pqm.id = mpqm.patient_quality_measure_id
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN reveleer_projects ptr on ptr.state_payer_id = sp.patient_state_payer_id
    LEFT JOIN fdw_member_doc.patient_qm_assessment_answers paa ON w.id = paa.msh_cca_worksheet_id and mcwpqm.patient_qm_assessment_id = paa.patient_qm_assessment_id
    LEFT JOIN fdw_member_doc.msh_quality_measure_assessment_answers qmaa
              ON qmaa.id = paa.qm_assessment_answer_id
    LEFT JOIN fdw_member_doc.msh_quality_measure_assessments qma ON qmaa.msh_quality_measure_assessment_id = qma.id
WHERE
      v.date >= '2023-01-01'
  AND v.type_id = 'cca_recon'
  and (pqm.status, coalesce(mpqm.substatus,'')) in (('open', ''), ('closed_pending', ''), ('open', 'exclusion'), ('open', 'non_compliant'), ('open', 'refused'), ('open', 'research'))
  AND pqm.source IN ('mco', 'mco_fall_off', 'post_mco_proxy')
  AND w.invalid_reason ISNULL
  AND qm.is_reveleer
  AND sp.is_quality_measures
  AND qm.name IN (
                  'Breast Cancer Screening',
                  'Colorectal Screening',
                  'Controlling High Blood Pressure < 140/90',
                  'Eye Exam for Patients with Diabetes',
                  'Hemoglobin A1c Control for Patients With Diabetes',
                  'Kidney Health Evaluation for Patients with Diabetes',
                  'Osteoporosis Management', -- Note: none found for time range
                  'Care for Older Adult: Pain Assessment',
                  'Care for Older Adult: Medication Review',
                  'Care for Older Adult: Functional Status Assessment'
    )
  and ptr.id = 38 -- UHC KY
--   and ptr.id in ( 34,35,36,37,38 ) -- 5 new projects, only uhc ky has anything
;

-- SELECT
-- rp.name
--   , COUNT(*)
-- FROM
--     _patient_measures pm
-- join reveleer_projects rp on rp.state_payer_id = pm.state_payer_id
-- GROUP BY
--     1;


DROP TABLE IF EXISTS _deduped_measure_codes;
CREATE TEMP TABLE _deduped_measure_codes AS
SELECT
    pm.patient_id
  , pm.code
  , pm.payer_name
  , pm.state_payer_id
  , coalesce(pm.msh_physician_id, pm.primary_physician_id) msh_physician_id
  , pm.primary_referring_partner_id
  , pm.date_of_visit visit_completed_at
  , MIN(pm.patient_quality_measure_id)    patient_quality_measure_id
  , ARRAY_AGG(DISTINCT pm.source) FILTER ( WHERE pm.source IS NOT NULL )
FROM
    _patient_measures pm
GROUP BY 1,2,3,4,5,6,7
;


DROP TABLE IF EXISTS _reveleer_chase_file;
CREATE TEMP TABLE _reveleer_chase_file AS
SELECT
    d.patient_id
  , d.state_payer_id
  , ROW_NUMBER() OVER ()                             row_id
  , d.payer_name                                     health_plan
  , NULL                                             contract
  , d.patient_id                                     member_id
  , 'Medicare'                                       line_of_business
  , 'Medicare'                                       product
  , d.patient_quality_measure_id                     sample_id
  , NULL                                             sequence
  , d.code                                           measure_id
  , d.patient_quality_measure_id                     chase_id
  , NULL                                             enrollee_id
  , p.first_name                                     member_fname
  , p.last_name                                      member_lname
  , NULL                                             member_mi
  , LEFT(p.gender, 1)                                member_gender
  , TO_CHAR(p.dob, 'MM/DD/YYYY')                     member_dob
  , REGEXP_REPLACE(pa.line1, E'[\\n\\r]+', '', 'g')  member_address1
  , REGEXP_REPLACE(pa.line2, E'[\\n\\r]+', '', 'g')  member_address2
  , REGEXP_REPLACE(pa.city, E'[\\n\\r]+', '', 'g')   member_city
  , REGEXP_REPLACE(pa.state, E'[\\n\\r]+', '', 'g')  member_state
  , LEFT(pa.postal_code, 5)                          member_zip
  , NULL                                             member_phone
  , NULL                                             member_cellphone
  , NULL                                             member_fax
  , NULL                                             member_email
  , NULL                                             member_last4
  , NULL                                             user_defined_values
  , 'Y'                                              active
  , 'Reveleer'                                       retrieval_source
  , NULL                                             chart_action
  , NULL                                             third_party_vendor
  , d.msh_physician_id                               provider_id
  , phys.first_name                                  provider_firstname
  , phys.last_name                                   provider_lastname
  , phys.npi                                         provider_npi
  , NULL                                             tin
  , NULL                                             provider_specialty
  , NULL                                             provider_taxonomy
  , '4059999999' chart_address_phone -- REGEXP_REPLACE(rp.admin_contact_phone, '^1', '') chart_address_phone
  , NULL                                             chart_address_extension
  , NULL                                             chart_address_fax
  , NULL                                             chart_address_email
  , NULL                                             chart_address_secondaryphone
  , rpo.name                                         chart_address_grouping
  , rp.id                                            chart_site_id
  , 'Rendering Location'                             chart_address_type
  , rp.address1                                      chart_address1
  , rp.address2                                      chart_address2
  , rp.city                                          chart_city
  , rp.state                                         chart_state
  , rp.zip                                           chart_zip_code
  , NULL                                             comment
  , NULL                                             alternate_address_phone
  , NULL                                             alternate_address_extension
  , NULL                                             alternate_address_fax
  , NULL                                             alternate_address_email
  , NULL                                             alternate_address_secondary_phone
  , NULL                                             alternate_address_grouping
  , NULL                                             alternate_site_id
  , NULL                                             alternate_address_type
  , NULL                                             alternate_address1
  , NULL                                             alternate_address2
  , NULL                                             alternate_city
  , NULL                                             alternate_state
  , NULL                                             alternate_zipcode
  , rp.name                                          group_name
  , NULL                                             contact_name
  , NULL                                             dos_from
  , NULL                                             dos_through
  , NULL                                             chart_address_tag
  , NULL                                             chase_tag
  , NULL                                             chart_filename
FROM
    _deduped_measure_codes d
    JOIN fdw_member_doc.patients p ON p.id = d.patient_id
    JOIN fdw_member_doc.patient_addresses pa ON pa.patient_id = p.id
    JOIN fdw_member_doc.msh_physicians phys ON phys.id = d.msh_physician_id
    JOIN fdw_member_doc.referring_partners rp ON rp.id = d.primary_referring_partner_id
    JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id
ORDER BY
    d.visit_completed_at
    ;



-- Breast Cancer Screening	                                                 BCS	    Date of mammogram
-- Colorectal Cancer Screening	                                             COL	    Date of screening
-- Controlling High Blood Pressure	                                         CBP, BPD	Systolic Diastolic
-- Eye Exam for Patients WITH Diabetes                                       EED	    DATE OF test
-- Hemoglobin A1c Control FOR Patients WITH Diabetes	                     HBD	    A1c
-- Kidney Health Evaluation FOR Patients WITH Diabetes	                     KED	    DATE OF screening
-- Osteoporosis Management IN Women Who Had a Fracture	                     OMW	    DATE OF screening
-- Care FOR Older Adults: Pain assessment	                                 COA	    Does the patient have pain? Yes OR NO
-- Care FOR Older Adults: Medication Review	                                 COA	    Medication LIST documented IN the medical RECORD? Yes OR NO Medication LIST reviewed BY PROVIDER OR pharmacist? Yes OR NO
-- Care FOR Older Adults: Functional Assessment	                             COA	    "Completed in EMR" OR "Completed in CCA Worksheet"
-- SELECT DISTINCT
--     measure_name
--   , measure_assessment_name
--   , measure_answer
--   , measure_value
-- FROM
--     _patient_measures
-- ORDER BY
--     1, 2, 3;

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
  , '12/30/2022'                  attribute_value
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
  , '12/31/2022'                  attribute_value
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
  AND measure_value IS NOT NULL;




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
  , 'Hba1c Test'                                                          attribute_group_name
  , 'Hba1cTestDate'                                                       attribute_code
  , TO_CHAR(COALESCE(pm.ans_procedure_date, date_of_visit), 'MM/DD/YYYY') attribute_value
FROM
    _patient_measures pm
WHERE
      pm.code = 'HBD'
  AND pm.measure_assessment_name ~* 'a1c'
  AND measure_value ~* '^\d$|(^\d\.\d$)'; -- all sorts of junk in measure value, fairly strict regex on just #'s



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
  AND measure_value ~* '^\d$|(^\d\.\d$)'; -- all sorts of junk in measure value, fairly strict regex on just #'s

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
  AND date_of_visit IS NOT NULL
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
  AND date_of_visit IS NOT NULL
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
  , CASE
        WHEN pm.measure_answer = 'Medication list documented' AND pm2.measure_answer = 'Medication list reviewed' THEN 1
        ELSE 2 END      attribute_value
FROM
    _patient_measures pm
    LEFT JOIN _patient_measures pm2 ON pm2.cca_worksheet_id = pm.cca_worksheet_id AND pm2.code = 'COA'
        AND pm2.measure_name = 'Care for Older Adult: Medication Review'
        AND pm2.measure_answer IS NOT NULL
        AND pm2.measure_answer_group = 'reviewed'
        AND pm2.date_of_visit IS NOT NULL
WHERE
      pm.code = 'COA'
  AND pm.measure_name = 'Care for Older Adult: Medication Review'
  AND pm.measure_answer IS NOT NULL
  AND pm.measure_answer_group = 'documented'
  AND pm.date_of_visit IS NOT NULL
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
  AND measure_answer IS NOT NULL
  AND date_of_visit IS NOT NULL;




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
  , 'Fit-Dna Test'                       attribute_group_name
  , 'FitDnaTestDate'                     attribute_code
  , TO_CHAR(date_of_visit, 'MM/DD/YYYY') attribute_value
FROM
    _patient_measures pm
WHERE
      pm.code = 'COL'
  AND pm.measure_answer_group IN ('screening_type', 'na_already_completed_type')
  AND pm.measure_answer = 'FIT-DNA test'
  AND measure_answer IS NOT NULL
  AND date_of_visit IS NOT NULL;


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
  AND pm.measure_answer_group IN ('screening_type', 'na_already_completed_type')
  AND pm.measure_answer = 'FIT-DNA test'
  AND measure_answer IS NOT NULL
  AND date_of_visit IS NOT NULL;



INSERT
INTO
    _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                 attribute_code, attribute_value)
SELECT DISTINCT
    pm.patient_id
  , pm.state_payer_id
  , pm.code
  , pm.patient_quality_measure_id
  , 'Colonoscopy Test'                   attribute_group_name
  , 'ColonoscopyTestDate'                attribute_code
  , TO_CHAR(date_of_visit, 'MM/DD/YYYY') attribute_value
FROM
    _patient_measures pm
WHERE
      pm.code = 'COL'
  AND pm.measure_answer_group IN ('screening_type', 'na_already_completed_type')
  AND pm.measure_answer = 'Colonoscopy'
  AND measure_answer IS NOT NULL
  AND date_of_visit IS NOT NULL;


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
  AND pm.measure_answer_group IN ('screening_type', 'na_already_completed_type')
  AND pm.measure_answer = 'Colonoscopy'
  AND measure_answer IS NOT NULL
  AND date_of_visit IS NOT NULL;



INSERT
INTO
    _attributes (patient_id, state_payer_id, measure_id, patient_quality_measure_id, attribute_group_name,
                 attribute_code, attribute_value)
SELECT DISTINCT
    pm.patient_id
  , pm.state_payer_id
  , pm.code
  , pm.patient_quality_measure_id
  , 'Sigmoidoscopy Test'                 attribute_group_name
  , 'SigmoidoscopyTestDate'              attribute_code
  , TO_CHAR(date_of_visit, 'MM/DD/YYYY') attribute_value
FROM
    _patient_measures pm
WHERE
      pm.code = 'COL'
  AND pm.measure_answer_group IN ('screening_type', 'na_already_completed_type')
  AND pm.measure_answer = 'Flexible Sigmoidoscopy'
  AND measure_answer IS NOT NULL
  AND date_of_visit IS NOT NULL;



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
  AND pm.measure_answer_group IN ('screening_type', 'na_already_completed_type')
  AND pm.measure_answer = 'Flexible Sigmoidoscopy'
  AND measure_answer IS NOT NULL
  AND date_of_visit IS NOT NULL;



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
  , TO_CHAR(date_of_visit, 'MM/DD/YYYY') attribute_value
FROM
    _patient_measures pm
WHERE
      pm.code = 'COL'
  AND pm.measure_answer_group IN ('screening_type', 'na_already_completed_type')
  AND pm.measure_answer = 'Fecal Occult Blood Test'
  AND measure_answer IS NOT NULL
  AND date_of_visit IS NOT NULL;


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
  AND pm.measure_answer_group IN ('screening_type', 'na_already_completed_type')
  AND pm.measure_answer = 'Fecal Occult Blood Test'
  AND measure_answer IS NOT NULL
  AND date_of_visit IS NOT NULL;


------------------------------------------------------------------------------------------------------------------------
/* END raw attributes */
------------------------------------------------------------------------------------------------------------------------

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

SELECT * FROM _attribute_file;
SELECT * FROM _reveleer_chase_file;
------------------------------------------------------------------------------------------------------------------------
/* insert into perminent tables */
------------------------------------------------------------------------------------------------------------------------
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
                                 chase_tag, chart_filename, inserted_at, updated_at, state_payer_id)
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
                                 chase_tag, chart_filename, now(), now(), state_payer_id
                                     from _reveleer_chase_file cf
where
--     patient_id not in ( 16777, 267852, 335530, 38156, 38156, 38156, 38156, 38205, 38205, 38205, 38205, 47446, 47446, 47446) -- new bcbstn
(measure_id != 'OMW' or exists(select 1 from _attributes a WHERE a.patient_id = cf.patient_id and a.measure_id = 'OMW')) -- need to have fracture date
-- and state_payer_id = 282;
;

INSERT
INTO
    reveleer_attribute_file_details (patient_id, row_id, member_id, sample_id, measure_id, attribute_group_name,
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
where exists(select 1 from _reveleer_chase_file c where c.sample_id = a.sample_id);
-- CREATE TEMP TABLE _reveleer_chase_file
;

SELECT *
FROM
    reveleer_files ORDER BY id;

SELECT *
FROM
    reveleer_chase_file_details WHERE reveleer_file_id ISNULL ;
;

SELECT * FROM reveleer_attribute_file_details ;
SELECT * FROM reveleer_files;

-- DELETE FROM reveleer_attribute_file_details WHERE reveleer_file_id = 35;
-- DELETE FROM reveleer_files WHERE id = 35;

-- delete FROM
--     reveleer_files where id = 135;
-- TRUNCATE reveleer_chase_file_details RESTART IDENTITY ;
-- TRUNCATE reveleer_attribute_file_details RESTART IDENTITY ;
-- TRUNCATE reveleer_files RESTART IDENTITY cascade
