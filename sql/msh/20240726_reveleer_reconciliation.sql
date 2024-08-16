------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
-- create index on analytics.junk.reveleer_life_of_chase_20240726(clientchasekey);
SELECT count(*) FROM analytics.junk.reveleer_life_of_chase_20240726;
SELECT * FROM analytics.junk.reveleer_life_of_chase_20240726;
SELECT *
FROM
    analytics.junk.reveleer_life_of_chase_20240726
WHERE
    clientchasekey ISNULL ;



-- in reveleer, never sent
SELECT r.*
FROM
    analytics.junk.reveleer_life_of_chase_20240726 r
    LEFT JOIN reveleer_chase_file_details cfd ON cfd.chase_id = r.clientchasekey AND cfd.yr = 2024
WHERE
    cfd.id ISNULL
;

DROP TABLE IF EXISTS _sent_but_not_returned;
CREATE TEMP TABLE _sent_but_not_returned AS
SELECT cfd.*
FROM
    reveleer_chase_file_details cfd
    LEFT JOIN analytics.junk.reveleer_life_of_chase_20240726 r ON cfd.chase_id = r.clientchasekey
WHERE
    r.chaseid ISNULL
AND cfd.yr = 2024
;

SELECT count(*), count(distinct chase_id)
FROM
    _sent_but_not_returned ;

CREATE TABLE junk.reveleer_missing_measures_resend_pqms_20240801 AS
SELECT DISTINCT
    UNNEST(rc.qm_patient_measure_ids) pqm_id
  , rc.reveleer_project_id
  , rc.id                             chase_id
, rc.patient_id
, rc.measure_code
FROM
    _sent_but_not_returned nr
    JOIN reveleer_chases rc ON nr.chase_id = rc.id
WHERE
    rc.yr = 2024
;
SELECT  rp.name, count(*)
FROM
    junk.reveleer_missing_measures_resend_pqms_20240801 m
join reveleer_projects rp on m.reveleer_project_id = rp.id
GROUP BY rp.name
;



-- SELECT *
-- FROM
UPDATE
    reveleer_chases c
SET
    external_chase_id = r.chaseid,
    is_confirmed_in_reveleer_system = TRUE,
    confirmed_in_reveleer_system_at = NOW(),
    updated_at = NOW()
FROM
    analytics.junk.reveleer_life_of_chase_20240726 r
WHERE
    c.yr = 2024
and c.id = r.clientchasekey
and not c.is_confirmed_in_reveleer_system
;

SELECT
    is_confirmed_in_reveleer_system
  , COUNT(*)
FROM
    reveleer_chases rc
where rc.yr = 2024
and rc.is_active

GROUP BY
    1;

SELECT *
FROM
    reveleer_chase_file_details cfd
join reveleer_chases rc on cfd.patient_id = rc.patient_id and cfd.measure_id = rc.measure_code
WHERE
    chase_id ISNULL ;

SELECT * FROM reveleer_chase_file_details WHERE patient_id = 245003 order by id desc;

select * from raw.milliman_pro_20240802;
select * from raw.milliman_ret_20240802;

SELECT *
FROM
        analytics.junk.chases_sent_not_in_reveleer_20240807 j
join junk.reveleer_life_of_chase_20240726 j2 on j2.clientchasekey = j.reveleer_chase_id
;
SELECT
--     j.reveleer_chase_id
--   , COUNT(*)
--   , COUNT(DISTINCT rcfd.id)
rcfd.*
FROM
    analytics.junk.chases_sent_not_in_reveleer_20240807 j
    JOIN reveleer_chase_file_details rcfd ON j.reveleer_chase_id = rcfd.reveleer_chase_id
;
-- GROUP BY
--     1
-- HAVING
--     COUNT(DISTINCT rcfd.id) < COUNT(*);

-- ORDER BY
--     j.reveleer_chase_id, rcfd.id
;
SELECT
    reveleer_chase_id
  , COUNT(*)
FROM
    analytics.junk.chases_sent_not_in_reveleer_20240807 j
GROUP BY
    1
HAVING
    COUNT(*) > 1;

SELECT count(reveleer_chase_id), count(distinct reveleer_chase_id) FROM analytics.junk.chases_sent_not_in_reveleer_20240807 j



------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM reveleer_chases WHERE id in ( 24036850, 23385169 );
SELECT *
FROM
    reveleer_chases rc
join reveleer_compliance_file_details rcfd ON rc.id = rcfd.reveleer_chase_id
WHERE
    external_chase_id IN ('24036850', '23385169');

call sp_reveleer_data_stager();

SELECT * FROM reveleer_chases;
SELECT * FROM reveleer_files;

-- data in files
SELECT * FROM reveleer_chase_file_details;
SELECT * FROM reveleer_attribute_file_details;
SELECT * FROM reveleer_compliance_file_details;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    reveleer_attribute_file_details
WHERE
     sample_id = '144377'
  OR sample_id = '139139'
ORDER BY
    patient_id, inserted_at;

select * from fdw_member_doc.patient_blood_pressures;
SELECT *
FROM
    reveleer_attribute_file_details WHERE attribute_code = 'OsteoporosisFractureDate' order by attribute_value desc;
SELECT
    must_close_by_date
  , msh_measure_open_date
  , must_close_by_date - 180 fracture_date
  , measure_source_key
  , measure_key
  , operational_year
FROM
    fdw_member_doc.qm_patient_measures
WHERE
--       id = 1260887
    measure_key = 'omw_osteoporosis_management'
  AND must_close_by_date - 180 >= '2024-07-01'
;

SELECT count(*)
FROM
    reveleer_cca_pdfs WHERE inserted_at::date = now()::date;

SELECT '2024-06-30'::date;

SELECT *
FROM
    fdw_member_doc.patient_eye_exams  where patient_id in ( 843842,585633 );

SELECT * FROM reveleer_chases where id = 1912015 or external_chase_id = '';
select * from fdw_member_doc.qm_pm_toc_engagement_after_discharge_wfs peid where patient_measure_id in (1104606,1262164);

select 'mrp' measure, patient_measure_id, admit_date, discharge_date from fdw_member_doc.qm_pm_toc_med_rec_wfs mrp where patient_measure_id in (1104606,1262164) union
select 'rdi' measure, patient_measure_id, admit_date, discharge_date from fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs where patient_measure_id in (1104606,1262164);

2690|684438|1893229|TRC|Discharge From Hospital|DischargeFromHospitalDate|07/11/2024|1|ADMIN
32691|684438|1893229|TRC|Inpatient Admission|InpatientAdmissionDate|07/10/2024|1|ADMIN

32692|684438|1893229|TRC|Inpatient Admission|InpatientAdmissionDate|06/09/2024|2|ADMIN

attribute_group_name,attribute_code,attribute_value
Discharge From Hospital,DischargeFromHospitalDate,07/11/2024
Inpatient Admission,InpatientAdmissionDate,       07/10/2024
Inpatient Admission,InpatientAdmissionDate,       06/09/2024

SELECT * FROM reveleer_chases where id = 1893229 or external_chase_id = '';
SELECT *
FROM
 fdw_member_doc.qm_patient_measures pm where pm.id in (1104606,1262164);

SELECT *
FROM
    reveleer_attribute_file_details where sample_id = '1893229';


select 'peid' measure, admit_date, discharge_date from fdw_member_doc.qm_pm_toc_engagement_after_discharge_wfs peid where patient_measure_id in (1104606,1262164) union
select 'mrp' measure, admit_date, discharge_date from fdw_member_doc.qm_pm_toc_med_rec_wfs mrp where patient_measure_id in (1104606,1262164) union
select 'rdi' measure, admit_date, discharge_date from fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs where patient_measure_id in (1104606,1262164);

35773|925998|1923245|TRC|Discharge From Hospital|DischargeFromHospitalDate|07/14/2024|1|ADMIN
35774|925998|1923245|TRC|Inpatient Admission|InpatientAdmissionDate|07/12/2024|1|ADMIN
35775|925998|1923245|TRC|Inpatient Admission|InpatientAdmissionDate|07/13/2024|2|ADMIN
SELECT * FROM reveleer_chases where id = 1923245 or external_chase_id = '';

select 'peid' measure, admit_date, discharge_date from fdw_member_doc.qm_pm_toc_engagement_after_discharge_wfs peid where patient_measure_id in (1262791,1262794) union
select 'mrp' measure, admit_date, discharge_date from fdw_member_doc.qm_pm_toc_med_rec_wfs mrp where patient_measure_id in (1262791,1262794) union
select 'rdi' measure, admit_date, discharge_date from fdw_member_doc.qm_pm_toc_receipt_of_discharge_information_wfs where patient_measure_id in (1262791,1262794);
