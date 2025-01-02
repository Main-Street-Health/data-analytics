SELECT *
FROM
    analytics.public.reveleer_chase_file_details cfd
WHERE
    reveleer_chase_id IN ( 22915482, 22916530, 22910358, 22910910, 22914126 )
;

SELECT *
FROM
    fdw_member_doc.qm_ref_patient_measure_statuses;

------------------------------------------------------------------------------------------------------------------------
/* potentially delete these to re run */
------------------------------------------------------------------------------------------------------------------------
select * from reveleer_files where file_name ~* '2135_admin_msh_202406131759'
select * from reveleer_files where file_name ~* '2176_admin_msh_202406131759'
select * from reveleer_files where file_name ~* '2182_admin_msh_202406131759'

SELECT * FROM reveleer_files WHERE id IN (4568, 4567, 4570);

delete FROM reveleer_chase_file_details WHERE reveleer_file_id IN (4568, 4567, 4570);
delete FROM reveleer_attribute_file_details WHERE reveleer_file_id IN (4568, 4567, 4570);
delete FROM reveleer_compliance_file_details WHERE reveleer_file_id IN (4568, 4567, 4570);

SELECT
    reveleer_project_id
  , COUNT(DISTINCT chase_id)
FROM
    reveleer_chase_file_details
WHERE
    reveleer_file_id ISNULL
GROUP BY reveleer_project_id
;
reveleer_project_id,count
236,9203
238,6743
241,2987

reveleer_project_id,count
236,5736
238,5368
241,3269

SELECT
    c.*
FROM
    reveleer_compliance_file_details c
WHERE
      reveleer_file_id ISNULL
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      reveleer_chase_file_details cfd
                  WHERE
                        cfd.chase_id::TEXT = c.sample_id
                    AND cfd.reveleer_file_id IS NULL )

;

SELECT *
FROM
    reveleer_files f
WHERE
    f.file_name ~* ANY
    (array[ '2176_msh_202406131807', '2182_msh_202406131808', '2183_msh_202406131807', '2189_msh_202406131807' ]);
id
(4576,4573,4574,4580)
delete FROM reveleer_chase_file_details WHERE reveleer_file_id IN (4576,4573,4574,4580);
delete FROM reveleer_attribute_file_details WHERE reveleer_file_id IN (4576,4573,4574,4580);
delete FROM reveleer_compliance_file_details WHERE reveleer_file_id IN (4576,4573,4574,4580);
reveleer_project_id

SELECT *
FROM
    reveleer_attribute_file_details
WHERE
    sample_id = '218675';



SELECT *
FROM
    reveleer_files f
WHERE
    f.file_name ~* ANY
    (array[
        '2135_admin_msh_202406131759', '2176_admin_msh_202406131759', '2182_admin_msh_202406131759' ]);
id
( 4568 , 4567 , 4570 )
delete FROM reveleer_chase_file_details WHERE reveleer_file_id IN ( 4568 , 4567 , 4570 );
delete FROM reveleer_attribute_file_details WHERE reveleer_file_id IN ( 4568 , 4567 , 4570 );
delete FROM reveleer_compliance_file_details WHERE reveleer_file_id IN ( 4568 , 4567 , 4570 );
reveleer_project_id

SELECT *
FROM
    reveleer_attribute_file_details
where reveleer_file_id ISNULL
ORDER BY
    measure_id, member_id, attribute_code

;


DELETE
FROM
    reveleer_attribute_file_details a1
WHERE
      reveleer_file_id ISNULL
  AND attribute_code = 'Diastolic'
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      reveleer_attribute_file_details a2
                  WHERE
                        a2.patient_id = a1.patient_id
                    AND a2.attribute_code = 'Systolic'
                    AND a2.reveleer_file_id IS NULL )
;


DELETE FROM reveleer_attribute_file_details WHERE reveleer_file_id ISNULL ;
DELETE FROM reveleer_compliance_file_details WHERE reveleer_file_id ISNULL ;
DELETE FROM reveleer_chase_file_details WHERE reveleer_file_id ISNULL ;

-- reveleer_project_id IN (238, 236, 241)
------------------------------------------------------------------------------------------------------------------------
/* BP cleanup */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _pats;
CREATE TEMP TABLE _pats AS
SELECT distinct  patient_id
FROM
    reveleer_chases c
WHERE
    c.measure_code = 'CBP';

CREATE INDEX ON _pats(patient_id);


DROP TABLE IF EXISTS _pats_w_bp;
CREATE TEMP TABLE _pats_w_bp AS
SELECT
--     DISTINCT ON (pbp.patient_id)
    pbp.*
FROM
    fdw_member_doc.patient_blood_pressures pbp
    JOIN _pats p ON p.patient_id = pbp.patient_id
WHERE
    pbp.encounter_date >= '2024-01-01'::date
  and pbp.systolic BETWEEN 40 AND 300
  AND pbp.diastolic BETWEEN 20 AND 200

ORDER BY
    pbp.patient_id, pbp.encounter_date DESC
;
-- SELECT * FROM _pats_w_bp;


DROP TABLE IF EXISTS _sent_to_reveleer;
CREATE TEMP TABLE _sent_to_reveleer AS
SELECT
    a_date.reveleer_file_id
  , a_date.attribute_value
  , a_date.attribute_value::DATE date
  , a_sys.attribute_value::INT   sys
  , a_dia.attribute_value::INT   dia
  , a_date.reveleer_chase_id
  , a_date.patient_id
FROM
    reveleer_attribute_file_details a_date
    JOIN reveleer_attribute_file_details a_sys ON a_sys.patient_id = a_date.patient_id
        AND a_sys.attribute_group_name = 'Blood Preassure Test'
        AND a_sys.attribute_code = 'Systolic'
        AND a_sys.reveleer_file_id = a_date.reveleer_file_id
        AND a_sys.reveleer_chase_id = a_date.reveleer_chase_id
        AND a_sys.yr = 2024
    JOIN reveleer_attribute_file_details a_dia ON a_dia.patient_id = a_date.patient_id
        and a_dia.attribute_group_name = 'Blood Preassure Test'
        AND a_dia.attribute_code = 'Diastolic'
        AND a_dia.reveleer_file_id = a_date.reveleer_file_id
        AND a_dia.reveleer_chase_id = a_date.reveleer_chase_id
        AND a_dia.yr = 2024
    JOIN reveleer_projects rp ON a_date.reveleer_project_id = rp.id
        AND rp.is_active
        AND rp.yr = 2024
WHERE
      a_date.attribute_code = 'BloodPreassureTestDate'
  AND a_date.yr = 2024
  AND a_date.reveleer_chase_id IS NOT NULL
  and a_date.attribute_group_name = 'Blood Preassure Test'
;

SELECT * FROM _sent_to_reveleer;
SELECT * FROM _pats_w_bp;


create table junk.reveleer_bp_issue_20240620 as
SELECT
    str.reveleer_file_id
  , str.attribute_value
  , str.reveleer_chase_id
  , str.patient_id
  , str.date
  , pbp.encounter_date
  , str.sys
  , pbp.systolic
  , str.dia wrong_diastolic
  , pbp.diastolic correct_diastolic
FROM
    _sent_to_reveleer str
    LEFT JOIN _pats_w_bp pbp ON pbp.patient_id = str.patient_id
        AND pbp.encounter_date = str.date
        AND pbp.systolic = str.sys
--         AND pbp.diastolic = str.dia
WHERE
--     pbp.patient_id ISNULL
        pbp.diastolic != str.dia

ORDER BY
    str.patient_id, str.date
;





select count(*), count(distinct patient_id) from junk.reveleer_bp_issue_20240620;

SELECT * FROM reveleer_chase_file_details WHERE reveleer_file_id ISNULL ;
SELECT * FROM reveleer_attribute_file_details WHERE reveleer_file_id ISNULL ;

SELECT
    patient_id
  , measure_code
  , COUNT(*)
  , ARRAY_AGG(DISTINCT rp.reveleer_id)
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
WHERE
      rc.yr = 2024
  AND reveleer_project_id != 2239
GROUP BY
    1, 2
HAVING
    COUNT(DISTINCT reveleer_project_id) > 1
;


