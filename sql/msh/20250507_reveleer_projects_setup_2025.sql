SELECT *
FROM
    fdw_member_doc.qm_cfg_measure_payers where payer_id = 38 and send_to_reveleer;
SELECT *
FROM
    reveleer_projects
where yr = 2024
;
INSERT
INTO
    reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES
    ('bcbs_tn', 38, NULL, 3036, 2025, TRUE);


SELECT
    cfd.reveleer_project_id, rc.patient_id, rc.measure_code, rc.qm_patient_measure_id, rc.id msh_chase_id, rc.external_chase_id rev_chase_id
FROM
    reveleer_chase_file_details cfd
join reveleer_projects rp ON cfd.reveleer_project_id = rp.id --and rp.yr = 2025
join reveleer_chases rc on cfd.reveleer_chase_id = rc.id
WHERE
--     cfd.yr = 2025
-- and
-- cfd.reveleer_file_id ISNULL
cfd.reveleer_file_id = 7525
order by patient_id, measure_code
-- GROUP BY
--     1;

SELECT *
FROM
    reveleer_files order by id desc;
-- DELETE FROM reveleer_chase_file_details WHERE reveleer_file_id ISNULL ;
-- DELETE FROM reveleer_attribute_file_details WHERE reveleer_file_id ISNULL ;
-- DELETE FROM reveleer_compliance_file_details WHERE reveleer_file_id ISNULL ;

SELECT *
FROM
     raw.reveleer_chase_successes
order by id desc

------------------------------------------------------------------------------------------------------------------------
/* Clean ou */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _to_del;
CREATE TEMP TABLE _to_del AS
SELECT *
FROM
    reveleer_chases rc
where rc.external_chase_id IS NULL
and rc.yr = 2025
and rc.reveleer_project_id = 430
-- and exists(select 1 from reveleer_chase_file_details cfd where cfd.reveleer_chase_id = rc.id and cfd.reveleer_file_id is not null)
-- and rc.measure_code = 'OMW-P'
;
SELECT
    COUNT(*)
FROM
    _to_del;


DELETE
 FROM reveleer_chase_file_details WHERE reveleer_chase_id in (select id from _to_del) ;
DELETE
 FROM reveleer_attribute_file_details WHERE reveleer_chase_id in (select id from _to_del) ;
DELETE
FROM
    reveleer_chases
WHERE id in (select id from _to_del);

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    reveleer_chases where external_chase_id ISNULL and reveleer_project_id = 430;

------------------------------------------------------------------------------------------------------------------------
/*  */

------------------------------------------------------------------------------------------------------------------------
-- patient_id IN (1686231, 1686308, 1686483)
call sp_reveleer_stage_new_cca_pdfs_for_upload();
SELECT *
FROM
    reveleer_cca_pdfs WHERE yr = 2025;

SELECT
    rc.measure_code
  , COUNT(*)
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
WHERE
      rp.yr = 2025
  AND EXISTS( SELECT 1
              FROM reveleer_chase_file_details cfd
              WHERE cfd.chase_id = rc.id AND cfd.reveleer_file_id IS NOT NULL )
GROUP BY
    1
;

------------------------------------------------------------------------------------------------------------------------
/* early 2025 cleanup */
------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
WHERE
      rc.yr = 2025
  AND rp.yr = 2024;

UPDATE reveleer_chases rc
SET
    yr = 2024
FROM
    reveleer_projects rp
WHERE
      rp.id = rc.reveleer_project_id
  AND rc.yr = 2025
  AND rp.yr = 2024
and not exists(
    select 1 from reveleer_chases rc2
             where rc2.patient_id = rc.patient_id and rc2.measure_code = rc.measure_code
             and rc2.yr = 2024
)
;


;
begin;

delete
FROM
    reveleer_chases rc
    using reveleer_projects rp
WHERE
      rc.yr = 2025
  AND rp.yr = 2024
  and rc.reveleer_project_id = rp.id
and not exists(select 1 from reveleer_chase_file_details cfd where cfd.chase_id = rc.id and cfd.reveleer_file_id is not null)
;
end;



SELECT
    COUNT(*)
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
WHERE
      rc.yr = 2025
  AND rp.yr = 2024
  AND EXISTS( SELECT
                  1
              FROM
                  reveleer_chases rc2
                  JOIN reveleer_chase_file_details rcfd ON rc2.id = rcfd.reveleer_chase_id
              WHERE
                    rc2.patient_id = rc.patient_id
                AND rc2.measure_code = rc.measure_code
                AND rc2.yr = 2024
                AND rcfd.reveleer_file_id IS NOT NULL )
;

------------------------------------------------------------------------------------------------------------------------
/* archive and delete */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS junk.reveleer_2024_labeleed_2025;
CREATE TABLE junk.reveleer_2024_labeleed_2025 AS
SELECT rc.*
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
WHERE
      rc.yr = 2025
  AND rp.yr = 2024;

create table junk.reveleer_2024_labeleed_2025_cfd as
SELECT *
FROM
    reveleer_chase_file_details
WHERE
    chase_id IN ( SELECT id FROM junk.reveleer_2024_labeleed_2025 );

create table junk.reveleer_2024_labeleed_2025_attr as
SELECT *
FROM
    reveleer_attribute_file_details
WHERE
    reveleer_chase_id IN ( SELECT id FROM junk.reveleer_2024_labeleed_2025 );

create table junk.reveleer_2024_labeleed_2025_comp as
SELECT *
FROM
    reveleer_compliance_file_details
WHERE
    reveleer_chase_id IN ( SELECT id FROM junk.reveleer_2024_labeleed_2025 );

begin;
DELETE FROM reveleer_attribute_file_details
WHERE id in (select id from junk.reveleer_2024_labeleed_2025_attr);
DELETE FROM reveleer_compliance_file_details
WHERE id in (select id from junk.reveleer_2024_labeleed_2025_comp);
DELETE FROM reveleer_chase_file_details
WHERE id in (select id from junk.reveleer_2024_labeleed_2025_cfd);
DELETE FROM reveleer_chases
WHERE id in (select id from junk.reveleer_2024_labeleed_2025);
end;

SELECT *
FROM
    reveleer_chases where yr = 2025 and qm_patient_measure_ids != '{}';

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------

'aetna', 3070
'centene', 3069
'cigna', 3071
'elevance', 3072
'humana', 3073
'uHC', 3074
'viva', 3075
'wellcare', 3067
'wellmark', 3076
'wellmed', 3077

SELECT *
FROM
    fdw_member_doc.payers p
where p.name ~* any(array[ 'aetna','centene','cigna','elevance','humana','uHC','viva','wellcare','wellmark','wellmed', 'united'])
order by name
;

-- first [done]
INSERT
INTO
    reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES
    ('bcbs_tn', 38, NULL, 3036, 2025, TRUE);


-- next
-- Wellcare
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('wellcare', 49, NULL, 3067, 2025, TRUE);

-- Centene
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('centene', 249, NULL, 3069, 2025, TRUE);

-- rest

-- Aetna
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('aetna',   1, NULL, 3070, 2025, TRUE);



-- Cigna
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('cigna',   40, NULL, 3071, 2025, TRUE);

-- Elevance
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('elevance',2, NULL, 3072, 2025, TRUE);

-- Humana
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('humana',  44, NULL, 3073, 2025, TRUE);

-- UHC
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('uhc',     47, NULL, 3074, 2025, TRUE);

-- Viva
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('viva',    48, NULL, 3075, 2025, TRUE);

-- Wellmark
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('wellmark',50, NULL, 3076, 2025, TRUE);

-- Wellmed
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('wellmed', 147, NULL, 3077, 2025, TRUE);

-- Optum
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('Optum', 308, NULL, 3200, 2025, TRUE);

-- HCSC (BCBS IL, OK, NM, MT, & TX)
INSERT INTO reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES ('HCSC (BCBS IL, OK, NM, MT, & TX)', 42, NULL, 3236, 2025, TRUE);

