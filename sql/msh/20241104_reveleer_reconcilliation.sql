SELECT *
FROM
    reveleer_chases where external_chase_id in (
  '22911291'
, '22911290'
, '22911289'
, '22911288'
, '22911287'
, '22911286'
, '22911285'
, '22911284'
, '22911283'
, '22911282'
    );
SELECT rc.*, cfd.*
FROM
    reveleer_chases rc
left join reveleer_chase_file_details cfd on rc.id = cfd.reveleer_chase_id and cfd.reveleer_file_id is not null
WHERE
    rc.id IN (
              311216, 366654, 293824, 304913, 667930, 307673, 415869, 309760, 311214, 496449
        )
and rc.patient_id = 147660
-- and cfd.id ISNULL
;

SELECT *
FROM
    fdw_member_doc.qm_patient_measures
WHERE
    id = 3420;
SELECT count(*)
FROM
    analytics.junk.reveleer_maybe_missing_20241104 j
join reveleer_chases rc on rc.external_chase_id = j.rev_chase_id::text
;
--     id = 2980;


SELECT rc.inserted_at, rp.name
FROM
    reveleer_chases rc
join reveleer_projects rp on rc.reveleer_project_id = rp.id
-- join reveleer_chase_file_details cfd on rc.id = cfd.reveleer_chase_id
WHERE
    rc.id in (1818306, 304913)
--     rc.external_chase_id = '24338619';


SELECT *
FROM
    ;