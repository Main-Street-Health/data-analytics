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


------------------------------------------------------------------------------------------------------------------------
/* missing docs */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM reveleer_cca_pdfs where patient_id = 15115;
SELECT * FROM fdw_member_doc.qm_patient_measures where patient_id = 15115;
SELECT * FROM fdw_member_doc.documents where id = 760802;
sELECT * FROM fdw_member_doc.documents where patient_id = 15115;
SELECT *
FROM
    fdw_member_doc.document_types;
ked_egfr_results, ked_uacr_results

-- call sp_reveleer_stage_new_cca_pdfs_for_upload()
    SELECT * FROM fdw_member_doc.patient_kidney_health_evaluations;
;

-- select count(*) from (
DROP TABLE IF EXISTS _docs;
CREATE TEMP TABLE _docs AS
SELECT
    wf.patient_measure_id
  , egfr.document_id
, pm.measure_status_key
, pm.is_active
FROM
    fdw_member_doc.qm_pm_kidney_health_evaluations_wfs wf
    JOIN fdw_member_doc.patient_kidney_health_evaluations egfr ON egfr.id = wf.egfr_evaluation_id
    JOIN fdw_member_doc.qm_patient_measures pm on pm.id = wf.patient_measure_id
WHERE
    egfr.document_id IS NOT NULL
UNION
SELECT
    wf.patient_measure_id
  , uacr.document_id
  , pm.measure_status_key
  , pm.is_active
FROM
    fdw_member_doc.qm_pm_kidney_health_evaluations_wfs wf
    JOIN fdw_member_doc.patient_kidney_health_evaluations uacr ON uacr.id = wf.uacr_evaluation_id
    JOIN fdw_member_doc.qm_patient_measures pm on pm.id = wf.patient_measure_id
WHERE
    uacr.document_id IS NOT NULL
-- ) x
;
create index on _docs(patient_measure_id);

SELECT
    d.is_active
  , d.measure_status_key = 'closed_system' is_closed_system
  , rp.name
  , COUNT(DISTINCT d.document_id) n_docs
  , COUNT(DISTINCT d.patient_measure_id) n_measures
FROM
    _docs d
    JOIN reveleer_chases rc ON d.patient_measure_id = ANY (rc.qm_patient_measure_ids) AND rc.yr = 2024
join reveleer_projects rp ON rc.reveleer_project_id = rp.id
WHERE
    NOT EXISTS( SELECT 1 FROM reveleer_cca_pdfs p WHERE p.document_id = d.document_id )
GROUP BY
    1, 2, 3;

SELECT d.type_id, count(*)
FROM
    reveleer_cca_pdfs p
join fdw_member_doc.documents d on p.document_id = d.id
where p.yr = 2024

SELECT *
FROM
    fdw_member_doc.patient_blood_pressures;