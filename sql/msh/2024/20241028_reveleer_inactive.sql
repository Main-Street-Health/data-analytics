SELECT * FROM junk.reveleer_inactives_20241025;
create unique INDEX on junk.reveleer_inactives_20241025(msh_chase_id);

DROP TABLE IF EXISTS _active_reco;
CREATE TEMP TABLE _active_reco AS
SELECT
    rc.id                      msh_chase_id
  , rc.external_chase_id       rev_chase_id
  , rp.name
  , rp.reveleer_id
  , rc.patient_id
  , qpm.measure_key
  , qpm.is_active
  , i.msh_chase_id IS NOT NULL on_prev_inactive_file
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
    JOIN fdw_member_doc.qm_patient_measures qpm ON qpm.id = ANY (rc.qm_patient_measure_ids)
    LEFT JOIN junk.reveleer_inactives_20241025 i ON rc.id = i.msh_chase_id
WHERE
      rc.yr = 2024
  AND qpm.operational_year = 2024




;
-- reveleer_inactive_reconciliation_20241028
SELECT *
FROM
    _active_reco;

------------------------------------------------------------------------------------------------------------------------
/* DOCS */
------------------------------------------------------------------------------------------------------------------------

SELECT
    rcp.id IS NOT NULL we_sent
  , COUNT(*)
FROM
    junk.documents_sent_to_reveleer_2024128 j
    LEFT JOIN reveleer_cca_pdfs rcp ON j.file_name = rcp.file_name
GROUP BY
    1
;

-- we_never_sent_20241028
SELECT
    j.*
FROM
    junk.documents_sent_to_reveleer_2024128 j
    LEFT JOIN reveleer_cca_pdfs rcp ON j.file_name = rcp.file_name
where rcp.id ISNULL
;
-- we_sent_20241029
SELECT
    rcp.uploaded_at >= '2024-09-12' is_new_naming_convention
    , j.*
FROM
    junk.documents_sent_to_reveleer_2024128 j
    JOIN reveleer_cca_pdfs rcp ON j.file_name = rcp.file_name
;
SELECT
    rcp.uploaded_at >= '2024-09-12' is_new_naming_convention
, rcp.file_name
, uploaded_at
-- ,     count(*)
FROM
    junk.documents_sent_to_reveleer_2024128 j
    JOIN reveleer_cca_pdfs rcp ON j.file_name = rcp.file_name
where uploaded_at >= '2024-09-01'
order by uploaded_at
-- JOIN reveleer_chases rc on rcp.reveleer_chase_id = rc.id
-- GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------
/* chase reconciliation
   internal chase id
   rev chase id
   gap type
   gap status
   health plan
   coop id
   patient first name
   patient last name
   patient dob
   */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _sent_chases;
CREATE TEMP TABLE _sent_chases AS
SELECT
    rc.id                             msh_chase_id
  , rc.external_chase_id
     , rc.measure_code
  , UNNEST(rc.qm_patient_measure_ids) pqm_id
  , rp.name                           reveleer_project_name
  , rp.payer_id
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
WHERE
      rc.yr = 2024
  AND EXISTS( SELECT
                  1
              FROM
                  reveleer_chase_file_details cfd
              WHERE
                    rc.id = cfd.reveleer_chase_id
                AND cfd.yr = 2024
                AND cfd.reveleer_file_id IS NOT NULL );
create index on _sent_chases(pqm_id);

-- all_sent_chases_20241029
SELECT
    sc.msh_chase_id
  , sc.external_chase_id
  , sc.measure_code
  , sc.pqm_id
  , sc.reveleer_project_name
  , sc.payer_id
  , pm.id        coop_pqm_id
  , pm.is_active coop_pqm_is_active
  , pm.measure_key
  , pm.patient_id
  , pm.measure_status_key
  , p.first_name
  , p.last_name
  , p.dob
FROM
    _sent_chases sc
    JOIN fdw_member_doc.qm_patient_measures pm ON sc.pqm_id = pm.id
    JOIN fdw_member_doc.patients p ON pm.patient_id = p.id
;
------------------------------------------------------------------------------------------------------------------------
/* confirm closed list */
------------------------------------------------------------------------------------------------------------------------
-- reveleer_confirm_on_plan_close_list_most_recent_2241107
SELECT
    j.*
  , pm.id pqm_id
  , pm.measure_status_key
FROM
    junk.reveleer_confirm_on_plan_close_list_most_recent_2241107 j
    JOIN reveleer_chases rc ON rc.id = j.msh_chase_id
    JOIN fdw_member_doc.qm_patient_measures pm ON pm.id = ANY (rc.qm_patient_measure_ids)
;

SELECT * FROM fdw_member_doc.sure_scripts_pharmacies;
SELECT * FROM raw.lab_corp_response;