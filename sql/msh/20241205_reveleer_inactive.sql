SELECT * FROM junk.reveler_inactives_20241205;
SELECT count(*) FROM junk.reveler_inactives_20241205;
create unique INDEX on junk.reveler_inactives_20241205(client_chase_key);

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
  , i.client_chase_key IS NOT NULL on_prev_inactive_file
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
    JOIN fdw_member_doc.qm_patient_measures qpm ON qpm.id = ANY (rc.qm_patient_measure_ids)
    LEFT JOIN junk.reveler_inactives_20241205 i ON rc.id = i.client_chase_key
WHERE
      rc.yr = 2024
  AND qpm.operational_year = 2024


;
-- reveleer_inactive_reconciliation_20241205
SELECT *
FROM
    _active_reco;
-- call sp_reveleer_data_stager();



