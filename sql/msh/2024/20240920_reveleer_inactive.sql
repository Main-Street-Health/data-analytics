
DROP TABLE IF EXISTS _active_reco;
CREATE TEMP TABLE _active_reco AS
SELECT
    rc.id
  , rc.patient_id
  , qpm.measure_key
  , qpm.is_active
  , i.chase_id IS NOT NULL on_prev_inactive_file
FROM
    reveleer_chases rc
    JOIN fdw_member_doc.qm_patient_measures qpm ON qpm.id = ANY (rc.qm_patient_measure_ids)
    LEFT JOIN analytics.junk."20240918_inactivated_gaps_reveleer" i ON rc.id = i.chase_id
where rc.yr = 2024
and qpm.operational_year = 2024


;
SELECT 40000.0/ count(*)
FROM
    reveleer_chases ;
;
SELECT *
FROM
    _active_reco
where not is_active and not on_prev_inactive_file;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
    ;
SELECT
    rc.id                  chase_id
  , rc.external_chase_id
  , rc.patient_id
  , qpm.measure_key
  , qpm.is_active
  , i.chase_id IS NOT NULL on_prev_inactive_file
FROM
    analytics.junk."20240918_inactivated_gaps_reveleer" i
    JOIN reveleer_chases rc ON rc.id = i.chase_id
    JOIN fdw_member_doc.qm_patient_measures qpm ON qpm.id = ANY (rc.qm_patient_measure_ids)
WHERE
      rc.yr = 2024
  AND qpm.operational_year = 2024
  AND qpm.is_active;

SELECT
    rc.id
   , rc.external_chase_id
  , rc.patient_id
  , qpm.measure_key
  , qpm.is_active
  , i.chase_id IS NOT NULL on_prev_inactive_file
FROM
    reveleer_chases rc
    JOIN fdw_member_doc.qm_patient_measures qpm ON qpm.id = ANY (rc.qm_patient_measure_ids)
    LEFT JOIN analytics.junk."20240918_inactivated_gaps_reveleer" i ON rc.id = i.chase_id
where rc.yr = 2024
  and qpm.operational_year = 2024
and i.chase_id ISNULL

SELECT *
FROM
    ;

;
SELECT 40000.0/ count(*)
FROM
    reveleer_chases ;
;
SELECT count(*)
FROM
    _active_reco
where not is_active and not on_prev_inactive_file;
SELECT
    rc.id                  chase_id
  , rc.external_chase_id
  , rc.patient_id
  , qpm.measure_key
  , qpm.is_active
  , i.chase_id IS NOT NULL on_prev_inactive_file
FROM
    analytics.junk."20240918_inactivated_gaps_reveleer" i
    JOIN reveleer_chases rc ON rc.id = i.chase_id
    JOIN fdw_member_doc.qm_patient_measures qpm ON qpm.id = ANY (rc.qm_patient_measure_ids)
WHERE
      rc.yr = 2024
  AND qpm.operational_year = 2024
  AND qpm.is_active;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS _inactive_chases;
CREATE TEMP TABLE _inactive_chases AS
SELECT
    rc.id
  , rc.external_chase_id
  , rp.name
  , rp.reveleer_id         rev_proj_id
  , rc.patient_id
  , qpm.measure_key
  , qpm.is_active
  , i.chase_id IS NOT NULL on_prev_inactive_file
FROM
    reveleer_chases rc
    JOIN fdw_member_doc.qm_patient_measures qpm ON qpm.id = ANY (rc.qm_patient_measure_ids)
    JOIN reveleer_projects rp ON rp.id = rc.reveleer_project_id
    LEFT JOIN analytics.junk."20240918_inactivated_gaps_reveleer" i ON rc.id = i.chase_id
WHERE
      rc.yr = 2024
  AND qpm.operational_year = 2024
  AND i.chase_id ISNULL
  AND NOT qpm.is_active
  AND EXISTS( SELECT 1 FROM reveleer_chase_file_details cfd WHERE cfd.chase_id = rc.id AND cfd.yr = 2024 )
;


-- inactive_chases_not_already_inactived_20240920
SELECT
    id chase_id, external_chase_id, name, rev_proj_id, patient_id, measure_key, is_active, on_prev_inactive_file
FROM
    _inactive_chases ;