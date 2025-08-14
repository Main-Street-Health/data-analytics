-- Immediate recommendation for tonight’s deletion job: Delete all tasks and measure for patient-measures that…
-- Have one-fill only in that measure this year AND patient attribution status = ma_at_risk_no, ma_at_risk_pending, om_at_risk_pending, or partner_pfp_pending
-- OR
-- Have one-fill only in that measure this year AND patient attribution status = ma_at_risk_yes AND ADR < 20

DROP TABLE IF EXISTS _to_cancel;
CREATE TEMP TABLE _to_cancel AS
SELECT DISTINCT
    patient_task_id
  , m.patient_measure_id
  , pf.qm_pm_med_adh_wf_id
FROM
    qm_pm_med_adh_potential_fills pf
    JOIN qm_pm_med_adh_metrics m ON m.patient_measure_id = pf.patient_measure_id
    JOIN supreme_pizza sp ON sp.patient_id = m.patient_id
    JOIN patient_tasks pt ON pt.id = pf.patient_task_id AND pt.status IN ('new', 'in_progress')
WHERE
      pf.inserted_at > NOW() - '3 days'::INTERVAL
  AND m.fill_count = 1
  AND (
          sp.attribution_status IN ('ma_at_risk_no', 'ma_at_risk_pending', 'om_at_risk_pending', 'partner_pfp_pending')
              OR (
              sp.attribution_status = 'ma_at_risk_no' AND m.adr < 20
              )
          )
;
BEGIN;

WITH
    upd_tasks AS (
        UPDATE patient_tasks pt
            SET status = 'cancelled', updated_at = NOW()
            FROM _to_cancel tc
            WHERE tc.patient_task_id = pt.id
            RETURNING * )
INSERT
INTO
    patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at, updated_at)
SELECT
    ut.id
  , 98
  , 'cancelled'
  , 'AutoClosed - Cancelled - Too Many Tasks'
  , NULL
  , NOW()
  , NOW()
FROM
    upd_tasks ut;

UPDATE qm_pm_med_adh_wfs wf
SET
    is_active = FALSE, is_closed = TRUE, updated_at = NOW(), updated_by_id = 98
FROM
    _to_cancel tc
WHERE
      wf.id = tc.qm_pm_med_adh_wf_id
  AND wf.is_active
;



end;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS _to_cancel;
CREATE TEMP TABLE _to_cancel AS
SELECT DISTINCT
    patient_task_id
  , m.patient_measure_id
  , pf.qm_pm_med_adh_wf_id
FROM
    qm_pm_med_adh_potential_fills pf
    JOIN qm_pm_med_adh_metrics m ON m.patient_measure_id = pf.patient_measure_id
    JOIN supreme_pizza sp ON sp.patient_id = m.patient_id
    JOIN patient_tasks pt ON pt.id = pf.patient_task_id AND pt.status IN ('new', 'in_progress')
WHERE
      pf.inserted_at > NOW() - '3 days'::INTERVAL
  AND m.fill_count = 1
  AND (
          sp.attribution_status IN ('ma_at_risk_no', 'ma_at_risk_pending', 'om_at_risk_pending', 'partner_pfp_pending')
              OR (
              sp.attribution_status = 'ma_at_risk_yes' AND m.adr < 20
              )
          )
;
BEGIN;

WITH
    upd_tasks AS (
        UPDATE patient_tasks pt
            SET status = 'cancelled', updated_at = NOW()
            FROM _to_cancel tc
            WHERE tc.patient_task_id = pt.id
            RETURNING * )
INSERT
INTO
    patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at, updated_at)
SELECT
    ut.id
  , 98
  , 'cancelled'
  , 'AutoClosed - Cancelled - Too Many Tasks'
  , NULL
  , NOW()
  , NOW()
FROM
    upd_tasks ut;

UPDATE qm_pm_med_adh_wfs wf
SET
    is_active = FALSE, is_closed = TRUE, updated_at = NOW(), updated_by_id = 98
FROM
    _to_cancel tc
WHERE
      wf.id = tc.qm_pm_med_adh_wf_id
  AND wf.is_active
;



end;

SELECT * FROM patient_medication_fills where patient_id = 224975 and drug_description ~* 'ENALAPRIL' order by last_filled_date desc; 50228023010
SELECT * FROM patient_medication_fills where patient_id = 1256337 and drug_description ~* 'Prava' order by last_filled_date desc; 84386003399
