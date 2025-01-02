-- close all open workflows
DROP TABLE IF EXISTS junk.med_adh_tasks_to_cancel_for_eoy;
CREATE TABLE junk.med_adh_tasks_to_cancel_for_eoy AS
SELECT pt.*, pf.patient_measure_id, pf.qm_pm_med_adh_wf_id
FROM
    qm_pm_med_adh_potential_fills pf
join patient_tasks pt on pt.id = pf.patient_task_id
where pt.status in ('new', 'in_progress')
;
begin;
ROLLBACK;
DROP TABLE IF EXISTS _cancelled;
CREATE TEMP TABLE _cancelled AS
        with upd as (
            UPDATE patient_tasks pt
SET
    status = 'cancelled', updated_at = NOW(), modified_by_id = 98
FROM
    junk.med_adh_tasks_to_cancel_for_eoy tc
WHERE
    tc.id = pt.id
RETURNING tc.id
            )
select * from upd
;
end;
;

SELECT * FROM patient_tasks WHERE status = 'cancelled' order by id desc;
SELECT *
FROM
    patient_task_activities pta
where pta.action = 'cancelled'
order by id desc
;
SELECT *
FROM
    _cancelled;

INSERT
INTO
    public.patient_task_activities (patient_task_id, user_id, action, value, reason, inserted_at, updated_at)
SELECT
    id
  , 98
  , 'cancelled'
  , 'cancelled due to end of year'
  , 'cancelled due to end of year'
  , NOW()
  , NOW()
FROM
    _cancelled;


UPDATE qm_pm_med_adh_wfs wf
SET
    is_active = FALSE, updated_at = NOW()
WHERE
    wf.is_active = TRUE;


