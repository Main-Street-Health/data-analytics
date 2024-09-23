------------------------------------------------------------------------------------------------------------------------
/* from banu mm */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 159043
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
------------------------------------------------------------------------------------------------------------------------
/* for cody mm */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 80071
    -- pt.id = 
    and pm.measure_key = 'med_adherence_hypertension'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods where patient_id = 80071 AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#';

------------------------------------------------------------------------------------------------------------------------
/* cody # 2 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 1388877
    -- pt.id =
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 1388877
  AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
    AND start_date >= '2024-01-01'
order by start_date
;

SELECT *
FROM
    file_router.;
SELECT count(*) FROM member_doc.oban.oban_jobs WHERE id > 174206640 and queue = 'inbound_files_processing' and state != 'completed' ;
SELECT * FROM member_doc.oban.oban_jobs WHERE id > 174206640 and queue = 'inbound_files_processing' and state != 'completed' order by id;
SELECT * FROM member_doc.oban.oban_jobs WHERE id = 174206640; and queue = 'inbound_files_processing' order by id;
update member_doc.oban.oban_jobs set state = 'cancelled', max_attempts = 1, cancelled_at = now() WHERE id = 174206640;
SELECT *
FROM
    oban.oban_jobs
WHERE
      worker ~* 'inbound'
  AND queue = 'inbound_files_processing'
  AND args ->> 'hash' ~* 'Main Street Health TN'
  AND state != 'cancelled'
  AND state != 'completed'
ORDER BY
    id DESC;

UPDATE member_doc.oban.oban_jobs
SET
    state = 'cancelled', max_attempts = 1, cancelled_at = NOW()
WHERE
    id IN (174206691, 174206690);
SELECT *
FROM
    oban.oban_jobs where attempt > oban_jobs.max_attempts;

select 3906 * 1.0 / 64699;
SELECT count(distinct qpm.id)
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures qpm ON m.patient_measure_id = qpm.id
    JOIN supreme_pizza sp ON m.patient_id = sp.patient_id AND sp.patient_payer_id = 44
WHERE
      qpm.is_active
  AND NOT m.is_excluded
and not exists(select 1 from qm_pm_med_adh_synth_periods s where s.patient_measure_id = qpm.id)
;

SELECT *
FROM
    payers;
------------------------------------------------------------------------------------------------------------------------
/* humana fire */
------------------------------------------------------------------------------------------------------------------------
SELECT count(*)
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures qpm ON m.patient_measure_id = qpm.id
    JOIN supreme_pizza sp ON m.patient_id = sp.patient_id AND sp.patient_payer_id = 44
WHERE
      qpm.is_active
  AND NOT m.is_excluded
and m.measure_source_key = 'mco'
;

------------------------------------------------------------------------------------------------------------------------
/* cody evening */
------------------------------------------------------------------------------------------------------------------------
SELECT last_fill_date, days_supply, next_fill_date, inserted_at
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures where patient_id = 115;

------------------------------------------------------------------------------------------------------------------------
/* check new exclusion */
------------------------------------------------------------------------------------------------------------------------
SELECT m.measure_key, count(*)
FROM
    qm_pm_med_adh_metrics m
where is_excluded
GROUP BY measure_key

;
------------------------------------------------------------------------------------------------------------------------
/* check reveleer job */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    member_doc.oban.oban_jobs where queue = 'reveleer' order by id desc;

------------------------------------------------------------------------------------------------------------------------
/* cody */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 595157
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    patient_medication_fills where patient_id = 595157 and drug_description ~* 'atorva' order by start_date --AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
                                                                                                            ;
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 31864
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;

SELECT *
FROM
    qm_pm_med_adh_metrics;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------

SELECT * FROM stage.qm_pm_med_adh_mco_measures WHERE patient_id = 38505 and measure_key = 'med_adherence_diabetes';
SELECT * FROM qm_pm_med_adh_synth_periods WHERE patient_id = 38505 and measure_key = 'med_adherence_diabetes';
SELECT *
FROM
    qm_pm_med_adh_metrics where patient_id = 38505 and measure_key = 'med_adherence_diabetes';;

------------------------------------------------------------------------------------------------------------------------
/* from eric */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.measure_status_key
  , pt.status
  , f.order_status
-- , wf.*
  , f.*
FROM
    qm_pm_med_adh_potential_fills f
    JOIN patient_tasks pt ON pt.id = f.patient_task_id
    JOIN qm_patient_measures m ON m.id = f.patient_measure_id
join qm_pm_med_adh_wfs wf on f.qm_pm_med_adh_wf_id = wf.id
WHERE
      f.is_current
  AND pt.status NOT IN ('new', 'in_progress')
  AND m.measure_status_key = 'past_due_pending_navigator'
  AND m.is_active
  AND f.order_status IS NULL

;
SELECT *
FROM
    qm_pm_med_adh_potential_fills f
WHERE
      f.is_current
  AND EXISTS( SELECT 1
              FROM qm_pm_med_adh_potential_fills pf
              WHERE pf.qm_pm_med_adh_wf_id = f.qm_pm_med_adh_wf_id AND pf.id > f.id  and not pf.is_current);

SELECT *
FROM
    qm_pm_med_adh_reverts;

select aa.*, qpmam.*
from qm_pm_med_adh_potential_fills f
         join patient_tasks pt on pt.id = f.patient_task_id
         join qm_patient_measures m on m.id = f.patient_measure_id
         join patient_task_activities aa on aa.patient_task_id = pt.id
join qm_pm_med_adh_metrics qpmam ON m.id = qpmam.patient_measure_id
where f.is_current
  and pt.status not in ('new', 'in_progress')
  and m.measure_status_key = 'past_due_pending_navigator'
  and m.is_active
  and f.order_status is null
  and aa.action <> 'update_status'
order by aa.inserted_at
;