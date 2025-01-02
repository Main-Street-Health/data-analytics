SELECT *
FROM
    qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;
SELECT *
FROM
    qm_pm_med_adh_handoffs where id = 7514677;

SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
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
  , pf.meds_on_hand_date
  , pf.days_supply
, pf.meds_on_hand_days_supply
  , pf.meds_on_hand_date + pf.meds_on_hand_days_supply
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
    pm.id = 345042
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;
-- UPDATE qm_pm_med_adh_wfs
-- SET
--     compliance_check_date = '2024-10-20'::DATE, updated_at = NOW(), updated_by_id = 98
-- WHERE
--     id = 111194;


SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id                                                  wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , pf.id                                                  pf_id
  , pf.drug_description
  , wf.compliance_check_date
  , pf.order_status
  , pf.meds_on_hand_date
  , pf.meds_on_hand_days_supply
  , wf.compliance_check_date when_we_requery_ss
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
WHERE
      pf.order_status = 'has_supply'
  AND wf.is_active
  AND wf.is_closed
  AND pf.is_current
-- and pf.meds_on_hand_date + 20 <= now()::date
ORDER BY
    pf.meds_on_hand_days_supply DESC
;
SELECT *
FROM
    qm_ref_measures WHERE key ~* 'adherence';



SELECT *
FROM
    patients p where id = 900224;

SELECT errors[3]
FROM
    oban_jobs
WHERE
    queue ~* 'med_adh'
and id = 3625227
;

update oban_jobs j
set state = 'available', max_attempts = j.max_attempts + 1, scheduled_at = now(), discarded_at = null
where j.id = 3625227

SELECT * FROM qm_pm_med_adh_metrics where patient_measure_id = 553035;

sel

has patient id 365872
SELECT * FROM qm_patient_measures where id = 553035;
SELECT * from patients where id in (365872, 900224 );

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------

create table junk.med_adh_has_supply_manual_update_20240812 as
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id                                                  wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , pf.id                                                  pf_id
  , pf.drug_description
  , wf.compliance_check_date
  , pf.order_status
  , pf.meds_on_hand_date
  , pf.meds_on_hand_days_supply
  , wf.compliance_check_date when_we_requery_ss
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
WHERE
      pf.order_status = 'has_supply'
  AND wf.is_active
  AND wf.is_closed
  AND pf.is_current
and pm.is_active
and pm.measure_status_key != 'one_fill_only_inactive'
  and pm.measure_status_key != 'lost_for_year'
and pf.meds_on_hand_date + 20 <= now()::date
ORDER BY
    pf.meds_on_hand_days_supply DESC
;
begin;
update qm_pm_med_adh_wfs w
set compliance_check_date = '2024-08-13'::date, updated_by_id = 98, updated_at = now()
from junk.med_adh_has_supply_manual_update_20240812 j
where j.wf_id = w.id;
end;


SELECT * FROM qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;
SELECT * FROM qm_pm_med_adh_handoffs order by id desc
SELECT pm.measure_status_key, *
FROM
    qm_pm_med_adh_metrics m
join qm_patient_measures pm on m.patient_measure_id = pm.id
WHERE
    m.patient_id = 47592;
------------------------------------------------------------------------------------------------------------------------
/* from cody */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    qm_pm_med_adh_metrics m

;
DROP TABLE IF EXISTS _mco_comp_failing;
CREATE TEMP TABLE _mco_comp_failing AS
SELECT
    patient_id
  , CASE WHEN "Measure Name" ~* 'chol'  THEN 'med_adherence_cholesterol'
         WHEN "Measure Name" ~* 'hyper' THEN 'med_adherence_hypertension'
         WHEN "Measure Name" ~* 'diab'  THEN 'med_adherence_diabetes' END measure_key
FROM
    member_doc.junk.compliance_detail_lost_20240812 j


;
SELECT
    pm.patient_id
  , pm.measure_key
  , pm.measure_status_key
  , m.measure_source_key
  , m.adr
  , m.pdc_to_date
  , m.next_fill_date
  , mco.adr
  , mco.pdc
FROM
    _mco_comp_failing f
    JOIN qm_pm_med_adh_metrics m ON m.patient_id = f.patient_id AND m.measure_key = f.measure_key
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
    JOIN stage.qm_pm_med_adh_mco_measures mco
         ON mco.patient_id = m.patient_id AND m.measure_key = mco.measure_key AND m.next_fill_date = mco.next_fill_date
WHERE
      TRUE
  AND m.patient_id <> 97407
  AND m.measure_source_key = 'mco'
;

SELECT
    pm.patient_id
  , pm.measure_key
  , pm.measure_status_key
  , m.measure_source_key
  , m.adr
  , m.pdc_to_date
  , m.next_fill_date
  , mco.adr
  , mco.pdc
, mco.next_fill_date
FROM
    _mco_comp_failing f
    JOIN qm_pm_med_adh_metrics m ON m.patient_id = f.patient_id AND m.measure_key = f.measure_key
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
    JOIN stage.qm_pm_med_adh_mco_measures mco
         ON mco.patient_id = m.patient_id AND m.measure_key = mco.measure_key
WHERE
      TRUE
  AND m.patient_id <> 97407
  AND m.measure_source_key = 'sure_scripts'
AND m.next_fill_date >= mco.next_fill_date
;

SELECT *
FROM
    qm_pm_med_adh_synth_periods WHERE
patient_id = 1455398 and measure_key = 'med_adherence_cholesterol'
;

-- ID122754

SELECT *
FROM
    250908;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
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
    pm.patient_id = 250908
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM member_doc.stage.qm_pm_med_adh_mco_measures WHERE patient_id = 479022 order by inserted_at desc;
SELECT qpm.measure_status_key, m.*
FROM
    qm_pm_med_adh_metrics m
join qm_patient_measures qpm ON m.patient_measure_id = qpm.id
WHERE
    m.patient_id = 479022
ORDER BY
    inserted_at DESC;

