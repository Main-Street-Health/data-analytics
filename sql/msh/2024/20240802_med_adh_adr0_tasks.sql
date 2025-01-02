
SELECT
    patient_id
  , patient_status
  , patient_substatus
  , is_medication_adherence
FROM
    supreme_pizza
WHERE
--     patient_id = 683895
    patient_id = 683826
;

SELECT *
FROM
    supreme_pizza
WHERE
      primary_referring_partner_id ISNULL
  AND is_medication_adherence;

SELECT
    m.patient_measure_id
  , m.patient_id
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
    pm.patient_id = 683826

    -- pt.id =
ORDER BY pm.measure_key, pf.id
;

SELECT * FROM qm_pm_med_adh_handoffs where patient_id = 683826 and qm_pm_med_adh_handoffs.measure_key = 'med_adherence_cholesterol'
SELECT * FROM qm_pm_status_periods where patient_measure_id = 510416 order by id;

SELECT
--     count(distinct pt.id)
    p.status
  , p.substatus
     , p.id, pm.measure_key
     , m.id
  , sp.is_medication_adherence
  , pm.measure_status_key
  , pm.is_active
  , m.adr
  , m.next_fill_date
  , m.is_excluded
  , pf.order_status
  , pt.status
  , pt.inserted_at
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_pm_med_adh_potential_fills pf ON pf.patient_measure_id = m.patient_measure_id
    JOIN patient_tasks pt ON pt.id = pf.patient_task_id AND pt.status IN ('new', 'in_progress')
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
    JOIN patients p ON p.id = pm.patient_id
    JOIN supreme_pizza sp ON p.id = sp.patient_id
WHERE
    m.adr = 0
-- and m.id = 34847972
;



id,measure_key
,
INSERT
INTO
    qm_pm_med_adh_handoffs (measure_key, patient_id, qm_pm_med_adh_metric_id, measure_year, processed_at,
                            measure_source_key, inserted_at, reason, signal_date, fall_off_status_key)
SELECT
    m.measure_key
  , m.patient_id
  , m.id
  , m.measure_year
  , NULL
  , m.measure_source_key
  , NOW()
  , 'adr=0 with open task'
  , NULL
  , NULL
FROM
    qm_pm_med_adh_metrics m
WHERE
      m.adr = 0
--   and m.id = 34847972
  and not m.is_excluded
  AND EXISTS( SELECT
                  1
              FROM
                  qm_pm_med_adh_potential_fills pf
                  JOIN patient_tasks pt ON pt.id = pf.patient_task_id AND pt.status IN ('new', 'in_progress')
              WHERE
                  pf.patient_measure_id = m.patient_measure_id )
;


SELECT *
FROM
    qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;

;
SELECT *
FROM
    patient_task_activities WHERE action = 'cancelled' order by id desc;
