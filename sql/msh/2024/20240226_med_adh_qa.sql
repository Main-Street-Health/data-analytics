DROP TABLE IF EXISTS _coop;
CREATE TEMP TABLE _coop AS
SELECT DISTINCT
    pm.patient_id
  , pm.measure_key
  , pf.next_fill_date
  , pf.inserted_at
  , pf.pharmacy_verified_fill_date
  , pf.days_supply
  , pf.updated_at
  , m.next_fill_date metric_next_fill_date
, wf.is_system_verified_closed
, wf.compliance_check_date
FROM
    fdw_member_doc.patient_tasks pt
    JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pt.id = pf.patient_task_id
    JOIN fdw_member_doc.qm_pm_med_adh_wfs wf on pf.qm_pm_med_adh_wf_id = wf.id and not wf.is_system_verified_closed
    JOIN fdw_member_doc.qm_pm_tasks qpt ON pt.id = qpt.patient_task_id
    JOIN fdw_member_doc.qm_patient_measures pm ON qpt.patient_measure_id = pm.id
    JOIN fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
WHERE
    pt.id IN (
              978598, 978673, 978674, 978689, 978690, 978701, 978713, 979953, 979982, 982194, 982196
        );



SELECT
    c.patient_id
  , c.measure_key
  , c.next_fill_date
  , c.inserted_at
  , c.pharmacy_verified_fill_date
  , c.days_supply
  , c.updated_at
  , c.metric_next_fill_date
  , c.is_system_verified_closed
  , c.compliance_check_date
  , ARRAY_AGG(DISTINCT pp.inserted_at::DATE ORDER BY pp.inserted_at::DATE)
FROM
    _coop c
    JOIN sure_scripts_panel_patients pp ON pp.patient_id = c.patient_id
        AND pp.inserted_at >= '2024-01-01'
GROUP BY
    c.patient_id, c.measure_key, c.next_fill_date, c.inserted_at, c.pharmacy_verified_fill_date, c.days_supply
                , c.updated_at, c.metric_next_fill_date, c.is_system_verified_closed, c.compliance_check_date


;


select * from fdw_member_doc.patient_medication_fills where patient_id = 328099 and measure_key = 'med_adherence_hypertension';
select * from fdw_member_doc.patient_medication_fills where patient_id = 343523 and measure_key = 'med_adherence_cholesterol';
select * from fdw_member_doc.patient_medication_fills where patient_id = 441822 and measure_key = 'med_adherence_diabetes';


SELECT *
FROM
    prd.patient_medications
WHERE
    patient_id = 612462
and drug_description ~* 'statin'
and start_date >= '2023-01-01'
;
------------------------------------------------------------------------------------------------------------------------
/* weird activity */
------------------------------------------------------------------------------------------------------------------------
    SELECT
        m.patient_id
      , m.patient_measure_id
      , m.measure_key
      , pm.measure_status_key
      , m.next_fill_date
      , wf.is_active
      , wf.is_closed
      , wf.is_reopened
      , wf.compliance_check_date
      , pf.order_status
      , pf.medication_status
      , pf.pharmacy_verified_fill_date
      , pf.pharmacy_verified_days_supply
      , pf.inserted_at
      , pf.updated_at
      , pt.status
    FROM
        qm_patient_measures pm
        JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
        LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
        LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
        LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    WHERE
        pt.id = 978674
    ;;
SELECT *
FROM
    patient_task_activities WHERE patient_task_id =         978674
;

inserted_at,updated_at
2024-02-18 12:58:11,2024-02-20 20:36:48

SELECT
    id
  , activity_key
  , activity_source_type_key
--   , status_period_id
--   , patient_task_id
--   , patient_measure_id
--   , msh_cca_worksheet_id
--   , activity_by_id
--   , provider_external_order_worksheet_id
--   , patient_procedure_code_id
--   , falloff_status
--   , patient_appointment_date
--   , is_no_show
  , description
  , activity_at
  , processed_at
  , inserted_at
  , updated_at
FROM
    qm_pm_activities a
WHERE
    a.patient_measure_id = 347841
order by a.id

------------------------------------------------------------------------------------------------------------------------
/* 2/27 Banu  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.inserted_at
  , pf.updated_at
  , pt.status
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
WHERE
    pm.patient_id = 343103
    ;;

SELECT
    id
  , activity_key
  , activity_source_type_key
--   , status_period_id
--   , patient_task_id
--   , patient_measure_id
--   , msh_cca_worksheet_id
--   , activity_by_id
--   , provider_external_order_worksheet_id
--   , patient_procedure_code_id
--   , falloff_status
--   , patient_appointment_date
--   , is_no_show
  , description
  , activity_at
  , processed_at
  , inserted_at
  , updated_at
FROM
    qm_pm_activities
WHERE
    patient_measure_id = 347980
order by id
;



