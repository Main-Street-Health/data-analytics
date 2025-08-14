SELECT * FROM qm_pm_med_adh_metrics where patient_id = 322685;
SELECT * FROM patient_sure_scripts_panels where patient_id = 322685;

;
SELECT *
FROM
    qm_pm_activities where patient_measure_id = 2998780 order by id;
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods where
patient_id = 322685 AND yr = 2025 AND measure_key = 'med_adherence_cholesterol';
SELECT *
FROM
    qm_pm_med_adh_handoffs
where
        patient_id = 322685 AND measure_year = 2025 AND measure_key = 'med_adherence_cholesterol'
order by id
;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
    patient_id = 118621
and yr = 2025
;

SELECT *
FROM
    qm_pm_med_adh_handoffs
where
        patient_id = 118621 AND measure_year = 2025 AND measure_key = 'med_adherence_diabetes'
order by id;

SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
where
    patient_id = 118621 AND measure_year = 2025 AND measure_key = 'med_adherence_diabetes'
;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT * from patient_sure_scripts_panels where patient_id = 118621
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------

SELECT * from qm_pm_med_adh_synth_periods where patient_id = 269085 and yr = 2025 and measure_key = 'med_adherence_cholesterol';
SELECT * from qm_pm_med_adh_metrics where patient_id = 269085 and measure_year = 2025 and measure_key = 'med_adherence_cholesterol';
SELECT * from stage.qm_pm_med_adh_mco_measures where patient_id = 269085 and measure_year = 2025 and measure_key = 'med_adherence_cholesterol';
SELECT * from qm_pm_med_adh_handoffs where patient_id = 269085 and measure_year = 2025 and measure_key = 'med_adherence_cholesterol' order by id;

SELECT start_at AT TIME ZONE  'america/chicago', *
FROM
    qm_pm_med_adh_priority_histories
WHERE
    med_adh_metric_id = 83932176;


------------------------------------------------------------------------------------------------------------------------
/* 951150 */
------------------------------------------------------------------------------------------------------------------------
SELECT * from qm_pm_med_adh_synth_periods where patient_id = 951150  and yr = 2025 and measure_key = 'med_adherence_cholesterol';
SELECT * from stage.qm_pm_med_adh_mco_measures where patient_id = 951150  and measure_year = 2025 and measure_key = 'med_adherence_cholesterol' order by signal_date ;
SELECT * from qm_pm_med_adh_metrics where patient_id = 951150  and measure_year = 2025 and measure_key = 'med_adherence_cholesterol';

------------------------------------------------------------------------------------------------------------------------
/* 20250728 */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    qm_pm_med_adh_metrics m
WHERE
      measure_year = 2025
  AND patient_id = 482600
  AND measure_key = 'med_adherence_hypertension'
;

SELECT *
FROM
    qm_pm_med_adh_handoffs h
WHERE
      measure_year = 2025
  AND patient_id = 482600
  AND measure_key = 'med_adherence_hypertension'
order by id
;

SELECT *
FROM
    qm_pm_med_adh_synth_periods sp
WHERE
      yr = 2025
  AND patient_id = 482600
  AND measure_key = 'med_adherence_hypertension'
;

SELECT * FROM patient_sure_scripts_panels where patient_id = 482600;
SELECT * FROM patient_medication_fills where patient_id = 482600 and start_date > '2025-01-01';


------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM qm_pm_med_adh_metrics where patient_id = 1405853 ;
SELECT * FROM stage.qm_pm_med_adh_mco_measures where patient_id = 1405853 ;

SELECT count(DISTINCT  patient_task_id)
FROM
    qm_pm_med_adh_potential_fills
where inserted_at >= now() - '1 day'::Interval
;
-- order by id desc;