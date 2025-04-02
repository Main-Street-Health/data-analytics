------------------------------------------------------------------------------------------------------------------------
/*
Hey - this working correctly?
Seems like they shouldn't be getting a task if the pt has meds on hand per MCO.
The MCO shows a 3/10 fill that goes until 6/8

Pt ID = 1112408
Measure = MAD
*/
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
      , m.priority_status
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
        pm.operational_year = 2025
        and pm.patient_id =  1112408
        -- pt.id =
        and pm.measure_key = 'med_adherence_diabetes'
    ORDER BY pm.measure_key, pf.id
    ;

-- look at data from the mco
SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
    patient_id = 1112408 and measure_key = 'med_adherence_diabetes'
order by signal_date desc
;

------------------------------------------------------------------------------------------------------------------------
/*
 Response
 The most recent data from the mco based on signal date shows a fill on 1/5
 The 3/10 fill is the second most recent data. It's likely this one should be flagged as a reversal
 */
------------------------------------------------------------------------------------------------------------------------