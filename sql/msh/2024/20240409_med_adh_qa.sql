SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , pm.measure_source_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.next_fill_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
        --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
    LEFT JOIN qm_pm_med_adh_synth_periods sp ON pm.id = sp.patient_measure_id
-- join qm_pm_med_adh_exclusions ex on ex.measure_key = pm.measure_key and ex.patient_id = pm.patient_id
WHERE
    pm.patient_id = 243175
ORDER BY
    pm.measure_key, pf.id
;

SELECT * FROM qm_pm_activities WHERE patient_measure_id = 402486 order by id;
-- SELECT * FROM qm_pm_med_adh_synth_periods WHERE patient_id = 243175;
------------------------------------------------------------------------------------------------------------------------
/* 2 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.next_fill_date pf_nfd
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
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
    pm.patient_id = 236608
and pm.measure_key = 'med_adherence_hypertension'
ORDER BY pm.measure_key, pf.id
;
------------------------------------------------------------------------------------------------------------------------
/* Cody */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
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
    pm.patient_id = 238816
and pm.measure_key = 'med_adherence_hypertension'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods sp
WHERE
      sp.patient_id = 238816
  AND sp.measure_key = 'med_adherence_hypertension'
;
SELECT *
FROM
    patient_medication_fills sp
WHERE
      sp.patient_id = 238816
  AND sp.measure_key = 'med_adherence_hypertension'
and start_date >= '2024-01-01'
;

-- analytics
SELECT patient_id, drug_description, last_filled_date, days_supply, inserted_at
FROM
    sure_scripts_med_history_details
WHERE
      patient_id = '238816'
  AND sure_scripts_med_history_id = 11980
and drug_description ~* 'losartan'
and last_filled_date >= '2024-01-01'
order by last_filled_date
;
SELECT *
FROM
    oban.oban_crons WHERE opts->'args'->>'sql'~*'msh_daily'
SELECT *
FROM
    navigator_schedules;
;
SELECT * FROM member_doc.stage.qm_pm_med_adh_mco_measures WHERE  next_fill_date ISNULL ;

SELECT * FROM public.qm_ref_measure_reveleer;
SELECT * FROM public.qm_ref_measures;

SELECT *
FROM
    qm_patient_measures pqm
    JOIN qm_mco_patient_measures m ON pqm.mco_patient_measure_id = m.id
;

------------------------------------------------------------------------------------------------------------------------
/* Another one off query for cody? */
------------------------------------------------------------------------------------------------------------------------
INSERT
INTO
    public.sure_scripts_panel_patients (patient_id, sequence, last_name, first_name, middle_name, prefix,
                                        suffix, address_line_1, address_line_2, city, state, zip, dob, gender, npi,
                                        updated_at, inserted_at, reason_for_query)
SELECT DISTINCT
    ptp.patient_id
  , ROW_NUMBER() OVER (ORDER BY ptp.patient_id)           sequence
  , REGEXP_REPLACE(p.last_name, E'[\\n\\r]+', '', 'g')    last_name
  , REGEXP_REPLACE(p.first_name, E'[\\n\\r]+', '', 'g')   first_name
  , NULL                                                  middle_name
  , NULL                                                  prefix
  , NULL                                                  suffix
  , REGEXP_REPLACE(pa.line1, E'[\\n\\r]+', '', 'g')       address_line_1
  , REGEXP_REPLACE(pa.line2, E'[\\n\\r]+', '', 'g')       address_line_2
  , REGEXP_REPLACE(pa.city, E'[\\n\\r]+', '', 'g')        city
  , REGEXP_REPLACE(pa.state, E'[\\n\\r]+', '', 'g')       state
  , REGEXP_REPLACE(pa.postal_code, E'[\\n\\r]+', '', 'g') zip
  , p.dob
  , LEFT(p.gender, 1)                                     gender
  , COALESCE(mp.npi::TEXT, '1023087954')                  npi
  , NOW()                                                 updated_at
  , NOW()                                                 inserted_at
  , ptp.reason
FROM
        ( SELECT 349978 patient_id, 'cody one off' reason ) ptp
        JOIN fdw_member_doc.patients p ON ptp.patient_id = p.id
        JOIN fdw_member_doc.patient_addresses pa ON p.id = pa.patient_id
        LEFT JOIN fdw_member_doc.patient_contacts pc
                  ON p.id = pc.patient_id AND pc.relationship = 'physician' AND pc.is_primary
        LEFT JOIN fdw_member_doc.msh_physicians mp ON mp.contact_id = pc.contact_id AND mp.npi IS NOT NULL
WHERE
      -- don't add if patient already exists
      NOT EXISTS( SELECT
                      1
                  FROM
                      public.sure_scripts_panel_patients sspp
                  WHERE
                        sspp.sure_scripts_panel_id ISNULL
                    AND sspp.patient_id = ptp.patient_id )
  AND LENGTH(p.first_name) >= 2 -- SS requires Two of a person's names (Last Name, First Name, Middle Name) must have 2 or more characters.
  AND LENGTH(p.last_name) >= 2
;

------------------------------------------------------------------------------------------------------------------------
/* 4/11 */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    qm_pm_med_adh_handoffs where processed_at ISNULL ;
SELECT * FROM oban_jobs WHERE queue ~* 'med' order by id;

SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
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
    pm.patient_id = 885880

ORDER BY pm.measure_key, pf.id
;
SELECT * FROM qm_pm_med_adh_handoffs WHERE patient_id = 885880 order by id;
SELECT * FROM qm_pm_med_adh_synth_periods WHERE patient_id = 885880 and measure_key = 'med_adherence_diabetes' order by analytics_id
SELECT * FROM stage.qm_pm_med_adh_mco_measures WHERE patient_id = 885880 and measure_key = 'med_adherence_diabetes' order by next_fill_date
;
-- DELETE
select *
FROM
    qm_pm_med_adh_handoffs
WHERE processed_at ISNULL ;
call qm_pm_med_adh_process();
SELECT *
FROM
    oban_jobs
WHERE
    queue ~* 'med'
and id = 2969187
ORDER BY
    id;

