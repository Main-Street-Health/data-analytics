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
    fdw_member_doc.qm_patient_measures pm
    JOIN fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 302662
    -- pt.id =
ORDER BY pm.measure_key, pf.id
;

--6/26 - 9/24
SELECT *
FROM
    fdw_member_doc.patient_medication_fills
WHERE
      patient_id = 302662
  AND measure_key = 'med_adherence_cholesterol';

-- 3/25-6/26
SELECT *
FROM
    fdw_member_doc_stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 302662
  AND measure_key = 'med_adherence_cholesterol'


------------------------------------------------------------------------------------------------------------------------
/* failed job */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    fdw_member_doc.oban_jobs where queue ~* 'med_adh';
SELECT *
FROM
    fdw_member_doc.qm_pm_med_adh_metrics WHERE patient_measure_id = 527358;
SELECT m.patient_id, pm.patient_id
FROM
    fdw_member_doc.qm_pm_med_adh_metrics m
join fdw_member_doc.qm_patient_measures pm on pm.id = m.patient_measure_id
WHERE
      m.patient_id = 347380
  AND m.measure_key = 'med_adherence_hypertension'
;
SELECT m.* --m.patient_id, pm.patient_id
FROM
    fdw_member_doc.qm_pm_med_adh_metrics m
-- join fdw_member_doc.qm_patient_measures pm on pm.id = m.patient_measure_id
WHERE
      m.patient_id = 347380
  AND m.measure_key = 'med_adherence_hypertension'
;
161,255,816
9,223,372,036,854,775,807

SELECT m.patient_id, m.*
FROM
    fdw_member_doc.qm_pm_med_adh_metrics m
WHERE
      m.patient_id = 915192
  AND m.measure_key = 'med_adherence_hypertension'
;
SELECT * FROM fdw_member_doc.patients where id = 347380;
SELECT *
FROM
    fdw_member_doc.patients
WHERE
      last_name LIKE 'Gaylord'
  AND first_name LIKE 'Dennis';
SELECT *
FROM
    fdw_member_doc.qm_patient_measures pm
WHERE
    pm.id = 527358;



SELECT *
FROM
    oban_jobs
WHERE
    queue ~* 'med_adh'
ORDER BY
    id DESC;

SELECT
    id
  , patient_task_id
  , qm_pm_med_adh_wf_id
  , patient_measure_id
  , is_system_verified_closed
  , system_verified_closed_at
  , ndc
  , drug_description
  , start_date
  , days_supply
  , end_date
  , quantity
  , sold_date
  , last_filled_date
  , written_date
  , prescriber_name
  , prescriber_npi
  , prescriber_phone
  , dispenser_type
  , dispenser_name
  , dispenser_npi
  , dispenser_phone
  , refills_remaining
  , src
  , medication_status
  , contacted_pharmacy_to_cancel
  , order_status
  , patient_refused_reason
  , visit_date
  , is_task_reopened
  , expected_discharge_date
  , unknown_discharge_date
  , pharmacy_not_found
  , pharmacy_id
  , delivery_refused_reason
  , part_d_covered
  , completed_at
  , completed_by_id
  , deleted_at
  , deleted_by_id
  , created_by_id
  , updated_by_id
  , inserted_at
  , updated_at
  , next_fill_date
  , moved_to_ninety_day_status
  , moved_to_ninety_day_reason
  , pharmacy_verified_fill_date
  , pharmacy_verified_days_supply
  , is_high_cost
  , is_msh_provider
  , is_current
  , patient_refused_reason_other
  , is_experiencing_side_effects
  , has_indicated_medication_not_needed
  , has_provider_conversation_occurred
  , alt_pharmacy_name
  , alt_pharmacy_phone
  , facility_filling_medication_on_discharge
  , facility_fill_duration
  , discharger_name
  , meds_on_hand_date
  , meds_on_hand_days_supply
  , dispenser_ncpdpid
  , sure_scripts_pharmacy_id
  , compliance_check_date
FROM
    qm_pm_med_adh_potential_fills where meds_on_hand_date is not null order by id desc;



SELECT *
FROM
    _mismatches;
;

-- UPDATE fdw_member_doc.qm_pm_med_adh_metrics
-- SET
--     patient_id = 915192::BIGINT
-- WHERE
--       id = 4245801::BIGINT;
--   AND patient_id = 347380::BIGINT
--   AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE
--       '#' AND measure_year = 2024::INTEGER AND patient_measure_id = 527358::BIGINT AND
--       measure_source_key LIKE 'sure#_scripts' ESCAPE
--       '#' AND fill_count = 3::INTEGER AND ipsd = '2024-02-03'::DATE AND next_fill_date = '2024-10-30'::DATE AND
--       days_covered_to_period_end = 270::INTEGER AND days_not_covered = 0::INTEGER AND
--       absolute_fail_date = '2025-01-04'::DATE AND calc_to_date = '2024-07-23'::DATE AND
--       pdc_to_date = 1.00::NUMERIC(5, 2) AND adr = 67::INTEGER AND failed_last_year = FALSE::BOOLEAN AND
--       inserted_at = '2024-03-03 03:16:04'::TIMESTAMP(0) AND updated_at = '2024-07-23 10:04:05'::TIMESTAMP(0) AND
--       is_on_90_day_supply = TRUE::BOOLEAN AND has_side_effects = FALSE::BOOLEAN AND is_excluded = FALSE::BOOLEAN;
-- 

SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , pm.is_active
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
    pm.patient_id = 528634
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;

SELECT * FROM qm_pm_activities WHERE patient_measure_id = 507598 order by id;
SELECT * FROM qm_pm_status_periods WHERE patient_measure_id = 507598 order by id;

SELECT * FROM patient_medication_fills where patient_id = 528634 and measure_key = 'med_adherence_cholesterol';
SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 528634
  AND measure_key = 'med_adherence_cholesterol';;


SELECT pm.measure_status_key, count(*)
FROM
    qm_pm_med_adh_metrics m
join qm_patient_measures pm on m.patient_measure_id = pm.id
GROUP BY pm.measure_status_key
;
SELECT *
FROM
    pg_stat_statements WHERE queryid = -2460888075760826771;

SELECT distinct on (p.patient_id) p.patient_id, inserted_at
FROM
    md_portal_roster_patients p
join md_portal_rosters r on r.id = p.md_portal_roster_id
WHERE
    p.patient_id IN (
                   323333, 1003622, 1007430, 1007431, 1236799, 1237078, 1241514, 1246379, 1246385, 1272167, 1285375,
                   1285407, 1285433, 1285452, 1285466, 1350702, 1367167, 1400810, 1412864, 1422580, 1430087, 1430101,
                   1443501, 1443662, 1443681, 1443683, 1443684, 1443699, 1443704, 1443713, 1443735, 1443736, 1443742,
                   1443745, 1443746, 1443754, 1443756, 1443761, 1443762, 1443763, 1443764, 1443770, 1443924, 1444147,
                   1444306, 325229, 325455, 443044, 443917, 447108, 967164, 967532
        )
ORDER BY p.patient_id, r.inserted_at desc
;
SELECT p.id patient_id, gender, p.first_name, p.last_name, p.dob, p.inserted_at, updated_at
FROM
    patients p
    where
p.id IN (
                   323333, 1003622, 1007430, 1007431, 1236799, 1237078, 1241514, 1246379, 1246385, 1272167, 1285375,
                   1285407, 1285433, 1285452, 1285466, 1350702, 1367167, 1400810, 1412864, 1422580, 1430087, 1430101,
                   1443501, 1443662, 1443681, 1443683, 1443684, 1443699, 1443704, 1443713, 1443735, 1443736, 1443742,
                   1443745, 1443746, 1443754, 1443756, 1443761, 1443762, 1443763, 1443764, 1443770, 1443924, 1444147,
                   1444306, 325229, 325455, 443044, 443917, 447108, 967164, 967532
        )
and gender <> 'Unknown'
;

SELECT p.id patient_id, gender, p.first_name, p.last_name, p.dob, p.inserted_at
FROM
    patients p
    LEFT JOIN patient_addresses pa
              ON p.id = pa.patient_id AND pa.line1 IS NOT NULL AND pa.city IS NOT NULL AND pa.state IS NOT NULL AND
                 pa.postal_code IS NOT NULL AND pa.line1 !~* 'unknown'::TEXT
    JOIN supreme_pizza sp ON p.id = sp.patient_id AND sp.is_md_portal_full
    JOIN patient_referring_partners prp ON prp.patient_id = p.id
    JOIN referring_partners rp ON prp.referring_partner_id = rp.id AND rp.organization_id <> 7
    JOIN referring_partners primary_rp
         ON sp.primary_referring_partner_id = primary_rp.id AND primary_rp.organization_id <> 7
    JOIN msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id
WHERE
      p.gender <> 'Unknown'::TEXT
  AND COALESCE(pa.line1, primary_rp.address1) IS NOT NULL
  AND sp.is_md_portal_full
  AND NOT (EXISTS ( SELECT
                        1
                    FROM
                        patient_referring_partners prp2
                        JOIN referring_partners rp2 ON prp2.referring_partner_id = rp2.id
                        JOIN msh_referring_partner_organizations mrpo2 ON rp2.organization_id = mrpo2.id
                    WHERE
                          p.id = prp2.patient_id
                      AND mrpo2.id = 248 ))
and
p.id IN (
                   323333, 1003622, 1007430, 1007431, 1236799, 1237078, 1241514, 1246379, 1246385, 1272167, 1285375,
                   1285407, 1285433, 1285452, 1285466, 1350702, 1367167, 1400810, 1412864, 1422580, 1430087, 1430101,
                   1443501, 1443662, 1443681, 1443683, 1443684, 1443699, 1443704, 1443713, 1443735, 1443736, 1443742,
                   1443745, 1443746, 1443754, 1443756, 1443761, 1443762, 1443763, 1443764, 1443770, 1443924, 1444147,
                   1444306, 325229, 325455, 443044, 443917, 447108, 967164, 967532
        )

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
    fdw_member_doc.qm_patient_measures pm
    JOIN fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 124121
    -- pt.id = 
ORDER BY pm.measure_key, pf.id
;
SELECT adr, *
FROM
    fdw_member_doc_stage.qm_pm_med_adh_mco_measures
WHERE
    patient_id = 124121
ORDER BY
    inserted_at DESC
;
SELECT *
FROM
    fdw_member_doc.payers where id = 1;

SELECT
  *
FROM
 raw.aetna_quality_rx_part_d
-- WHERE member_last_name = 'HUBBARD' and member_first_name = 'KENNETH'
--  measure = 'ADH-DIABETES'
--  AND humana_patient_id = 'H7920776300'
ORDER BY inserted_at DESC
;
call staging.rts_aetna_gap_rx_adherence();

------------------------------------------------------------------------------------------------------------------------
/* revert */
------------------------------------------------------------------------------------------------------------------------
-- SELECT pf.patient_task_id, wf.*
SELECT pf.patient_task_id, *
FROM
    qm_pm_med_adh_wfs wf
    JOIN member_doc.public.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
WHERE
      pf.patient_task_id IN (1468804, 1425641, 1535330, 1604784, 1430993)
  AND NOT wf.is_active
  AND NOT wf.is_system_verified_closed
and exists(select 1 from qm_pm_med_adh_wfs wf2
                   where wf2.patient_measure_id = wf.patient_measure_id and wf2.id > wf.id
                    )
;
SELECT *
FROM
    qm_pm_med_adh_wfs WHERE patient_measure_id = 763335 order by id;



UPDATE qm_pm_med_adh_wfs wf
SET
    is_active = TRUE, compliance_check_date = NOW()::DATE, updated_at = NOW(), updated_by_id = 98
FROM
    member_doc.public.qm_pm_med_adh_potential_fills pf
WHERE
      pf.qm_pm_med_adh_wf_id = wf.id
  AND pf.patient_task_id IN (1425641, 1535330, 1604784, 1430993)
;




1426439
SELECT *
FROM
    qm_pm_med_adh_metrics WHERE patient_measure_id = 349571
;