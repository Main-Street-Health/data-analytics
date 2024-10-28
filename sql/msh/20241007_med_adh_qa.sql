------------------------------------------------------------------------------------------------------------------------
/* cody why not query */
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
     , pf.expected_discharge_date
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
    pm.patient_id = 124271
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
--analytics - last query 2024-03-29 11:37:21.633295
-- SELECT * FROM sure_scripts_panel_patients where patient_id = 124271;

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
    pm.patient_id = 935021
    -- pt.id = 
    and pm.measure_key = 'med_adherence_hypertension'
ORDER BY pm.measure_key, pf.id
;
------------------------------------------------------------------------------------------------------------------------
/* processing handoffs issue */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      qm_pm_med_adh_handoffs.patient_id = 239490
  AND measure_key = 'med_adherence_cholesterol';

SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
     patient_id = 239490
  AND measure_key = 'med_adherence_cholesterol';
SELECT *
FROM
    patient_medication_fills
WHERE
     patient_id = 239490
  AND measure_key = 'med_adherence_cholesterol';

SELECT *
FROM
    patient_medication_fills where patient_medication_fills.analytics_id in ( 18878923, 16525442)

/* switch to analytics */
DROP TABLE IF EXISTS _mismatched_measure_keys;
CREATE TEMP TABLE _mismatched_measure_keys AS 
SELECT pm.analytics_id, pm.measure_key old_key, mamm.coop_measure_key new_key
FROM
    fdw_member_doc.patient_medication_fills pm
    LEFT JOIN ref.med_adherence_measures m
    JOIN ref.med_adherence_measure_names mamm ON mamm.analytics_measure_id = m.measure_id
    JOIN ref.med_adherence_value_sets vs ON m.value_set_id = vs.value_set_id
         ON vs.code = pm.ndc
             AND pm.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
             AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
             AND m.is_med = 'Y'
             AND m.is_exclusion = 'N'
             AND m.measure_version = '2024'
where pm.measure_key is distinct from mamm.coop_measure_key
;
SELECT
    old_key
  , new_key
  , COUNT(*)
FROM
    _mismatched_measure_keys
GROUP BY
    1, 2;
SELECT *
FROM
    _mismatched_measure_keys
WHERE
    old_key LIKE 'med#_adherence#_hypertension' ESCAPE '#' AND new_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';
SELECT *
FROM
    fdw_member_doc.patient_medication_fills WHERE analytics_id = -3092643;
SELECT *
FROM
    _mismatched_measure_keys m
join fdw_member_doc.patient_medication_fills f on f.analytics_id = m.analytics_id
WHERE
old_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#' AND new_key IS NULL
SELECT *
FROM
    analytics.ref.med_adherence_value_sets WHERE description ~* 'ATORVASTATIN 40 MG TABLET';


-- INSERT
-- INTO
--     ref.med_adherence_value_sets (value_set_id, value_set_subgroup, value_set_item, code_type, code, description,
--                                   route, dosage_form, ingredient, strength, units, is_recycled, from_date, thru_date,
--                                   attribute_type, attribute_value, inserted_at, updated_at)
-- VALUES
--     ('STATINS', 'STATINS', 'ATORVASTATIN', 'NDC', '70710177200', 'ATORVASTATIN 40 MG TABLET', 'ORAL', 'TABLET',
--      NULL, NULL, NULL, 'N', '1900-01-01', '2099-12-31', NULL, 'Manually added by BP 2024-10-07', now(),
-- now())
-- returning id
-- 188299
;
-- create table stage.pat_med_fills_upd_20241007 (analytics_id bigint PRIMARY KEY , new_measure_key text);
-- call cb.x_util_rebuild_fdw_stage();
INSERT
INTO
    fdw_member_doc_stage.pat_med_fills_upd_20241007 (analytics_id, new_measure_key)
    select analytics_id, new_key
from    _mismatched_measure_keys m
;
-- back to member doc

UPDATE patient_medication_fills f
SET
    measure_key = m.new_measure_key, updated_at = NOW()
FROM
    stage.pat_med_fills_upd_20241007 m
WHERE
    m.analytics_id = f.analytics_id
;
------------------------------------------------------------------------------------------------------------------------
/* suspecting guidelines issue from banu
   excludeInWorksheetReason: excludeInWorksheetReason
          ? excludeInWorksheetReason
          : isIncludedInWorksheet
            ? null
            : 'msh_suspecting_guidelines', // Machine excluded reason
      }),

*/
------------------------------------------------------------------------------------------------------------------------

SELECT
    xdx.patient_id
  , i.code_formatted
  , source
  , hcc_number
  , exclude_in_worksheet_reason
  , exclude_in_worksheet_rule_reasons
  , include_in_worksheet
  , include_in_worksheet_machine
  , qa_reviewed
  , needs_review
FROM
    msh_external_emr_diagnoses xdx
    JOIN icd10s i ON xdx.icd10_id = i.id
WHERE
      xdx.patient_id = 1111940
  AND i.code_formatted = 'E11.69'
;

------------------------------------------------------------------------------------------------------------------------
/* cody - why excluded */
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
    pm.patient_id = 1347220
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_exclusions
WHERE
      patient_id = 1347220
  AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#';

SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 1347220
  AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
ORDER BY
    signal_date DESC;
------------------------------------------------------------------------------------------------------------------------
/* burkey */
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
--     pm.patient_id =
    pt.id = 1805495
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 442000
and drug_description ~* 'Metoprolol'
--   AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#';
--analytics
SELECT *
FROM
    analytics.ref.med_adherence_value_sets
WHERE
    med_adherence_value_sets.code IN ('00378003210', '00378001805');
SELECT distinct m.measure_id, vs.value_set_id, vs.value_set_item
FROM
    analytics.ref.med_adherence_value_sets vs
join ref.med_adherence_measures m on vs.value_set_id = m.value_set_id
WHERE description ~* 'Metoprolol'

------------------------------------------------------------------------------------------------------------------------
/* cody source */
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
    pm.patient_id = 682471
    -- pt.id = 
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT start_date, end_date, inserted_at
FROM
    qm_pm_med_adh_synth_periods
WHERE
      patient_id = 682471
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';

SELECT last_fill_date, next_fill_date, signal_date, is_reversal
FROM
    stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 682471
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';
------------------------------------------------------------------------------------------------------------------------
/* banu mdp */
------------------------------------------------------------------------------------------------------------------------
SELECT
    xdx.patient_id
  , i.code_formatted
  , xdx.suspect_reason_machine
  , xdx.suspect_reason
  , xdx.source
  , xdx.cms_contract_year
  , xdx.hcc_number
  , xdx.most_recent_suspect_evidence_date
  , xdx.exclude_in_worksheet_reason
  , xdx.exclude_in_worksheet_rule_reasons
  , xdx.include_in_worksheet
  , xdx.include_in_worksheet_machine
  , xdx.qa_reviewed
  , xdx.needs_review
     , xdx.inserted_at
, wdx.*
FROM
    msh_external_emr_diagnoses xdx
    JOIN icd10s i ON xdx.icd10_id = i.id
join msh_cca_worksheet_dxs wdx on wdx.external_emr_diagnosis_id = xdx.id
WHERE
      xdx.patient_id = 837817
  AND i.code_formatted = 'E11.69'
  AND xdx.cms_contract_year = 2024
;

SELECT *
FROM
    member_doc.stage.msh_md_portal_suspects_history
WHERE
      golgi_patient_id = 837817
  AND icd_10_code = 'E11.69'

;


------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _dupes;
CREATE TEMP TABLE _dupes AS
SELECT patient_emr_id, count(*)
FROM
    supreme_pizza
where patient_emr_id is not null
and patient_status = 'active'
GROUP BY 1
having count(*) > 1
;
SELECT d.patient_emr_id, sp.patient_id, p.full_name, p.dob, p.status, p.substatus
FROM
    _dupes d
join supreme_pizza sp ON d.patient_emr_id = sp.patient_emr_id
join patients p on sp.patient_id = p.id
order by d.patient_emr_id
;
------------------------------------------------------------------------------------------------------------------------
/*  */
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
    pm.patient_id = 1211357
    -- pt.id =
    and pm.measure_key = 'med_adherence_diabetes'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods where patient_id = 1211357 AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';
------------------------------------------------------------------------------------------------------------------------
/*  */
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
    pm.patient_id = 1428057
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , pm.is_active
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
     , m.updated_at
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
    pm.patient_id = 674245
    -- pt.id = 
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
------------------------------------------------------------------------------------------------------------------------
/*  */
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
    pm.patient_id = 295389
    -- pt.id = 
    and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_synth_periods where patient_id = 295389 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';



------------------------------------------------------------------------------------------------------------------------
/* manual stage to check for reversals for cody */
------------------------------------------------------------------------------------------------------------------------
--
-- DROP TABLE IF EXISTS _last_sent;
-- CREATE TEMP TABLE _last_sent AS
-- SELECT patient_id, max(inserted_at) last_sent_at
-- FROM
--     sure_scripts_panel_patients GROUP BY patient_id ;
-- SELECT
--     COUNT(*)
-- FROM
--     _last_sent ls
-- WHERE
--       last_sent_at < NOW() - '90 days'::INTERVAL
--   AND EXISTS( SELECT
--                   1
--               FROM
--                   fdw_member_doc.qm_pm_med_adh_metrics m
--                   JOIN fdw_member_doc.qm_patient_measures pm ON pm.id = m.patient_measure_id
--               where ls.patient_id = m.patient_id
--               and pm.is_active
--               and pm.measure_source_key = 'sure_scripts'
--               );
--
--
--         DROP TABLE IF EXISTS _patients_to_pull;
--         CREATE TEMP TABLE _patients_to_pull (
--             patient_id BIGINT PRIMARY KEY NOT NULL,
--             reason     TEXT   NOT NULL
--         );
-- INSERT
-- INTO
--     _patients_to_pull (patient_id, reason)
-- SELECT DISTINCT
--     ls.patient_id
--   , 'manual: reversal cleanup for cody'
-- FROM
--     _last_sent ls
-- WHERE
--       last_sent_at < NOW() - '90 days'::INTERVAL
--   AND EXISTS( SELECT
--                   1
--               FROM
--                   fdw_member_doc.qm_pm_med_adh_metrics m
--                   JOIN fdw_member_doc.qm_patient_measures pm ON pm.id = m.patient_measure_id
--               WHERE
--                     ls.patient_id = m.patient_id
--                 AND pm.is_active
--                 AND pm.measure_source_key = 'sure_scripts' );
--
--
--    INSERT
--         INTO
--             public.sure_scripts_panel_patients (patient_id, sequence, last_name, first_name, middle_name, prefix,
--                                                 suffix, address_line_1, address_line_2, city, state, zip, dob, gender,
--                                                 npi,
--                                                 updated_at, inserted_at, reason_for_query)
--         SELECT DISTINCT
--             p.id                                                  patient_id
--           , ROW_NUMBER() OVER (ORDER BY p.id)                     sequence
--           , REGEXP_REPLACE(p.last_name, E'[\\n\\r]+', '', 'g')    last_name
--           , REGEXP_REPLACE(p.first_name, E'[\\n\\r]+', '', 'g')   first_name
--           , NULL                                                  middle_name
--           , NULL                                                  prefix
--           , NULL                                                  suffix
--           , REGEXP_REPLACE(pa.line1, E'[\\n\\r]+', '', 'g')       address_line_1
--           , REGEXP_REPLACE(pa.line2, E'[\\n\\r]+', '', 'g')       address_line_2
--           , REGEXP_REPLACE(pa.city, E'[\\n\\r]+', '', 'g')        city
--           , REGEXP_REPLACE(pa.state, E'[\\n\\r]+', '', 'g')       state
--           , REGEXP_REPLACE(pa.postal_code, E'[\\n\\r]+', '', 'g') zip
--           , p.dob
--           , LEFT(p.gender, 1)                                     gender
--           , COALESCE(mp.npi::TEXT, '1023087954')                  npi
--           , NOW()                                                 updated_at
--           , NOW()                                                 inserted_at
--           , ptp.reason
--         FROM
--             _patients_to_pull ptp
--             JOIN fdw_member_doc.patients p ON ptp.patient_id = p.id
--             JOIN fdw_member_doc.patient_addresses pa ON p.id = pa.patient_id
--             LEFT JOIN fdw_member_doc.patient_contacts pc
--                       ON p.id = pc.patient_id AND pc.relationship = 'physician' AND pc.is_primary
--             LEFT JOIN fdw_member_doc.msh_physicians mp ON mp.contact_id = pc.contact_id AND mp.npi IS NOT NULL
--         WHERE
--               -- don't add if patient already exists
--               NOT EXISTS( SELECT
--                               1
--                           FROM
--                               public.sure_scripts_panel_patients sspp
--                           WHERE
--                                 sspp.sure_scripts_panel_id ISNULL
--                             AND sspp.patient_id = p.id )
--           AND LENGTH(p.first_name) >= 2 -- SS requires Two of a person's names (Last Name, First Name, Middle Name) must have 2 or more characters.
--           AND LENGTH(p.last_name) >= 2;
-- --   and exists(select 1 from fdw_member_doc.supreme_pizza sp where sp.patient_id = ls.patient_id and sp.is_medication_adherence)
--
-- ;
------------------------------------------------------------------------------------------------------------------------
/* mdp cleanup */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _most_recent_source;
CREATE TEMP TABLE _most_recent_source AS
SELECT golgi_patient_id patient_id, icd10_id, max(source_date) most_recent_history
FROM
    stage.msh_md_portal_suspects_history
GROUP BY golgi_patient_id, icd10_id
;
create UNIQUE INDEX  on _most_recent_source(patient_id, icd10_id, most_recent_history)
SELECT count(*)
FROM
    _most_recent_source;

DROP TABLE IF EXISTS junk.mdp_with_bad_suspect_evidence_date_upd_20241011;
CREATE TABLE junk.mdp_with_bad_suspect_evidence_date_upd_20241011 AS
SELECT
    xdx.id                                xdx_id
  , xdx.most_recent_suspect_evidence_date old_most_recent_suspect_evidence_date
  , mrs.most_recent_history               new_most_recent_suspect_evidence_date
FROM
    _most_recent_source mrs
    JOIN msh_external_emr_diagnoses xdx ON xdx.patient_id = mrs.patient_id
        AND xdx.icd10_id = mrs.icd10_id
        AND xdx.most_recent_suspect_evidence_date < mrs.most_recent_history
WHERE
      xdx.source = 'md_portal'
  AND xdx.cms_contract_year = 2024
  AND xdx.diagnosis_type = 'suspect'
;
-- 335k pat
-- 1194222
update msh_external_emr_diagnoses xdx
set most_recent_suspect_evidence_date = new_most_recent_suspect_evidence_date, updated_at = now()
FROM
    junk.mdp_with_bad_suspect_evidence_date_upd_20241011 j
where j.xdx_id = xdx.id
;

WITH
    prep_pats AS ( SELECT
                       ARRAY_AGG(DISTINCT xdx.patient_id) pis
                   FROM
                       msh_external_emr_diagnoses xdx
                       JOIN junk.mdp_with_bad_suspect_evidence_date_upd_20241011 j ON j.xdx_id = xdx.id )
SELECT
    1
FROM
    prep_pats pp
        -- cross join stage._process_cca_groupings_for_cca_draft_multi(pp.pis)
    CROSS JOIN daug.cca_xdx_prep_master(pp.pis);
