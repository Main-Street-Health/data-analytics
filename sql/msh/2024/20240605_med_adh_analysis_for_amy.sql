DROP TABLE IF EXISTS _base;
CREATE TEMP TABLE _base AS
SELECT DISTINCT
    pm.patient_id
  , pm.measure_key
  , m.patient_measure_id
, m.pdc_to_date
, m.adr
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
                                        and not m.is_excluded
        and pm.is_active
    JOIN cache_register cr ON m.patient_id = cr.patient_id
WHERE
     (pm.measure_key = 'med_adherence_diabetes' AND cr.is_med_adh_mad_convertable_to_90_days)
  OR (pm.measure_key = 'med_adherence_cholesterol' AND cr.is_med_adh_mac_convertable_to_90_days)
  OR (pm.measure_key = 'med_adherence_hypertension' AND cr.is_med_adh_mah_convertable_to_90_days);

-- Scenario 1:
DROP TABLE IF EXISTS _output;
CREATE TEMP TABLE _output AS
SELECT
    b.measure_key
  , 'scenario 1' scenario
  , COUNT(*)     nd_measures
FROM
    _base b
    JOIN qm_pm_med_adh_metrics m ON b.patient_measure_id = m.patient_measure_id
WHERE
--     Patient was compliant last year - defined as
-- We have received signal from the health plan that member was compliant in 2023 - this would be from med adherence data from 2023
    EXISTS( SELECT
                1
        FROM
                rpt.medication_adherence_performance_detail d
        WHERE
            d.measure_year = 2023
          AND d.measure_key ~* 'med_adherence'
          AND d.most_recent_signal_flag
          AND d.patient_id = b.patient_id
          AND d.measure_key = b.measure_key
          AND d.compliance_flag )
--    Or
-- patient is NOT on a red list file we have gotten in 2024 for the 2023 year (e.g. red list as they failed in 2023)
-- PDC >90%
  OR (
    NOT m.failed_last_year
        AND b.pdc_to_date >= .9
    )
GROUP BY
    1, 2
;

INSERT
INTO
    _output (measure_key, scenario, nd_measures)
-- Scenario 2:
SELECT b.measure_key, 'scenario 2', count(*)
FROM
    _base b
    JOIN qm_pm_med_adh_metrics m ON b.patient_measure_id = m.patient_measure_id
WHERE
-- Past MCO Compliance data not taken into account
-- patient is NOT on a red list file we have gotten in 2024 for the 2023 year (e.g. red list as they failed in 2023)
-- PDC >95% and
-- ADR is >30
    NOT m.failed_last_year
  AND b.pdc_to_date >= .95
  AND b.adr > 30
GROUP BY 1
;
SELECT *
FROM
    _output;
SELECT measure_key, count(distinct patient_measure_id)
FROM
    _base
GROUP BY measure_key
;