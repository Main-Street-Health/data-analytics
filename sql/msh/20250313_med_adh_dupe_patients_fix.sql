
SELECT * FROM qm_pm_med_adh_metrics WHERE patient_measure_id = 3132849;
SELECT patient_id, measure_key, operational_year FROM qm_patient_measures WHERE id = 3132849;
SELECT *
FROM
    qm_pm_med_adh_metrics
WHERE
      patient_id = 982219
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#' AND measure_year = 2025;

-- SELECT m.inserted_at::date, count(*)
-- FROM
UPDATE qm_pm_med_adh_metrics m
SET
    patient_measure_id = pm.id, updated_at = now()
FROM
    qm_patient_measures pm
WHERE
      pm.patient_id = m.patient_id
  AND pm.measure_key = m.measure_key
  AND pm.operational_year = m.measure_year
  AND m.patient_measure_id ISNULL
;

SELECT *
FROM
    qm_pm_med_adh_metrics WHERE patient_measure_id = 2921085;

SELECT *
FROM
    qm_patient_measures WHERE  id = 2921085;
select * from patients p where id in (1237499, 74325);
SELECT *
FROM
    qm_pm_med_adh_metrics
WHERE
      patient_id = 74325
  AND measure_year = 2025
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';

-- There's a Duke patient, the one with that's inactive is currently set on the Med Adherence metric with the patient measure ID. We need to just delete the patient measure ID from that one so that the better metric can update.

UPDATE qm_pm_med_adh_metrics
SET
    patient_measure_id = NULL
, updated_at = now()
WHERE
    id = 83956234;

------------------------------------------------------------------------------------------------------------------------
/* again 2932936 */
------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM
    qm_pm_med_adh_metrics WHERE patient_measure_id = 2932936;
-- SELECT *
-- FROM
-- its all the same patient
update
    qm_pm_med_adh_metrics
set patient_measure_id = null, updated_at = now()
where patient_id = 1237499 and measure_year = 2025;

SELECT *
FROM
    qm_patient_measures
WHERE
      patient_id = 1237499
  AND operational_year = 2025
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';

SELECT *
FROM
    qm_patient_measures where id = 2932936;

1237499, 74325

UPDATE qm_pm_med_adh_metrics
SET
    patient_measure_id = NULL
, updated_at = now()
WHERE
    id = 83876372;

-- no mismatches
SELECT
    m.patient_measure_id
  , m.patient_id
  , m.measure_key
  , m.measure_year
  , pm.id
  , pm.patient_id
  , pm.measure_key
  , pm.operational_year
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures pm ON
        pm.patient_id = m.patient_id
            AND pm.measure_key = m.measure_key
            AND pm.operational_year = m.measure_year
WHERE
      m.patient_measure_id IS NOT NULL
  AND m.patient_measure_id != pm.id
;


