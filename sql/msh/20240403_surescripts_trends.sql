CREATE TABLE junk.surescripts_patient_effective_dates_20240404 AS
SELECT DISTINCT
    patient_id::bigint
  , d.bom
FROM
    sure_scripts_med_history_details mh
    JOIN ref.dates d ON d.bom BETWEEN mh.effective_date AND mh.expiration_date
        AND d.eom BETWEEN mh.effective_date AND mh.expiration_date
WHERE
      note IS DISTINCT FROM 'Patient Not Found.'
  AND d.day = d.bom
;
CREATE INDEX on junk.surescripts_patient_effective_dates_20240404(patient_id, bom);

ALTER TABLE junk.surescripts_patient_effective_dates_20240404
    ADD month_range daterange;

ALTER TABLE junk.surescripts_patient_effective_dates_20240404 ALTER COLUMN patient_id TYPE bigint using patient_id::bigint;

UPDATE
    junk.surescripts_patient_effective_dates_20240404
SET
    month_range = DATERANGE(bom, (bom + '1 month'::INTERVAL)::DATE - 1, '[]');

CREATE INDEX on junk.surescripts_patient_effective_dates_20240404 USING GIST (month_range);


DROP TABLE IF EXISTS _patient_meds_filled_in_month;
CREATE TEMP TABLE _patient_meds_filled_in_month AS
SELECT
    p.patient_id
  , p.bom
  , COUNT(*)                       total_fills
  , COUNT(DISTINCT ndc)            distinct_meds_filled
  , COUNT(DISTINCT prescriber_npi) distinct_prescribers
--   , pm.start_date
--   , pm.ndc
FROM
    junk.surescripts_patient_effective_dates_20240404 p
    JOIN prd.patient_medications pm ON p.patient_id = pm.patient_id AND pm.start_date <@ p.month_range
GROUP BY
    1, 2
    ;

DROP TABLE IF EXISTS _patient_meds_overlap_month;
CREATE TEMP TABLE _patient_meds_overlap_month AS
SELECT
    p.patient_id
  , p.bom
  , COUNT(*)                       total_fills
  , COUNT(DISTINCT ndc)            distinct_meds_filled
  , COUNT(DISTINCT prescriber_npi) distinct_prescribers
--   , pm.start_date
--   , pm.ndc
FROM
    junk.surescripts_patient_effective_dates_20240404 p
    JOIN prd.patient_medications pm ON p.patient_id = pm.patient_id AND daterange(pm.start_date, pm.end_date + 1, '[)') && p.month_range
where start_date <= end_date
GROUP BY
    1, 2
    ;


create table junk.surescripts_patient_month_summaries_20240404 as
SELECT
    p.patient_id
  , p.bom
  , month_range
  , starts.total_fills           start_total_fills
  , starts.distinct_meds_filled  start_distinct_meds_filled
  , starts.distinct_prescribers  start_distinct_prescribers
  , ovrlaps.total_fills          ovlp_total_fills
  , ovrlaps.distinct_meds_filled ovlp_distinct_meds_filled
  , ovrlaps.distinct_prescribers ovlp_distinct_prescribers
FROM
    junk.surescripts_patient_effective_dates_20240404 p
    LEFT JOIN _patient_meds_filled_in_month starts ON p.patient_id = starts.patient_id AND p.bom = starts.bom
    LEFT JOIN _patient_meds_overlap_month ovrlaps ON p.patient_id = ovrlaps.patient_id AND p.bom = ovrlaps.bom;


SELECT
    bom
  , COUNT(patient_id)               n_patient_id
  , SUM(start_total_fills)          n_start_total_fills
  , SUM(start_distinct_meds_filled) n_start_distinct_meds_filled
  , SUM(start_distinct_prescribers) n_start_distinct_prescribers
  , SUM(ovlp_total_fills)           n_ovlp_total_fills
  , SUM(ovlp_distinct_meds_filled)  n_ovlp_distinct_meds_filled
  , SUM(ovlp_distinct_prescribers)  n_ovlp_distinct_prescribers
FROM
    junk.surescripts_patient_month_summaries_20240404
GROUP BY 1
order by 1
;


