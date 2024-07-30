SELECT *
FROM
    fdw_member_doc.patient_medication_fills WHERE analytics_id = 12308878;



-- dupe patients
SELECT * FROM fdw_member_doc.empi_patient_matches WHERE p1_patient_id IN (346294, 399455) or p2_patient_id IN (346294, 399455);
SELECT * FROM fdw_member_doc.msh_patient_status_substatus_overrides WHERE patient_id IN (346294, 399455);
SELECT * FROM gmm.global_members gm WHERE gm.patient_id IN  (346294, 399455);


399455 -- coop has sp, no med fill | analytics: has pm, has sp, has ss in 2024
346294 -- coop has fill, no sp     | analytics: no pm, no sp, no ss in 2024

SELECT *
FROM
--     fdw_member_doc.qm_pm_med_adh_synth_periods
        fdw_member_doc.patient_medication_fills
where
      patient_id = 346294
--     patient_id = 399455
  AND DATE_PART('year', start_date) = 2024
order by start_date
--   AND measure_key = 'med_adherence_diabetes'
;

SELECT *
FROM
    analytics.prd.patient_medications
--     prd.patient_med_adherence_synth_periods
WHERE
      patient_id = 399455
--     patient_id = 346294
  AND DATE_PART('year', start_date) = 2024
ORDER BY
    id;

SELECT *
FROM
    fdw_member_doc.patient_medication_fills
WHERE
    patient_id = 346294 -- 2/13
--     analytics_id = 12308878;
    ;


SELECT
    mhd.product_code
  , mhd.drug_description
  , last_filled_date
  , sold_date
, days_supply
, product_code_qualifier
, mhd.id
FROM
    sure_scripts_med_history_details mhd
    JOIN sure_scripts_med_histories mh ON mhd.sure_scripts_med_history_id = mh.id
WHERE
--       patient_id = '399455'
    patient_id = '346294'
  AND last_filled_date::DATE >= '2024-01-01'
  AND
--     mhd.id = 41892436;
-- 9967 -- where its from
      -- latest mhd
      mhd.sure_scripts_med_history_id = 10759;

SELECT *
FROM
    sure_scripts_med_history_details
WHERE
    patient_id = '346294'
  AND last_filled_date::DATE >= '2024-01-01'
;
DROP TABLE IF EXISTS _missmatch;
CREATE TEMP TABLE _missmatch AS
SELECT pm.id
FROM
    analytics.prd.patient_medications pm
join fdw_member_doc.patient_medication_fills f on f.analytics_id = pm.id and f.patient_id != pm.patient_id
;
UPDATE fdw_member_doc.patient_medication_fills f
SET
    patient_id = pm.patient_id, updated_at = now()
FROM
    _missmatch m
    JOIN prd.patient_medications pm ON pm.id = m.id
WHERE
    f.analytics_id = m.id
;
-- SELECT * FROM fdw_member_doc.users u where u.last_name = 'Pierson'; 98

