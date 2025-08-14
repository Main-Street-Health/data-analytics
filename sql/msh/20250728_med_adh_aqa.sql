
SELECT * from prd.patient_medication_deletions where patient_id = 118621 order by id desc
SELECT * from prd.patient_medication_deletions where patient_id = 269085 order by id desc;
SELECT * FROM sure_scripts_panel_patients where patient_id = 482600;
SELECT * FROM prd.patient_medications where patient_id = 482600 and start_date > '2025-01-01';

SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 22705
  AND patient_id = '482600'
ORDER BY
    last_filled_date;

SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      sure_scripts_panel_id = 22705
and product_code is not null
;
--   AND patient_id = '482600'

-- stuff on the panel


SELECT
    deleted_at
  , COUNT(distinct patient_id) nd_patients
  , COUNT(*) n_fills
  , COUNT(*) * 1.0 / COUNT(distinct patient_id) fills_per_patient_deleted
FROM
    analytics.prd.patient_medication_deletions
WHERE
    deleted_at > NOW() - '4 week'::INTERVAL
GROUP BY
    1
ORDER BY
    1
;
SELECT *
FROM
    sure_scripts_med_histories where inserted_at >= '2025-07-21';
SELECT
    inserted_at::DATE his_received_date
  , product_code_qualifier
  , COUNT(*)
FROM
    sure_scripts_med_history_details
WHERE
    sure_scripts_med_history_id = 23398 -- IN (23497, 23530, 23563, 23596, 23629, 23662)
GROUP BY
    1, 2
ORDER BY
    1, 2
;


SELECT *
FROM
    sure_scripts_med_history_details mhd
WHERE
    mhd.sure_scripts_med_history_id = ( SELECT
                                            MAX(id) id
                                        FROM
                                            sure_scripts_med_histories )
;


--     sure_scripts_med_history_id = 23662
--     sure_scripts_med_history_id IN (23497, 23530, 23563, 23596, 23629, 23662)
-- and sold_date is not null
;


