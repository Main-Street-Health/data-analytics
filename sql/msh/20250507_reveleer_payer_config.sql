-- alter table qm_cfg_measure_payers add COLUMN send_to_reveleer bool not null default false;
UPDATE qm_cfg_measure_payers c
SET
    send_to_reveleer = TRUE
FROM
    junk.rev_plans_and_measures_2025 j
WHERE
      c.id = j.id
  AND TRIM(j."Send to Reveleer?") = 'Yes'
;

SELECT
    patient_id
  , MIN(panel_sent_at)       first_panel_sent_at
  , MAX(panel_sent_at)       last_panel_sent_at
  , COUNT(panel_sent_at)     n_panels_sent
  , MAX(history_received_at) latest_hist_received_at
FROM
    patient_sure_scripts_panels
WHERE
    patient_id IN (
                   332368, 1497365, 707522, 742058, 926787, 73654, 399526, 1656565, 192629, 901350, 1095702, 886889,
                   189056, 824987, 686177, 1323523, 1485189, 392367
        )
GROUP BY
    patient_id
;

