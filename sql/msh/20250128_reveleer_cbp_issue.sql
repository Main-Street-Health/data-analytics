SELECT *
FROM
    reveleer_attribute_file_details a
    JOIN reveleer_chases rc ON a.reveleer_chase_id = rc.id
WHERE
      a.yr = 2024
  AND a.patient_id = 755944
  AND rc.measure_code = 'CBP' ;

SELECT *
FROM
    fdw_member_doc.patient_blood_pressures bp
WHERE
    bp.patient_id = 755944
;


SELECT external_chase_id
FROM
    fdw_member_doc.qm_patient_measures pm
    JOIN reveleer_chases rc
         ON pm.id = ANY (rc.qm_patient_measure_ids);
SELECT *
FROM
    analytics.oban.oban_crons;

