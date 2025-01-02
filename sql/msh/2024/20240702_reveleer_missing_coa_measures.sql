SELECT * FROM public.reveleer_chases where patient_id = 1389208;
SELECT * FROM fdw_member_doc.qm_patient_measures where patient_id = 1389208 and measure_key ~* 'coa_pain_assessment';
SELECT *
FROM
    fdw_member_doc.qm_patient_measures
WHERE
  measure_key ~* 'coa_pain_assessment'
and patient_id in (
1389143, 1389109, 1388981, 1388722, 1388681, 1388642, 1388627, 1388613, 1388571
)