SELECT *
FROM
    prd.patient_medications pm
    JOIN ref.med_adherence_value_sets vs ON vs.code = pm.ndc
        AND pm.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
    JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
        AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
        AND m.is_med = 'Y'
        AND m.is_exclusion = 'N'
    JOIN ref.med_adherence_measure_names mamm ON mamm.analytics_measure_id = m.measure_id


;
for each day of the year
 is compliant
 is non compliant
 has open med adh task