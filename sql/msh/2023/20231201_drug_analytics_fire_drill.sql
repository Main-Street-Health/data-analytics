
DROP TABLE IF EXISTS _drug_counts;
CREATE TEMP TABLE _drug_counts AS
SELECT 'Trulicity' drug, count(distinct patient_id) nd FROM sure_scripts_med_history_details WHERE drug_description ~*      'Trulicity' union
SELECT 'Bydureon bcice' drug, count(distinct patient_id) nd FROM sure_scripts_med_history_details WHERE drug_description ~* 'Bydureon bcice' union
SELECT 'Byetta' drug, count(distinct patient_id) nd FROM sure_scripts_med_history_details WHERE drug_description ~*         'Byetta' union
SELECT 'Ozempic' drug, count(distinct patient_id) nd FROM sure_scripts_med_history_details WHERE drug_description ~*        'Ozempic' union
SELECT 'Victoza' drug, count(distinct patient_id) nd FROM sure_scripts_med_history_details WHERE drug_description ~*        'Victoza' union
SELECT 'Saxenda' drug, count(distinct patient_id) nd FROM sure_scripts_med_history_details WHERE drug_description ~*        'Saxenda' union
SELECT 'Adlyxin' drug, count(distinct patient_id) nd FROM sure_scripts_med_history_details WHERE drug_description ~*        'Adlyxin' union
SELECT 'Rybelsus' drug, count(distinct patient_id) nd FROM sure_scripts_med_history_details WHERE drug_description ~*       'Rybelsus' union
SELECT 'Mounjaro' drug, count(distinct patient_id) nd FROM sure_scripts_med_history_details WHERE drug_description ~*       'Mounjaro';
;

-- any drug
SELECT count(distinct patient_id) nd
FROM sure_scripts_med_history_details WHERE drug_description ~*      'Trulicity'
 or drug_description ~* 'Bydureon bcice'
or drug_description ~*         'Byetta'
or drug_description ~*        'Ozempic'
or drug_description ~*        'Victoza'
or drug_description ~*        'Saxenda'
or drug_description ~*        'Adlyxin'
or drug_description ~*       'Rybelsus'
or drug_description ~*       'Mounjaro'
;

SELECT count(distinct patient_id)
FROM
    prd.patient_medications pm

    JOIN ref.med_adherence_measures m ON pm.ndc = vs.code AND vs.code_type = 'NDC'
                                                        AND pm.value_set_item = vs.value_set_item
                                                        AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
                JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
            WHERE
                m.is_med = 'Y'
                and m.is_exclusion <> 'Y'
                and m.measure_id = 'PDC-DR'
;

SELECT count(distinct patient_id) FROM sure_scripts_med_history_details;
-- 97012
SELECT *
FROM
    _drug_counts;