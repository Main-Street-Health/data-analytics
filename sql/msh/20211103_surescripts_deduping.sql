DROP TABLE IF EXISTS _pot_dupes;
CREATE TEMP TABLE _pot_dupes AS
SELECT Distinct
    pm1.patient_id
  , m.measure_id
  , pm1.ndc
  , pm1.drug_description
  , pm1.last_filled_date                        last_filled_date1
  , pm2.last_filled_date                        last_filled_date2
  , pm1.sold_date                               sold_date1
  , pm2.sold_date                               sold_date2
  , pm1.days_supply                             days_supply1
  , pm2.days_supply                             days_supply2
  , pm1.dispenser_name                          dispenser_name1
  , pm2.dispenser_name                          dispenser_name2
  , pm1.refills_remaining                       refills_remaining1
  , pm2.refills_remaining                       refills_remaining2
  , pm1.last_filled_date = pm2.last_filled_date is_exact_match
  , pm1.id                                      first_patient_medication_id
  , pm2.id                                      second_patient_medication_id
  , mhd1.id                                     sure_scripts_med_history_detail_id1
  , mhd2.id                                     sure_scripts_med_history_detail_id2
  , mhd1.sure_scripts_med_history_id            sure_scripts_med_history_id1
  , mhd2.sure_scripts_med_history_id            sure_scripts_med_history_id2
  , mhd1.source_description                     source1
  , mhd2.source_description                     source2
  , pmay.next_fill_date >= now()::date          currently_compliant
  , pmay.ipsd
  , pmay.next_fill_date
  , pmay.absolute_fail_date
  , pmay.adr
  , pmay.pdc_to_date
FROM
    prd.patient_medications pm1
    JOIN sure_scripts_med_history_details mhd1 ON pm1.last_src_id = mhd1.id
    JOIN ref.med_adherence_value_sets vs
         ON vs.code = pm1.ndc AND pm1.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
    JOIN ref.med_adherence_measures m
         ON m.value_set_id = vs.value_set_id AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
    JOIN prd.patient_med_adherence_year_measures pmay
         ON pmay.patient_id = pm1.patient_id AND pmay.measure_id = m.measure_id
    JOIN prd.patient_medications pm2 ON pm1.patient_id = pm2.patient_id
        AND pm1.ndc = pm2.ndc
        AND pm1.days_supply = pm2.days_supply
        AND pm1.last_filled_date BETWEEN pm2.last_filled_date - 3 AND pm2.last_filled_date + 3
        AND pm1.sold_date IS DISTINCT FROM pm2.sold_date
        AND pm1.id < pm2.id
    JOIN sure_scripts_med_history_details mhd2
         ON pm2.last_src_id = mhd2.id
             AND mhd2.sure_scripts_med_history_id <> mhd1.sure_scripts_med_history_id
ORDER BY
    pm1.patient_id, pm1.ndc, pm1.id
;


-- SELECT count(distinct (pd.patient_id, measure_id))
DROP TABLE IF EXISTS _ones_to_clean_up;
CREATE TEMP TABLE _ones_to_clean_up AS
SELECT pd.*
FROM
    _pot_dupes pd
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pd.patient_id
        AND sp.is_medication_adherence
        AND sp.attribution_status = 'ma_at_risk_yes'
WHERE
      pd.is_exact_match
  AND pd.currently_compliant
  AND pd.adr - pd.days_supply2 > 0
ORDER BY pd.patient_id, pd.measure_id, pd.last_filled_date1
;

-- check if any dupes -- 0
SELECT first_patient_medication_id, count(*)
FROM
    _ones_to_clean_up cu
GROUP BY 1
having count(*) > 1

-- check if any chains -- 0
SELECT *
FROM
    _ones_to_clean_up cu where exists(select 1 from _ones_to_clean_up x where x.second_patient_medication_id = cu.first_patient_medication_id);

------------------------------------------------------------------------------------------------------------------------
/*
 current
    med hist detail (deduped by patient_id+ndc+start_date+days_supply)-> prd.patient_medications
    prd.patient_med_adherence_synth_periods is built using sproc and list of patient_ids. uses prd.patient_medications
 calculate prd.patient_med_adherence_measures using synth periods
 update prd.patient_med_adherence_measure_year
 */
------------------------------------------------------------------------------------------------------------------------





SELECT
    COUNT(*)
  , COUNT(DISTINCT (pd.patient_id, measure_id))                                   patient_measures
  , COUNT(DISTINCT (pd.patient_id, measure_id)) FILTER (WHERE currently_compliant) patient_measures_currently_compliant
FROM
    _pot_dupes pd
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pd.patient_id
        AND sp.is_medication_adherence
;
SELECT count(*) FROM prd.patient_med_adherence_year_measures; -- 103445 total
SELECT * FROM prd.patient_med_adherence_year_measures;

SELECT
--     pd.*
    COUNT(*)
  , COUNT(DISTINCT (pd.patient_id, measure_id))                                    patient_measures
  , COUNT(DISTINCT (pd.patient_id, measure_id)) FILTER (WHERE adr <= 0)            patient_measures_already_failed
  , COUNT(DISTINCT (pd.patient_id, measure_id)) FILTER (WHERE currently_compliant) patient_measures_currently_compliant
  , COUNT(DISTINCT (pd.patient_id, measure_id))
    FILTER (WHERE currently_compliant AND adr - pd.days_supply2 <= 0)              patient_measures_currently_compliant_likely_failed
  , COUNT(DISTINCT (pd.patient_id, measure_id))
    FILTER (WHERE currently_compliant AND adr - pd.days_supply2 > 0)               patient_measures_currently_compliant_adr_above_0
FROM
    _pot_dupes pd
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pd.patient_id
        AND sp.is_medication_adherence
        AND sp.attribution_status = 'ma_at_risk_yes'
where pd.is_exact_match
;



-- ORDER BY
--     pd.patient_id, pd.measure_id, pd.first_patient_medication_id
;

