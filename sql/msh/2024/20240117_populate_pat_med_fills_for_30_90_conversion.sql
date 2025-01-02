DROP TABLE IF EXISTS _pat_meds;
CREATE TEMP TABLE _pat_meds AS
SELECT distinct
    pm.id                                                                     analytics_id
  , pm.patient_id
  , CASE WHEN m.measure_id = 'PDC-DR'   THEN 'med_adherence_diabetes'
         WHEN m.measure_id = 'PDC-STA'  THEN 'med_adherence_cholesterol'
         WHEN m.measure_id = 'PDC-RASA' THEN 'med_adherence_hypertension' END measure_key
  , pm.ndc
  , pm.drug_description
  , pm.start_date
  , pm.days_supply
  , pm.end_date
  , pm.quantity
  , pm.refills_remaining
  , pm.prescriber_name
  , pm.prescriber_npi
  , pm.prescriber_phone
  , pm.dispenser_type
  , pm.dispenser_name
  , pm.dispenser_npi
  , pm.dispenser_phone
  , pm.sold_date
  , pm.last_filled_date
  , pm.written_date
  , pm.src
  , pm.inserted_at                                                            received_at
  , NOW()                                                                     inserted_at
  , NOW()                                                                     updated_at
FROM
    prd.patient_medications pm
    JOIN ref.med_adherence_value_sets vs
         ON vs.code = pm.ndc
             AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
    JOIN ref.med_adherence_measures m
         ON m.value_set_id = vs.value_set_id
             AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
             AND m.is_med = 'Y'
             AND m.is_exclusion = 'N'
;

SELECT count(*) FROM _pat_meds;



INSERT
INTO fdw_member_doc.patient_medication_fills (analytics_id, patient_id, measure_key, ndc, drug_description, start_date, days_supply, end_date, quantity, refills_remaining, prescriber_name, prescriber_npi, prescriber_phone, dispenser_type, dispenser_name, dispenser_npi, dispenser_phone, sold_date, last_filled_date, written_date, src, received_at, inserted_at, updated_at)
SELECT
    analytics_id, patient_id, measure_key, ndc, drug_description, start_date, days_supply, end_date, quantity, refills_remaining, prescriber_name, prescriber_npi, prescriber_phone, dispenser_type, dispenser_name, dispenser_npi, dispenser_phone, sold_date, last_filled_date, written_date, src, received_at, inserted_at, updated_at
FROM _pat_meds;

-- member doc
SELECT *
FROM
    public.patient_medication_fills;
SELECT * FROM qm_ref_high_cost_meds;
INSERT
INTO
    qm_ref_high_cost_meds (med_name, measure_key, inserted_at, updated_at)
VALUES
('altoprev', 'med_adherence_cholesterol', now(), now()),
(      'crestor', 'med_adherence_cholesterol', now(), now()),
(      'ezallor sprinkle', 'med_adherence_cholesterol', now(), now()),
(      'lescol XL', 'med_adherence_cholesterol', now(), now()),
(      'lipitor', 'med_adherence_cholesterol', now(), now()),
(      'livalo', 'med_adherence_cholesterol', now(), now()),
(      'zocor', 'med_adherence_cholesterol', now(), now()),
(      'zypitamag', 'med_adherence_cholesterol', now(), now()),
(      'fluvastatin', 'med_adherence_cholesterol', now(), now()),
(      'bydureon bcise', 'med_adherence_diabetes', now(), now()),
(      'byetta', 'med_adherence_diabetes', now(), now()),
(      'farxiga', 'med_adherence_diabetes', now(), now()),
(      'glyxambi', 'med_adherence_diabetes', now(), now()),
(      'invokamet', 'med_adherence_diabetes', now(), now()),
(      'invokamet xr', 'med_adherence_diabetes', now(), now()),
(      'invokana', 'med_adherence_diabetes', now(), now()),
(      'janumet', 'med_adherence_diabetes', now(), now()),
(      'janumet xr', 'med_adherence_diabetes', now(), now()),
(      'januvia', 'med_adherence_diabetes', now(), now()),
(      'jardiance', 'med_adherence_diabetes', now(), now()),
(      'jentadueto', 'med_adherence_diabetes', now(), now()),
(      'jentadueto xr', 'med_adherence_diabetes', now(), now()),
(      'mounjaro', 'med_adherence_diabetes', now(), now()),
(      'onglyza', 'med_adherence_diabetes', now(), now()),
(      'ozempic', 'med_adherence_diabetes', now(), now()),
(      'rybelsus', 'med_adherence_diabetes', now(), now()),
(      'saxagliptin', 'med_adherence_diabetes', now(), now()),
(      'steglatro', 'med_adherence_diabetes', now(), now()),
(      'synjardy', 'med_adherence_diabetes', now(), now()),
(      'synjardy xr', 'med_adherence_diabetes', now(), now()),
(      'tradjenta', 'med_adherence_diabetes', now(), now()),
(      'trijardy xr', 'med_adherence_diabetes', now(), now()),
(      'trulicity', 'med_adherence_diabetes', now(), now()),
(      'victoza', 'med_adherence_diabetes', now(), now()),
(      'xigduo xr', 'med_adherence_diabetes', now(), now()),
(      'soliqua', 'med_adherence_diabetes', now(), now()),
(      'xultophy', 'med_adherence_diabetes', now(), now()),
(      'glumetza', 'med_adherence_diabetes', now(), now()),
(      'metformin ER (gastric or osmotic)', 'med_adherence_diabetes', now(), now()),
(      'riomet ER', 'med_adherence_diabetes', now(), now()),
(      'riomet', 'med_adherence_diabetes', now(), now()),
(      'alogliptin', 'med_adherence_diabetes', now(), now()),
(      'nesina', 'med_adherence_diabetes', now(), now()),
(      'Amaryl', 'med_adherence_diabetes', now(), now()),
(      'glucotrol XL', 'med_adherence_diabetes', now(), now()),
(      'glynase', 'med_adherence_diabetes', now(), now()),
(      'nateglinide', 'med_adherence_diabetes', now(), now()),
(      'repaglinide', 'med_adherence_diabetes', now(), now()),
(      'alogliptin-metformin', 'med_adherence_diabetes', now(), now()),
(      'Kazano', 'med_adherence_diabetes', now(), now()),
(      'Kombiglyze XR', 'med_adherence_diabetes', now(), now()),
(      'actos', 'med_adherence_diabetes', now(), now()),
(      'segluromet', 'med_adherence_diabetes', now(), now()),
(      'inpefa', 'med_adherence_diabetes', now(), now()),
(      'accuretic', 'med_adherence_hypertension', now(), now()),
(      'lotensin', 'med_adherence_hypertension', now(), now()),
(      'vaseretic', 'med_adherence_hypertension', now(), now()),
(      'zestoretic', 'med_adherence_hypertension', now(), now()),
(      'captopril-Hydrochlorothiazide', 'med_adherence_hypertension', now(), now()),
(      'coreg', 'med_adherence_hypertension', now(), now()),
(      'coreg CR', 'med_adherence_hypertension', now(), now()),
(      'atacand HCT', 'med_adherence_hypertension', now(), now()),
(      'avalide', 'med_adherence_hypertension', now(), now()),
(      'benicar HCT', 'med_adherence_hypertension', now(), now()),
(      'diovan HCT', 'med_adherence_hypertension', now(), now()),
(      'edarbyclor', 'med_adherence_hypertension', now(), now()),
(      'hyzaar', 'med_adherence_hypertension', now(), now()),
(      'micardis HCT', 'med_adherence_hypertension', now(), now()),
(      'telmisartan-Hydrochlorothiazide', 'med_adherence_hypertension', now(), now()),
(      'accupril', 'med_adherence_hypertension', now(), now()),
(      'altace', 'med_adherence_hypertension', now(), now()),
(      'epaned', 'med_adherence_hypertension', now(), now()),
(      'lotensin', 'med_adherence_hypertension', now(), now()),
(      'qbrelis', 'med_adherence_hypertension', now(), now()),
(      'vasotec', 'med_adherence_hypertension', now(), now()),
(      'zestril', 'med_adherence_hypertension', now(), now()),
(      'captopril', 'med_adherence_hypertension', now(), now()),
(      'atacand', 'med_adherence_hypertension', now(), now()),
(      'avapro', 'med_adherence_hypertension', now(), now()),
(      'benicar', 'med_adherence_hypertension', now(), now()),
(      'cozaar', 'med_adherence_hypertension', now(), now()),
(      'diovan', 'med_adherence_hypertension', now(), now()),
(      'edarbi', 'med_adherence_hypertension', now(), now()),
(      'micardis', 'med_adherence_hypertension', now(), now()),
(      'candesartan', 'med_adherence_hypertension', now(), now())
