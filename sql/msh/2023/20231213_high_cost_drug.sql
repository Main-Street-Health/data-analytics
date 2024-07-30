DROP TABLE IF EXISTS _high_cost_drugs;
CREATE TEMP TABLE _high_cost_drugs AS
SELECT lower(drug_name) drug_name
FROM
    ( VALUES
          ('altoprev'),
          ('crestor'),
          ('ezallor sprinkle'),
          ('lescol XL'),
          ('lipitor'),
          ('livalo'),
          ('zocor'),
          ('zypitamag'),
          ('fluvastatin'),
          ('bydureon bcise'),
          ('byetta'),
          ('farxiga'),
          ('glyxambi'),
          ('invokamet'),
          ('invokamet xr'),
          ('invokana'),
          ('janumet'),
          ('janumet xr'),
          ('januvia'),
          ('jardiance'),
          ('jentadueto'),
          ('jentadueto xr'),
          ('mounjaro'),
          ('onglyza'),
          ('ozempic'),
          ('rybelsus'),
          ('saxagliptin'),
          ('steglatro'),
          ('synjardy'),
          ('synjardy xr'),
          ('tradjenta'),
          ('trijardy xr'),
          ('trulicity'),
          ('victoza'),
          ('xigduo xr'),
          ('soliqua'),
          ('xultophy'),
          ('glumetza'),
          ('metformin ER (gastric or osmotic)'),
          ('riomet ER'),
          ('riomet'),
          ('alogliptin'),
          ('nesina'),
          ('Amaryl'),
          ('glucotrol XL'),
          ('glynase'),
          ('nateglinide'),
          ('repaglinide'),
          ('alogliptin-metformin'),
          ('Kazano'),
          ('Kombiglyze XR'),
          ('actos'),
          ('segluromet'),
          ('inpefa'),
          ('accuretic'),
          ('lotensin'),
          ('vaseretic'),
          ('zestoretic'),
          ('captopril-Hydrochlorothiazide'),
          ('coreg'),
          ('coreg CR'),
          ('atacand HCT'),
          ('avalide'),
          ('benicar HCT'),
          ('diovan HCT'),
          ('edarbyclor'),
          ('hyzaar'),
          ('micardis HCT'),
          ('telmisartan-Hydrochlorothiazide'),
          ('accupril'),
          ('altace'),
          ('epaned'),
          ('lotensin'),
          ('qbrelis'),
          ('vasotec'),
          ('zestril'),
          ('captopril'),
          ('atacand'),
          ('avapro'),
          ('benicar'),
          ('cozaar'),
          ('diovan'),
          ('edarbi'),
          ('micardis'),
          ('candesartan') ) x(drug_name);


DROP TABLE IF EXISTS _patient_fills;
CREATE TEMP TABLE _patient_fills AS
SELECT distinct on (patient_id, ndc)
    patient_id
  , ndc
  , lower(drug_description) drug_description
  , vs.value_set_item
  , m.measure_id
  , last_filled_date
  , days_supply
FROM
    prd.patient_medications mhd
    JOIN ref.med_adherence_value_sets vs
         ON vs.code = mhd.ndc AND mhd.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
    JOIN ref.med_adherence_measures m
         ON m.value_set_id = vs.value_set_id AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
             AND is_med = 'Y'
             AND is_exclusion = 'N'
order by patient_id, ndc, last_filled_date desc
;
SELECT
    *
FROM
    _patient_fills pf
;
DROP TABLE IF EXISTS patient_fills_w_high_cost;
CREATE TEMP TABLE patient_fills_w_high_cost AS
SELECT
    patient_id
  , ndc
  , drug_description
  , value_set_item
  , measure_id
  , last_filled_date
  , days_supply
  , drug_name
  , hcd.drug_name IS NOT NULL is_high_cost
FROM
    _patient_fills pf
    LEFT JOIN _high_cost_drugs hcd ON pf.drug_description ~ hcd.drug_name
GROUP BY
    patient_id, ndc, drug_description, value_set_item, measure_id, last_filled_date, days_supply, drug_name
;

-- should be 0
SELECT
    patient_id
  , ndc
  , drug_description
  , value_set_item
  , measure_id
  , last_filled_date
  , days_supply
  , drug_name
  , COUNT(*)
FROM
    patient_fills_w_high_cost
GROUP BY
    patient_id
  , ndc
  , drug_description
  , value_set_item
  , measure_id
  , last_filled_date
  , days_supply
  , drug_name
HAVING
    COUNT(*) > 1
;
SELECT
    pf.patient_id
  , pf.ndc
  , pf.drug_description
  , pf.value_set_item
  , pf.measure_id
  , pf.last_filled_date
  , pf.days_supply
  , pf.drug_name
  , pf.is_high_cost
  , sp.primary_physician_id
  , mp.full_name
  , mp.npi
  , rp.name
  , sp.primary_referring_partner_id
FROM
    patient_fills_w_high_cost pf
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pf.patient_id
    JOIN fdw_member_doc.msh_physicians mp ON mp.id = sp.primary_physician_id
    JOIN fdw_member_doc.referring_partners rp ON rp.id = sp.primary_referring_partner_id
;



