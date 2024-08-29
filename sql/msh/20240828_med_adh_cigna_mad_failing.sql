SELECT * FROM fdw_member_doc.payers where name ~* 'cigna';

-- cigna failing diabetes
DROP TABLE IF EXISTS _failing;
CREATE TEMP TABLE _failing AS
WITH
    latest AS ( SELECT DISTINCT ON (patient_id) *
                FROM
                    fdw_member_doc_stage.qm_pm_med_adh_mco_measures m
                WHERE
                      payer_id = 40
                  AND measure_key = 'med_adherence_diabetes'
                ORDER BY patient_id, inserted_at DESC )
SELECT *
FROM
    latest
WHERE
    pdc < .8
    ;
DROP TABLE IF EXISTS _failing_latest_med;
CREATE TEMP TABLE _failing_latest_med AS
SELECT DISTINCT ON (f.patient_id)
    f.patient_id
  , f.measure_key
  , f.pdc
  , f.adr
  , f.inserted_at
  , pm.ndc
  , pm.drug_description
  , pm.last_filled_date
  , pm.days_supply
FROM
    _failing f
    LEFT JOIN fdw_member_doc.patient_medication_fills pm
              ON pm.patient_id = f.patient_id AND pm.measure_key = f.measure_key AND pm.last_filled_date >= '2024-01-01'
ORDER BY
    f.patient_id, pm.last_filled_date DESC
;

SELECT * FROM _failing_latest_med;
;
WITH
    glp1s AS ( SELECT DISTINCT
                   vs.code
               FROM
                   ref.med_adherence_value_sets vs
                   JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
               WHERE
                     measure_id = 'PDC-DR'
                 AND m.value_set_id = 'GIP_GLP1_RECEPTOR_AGONISTS'
                 AND vs.code_type = 'NDC' )
SELECT *
     , g.code IS NOT NULL is_glp1
FROM
    _failing_latest_med m
    LEFT JOIN glp1s g ON g.code = m.ndc
;

SELECT *
FROM
    ref.med_adherence_value_sets vs
join ref.med_adherence_measures m on m.value_set_id = vs.value_set_id
where measure_id = 'PDC-DR'
and m.value_set_id = 'GIP_GLP1_RECEPTOR_AGONISTS'
;

------------------------------------------------------------------------------------------------------------------------
/* all payers */
------------------------------------------------------------------------------------------------------------------------
-- cigna failing diabetes
DROP TABLE IF EXISTS _failing;
CREATE TEMP TABLE _failing AS
WITH
    latest AS ( SELECT DISTINCT ON (patient_id) *
                FROM
                    fdw_member_doc_stage.qm_pm_med_adh_mco_measures m
                WHERE
                  measure_key = 'med_adherence_diabetes'
                ORDER BY patient_id, inserted_at DESC )
SELECT *
FROM
    latest
WHERE
    pdc < .8
    ;
DROP TABLE IF EXISTS _failing_latest_med;
CREATE TEMP TABLE _failing_latest_med AS
SELECT DISTINCT ON (f.patient_id)
    f.patient_id
  , f.measure_key
  , f.pdc
  , f.adr
  , f.inserted_at
  , pm.ndc
  , pm.drug_description
  , pm.last_filled_date
  , pm.days_supply
FROM
    _failing f
    LEFT JOIN fdw_member_doc.patient_medication_fills pm
              ON pm.patient_id = f.patient_id AND pm.measure_key = f.measure_key AND pm.last_filled_date >= '2024-01-01'
ORDER BY
    f.patient_id, pm.last_filled_date DESC
;

SELECT * FROM _failing_latest_med;
;
WITH
    glp1s AS ( SELECT DISTINCT
                   vs.code
               FROM
                   ref.med_adherence_value_sets vs
                   JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
               WHERE
                     measure_id = 'PDC-DR'
                 AND m.value_set_id = 'GIP_GLP1_RECEPTOR_AGONISTS'
                 AND vs.code_type = 'NDC' )
SELECT *
     , g.code IS NOT NULL is_glp1
FROM
    _failing_latest_med m
    LEFT JOIN glp1s g ON g.code = m.ndc
;

