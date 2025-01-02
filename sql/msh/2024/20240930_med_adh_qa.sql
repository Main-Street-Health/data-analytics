SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
     , m.is_excluded
  , m.pdc_to_date
  , m.measure_source_key
     , pm.is_active
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 115
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT signal_date, inserted_at, updated_at, next_fill_date
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures where patient_id = 115 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#' order by signal_date desc;
SELECT * FROM qm_pm_med_adh_handoffs WHERE patient_id = 115 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#' order by id desc;
SELECT * FROM qm_patient_config WHERE patient_id = 115 AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';

SELECT max(inserted_at)
FROM
    qm_pm_med_adh_potential_fills;
SELECT * FROM qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;
------------------------------------------------------------------------------------------------------------------------
/* potential dupes */
------------------------------------------------------------------------------------------------------------------------
select * from qm_pm_med_adh_metrics where patient_measure_id =  1295262
SELECT *
FROM
    qm_patient_measures
WHERE
--       patient_id = 811463
    patient_id = 1273031
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#';

SELECT *
FROM
    patients p
where p.id = 811463
;

SELECT *
FROM
    patients p where first_name = 'Ricky' and last_name = 'Sartin';

-- # 2
-- metric trying to create a pqm but it already exists, pid is inactive
SELECT *
FROM
    qm_pm_med_adh_metrics
WHERE
    patient_measure_id = 644146;

-- has patient id 550667
SELECT * FROM qm_patient_measures where id = 644146;

-- searching by inactive pid + measure, pqm doesnt exist
SELECT *
FROM
    qm_patient_measures
WHERE
    patient_id = 968971 AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#';


SELECT *
FROM
    patients p
WHERE
      last_name LIKE 'Johnston'
  AND first_name LIKE 'Timothy'
  AND dob = '1956-10-29'::DATE;



-- active pid, pqm exists
SELECT *
FROM
    qm_patient_measures
WHERE
    patient_id = 550667 AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#' ;

-- new metric with active pid
SELECT *
FROM
    qm_pm_med_adh_metrics
WHERE
    patient_measure_id = 644146;

SELECT id, patient_id, patient_measure_id, inserted_at, updated_at
FROM
    qm_pm_med_adh_metrics
WHERE
      patient_id IN (968971, 550667)
  AND measure_key LIKE 'med#_adherence#_diabetes' ESCAPE '#' ;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT m.patient_id, m.measure_key, m.measure_year, m.measure_source_key, m.patient_measure_id, pm.id
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures pm
         ON pm.patient_id = m.patient_id
             AND pm.measure_key = m.measure_key
             AND pm.operational_year = m.measure_year
             AND m.patient_measure_id is distinct from pm.id
;

SELECT
    pm.patient_id
     , m.id
  , m.patient_id
  , m.measure_key
  , m.measure_year
  , m.measure_source_key
  , m.patient_measure_id
    , h.id
  , pm.id pqm_id
  , p.first_name
  , p.last_name
  , p.dob
  , p.status
  , p.substatus
  , p2.first_name
  , p2.last_name
  , p2.dob
  , p2.status
  , p2.substatus
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
        AND pm.patient_id != m.patient_id
    JOIN patients p ON m.patient_id = p.id
    JOIN patients p2 ON pm.patient_id = p2.id
left join qm_pm_med_adh_handoffs h on m.id = h.qm_pm_med_adh_metric_id and h.processed_at ISNULL
where p.status = 'hard_delete' and p.substatus = 'duplicate'
;

update
    qm_pm_med_adh_metrics
WHERE
    id IN ('4895', '9285592', '9285695', '56207608', '56463930', '56463931');

SELECT *
FROM
    qm_pm_med_adh_handoffs WHERE qm_pm_med_adh_metric_id IN ('4895', '9285592', '9285695', '56207608', '56463930', '56463931');
;



UPDATE qm_pm_med_adh_metrics
SET
    patient_measure_id = NULL, updated_at = NOW()
WHERE
    id IN ('39004139', '15274779', '39800665', '35779280', '35888718', '49832613', '35733184', '4991329', '8525045',
           '8525046', '8525047', '8525128', '102037');


------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 548348
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    patient_medication_fills where patient_id = 548348 AND drug_description ~* 'metformin'
------------------------------------------------------------------------------------------------------------------------
/* banu  */
------------------------------------------------------------------------------------------------------------------------
    SELECT
        m.patient_id
      , m.measure_key
      , m.patient_measure_id
      , pm.measure_status_key
      , m.next_fill_date
      , m.adr
      , m.pdc_to_date
      , m.measure_source_key
      , wf.id wf_id
      , wf.is_active
      , wf.is_closed
      , wf.is_reopened
      , wf.compliance_check_date
      , pf.id pf_id
      , pf.drug_description
      , pf.order_status
      , pf.medication_status
      , pf.pharmacy_verified_fill_date
      , pf.pharmacy_verified_days_supply
      , pf.system_verified_closed_at
      , pf.inserted_at
      , pf.updated_at
      , pt.status
      , pt.id
    FROM
        qm_patient_measures pm
        JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
        LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
        LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
        LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
        --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
    --         AND pmf.measure_key = pm.measure_key
    --         AND DATE_PART('year', pmf.start_date) = pm.operational_year
    --    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
    WHERE
        pm.patient_id =797009
--         pt.id =
        -- and pm.measure_key = 'med_adherence_cholesterol'
    ORDER BY pm.measure_key, pf.id
    ;

------------------------------------------------------------------------------------------------------------------------
/* banu afternoon 10/1 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
--     pm.patient_id =
    pt.id = 1917078
    -- and pm.measure_key = 'med_adherence_cholesterol'
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 76765
  AND measure_key LIKE 'med#_adherence#_cholesterol' ESCAPE '#'
order by signal_date desc
;

------------------------------------------------------------------------------------------------------------------------
/* codey 21 90 day thing */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    patient_medication_fills WHERE patient_id = 901170 and measure_key ~* 'hyp'
order by next_fill_date
;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
;
SELECT
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 1444200
    -- pt.id = 
    -- and pm.measure_key = 'med_adherence_cholesterol'
  AND pm.measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
ORDER BY pm.measure_key, pf.id
;
SELECT
    last_fill_date
  , next_fill_date
  , inserted_at
  , signal_date
, is_reversal
FROM
    stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 1444200
  AND measure_key LIKE 'med#_adherence#_hypertension' ESCAPE '#'
ORDER BY
    signal_date DESC
;
SELECT *
FROM
    qm_pm_med_adh_potential_fills
WHERE
    id = 247019;

SELECT *
FROM
    patient_tasks where id = 1930343;

