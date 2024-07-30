SELECT * from public.qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pot_fill_id
  , pf.order_status
  , pf.medication_status
     , pf.system_verified_closed_at
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.inserted_at
  , pf.updated_at
  , pt.status
, pf.next_fill_date
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
WHERE
    pm.patient_id = 613340
;

SELECT *
FROM
    patient_medication_fills pf
WHERE
      patient_id = 613340
  AND measure_key = 'med_adherence_diabetes'
order by start_date


;
SELECT *
FROM
    stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 613340
  AND measure_key = 'med_adherence_diabetes'

;
-- fill 2/13 -> 3/13

SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
      patient_id = 613340
--   AND measure_key = 'med_adherence_diabetes'
;



SELECT *
FROM
    qm_pm_activities
WHERE
    patient_measure_id = 412193
ORDER BY
    id;

------------------------------------------------------------------------------------------------------------------------
/* check, all  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.*
FROM
    supreme_pizza sp
    JOIN qm_pm_med_adh_metrics m ON m.patient_id = sp.patient_id
WHERE
      sp.is_medication_adherence
  AND EXISTS( SELECT
                  1
              FROM
                  patient_medication_fills pmf
              WHERE
                    pmf.patient_id = m.patient_id
                AND pmf.measure_key = m.measure_key
                AND DATE_PART('year', pmf.start_date) = m.measure_year
                AND pmf.start_date + days_supply > m.next_fill_date );


SELECT *
FROM
    qm_pm_med_adh_metrics m

    JOIN qm_pm_med_adh_synth_periods sp ON m.patient_id = sp.patient_id
        AND sp.measure_key = m.measure_key
        AND sp.yr = m.measure_year
--     JOIN patient_medication_fills pmf ON m.patient_id = pmf.patient_id
--         AND pmf.measure_key = m.measure_key
--         AND DATE_PART('year', pmf.start_date) = m.measure_year
WHERE
    m.id = 5474982
;




------------------------------------------------------------------------------------------------------------------------
/* move to analytics */
------------------------------------------------------------------------------------------------------------------------
-- somehow not all synth periods were generated/saved

SELECT *
FROM
    sure_scripts_panel_patients sp
WHERE
      sp.patient_id = 613340
order by sp.id
;

SELECT *
FROM
    prd.patient_med_adherence_synth_periods sp
WHERE
      sp.patient_id = 613340
--   AND sp.measure_id = 'PDC-DR'
and batch_id = 11056
;

    ;
--   AND batch_id = 10858
-- | batch\_id |
-- | :--- |
-- | 10858 |
-- | 9472 |
-- | 10264 |
-- | 10297 |
-- | 10561 |

;
-- 2024-03-11 13:01:17.789810

SELECT *
FROM
    analytics.prd.patient_med_adherence_synth_period_batches
WHERE
    id IN (10858, 9472, 10264, 10297, 10561)
order by id
;


SELECT *
FROM
    prd.patient_medications pm
WHERE
      pm.patient_id = 613340
  AND pm.drug_description ~* 'metformin'
order by start_date + days_supply
;

-- med is listed as diabetes drug, re added 3/11/2024
SELECT
    m.*, vs.*
FROM
    analytics.ref.med_adherence_value_sets vs
    JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
WHERE
    vs.code = '23155084310'
and measure_version = '2024'
and measure_id = 'PDC-DR'
;



-- 2024-02-25 16:22:34.199794

-- is med adh
SELECT is_medication_adherence FROM fdw_member_doc.supreme_pizza WHERE patient_id = 613340;

-- no exclusion
SELECT * FROM analytics.prd.patient_med_adherence_exclusions WHERE patient_id = 613340


-- med adh data process overview
-- analytics
-- public.sp_populate_sure_scripts_panel_patients - we send file of patients to reveleer
-- etl.sp_med_adherence_load_surescripts_to_coop - dedupe, build synth periods, load return data from ss to coop
-- 
-- member doc db
-- public.qm_pm_med_adh_process()
-- oban job to run MD.QualityMeasures2.Workflows.MedAdhWorker.process

SELECT max(id)
FROM
    sure_scripts_panels p
SELECT *
FROM
    sure_scripts_med_history_details where sure_scripts_panel_id = 10396;
;

------------------------------------------------------------------------------------------------------------------------
/* from banu
    Pt Id : 675648
    This patient has 3 Cholesterol tasks -
    1) The patient picked up the med on 3/7 (I see this under patient medications), but the task got generated on 3/12
    2) for whatever reason the nav marked the first task that was generated on 3/12 as med discontinued (Asked the MM to check why) but then the same task was generated again on 3/13 and then on 3/14 but the 2nd and 3rd task isn't showing "task reopened flag"

    I am worried about this one. I am adding to the doc but this is the first one we may need to look into later.
*/
------------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT
--     m.patient_id
--   , m.patient_measure_id
--   , m.measure_key
--   , pm.measure_status_key
--   , m.next_fill_date
--   , wf.is_active
--   , wf.is_closed
--   , wf.is_reopened
--   , wf.compliance_check_date
--   , pf.id
--   , wf.id
--   , pf.next_fill_date
--   , pf.order_status
--   , pf.medication_status
--   , pf.pharmacy_verified_fill_date
--   , pf.pharmacy_verified_days_supply
--   , pf.inserted_at
--   , pf.updated_at
--   , pt.status
--    mco.*
pmf.*
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
            LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
        AND pmf.measure_key = pm.measure_key
        AND DATE_PART('year', pmf.start_date) = pm.operational_year
--     LEFT JOIN qm_pm_med_adh_synth_periods sp ON pm.id = sp.patient_measure_id
-- join stage.qm_pm_med_adh_mco_measures mco on mco.patient_id = pm.patient_id and mco.measure_key = pm.measure_key and mco.measure_year = pm.operational_year
WHERE
    pm.patient_id = 675648
-- ORDER BY
--     pf.id
;

+----------------------------------------------------------------------+-------------------+------------------------+
|description                                                           |inserted_at        |activity_source_type_key|
+----------------------------------------------------------------------+-------------------+------------------------+
|Patient Measure was created from an external source sure_scripts      |2024-02-28 13:43:51|external                |
|Pending Compliance Check: Fill date 2024-03-06 is within five days ago|2024-03-06 19:54:12|wf_worker               |
|Past Due - Pending Nav Action: Fill date 2024-03-06 is five days ago  |2024-03-12 13:06:49|wf_worker               |
|Lost - ADR>0: Medication Discontinued                                 |2024-03-13 12:24:34|task                    |
|Past Due - Pending Nav Action: Fill date 2024-03-06 is five days ago  |2024-03-13 12:48:11|wf_worker               |
|Lost - ADR>0: Medication Discontinued                                 |2024-03-13 14:48:48|task                    |
|Past Due - Pending Nav Action: Fill date 2024-03-06 is five days ago  |2024-03-14 14:17:49|wf_worker               |
|Pending Compliance Check: Patient says picked up meds                 |2024-03-15 13:48:26|task                    |
+----------------------------------------------------------------------+-------------------+------------------------+

select description, inserted_at, activity_source_type_key
from
    qm_pm_activities where patient_measure_id = 464037
order by id
;
select *
from
    qm_pm_status_periods where patient_measure_id = 464037
order by id
;
SELECT impact_date
FROM
    patient_quality_measures;

SELECT * FROM sure_scripts_pharmacies;

SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
      measure_key = 'med_adherence_cholesterol'
  AND patient_id = 675648;

SELECT *
FROM
    patient_medication_fills
WHERE
      measure_key = 'med_adherence_cholesterol'
  AND patient_id = 675648;

------------------------------------------------------------------------------------------------------------------------
/* swithc to analytics */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    prd.patient_med_adherence_synth_periods sp
    JOIN ref.med_adherence_measure_names mn ON sp.measure_id = mn.analytics_measure_id
WHERE
      mn.coop_measure_key = 'med_adherence_cholesterol'
  AND sp.patient_id = 675648
  AND sp.batch_id = 10858
;
SELECT *
FROM prd.patient_med_adherence_synth_period_batches WHERE id = 10858;
SELECT *
FROM
    sure_scripts_med_history_details
WHERE sure_scripts_med_history_id = 10957
  and patient_id = '675648'
  and drug_description ~* 'atorvastatin';


SELECT *
FROM
    prd.patient_medications pm
    JOIN ref.med_adherence_value_sets vs
         ON vs.code = pm.ndc AND pm.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
    JOIN ref.med_adherence_measures m ON m.value_set_id = vs.value_set_id
        AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
        AND measure_version = '2024'

    JOIN ref.med_adherence_measure_names mn ON m.measure_id = mn.analytics_measure_id
WHERE
      mn.coop_measure_key = 'med_adherence_cholesterol'
  AND pm.patient_id = 675648
  AND m.is_med = 'Y'
  AND m.is_exclusion = 'N'
ORDER BY
    pm.start_date
;



------------------------------------------------------------------------------------------------------------------------
/* fix synths */
------------------------------------------------------------------------------------------------------------------------
WITH
    inputs AS ( SELECT
                    start_date
                  , end_date
                  , patient_ids
                  , sure_scripts_med_history_id
                FROM
                    prd.patient_med_adherence_synth_period_batches b
                WHERE
                    b.inserted_at::DATE BETWEEN '2024-01-01' AND '2024-03-11' )
SELECT *
FROM
    inputs i
    CROSS JOIN prd.fn_build_med_adherence_synthetics(i.start_date, i.end_date, i.patient_ids,
                                                     i.sure_scripts_med_history_id) x
;

create table junk.patients_to_rebuild_synths_for as
SELECT
    distinct patient_id
--     start_date
--   , end_date
--   , patient_ids
--   , sure_scripts_med_history_id
-- , id batch_id
FROM
    prd.patient_med_adherence_synth_period_batches b
join prd.patient_med_adherence_synth_periods sp on sp.batch_id = b.id
WHERE
    b.inserted_at::DATE BETWEEN '2024-01-01' AND '2024-03-11';

create index on junk.patients_to_rebuild_synths_for(patient_id);
create index on prd.patient_med_adherence_synth_periods(patient_id);

drop    TABLE junk.patients_to_rebuild_synths_for_20240318;
CREATE TABLE junk.patients_to_rebuild_synths_for_20240318 AS
SELECT
    j.patient_id
  , MAX(sp.batch_id) latest_batch_id
FROM
    junk.patients_to_rebuild_synths_for j
    JOIN prd.patient_med_adherence_synth_periods sp ON j.patient_id = sp.patient_id
GROUP BY
    1
;
DROP TABLE IF EXISTS _latest_coop_batch;
CREATE TEMP TABLE _latest_coop_batch AS
SELECT patient_id, max(batch_id) latest_batch_id
FROM
    fdw_member_doc.qm_pm_med_adh_synth_periods
--     junk.patients_to_rebuild_synths_for_20240318 j
GROUP BY 1
;
DROP TABLE IF EXISTS _pats_to_replace;
CREATE TEMP TABLE _pats_to_replace AS
SELECT
--     count(*)
    lcb.patient_id, j.latest_batch_id new_batch_id
FROM
    _latest_coop_batch lcb
join junk.patients_to_rebuild_synths_for_20240318 j on j.patient_id = lcb.patient_id
where lcb.latest_batch_id < j.latest_batch_id

;


begin;
rollback;


    DELETE
    FROM
        fdw_member_doc.qm_pm_med_adh_synth_periods sp
    WHERE
          EXISTS( SELECT
                      1
                  FROM
                      _pats_to_replace cpm
                  WHERE
                      cpm.patient_id = sp.patient_id )
      AND DATE_PART('year', sp.start_date) = 2024;


    INSERT
    INTO
        fdw_member_doc.qm_pm_med_adh_synth_periods (analytics_id, patient_id, measure_key,
                                                    batch_id, fn_iteration, is_moved, join_key, days_supply, rn,
                                                    start_date, end_date, overlap_id, overlap_start_date,
                                                    overlap_end_date, value_set_item, og_start_date, og_end_date,
                                                    prev_start_date, prev_days_supply, patient_medication_ids, ndcs,
                                                    inserted_at, updated_at, yr)
    select
        id analytics_id,  sp.patient_id, coop_measure_key measure_key,
        batch_id, fn_iteration, is_moved, join_key, days_supply, rn,
        start_date, end_date, overlap_id, overlap_start_date,
        overlap_end_date, value_set_item, og_start_date, og_end_date,
        prev_start_date, prev_days_supply, patient_medication_ids, ndcs,
        sp.inserted_at, now(), date_part('year', sp.start_date)
    from prd.patient_med_adherence_synth_periods sp
    join ref.med_adherence_measure_names mamm on mamm.analytics_measure_id = sp.measure_id
    join _pats_to_replace ptr on sp.patient_id = ptr.patient_id and sp.batch_id = ptr.new_batch_id

end;
------------------------------------------------------------------------------------------------------------------------
/* MD DB
   Purge and recreate the handoffs not processed
*/
------------------------------------------------------------------------------------------------------------------------
DELETE
FROM
    qm_pm_med_adh_handoffs h
WHERE
    h.processed_at ISNULL;


call qm_pm_med_adh_process();
SELECT state, * FROM oban_jobs WHERE queue = 'qm_pm_med_adherence' ORDER BY id desc;

------------------------------------------------------------------------------------------------------------------------
/* From banu's word doc */
------------------------------------------------------------------------------------------------------------------------
SELECT distinct
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , pf.next_fill_date
     , pf.id
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
    , sp.*
--   , pf.pharmacy_verified_fill_date
--   , pf.pharmacy_verified_days_supply
--   , pf.system_verified_closed_at
--   , pf.inserted_at
--   , pf.updated_at
--   , pt.status
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
   left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 954948
and pm.measure_key = 'med_adherence_diabetes'
-- ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_potential_fills pf
where id = 16660
;
-- update nfd to metric nfd
UPDATE public.qm_pm_med_adh_potential_fills
SET
    next_fill_date = '2024-03-04'::DATE
WHERE
    id = 16660::BIGINT;

-- this updated 40 pot fills to bring nfd up to metric nfd
-- WITH
--     late      AS ( SELECT *
--                    FROM
--                        qm_pm_med_adh_metrics m
--                    WHERE
--                          m.next_fill_date < NOW() - '5 days'::INTERVAL
--                      AND m.measure_source_key = 'sure_scripts' )
--   , max_nfd   AS ( SELECT
--                        pf.patient_measure_id
--                      , MAX(pf.next_fill_date) nfd
--                    FROM
--                        qm_pm_med_adh_potential_fills pf
--                        JOIN late l ON pf.patient_measure_id = l.patient_measure_id
--                    GROUP BY 1 )
--   , new_dates AS ( SELECT
--                        m.next_fill_date new_next_fill_date
--                      , qpmapf.id        pf_id
--                    FROM
--                        qm_pm_med_adh_metrics m
--                        JOIN qm_patient_measures pm ON m.patient_measure_id = pm.id
--                        JOIN max_nfd mnfd ON mnfd.patient_measure_id = m.patient_measure_id
--                        JOIN qm_pm_med_adh_potential_fills qpmapf
--                             ON m.patient_measure_id = qpmapf.patient_measure_id AND qpmapf.next_fill_date = mnfd.nfd
--                                 AND NOT qpmapf.is_system_verified_closed
--                                 AND qpmapf.is_current
--                    WHERE
--                          m.next_fill_date > mnfd.nfd
--                      AND pm.is_active
--                      AND pm.measure_source_key = 'sure_scripts'
--                      AND NOT EXISTS( SELECT
--                                          1
--                                      FROM
--                                          qm_pm_med_adh_wfs wf
--                                      WHERE
--                                            wf.is_active
--                                        AND wf.patient_measure_id = m.patient_measure_id ) )
-- UPDATE qm_pm_med_adh_potential_fills pot
-- SET
--     next_fill_date = nd.new_next_fill_date, updated_at = NOW()
-- FROM
--     new_dates nd
-- WHERE
--     pot.id = nd.pf_id
-- ;

------------------------------------------------------------------------------------------------------------------------
/* #4 */
------------------------------------------------------------------------------------------------------------------------
SELECT c.med_adherences_start_date, m.*, pm.*, pt.*
FROM
    referring_partners rp
join msh_referring_partner_feature_config c on rp.id = c.referring_partner_id
join supreme_pizza sp on sp.primary_referring_partner_id = rp.id and sp.is_medication_adherence
join qm_pm_med_adh_metrics m on m.patient_id = sp.patient_id
join qm_patient_measures pm on m.patient_measure_id = pm.id
join qm_pm_med_adh_potential_fills qpmapf ON pm.id = qpmapf.patient_measure_id
left join patient_tasks pt on qpmapf.patient_task_id = pt.id
WHERE
--     rp.id = 1002
--     rp.id = 892
--     rp.id = 1212
    rp.id = 1473
;
------------------------------------------------------------------------------------------------------------------------
/* #5 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
     , m.id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
     , pf.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id =  953977
and pf.patient_task_id = 1030180
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_handoffs h
WHERE h.qm_pm_med_adh_metric_id = 3587446
order by id
;
SELECT *
FROM
    qm_pm_activities
WHERE
    patient_measure_id = 395002
ORDER BY
    id;

------------------------------------------------------------------------------------------------------------------------
/* #2 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
     , m.adr
  , pm.measure_status_key
     , pm.measure_source_key
  , m.next_fill_date
     , m.pdc_
     , pf.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
, pt.task_type
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
--     pm.patient_id =
pf.patient_task_id = 1027392
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_activities WHERE patient_measure_id = 347418 order by id;

------------------------------------------------------------------------------------------------------------------------
/* # 1 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
     , m.measure_source_key
  , pm.measure_status_key
  , pm.measure_source_key
  , m.next_fill_date
  , pf.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.inserted_at
  , pm.is_active
  , pt.status
, pm.measure_source_key
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
--     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
--     pm.patient_id =
pt.id = 1020145
ORDER BY
    pm.measure_key, pf.id
;
SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures WHERE patient_id = 239730 and measure_key ~* 'chol';
;
SELECT *
FROM
    qm_pm_activities WHERE patient_measure_id = 368399 ORDER BY id ;
SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      patient_id = 239730
  AND measure_key ~* 'chol'
ORDER BY
    id;;
SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 239730
  AND measure_key ~* 'chol'
ORDER BY
    patient_medication_fills.analytics_id;

------------------------------------------------------------------------------------------------------------------------
/* someone is lying

   ss task
   reopened
   pharmacy verified fill
   and pharmacy found
   pinged 5 days after


   task
   -> closed pat picked up
   -> reopened 5 days later
   -> select pharmacy verified filled
   */
------------------------------------------------------------------------------------------------------------------------
SELECT
    mapf.patient_task_id
  , qm.patient_measure_id
  , mapf.start_date
  , mapf.pharmacy_verified_fill_date
  , mapf.pharmacy_verified_fill_date + mapf.pharmacy_verified_days_supply + 7 AS pharmacy_verified_plus_7
  , CASE
        WHEN mapf.pharmacy_verified_fill_date + 7 BETWEEN start_at AND end_at
            THEN 'Audit Task'
        ELSE 'No Audit' END                                                   AS Flag
  , qm.measure_status_key
  , qm.start_at
  , qm.end_at
FROM
    qm_pm_med_adh_potential_fills mapf
    LEFT JOIN qm_pm_status_periods qm ON mapf.patient_measure_id = qm.patient_measure_id
WHERE
      measure_status_key IN ('past_due_pending_navigator')
  and not mapf.pharmacy_not_found
  AND mapf.pharmacy_verified_fill_date IS NOT NULL;



SELECT *
FROM
    qm_pm_med_adh_potential_fills pf
join qm_pm_med_adh_wfs wf on pf.qm_pm_med_adh_wf_id = wf.id

;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    referring_partners WHERE name ~* 'Troy';
SELECT
    COUNT(DISTINCT sp.patient_id)                                        nd_pats
  , COUNT(DISTINCT sp.patient_id) FILTER (WHERE is_medication_adherence) nd_pats_w_med_adh_flag
  , COUNT(DISTINCT sp.patient_id) FILTER (WHERE m.id IS NOT NULL)        nd_pats_med_adh_measure
FROM
    supreme_pizza sp
    LEFT JOIN qm_pm_med_adh_metrics m ON m.patient_id = sp.patient_id
WHERE
    primary_referring_partner_id = 125




------------------------------------------------------------------------------------------------------------------------
/*from david 6386 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.inserted_at
  , pf.is_current
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.updated_at
  , pt.status
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
--     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
      pm.patient_id = 6386
  AND pm.measure_key = 'med_adherence_cholesterol'
ORDER BY
    pm.measure_key, pf.id
;
SELECT
   activity_key
  , patient_measure_source_key
  , activity_source_type_key
  , description
  , inserted_at
  , status_period_id
  , activity_at
  , patient_task_id
  , patient_measure_id
  , msh_cca_worksheet_id
  , activity_by_id
  , provider_external_order_worksheet_id
  , patient_procedure_code_id
  , falloff_status
  , patient_appointment_date
  , is_no_show
  , processed_at
  , updated_at
FROM
    qm_pm_activities
WHERE
    patient_measure_id = 359999
ORDER BY
    id
;

;
------------------------------------------------------------------------------------------------------------------------
/* 3/25 */
------------------------------------------------------------------------------------------------------------------------
    SELECT
        m.patient_id
      , m.patient_measure_id
      , m.measure_key
      , pm.measure_status_key
      , m.next_fill_date
      , wf.id wf_id
      , wf.is_active
      , wf.is_closed
      , wf.is_reopened
      , wf.compliance_check_date
      , pf.id pf_id
      , pf.order_status
      , pf.medication_status
      , pf.pharmacy_verified_fill_date
      , pf.pharmacy_verified_days_supply
      , pf.system_verified_closed_at
      , pf.inserted_at
      , pf.updated_at
      , pt.status
    FROM
        qm_patient_measures pm
        JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
        LEFT JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
        LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
        LEFT JOIN patient_tasks pt ON pf.patient_task_id = pt.id
        --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
    --         AND pmf.measure_key = pm.measure_key
    --         AND DATE_PART('year', pmf.start_date) = pm.operational_year
    --    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
    WHERE
--         pt.status in ('new', 'in_progress')
        pt.id = 1035297
    ORDER BY pm.measure_key, pf.id
    ;

SELECT *
FROM
    qm_pm_activities
WHERE
    patient_measure_id = 453485
ORDER BY
    id;


DELETE
FROM
    qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;
-- call qm_pm_med_adh_process();
-- UPDATE public.oban_jobs
-- SET
--                                   max_attempts = max_attempts + 1
--                                  , scheduled_at = now()
--                                  , discarded_at = null,state = 'available'
-- WHERE
--     id = 2902774;

SELECT state, errors[attempt]
FROM
    oban_jobs
where id = 2902774
;

SELECT count(*)
FROM
    qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;


-- SELECT *
-- delete FROM
--     qm_pm_med_adh_handoffs
-- WHERE
--       patient_id = 399455
--   AND measure_key = 'med_adherence_diabetes'
--   AND processed_at ISNULL ;

SELECT *
FROM
--     qm_pm_med_adh_synth_periods
        patient_medication_fills
where
      patient_id = 399455
  AND DATE_PART('year', start_date) = 2024
order by start_date
--   AND measure_key = 'med_adherence_diabetes'
;

-- synth period without fill?
SELECT *
FROM
    qm_pm_med_adh_synth_periods sp
WHERE
    NOT EXISTS( SELECT 1
                FROM patient_medication_fills f
                WHERE f.patient_id = sp.patient_id AND f.measure_key = sp.measure_key )
;



DROP TABLE IF EXISTS _wfs;
CREATE TEMP TABLE _wfs AS 
SELECT
    m.patient_id
  , m.patient_measure_id
  , COUNT(*)
  , MAX(wf.id) keep_id
  , MIN(wf.id) inactive_id
FROM
    qm_patient_measures pm
    JOIN qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    JOIN qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id AND wf.is_active
GROUP BY
    1, 2
HAVING
    COUNT(*) > 1

update qm_pm_med_adh_wfs wf
set is_active = false, updated_at = now()
from _wfs w
where w.inactive_id = wf.id

UPDATE patient_tasks pt
SET
    status = 'in_progress', updated_at = NOW()
FROM
    _wfs w
    JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = w.keep_id
WHERE
      pt.status = 'closed'
  AND pt.id = pf.patient_task_id
;
------------------------------------------------------------------------------------------------------------------------
/* oban issue
   UPDATE public.oban_jobs
SET
    max_attempts = max_attempts + 1
  , scheduled_at = NOW()
  , discarded_at = NULL, state = 'available'
WHERE
    id = 2911164;

   */
------------------------------------------------------------------------------------------------------------------------
SELECT errors[oban_jobs.attempt], state, *
FROM
    oban_jobs
where id = 2911164
order by id desc;
;
SELECT count(*)
FROM
    patient_tasks WHERE inserted_at > now() - '2 hours'::interval and task_type ~* 'med_adh';
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.id
  , pt.status
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
    pm.id = 405594
and wf.is_active
-- and pf.is_current
ORDER BY pm.measure_key, pf.id
;

SELECT *
FROM
    qm_pm_med_adh_wfs wf
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
WHERE wf.patient_measure_id = 405594;
;

SELECT patient_measure_id
FROM
    qm_pm_med_adh_wfs wf
where wf.is_active
GROUP BY 1
having count(*) > 1
;
update  qm_pm_med_adh_wfs wf
set is_active = false, updated_at = now()
from
    ( SELECT
          patient_measure_id
        , MIN(id) to_inactivate
      FROM
          qm_pm_med_adh_wfs
      WHERE
          patient_measure_id IN (
                                 432869,
                                 406408,
                                 444595,
                                 405594
              )
      GROUP BY 1
       ) x
where x.patient_measure_id = wf.patient_measure_id
and x.to_inactivate = wf.id
;



SELECT
    q0."id"
  , q0."compliance_check_date"
  , q0."is_closed"
  , q0."closed_at"
  , q0."is_system_verified_closed"
  , q0."system_verified_closed_at"
  , q0."is_reopened"
  , q0."reopened_reason"
  , q0."has_side_effects"
  , q0."is_active"
  , q0."deleted_at"
  , q0."closed_by_id"
  , q0."deleted_by_id"
  , q0."updated_by_id"
  , q0."patient_measure_id"
  , q0."measure_key"
  , q0."qm_pm_med_adh_metric_id"
  , q0."inserted_at"
  , q0."updated_at"
  , q1."id"
  , q1."must_close_by_date"
  , q1."msh_measure_open_date"
  , q1."is_star_reporting_year"
  , q1."is_stale"
  , q1."is_stale_why"
  , q1."is_stale_why_tech"
  , q1."is_stale_at"
  , q1."is_research_eligible"
  , q1."is_worksheet_eligible"
  , q1."is_active"
  , q1."apt_eligible_start_date"
  , q1."operational_year"
  , q1."is_callable"
  , q1."notes"
  , q1."measure_status_key"
  , q1."measure_source_key"
  , q1."patient_id"
  , q1."nav_next_step_key"
  , q1."measure_key"
  , q1."fall_off_status_key"
  , q1."mco_patient_measure_id"
  , q1."updated_by_id"
  , q1."inserted_at"
  , q1."updated_at"
  , q2."id"
  , q2."measure_year"
  , q2."fill_count"
  , q2."ipsd"
  , q2."next_fill_date"
  , q2."days_covered_to_period_end"
  , q2."days_not_covered"
  , q2."absolute_fail_date"
  , q2."calc_to_date"
  , q2."pdc_to_date"
  , q2."adr"
  , q2."failed_last_year"
  , q2."is_on_90_day_supply"
  , q2."has_side_effects"
  , q2."is_excluded"
  , q2."patient_id"
  , q2."measure_source_key"
  , q2."measure_key"
  , q2."patient_measure_id"
  , q2."inserted_at"
  , q2."updated_at"
FROM
    "qm_pm_med_adh_wfs" AS q0
    INNER JOIN "qm_patient_measures" AS q1 ON q1."id" = q0."patient_measure_id"
    INNER JOIN "qm_pm_med_adh_metrics" AS q2 ON q2."id" = q0."qm_pm_med_adh_metric_id"
WHERE
      (q0."is_active")
  AND (q1."is_active")
  AND (q1."id" = 405594);


-- issue is open tasks associated to non current pf
SELECT *
FROM
    patient_tasks pt
join qm_pm_med_adh_potential_fills qpmapf ON pt.id = qpmapf.patient_task_id
where not qpmapf.is_current and pt.status in ('new', 'in_progress')
;
    update patient_tasks pt
        set status = 'closed'
from qm_pm_med_adh_potential_fills qpmapf
where not qpmapf.is_current and pt.status in ('new', 'in_progress')
    and pt.id = qpmapf.patient_task_id
;

SELECT *
FROM
    qm_pm_med_adh_potential_fills pf
join qm_pm_med_adh_wfs wf on pf.qm_pm_med_adh_wf_id = wf.id and wf.is_active
where pf.patient_measure_id in ( SELECT
                                  pf.patient_measure_id
                              --   , COUNT(*)
-- , count(distinct pf.id)
--   , array_agg(distinct pt.task_type)
                              FROM
                                  qm_pm_med_adh_potential_fills pf
                                  JOIN patient_tasks pt
                                       ON pf.patient_task_id = pt.id AND pt.status IN ('new', 'in_progress')
                              where pf.is_current
                              GROUP BY
                                  1
                              HAVING
                                  COUNT(*) > 1 )
order by pf.patient_measure_id, pf.qm_pm_med_adh_wf_id, pf.patient_task_id

;


;
SELECT
    patient_measure_id
  , COUNT(*)
FROM
    qm_pm_med_adh_wfs
WHERE
    is_active
GROUP BY 1 having count(*) > 1
;
CREATE UNIQUE INDEX ON qm_pm_med_adh_wfs(patient_measure_id) WHERE is_active;

SELECT *
FROM
    qm_patient_measures
WHERE
    id = 344559;

SELECT *
FROM
    qm_pm_med_adh_wfs wf
-- join qm_pm_med_adh_potential_fills qpmapf ON wf.id = qpmapf.qm_pm_med_adh_wf_id
WHERE
    wf.patient_measure_id = 344559;

SELECT * FROM qm_pm_med_adh_handoffs WHERE patient_id = 321414 and measure_key ~* 'chol' order by id;
SELECT * FROM qm_pm_med_adh_synth_periods WHERE patient_id = 321414 and measure_key ~* 'chol' order by start_date;
SELECT * FROM qm_pm_med_adh_exclusions WHERE patient_id = 321414;

SELECT activity_key, patient_measure_source_key, activity_source_type_key, description, inserted_at
    from
    qm_pm_activities
WHERE
    patient_measure_id = 344559 order by id;

SELECT *
FROM
    qm_pm_med_adh_metrics WHERE patient_measure_id = 344559;
SELECT notes FROM qm_patient_measures;

------------------------------------------------------------------------------------------------------------------------
/* 847391 */
------------------------------------------------------------------------------------------------------------------------

SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
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
--     pm.patient_id = 847391
pt.id = 1035006
ORDER BY pm.measure_key, pf.id
;;

SELECT measure_status_key, count(*)
FROM
    qm_patient_measures WHERE  measure_key ~* 'med_adh'
GROUP BY 1 order by 2
;
SELECT r.inserted_at sent_at, r.file_name
FROM
    md_portal_roster_patients p
join md_portal_rosters r on p.md_portal_roster_id = r.id
WHERE
    patient_id = 12576
order by r.inserted_at
;

    SELECT
        m.patient_id
      , m.patient_measure_id
      , m.measure_key
      , pm.measure_status_key
      , m.next_fill_date
      , wf.id wf_id
      , wf.is_active
      , wf.is_closed
      , wf.is_reopened
      , wf.compliance_check_date
      , pf.id pf_id
      , pf.order_status
      , pf.medication_status
      , pf.pharmacy_verified_fill_date
      , pf.pharmacy_verified_days_supply
      , pf.system_verified_closed_at
      , pf.inserted_at
      , pf.updated_at
      , pt.status
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
        pm.patient_id = 194078
    ORDER BY pm.measure_key, pf.id
    ;;
SELECT *
FROM
    qm_pm_med_adh_exclusions WHERE patient_id =         194078
;
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , m.fill_count
     , m.measure_source_key
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
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
    pm.patient_id = 345365
ORDER BY
    pm.measure_key, pf.id
;
SELECT *
FROM
    qm_pm_med_adh_exclusions
WHERE
    patient_id = 194078;
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
     , pm.is_active
  , pm.measure_status_key
  , m.next_fill_date
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
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
    pm.patient_id =     194078

ORDER BY pm.measure_key, pf.id
;

SELECT *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
WHERE
      patient_id = 59214
  AND measure_key ~* 'dia';

SELECT *
FROM
    qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;
SELECT *
FROM
    oban_jobs
WHERE
    queue ~* 'med_adh'
ORDER BY
    id;
SELECT *
FROM
    payers p ;

SELECT count(*)
FROM
    patients WHERE payer_id = 47;
------------------------------------------------------------------------------------------------------------------------
/* 3/29 */
------------------------------------------------------------------------------------------------------------------------
-- DELETE
-- FROM
--     qm_pm_med_adh_handoffs h
-- WHERE
--     h.processed_at ISNULL;
--
-- call qm_pm_med_adh_process();
-- SELECT state, * FROM oban_jobs WHERE queue = 'qm_pm_med_adherence' ORDER BY id desc;
SELECT state, errors[oban_jobs.attempt], *
FROM oban_jobs WHERE id = 2919509
    ;
   UPDATE public.oban_jobs
SET
    max_attempts = max_attempts + 1
  , scheduled_at = NOW()
  , discarded_at = NULL, state = 'available'
WHERE
    id = 2919509;

--
-- update qm_pm_med_adh_potential_fills
-- set order_status = case
--                        when medication_status = 'prn' then 'med_prn'
--                        when medication_status = 'discontinued' then 'med_discontinued'
--                        when order_status = 'prn_medication' then 'med_prn'
--                        when order_status = 'medication_discontinued' then 'med_discontinued' end
select * from  qm_pm_med_adh_potential_fills
where order_status in ('prn_medication', 'medication_discontinued')
   or medication_status in ('prn', 'discontinued');

SELECT *
FROM
    qm_pm_med_adh_metrics WHERE fill_count ISNULL ;
SELECT * FROM postgis_version();
SELECT postgis_extensions_upgrade();

SELECT
    COUNT(*)
FROM
    qm_pm_med_adh_metrics m
left join qm_pm_med_adh_exclusions e on e.patient_id = m.patient_id and e.measure_key = m.measure_key
WHERE
    is_excluded
and e.analytics_id ISNULL
;

SELECT
    count(*)
FROM
    qm_pm_med_adh_exclusions e
    LEFT JOIN qm_pm_med_adh_metrics m ON e.patient_id = m.patient_id AND e.measure_key = m.measure_key --AND is_excluded
WHERE
    m.id ISNULL
;

SELECT *
FROM qm_pm_med_adh_metrics m
join qm_patient_measures qpm ON m.patient_measure_id = qpm.id
where is_excluded
and qpm.is_active
    ;
SELECT *
FROM
    qm_ref_patient_measure_statuses;

SELECT *
FROM
    oban_jobs where id = 2929951;

SELECT qpm.measure_status_key, count(*)
FROM
    qm_pm_med_adh_metrics m
join qm_patient_measures qpm ON m.patient_measure_id = qpm.id
where m.is_excluded
-- and qpm.is_active
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
/* wf unique constraint error */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM qm_pm_med_adh_wfs WHERE patient_measure_id = 422350;
SELECT * FROM qm_pm_activities WHERE patient_measure_id = 422350 order by id;
SELECT * FROM qm_patient_measures WHERE id = 422350 order by id;
SELECT * FROM qm_pm_med_adh_metrics WHERE measure_key = 'med_adherence_hypertension' and patient_id = 70764;
SELECT * FROM qm_pm_med_adh_synth_periods WHERE measure_key = 'med_adherence_hypertension' and patient_id = 70764;
SELECT * FROM qm_pm_med_adh_handoffs WHERE measure_key = 'med_adherence_hypertension' and patient_id = 70764;

DELETE
FROM
    qm_pm_med_adh_handoffs h
WHERE
    h.processed_at ISNULL;

call qm_pm_med_adh_process();


SELECT
    state
  , *
FROM
    oban_jobs
WHERE
      queue ~* 'med_adh'
  AND id = 2946430
ORDER BY
    id DESC
;

SELECT * FROM qm_pm_med_adh_wfs WHERE patient_measure_id = 422350;
SELECT * FROM qm_pm_activities WHERE patient_measure_id = 422350 order by id;
SELECT * FROM qm_patient_measures WHERE id = 422350 order by id;
SELECT * FROM qm_pm_med_adh_metrics WHERE measure_key = 'med_adherence_hypertension' and patient_id = 70764;
SELECT * FROM qm_pm_med_adh_synth_periods WHERE measure_key = 'med_adherence_hypertension' and patient_id = 70764;
SELECT * FROM qm_pm_med_adh_handoffs WHERE measure_key = 'med_adherence_hypertension' and patient_id = 70764;

SELECT *
FROM
    qm_pm_med_adh_wfs wf
where wf.is_active
and exists(select 1 from qm_patient_measures pm where pm.id = wf.patient_measure_id and not pm.is_active)
;
SELECT *
FROM
    qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;