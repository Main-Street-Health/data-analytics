SELECT *
FROM
    sure_scripts_panels order by id desc;
9307

SELECT id, inserted_at, trl_processed_record_count, sure_scripts_panel_id
FROM
    sure_scripts_med_histories order by id desc;

DROP TABLE IF EXISTS _pat_meds_sp;
CREATE TEMP TABLE _pat_meds_sp AS
SELECT sp.id synth_period_id, patient_medication_ids
FROM
    prd.patient_med_adherence_synth_periods sp
join prd.patient_med_adherence_synth_period_batches spb on spb.id = sp.batch_id
where spb.sure_scripts_med_history_id in ( 9934,9901,9868,9835,9802,9736,9703,9472,9439 )
;

DROP TABLE IF EXISTS _missing;
CREATE TEMP TABLE _missing AS
SELECT *
FROM
    _pat_meds_sp pm_sp
where not exists(
    select 1
    from prd.patient_medications pm where pm.id = any(pm_sp.patient_medication_ids)
    )
;
select count(*) from _missing


create table junk.med_adh_synth_periods_deleted_from_coop_20240212 as
SELECT sp.*
FROM
    _missing m
    JOIN fdw_member_doc.qm_pm_med_adh_synth_periods sp ON sp.analytics_id = m.synth_period_id;

select * from junk.med_adh_synth_periods_deleted_from_coop_20240212 j where patient_id = 154830;
select count(*), count(distinct analytics_id) from junk.med_adh_synth_periods_deleted_from_coop_20240212 ;

delete from fdw_member_doc.qm_pm_med_adh_synth_periods coop
       using junk.med_adh_synth_periods_deleted_from_coop_20240212 j
where coop.analytics_id = j.analytics_id;

update fdw_member_doc.qm_pm_med_adh_synth_periods sp
set yr = date_part('year', sp.start_date)
where sp.yr ISNULL;

DROP TABLE IF EXISTS _no_longer_active_measures;
CREATE TEMP TABLE _no_longer_active_measures AS
SELECT qm.*
FROM
    fdw_member_doc.qm_pm_med_adh_metrics met
-- where patient_measure_id ISNULL
    JOIN fdw_member_doc.qm_patient_measures qm
         ON met.patient_measure_id = qm.id
WHERE
    NOT EXISTS( SELECT
                    1
                FROM
                    fdw_member_doc.qm_pm_med_adh_synth_periods sp
                WHERE
                      sp.patient_id = met.patient_id
                  AND sp.measure_key = met.measure_key
                  AND sp.yr = met.measure_year )
;
SELECT * FROM fdw_member_doc.qm_pm_med_adh_synth_periods where yr ISNULL ;

DELETE
FROM
    fdw_member_doc.qm_pm_med_adh_handoffs h
    USING _no_longer_active_measures nla
WHERE
      h.patient_id = nla.patient_id
  AND h.measure_key = nla.measure_key
  AND h.processed_at ISNULL;

SELECT
    pm.*
-- , m.*
-- , sp.*
, pf.*
FROM
    qm_patient_measures pm
        join qm_pm_med_adh_metrics m on pm.id = m.patient_measure_id
left join qm_pm_med_adh_synth_periods sp on sp.patient_id = pm.patient_id and sp.measure_key = pm.measure_key
join qm_pm_med_adh_potential_fills pf on pm.id = pf.patient_measure_id
WHERE
    pm.id = 344343
-- and sp.analytics_id ISNULL
;
SELECT is_medication_adherence
FROM
    supreme_pizza where patient_id = 67765;

;
SELECT *
FROM
    qm_pm_med_adh_handoffs
WHERE
      patient_id = 67765
  AND measure_key = 'med_adherence_cholesterol';
SELECT *
FROM
    md_portal_patients;

ALTER TABLE md_portal_patients
    DROP CONSTRAINT IF EXISTS md_portal_patients_patient_id_fkey;

SELECT *
FROM
    patient_medication_fills WHERE patient_id = 267618 and measure_key is not null
order by start_date desc
;

SELECT *
FROM
    patient_medication_fills WHERE patient_id = 11855 and measure_key is not null
order by start_date desc
;

SELECT *
FROM
    qm_patient_measures
--         qm_pm_med_adh_synth_periods
WHERE
    patient_id = 641472;

select pm.id, pm.measure_key, pt.task_type, pt.status, pm.is_active
from patient_tasks pt
         left join qm_pm_med_adh_potential_fills pf on pf.patient_task_id = pt.id
         left join qm_patient_measures pm on pm.patient_id = pt.patient_id and pt.task_type ~ pm.measure_key
left join public.qm_pm_med_adh_synth_periods sp ON pm.id = sp.patient_measure_id
where pt.task_type ~ 'med_adh'
  and pt.status in ('new', 'in_progress')
  and pt.task_type !~ '90'
  and pf.id is null
and sp.analytics_id ISNULL
;



      "ALTER TABLE factsheet_trading_account DROP CONSTRAINT factsheet_trading_account_trading_account_id_fkey"



