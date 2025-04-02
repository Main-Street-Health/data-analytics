------------------------------------------------------------------------------------------------------------------------
/* from sean */
------------------------------------------------------------------------------------------------------------------------

drop table if exists _raw_alr;
create temp table _raw_alr as
select * from raw.mssp_alr_1_1 where mssp_alr_parameter_id = 62;

drop table if exists _coop_mbis;
create temp table _coop_mbis as
select distinct on (mbi.patient_id) mbi.*
    from fdw_member_doc.patient_mbi_and_medicare_dates mbi
    join fdw_member_doc.supreme_pizza sp on sp.patient_id = mbi.patient_id
order by mbi.patient_id, mbi.id desc;
create unique index udx__coop_mbis on _coop_mbis(patient_id);

drop table if exists _pats;
create temp table _pats as
with cte as (
select
    x.inbound_file_id,
    x.id raw_id,
    coalesce(gm.patient_id, mb.patient_id) patient_id,
    x.bene_mbi_id mbi,
    x.payer_id,
    smm.member_id,
    ra.bene_mbi_id is not null is_on_alr,
    substring(x.meta->'extra_args'->>'original_name' from 'A\d{4}') contract_id,
    (substring(x.meta->'extra_args'->>'original_name' from '.*(\d{4})Q'))::int measure_year,
    coalesce(htn_age = '1' and htn_dx = '1' and htn_encounter = '1' and htn_exclusion = '.',false) is_cbp,
    coalesce(dep_age = '1' and dep_encounter = '1' and dep_exclusion = '.',false) is_depression,
    coalesce(dm_age = '1' and dm_dx = '1' and dm_encounter = '1' and dm_exclusion = '.',false) is_hbd

from
    raw.mssp_alr_mcqm x
        left join prd.source_member_mappings smm on smm.payer_member_id = lower(trim(x.bene_mbi_id))
            and smm.is_mbi
            and smm.payer_id = 81
        left join gmm.global_mco_member_mappings mco on mco.member_id = smm.member_id
        left join gmm.global_members gm on gm.id = mco.global_member_id and gm.patient_id is not null
        left join _raw_alr ra on ra.bene_mbi_id = x.bene_mbi_id
        left join _coop_mbis mb on mb.mbi = x.bene_mbi_id
where
    x.inbound_file_id >= 20936004 /*2024Q4*/), filt as (
select *
from cte
where is_cbp or is_depression or is_hbd)
select
    --distinct on (mbi, measure_key)
    raw_id,
    inbound_file_id,
    patient_id,
    measure_year,
    mbi,
    payer_id,
    member_id,
    contract_id,
    measure_key,
    'proxy' measure_source_key,
    'pending_scheduling_clinic' measure_status_key,
    is_on_alr
from filt
        cross join lateral ( values
            ('cbp_controlling_high_blood_pressure', is_cbp),
            ('hbd_hemoglobin_a1c_control_for_patients_with_diabetes', is_hbd),
            ('dsf_depression', is_depression)
            ) dx (measure_key, is_active)
where
    is_active
order by mbi, measure_key, contract_id = 'A5404' desc, contract_id
;

/*select
    contract_id,
    measure_key,
    count(*) rec_count,
    count(member_id) member_ids,
    count(patient_id) patient_ids,
    count(*) filter ( where is_on_alr ) on_q4_alr,
    count(*) filter ( where patient_id is not null or is_on_alr ) patient_id_or_on_alr
from _pats
group by 1,2 order by 1,2;*/

drop table if exists _qms;
create temp table _qms as
select * from fdw_member_doc.qm_patient_measures where operational_year = 2024;

drop table if exists _chase;
create temp table _chase as
select distinct unnest(qm_patient_measure_ids) qm_patient_measure_id, external_chase_id, id msh_chase_id
from public.reveleer_chases;
create unique index udx__chase on _chase(qm_patient_measure_id, external_chase_id);

drop table if exists _unpivoted;
create temp table _unpivoted as
select
    p.raw_id,
    p.inbound_file_id,
    p.patient_id,
    p.measure_year,
    p.mbi,
    p.payer_id,
    p.member_id,
    p.contract_id,
    p.measure_key,
    qpm.id qm_patient_measure_id,
    qpm.is_active,
    qpm.inserted_at qm_ins_at,
    ch.external_chase_id rev_chase_id -- NEED THIS
, ch.msh_chase_id
from _pats p
    left join _qms qpm on qpm.patient_id = p.patient_id
        and qpm.measure_key = p.measure_key
    left join _chase ch on ch.qm_patient_measure_id = qpm.id
;

drop table if exists _pivoted;
create temp table _pivoted as
select
    patient_id,
    mbi,
    payer_id,
    member_id,
    contract_id,
    bool_or(measure_key ~* 'cbp') has_cbp,
    max(qm_patient_measure_id) filter ( where measure_key ~* 'cbp' ) cbp_qm_patient_measure_id,
    max(rev_chase_id) filter ( where measure_key ~* 'cbp' ) cbp_rev_chase_id,
    bool_or(measure_key ~* 'dsf') has_dsf,
    max(qm_patient_measure_id) filter ( where measure_key ~* 'dsf' ) dsf_qm_patient_measure_id,
    max(rev_chase_id) filter ( where measure_key ~* 'dsf' ) dsf_rev_chase_id,
    bool_or(measure_key ~* 'hbd') has_hbd,
    max(qm_patient_measure_id) filter ( where measure_key ~* 'hbd' ) hbd_qm_patient_measure_id,
    max(rev_chase_id) filter ( where measure_key ~* 'hbd' ) hbd_rev_chase_id
from _unpivoted
group by patient_id, mbi, payer_id, member_id, contract_id
;

select * from _unpivoted;
select * from _pivoted;

-- I think this is our focus zone
select count(*) from _unpivoted
where qm_patient_measure_id is not null
  and msh_chase_id ISNULL ;
--     and rev_chase_id is null;

SELECT
    COUNT(*)
  , COUNT(*) FILTER ( WHERE rc.external_chase_id IS NOT NULL )
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
WHERE
      rp.payer_id = 81
  AND rp.yr = 2024
  AND rc.inserted_at > NOW() - '24 hours'::INTERVAL
;

SELECT *
FROM
    analytics.oban.oban_jobs WHERE queue ~* 'reveleer';



-- select distinct
--     case
--         when pred_percentile between 0 and 20 then '0-25'
--         when pred_percentile between 20 and 40 then '25-50'
--         when pred_percentile between 40 and 60 then '40-60'
--         when pred_percentile between 60 and 80 then '60-80'
--         when pred_percentile between 80 and 90 then '80-90'
--         when pred_percentile between 90 and 95 then '90-95'
--         when pred_percentile between 95 and 99 then '95-99'
--         when pred_percentile > 99 then '99+'
--         else null end as pred_percentile_bucket,
--         avg(tc_pmpm_tg) as avg_actual_pmpm,
--         percentile_cont(0.5) within group ( order by tc_pmpm_tg ) as median_actual_pmpm,
--         min(tc_pmpm_tg) as min_actual_pmpm,
--         max(tc_pmpm_tg) as max_actual_pmpm,
--         count(distinct member_id) as members
-- from
--     dh.cost_preds_output
--
-- group by 1