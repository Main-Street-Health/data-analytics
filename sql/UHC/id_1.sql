set schema 'cb';

create temporary table _iduhc as
with sr as (
    select 5 mab_id
),
exclude as (
    select
        sr.mab_id, v.member_id
    from
        sr
        join vw_mab_mbr_yr v on v.mab_id = sr.mab_id and v.is_pre = 1
                                    and (
                                        v.esrd_ddos > 10
                                        or
                                        v.transplant_ddos > 1
                                        or
                                        v.alcohol_ddos > 3
                                        or
                                        v.drug_ddos > 3
                                    )
),
id_rule as (
    select
        'id ' || t.mab_id::text id_run,
        t.member_id,
        t.mab_id,
        row_number() over (partition by t.mab_id order by t.savings + case when t.is_lob3 = 1 then 1000 else 0 end desc ) rank
    from
        mab_member_targets t
        join sr on sr.mab_id = t.mab_id
    where
        t.is_pre = 1
        and t.lvl ~ '2'
        --and not exists (select 1 from exclude e where e.member_id = t.member_id and e.mab_id = sr.mab_id)
),
 thresholds as (
     select * from (values (1000)) x(threshold)
 )
, id_mab as (
    select
        i.id_run,
        threshold,
        my.*,
        case when is_cat0 = 1 then '0. E'
             when is_cat4 = 1 then '4. A'
             when is_cat3 = 1 then '3. B'
             when is_cat2 = 1 then '2. C'
             when is_cat1 = 1 then '1. D'
        end category,
        t.sav_pct,
        t.savings,
        t.raf_sav_pct,
        t.ds_sav_pct,
        t.ip_sav_pct,
        t.snf_sav_pct,
        t.icf_sav_pct,
        t.ed_sav_pct,
        t.hh_sav_pct,
        t.pro_sav_pct,
        t.out_sav_pct,
        t.raf_savings,
        t.ds_savings,
        t.ip_savings,
        t.snf_savings,
        t.icf_savings,
        t.ed_savings,
        t.hh_savings,
        t.pro_savings,
        t.out_savings,
        t.savings_pmpm,
        t.raf_sav_pmpm,
        t.ds_sav_pmpm,
        t.ip_sav_pmpm,
        t.snf_sav_pmpm,
        t.icf_sav_pmpm,
        t.ed_sav_pmpm,
        t.hh_sav_pmpm,
        t.pro_sav_pmpm,
        t.out_sav_pmpm,
        t.is_self_directed,
        rank,
        ams.id_score,
        ams.ds_score,
        case when ex.member_id is not null then 1 else 0 end to_exclude
    from
        vw_mab_mbr_yr my
        join id_rule i on i.member_id = my.member_id and my.mab_id = i.mab_id
        join mab_member_targets t on t.mab_id = my.mab_id and t.member_id = my.member_id and my.is_pre = t.is_pre and t.lvl ~ '2'
        join thresholds th on  i.rank <= th.threshold
        join adjudication_batches ab on ab.mab_id = my.mab_id
        join adjudication_member_scores ams on ams.adjudication_batch_id = ab.id and ams.member_id = my.member_id and ams.adjudication_batch_id = 18
        left join exclude ex on ex.member_id = ams.member_id and ex.mab_id = my.mab_id
    order by my.member_id
)

select * from id_mab
;

select * from _iduhc i where i.date_of_death is null and id_score = 1 and ds_score in (1,2) and to_exclude = 0;

create table junk.uhc_id_consideration_08062020 as
select * from _iduhc i

select count(*) from junk.uhc_id_consideration_08062020 where date_of_death is null and id_score = 1;

select * from eligibility_segments es where es.member_id = 53--69;--53;

select ed.date, is_unaligned from eligibility_days ed where ed.member_id = 69


select * from eligibility_segments es where es.line_of_business_id = 2 and end_date > '2019-12-31'::date and es.mco_id = 2;

---
create table uhc_id_1_members_top_1k as
select * from junk.uhc_id_consideration_08062020
;
select
    count(distinct member_id) nd,
    ---- PRE
    (sum(pre.savings        )         / sum(pre.p_mm   ))::decimal(16,2) savings_pmpm_pre,
    (sum(pre.ds_savings     ) * 100.0 / sum(pre.savings))::decimal(16,2) ds_sav_pct,
    (sum(pre.raf_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) raf_sav_pct,
    (sum(pre.ip_savings     ) * 100.0 / sum(pre.savings))::decimal(16,2) ip_savings,
    (sum(pre.snf_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) snf_savings,
    (sum(pre.icf_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) icf_savings,
    (sum(pre.pro_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) pro_savings,
    (sum(pre.hh_savings     ) * 100.0 / sum(pre.savings))::decimal(16,2) hh_savings,
    (sum(pre.ed_savings     ) * 100.0 / sum(pre.savings))::decimal(16,2) ed_savings,
    (sum(pre.out_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) out_savings,

    (count(pre.member_id) filter( where pre.lob = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_1_pct,
    (count(pre.member_id) filter( where pre.lob = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_2_pct,
    (count(pre.member_id) filter( where pre.lob = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_3_pct,
    (count(pre.member_id) filter( where pre.grp = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_1_pct,
    (count(pre.member_id) filter( where pre.grp = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_2_pct,
    (count(pre.member_id) filter( where pre.grp = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_3_pct,

    (count(pre.member_id) filter( where pre.category ~ '4' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_4_pct,
    (count(pre.member_id) filter( where pre.category ~ '3' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_3_pct,
    (count(pre.member_id) filter( where pre.category ~ '2' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_2_pct,
    (count(pre.member_id) filter( where pre.category ~ '1' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_1_pct,
    (count(pre.member_id) filter( where pre.category ~ '0' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_0_pct,

    (sum(pre.tc ) / sum(pre.p_mm ))::decimal(16,2) pre_pmpm,

    (sum(case when pre.copd_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         copd_pct ,
    (sum(case when pre.heart_ddos        > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)        heart_pct ,
    (sum(case when pre.chf_ddos          > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)          chf_pct ,
    (sum(case when pre.pulmonar_ddos     > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)     pulmonar_pct ,
    (sum(case when pre.ckd_ddos          > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)          ckd_pct ,
    (sum(case when pre.diab_ddos         > 4 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         diab_pct ,
    (sum(case when pre.hyperlipid_ddos   > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)   hyperlipid_pct ,
    (sum(case when pre.stroke_ddos       > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)       stroke_pct ,
    (sum(case when pre.hypertension_ddos > 4 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0) hypertension_pct,
    (sum(case when pre.fall_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         fall_pct ,
    (sum(case when pre.liver_ddos        > 3 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)        liver_pct ,
    (sum(case when pre.hippfract_ddos    > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)    hippfract_pct ,
    (sum(case when pre.psychosis_ddos    > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)    psychosis_pct ,
    (sum(case when pre.depression_ddos   > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)   depression_pct ,
    (sum(case when pre.alzh_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         alzh_pct ,
    (sum(case when pre.dementia_ddos     > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)     dementia_pct ,
    (sum(case when pre.cancer_ddos       > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)       cancer_pct ,
    (sum(case when pre.paralysis_ddos    > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)    paralysis_pct ,
    (sum(case when pre.transplant_ddos   > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)   transplant_pct ,
    (sum(case when pre.esrd_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         esrd_pct ,

    sum(pre.copd_ddos         ) copd_ddos,
    sum(pre.heart_ddos        ) heart_ddos,
    sum(pre.chf_ddos          ) chf_ddos,
    sum(pre.pulmonar_ddos     ) pulmonar_ddos,
    sum(pre.diab_ddos         ) diab_ddos,
    sum(pre.hypertension_ddos ) hypertension_ddos,
    sum(pre.hyperlipid_ddos   ) hyperlipid_ddos,
    sum(pre.stroke_ddos       ) stroke_ddos,
    sum(pre.fall_ddos         ) fall_ddos,
    sum(pre.hippfract_ddos    ) hippfract_ddos,
    sum(pre.psychosis_ddos    ) psychosis_ddos,
    sum(pre.depression_ddos   ) depression_ddos,
    sum(pre.liver_ddos        ) liver_ddos,
    sum(pre.dementia_ddos     ) dementia_ddos,
    sum(pre.cancer_ddos       ) cancer_ddos,
    sum(pre.paralysis_ddos    ) paralysis_ddos,
    sum(pre.ckd_ddos          ) ckd_ddos,
    sum(pre.alzh_ddos         ) alzh_ddos,
    sum(pre.esrd_ddos         ) esrd_ddos,
    sum(pre.transplant_ddos   ) transplant_ddos,

    avg(
        case when pre.copd_ddos         > 2 then  4 else 0 end +
        case when pre.heart_ddos        > 2 then  4 else 0 end +
        case when pre.chf_ddos          > 2 then  4 else 0 end +
        case when pre.pulmonar_ddos     > 2 then  3 else 0 end +
        case when pre.diab_ddos         > 4 then  2 else 0 end +
        case when pre.hypertension_ddos > 4 then  2 else 0 end +
        case when pre.hyperlipid_ddos   > 2 then  1 else 0 end +
        case when pre.stroke_ddos       > 2 then  1 else 0 end +
        case when pre.fall_ddos         > 2 then  1 else 0 end +
        case when pre.hippfract_ddos    > 2 then  1 else 0 end +
        case when pre.psychosis_ddos    > 2 then  0 else 0 end +
        case when pre.depression_ddos   > 2 then  0 else 0 end + -- BH carve out w/ UHC
        case when pre.liver_ddos        > 3 then -1 else 0 end +
        case when pre.dementia_ddos     > 2 then -1 else 0 end +
        case when pre.cancer_ddos       > 2 then -1 else 0 end +
        case when pre.paralysis_ddos    > 2 then -1 else 0 end +
        case when pre.ckd_ddos          > 2 then -2 else 0 end +
        case when pre.alzh_ddos         > 2 then -2 else 0 end +
        case when pre.esrd_ddos         > 2 then -2 else 0 end +
        case when pre.transplant_ddos   > 2 then -3 else 0 end
    )::decimal(16,2) cb_dburden_score,
    --
    (sum(pre.tc ) / sum(pre.p_mm ))::decimal(16,2) pre_pmpm,
    ---
    '___sav___' _savings,
    --
    min(pre.pmpm) min_pre_pmpm,
    max(pre.pmpm) max_pre_pmpm,
    count(1) filter (where pre.pmpm < 10) pre_n_lt_10_pmpm,
    count(1) n,
    (count(distinct  pre.member_id) filter (where pre.is_self_directed > 0 and pre.date_of_death is null  ) * 100.0 / count(distinct member_id))::decimal(16,2) self_directed_pct,
    count(distinct  pre.member_id) filter (where pre.p_mm > 0  )  nd_pre,
    count(distinct pre.member_id) filter (where to_exclude = 1) nd_exc,
    (count(distinct pre.member_id) filter (where date_of_death is not null) * 100.0 / count(distinct pre.member_id))::decimal(16,2),
    ((sum(hcbs_pcs_tc) + sum(hcbs_attd_tc)) / sum(p_mm))::decimal(16,2) apcs_pmpm,
    avg(rank) rank,
    min(rank) rank_min,
    max(rank) rank_max,
    count(distinct member_id) filter (where rank > 750) n_over_750
from
    junk.uhc_id_consideration_08062020 pre
where
    pre.id_score = 1
    and pre.ds_score in (1,2)
    and (
        coalesce(transplant_ddos, 0) = 0
        or
        date_of_death is null
    )
    and rank < 851
;


drop table _tmp_uhc;
create temporary table _tmp_uhc as
select
    pre.member_id, pre.mab_id, 18 adjudication_batch_id, pre.rank ranking,
    (pre.id_score = 1
    and pre.ds_score in (1,2)
    and (
        coalesce(transplant_ddos, 0) = 0
        or
        date_of_death is null
    )
    and rank < 851) is_identified
from
    junk.uhc_id_consideration_08062020 pre

select tu.is_identified, count(1), count(distinct tu.member_id) from _tmp_uhc tu group by 1

drop table perm.uhc_id_1_20200806;
create table perm.uhc_id_1_20200806 as
select
    tu.member_id, tu.is_identified, m.source_member_id, m.source_member_id2, date_of_birth, m.medicare_source_member_id, ranking, now() created_at
from
    _tmp_uhc tu
    join members m on tu.member_id = m.id and m.mco_id = 2


select
    u.member_id cb_member_id,
    u.source_member_id primary_source_member_id,
    u.date_of_birth
from
    perm.uhc_id_1_20200806 u
where
    u.is_identified

select * from perm.notes n;





set schema 'cb';
---------------------------------
drop table if exists _xxx;
create temporary table _xxx as
    select
        my.*,
        case when is_cat0 = 1 then '0. E'
             when is_cat4 = 1 then '4. A'
             when is_cat3 = 1 then '3. B'
             when is_cat2 = 1 then '2. C'
             when is_cat1 = 1 then '1. D'
        end category,
        t.sav_pct,
        t.savings,
        t.raf_sav_pct,
        t.ds_sav_pct,
        t.ip_sav_pct,
        t.snf_sav_pct,
        t.icf_sav_pct,
        t.ed_sav_pct,
        t.hh_sav_pct,
        t.pro_sav_pct,
        t.out_sav_pct,
        t.raf_savings,
        t.ds_savings,
        t.ip_savings,
        t.snf_savings,
        t.icf_savings,
        t.ed_savings,
        t.hh_savings,
        t.pro_savings,
        t.out_savings,
        t.savings_pmpm,
        t.raf_sav_pmpm,
        t.ds_sav_pmpm,
        t.ip_sav_pmpm,
        t.snf_sav_pmpm,
        t.icf_sav_pmpm,
        t.ed_sav_pmpm,
        t.hh_sav_pmpm,
        t.pro_sav_pmpm,
        t.out_sav_pmpm,
        t.is_self_directed
    from
        vwm_mab_mbr_yr my
        join mab_member_targets t on t.mab_id = my.mab_id and t.member_id = my.member_id and my.is_pre = t.is_pre and t.lvl ~ '2'
        -- join adjudication_batches ab on ab.mab_id = my.mab_id
        -- join adjudication_member_scores ams on ams.adjudication_batch_id = ab.id and ams.member_id = my.member_id and ams.adjudication_batch_id = 18
    where
        my.mab_id = 5
    order by my.member_id;


select count(1) from _xxx x;

---------------------------------
with trans as (
    select * from (values ('All', null), ('No Trans', 0), ('Trans only', 1)) x(descr, flag)
)
select
    x.descr,
    count(distinct member_id) nd,
    ---- PRE
    (sum(pre.tc               ) / sum(pre.p_mm))::decimal(16,2) pmpm      ,
    (sum(pre.ip_tc            ) / sum(pre.p_mm))::decimal(16,2) ip_pmpm   ,
    (sum(pre.snf_tc           ) / sum(pre.p_mm))::decimal(16,2) snf_pmpm  ,
    (sum(pre.icf_tc           ) / sum(pre.p_mm))::decimal(16,2) icf_pmpm  ,
    (sum(pre.ed_tc            ) / sum(pre.p_mm))::decimal(16,2) ed_pmpm   ,
    (sum(pre.hh_tc            ) / sum(pre.p_mm))::decimal(16,2) hh_pmpm   ,
    (sum(pre.pro_tc           ) / sum(pre.p_mm))::decimal(16,2) pro_pmpm  ,
    (sum(pre.out_tc           ) / sum(pre.p_mm))::decimal(16,2) out_pmpm  ,
    ((coalesce(sum(pre.hcbs_attd_tc),0) + coalesce(sum(pre.hcbs_pcs_tc),0))  / sum(mm))::decimal(16,2) hcbs_atd_pcs_pmpm,
    (sum(pre.ds_savings     ) * 100.0 / sum(pre.savings))::decimal(16,2) ds_sav_pct,
    (sum(pre.raf_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) raf_sav_pct,
    (sum(pre.ip_savings     ) * 100.0 / sum(pre.savings))::decimal(16,2) ip_savings,
    (sum(pre.snf_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) snf_savings,
    (sum(pre.icf_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) icf_savings,
    (sum(pre.pro_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) pro_savings,
    (sum(pre.hh_savings     ) * 100.0 / sum(pre.savings))::decimal(16,2) hh_savings,
    (sum(pre.ed_savings     ) * 100.0 / sum(pre.savings))::decimal(16,2) ed_savings,
    (sum(pre.out_savings    ) * 100.0 / sum(pre.savings))::decimal(16,2) out_savings,

    (count(pre.member_id) filter( where pre.lob = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_1_pct,
    (count(pre.member_id) filter( where pre.lob = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_2_pct,
    (count(pre.member_id) filter( where pre.lob = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_3_pct,
    (count(pre.member_id) filter( where pre.grp = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_1_pct,
    (count(pre.member_id) filter( where pre.grp = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_2_pct,
    (count(pre.member_id) filter( where pre.grp = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_3_pct,

    (count(pre.member_id) filter( where pre.category ~ '4' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_4_pct,
    (count(pre.member_id) filter( where pre.category ~ '3' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_3_pct,
    (count(pre.member_id) filter( where pre.category ~ '2' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_2_pct,
    (count(pre.member_id) filter( where pre.category ~ '1' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_1_pct,
    (count(pre.member_id) filter( where pre.category ~ '0' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_0_pct,


    (sum(case when pre.copd_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         copd_pct ,
    (sum(case when pre.heart_ddos        > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)        heart_pct ,
    (sum(case when pre.chf_ddos          > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)          chf_pct ,
    (sum(case when pre.pulmonar_ddos     > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)     pulmonar_pct ,
    (sum(case when pre.ckd_ddos          > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)          ckd_pct ,
    (sum(case when pre.diab_ddos         > 4 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         diab_pct ,
    (sum(case when pre.hyperlipid_ddos   > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)   hyperlipid_pct ,
    (sum(case when pre.stroke_ddos       > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)       stroke_pct ,
    (sum(case when pre.hypertension_ddos > 4 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0) hypertension_pct,
    (sum(case when pre.fall_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         fall_pct ,
    (sum(case when pre.liver_ddos        > 3 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)        liver_pct ,
    (sum(case when pre.hippfract_ddos    > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)    hippfract_pct ,
    (sum(case when pre.psychosis_ddos    > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)    psychosis_pct ,
    (sum(case when pre.depression_ddos   > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)   depression_pct ,
    (sum(case when pre.alzh_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         alzh_pct ,
    (sum(case when pre.dementia_ddos     > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)     dementia_pct ,
    (sum(case when pre.cancer_ddos       > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)       cancer_pct ,
    (sum(case when pre.paralysis_ddos    > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)    paralysis_pct ,
    (sum(case when pre.transplant_ddos   > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)   transplant_pct ,
    (sum(case when pre.esrd_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct pre.member_id))::decimal(16,0)         esrd_pct ,

    sum(pre.copd_ddos         ) copd_ddos,
    sum(pre.heart_ddos        ) heart_ddos,
    sum(pre.chf_ddos          ) chf_ddos,
    sum(pre.pulmonar_ddos     ) pulmonar_ddos,
    sum(pre.diab_ddos         ) diab_ddos,
    sum(pre.hypertension_ddos ) hypertension_ddos,
    sum(pre.hyperlipid_ddos   ) hyperlipid_ddos,
    sum(pre.stroke_ddos       ) stroke_ddos,
    sum(pre.fall_ddos         ) fall_ddos,
    sum(pre.hippfract_ddos    ) hippfract_ddos,
    sum(pre.psychosis_ddos    ) psychosis_ddos,
    sum(pre.depression_ddos   ) depression_ddos,
    sum(pre.liver_ddos        ) liver_ddos,
    sum(pre.dementia_ddos     ) dementia_ddos,
    sum(pre.cancer_ddos       ) cancer_ddos,
    sum(pre.paralysis_ddos    ) paralysis_ddos,
    sum(pre.ckd_ddos          ) ckd_ddos,
    sum(pre.alzh_ddos         ) alzh_ddos,
    sum(pre.esrd_ddos         ) esrd_ddos,
    sum(pre.transplant_ddos   ) transplant_ddos,

    avg(
        case when pre.copd_ddos         > 2 then  4 else 0 end +
        case when pre.heart_ddos        > 2 then  4 else 0 end +
        case when pre.chf_ddos          > 2 then  4 else 0 end +
        case when pre.pulmonar_ddos     > 2 then  3 else 0 end +
        case when pre.diab_ddos         > 4 then  2 else 0 end +
        case when pre.hypertension_ddos > 4 then  2 else 0 end +
        case when pre.hyperlipid_ddos   > 2 then  1 else 0 end +
        case when pre.stroke_ddos       > 2 then  1 else 0 end +
        case when pre.fall_ddos         > 2 then  1 else 0 end +
        case when pre.hippfract_ddos    > 2 then  1 else 0 end +
        case when pre.psychosis_ddos    > 2 then  0 else 0 end +
        case when pre.depression_ddos   > 2 then  0 else 0 end + -- BH carve out w/ UHC
        case when pre.liver_ddos        > 3 then -1 else 0 end +
        case when pre.dementia_ddos     > 2 then -1 else 0 end +
        case when pre.cancer_ddos       > 2 then -1 else 0 end +
        case when pre.paralysis_ddos    > 2 then -1 else 0 end +
        case when pre.ckd_ddos          > 2 then -2 else 0 end +
        case when pre.alzh_ddos         > 2 then -2 else 0 end +
        case when pre.esrd_ddos         > 2 then -2 else 0 end +
        case when pre.transplant_ddos   > 2 then -3 else 0 end
    )::decimal(16,2) cb_dburden_score,
    --
    (sum(pre.tc ) / sum(pre.p_mm ))::decimal(16,2) pre_pmpm,
    ---
    '___sav___' _savings,
    --
    min(pre.pmpm) min_pre_pmpm,
    max(pre.pmpm) max_pre_pmpm,
    count(1) filter (where pre.pmpm < 10) pre_n_lt_10_pmpm,
    count(1) n,
    --(count(distinct  pre.member_id) filter (where pre.is_self_directed > 0 and pre.date_of_death is null  ) * 100.0 / count(distinct member_id))::decimal(16,2) self_directed_pct,
    count(distinct  pre.member_id) filter (where pre.p_mm > 0  )  nd_pre,
    --count(distinct pre.member_id) filter (where to_exclude = 1) nd_exc,
    (count(distinct pre.member_id) filter (where date_of_death is not null) * 100.0 / count(distinct pre.member_id))::decimal(16,2),
    ((sum(hcbs_pcs_tc) + sum(hcbs_attd_tc)) / sum(p_mm))::decimal(16,2) apcs_pmpm
    --count(distinct member_id) filter (where rank > 750) n_over_750
from
    _xxx pre
    join trans x on true =
                    case when x.flag is null then true
                         when x.flag = 1 then coalesce(transplant_ddos, 0) > 0
                         when x.flag = 0 then coalesce(transplant_ddos, 0) = 0
                    end
group by
    x.descr
;


---------------------------------------------------
--- INTERESTIG
drop table perm.uhc_id_interesting_20200812;
create table perm.uhc_id_interesting_20200812 as
select
    tu.member_id, m.source_member_id, m.source_member_ids2, date_of_birth, m.medicare_source_member_id,
    transplant_ddos,
    now() created_at
from
    _xxx tu
    join members m on tu.member_id = m.id and m.mco_id = 2
;

select
    u.member_id cb_member_id,
    u.source_member_id primary_source_member_id,
    u.date_of_birth,
    case when u.transplant_ddos > 0 then '*' else '' end note
from
    perm.uhc_id_interesting_20200812 u




