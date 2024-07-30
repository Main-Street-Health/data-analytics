set schema 'cb';

select x.member_id from junk.adjudciation_scores_batch_18 x order by 1;




-- truncate adjudication_batches restart identity ;
-- truncate adjudication_members restart identity ;

select * from scoring_runs sr;
    -- ** CLEAN UP **
    -- delete (3,6)
    -- delete from adjudication_batches ab where ab.id = 8
    -- delete from adjudication_members am where am.id = 8

-- 2018 6 -> 10
select adjudication_batch_create(1, 750, '2', false, 3);
-- 2017 3 -> 9
select adjudication_batch_create(2, 750, '2', false, 2);
-- 2017 v2 -> 11
select adjudication_batch_create(3, 750, '2', false, 5);
-- 2018 v2 -> 12 -> 15 -> 16
select adjudication_batch_create(4, 750, '2', false, 6);


select adjudication_batch_create(5, 1000, '2', true , null );

select count(distinct am.member_id) from adjudication_members am where am.adjudication_batch_id = 18
select am.member_id from adjudication_members am where am.adjudication_batch_id = 18 order by 1 asc

select
    m.id mab_id,
    sr.id scoring_run_id,
    ab.id adjudication_id,
    mdl.name,
    mdl.id,
    m.id_date
from
     scoring_runs sr
     join models mdl on sr.model_id = mdl.id
     join mabs m on sr.mab_id = m.id
     join adjudication_batches ab on m.id = ab.mab_id
where
    sr.mab_id in (1,2,3,4)
    and sr.id in (2,3,5,6)
    and ab.id in (9,10,11,12)
order by 1
;


--- btchs
drop table if exists _btchs;
create temporary table _btchs as
select
    x.*, ab.mab_id, m.id_date
from
    ( values
       (10, 2018,  3, '2018 model_v1'),
       ( 9, 2017,  2, '2017 model_v1'),
       (11, 2017,  5, '2017 model_v2'),
       (12, 2018,  6, '2018 model_v2')
    ) x(adjudication_batch_id, yr, scoring_run_id, adj_run)
    join adjudication_batches ab on ab.id = x.adjudication_batch_id
    join mabs m on ab.mab_id = m.id
;
select * from _btchs ab;

-- qa #'s
select
    ab.yr, ab.adj_run, ab.adjudication_batch_id,
    count(am.member_id),
    count(distinct am.member_id),
    count(t.member_id) n_targets
from
    adjudication_members am
    join _btchs ab on am.adjudication_batch_id = ab.adjudication_batch_id
    left join mab_member_targets t on t.member_id = am.member_id and t.lvl ~ '2' and t.mab_id = ab.mab_id
group by 1,2,3;

select
    ams.adjudication_batch_id,
    --ams.id_score,
    ams.ds_score,
    count(ams.member_id)
from adjudication_member_scores ams
group by 1,2
order by 1,2
/*
    select
        ams.adjudication_batch_id,
        ams.id_score,
        count(ams.id) ND
    from
        cb.adjudication_member_scores ams
    group by 1, 2
    order by 1, 2;
*/

-- pct of savings from each intervention & category


with sr as (
    select * from (values
            (4,7),
            (3,8)--,
            --(5,9)
        ) x(mab_id, score_run_id)
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
                                    )
),
id_score as (
        select
        'score ' || t.mab_id id_run,
        t.member_id,
        t.mab_id,
        --row_number() over (partition by t.lvl, t.is_pre order by t.savings desc ) rank
        row_number() over (partition by t.mab_id order by msr.score desc ) rank
    from
        mab_member_targets t
        cross join sr
        join member_scores msr on msr.scoring_run_id = sr.score_run_id and t.member_id = msr.member_id
    where
        t.is_pre = 1
        and t.mab_id = sr.mab_id
        and t.lvl ~ '2'
        and not exists (select 1 from exclude e where e.member_id = t.member_id and e.mab_id = sr.mab_id)

),
id_rule as (
    select
        'id ' || t.mab_id::text id_run,
        t.member_id,
        t.mab_id,
        row_number() over (partition by t.mab_id order by t.savings + case when t.is_lob3 = 1 then 1200 else 0 end desc ) rank
    from
        mab_member_targets t
        join sr on sr.mab_id = t.mab_id
    where
        t.is_pre = 1
        and t.lvl ~ '2'
        and not exists (select 1 from exclude e where e.member_id = t.member_id and e.mab_id = sr.mab_id)
),
 thresholds as (
     select * from (values (800)) x(threshold)
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
        rank
    from
        vw_mab_mbr_yr my
        join (
                 select * from id_score
                 union all
                 select * from id_rule
             ) i on i.member_id = my.member_id and my.mab_id = i.mab_id
        join mab_member_targets t on t.mab_id = my.mab_id and t.member_id = my.member_id and my.is_pre = t.is_pre and t.lvl ~ '2'
        join thresholds th on  i.rank <= th.threshold
    order by my.member_id
)
select
    pre.id_run,
    pre.mab_id,
    pre.threshold,
    pre.id_date,
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
    ----


    (count(pre.member_id) filter( where post.lob = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_1_pct,
    (count(pre.member_id) filter( where post.lob = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_2_pct,
    (count(pre.member_id) filter( where post.lob = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_3_pct,
    (count(pre.member_id) filter( where post.grp = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_1_pct,
    (count(pre.member_id) filter( where post.grp = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_2_pct,
    (count(pre.member_id) filter( where post.grp = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_3_pct,

    (count(pre.member_id) filter( where post.category ~ '4' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_4_pct,
    (count(pre.member_id) filter( where post.category ~ '3' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_3_pct,
    (count(pre.member_id) filter( where post.category ~ '2' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_2_pct,
    (count(pre.member_id) filter( where post.category ~ '1' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_1_pct,
    (count(pre.member_id) filter( where post.category ~ '0' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_0_pct,
    --
    (sum(1) filter (where post.is_eol or post.grp = 1 or post.lob = 2) * 100.0 / count(1))::decimal(16,2) lost_pct,
    (sum(1) filter (where post.is_eol) * 100.0 / count(1))::decimal(16,2) eol_pct,
    --
    (sum(pre.tc ) / sum(pre.p_mm ))::decimal(16,2) pre_pmpm,
    (sum(post.tc) / sum(post.p_mm))::decimal(16,2) post_pmpm,
    ---
    '___sav___' _savings,
    pre.mab_id,
    pre.id_run,
    pre.threshold,
    sum(post.savings) savings_tc,
    (sum(post.savings) / sum(post.p_mm))::decimal(16,2) savings_pmpm,
    (sum(post.ds_savings) * 100.0 / sum(post.savings))::decimal(16,2) ds_sav_pct,
    (sum(post.raf_savings) * 100.0 / sum(post.savings))::decimal(16,2) raf_sav_pct,
    (sum(post.ip_savings) * 100.0 / sum(post.savings))::decimal(16,2) ip_savings,
    (sum(post.snf_savings) * 100.0 / sum(post.savings))::decimal(16,2) snf_savings,
    (sum(post.icf_savings) * 100.0 / sum(post.savings))::decimal(16,2) icf_savings,
    (sum(post.pro_savings) * 100.0 / sum(post.savings))::decimal(16,2) pro_savings,
    (sum(post.hh_savings) * 100.0 / sum(post.savings))::decimal(16,2) hh_savings,
    (sum(post.ed_savings) * 100.0 / sum(post.savings))::decimal(16,2) ed_savings,
    (sum(post.out_savings) * 100.0 / sum(post.savings))::decimal(16,2) out_savings,
    --
    min(pre.pmpm) min_pre_pmpm,
    max(pre.pmpm) max_pre_pmpm,
    min(post.pmpm) min_post_pmpm,
    max(post.pmpm) max_post_pmpm,
    count(1) filter (where pre.pmpm < 10) pre_n_lt_10_pmpm,
    count(1) filter (where post.pmpm < 10) post_n_lt_10_pmpm,
    sum(1) filter (where post.is_eol) eol_N,
    sum(100) filter (where post.is_eol) / count(1) mortality,
    count(1) n,
    count(distinct  pre.member_id) filter (where pre.p_mm > 0  )  nd_pre,
    count(distinct post.member_id) filter (where post.p_mm > 0 )  nd_post,
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
    )::decimal(16,2) cb_dburden_score
from
    id_mab pre
    left join id_mab post on post.member_id = pre.member_id and post.is_pre = 0 and pre.mab_id = post.mab_id and pre.id_run = post.id_run and pre.threshold = post.threshold
where
    pre.is_pre = 1
group by  2, 1, 3, 4
;











SELECT * FROM cb.vw_mab_training_data WHERE mab_id = 5;

with id as (
    select
        adj_run,
        sr.member_id,
        b.mab_id,
        --row_number() over (partition by t.lvl, t.is_pre order by t.savings desc ) rank_savings,
        row_number() over (partition by b.adj_run order by sr.score desc ) rank
    from
        _btchs b
        join mab_member_targets t on t.mab_id = b.mab_id
        join member_scores sr on b.scoring_run_id = sr.scoring_run_id
)
, id_mab as (
    select
        i.adj_run,
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
        rank
    from
        vw_mab_mbr_yr my
        join id i on i.member_id = my.member_id and my.mab_id = i.mab_id
        join mab_member_targets t on t.mab_id = my.mab_id and t.member_id = my.member_id and my.is_pre = t.is_pre and t.lvl ~ '2'
    where
        i.rank <= 750
    order by my.member_id
)
select
    pre.adj_run,
    pre.mab_id,
    pre.id_date,

    (count(pre.member_id) filter( where post.lob = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_1_pct,
    (count(pre.member_id) filter( where post.lob = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_2_pct,
    (count(pre.member_id) filter( where post.lob = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_3_pct,
    (count(pre.member_id) filter( where post.grp = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_1_pct,
    (count(pre.member_id) filter( where post.grp = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_2_pct,
    (count(pre.member_id) filter( where post.grp = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_3_pct,

    (count(pre.member_id) filter( where post.category ~ '4' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_4_pct,
    (count(pre.member_id) filter( where post.category ~ '3' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_3_pct,
    (count(pre.member_id) filter( where post.category ~ '2' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_2_pct,
    (count(pre.member_id) filter( where post.category ~ '1' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_1_pct,
    (count(pre.member_id) filter( where post.category ~ '0' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_0_pct,
    --
    (sum(1) filter (where post.is_eol or post.grp = 1 or post.lob = 2) * 100.0 / count(1))::decimal(16,2) lost_pct,
    (sum(1) filter (where post.is_eol) * 100.0 / count(1))::decimal(16,2) eol_pct,
    --
    (sum(pre.tc ) / sum(pre.p_mm ))::decimal(16,2) pre_pmpm,
    (sum(post.tc) / sum(post.p_mm))::decimal(16,2) post_pmpm,
    ---
    '___sav___' _savings,

    (sum(post.raf_savings) / sum(post.p_mm))::decimal(16,2) raf_savings_tgt,
    (sum(post.savings) / sum(post.p_mm))::decimal(16,2) savings_pmpm,
    (sum(post.ds_savings) * 100.0 / sum(post.savings))::decimal(16,2) ds_sav_pct,
    (sum(post.raf_savings) * 100.0 / sum(post.savings))::decimal(16,2) raf_sav_pct,

    (sum(post.ip_savings) * 100.0 / sum(post.savings))::decimal(16,2) ip_savings,
    (sum(post.snf_savings) * 100.0 / sum(post.savings))::decimal(16,2) snf_savings,
    (sum(post.icf_savings) * 100.0 / sum(post.savings))::decimal(16,2) icf_savings,
    (sum(post.pro_savings) * 100.0 / sum(post.savings))::decimal(16,2) pro_savings,
    (sum(post.hh_savings) * 100.0 / sum(post.savings))::decimal(16,2) hh_savings,
    (sum(post.ed_savings) * 100.0 / sum(post.savings))::decimal(16,2) ed_savings,
    (sum(post.out_savings) * 100.0 / sum(post.savings))::decimal(16,2) out_savings,

    --
    min(pre.pmpm) min_pre_pmpm,
    max(pre.pmpm) max_pre_pmpm,
    min(post.pmpm) min_post_pmpm,
    max(post.pmpm) max_post_pmpm,
    count(1) filter (where pre.pmpm < 10) pre_n_lt_10_pmpm,
    count(1) filter (where post.pmpm < 10) post_n_lt_10_pmpm,
    sum(1) filter (where post.is_eol) eol_N,
    sum(100) filter (where post.is_eol) / count(1) mortality,
    count(1) n,
    count(distinct  pre.member_id) filter (where pre.p_mm > 0  )  nd_pre,
    count(distinct post.member_id) filter (where post.p_mm > 0 )  nd_post
from
    id_mab pre
    left join id_mab post on post.member_id = pre.member_id and post.is_pre = 0 and pre.mab_id = post.mab_id
where
    pre.is_pre = 1
group by 1, 2, 3
;



with id as (
    select
        adj_run,
        sr.member_id,
        b.mab_id,
        b.adjudication_batch_id,
        row_number() over (partition by t.lvl, t.is_pre, adj_run order by t.savings desc, t.member_id desc ) rank
        --row_number() over (partition by b.adj_run order by sr.score desc ) rank
    from
        _btchs b
        join member_scores sr on b.scoring_run_id = sr.scoring_run_id
        join mab_member_targets t on t.mab_id = b.mab_id and t.lvl ~ '2' and t.is_pre = 1 and t.member_id = sr.member_id
)
select
    adj_run,
    id.adjudication_batch_id,
    ab.use_rule,
    count(id.member_id) n,
    count(distinct id.member_id) nd,
    count(distinct am.member_id) nd
from
    id
    join adjudication_batches ab on ab.id = id.adjudication_batch_id
    left join adjudication_members am on id.adjudication_batch_id = am.adjudication_batch_id and am.member_id = id.member_id
where
    id.rank <= 750
group by
    1, 2, 3
;


with id as (
    select
        ab.adj_run,
        am.adjudication_batch_id,
        t.*
    from
        mab_member_targets t
        join _btchs ab on t.mab_id = ab.mab_id
        join adjudication_members am on ab.adjudication_batch_id = am.adjudication_batch_id and am.member_id = t.member_id
    where
        t.is_pre = 1 and t.lvl ~ '2'
)
, id_mab as (
    select
        i.adj_run,
        my.*,
        case when is_cat0 = 1 then 'E'
             when is_cat4 = 1 then 'A'
             when is_cat3 = 1 then 'B'
             when is_cat2 = 1 then 'C'
             when is_cat1 = 1 then 'D'
        end category,
        sav_pct, savings, raf_sav_pct, ds_sav_pct, ip_sav_pct, snf_sav_pct, icf_sav_pct, ed_sav_pct, hh_sav_pct, pro_sav_pct, out_sav_pct, raf_savings, ds_savings, ip_savings, snf_savings, icf_savings, ed_savings, hh_savings, pro_savings, out_savings, savings_pmpm, raf_sav_pmpm, ds_sav_pmpm, ip_sav_pmpm, snf_sav_pmpm, icf_sav_pmpm, ed_sav_pmpm, hh_sav_pmpm, pro_sav_pmpm, out_sav_pmpm, is_self_directed
    from
        vw_mab_mbr_yr my
        join id i on i.member_id = my.member_id and my.mab_id = i.mab_id and my.is_pre = my.is_pre
    order by my.member_id
)
select
    pre.adj_run,
    pre.mab_id,
    pre.id_date,
    (count(pre.member_id) filter( where post.lob = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_1_pct,
    (count(pre.member_id) filter( where post.lob = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_2_pct,
    (count(pre.member_id) filter( where post.lob = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_3_pct,
    (count(pre.member_id) filter( where post.grp = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_1_pct,
    (count(pre.member_id) filter( where post.grp = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_2_pct,
    (count(pre.member_id) filter( where post.grp = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_3_pct,

    (count(pre.member_id) filter( where post.category ~ '4' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_4_pct,
    (count(pre.member_id) filter( where post.category ~ '3' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_3_pct,
    (count(pre.member_id) filter( where post.category ~ '2' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_2_pct,
    (count(pre.member_id) filter( where post.category ~ '1' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_1_pct,
    (count(pre.member_id) filter( where post.category ~ '0' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_0_pct,

    (sum(1) filter (where post.is_eol or post.grp = 1 or post.lob = 2) * 100.0 / count(1))::decimal(16,2) lost_pct,
    (sum(pre.tc ) / sum(pre.p_mm ))::decimal(16,2) pre_pmpm,
    (sum(post.tc) / sum(post.p_mm))::decimal(16,2) post_pmpm,
    min(pre.pmpm) min_pre_pmpm,
    max(pre.pmpm) max_pre_pmpm,
    min(post.pmpm) min_post_pmpm,
    max(post.pmpm) max_post_pmpm,
    count(1) filter (where pre.pmpm < 10) pre_n_lt_10_pmpm,
    count(1) filter (where post.pmpm < 10) post_n_lt_10_pmpm,
    sum(1) filter (where post.is_eol) eol_N,
    sum(100) filter (where post.is_eol) / count(1) mortality,
    count(1) n,
    count(distinct  pre.member_id) filter (where pre.p_mm > 0  )  nd_pre,
    count(distinct post.member_id) filter (where post.p_mm > 0 )  nd_post
from
    id_mab pre
    left join id_mab post on post.member_id = pre.member_id and post.is_pre = 0 and pre.mab_id = post.mab_id
where
    pre.is_pre = 1
group by 1, 2, 3
;

-------------------------------------------------------------------------
-- ADJ SCORES -----------------------------------------------------------
with map as (select * from (values (1, 4), (2, 3)) x(mab_from, mab_to))
select
    ab.mab_id,
    mab.id_date,
    id_score,
    count(ams.member_id) n,
    count(distinct ams.member_id) nd,
    avg(
     case when vpre.copd_ddos         > 2 then  4 else 0 end +
     case when vpre.heart_ddos        > 2 then  4 else 0 end +
     case when vpre.chf_ddos          > 2 then  4 else 0 end +
     case when vpre.pulmonar_ddos     > 2 then  3 else 0 end +
     case when vpre.diab_ddos         > 4 then  2 else 0 end +
     case when vpre.hypertension_ddos > 4 then  2 else 0 end +
     case when vpre.hyperlipid_ddos   > 2 then  1 else 0 end +
     case when vpre.stroke_ddos       > 2 then  1 else 0 end +
     case when vpre.fall_ddos         > 2 then  1 else 0 end +
     case when vpre.hippfract_ddos    > 2 then  1 else 0 end +
     case when vpre.psychosis_ddos    > 2 then  0 else 0 end +
     case when vpre.depression_ddos   > 2 then  0 else 0 end + -- BH carve out w/ UHC
     case when vpre.liver_ddos        > 3 then -1 else 0 end +
     case when vpre.dementia_ddos     > 2 then -1 else 0 end +
     case when vpre.cancer_ddos       > 2 then -1 else 0 end +
     case when vpre.paralysis_ddos    > 2 then -1 else 0 end +
     case when vpre.ckd_ddos          > 2 then -2 else 0 end +
     case when vpre.alzh_ddos         > 2 then -2 else 0 end +
     case when vpre.esrd_ddos         > 2 then -2 else 0 end +
     case when vpre.transplant_ddos   > 2 then -3 else 0 end
    )::decimal(16,2) cb_dburden_score,

     (sum(case when vpre.copd_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)         copd_pct ,
     (sum(case when vpre.heart_ddos        > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)        heart_pct ,
     (sum(case when vpre.chf_ddos          > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)          chf_pct ,
     (sum(case when vpre.pulmonar_ddos     > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)     pulmonar_pct ,
     (sum(case when vpre.ckd_ddos          > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)          ckd_pct ,
     (sum(case when vpre.diab_ddos         > 4 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)         diab_pct ,
     (sum(case when vpre.hyperlipid_ddos   > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)   hyperlipid_pct ,
     (sum(case when vpre.stroke_ddos       > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)       stroke_pct ,
     (sum(case when vpre.hypertension_ddos > 4 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0) hypertension_pct,
     (sum(case when vpre.fall_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)         fall_pct ,
     (sum(case when vpre.liver_ddos        > 3 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)        liver_pct ,
     (sum(case when vpre.hippfract_ddos    > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)    hippfract_pct ,
     (sum(case when vpre.psychosis_ddos    > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)    psychosis_pct ,
     (sum(case when vpre.depression_ddos   > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)   depression_pct ,
     (sum(case when vpre.alzh_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)         alzh_pct ,
     (sum(case when vpre.dementia_ddos     > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)     dementia_pct ,
     (sum(case when vpre.cancer_ddos       > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)       cancer_pct ,
     (sum(case when vpre.paralysis_ddos    > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)    paralysis_pct ,
     (sum(case when vpre.transplant_ddos   > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)   transplant_pct ,
     (sum(case when vpre.esrd_ddos         > 2 then 1 else 0 end ) * 100.0 / count(distinct ams.member_id))::decimal(16,0)         esrd_pct
from
    cb.adjudication_member_scores ams
    join adjudication_batches ab  on ams.adjudication_batch_id = ab.id
    join mabs mab on ab.mab_id = mab.id
    join map on map.mab_from = ab.mab_id
    --
    join mab_member_targets tpre  on tpre.member_id  = ams.member_id and tpre.is_pre  = 1 and tpre.lvl  ~ '2' and tpre.mab_id  = map.mab_to
    join mab_member_targets tpost on tpost.member_id = ams.member_id and tpost.is_pre = 0 and tpost.lvl ~ '2' and tpost.mab_id = map.mab_to
    --
    join vw_mab_mbr_yr vpre  on ams.member_id = vpre.member_id  and vpre.mab_id  = map.mab_to and vpre.is_pre  = 1
    join vw_mab_mbr_yr vpost on ams.member_id = vpost.member_id and vpost.mab_id = map.mab_to and vpost.is_pre = 0
group by 1, 2, 3
order by 3, 2;





select
    ams.id,
    ams.adjudication_batch_id,
    ams.id_score,
    ams.ds_score,
    ams.member_id,
    case when tpre.is_cat0 = 1 then '0. E'
         when tpre.is_cat4 = 1 then '1. A'
         when tpre.is_cat3 = 1 then '2. B'
         when tpre.is_cat2 = 1 then '3. C'
         when tpre.is_cat1 = 1 then '3. D'
    end cat_pre,
    case when tpost.is_cat0 = 1 then '0. E'
         when tpost.is_cat4 = 1 then '1. A'
         when tpost.is_cat3 = 1 then '2. B'
         when tpost.is_cat2 = 1 then '3. C'
         when tpost.is_cat1 = 1 then '3. D'
    end cat_post,
    tpost.is_cat0,
    ams.note,

    vpre.cancer_ddos,
    vpost.cancer_ddos,
    vpre.transplant_ddos pre_trans,
    vpost.transplant_ddos post_trans,
    '___' _,
    vpre.pmpm,
    ((coalesce(vpost.hcbs_attd_pmpm,0) + coalesce(vpost.hcbs_pcs_pmpm,0)) / (15 * 4))::int acpc_wkly_hrs,
    vpost.pmpm,
    vpost.is_unaligned,
    vpost.ip_ddos,
    vpost.ip_pmpm,
    vpost.icf_pmpm,
    vpost.snf_pmpm,
    vpost.pro_pmpm,
    vpost.out_pmpm,
    vpost.hh_pmpm,
    (coalesce(vpost.hcbs_attd_pmpm,0) + coalesce(vpost.hcbs_pcs_pmpm,0)) hcbs_acpc_pmpm,
    vpost.amb_pmpm,
    vpost.rx_pmpm,
    vpost.hcbs_pmpm,
    vpost.hcbs_respite_pmpm,
    vpost.is_eol
from
    cb.adjudication_member_scores ams
    join adjudication_batches ab on ams.adjudication_batch_id = ab.id
    join mab_member_targets tpre on tpre.member_id = ams.member_id and tpre.is_pre = 1 and tpre.lvl ~ '2' and tpre.mab_id = ab.mab_id
    join mab_member_targets tpost on tpost.member_id = ams.member_id and tpost.is_pre = 0 and tpost.lvl ~ '2' and tpost.mab_id = ab.mab_id
    join vw_mab_mbr_yr vpre on ams.member_id = vpre.member_id and vpre.mab_id = ab.mab_id and vpre.is_pre = 1
    join vw_mab_mbr_yr vpost on ams.member_id = vpost.member_id and vpost.mab_id = ab.mab_id and vpost.is_pre = 0
where
    ams.id_score = 3
    -- (vpost.transplant_ddos > 0 or vpre.transplant_ddos > 0)
;


select
    id_date, am.id, v.member_id, v.is_pre, v.hcbs_attd_tc, v.hcbs_pcs_tc, v.start_date,
    --mt.ds_sav_pmpm,
    am.adj_json->'pre_json'->'member_savings_json'->'ds_savings' ds_savings,
    am.adj_json->'post_json'->'member_savings_json'->'ds_savings' ds_savings
from
    adjudication_batches ab
    join adjudication_members am on ab.id = am.adjudication_batch_id and am.member_id = 207
    join vw_mab_mbr_yr v on ab.mab_id = v.mab_id and am.member_id = v.member_id
    --join mab_member_targets mt on mt.mab_id = ab.mab_id and mt.member_id = v.member_id and mt.is_pre = v.is_pre and mt.lvl ~ '2'
where
    ab.id = 9
;

----------------------------
----------------------------
----------------------------
with _controls as (
    select 2 mab_id, 750 num_pats_to_id, '2' lvl, false use_rule, 2 scoring_run_id
), _id as (
    select
        t.*,
        row_number() over (partition by t.lvl, t.is_pre order by t.savings desc ) rank_savings,
        row_number() over (partition by t.lvl, t.is_pre order by sr.score desc ) rank_score
    from
        mab_member_targets t
        join _controls c on t.lvl ~ c.lvl and t.mab_id = c.mab_id
        left join member_scores sr on c.scoring_run_id = sr.id and sr.member_id = t.member_id and c.use_rule = false
    where
        t.is_pre = 1
), _id_mab as (
    select
        my.*,
        case when is_cat0 = 1 then '0. E'
             when is_cat4 = 1 then '1. A'
             when is_cat3 = 1 then '2. B'
             when is_cat2 = 1 then '3. C'
             when is_cat1 = 1 then '3. D'
        end category,
        sav_pct, savings, raf_sav_pct, ds_sav_pct, ip_sav_pct, snf_sav_pct, icf_sav_pct, ed_sav_pct, hh_sav_pct, pro_sav_pct, out_sav_pct, raf_savings, ds_savings, ip_savings, snf_savings, icf_savings, ed_savings, hh_savings, pro_savings, out_savings, savings_pmpm, raf_sav_pmpm, ds_sav_pmpm, ip_sav_pmpm, snf_sav_pmpm, icf_sav_pmpm, ed_sav_pmpm, hh_sav_pmpm, pro_sav_pmpm, out_sav_pmpm, rank_savings, rank_score, is_self_directed
    from
        vw_mab_mbr_yr my
        join _id i on i.member_id = my.member_id and my.mab_id = i.mab_id
        join _controls c on  (
                        (c.use_rule and i.rank_savings <= c.num_pats_to_id)
                        or
                        (c.use_rule = false and i.rank_score <= c.num_pats_to_id)
                    )
    order by my.member_id
)
select
    pre.id_date,
    (count(pre.member_id) filter( where post.lob = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_1_pct,
    (count(pre.member_id) filter( where post.lob = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_2_pct,
    (count(pre.member_id) filter( where post.lob = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) lob_3_pct,
    (count(pre.member_id) filter( where post.grp = 1 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_1_pct,
    (count(pre.member_id) filter( where post.grp = 2 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_2_pct,
    (count(pre.member_id) filter( where post.grp = 3 ) * 100.0 / count(pre.member_id))::decimal(16,2) grp_3_pct,

    (count(pre.member_id) filter( where post.category ~ '4' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_4_pct,
    (count(pre.member_id) filter( where post.category ~ '3' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_3_pct,
    (count(pre.member_id) filter( where post.category ~ '2' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_2_pct,
    (count(pre.member_id) filter( where post.category ~ '1' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_1_pct,
    (count(pre.member_id) filter( where post.category ~ '0' ) * 100.0 / count(pre.member_id))::decimal(16,2) cat_0_pct,

    (sum(1) filter (where post.is_eol or post.grp = 1 or post.lob = 2) * 100.0 / count(1))::decimal(16,2) lost_pct,
    (sum(pre.tc ) / sum(pre.p_mm ))::decimal(16,2) pre_pmpm,
    (sum(post.tc) / sum(post.p_mm))::decimal(16,2) post_pmpm,
    min(pre.pmpm) min_pre_pmpm,
    max(pre.pmpm) max_pre_pmpm,
    min(post.pmpm) min_post_pmpm,
    max(post.pmpm) max_post_pmpm,
    count(1) filter (where pre.pmpm < 10) pre_n_lt_10_pmpm,
    count(1) filter (where post.pmpm < 10) post_n_lt_10_pmpm,
    sum(1) filter (where post.is_eol) eol_N,
    sum(100) filter (where post.is_eol) / count(1) mortality,
    count(1) n,
    count(distinct  pre.member_id) filter (where pre.p_mm > 0  )  nd_pre,
    count(distinct post.member_id) filter (where post.p_mm > 0 )  nd_post
from
    _id_mab pre
    left join _id_mab post on post.member_id = pre.member_id and post.is_pre = 0
where
    pre.is_pre = 1
group by 1
;


with id as (
    select
        adj_run,
        sr.member_id,
        b.mab_id,
        b.adjudication_batch_id,
        --row_number() over (partition by t.lvl, t.is_pre, adj_run order by t.savings desc, t.member_id desc ) rank
        row_number() over (partition by b.adj_run order by sr.score desc ) rank
    from
        ( select * from (values ('x', 4, 6, 16)) x(adj_run, mab_id, scoring_run_id, adjudication_batch_id) ) b -- _btchs b
        join member_scores sr on b.scoring_run_id = sr.scoring_run_id
        join mab_member_targets t on t.mab_id = b.mab_id and t.lvl ~ '2' and t.is_pre = 1 and t.member_id = sr.member_id
)
select
    adj_run,
    id.adjudication_batch_id,
    ab.use_rule,
    count(id.member_id) n,
    count(distinct id.member_id) nd,
    count(distinct am.member_id) nd
from
    id
    join adjudication_batches ab on ab.id = id.adjudication_batch_id
    left join adjudication_members am on id.adjudication_batch_id = am.adjudication_batch_id and am.member_id = id.member_id
where
    id.rank <= 750
group by
    1, 2, 3
;