select
    d.bom,
    count(distinct c.member_id) nd_members,
    count(distinct c.id) nd_claims,
    count(1) n,
    sum(c.paid_amount) paid_amount,
    (sum(c.paid_amount) * 1.0 / count(distinct c.member_id))::decimal(16,2) pmpm
from
    mcos mc
    join claims c on c.mco_id = mc.id
    join ref.dates d on d.day = c.date_from
where
    mc.id = 13
group by 1
order by 1

;
set schema 'cb';

-- drop table if exists _controls;
-- create temporary table _controls as
-- with x as (
--     select
--         *
--     from
--         (
--             values
--             ( 1, 'Anthem TN',      '3/1/2018'::date  , null::date ),
--             ( 2, 'UHC TN',         '1/1/2017'::date  , null::date ),
--             ( 3, 'CareSource OH',  '10/1/2018'::date , '8/1/2020'::date ),
--             ( 4, 'UHC FL',         '1/1/2018'::date  , null::date ),
--             ( 5, 'UHC TX',         '1/1/2018'::date  , null::date ),
--             ( 6, 'UHC KS',         '4/1/2018'::date  , null::date ),
--             ( 7, 'Anthem IA',      '9/1/2018'::date  , null::date ),
--             ( 8, 'IA Centene ITC', '7/1/2019'::date  , null::date ),
--             (12, 'UHC VA'        , '7/1/2019'::date  , null::date ),
--             (13, 'UHC MA'        , '2019-08-01'::date  , null::date )
--         ) x(mco_id, mco_name, start_date, end_date)
-- )
-- select
--     x.mco_id, x.mco_name, x.start_date::date start_date, coalesce(x.end_date, mc.end_date_for_ds::date) end_date
-- from
--     mcos mc
--     join x on x.mco_id = mc.id
-- ;

drop table if exists _controls;
create temporary table _controls as
SELECT
    m.id                                                 mco_id
  , name                                                 mco_name
  , GREATEST(m.claims_start_date, '2017-01-01'::DATE)    start_date
  , COALESCE(m.claims_end_date, m.end_date_for_ds::DATE) end_date
FROM
    mcos m
WHERE
    m.claims_start_date IS NOT NULL
;

--drop table if exists junk.ip_mrs;
--create table junk.ip_mbrs as
drop table if exists junk.ip_keepers_new;
create table junk.ip_keepers_new as
select
    *
from (
    select
        cc.mco_id,
        cc.mco_name,
        ed_eom.member_id,
        bool_or(ed_eom.line_of_business_id in (1,3)) is_lob_ok,
        bool_or(ed_eom.ggroup > 1) is_group_ok,
        bool_or(ed_eom.is_unaligned is false) is_unaligned_ok,
        bool_or(extract(year from age(d.eom, m.date_of_birth))::int > 20) is_age_ok
    from
        _controls cc
        join members m on m.mco_id = cc.mco_id
        join (select distinct eom from ref.dates) d on d.eom between cc.start_date and cc.end_date
        join eligibility_days ed_eom on ed_eom.member_id = m.id and ed_eom.mco_id = cc.mco_id and ed_eom.date = d.eom
    group by 1,2,3
) x
where x.is_lob_ok and x.is_group_ok and x.is_unaligned_ok and x.is_age_ok
;

create index idx_junk_ip_keepers_new on junk.ip_keepers_new(member_id)


;

drop table if exists junk.ip_features2_new;
-- create table junk.ip_features2_new as
insert into junk.ip_features2_new
select
    cc.mco_id,
    cc.mco_name,
    d.eom,
    ed_eom.member_id,
    ed_eom.line_of_business_id,
    ed_eom.ggroup,
    ed_eom.is_unaligned,
    extract(year from age(d.eom, m.date_of_birth))::int age,
    m.gender,
    1 cwmm,
    d.days_in_month,
    (count(distinct ed.id) * 1.0 / d.days_in_month)::decimal(16,2) cpmm,
    sum(paid_amount) filter (  where is_rx                )  rx_tc,
    sum(paid_amount) filter (  where service_type_id = 0  )  other_tc,
    sum(paid_amount) filter (  where service_type_id = 1  )  ip_tc,
    sum(paid_amount) filter (  where service_type_id = 2  )  er_tc,
    sum(paid_amount) filter (  where service_type_id = 3  )  out_tc,
    sum(paid_amount) filter (  where service_type_id = 4  )  snf_tc,
    sum(paid_amount) filter (  where service_type_id = 11 )  icf_tc,
    sum(paid_amount) filter (  where service_type_id = 5  )  hh_tc,
    sum(paid_amount) filter (  where service_type_id = 6  )  amb_tc,
    sum(paid_amount) filter (  where service_type_id = 7  )  hsp_tc,
    sum(paid_amount) filter (  where service_type_id = 8  )  pro_tc,
    sum(paid_amount) filter (  where service_type_id = 9  )  spc_fac_tc,
    sum(paid_amount) filter (  where service_type_id = 12 )  dme_tc,
    sum(paid_amount) filter (  where service_type_id = 13 )  cls_tc,
    sum(paid_amount) filter (  where service_type_id = 18 )  hha_tc,
    sum(paid_amount) filter (  where service_type_id = 17 )  hcbs_attdpcs_tc,
    sum(paid_amount) filter (  where service_type_id = 10 )  hcbs_other_tc,
    sum(paid_amount) filter (  where service_type_id = 15 )  hcbs_support_house_tc,
    sum(paid_amount) filter (  where service_type_id = 16 )  hcbs_adult_day_tc,

    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 1  )  ip_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 2  )  er_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 3  )  out_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 4  )  snf_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 11 )  icf_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 5  )  hh_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 6  )  amb_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 7  )  hsp_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 8  )  pro_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 9  )  spc_fac_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 12 )  dme_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 13 )  cls_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 18 )  hha_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 17 )  hcbs_attdpcs_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 10 )  hcbs_other_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 15 )  hcbs_support_house_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 16 )  hcbs_adult_day_ddos_span,
    sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 0  )  other_ddos_span,

    count(distinct c.date_from) filter (  where service_type_id = 1  )  ip_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 2  )  er_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 3  )  out_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 4  )  snf_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 11 )  icf_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 5  )  hh_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 6  )  amb_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 7  )  hsp_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 8  )  pro_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 9  )  spc_fac_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 12 )  dme_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 13 )  cls_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 18 )  hha_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 17 )  hcbs_attdpcs_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 10 )  hcbs_other_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 15 )  hcbs_support_house_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 16 )  hcbs_adult_day_ddos,
    count(distinct c.date_from) filter (  where service_type_id = 0  )  other_ddos
from
    _controls  cc
    join members m on m.mco_id = cc.mco_id
    join junk.ip_keepers_new k on k.member_id = m.id
    join ref.dates d on d.day between cc.start_date and cc.end_date
    join eligibility_days ed_eom on ed_eom.member_id = m.id and ed_eom.mco_id = cc.mco_id and ed_eom.date = d.eom
    join eligibility_days ed     on ed.member_id     = m.id and ed.mco_id     = cc.mco_id and ed.date = d.day
    left join claims c           on c.member_id      = m.id and  c.mco_id     = cc.mco_id and c.date_from = d.day
where
    cc.mco_id = (select min(mco_id) mco_id from _controls ctrls where not exists(select 1 from junk.ip_features2_new n where n.mco_id = ctrls.mco_id))

    --cc.mco_id in (1, 2, 3, 4)
    -- cc.mco_id in (5, 6, 7)
--      cc.mco_id in (8,12,13)
group by 1,2,3,4,5,6,7,8,9,10,11
;

drop table if exists junk.ip_chronic_features_new;
create table junk.ip_chronic_features_new as
select
    md.mco_id,
    md.member_id,
    d.eom,
    ---
    count(distinct md.id) filter ( where hcbs_pers_tc > 0 ) hcbs_pers_ddos ,
    count(distinct md.id) filter ( where hcbs_assist_tech_tc > 0 ) hcbs_assist_tech_ddos ,
    count(distinct md.id) filter ( where oxygen_tc > 0 ) oxygen_ddos ,
    count(distinct md.id) filter ( where hosp_bed_tc > 0 ) hosp_bed_ddos ,
    count(distinct md.id) filter ( where chf_tc > 0 ) chf_ddos ,
    count(distinct md.id) filter ( where heart_tc > 0 ) heart_ddos ,
    count(distinct md.id) filter ( where copd_tc > 0 ) copd_ddos ,
    count(distinct md.id) filter ( where pulmonar_tc > 0 ) pulmonar_ddos ,
    count(distinct md.id) filter ( where cancer_tc > 0 ) cancer_ddos ,
    count(distinct md.id) filter ( where ckd_tc > 0 ) ckd_ddos ,
    count(distinct md.id) filter ( where esrd_tc > 0 ) esrd_ddos ,
    count(distinct md.id) filter ( where lipidy_tc > 0 ) lipidy_ddos ,
    count(distinct md.id) filter ( where diab_tc > 0 ) diab_ddos ,
    count(distinct md.id) filter ( where alzh_tc > 0 ) alzh_ddos ,
    count(distinct md.id) filter ( where demented_tc > 0 ) demented_ddos ,
    count(distinct md.id) filter ( where stroke_tc > 0 ) stroke_ddos ,
    count(distinct md.id) filter ( where hyper_tc > 0 ) hyper_ddos ,
    count(distinct md.id) filter ( where fall_tc > 0 ) fall_ddos ,
    count(distinct md.id) filter ( where trans_tc > 0 ) trans_ddos ,
    count(distinct md.id) filter ( where liver_tc > 0 ) liver_ddos ,
    count(distinct md.id) filter ( where hippy_tc > 0 ) hippy_ddos ,
    count(distinct md.id) filter ( where depressed_tc > 0 ) depressed_ddos ,
    count(distinct md.id) filter ( where psycho_tc > 0 ) psycho_ddos ,
    count(distinct md.id) filter ( where druggy_tc > 0 ) druggy_ddos ,
    count(distinct md.id) filter ( where boozy_tc > 0 ) boozy_ddos ,
    count(distinct md.id) filter ( where paralyzed_tc > 0 ) paralyzed_ddos ,
    count(distinct md.id) filter ( where mono_tc > 0 ) mono_ddos ,
    count(distinct md.id) filter ( where mono_dom_tc > 0 ) mono_dom_ddos ,
    count(distinct md.id) filter ( where hemi_tc > 0 ) hemi_ddos ,
    count(distinct md.id) filter ( where hemi_dom_tc > 0 ) hemi_dom_ddos ,
    count(distinct md.id) filter ( where para_tc > 0 ) para_ddos ,
    count(distinct md.id) filter ( where quad_tc > 0 ) quad_ddos ,
    count(distinct md.id) filter ( where tbi_tc > 0 ) tbi_ddos ,
    count(distinct md.id) filter ( where obese_tc > 0 ) obese_ddos ,
    count(distinct md.id) filter ( where pressure_ulcer_tc > 0 ) pressure_ulcer_ddos ,
    count(distinct md.id) filter ( where hemophilia_tc > 0 ) hemophilia_ddos,
    sum(hcbs_pers_tc)            hcbs_pers_tc,
    sum(hcbs_assist_tech_tc)     hcbs_assist_tech_tc,
    sum(oxygen_tc)               oxygen_tc,
    sum(hosp_bed_tc)             hosp_bed_tc,
    sum(chf_tc)                  chf_tc,
    sum(heart_tc)                heart_tc,
    sum(copd_tc)                 copd_tc,
    sum(pulmonar_tc)             pulmonar_tc,
    sum(cancer_tc)               cancer_tc,
    sum(ckd_tc)                  ckd_tc,
    sum(esrd_tc)                 esrd_tc,
    sum(lipidy_tc)               lipidy_tc,
    sum(diab_tc)                 diab_tc,
    sum(alzh_tc)                 alzh_tc,
    sum(demented_tc)             demented_tc,
    sum(stroke_tc)               stroke_tc,
    sum(hyper_tc)                hyper_tc,
    sum(fall_tc)                 fall_tc,
    sum(trans_tc)                trans_tc,
    sum(liver_tc)                liver_tc,
    sum(hippy_tc)                hippy_tc,
    sum(depressed_tc)            depressed_tc,
    sum(psycho_tc)               psycho_tc,
    sum(druggy_tc)               druggy_tc,
    sum(boozy_tc)                boozy_tc,
    sum(paralyzed_tc)            paralyzed_tc,
    sum(mono_tc)                 mono_tc,
    sum(mono_dom_tc)             mono_dom_tc,
    sum(hemi_tc)                 hemi_tc,
    sum(hemi_dom_tc)             hemi_dom_tc,
    sum(para_tc)                 para_tc,
    sum(quad_tc)                 quad_tc,
    sum(tbi_tc)                  tbi_tc,
    sum(obese_tc)                obese_tc,
    sum(pressure_ulcer_tc)       pressure_ulcer_tc,
    sum(hemophilia_tc)           hemophilia_tc
from
    _controls cc
    join members m      on m.mco_id     = cc.mco_id
    join junk.ip_keepers_new k on k.member_id = m.id
    join member_days md on md.member_id = m.id      and md.mco_id = m.mco_id and md.date between cc.start_date and cc.end_date
    join ref.dates d    on d.day        = md.date
group by 1,2,3
;

create index idx_jnk_ic_ftr_new on junk.ip_chronic_features_new(mco_id, member_id, eom);
create index idx_jnk_ip_ftr_new on junk.ip_features2_new(mco_id, member_id, eom);

;
drop table if exists junk.ip_features_all_new_new;
create table junk.ip_features_all_new_new as
select
    ipf.line_of_business_id in (1,3)
    and ipf.ggroup > 1
    and ipf.is_unaligned is false
    and ipf.age > 20 is_cb_eligible,
    mc.id mco_id,
    mc.name mco_name,
    mc.state mco_state,
    ipf.eom,
    ipf.member_id,
    ipf.line_of_business_id,
    ipf.ggroup,
    ipf.is_unaligned,
    ipf.age,
    ipf.gender,
    ipf.cwmm,
    ipf.days_in_month,
    ipf.cpmm,
    ipf.rx_tc,
    ipf.other_tc,
    ipf.ip_tc,
    ipf.er_tc,
    ipf.out_tc,
    ipf.snf_tc,
    ipf.icf_tc,
    ipf.hh_tc,
    ipf.amb_tc,
    ipf.hsp_tc,
    ipf.pro_tc,
    ipf.spc_fac_tc,
    ipf.dme_tc,
    ipf.cls_tc,
    ipf.hha_tc,
    ipf.hcbs_attdpcs_tc,
    ipf.hcbs_other_tc,
    ipf.hcbs_support_house_tc,
    ipf.hcbs_adult_day_tc,
    ipf.ip_ddos_span,
    ipf.er_ddos_span,
    ipf.out_ddos_span,
    ipf.snf_ddos_span,
    ipf.icf_ddos_span,
    ipf.hh_ddos_span,
    ipf.amb_ddos_span,
    ipf.hsp_ddos_span,
    ipf.pro_ddos_span,
    ipf.spc_fac_ddos_span,
    ipf.dme_ddos_span,
    ipf.cls_ddos_span,
    ipf.hha_ddos_span,
    ipf.hcbs_attdpcs_ddos_span,
    ipf.hcbs_other_ddos_span,
    ipf.hcbs_support_house_ddos_span,
    ipf.hcbs_adult_day_ddos_span,
    ipf.other_ddos_span,
    ipf.ip_ddos,
    ipf.er_ddos,
    ipf.out_ddos,
    ipf.snf_ddos,
    ipf.icf_ddos,
    ipf.hh_ddos,
    ipf.amb_ddos,
    ipf.hsp_ddos,
    ipf.pro_ddos,
    ipf.spc_fac_ddos,
    ipf.dme_ddos,
    ipf.cls_ddos,
    ipf.hha_ddos,
    ipf.hcbs_attdpcs_ddos,
    ipf.hcbs_other_ddos,
    ipf.hcbs_support_house_ddos,
    ipf.hcbs_adult_day_ddos,
    ipf.other_ddos,
    coalesce(hcbs_pers_ddos          ,0) hcbs_pers_ddos,
    coalesce(hcbs_assist_tech_ddos   ,0) hcbs_assist_tech_ddos,
    coalesce(oxygen_ddos             ,0) oxygen_ddos,
    coalesce(hosp_bed_ddos           ,0) hosp_bed_ddos,
    coalesce(chf_ddos                ,0) chf_ddos,
    coalesce(heart_ddos              ,0) heart_ddos,
    coalesce(copd_ddos               ,0) copd_ddos,
    coalesce(pulmonar_ddos           ,0) pulmonar_ddos,
    coalesce(cancer_ddos             ,0) cancer_ddos,
    coalesce(ckd_ddos                ,0) ckd_ddos,
    coalesce(esrd_ddos               ,0) esrd_ddos,
    coalesce(lipidy_ddos             ,0) lipidy_ddos,
    coalesce(diab_ddos               ,0) diab_ddos,
    coalesce(alzh_ddos               ,0) alzh_ddos,
    coalesce(demented_ddos           ,0) demented_ddos,
    coalesce(stroke_ddos             ,0) stroke_ddos,
    coalesce(hyper_ddos              ,0) hyper_ddos,
    coalesce(fall_ddos               ,0) fall_ddos,
    coalesce(trans_ddos              ,0) trans_ddos,
    coalesce(liver_ddos              ,0) liver_ddos,
    coalesce(hippy_ddos              ,0) hippy_ddos,
    coalesce(depressed_ddos          ,0) depressed_ddos,
    coalesce(psycho_ddos             ,0) psycho_ddos,
    coalesce(druggy_ddos             ,0) druggy_ddos,
    coalesce(boozy_ddos              ,0) boozy_ddos,
    coalesce(paralyzed_ddos          ,0) paralyzed_ddos,
    coalesce(mono_ddos               ,0) mono_ddos,
    coalesce(mono_dom_ddos           ,0) mono_dom_ddos,
    coalesce(hemi_ddos               ,0) hemi_ddos,
    coalesce(hemi_dom_ddos           ,0) hemi_dom_ddos,
    coalesce(para_ddos               ,0) para_ddos,
    coalesce(quad_ddos               ,0) quad_ddos,
    coalesce(tbi_ddos                ,0) tbi_ddos,
    coalesce(obese_ddos              ,0) obese_ddos,
    coalesce(pressure_ulcer_ddos     ,0) pressure_ulcer_ddos,
    coalesce(hemophilia_ddos         ,0) hemophilia_ddos,
    coalesce(hcbs_pers_tc            ,0) hcbs_pers_tc,
    coalesce(hcbs_assist_tech_tc     ,0) hcbs_assist_tech_tc,
    coalesce(oxygen_tc               ,0) oxygen_tc,
    coalesce(hosp_bed_tc             ,0) hosp_bed_tc,
    coalesce(chf_tc                  ,0) chf_tc,
    coalesce(heart_tc                ,0) heart_tc,
    coalesce(copd_tc                 ,0) copd_tc,
    coalesce(pulmonar_tc             ,0) pulmonar_tc,
    coalesce(cancer_tc               ,0) cancer_tc,
    coalesce(ckd_tc                  ,0) ckd_tc,
    coalesce(esrd_tc                 ,0) esrd_tc,
    coalesce(lipidy_tc               ,0) lipidy_tc,
    coalesce(diab_tc                 ,0) diab_tc,
    coalesce(alzh_tc                 ,0) alzh_tc,
    coalesce(demented_tc             ,0) demented_tc,
    coalesce(stroke_tc               ,0) stroke_tc,
    coalesce(hyper_tc                ,0) hyper_tc,
    coalesce(fall_tc                 ,0) fall_tc,
    coalesce(trans_tc                ,0) trans_tc,
    coalesce(liver_tc                ,0) liver_tc,
    coalesce(hippy_tc                ,0) hippy_tc,
    coalesce(depressed_tc            ,0) depressed_tc,
    coalesce(psycho_tc               ,0) psycho_tc,
    coalesce(druggy_tc               ,0) druggy_tc,
    coalesce(boozy_tc                ,0) boozy_tc,
    coalesce(paralyzed_tc            ,0) paralyzed_tc,
    coalesce(mono_tc                 ,0) mono_tc,
    coalesce(mono_dom_tc             ,0) mono_dom_tc,
    coalesce(hemi_tc                 ,0) hemi_tc,
    coalesce(hemi_dom_tc             ,0) hemi_dom_tc,
    coalesce(para_tc                 ,0) para_tc,
    coalesce(quad_tc                 ,0) quad_tc,
    coalesce(tbi_tc                  ,0) tbi_tc,
    coalesce(obese_tc                ,0) obese_tc,
    coalesce(pressure_ulcer_tc       ,0) pressure_ulcer_tc,
    coalesce(hemophilia_tc           ,0) hemophilia_tc
from
    junk.ip_features2_new ipf
    join mcos mc on mc.id = ipf.mco_id
    left join junk.ip_chronic_features_new icf on ipf.member_id = icf.member_id and ipf.eom = icf.eom and ipf.mco_id = icf.mco_id

;


select count(*) from junk.ip_features_all_new_new
-- 14,501,885
select * from junk.ip_features_all_new_new
-- select * from recon.vm_c_batch_latest_mm_costs_for_all_mcos where v.

select distinct x.mco_id from junk.ip_features_all_new x order by 1

select count(1) from junk.ip_features_all_new x where x.mco_id = 13