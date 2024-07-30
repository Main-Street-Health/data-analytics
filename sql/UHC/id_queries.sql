set schema 'cb';

-- view
-- 2017-2018
-- predict on 2019
-- sp_mab_mbr(_descr text, _mco_id integer, _n_pre_periods integer, _n_post_periods integer, _id_date date, _exclude_bh boolean DEFAULT false)
call sp_mab_mbr('2017_train_v2_copd_chf_heart', 2, 12, 12, '2017-12-31');
call sp_mab_mbr('2017_train_v2_copd_chf_heart', 2, 12, 12, '2018-12-31');

call sp_mab_mbr('1st ID UHC', 2, 12, 0, '2019-12-31');
select * from mabs m;


-- sp_mab_poo_etr(_mco_id integer, _n_pre_periods integer, _n_post_periods integer, _id_date date, _mab_id bigint)
call sp_mab_poo_etr_target_mkr(3);
call sp_mab_poo_etr_target_mkr(4);


create table junk.mab_member_targets as
select * from mab_member_targets mmt where mmt.mab_id in (3,4);

select
    mmt.lvl,
    avg(mmt.savings - trsh.savings)::decimal(16,2) delta_avg,
    min(mmt.savings - trsh.savings) delta_min,
    max(mmt.savings - trsh.savings) delta_max,
    count(mmt.member_id) n,
    count(distinct mmt.member_id) nd,
    count(distinct trsh.member_id) nd
from
  mab_member_targets mmt
  left join junk.mab_member_targets trsh on trsh.member_id = mmt.member_id and trsh.is_pre = mmt.is_pre and trsh.lvl = mmt.lvl and trsh.mab_id = mmt.mab_id
where
    mmt.mab_id in (4)
    and mmt.is_pre = 1
group by 1
order by 1,2 desc;


select count(member_id), count(distinct member_id) from mab_poo mp where mp.mab_id = 5;
-- call sp_mab_poo_etr_target_mkr(5);
select count(distinct mmt.member_id) from mab_member_targets mmt
where mmt.mab_id = 5;


select heart_ddos, chf_ddos, copd_ddos from vw_mab_training_data vmtd where mab_id = 4;

select * from mabs m;
truncate mabs restart identity ;
truncate mab_poo  restart identity ;
truncate mab_member_targets  restart identity ;
truncate mab_eol  restart identity ;
truncate mab_excluded_members  restart identity ;
truncate mab_member_target_economic_savings  restart identity ;


select * from mabs m;
call zz_sp_mab_mbr_no_rebuild('2018_t', 2, 12, 12, '2018-12-31');
call sp_mab_poo_etr_target_mkr(1)

select avg(vt.is_male), avg(vt.age) from vw_mab_training_data vt where vt.mab_id = 1
;
call zz_sp_mab_mbr_no_rebuild('2017_t', 2, 12, 12, '2017-12-31');
call sp_mab_poo_etr_target_mkr(2)
select distinct mp.mab_id from mab_poo mp;
select distinct mp.mab_id from mab_member_targets mp;


select distinct member_id, t.savings_tgt, t.savings_ft, t.ip_savings_tgt from vw_mab_training_data t where t.mab_id = 1 and t.lvl_tgt ~ '2. m' order by 2, 1;

with mmabs as ( select unnest(Array[1,2]) mab_id),
    pre as (
        select mmt.*,
            row_number() over (partition by lvl, mmt.mab_id order by mmt.savings desc) rn
            --row_number() over (partition by lvl order by random(), member_id desc) rn
        from mab_member_targets mmt
             join mmabs m on mmt.mab_id = m.mab_id and is_pre = 1

    ),
    post as (
        select mmt.*,
        row_number() over (partition by lvl, mmt.mab_id order by mmt.savings desc, mmt.member_id) rn
        from mab_member_targets mmt
             join mmabs m on mmt.mab_id = m.mab_id and is_pre = 0
    ),
    tranches as (
        select * from (values (500), (750), (1000), (1500)) x(num)
        -- select * from (values (2051)) x(num)
    ),
    tot_pop as (
        select mmt.mab_id, count(distinct member_id) nd
        from mab_member_targets mmt
             join mmabs m on mmt.mab_id = m.mab_id and is_pre = 0
        group by 1
    )
    select
        pre.mab_id,
        pre.lvl,
        post.lvl,
        t.num,
        tp.nd,
        (t.num * 100.0 / tp.nd)::decimal(16,2) propotion,
        (count(distinct post.member_id) filter (where post.rn <= t.num) * 100.0 / t.num)::decimal(16,2) pct_correct,
        (count(distinct post.member_id) filter (where post.rn >= (tp.nd - (tp.nd * .20))) * 100.0 / t.num)::decimal(16,2) pct_dead_wrong,
        avg(pre.ip_pmpm )::decimal(16,2) pre_ip_pmpm,
        avg(post.ip_pmpm)::decimal(16,2) prost_ip_pmpm,
        (sum(post.savings) / sum(post.mm))::decimal(16,2) sav_pmpm
    from
        post
        join pre on pre.member_id = post.member_id and post.mab_id = pre.mab_id and post.lvl = pre.lvl
        join tot_pop tp on tp.mab_id = pre.mab_id
        join tranches t on pre.rn <= t.num and t.num = 500
    group by 1, 2, 3, 4, 5, 6
    order by 1, 2, 3, 4, 5, 6
;

refresh materialized view vw_claim_member_months;
refresh materialized view vw_member_month_eligibility;


select t.lvl_tgt, sum(t.savings_tgt) / sum(t.mm_tgt) sav_pmpm from vw_mab_training_data t where t.mab_id = 1 and t.lvl_tgt ~ '1' group by 1 union all
select t.lvl_tgt, sum(t.savings_tgt) / sum(t.mm_tgt) sav_pmpm from vw_mab_training_data t where t.mab_id = 1 and t.lvl_tgt ~ '2' group by 1 union all
select t.lvl_tgt, sum(t.savings_tgt) / sum(t.mm_tgt) sav_pmpm from vw_mab_training_data t where t.mab_id = 1 and t.lvl_tgt ~ '3' group by 1;
-- select lvl_ft, member_id, t.savings_ft, t.savings_tgt from vw_mab_training_data t where t.mab_id = 1 and member_id = 3048;



select c.paid_date from claims c where c.mco_id = 2;

    drop table if exists _dates;
    create temporary table _dates as
    select
        distinct d.year yr, d.bom start_date, d.eom end_date, 2 mco_id
    from
        ref.dates d
    where
        d.year between 2017 and 2019
    ;

select coalesce(i.cat_3, i.cat_2, i.cat_1) from ref.icd10s i

    drop table if exists _pre_post_mm;
    create temporary table _pre_post_mm as
    select
        m.id member_id, yr, mp.mco_id, start_date, end_date, m.date_of_death,
        case when m.date_of_death <= end_date then true else false end     is_eol,
        count(distinct case when line_of_business_id = 1 then ed.date end) _lob_1_days,
        count(distinct case when line_of_business_id = 2 then ed.date end) _lob_2_days,
        count(distinct case when line_of_business_id = 3 then ed.date end) _lob_3_days,
        count(distinct case when ed.ggroup::int = 1 then ed.date end) _grp_1_days,
        count(distinct case when ed.ggroup::int = 2 then ed.date end) _grp_2_days,
        count(distinct case when ed.ggroup::int = 3 then ed.date end) _grp_3_days,
        count(distinct case when ed.ggroup::int = 5 then ed.date end) _grp_5_days,
        count(distinct case when ed.ggroup::int = 6 then ed.date end) _grp_6_days,
        count(distinct case when ed.ggroup::int = 7 then ed.date end) _grp_7_days,
        count(distinct case when ed.ggroup::int = 8 then ed.date end) _grp_8_days,
        count(distinct case when ed.is_unaligned then ed.date end) _unaligned_days,
        --max(case when mtp.mm_eol = '-1' then ed.line_of_business_id end) lob,
        --max(case when mtp.mm_eol = '-1' then              ed.ggroup end) grp,
        bool_or(ed.is_unaligned) is_unaligned,
        sum(md.total_$ ) tc,
        sum(md.hcbs_$  ) hcbs_tc,
        sum(md.icf_$   ) icf_tc,
        sum(md.ip_$    ) ip_tc,
        sum(md.rx_$    ) rx_tc,
        sum(md.ed_$    ) ed_tc,
        sum(md.snf_$   ) snf_tc,
        sum(md.out_$   ) out_tc,
        sum(md.pro_$   ) pro_tc,
        sum(md.spfac_$ ) spfac_tc,
        sum(md.amb_$   ) amb_tc,
        sum(md.hh_$    ) hh_tc,
        sum(md.hosp_$  ) hosp_tc,
        sum(md.oth_$   ) oth_tc,

        (count(distinct ed.date) * 1.0 / 30)::decimal(16,2) p_mm,
        max(case when ed.id is not null then 1 end) mm,

        sum(hcbs_respite_$        ) hcbs_respite_tc,
        sum(hcbs_fam_care_stip_$  ) hcbs_fam_care_stip_tc,
        sum(hcbs_com_trans_$      ) hcbs_com_trans_tc,
        sum(hcbs_educ_train_$     ) hcbs_educ_train_tc,
        sum(hcbs_com_liv_fam_$    ) hcbs_com_liv_fam_tc,
        sum(hcbs_com_liv_$        ) hcbs_com_liv_tc,
        sum(hcbs_attend_care_$    ) hcbs_attend_care_tc,
        sum(hcbs_com_trans_waiv_$ ) hcbs_com_trans_waiv_tc,
        sum(hcbs_home_meal_$      ) hcbs_home_meal_tc,
        sum(hcbs_pers_care_$      ) hcbs_pers_care_tc,
        sum(hcbs_ther_behav_$     ) hcbs_ther_behav_tc,
        sum(hcbs_unsk_respite_$   ) hcbs_unsk_respite_tc,
        sum(hcbs_waiv_svc_$       ) hcbs_waiv_svc_tc,
        sum(case when md.total_$ > 0 then 1 end ) ddos,
        sum(case when md.hcbs_$  > 0 then 1 end ) hcbs_ddos,
        sum(case when md.icf_$   > 0 then 1 end ) icf_ddos,
        sum(case when md.ip_$    > 0 then 1 end ) ip_ddos,
        sum(case when md.rx_$    > 0 then 1 end ) rx_ddos,
        sum(case when md.ed_$    > 0 then 1 end ) ed_ddos,
        sum(case when md.snf_$   > 0 then 1 end ) snf_ddos,
        sum(case when md.out_$   > 0 then 1 end ) out_ddos,
        sum(case when md.pro_$   > 0 then 1 end ) pro_ddos,
        sum(case when md.spfac_$ > 0 then 1 end ) spfac_ddos,
        sum(case when md.amb_$   > 0 then 1 end ) amb_ddos,
        sum(case when md.hh_$    > 0 then 1 end ) hh_ddos,
        sum(case when md.hosp_$  > 0 then 1 end ) hosp_ddos,
        sum(case when md.oth_$   > 0 then 1 end ) oth_ddos,
        sum(case when is_pcp         then 1 end ) pcp_ddos,
        sum(case when is_pulmonar    then 1 end ) pulmonar_ddos,
        sum(case when is_cancer      then 1 end ) cancer_ddos,
        sum(case when is_ckd         then 1 end ) ckd_ddos,
        sum(case when is_esrd        then 1 end ) esrd_ddos,
        sum(case when is_lipidy      then 1 end ) hyperlipid_ddos,
        sum(case when is_diab        then 1 end ) diab_ddos,
        sum(case when is_alzh        then 1 end ) alzh_ddos,
        sum(case when is_demented    then 1 end ) dementia_ddos,
        sum(case when is_stroke      then 1 end ) stroke_ddos,
        sum(case when is_hyper       then 1 end ) hypertension_ddos,
        sum(case when is_fall        then 1 end ) fall_ddos,
        sum(case when is_trans       then 1 end ) transplant_ddos,
        sum(case when is_liver       then 1 end ) liver_ddos,
        sum(case when is_hippy       then 1 end ) hippfract_ddos,
        sum(case when is_depressed   then 1 end ) depression_ddos,
        sum(case when is_psycho      then 1 end ) psychosis_ddos,
        sum(case when is_drugy       then 1 end ) drug_ddos,
        sum(case when is_boozy       then 1 end ) alcohol_ddos,
        sum(case when is_paralyzed   then 1 end ) paralysis_ddos
    from
        _dates mp
        join members m on m.mco_id = mp.mco_id
        join eligibility_days ed on ed.member_id = m.id
                                         and ed.mco_id = mp.mco_id
                                         and ed.ggroup::int in (2,3)
                                         and ed.date between mp.start_date and mp.end_date
        left join member_days      md on md.member_id = m.id
                                             and md.mco_id = mp.mco_id
                                             and md.date = ed.date
                                          --   and (_exclude_bh is false or md.has_bh is false)
--    where
--        (ed.id is not null or md.id is not null)
    group by 1, 2, 3, 4, 5, 6, 7
    order by 1, 2, 3, 4 desc, 5 desc;



    select
        mco_id, start_date, end_date, count(distinct mm.member_id) nd_mbr, (count(case when is_eol then 1 end) * 100.0 / count(distinct member_id))::decimal(16,2) pct_eol,
        --_lob_1_days, _lob_2_days, _lob_3_days, _grp_1_days, _grp_2_days, _grp_3_days, _grp_5_days, _grp_6_days, _grp_7_days, _grp_8_days, _unaligned_days, is_unaligned,
        (sum(tc) / sum(p_mm))::decimal(16,2) pmpm, (sum(hcbs_tc) / sum(p_mm))::decimal(16,2) hcbs_pmpm, (sum(icf_tc) / sum(p_mm))::decimal(16,2) icf_pmpm, (sum(ip_tc) / sum(p_mm))::decimal(16,2) ip_pmpm, (sum(rx_tc) / sum(p_mm))::decimal(16,2) rx_pmpm, (sum(ed_tc) / sum(p_mm))::decimal(16,2) ed_pmpm, (sum(snf_tc) / sum(p_mm))::decimal(16,2) snf_pmpm, (sum(out_tc) / sum(p_mm))::decimal(16,2) out_pmpm, (sum(pro_tc) / sum(p_mm))::decimal(16,2) pro_pmpm, (sum(spfac_tc) / sum(p_mm))::decimal(16,2) spfac_pmpm, (sum(amb_tc) / sum(p_mm))::decimal(16,2) amb_pmpm, (sum(hh_tc) / sum(p_mm))::decimal(16,2) hh_pmpm, (sum(hosp_tc) / sum(p_mm))::decimal(16,2) hosp_pmpm, (sum(oth_tc) / sum(p_mm))::decimal(16,2) oth_pmpm,
        (sum(hcbs_respite_tc) / sum(p_mm))::decimal(16,2) hcbs_respite_pmpm, (sum(hcbs_fam_care_stip_tc) / sum(p_mm))::decimal(16,2) hcbs_fam_care_stip_pmpm, (sum(hcbs_com_trans_tc) / sum(p_mm))::decimal(16,2) hcbs_com_trans_pmpm, (sum(hcbs_educ_train_tc) / sum(p_mm))::decimal(16,2) hcbs_educ_train_pmpm, (sum(hcbs_com_liv_fam_tc) / sum(p_mm))::decimal(16,2) hcbs_com_liv_fam_pmpm, (sum(hcbs_com_liv_tc) / sum(p_mm))::decimal(16,2) hcbs_com_liv_pmpm, (sum(hcbs_attend_care_tc) / sum(p_mm))::decimal(16,2) hcbs_attend_care_pmpm, (sum(hcbs_com_trans_waiv_tc) / sum(p_mm))::decimal(16,2) hcbs_com_trans_waiv_pmpm, (sum(hcbs_home_meal_tc) / sum(p_mm))::decimal(16,2) hcbs_home_meal_pmpm, (sum(hcbs_pers_care_tc) / sum(p_mm))::decimal(16,2) hcbs_pers_care_pmpm, (sum(hcbs_ther_behav_tc) / sum(p_mm))::decimal(16,2) hcbs_ther_behav_pmpm, (sum(hcbs_unsk_respite_tc) / sum(p_mm))::decimal(16,2) hcbs_unsk_respite_pmpm, (sum(hcbs_waiv_svc_tc)/ sum(p_mm))::decimal(16,2) hcbs_waiv_svc_tc,
        sum(p_mm) pmm, sum(mm) mm,
        sum(hcbs_respite_tc) hcbs_respite_tc, sum(hcbs_fam_care_stip_tc) hcbs_fam_care_stip_tc, sum(hcbs_com_trans_tc) hcbs_com_trans_tc, sum(hcbs_educ_train_tc) hcbs_educ_train_tc, sum(hcbs_com_liv_fam_tc) hcbs_com_liv_fam_tc, sum(hcbs_com_liv_tc) hcbs_com_liv_tc, sum(hcbs_attend_care_tc) hcbs_attend_care_tc, sum(hcbs_com_trans_waiv_tc) hcbs_com_trans_waiv_tc, sum(hcbs_home_meal_tc) hcbs_home_meal_tc, sum(hcbs_pers_care_tc) hcbs_pers_care_tc, sum(hcbs_ther_behav_tc) hcbs_ther_behav_tc, sum(hcbs_unsk_respite_tc) hcbs_unsk_respite_tc, sum(hcbs_waiv_svc_tc) hcbs_waiv_svc_tc,
        sum(tc) tc, sum(hcbs_tc) hcbs_tc, sum(icf_tc) icf_tc, sum(ip_tc) ip_tc, sum(rx_tc) rx_tc, sum(ed_tc) ed_tc, sum(snf_tc) snf_tc, sum(out_tc) out_tc, sum(pro_tc) pro_tc, sum(spfac_tc) spfac_tc, sum(amb_tc) amb_tc, sum(hh_tc) hh_tc, sum(hosp_tc) hosp_tc, sum(oth_tc) oth_tc,
        sum(case when hcbs_ddos > 0  then 1 end) hcbs_ddos, sum(case when icf_ddos > 0  then 1 end) icf_ddos, sum(case when ip_ddos > 0  then 1 end) ip_ddos, sum(case when rx_ddos > 0  then 1 end) rx_ddos, sum(case when ed_ddos > 0  then 1 end) ed_ddos, sum(case when snf_ddos > 0  then 1 end) snf_ddos, sum(case when out_ddos > 0  then 1 end) out_ddos, sum(case when pro_ddos > 0  then 1 end) pro_ddos, sum(case when spfac_ddos > 0  then 1 end) spfac_ddos, sum(case when amb_ddos > 0  then 1 end) amb_ddos, sum(case when hh_ddos > 0  then 1 end) hh_ddos, sum(case when hosp_ddos > 0  then 1 end) hosp_ddos, sum(case when oth_ddos > 0  then 1 end) oth_ddos, sum(case when pcp_ddos > 0  then 1 end) pcp_ddos, sum(case when pulmonar_ddos > 0  then 1 end) pulmonar_ddos, sum(case when cancer_ddos > 0  then 1 end) cancer_ddos, sum(case when ckd_ddos > 0  then 1 end) ckd_ddos, sum(case when esrd_ddos > 0  then 1 end) esrd_ddos, sum(case when hyperlipid_ddos > 0  then 1 end) hyperlipid_ddos, sum(case when diab_ddos > 0  then 1 end) diab_ddos, sum(case when alzh_ddos > 0  then 1 end) alzh_ddos, sum(case when dementia_ddos > 0  then 1 end) dementia_ddos, sum(case when stroke_ddos > 0  then 1 end) stroke_ddos, sum(case when hypertension_ddos > 0  then 1 end) hypertension_ddos, sum(case when fall_ddos > 0  then 1 end) fall_ddos, sum(case when transplant_ddos > 0  then 1 end) transplant_ddos, sum(case when liver_ddos > 0  then 1 end) liver_ddos, sum(case when hippfract_ddos > 0  then 1 end) hippfract_ddos, sum(case when depression_ddos > 0  then 1 end) depression_ddos, sum(case when psychosis_ddos > 0  then 1 end) psychosis_ddos, sum(case when drug_ddos > 0  then 1 end) drug_ddos, sum(case when alcohol_ddos > 0  then 1 end) alcohol_ddos, sum(case when paralysis_ddos > paralysis_ddos then 1 end) paralysis_ddos
    from
        _pre_post_mm mm
    group by 1,2,3
    order by 1,2,3


    select
        x.mco_id, yr, count(member_id) nd_mbrs, (count(case when is_eol = 1 then 1 end) * 100.0 / count(distinct member_id))::decimal(16,2) pct_eol,
        count(distinct case when lob = 3 then member_id end) nd_aligned,
        count(distinct case when lob = 1 and is_unaligned then member_id end) nd_unaligned_duals,
        count(distinct case when lob = 1 and is_unaligned = false then member_id end) nd_medicaid,
        (sum(tc) / sum(p_mm))::decimal(16,2) pmpm, (sum(hcbs_tc) / sum(p_mm))::decimal(16,2) hcbs_pmpm, (sum(icf_tc) / sum(p_mm))::decimal(16,2) icf_pmpm, (sum(ip_tc) / sum(p_mm))::decimal(16,2) ip_pmpm, (sum(rx_tc) / sum(p_mm))::decimal(16,2) rx_pmpm, (sum(ed_tc) / sum(p_mm))::decimal(16,2) ed_pmpm, (sum(snf_tc) / sum(p_mm))::decimal(16,2) snf_pmpm, (sum(out_tc) / sum(p_mm))::decimal(16,2) out_pmpm, (sum(pro_tc) / sum(p_mm))::decimal(16,2) pro_pmpm, (sum(spfac_tc) / sum(p_mm))::decimal(16,2) spfac_pmpm, (sum(amb_tc) / sum(p_mm))::decimal(16,2) amb_pmpm, (sum(hh_tc) / sum(p_mm))::decimal(16,2) hh_pmpm, (sum(hosp_tc) / sum(p_mm))::decimal(16,2) hosp_pmpm, (sum(oth_tc) / sum(p_mm))::decimal(16,2) oth_pmpmc,
        (sum(hcbs_respite_tc) / sum(p_mm))::decimal(16,2) hcbs_respite_pmpm, (sum(hcbs_fam_care_stip_tc) / sum(p_mm))::decimal(16,2) hcbs_fam_care_stip_pmpm, (sum(hcbs_com_trans_tc) / sum(p_mm))::decimal(16,2) hcbs_com_trans_pmpm, (sum(hcbs_educ_train_tc) / sum(p_mm))::decimal(16,2) hcbs_educ_train_pmpm, (sum(hcbs_com_liv_fam_tc) / sum(p_mm))::decimal(16,2) hcbs_com_liv_fam_pmpm, (sum(hcbs_com_liv_tc) / sum(p_mm))::decimal(16,2) hcbs_com_liv_pmpm, (sum(hcbs_attend_care_tc) / sum(p_mm))::decimal(16,2) hcbs_attend_care_pmpm, (sum(hcbs_com_trans_waiv_tc) / sum(p_mm))::decimal(16,2) hcbs_com_trans_waiv_pmpm, (sum(hcbs_home_meal_tc) / sum(p_mm))::decimal(16,2) hcbs_home_meal_pmpm, (sum(hcbs_pers_care_tc) / sum(p_mm))::decimal(16,2) hcbs_pers_care_pmpm, (sum(hcbs_ther_behav_tc) / sum(p_mm))::decimal(16,2) hcbs_ther_behav_pmpm, (sum(hcbs_unsk_respite_tc) / sum(p_mm))::decimal(16,2) hcbs_unsk_respite_pmpm, (sum(hcbs_waiv_svc_tc)/ sum(p_mm))::decimal(16,2) hcbs_waiv_svc_tc,
        sum(p_mm) p_mm, sum(mm) mm,
        sum(hcbs_respite_tc) hcbs_respite_tc, sum(hcbs_fam_care_stip_tc) hcbs_fam_care_stip_tc, sum(hcbs_com_trans_tc) hcbs_com_trans_tc, sum(hcbs_educ_train_tc) hcbs_educ_train_tc, sum(hcbs_com_liv_fam_tc) hcbs_com_liv_fam_tc, sum(hcbs_com_liv_tc) hcbs_com_liv_tc, sum(hcbs_attend_care_tc) hcbs_attend_care_tc, sum(hcbs_com_trans_waiv_tc) hcbs_com_trans_waiv_tc, sum(hcbs_home_meal_tc) hcbs_home_meal_tc, sum(hcbs_pers_care_tc) hcbs_pers_care_tc, sum(hcbs_ther_behav_tc) hcbs_ther_behav_tc, sum(hcbs_unsk_respite_tc) hcbs_unsk_respite_tc, sum(hcbs_waiv_svc_tc) hcbs_waiv_svc_tc,
        sum(tc) tc, sum(hcbs_tc) hcbs_tc, sum(icf_tc) icf_tc, sum(ip_tc) ip_tc, sum(rx_tc) rx_tc, sum(ed_tc) ed_tc, sum(snf_tc) snf_tc, sum(out_tc) out_tc, sum(pro_tc) pro_tc, sum(spfac_tc) spfac_tc, sum(amb_tc) amb_tc, sum(hh_tc) hh_tc, sum(hosp_tc) hosp_tc, sum(oth_tc) oth_tc,
        count(is_icf) n_icf, count(is_ip) n_ip, count(is_rx) n_rx, count(is_ed) n_ed, count(is_snf) n_snf, count(is_out) n_out, count(is_pro) n_pro, count(is_spfac) n_spfac, count(is_amb) n_amb, count(is_hh) n_hh, count(is_hosp) n_hosp, count(is_oth) n_oth, count(is_pcp) n_pcp, count(is_pulmonar) n_pulmonar, count(is_cancer) n_cancer, count(is_ckd) n_ckd, count(is_esrd) n_esrd, count(is_hyperlipid) n_hyperlipid, count(is_diab) n_diab, count(is_alzh) n_alzh, count(is_dementia) n_dementia, count(is_stroke) n_stroke, count(is_hypertension) n_hypertension, count(is_fall) n_fall, count(is_transplant) n_transplant, count(is_liver) n_liver, count(is_hippfract) n_hippfract, count(is_depression) n_depression, count(is_psychosis) n_psychosis, count(is_drug) n_drug, count(is_alcohol) n_alcohol, count(is_paralysis) n_paralysis
    from (
        select
            mco_id, yr, mm.member_id,
            case when _lob_3_days > _lob_2_days and _lob_3_days > _lob_1_days then 3
                 when _lob_2_days > _lob_1_days then 2
                 when _lob_1_days > 0 then 1
            end lob,

            is_unaligned,
            max(case when is_eol then 1 end) is_eol,
            --_lob_1_days, _lob_2_days, _lob_3_days, _grp_1_days, _grp_2_days, _grp_3_days, _grp_5_days, _grp_6_days, _grp_7_days, _grp_8_days, _unaligned_days, is_unaligned,
            sum(p_mm) p_mm, sum(mm) mm,
            sum(hcbs_respite_tc) hcbs_respite_tc, sum(hcbs_fam_care_stip_tc) hcbs_fam_care_stip_tc, sum(hcbs_com_trans_tc) hcbs_com_trans_tc, sum(hcbs_educ_train_tc) hcbs_educ_train_tc, sum(hcbs_com_liv_fam_tc) hcbs_com_liv_fam_tc, sum(hcbs_com_liv_tc) hcbs_com_liv_tc, sum(hcbs_attend_care_tc) hcbs_attend_care_tc, sum(hcbs_com_trans_waiv_tc) hcbs_com_trans_waiv_tc, sum(hcbs_home_meal_tc) hcbs_home_meal_tc, sum(hcbs_pers_care_tc) hcbs_pers_care_tc, sum(hcbs_ther_behav_tc) hcbs_ther_behav_tc, sum(hcbs_unsk_respite_tc) hcbs_unsk_respite_tc, sum(hcbs_waiv_svc_tc) hcbs_waiv_svc_tc,
            sum(tc) tc, sum(hcbs_tc) hcbs_tc, sum(icf_tc) icf_tc, sum(ip_tc) ip_tc, sum(rx_tc) rx_tc, sum(ed_tc) ed_tc, sum(snf_tc) snf_tc, sum(out_tc) out_tc, sum(pro_tc) pro_tc, sum(spfac_tc) spfac_tc, sum(amb_tc) amb_tc, sum(hh_tc) hh_tc, sum(hosp_tc) hosp_tc, sum(oth_tc) oth_tc,
            max(case when hcbs_ddos > 2  then 1 end) is_hcbs, max(case when icf_ddos > 2  then 1 end) is_icf, max(case when ip_ddos > 2  then 1 end) is_ip, max(case when rx_ddos > 2  then 1 end) is_rx, max(case when ed_ddos > 2  then 1 end) is_ed, max(case when snf_ddos > 2  then 1 end) is_snf, max(case when out_ddos > 2  then 1 end) is_out, max(case when pro_ddos > 2  then 1 end) is_pro, max(case when spfac_ddos > 2  then 1 end) is_spfac, max(case when amb_ddos > 2  then 1 end) is_amb, max(case when hh_ddos > 2  then 1 end) is_hh, max(case when hosp_ddos > 2  then 1 end) is_hosp, max(case when oth_ddos > 2  then 1 end) is_oth, max(case when pcp_ddos > 2  then 1 end) is_pcp, max(case when pulmonar_ddos > 2  then 1 end) is_pulmonar, max(case when cancer_ddos > 2  then 1 end) is_cancer, max(case when ckd_ddos > 2  then 1 end) is_ckd, max(case when esrd_ddos > 2  then 1 end) is_esrd, max(case when hyperlipid_ddos > 2  then 1 end) is_hyperlipid, max(case when diab_ddos > 2  then 1 end) is_diab, max(case when alzh_ddos > 2  then 1 end) is_alzh, max(case when dementia_ddos > 2  then 1 end) is_dementia, max(case when stroke_ddos > 2  then 1 end) is_stroke, max(case when hypertension_ddos > 2  then 1 end) is_hypertension, max(case when fall_ddos > 2  then 1 end) is_fall, max(case when transplant_ddos > 2  then 1 end) is_transplant, max(case when liver_ddos > 2  then 1 end) is_liver, max(case when hippfract_ddos > 2  then 1 end) is_hippfract, max(case when depression_ddos > 2  then 1 end) is_depression, max(case when psychosis_ddos > 2  then 1 end) is_psychosis, max(case when drug_ddos > 2  then 1 end) is_drug, max(case when alcohol_ddos > 2  then 1 end) is_alcohol, max(case when paralysis_ddos > paralysis_ddos then 1 end) is_paralysis
        from
            _pre_post_mm mm
        group by 1,2,3,4,5
    ) x
    --where lob = 3
    group by 1,2
    order by 1,2,3


------------------------------------------------------------------------------------------------------------
-- MODEL

select * from models m
select * from scoring_runs sr;
select * from member_scores ms;

create temporary table  _stack_rank as
SELECT
    row_number() over (order by s.pred desc) rn,
    s.pred,
    s.member_id
FROM junk.preds_v1_h_target_annual_feats s;



select
    lvl,
    count(         member_id) n,
    count(distinct member_id) nd,
    min(x.rn        ) min_rn,
    min(x.rn        ) max_rn,
    sum(savings     )::decimal(16,2) savings_tc,
    (sum(savings     ) / sum(mm))::decimal(16,2) savings_pmpm,
    avg(pred         )::decimal(16,2) avg_pred,
    (sum(savings     ) / count(member_id))::decimal(16,2) sav_pm,
    (sum(raf_savings ) / count(member_id))::decimal(16,2) raf_sav_pm,
    (sum(ds_savings  ) / count(member_id))::decimal(16,2) ds_sav_pm,
    (sum(ip_savings  ) / count(member_id))::decimal(16,2) ip_sav_pm,
    (sum(snf_savings ) / count(member_id))::decimal(16,2) snf_sav_pm,
    (sum(icf_savings ) / count(member_id))::decimal(16,2) icf_sav_pm,
    (sum(ed_savings  ) / count(member_id))::decimal(16,2) ed_sav_pm,
    (sum(hh_savings  ) / count(member_id))::decimal(16,2) hh_sav_pm,
    (sum(pro_savings ) / count(member_id))::decimal(16,2) pro_sav_pm,
    (sum(out_savings ) / count(member_id))::decimal(16,2) out_sav_pm,

    (sum(raf_savings ) * 100.0 / sum(savings))::decimal(16,2) raf_pct_of_sav,
    (sum(ds_savings  ) * 100.0 / sum(savings))::decimal(16,2) ds_pct_of_sav,
    (sum(ip_savings  ) * 100.0 / sum(savings))::decimal(16,2) ip_pct_of_sav,
    (sum(snf_savings ) * 100.0 / sum(savings))::decimal(16,2) snf_pct_of_sav,
    (sum(icf_savings ) * 100.0 / sum(savings))::decimal(16,2) icf_pct_of_sav,
    (sum(ed_savings  ) * 100.0 / sum(savings))::decimal(16,2) ed_pct_of_sav,
    (sum(hh_savings  ) * 100.0 / sum(savings))::decimal(16,2) hh_pct_of_sav,
    (sum(pro_savings ) * 100.0 / sum(savings))::decimal(16,2) pro_pct_of_sav,
    (sum(out_savings ) * 100.0 / sum(savings))::decimal(16,2) out_pct_of_sav,

    (sum(is_eol          ) * 100.0 / count(1))::decimal(16,2) mortality_pct ,
    (sum(is_unaligned    ) * 100.0 / count(1))::decimal(16,2) unaligned_pct ,
    (sum(is_self_directed) * 100.0 / count(1))::decimal(16,2) self_directed_pct ,

    (count(case when lob = 1 then 1 end) * 100.0 / count(1))::decimal(16,2) lob_1_pct ,
    (count(case when lob = 2 then 1 end) * 100.0 / count(1))::decimal(16,2) lob_2_pct ,
    (count(case when lob = 3 then 1 end) * 100.0 / count(1))::decimal(16,2) lob_3_pct ,

    (count(case when grp = 1     then 1 end) * 100.0 / count(1))::decimal(16,2) grp_1_pct ,
    (count(case when grp = 2     then 1 end) * 100.0 / count(1))::decimal(16,2) grp_2_pct ,
    (count(case when grp = 3     then 1 end) * 100.0 / count(1))::decimal(16,2) grp_3_pct ,
    (count(case when grp = 45678 then 1 end) * 100.0 / count(1))::decimal(16,2) grp_45678_pct ,

    (count(case when sav_cat = 4 then 1 end) * 100.0 / count(1))::decimal(16,2) grp_4_pct ,
    (count(case when sav_cat = 3 then 1 end) * 100.0 / count(1))::decimal(16,2) grp_3_pct ,
    (count(case when sav_cat = 2 then 1 end) * 100.0 / count(1))::decimal(16,2) grp_2_pct ,
    (count(case when sav_cat = 1 then 1 end) * 100.0 / count(1))::decimal(16,2) grp_1_pct,
    (count(case when sav_cat = 0 then 1 end) * 100.0 / count(1))::decimal(16,2) grp_0_pct,

    avg(pct_sav     )::decimal(16,2) sav_pct_avg,
    min(pct_sav     )::decimal(16,2) sav_pct_min,
    max(pct_sav     )::decimal(16,2) sav_pct_max,
    avg(raf_sav_pct )::decimal(16,2) raf_sav_pct_avg,
    avg(ds_sav_pct  )::decimal(16,2) ds_sav_pct_avg,
    avg(ip_sav_pct  )::decimal(16,2) ip_sav_pct_avg,
    avg(snf_sav_pct )::decimal(16,2) snf_sav_pct_avg,
    avg(icf_sav_pct )::decimal(16,2) icf_sav_pct_avg,
    avg(ed_sav_pct  )::decimal(16,2) ed_sav_pct_avg,
    avg(hh_sav_pct  )::decimal(16,2) hh_sav_pct_avg,
    avg(pro_sav_pct )::decimal(16,2) pro_sav_pct_avg,
    avg(out_sav_pct )::decimal(16,2) out_sav_pct_avg,


    sum(raf_savings ) raf_savings_tc,
    sum(ds_savings  ) ds_savings_tc,
    sum(ip_savings  ) ip_savings_tc,
    sum(snf_savings ) snf_savings_tc,
    sum(icf_savings ) icf_savings_tc,
    sum(ed_savings  ) ed_savings_tc,
    sum(hh_savings  ) hh_savings_tc,
    sum(pro_savings ) pro_savings_tc,
    sum(out_savings ) out_savings_tc,


    sum(mm          ) mm,

    (sum(tc               ) / sum(mm))::decimal(16,2) pmpm      ,
    (sum(hcbs_atd_pcs_tc  ) / sum(mm))::decimal(16,2) hcbs_atd_pcs_pmpm                 ,
    (sum(ip_tc            ) / sum(mm))::decimal(16,2) ip_pmpm   ,
    (sum(snf_tc           ) / sum(mm))::decimal(16,2) snf_pmpm  ,
    (sum(icf_tc           ) / sum(mm))::decimal(16,2) icf_pmpm  ,
    (sum(ed_tc            ) / sum(mm))::decimal(16,2) ed_pmpm   ,
    (sum(hh_tc            ) / sum(mm))::decimal(16,2) hh_pmpm   ,
    (sum(pro_tc           ) / sum(mm))::decimal(16,2) pro_pmpm  ,
    (sum(out_tc           ) / sum(mm))::decimal(16,2) out_pmpm  ,

    avg(savings_pmpm     )::decimal(16,2) savings_pmpm_avg,
    avg(raf_sav_pmpm     )::decimal(16,2) raf_sav_pmpm_avg,
    avg(ds_sav_pmpm      )::decimal(16,2) ds_sav_pmpm_avg,
    avg(ip_sav_pmpm      )::decimal(16,2) ip_sav_pmpm_avg,
    avg(snf_sav_pmpm     )::decimal(16,2) snf_sav_pmpm_avg,
    avg(icf_sav_pmpm     )::decimal(16,2) icf_sav_pmpm_avg,
    avg(ed_sav_pmpm      )::decimal(16,2) ed_sav_pmpm_avg,
    avg(hh_sav_pmpm      )::decimal(16,2) hh_sav_pmpm_avg,
    avg(pro_sav_pmpm     )::decimal(16,2) pro_sav_pmpm_avg,
    avg(out_sav_pmpm     )::decimal(16,2) out_sav_pmpm_avg,
    avg(tc_pmpm          )::decimal(16,2) tc_pmpm_avg,
    avg(hcbs_attd_pmpm   )::decimal(16,2) hcbs_attd_pmpm_avg,
    avg(ip_pmpm          )::decimal(16,2) ip_pmpm_avg,
    avg(snf_pmpm         )::decimal(16,2) snf_pmpm_avg,
    avg(icf_pmpm         )::decimal(16,2) icf_pmpm_avg,
    avg(ed_pmpm          )::decimal(16,2) ed_pmpm_avg,
    avg(hh_pmpm          )::decimal(16,2) hh_pmpm_avg,
    avg(pro_pmpm         )::decimal(16,2) pro_pmpm_avg,
    avg(out_pmpm         )::decimal(16,2) out_pmpm_avg
from
(
    select
        sr.rn,
        sr.pred,
        sw.lvl,
        sw.member_id,
        sw.savings,
        sw.raf_savings,
        sw.ds_savings,
        sw.ip_savings,
        sw.snf_savings,
        sw.icf_savings,
        sw.ed_savings,
        sw.hh_savings,
        sw.pro_savings,
        sw.out_savings,


        sw.pct_sav,
        sw.raf_sav_pct,
        sw.ds_sav_pct,
        sw.ip_sav_pct,
        sw.snf_sav_pct,
        sw.icf_sav_pct,
        sw.ed_sav_pct,
        sw.hh_sav_pct,
        sw.pro_sav_pct,
        sw.out_sav_pct,
        sw.is_eol,
        sw.is_unaligned,
        case when sw.n_lob3 = 1 then 3
             when sw.n_lob2 = 1 then 2
             when sw.n_lob1 = 1 then 1
        end lob,
        case when sw.n_grp1     = 1 then 1
             when sw.n_grp2     = 1 then 2
             when sw.n_grp3     = 1 then 3
             when sw.n_grp45678 = 1 then 45678
        end grp,
        case when pct_self_directed > 50 then 1 else 0 end is_self_directed,
        case when n_cat4 = 1 then 4
             when n_cat3 = 1 then 3
             when n_cat2 = 1 then 2
             when n_cat1 = 1 then 1
             when n_cat0 = 1 then 0
        end sav_cat,

        sw.tc,
        sw.hcbs_atd_pcs_tc,
        sw.ip_tc,
        sw.snf_tc,
        sw.icf_tc,
        sw.ed_tc,
        sw.hh_tc,
        sw.pro_tc,
        sw.out_tc,

        sw.savings_pmpm,
        sw.raf_sav_pmpm,
        sw.ds_sav_pmpm,
        sw.ip_sav_pmpm,
        sw.snf_sav_pmpm,
        sw.icf_sav_pmpm,
        sw.ed_sav_pmpm,
        sw.hh_sav_pmpm,
        sw.pro_sav_pmpm,
        sw.out_sav_pmpm,

        tc_pmpm,
        hcbs_attd_pmpm,
        ip_pmpm,
        snf_pmpm,
        icf_pmpm,
        ed_pmpm,
        hh_pmpm,
        pro_pmpm,
        out_pmpm,
        mm
    from
        _stack_rank sr
        join mab_poo_etr_swg_members sw on sr.member_id = sw.member_id and sw.mab_id = 5
) x
where x.rn <= 750
group by 1
order by x.lvl asc
;






drop table if exists _clms;
create temp table _clms as
select
    extract(year from c.date_from) yr,
    sum(paid_amount) paid,
    sum(paid_amount) filter (where is_rx) rx_paid,
    sum(paid_amount) filter (where not is_rx) med_paid,
    count(distinct c.member_id) nd
from
    claims c
where
    c.mco_id = 2
group by 1

select
    c.yr,
    paid,
    rx_paid,
    med_paid,
    (paid     / c.nd)::decimal(16,2) paid_pm,
    (rx_paid  / c.nd)::decimal(16,2) rx_paid_pm,
    (med_paid / c.nd)::decimal(16,2) med_paid_pm,
    nd
from _clms c

select
    *
from
    member_days md
where



select
    lvl,
    count(case when is_self_directed = 1 then member_id end) self_directed,
    count(case when is_self_directed = 0 then member_id end) not_self_directed,
    count(case when is_lob1 = 1 then member_id end) lob1,
    count(case when is_lob2 = 1 then member_id end) lob2,
    count(case when is_lob3 = 1 then member_id end) lob3,
    count(case when is_unaligned = 1 then member_id end) un_aligned,
    (sum(x.savings) / sum(x.mm))::decimal(16,2)
from
    (
        select
            ms.member_id,
            pre.lvl,
            row_number() over (partition by pre.lvl order by ms.score desc) rn,
            post.savings,
            post.is_self_directed,
            post.is_lob1,
            post.is_lob2,
            post.is_lob3,
            post.is_unaligned,
            post.mm
        from
            scoring_runs sr
            join member_scores ms on sr.id = ms.scoring_run_id
            join mab_member_targets pre on pre.member_id = ms.member_id and sr.mab_id = pre.mab_id and pre.is_self_directed = 0 and pre.is_pre = 1
            left join mab_member_targets post on ms.member_id = post.member_id and sr.mab_id = post.mab_id and post.lvl = pre.lvl and post.is_pre = 0
        where
            sr.id = 2
    ) x
where x.rn <= 750
group by 1


select
    lvl,
    count(case when is_self_directed = 1 then member_id end) self_directed,
    count(case when is_self_directed = 0 then member_id end) not_self_directed,
    count(case when is_lob1 = 1 then member_id end) lob1,
    count(case when is_lob2 = 1 then member_id end) lob2,
    count(case when is_lob3 = 1 then member_id end) lob3,
    count(case when is_unaligned = 1 then member_id end) un_aligned,
    (sum(x.savings) / sum(x.mm))::decimal(16,2)
from
    (
        select
            pre.member_id,
            pre.lvl,
            row_number() over (partition by pre.lvl order by pre.savings desc) rn,
            post.savings,
            post.is_self_directed,
            post.is_lob1,
            post.is_lob2,
            post.is_lob3,
            post.is_unaligned,
            post.mm
        from
            mab_member_targets pre
            left join mab_member_targets post on pre.member_id = post.member_id and post.mab_id = pre.mab_id and pre.lvl = post.lvl and post.is_pre = 0
        where
            pre.mab_id = 2
            and pre.is_pre = 1
            and pre.is_self_directed = 0
    ) x
where x.rn <= 750
group by 1