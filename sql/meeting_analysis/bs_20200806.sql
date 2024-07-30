set schema 'cb';

select
    vm.is_pre,
    vm.lob,
    vm.grp,
    case when vm.is_unaligned then 1 else 0 end is_unaligned,

    (sum(vm.tc) / sum(vm.p_mm))::decimal(16,2) pmpm,
    ((sum(vm.hcbs_attd_tc) + sum(vm.hcbs_pcs_tc)) / sum(vm.p_mm))::decimal(16,2) hcbs_attd_pcs_pmpm,
    count(distinct vm.member_id) nd,
    count(vm.member_id) n,

    '_int_' _int_,
    count(distinct vm.member_id) filter (where mmt.member_id is not null) int_nd,
    (sum(vm.tc) filter (where mmt.member_id is not null) / sum(vm.p_mm) filter (where mmt.member_id is not null) )::decimal(16,2) int_pmpm,
    ((sum(vm.hcbs_attd_tc) filter (where mmt.member_id is not null) + sum(vm.hcbs_pcs_tc) filter (where mmt.member_id is not null)) / sum(vm.p_mm) filter (where mmt.member_id is not null) )::decimal(16,2) int_hcbs_attd_pcs_pmpm,

    '_not_int_' _not_int_,
    count(distinct vm.member_id) filter (where mmt.member_id is null) not_int_nd,
    (sum(vm.tc) filter (where mmt.member_id is null) / sum(vm.p_mm) filter (where mmt.member_id is null) )::decimal(16,2) not_int_pmpm,
    ((sum(vm.hcbs_attd_tc) filter (where mmt.member_id is null) + sum(vm.hcbs_pcs_tc) filter (where mmt.member_id is null)) / sum(vm.p_mm) filter (where mmt.member_id is null) )::decimal(16,2) not_int_hcbs_attd_pcs_pmpm,

    '_id_' _id_,
    count(distinct vm.member_id) filter (where idd.member_id is not null) id_nd,
    (sum(vm.tc) filter (where idd.member_id is not null) / sum(vm.p_mm) filter (where idd.member_id is not null) )::decimal(16,2) id_pmpm,
    ((sum(vm.hcbs_attd_tc) filter (where idd.member_id is not null) + sum(vm.hcbs_pcs_tc) filter (where idd.member_id is not null)) / sum(vm.p_mm) filter (where idd.member_id is not null) )::decimal(16,2) id_hcbs_attd_pcs_pmpm,

    '_nid_' _nid_,
    count(distinct vm.member_id) filter (where idd.member_id is null and mmt.member_id is not null) not_id_nd,
    (sum(vm.tc) filter (where idd.member_id is null and mmt.member_id is not null) / sum(vm.p_mm) filter (where idd.member_id is null and mmt.member_id is not null) )::decimal(16,2) not_id_pmpm,
    ((sum(vm.hcbs_attd_tc) filter (where idd.member_id is null and mmt.member_id is not null) + sum(vm.hcbs_pcs_tc) filter (where idd.member_id is null and mmt.member_id is not null)) / sum(vm.p_mm) filter (where idd.member_id is null and mmt.member_id is not null) )::decimal(16,2) not_id_hcbs_attd_pcs_pmpm,

    '_new_' _new_,
    sum(vm.tc) tc,
    (sum(vm.hcbs_attd_tc) + sum(vm.hcbs_pcs_tc)) hcbs_attd_pcs_tc,
    sum(vm.p_mm) mm,
    sum(vm.tc) filter (where mmt.member_id is not null) int_tc,
    (sum(vm.hcbs_attd_tc) filter (where mmt.member_id is not null) + sum(vm.hcbs_pcs_tc) filter (where mmt.member_id is not null)) int_hcbs_attd_pcs_tc,
    sum(vm.p_mm) filter (where mmt.member_id is not null) int_mm,
    sum(vm.tc) filter (where mmt.member_id is null) not_int_tc,
    (sum(vm.hcbs_attd_tc) filter (where mmt.member_id is null) + sum(vm.hcbs_pcs_tc) filter (where mmt.member_id is null)) not_int_hcbs_attd_pcs_tc,
    sum(vm.p_mm) filter (where mmt.member_id is null) not_int_mm,
    sum(vm.tc ) filter (where idd.member_id is not null) id_tc,
    (sum(vm.hcbs_attd_tc) filter (where idd.member_id is not null) + sum(vm.hcbs_pcs_tc) filter (where idd.member_id is not null)) id_hcbs_attd_pcs_tc,
    sum(vm.p_mm) filter (where idd.member_id is not null) id_mm,
    sum(vm.tc) filter (where idd.member_id is null and mmt.member_id is not null) not_id_tc,
    (sum(vm.hcbs_attd_tc) filter (where idd.member_id is null and mmt.member_id is not null) + sum(vm.hcbs_pcs_tc) filter (where idd.member_id is null and mmt.member_id is not null)) not_id_hcbs_attd_pcs_tc,
    sum(vm.p_mm) filter (where idd.member_id is null and mmt.member_id is not null) not_id_mm,

    (sum(vm.hcbs_tc)) hcbs_tc,
    (sum(vm.hcbs_tc) filter (where mmt.member_id is not null)) int_hcbs_tc,
    (sum(vm.hcbs_tc) filter (where mmt.member_id is null)) not_hcbs_tc,
    (sum(vm.hcbs_tc) filter (where idd.member_id is not null)) id_hcbs_tc,

    (sum(vm.icf_tc)) icf_tc,
    (sum(vm.icf_tc) filter (where mmt.member_id is not null)) int_icf_tc,
    (sum(vm.icf_tc) filter (where mmt.member_id is null)) not_icf_tc,
    (sum(vm.icf_tc) filter (where idd.member_id is not null)) id_icf_tc,

    (sum(vm.snf_tc)) snf_tc,
    (sum(vm.snf_tc) filter (where mmt.member_id is not null)) int_snf_tc,
    (sum(vm.snf_tc) filter (where mmt.member_id is null)) not_snf_tc,
    (sum(vm.snf_tc) filter (where idd.member_id is not null)) id_snf_tc,

    (sum(vm.ip_tc)) ip_tc,
    (sum(vm.ip_tc) filter (where mmt.member_id is not null)) int_ip_tc,
    (sum(vm.ip_tc) filter (where mmt.member_id is null)) not_ip_tc,
    (sum(vm.ip_tc) filter (where idd.member_id is not null)) id_ip_tc,

    (sum(vm.rx_tc)) rx_tc,
    (sum(vm.rx_tc) filter (where mmt.member_id is not null)) int_rx_tc,
    (sum(vm.rx_tc) filter (where mmt.member_id is null)) not_rx_tc,
    (sum(vm.rx_tc) filter (where idd.member_id is not null)) id_rx_tc,

    (sum(vm.pro_tc)) pro_tc,
    (sum(vm.pro_tc) filter (where mmt.member_id is not null)) int_pro_tc,
    (sum(vm.pro_tc) filter (where mmt.member_id is null)) not_pro_tc,
    (sum(vm.pro_tc) filter (where idd.member_id is not null)) id_pro_tc,

    (sum(vm.out_tc)) out_tc,
    (sum(vm.out_tc) filter (where mmt.member_id is not null)) int_out_tc,
    (sum(vm.out_tc) filter (where mmt.member_id is null)) not_out_tc,
    (sum(vm.out_tc) filter (where idd.member_id is not null)) id_out_tc

from vw_mab_mbr_yr vm
     -- join members m on vm.member_id = m.id and m.mco_id = 2 and m.is_risk_carveout
     left join perm.uhc_id_1_20200806 idd on idd.member_id = vm.member_id and is_identified
     left join mab_member_targets mmt on mmt.mab_id = vm.mab_id and mmt.member_id = vm.member_id and mmt.is_pre = 1 and mmt.lvl ~ '2'
where vm.mab_id in ( 5 )
group by
    1, 2, 3, 4

select
    ed.is_unaligned,
    ed.ggroup,
    ed.line_of_business_id
from vw_mab_mbr_yr vm
     join eligibility_days ed on vm.id_date = ed.date and ed.member_id = vm.member_id and ed.mco_id = vm.mco_id
where
    vm.mab_id = 5
    and ed.line_of_business_id = 1
    and ed.is_unaligned;


select
    vm.age, vm.is_male, count(distinct vm.member_id) nd, count(distinct vm.member_id) filter (where idd.member_id is not null) nd_id
from vw_mab_mbr_yr vm
     join eligibility_days ed on vm.id_date = ed.date and ed.member_id = vm.member_id and ed.mco_id = vm.mco_id
     -- join members m on vm.member_id = m.id and m.mco_id = 2 and m.is_risk_carveout
     left join perm.uhc_id_1_20200806 idd on idd.member_id = vm.member_id and is_identified
     left join mab_member_targets mmt on mmt.mab_id = vm.mab_id and mmt.member_id = vm.member_id and mmt.is_pre = 1 and mmt.lvl ~ '2'
where vm.mab_id in ( 5 )
group by
    1, 2
order by 1, 2

select * from mab_member_target_economic_savings sav where sav.mab_id = 5 order by 1,2,3,4


select
    ed.ggroup,
    ed.line_of_business_id,
    count(distinct member_id) nd
from
    eligibility_days ed
    join members m on ed.member_id = m.id and m.is_risk_carveout
where
    ed.year = 2019
    and ed.mco_id = 2
group by
    1, 2

select av.evv_member_id from cb.appointments_visits av where mco_id = 2;

select distinct ha.payer_member_id, left(ha.payer_member_id, position('-' in ha.payer_member_id) -1 ) , right(ha.payer_member_id, 4) right_portion_of_member_id, ha.dob date_of_birth, first_name, last_name from hcbs_auths ha where mco_id = 2;

select distinct m.source_member_id, m.date_of_birth from members m where m.mco_id = 2;



select --count(*) -- 129,627
    extract(year from c.date_from) yearr,
    count(*)
from cb.claims c
    join cb.members m on c.member_id = m.id and m.mco_id = c.mco_id
    join staging.claims_medical st on st.mco_id = c.mco_id
                                      and st.source_member_id = m.source_member_id
                                      and st.source_claim_id = c.source_claim_id
                                      and st.claim_line_id = c.claim_line_id
where c.mco_id = 1 and c.is_rx is false and st.paid_amount = c.paid_amount
group by 1;


select
    (((coalesce(vm.hcbs_attd_tc,0) + (coalesce(vm.hcbs_pcs_tc,0))) / (p_mm)) / 15)::int hrs,
    count(vm.member_id) n,
    count(distinct vm.member_id) nd
from vw_mab_mbr_yr vm
     -- join members m on vm.member_id = m.id and m.mco_id = 2 and m.is_risk_carveout
     join perm.uhc_id_1_20200806 idd on idd.member_id = vm.member_id --and is_identified
     --left join mab_member_targets mmt on mmt.mab_id = vm.mab_id and mmt.member_id = vm.member_id and mmt.is_pre = 1 and mmt.lvl ~ '2'
where vm.mab_id in ( 5 )
group by 1
order by 2 desc, 3;

select
    (sum(vm.tc) / sum(vm.p_mm))::decimal(16,2) pmpm,
    ((sum(vm.hcbs_attd_tc) + sum(vm.hcbs_pcs_tc)) / sum(vm.p_mm))::decimal(16,2) hcbs_attd_pcs_pmpm,
    (sum(vm.hcbs_tc) / sum(vm.p_mm))::decimal(16,2) hcbs_pmpm,
    (sum(vm.ip_tc) / sum(vm.p_mm))::decimal(16,2)  ip_pmpm,
    (sum(vm.icf_tc) / sum(vm.p_mm))::decimal(16,2) icf_pmpm,
    (sum(vm.snf_tc) / sum(vm.p_mm))::decimal(16,2) snf_pmpm,
    (sum(vm.pro_tc) / sum(vm.p_mm))::decimal(16,2) pro_pmpm,
    (sum(vm.out_tc) / sum(vm.p_mm))::decimal(16,2) out_pmpm,
    (sum(vm.hh_tc) / sum(vm.p_mm))::decimal(16,2)  hh_pmpm,
    (sum(vm.ed_tc) / sum(vm.p_mm))::decimal(16,2)  ed_pmpm,
    (sum(vm.amb_tc) / sum(vm.p_mm))::decimal(16,2) amb_pmpm,
    ((coalesce(sum(vm.hosp_tc),0) + coalesce(sum(vm.oth_ddos),0) + coalesce(sum(vm.spfac_tc),0)) / sum(vm.p_mm))::decimal(16,2) oth_pmpm,
    (sum(vm.rx_tc) / sum(vm.p_mm))::decimal(16,2) rxpmpm,
    count(distinct vm.member_id) filter (where ed.line_of_business_id = 3) care_n,
    count(distinct vm.member_id) filter (where ed.line_of_business_id = 1) caid_n,
    count(distinct vm.member_id) filter (where ed.ggroup::int = 2) grp2_n,
    count(distinct vm.member_id) filter (where ed.ggroup::int = 3) grp3_n,

    count(distinct vm.member_id) filter (where ed.line_of_business_id = 3 and ed.ggroup::int = 2 ) care_grp_2_n,
    count(distinct vm.member_id) filter (where ed.line_of_business_id = 3 and ed.ggroup::int = 3 ) care_grp_3_n,
    count(distinct vm.member_id) filter (where ed.line_of_business_id = 1 and ed.ggroup::int = 2 ) caid_grp_2_n,
    count(distinct vm.member_id) filter (where ed.line_of_business_id = 1 and ed.ggroup::int = 3 ) caid_grp_3_n,

    count(distinct vm.member_id) filter (where ed.ggroup::int = 2) grp2_n,
    count(distinct vm.member_id) filter (where ed.ggroup::int = 3) grp3_n,

    ((sum(vm.hcbs_attd_tc) + sum(vm.hcbs_pcs_tc)) / sum(p_mm)) / (15) hrs_avg,
    min((coalesce(vm.hcbs_attd_tc,0) + (coalesce(vm.hcbs_pcs_tc,0))) / (p_mm)) / 15 min_hrs,
    max((coalesce(vm.hcbs_attd_tc,0) + (coalesce(vm.hcbs_pcs_tc,0))) / (p_mm)) / 15 max_hrs,
    count(distinct vm.member_id) nd,
    count(vm.member_id) n
from vwm_mab_mbr_yr vm
     join eligibility_days ed on ed.member_id = vm.member_id and vm.id_date = ed.date and ed.line_of_business_id in (1, 3) and ed.is_unaligned = false and ed.ggroup::int in (2,3)
     -- join members m on vm.member_id = m.id and m.mco_id = 2 and m.is_risk_carveout
     join perm.uhc_id_1_20200806 idd on idd.member_id = vm.member_id and is_identified
     --left join mab_member_targets mmt on mmt.mab_id = vm.mab_id and mmt.member_id = vm.member_id and mmt.is_pre = 1 and mmt.lvl ~ '2'
where vm.mab_id in ( 5 );

-- DME
with interesting as (
    select
        vm.member_id,
        sum(vm.mm) mm,
        sum(vm.p_mm) p_mm,
        max(end_date) end_date,
        min(start_date) start_date
    from
        mab_poo vm
        join eligibility_days ed on ed.member_id = vm.member_id and vm.id_date = ed.date and ed.line_of_business_id in (1) and ed.is_unaligned = false and ed.ggroup::int in (2,3)
    where vm.mab_id in ( 5 )
    group by 1
)
, dme as (
    select
        i.member_id,
        i.p_mm,
        i.mm,
        sum(c.paid_amount) dme_paid
    from
        interesting i
        left join claims c
                  join ref.procedure_codes pc on c.procedure_code_id = pc.id and pc.is_dme
            on c.mco_id = 2 and c.member_id = i.member_id and c.date_from between i.start_date and i.end_date
    group by 1, 2, 3
)
select
    (sum(dme.dme_paid) / sum(p_mm))::decimal(16,2) pmpm_dme_pmm,
    (sum(dme.dme_paid) / sum(mm))::decimal(16,2) pmpm_dme,
    count(member_id) n,
    count(distinct member_id) nd
from
    dme




select
    ha.id,
    source_authorization_id,
    mco_id,
    member_id,
    payer_member_id,
    evv_member_id,
    service_title,
    service_description,
    is_hcbs_pcs,
    is_respit,
    is_meal_delivery,
    start_date,
    end_date,
    units,
    units_daily,
    procedure_code,
    modifiers,
    first_name,
    last_name,
    dob,
    auth_ref_no,
    auth_days,
    auth_hours,
    evv_provider_id,
    hcbs_program,
    source_created_at,
    source_updated_at,
    created_input_id,
    last_input_id,
    created_at,
    updated_at
from
    hcbs_auths ha
;


create temporary table _interesting as
select
    vm.member_id, is_identified
from vwm_mab_mbr_yr vm
     join eligibility_days ed on ed.member_id = vm.member_id and vm.id_date = ed.date and ed.line_of_business_id in (1, 3) and ed.is_unaligned = false and ed.ggroup::int in (2,3)
     -- join members m on vm.member_id = m.id and m.mco_id = 2 and m.is_risk_carveout
     -- left join perm.uhc_id_1_20200806 idd on idd.member_id = vm.member_id and is_identified
     --left join mab_member_targets mmt on mmt.mab_id = vm.mab_id and mmt.member_id = vm.member_id and mmt.is_pre = 1 and mmt.lvl ~ '2'
where vm.mab_id in ( 5 );

select count(*) from _interesting i;

select
    *
from
    _interesting i
    join members m on i.member_id = m.id and m.mco_id = 2

select * from hcbs_auths ha where mco_id = 2



select
    vm.member_id, is_identified
from vwm_mab_mbr_yr vm
     join eligibility_days ed on ed.member_id = vm.member_id and vm.id_date = ed.date and ed.line_of_business_id in (1, 3) and ed.is_unaligned = false and ed.ggroup::int in (2,3)
     -- join members m on vm.member_id = m.id and m.mco_id = 2 and m.is_risk_carveout
     -- left join perm.uhc_id_1_20200806 idd on idd.member_id = vm.member_id and is_identified
     --left join mab_member_targets mmt on mmt.mab_id = vm.mab_id and mmt.member_id = vm.member_id and mmt.is_pre = 1 and mmt.lvl ~ '2'
where vm.mab_id in ( 5 );