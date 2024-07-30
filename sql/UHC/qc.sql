

with mbrs as (
    select distinct md1.member_id
    from cb.eligibility_days md1
    where md1.year = 2017 and md1.mco_id = 2

    intersect
    select distinct md2.member_id
    from cb.eligibility_days md2
    where md2.year = 2018 and md2.mco_id = 2

    intersect
    select distinct md3.member_id
    from cb.eligibility_days md3
    where md3.year = 2019 and md3.mco_id = 2
),
tc as (
    select
        ed.year,
        ed.month,
        ed.line_of_business_id,
        ed.ggroup,
        sum(total_$) tc,
        sum(rx_$) rx,
        sum(total_$) - sum(rx_$) med,
        sum(hcbs_$) hcbs_tc,
        sum(ip_$  ) ip_tc,
        --count(distinct ed.member_id) filter (where ed.line_of_business_id = 1) nd_lob_1,
        --count(distinct ed.member_id) filter (where ed.line_of_business_id = 2) nd_lob_2,
        --count(distinct ed.member_id) filter (where ed.line_of_business_id = 3) nd_lob_3,
        count(ed.date) / 30 as mm,
        count(distinct ed.member_id) nd
    from
        eligibility_days ed
        join mbrs m on m.member_id = ed.member_id
        left join member_days md on ed.member_id = md.member_id and ed.date = md.date and md.mco_id = ed.mco_id
    where
        ed.mco_id = 2
        and ed.is_unaligned = false
        and ed.line_of_business_id in (1,3)
        and ed.ggroup::int in (1,2,3)
        and ed.year in (2017, 2018, 2019)
    group by 1, 2, 3, 4
)
select
    line_of_business_id,
    ggroup,
    year,
    month,
    tc,
    rx,
    med,
    (tc  / nullif(nd,0))::decimal(16,2) pm,
    (rx  / nullif(nd,0))::decimal(16,2) rx_pm,
    (med / nullif(nd,0))::decimal(16,2) med_pm,
    '_' _,
    (tc  / nullif(mm,0))::decimal(16,2) pmpm,
    (rx  / nullif(mm,0))::decimal(16,2) rx_pmpm,
    (med / nullif(mm,0))::decimal(16,2) med_pmpm,
    (hcbs_tc / nullif(mm,0))::decimal(16,2) hcbs_pmpm,
    (ip_tc / nullif(mm,0))::decimal(16,2) ip_pmpm,
    nd
from tc
order by 1, 2, 3, 4;


-----------------------------------------------------------------------------------------------------



drop table if exists _clms;
create temp table _clms as
select member_id,
       date_from,
       sum(paid_amount) filter ( where is_rx is false ) med_tc,
       sum(paid_amount) filter ( where is_rx is true  ) rx_tc,
       sum(paid_amount) tc
from (
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2017q1_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2017q2_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2017q3_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2017q4_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2018q1_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2018q2_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2018q3_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2018q4_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2019q1_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2019q2_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2019q3_txt union all
    select member_id, date_from   ::date date_from, paid_amount::numeric paid_amount, false is_rx from raw.uhg_20200618_uhg_tn_medical_2019q4_txt union all
    select member_id, service_date::date date_from, paid_amount::numeric paid_amount, true  is_rx from raw.uhg_20200618_uhg_tn_pharmacy_2017_txt  union all
    select member_id, service_date::date date_from, paid_amount::numeric paid_amount, true  is_rx from raw.uhg_20200618_uhg_tn_pharmacy_2018_txt  union all
    select member_id, service_date::date date_from, paid_amount::numeric paid_amount, true  is_rx from raw.uhg_20200618_uhg_tn_pharmacy_2019_txt
    ) x
group by member_id, date_from;

drop table if exists _elig;
create temp table _elig as
select distinct e.member_id, dd.day,
       case when e.subproduct = 'MEDICARE' then 'medicare' else 'medicaid' end as lob,
       case when e.subproduct = 'MEDICARE' then 'medicare' else "group" end as ggroup
from raw.uhc_20200723_uhg_tn_all_membership_tn e
    join ref.dates dd on dd.day between e.effective_date::date and e.term_date::date
where day between '2017-01-01'::date and '2019-12-31'::date
order by 1, 2;

with mm as (
    select extract(year from day) yearr, member_id, lob, ggroup, count(day) / 30.4166666667 mm
    from _elig
    group by yearr, member_id, lob, ggroup
),
cd as (
    select extract(year from c.date_from) yearr, c.member_id, e.lob, e.ggroup, sum(c.med_tc) med_tc, sum(c.rx_tc) rx_tc, sum(c.tc) tc
    from _clms c
        left join _elig e on c.member_id = e.member_id
                                 and c.date_from = e.day
    group by yearr, c.member_id, e.lob, e.ggroup
)
select cd.yearr,
       cd.lob,
       --cd.ggroup,
       sum(cd.med_tc) med_tc,
       sum(cd.rx_tc) rx_tc,
       sum(cd.tc) tc,
       round(sum(mm.mm)) mm,
       (sum(cd.med_tc) / sum(mm.mm))::decimal(16,2) med_pmpm,
       (sum(cd.rx_tc)  / sum(mm.mm))::decimal(16,2) rx_pmpm,
       (sum(cd.tc)     / sum(mm.mm))::decimal(16,2) total_pmpm
from cd
    left join mm on mm.yearr = cd.yearr
                        and mm.member_id = cd.member_id
                        and mm.lob = cd.lob
                        and mm.ggroup = cd.ggroup
group by cd.yearr, cd.lob-- , cd.ggroup
order by cd.yearr, cd.lob-- , cd.ggroup;


select * from raw.uhc_20200723_uhg_tn_all_membership_tn u20200723utamt
select * from raw.agp_eligibility_20200628 a

select c.created_at from cb.claims c

select
    m.is_risk_carveout,
    sum(tc) / sum(mm) pmpm
from
    mab_poo mp
    join members m on mp.member_id = m.id
where mp.mab_id = 5
group by 1

set schema 'cb';
select am.adj_json->'pre_json' is not null, count(member_id) from adjudication_members am where am.adjudication_batch_id = 1 group by 1
select am.adj_json->'post_json'->'member_json' , count(member_id) from adjudication_members am where am.adjudication_batch_id = 1 group by 1

