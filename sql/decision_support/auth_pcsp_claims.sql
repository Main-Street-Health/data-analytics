select count(*) from junk.agp_hstar_auths aha;
select count(*) from junk.agp_hstar_pcsp ahp;



select * from junk.agp_hstar_auths aha
    join cb.members m on m.source_member_id = aha.member_id;


select source_member_id, mbi_id from cb.members m where m.mco_id = 1

select
    distinct a.member_id, a.hicn, a.mbi, a.medicaid_id
from raw.agp_eligibility_20200628 a

select
    count(distinct a.member_id) -- 7130
from
    raw.agp_eligibility_20200628 a
    join cb.members m on m.mco_id = 1 and trim(lower(a.medicaid_id)) = m.source_member_id



-- 116,905 : 7,466

select count(aha.member_id) N,  count(distinct aha.member_id) ND  from junk.agp_hstar_auths aha
        join raw.agp_eligibility_20200628 a on
                    -- trim(lower(a.medicaid_id)) = trim(lower(aha.member_id)) -- 0
                    trim(lower(a.member_id)) = trim(lower(aha.member_id)) -- 166,180 : 5,809

/*
 Summary
 - match 5,809 / 7,466 members = 77.8%  from the agp_hstar_auths to agp_eligibility
    - trim(lower(a.member_id)) = trim(lower(aha.member_id)) -- 166,180 : 5,809
    - all names match except small spelling issues
    - auth year matches:

            | yr   | n     | nd    |
            | :--- | :---  | :---  |
            | 2014 | 20    |     9 |
            | 2015 | 12299 | 2,035 |
            | 2016 | 28045 | 2,768 |
            | 2017 | 34425 | 3,293 |
            | 2018 | 35069 | 3,524 |
            | 2019 | 35602 | 3,738 |
            | 2020 | 20720 | 2,898 |

    - auth year no - matches
            |   yr |    n |    nd |
            | :--- | :--- | :---  |
            | 2014 |    7 |     4 |
            | 2015 | 4453 | 1,084 |
            | 2016 | 4854 | 1,146 |
            | 2017 | 1383 |   621 |
            | 2018 |  498 |   181 |
            | 2019 |  418 |   144 |
            | 2020 |  166 |    92 |

    - not matched historical eligibility to auths
    - 1,321 / 7,130 = 18.5%
            |   yr |    n |   nd |
            | :--- | :--- | :--- |
            | 2010 |    6 |    6 |
            | 2011 |    5 |    5 |
            | 2012 |    8 |    8 |
            | 2013 |   21 |   21 |
            | 2014 |   18 |   18 |
            | 2015 |  132 |  132 |
            | 2016 |  129 |  129 |
            | 2017 |  307 |  236 |
            | 2018 |  342 |  259 |
            | 2019 |  590 |  469 |
            | 2020 |  246 |  211 |

    - have PCSP for 4,326 / 7,466 auth'd members = 57.9%
        - Years: ** weird none match in '18, '19, '20
              |   yr |    nd |
              | :--- | :---  |
              | 2014 |     5 |
              | 2015 |   456 |
              | 2016 | 1,992 |
              | 2017 | 1,889 |
              | 2018 |     2 |

        - Only receive PCSP in these years
              |   yr |    nd |
              | :--- | :---  |
              | 2014 |    17 |
              | 2015 |   695 |
              | 2016 | 2,333 |
              | 2017 | 2,046 |
              | 2018 |     2 |

        - PCSP
            yr|    ND|is_deleted|not_deleted
          2014|    91|        28|     74
          2015| 3,525|     1,165|  2,944
          2016| 4,229|     2,342|  3,567
          2017| 4,238|     2,533|  3,696
          2018| 2,982|     1,026|  2,499
          2019| 3,821|     1,507|  3,175
          2020| 3,594|     3,594|  1,564


*/

-- matched auths
select
    -- distinct aha.first_name, aha.last_name, a.first_name, a.last_name
    extract(year from aha.start_date::date) yr,
    count(aha.member_id) N,
    count(distinct aha.member_id) ND
from
    junk.agp_hstar_auths aha
    join raw.agp_eligibility_20200628 a on trim(lower(a.member_id)) = trim(lower(aha.member_id)) -- 166,180 : 5,809
;

-- not matched auths
select
    -- distinct aha.first_name, aha.last_name, a.first_name, a.last_name
    extract(year from aha.start_date::date) yr,
    count(aha.member_id) N,
    count(distinct aha.member_id) ND
from
    junk.agp_hstar_auths aha
    left join raw.agp_eligibility_20200628 a on trim(lower(a.member_id)) = trim(lower(aha.member_id)) -- 166,180 : 5,809
 where
    a.member_id is null
group by 1
;

-- not matched historical eligibility
select
    -- distinct aha.first_name, aha.last_name, a.first_name, a.last_name
    --extract(year from a.effective_date::date) yr,
    count(a.member_id) N,
    count(distinct a.member_id) ND
from
    raw.agp_eligibility_20200628 a
    left join junk.agp_hstar_auths aha on trim(lower(a.member_id)) = trim(lower(aha.member_id)) -- 166,180 : 5,809
 where
    aha.member_id is null
--group by 1
;


------------------------------------------------
-- PCSP

drop table if exists pcsp_member_ids;
create temporary table pcsp_member_ids as
select distinct ahp.yr, ahp.healthstar_member_id::bigint healthstar_member_id from raw.pcsp_2017_2020 ahp where yr = '2019' ;
create index idx_pcsp_memberid on pcsp_member_ids(healthstar_member_id);

-- Have PCSP for Auth'd members
select
    ahp.healthstar_member_id is not null is_matched,
    count(distinct aha.member_id) ND
from
    junk.agp_hstar_auths aha
    left join pcsp_member_ids ahp on ahp.healthstar_member_id = aha.healthstar_member_id::bigint
group by 1;

select
    ahp.healthstar_member_id is not null is_matched,
    count(distinct aha.member_id) ND
from
    cb.hcbs_auths aha
    join vw_member_month_eligibility vmme on vmme.member_id = aha.member_id and vmme.mco_id = 1 and vmme.yearr = 2019
    left join pcsp_member_ids ahp on ahp.healthstar_member_id::bigint = aha.evv_member_id::bigint
where
    aha.mco_id = 1
group by 1;

create index idx_aha_healthstart_member_id on raw.pcsp_2017_2020(healthstar_member_id);
create index idx_agp_auths_healthstar_member_id on junk.agp_hstar_auths(healthstar_member_id);

select
    extract(year from aha.created_at::date) yr,
    count(distinct aha.member_id) ND,
    count(distinct case when ahp.healthstar_member_id is     null then aha.member_id end) ND_not_matched,
    count(distinct case when ahp.healthstar_member_id is not null then aha.member_id end) ND_matched
from
    junk.agp_hstar_auths aha
    left join pcsp_member_ids ahp on ahp.healthstar_member_id = aha.healthstar_member_id::bigint and ahp.yr::int = extract(year from aha.created_at::date)::int
group by 1
order by 1;

select x.member_id, x.mbi from raw.uhg_20200618_uhg_tn_memembership_all_txt x;
select distinct x.member_id from raw.uhg_20200618_uhg_tn_memembership_all_txt x;
select length(x.member_id), count(distinct member_id) from raw.uhg_20200618_uhg_tn_memembership_all_txt x group by 1 order by 2;


select
    ahp.yr,
    count(distinct ahp.healthstar_member_id) ND
from
    pcsp_member_ids ahp
group by 1
order by 1;

/*
    create table raw.pcsp_2017_2020 as
    select * from (
        select * from junk.pcsp_2017 p union all
        select * from junk.pcsp_2018 p union all
        select * from junk.pcsp_2019 p union all
        select * from junk.pcsp_2020 p
    ) x
*/


select * from raw.pcsp_2017_2020 x order by import_id, healthstar_member_id ;

drop table if exists _concepts;
create temporary table _concepts as
select concepts, count(1) N from (
    select x.import_id, x.source_member_id,
        array_agg(distinct x.concept_id order by x.concept_id) concepts
    from raw.pcsp_2017_2020 x group by 1, 2
) x group by 1
;

select c.concepts, array_length(c.concepts, 1), c.* from _concepts c order by c.count;
select count(c.concepts) from _concepts c;

select c.concepts[1], count(1) from _concepts c group by 1












select
    array_agg(pc.id) ids,
    array_agg(pc.number) numbers,
    pc.display
from
    junk.planofcare_concepts pc
where exists (select 1 from raw.pcsp_2017_2020 p where p.concept_id::int = pc.number and coalesce(trim(p.poc_concept_answer),'') <> '')
group by pc.display




drop table relevant_concepts;
create temporary table relevant_concepts as
select
    --pc.id,
    pc.number,
    max(pc.display) max_display,
    min(pc.display) min_display
from
    junk.planofcare_concepts pc
where exists (
    select
        1
    from
        raw.pcsp_2017_2020 p
    where
        p.concept_id::int = pc.number and coalesce(trim(p.poc_concept_answer),'') <> '')
group by 1
;

drop table if exists relevant_rollup_concepts;
create temporary table relevant_rollup_concepts as
select
    rc.max_display,
    array_agg(rc.number) numbers
from
    relevant_concepts rc
group by 1;

select
    array_agg(distinct yr) response_years,
    array_agg(distinct p.concept_id) concept_ids,
    count(distinct p.concept_id) ND_concepts,
    lower(trim(rc.max_display)) display,
    count(1) N,
    array_agg(distinct p.response) poc_concept_answers
    -- array_agg(distinct p.raw_concept_answer) raw_concept_answers,
    -- array_agg(distinct p.raw_response) raw_responses
from junk.pcsp_2017_2020 p
    join relevant_concepts rc on p.concept_id::int = rc.number
where
    --p.concept_id::bigint = 534756
    p.concept_id::int <> 534625
group by 4
having count(1) >= 500
       and array_agg(distinct yr) && Array[2019]
       and lower(trim(rc.max_display)) not like '%condition of%'
order by 4
;
select distinct p.raw_question_number from junk.pcsp_2017_2020 p;

select pc.procedure_code_description, aha.hcpcs, count(*) from junk.agp_hstar_auths aha
join ref.procedure_codes pc on pc.procedure_code = lower(trim(aha.hcpcs))
group by 1, 2;

select a.mbi, a.member_id, a.medicaid_id from raw.agp_eligibility_20200628 a

select
    trim(lower(a.medicaid_id)) source_member_id, -- source_member_id
    aha.member_id payer_member_id,            -- payer_member_id
    aha.healthstar_member_id evv_member_id, -- evv_member_id
    aha.service_title,
    aha.service_description,
    --extract( day from end_date::timestamp - start_date::timestamp) days,
    aha.hcpcs in ('S5125', 'T1019') is_hcbs_pcs,
    aha.hcpcs in ('S9125','S5150')  is_respit,
    aha.hcpcs in ('S5170') is_meal_delivery,
    aha.start_date,
    aha.end_date,
    aha.units,
    aha.units_daily,
    aha.units_weekly,
    aha.hcpcs procedure_code,
    aha.modifiers,
    aha.first_name,
    aha.last_name,
    aha.dob,
    aha.auth_ref_no,
    aha.days  auth_days,
    aha.hours auth_hours,
    aha.is_active,
    aha.provider_id evv_provider_id,
    -- provider_npi
    -- provider_tax_id,
    case when aha.program_id::int = 1 then 'choices'
         when aha.program_id::int = 2 then 'ecf'
    end hcbs_program,
    aha.updated_at source_updated_at,
    aha.created_at source_created_at
from junk.agp_hstar_auths aha
     left join raw.agp_eligibility_20200628 a on trim(lower(a.member_id)) = trim(lower(aha.member_id))
where
    --aha.hcpcs in ('S5125', 'T1019')
    aha.is_deleted <> '1'
order by
    aha.member_id,
    aha.start_date,
    aha.end_date
;

select
    hcpcs, count(1)
from junk.agp_hstar_auths aha
where
    --aha.hcpcs in ('S5125', 'T1019')
     aha.is_deleted <> '1'
group by 1;

select pc.procedure_code_description, aha.hcpcs, count(*) from junk.agp_hstar_auths aha
join ref.procedure_codes pc on pc.procedure_code = lower(trim(aha.hcpcs))
group by 1, 2;


alter function zz_create_view_from_table(text, text) owner to postgres;



-- RESPIT --


select
    -- ha.procedure_code, ha.payer_member_id, ha.start_date, ha.end_date, ha.id,
    -- ha2.procedure_code, ha2.payer_member_id, ha2.start_date, ha2.end_date, ha2.id
    ha.id, ha2.id
from
    cb.hcbs_auths ha
    join cb.hcbs_auths ha2 on ha.payer_member_id = ha2.payer_member_id
                              and (ha.start_date, ha.end_date) overlaps (ha2.start_date, ha2.end_date)
                              and ha.start_date <> ha2.end_date
                              and ha2.start_date <> ha.end_date
                              and ha.id > ha2.id
                              and ha.procedure_code = ha2.procedure_code
where
    ha.procedure_code <> 's5170'

select * from cb.hcbs_auths ha where ha.id in (117, 118)

select * from cb.hcbs_auths ha where ha.id in (63, 64)



select distinct string_to_array(auth_days, ','), auth_days from cb.hcbs_auths ha where ha.procedure_code <> 's5170' and array_length(string_to_array(auth_days, ','), 1) = 1;


select
    ha.mco_id,
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
    updated_at,
    id
from cb.hcbs_auths ha

;
drop materialized view vw_slam_hcbs_auths;
create materialized view vw_slam_hcbs_auths as
select
    ha.mco_id,
    ha.payer_member_id,
    ha.member_id,
    ha.evv_member_id,
    ha.first_name,
    ha.last_name,
    ha.dob,
    is_hcbs_pcs,
    ha.start_date,
    ha.end_date,
    row_number() over (order by random()) vid,
    sum(units) units,
    array_agg(ha.procedure_code) procedure_codes,
    array_agg(ha.modifiers) modifiers
    --array_agg(string_to_array(auth_days, ',')) auth_days
from
    cb.hcbs_auths ha
where
    ha.is_hcbs_pcs
group by
    1,2,3,4,5,6,7,8,9,10


select * from vw_slam_hcbs_auths va
    join vw_slam_hcbs_auths va2 on


select
    -- ha.procedure_code, ha.payer_member_id, ha.start_date, ha.end_date, ha.id,
    -- ha2.procedure_code, ha2.payer_member_id, ha2.start_date, ha2.end_date, ha2.id
    ha.vid, ha2.vid

from
    vw_slam_hcbs_auths ha
    join vw_slam_hcbs_auths ha2 on ha.payer_member_id = ha2.payer_member_id
                              and (ha.start_date, ha.end_date) overlaps (ha2.start_date, ha2.end_date)
                              and ha.start_date <> ha2.end_date
                              and ha2.start_date <> ha.end_date
                              and ha.vid > ha2.vid

select * from vw_slam_hcbs_auths va where va.vid in (166,58)


select d.day_text from ref.dates d;
select bom, payer_member_id, units from vw_hcbs_pcs_units_mm vh order by 3 desc;

select * from cb.hcbs_auths ha where ha.payer_member_id::bigint = 719656838 and (ha.start_date, ha.end_date) overlaps ('2019-01-01'::date, '2019-01-31'::date)
and is_hcbs_pcs
;

    select distinct trim(dayy) from
    (
        select unnest(string_to_array(ha.auth_days,',')) dayy
        from cb.hcbs_auths ha
    ) x

select ha.auth_days is null, count(*) N from cb.hcbs_auths ha where ha.is_hcbs_pcs  group by 1;
with x as (
    select *
    from cb.hcbs_auths ha
    where ha.is_hcbs_pcs and ha.auth_days is null and ha.start_date <> ha.end_date
          and ha.units_daily < units
), ooverlaps as (
    select
        distinct Array[x.id, x2.id] sett
    from
        x
        join x x2 on x.payer_member_id = x2.payer_member_id and (x.start_date, x.end_date) overlaps (x2.start_date, x2.end_date)
            and x.id > x2.id
)
select
    o.sett, h.id, h.*
from
    ooverlaps o
    join cb.hcbs_auths h on h.id in (select unnest(o.sett))
order by o.sett, h.id





drop materialized view vw_hcbs_pcs_units_mm;
create materialized view vw_hcbs_pcs_units_mm as
select
    *,
    (units / 4)::int hrs
from (
    select
        d.month, d.bom, d.eom, ha.payer_member_id, ha.evv_member_id, ha.member_id,
        sum(
          case when (ha.units / extract(day from ha.end_date::timestamp - ha.start_date::timestamp)) > ha.units_daily
          then ha.units_daily
          else (ha.units / extract(day from ha.end_date::timestamp - ha.start_date::timestamp)) end
        )::int units
    from
        cb.hcbs_auths ha
        join ref.dates d on d.day between ha.start_date and end_date and ha.auth_days ~ left(d.day_text, 3)
    where
        ha.is_hcbs_pcs
        and ha.payer_member_id::bigint <> 715724436 -- b/c fuck him
        and ha.start_date <> ha.end_date
        and ha.units_daily <= ha.units
    group by 1,2,3,4,5,6
) x


select
    distinct v.bom
from vw_hcbs_pcs_units_mm v
    order by 1 ;

drop table if exists junk.auth_comps_2019;
create table junk.auth_comps_2019 as
with mbr_2019 as (
    select
        *
    from
        (
            select
                v.payer_member_id,
                v.evv_member_id,
                v.member_id,
                (sum(case when v.bom = '2019-01-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_01,
                (sum(case when v.bom = '2019-02-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_02,
                (sum(case when v.bom = '2019-03-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_03,
                (sum(case when v.bom = '2019-04-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_04,
                (sum(case when v.bom = '2019-05-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_05,
                (sum(case when v.bom = '2019-06-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_06,
                (sum(case when v.bom = '2019-07-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_07,
                (sum(case when v.bom = '2019-08-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_08,
                (sum(case when v.bom = '2019-09-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_09,
                (sum(case when v.bom = '2019-10-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_10,
                (sum(case when v.bom = '2019-11-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_11,
                (sum(case when v.bom = '2019-12-01'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_12,

                (sum(case when v.bom between '2019-01-01'::date and '2019-03-31'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_q1,
                (sum(case when v.bom between '2019-04-01'::date and '2019-06-30'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_q2,
                (sum(case when v.bom between '2019-07-01'::date and '2019-09-30'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_q3,
                (sum(case when v.bom between '2019-10-01'::date and '2019-12-31'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019_q4,

                (sum(case when v.bom between '2019-01-01'::date and '2019-12-31'::date then v.hrs end / extract(day from v.eom::timestamp - bom::timestamp) * 30)::int) hrs_2019
            from
                vw_hcbs_pcs_units_mm v
            where
                extract(year from v.bom) = 2019
            group by 1,2,3
        ) x
    where
          hrs_2019_01 is not null and hrs_2019_02 is not null and hrs_2019_03 is not null
      and hrs_2019_04 is not null and hrs_2019_05 is not null and hrs_2019_06 is not null
      and hrs_2019_07 is not null and hrs_2019_08 is not null and hrs_2019_09 is not null
      and hrs_2019_10 is not null and hrs_2019_11 is not null and hrs_2019_12 is not null
),
 mbr_avg as (
     select
         ((hrs_2019_01 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_01,
         ((hrs_2019_02 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_02,
         ((hrs_2019_03 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_03,
         ((hrs_2019_04 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_04,
         ((hrs_2019_05 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_05,
         ((hrs_2019_06 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_06,
         ((hrs_2019_07 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_07,
         ((hrs_2019_08 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_08,
         ((hrs_2019_09 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_09,
         ((hrs_2019_10 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_10,
         ((hrs_2019_11 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_11,
         ((hrs_2019_12 * 12 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_12,

         ((hrs_2019_q1 *  4 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_q1,
         ((hrs_2019_q2 *  4 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_q2,
         ((hrs_2019_q3 *  4 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_q3,
         ((hrs_2019_q4 *  4 * 100.0 / hrs_2019))::decimal(16,2) pct_2019_q4,

         ((hrs_2019    *  1 * 100.0 / hrs_2019))::decimal(16,2) pct_2019,

         '_' _,

         *
     from
         mbr_2019 m
 )
select * from mbr_avg;


-- z_scores
create temporary table _fuckles as
with q_avg as (
    select
        avg(j.pct_2019_q1) avg_q1,
        avg(j.pct_2019_q2) avg_q2,
        avg(j.pct_2019_q3) avg_q3,
        avg(j.pct_2019_q4) avg_q4,
        stddev(j.pct_2019_q1) sdev_q1,
        stddev(j.pct_2019_q2) sdev_q2,
        stddev(j.pct_2019_q3) sdev_q3,
        stddev(j.pct_2019_q4) sdev_q4
    from junk.auth_comps_2019 j
)
select
    extract( year from age('12-31-2019'::date, m.date_of_birth)) age,
    ((a.pct_2019_q1 - qa.avg_q1) / qa.sdev_q1)::decimal(16,2) z_q1,
    ((a.pct_2019_q2 - qa.avg_q2) / qa.sdev_q2)::decimal(16,2) z_q2,
    ((a.pct_2019_q3 - qa.avg_q3) / qa.sdev_q3)::decimal(16,2) z_q3,
    ((a.pct_2019_q4 - qa.avg_q4) / qa.sdev_q4)::decimal(16,2) z_q4,
    a.*
from
    junk.auth_comps_2019 a
    join cb.members m on a.member_id = m.id and m.mco_id = 1
    cross join q_avg qa
order by 3 desc


select
    case when f.age between 90 and 1000 then '1 90-1000'
         when f.age between 65 and 89   then '2 65-89'
         when f.age between 45 and 64   then '3 45-64'
         else '4 < 45' end age_bands,
    avg(f.z_q1)::decimal(16,2) z_q1,
    avg(f.z_q2)::decimal(16,2) z_q2,
    avg(f.z_q3)::decimal(16,2) z_q3,
    avg(f.z_q4)::decimal(16,2) z_q4,
    '_' __,
    max(f.z_q1)::decimal(16,2) max_z_q1,
    max(f.z_q2)::decimal(16,2) max_z_q2,
    max(f.z_q3)::decimal(16,2) max_z_q3,
    max(f.z_q4)::decimal(16,2) max_z_q4,
    '_' _,
    min(f.z_q1)::decimal(16,2) min_z_q1,
    min(f.z_q2)::decimal(16,2) min_z_q2,
    min(f.z_q3)::decimal(16,2) min_z_q3,
    min(f.z_q4)::decimal(16,2) min_z_q4
from
    _fuckles f
group by 1
order by 1


select
    es.member_id,
    avg(line_of_business_id) avg_lob
from
    cb.eligibility_segments es
where es.mco_id = 1
    and (es.start_date, es.end_date) overlaps ('01-01-2019', '12-31-2019')
group by 1
having avg(line_of_business_id) <> 1 and avg(line_of_business_id) <> 3


refresh materialized view cb.vw_member_month_eligibility;
select distinct ggroup from cb.vw_member_month_eligibility where mco_id = 1;

create table midget as
select
    es.member_id,
    cb.round_half_down1(avg(ggroup::int)) ggroup
from
    cb.eligibility_segments es
where
      es.mco_id = 1
  and (es.start_date, es.end_date) overlaps ('01-01-2019', '12-31-2019')
group by 1


update cb.eligibility_days ed
    set ggroup = es.ggroup
from
    cb.eligibility_details e
    join cb.eligibility_segments es on e.eligibility_segment_id = es.id
where
    e.eligibility_day_id = ed.id
    and ed.mco_id = 1;

update cb.eligibility_details e
    set ggroup = es.ggroup
from
    cb.eligibility_segments es
where
    e.eligibility_segment_id = es.id
    and es.mco_id = 1;

select distinct ggroup from cb.vw_member_month_eligibility where mco_id = 1;

create temporary table _midget_fuckles as
with q_avg as (
    select
        avg(j.pct_2019_q1) avg_q1,
        avg(j.pct_2019_q2) avg_q2,
        avg(j.pct_2019_q3) avg_q3,
        avg(j.pct_2019_q4) avg_q4,
        stddev(j.pct_2019_q1) sdev_q1,
        stddev(j.pct_2019_q2) sdev_q2,
        stddev(j.pct_2019_q3) sdev_q3,
        stddev(j.pct_2019_q4) sdev_q4
    from junk.auth_comps_2019 j
)
select
    mig.ggroup,
    ((a.pct_2019_q1 - qa.avg_q1) / qa.sdev_q1)::decimal(16,2) z_q1,
    ((a.pct_2019_q2 - qa.avg_q2) / qa.sdev_q2)::decimal(16,2) z_q2,
    ((a.pct_2019_q3 - qa.avg_q3) / qa.sdev_q3)::decimal(16,2) z_q3,
    ((a.pct_2019_q4 - qa.avg_q4) / qa.sdev_q4)::decimal(16,2) z_q4,
    a.*
from
    junk.auth_comps_2019 a
    join midget mig on mig.member_id = a.member_id
    cross join q_avg qa
;

select
    ggroup,
    avg(f.z_q1)::decimal(16,2) z_q1,
    avg(f.z_q2)::decimal(16,2) z_q2,
    avg(f.z_q3)::decimal(16,2) z_q3,
    avg(f.z_q4)::decimal(16,2) z_q4,
    '_' __,
    max(f.z_q1)::decimal(16,2) max_z_q1,
    max(f.z_q2)::decimal(16,2) max_z_q2,
    max(f.z_q3)::decimal(16,2) max_z_q3,
    max(f.z_q4)::decimal(16,2) max_z_q4,
    '_' _,
    min(f.z_q1)::decimal(16,2) min_z_q1,
    min(f.z_q2)::decimal(16,2) min_z_q2,
    min(f.z_q3)::decimal(16,2) min_z_q3,
    min(f.z_q4)::decimal(16,2) min_z_q4
from
    _midget_fuckles f
group by 1
order by 1



select
    x.*
from (
    select
        member_id,
        round_half_down1(avg(case when e.mmonth between '01-01-2019' and '03-31-2019' then ggroup::int end)) ggroup_q1,
        round_half_down1(avg(case when e.mmonth between '04-01-2019' and '06-30-2019' then ggroup::int end)) ggroup_q2,
        round_half_down1(avg(case when e.mmonth between '07-01-2019' and '09-30-2019' then ggroup::int end)) ggroup_q3,
        round_half_down1(avg(case when e.mmonth between '10-01-2019' and '12-31-2019' then ggroup::int end)) ggroup_q4,
        '_' __,
        count(case when e.mmonth between '01-01-2019' and '03-31-2019' then member_id end) mmq1,
        count(case when e.mmonth between '04-01-2019' and '06-30-2019' then member_id end) mmq2,
        count(case when e.mmonth between '07-01-2019' and '09-30-2019' then member_id end) mmq3,
        count(case when e.mmonth between '10-01-2019' and '12-31-2019' then member_id end) mmq4
    from cb.vw_member_month_eligibility e where mco_id = 1 and e.yearr = 2019
    group by 1
) x
;

select distinct es.product_id, es.product, es.sub_product, line_of_business_id from eligibility_segments es where mco_id = 1;
select
    distinct
    product,
    subproduct,
    sub_group
from raw.agp_eligibility_20200628 a
where  subproduct ~ 'NON DUAL'


select distinct line_of_business from raw.agp_eligibility_20200628 a;

select
    distinct c.procedure_code, c.procedure_mod, pc.procedure_code_description
from claims c
    join ref.procedure_codes pc on c.procedure_code_id = pc.id
where
    c.mco_id = 1
    and c.procedure_code in ('t2033', 't2016')
order by 1, 2



select
    (ha.units / extract(day from ha.end_date::timestamp - ha.start_date::timestamp))/4 > 24, count(1)
from cb.hcbs_auths ha
where ha.is_hcbs_pcs and ha.start_date <> ha.end_date
group by 1;









drop table if exists  _fucked;
create temporary table _fucked as
select
    d.day, ha.payer_member_id,
    sum(
      case when (ha.units / extract(day from ha.end_date::timestamp - ha.start_date::timestamp)) > ha.units_daily
      then ha.units_daily
      else (ha.units / extract(day from ha.end_date::timestamp - ha.start_date::timestamp)) end
    ) units
from
    cb.hcbs_auths ha
    join ref.dates d on d.day between ha.start_date and end_date and ha.auth_days ~ left(d.day_text, 3)
where
    ha.is_hcbs_pcs
    and ha.start_date <> ha.end_date
    and ha.units_daily <= ha.units
group by 1,2
having sum(
      case when (ha.units / extract(day from ha.end_date::timestamp - ha.start_date::timestamp)) > ha.units_daily
      then ha.units_daily
      else (ha.units / extract(day from ha.end_date::timestamp - ha.start_date::timestamp)) end
    ) / 4 > 24

/******************
  ** FUCK THIS GUY 715724436
 ***/

select f.day, payer_member_id, units, units / 4 from _fucked f order by f.units desc

select * from cb.hcbs_auths ha where ha.payer_member_id::int = 721737041 and '2016-03-19' between ha.start_date and ha.end_date




select
    c.member_id,
    c.bom,
    c.lob,
    cm1.grp                                                                        grpm1,
    c.grp,
    cp1.grp                                                                        grp_1,
    cp2.grp                                                                        grp_2,
    case when cm1.member_id is not null then coalesce(cm1.icfs_tc, 0) + coalesce(cm1.snfa_tc, 0) + coalesce(cm1.ipat_tc, 0) end "tc-1",
    coalesce(c.icfs_tc, 0) + coalesce(c.snfa_tc, 0) + coalesce(c.ipat_tc, 0)       tc,
    case when cp1.member_id is not null then  coalesce(cp1.icfs_tc, 0) + coalesce(cp1.snfa_tc, 0) + coalesce(cp1.ipat_tc, 0) end "tc+1",
    case when cp2.member_id is not null then  coalesce(cp2.icfs_tc, 0) + coalesce(cp2.snfa_tc, 0) + coalesce(cp2.ipat_tc, 0) end "tc+2",
    --coalesce(c.icfs_tc,0) icfs_tc,
    --coalesce(c.snfa_tc,0) snfa_tc,
    --coalesce(c.ipat_tc,0) ipat_tc,
    am1.attd_pcs_hrs                                                               "hrs-1",
    a_0.attd_pcs_hrs                                                               hrs,
    ap1.attd_pcs_hrs                                                               "hrs+1",
    ap2.attd_pcs_hrs                                                               "hrs+2",
    ap3.attd_pcs_hrs                                                               "hrs+3"
from
    vw_ds_claims_mm c
    left join vw_ds_claims_mm cm1 on c.member_id = cm1.member_id and (c.bom - interval '1 month' ) = cm1.bom
    left join vw_ds_claims_mm cp1 on c.member_id = cp1.member_id and (c.bom + interval '1 month' ) = cp1.bom
    left join vw_ds_claims_mm cp2 on c.member_id = cp2.member_id and (c.bom + interval '2 month' ) = cp2.bom

    left join vw_ds_auth_mm am1   on c.member_id = am1.member_id and (c.bom - interval '1 month') = am1.bom
    left join vw_ds_auth_mm a_0   on c.member_id = a_0.member_id and c.bom = a_0.bom
    left join vw_ds_auth_mm ap1   on c.member_id = ap1.member_id and (c.bom + interval '1 month') = ap1.bom
    left join vw_ds_auth_mm ap2   on c.member_id = ap2.member_id and (c.bom + interval '2 month') = ap2.bom
    left join vw_ds_auth_mm ap3   on c.member_id = ap3.member_id and (c.bom + interval '3 month') = ap3.bom
where
      c.bom between '2019-01-01' and '2019-12-31'
  and exists(
              select
                  1
              from
                  vw_ds_auth_mm ha
              where
                    ha.mco_id = 1
                and ha.member_id = c.member_id
                and ha.attd_pcs_hrs > 0
                and ha.bom between '2019-01-01' and '2019-12-31'
          )
  and c.mco_id = 1
  and (c.icfs_tc > 0 or c.snfa_tc > 0 or c.ipat_tc > 0)

select
    c.bom,
    coalesce(c.icfs_tc, 0) + coalesce(c.snfa_tc, 0) + coalesce(c.ipat_tc, 0) fac_tc,
    a_0.attd_pcs_hrs
from
    vw_ds_claims_mm c
    left join vw_ds_auth_mm a_0 on c.member_id = a_0.member_id and c.bom = a_0.bom
where c.member_id = 23443


select
    grp_dec_18,
    MM_18,
    MM_19,
    count(distinct case when grp_dec_19 = 2 then x.member_id end) nd_2,
    count(distinct case when grp_dec_19 = 3 then x.member_id end) nd_3,
    count(distinct x.member_id) nd
from (
    select
        vmme.member_id,
        max(case when vmme.mmonth = '12-01-2018' then vmme.ggroup end)::int grp_dec_18,
        max(case when vmme.mmonth = '12-01-2019' then vmme.ggroup end)::int grp_dec_19,
        bool_or(vmme.mmonth = '01-01-2018') jan_18,
        bool_or(vmme.mmonth = '12-01-2018') dec_18,
        bool_or(vmme.mmonth = '01-01-2019') jan_19,
        bool_or(vmme.mmonth = '12-01-2019') dec_19,
        count(case when vmme.mmonth between '01-01-2018' and '12-31-2018' then 1 end ) MM_18,
        count(case when vmme.mmonth between '01-01-2019' and '12-31-2019' then 1 end ) MM_19
    from
        vw_member_month_eligibility vmme
    where
        vmme.mco_id = 1
        and vmme.ggroup::int = 2
    group by 1
) x
where x.grp_dec_18 = 2
group by 1, 2, 3


select
    x.jan_18 and x.dec_18 _elig_18,
    x.jan_19 and x.dec_19 _elig_19,
    count(distinct x.member_id) nd
from (
    select
        vmme.member_id,
        bool_or(vmme.mmonth = '01-01-2018') jan_18,
        bool_or(vmme.mmonth = '12-01-2018') dec_18,
        bool_or(vmme.mmonth = '01-01-2019') jan_19,
        bool_or(vmme.mmonth = '12-01-2019') dec_19
    from
        vw_member_month_eligibility vmme
    where
        vmme.mco_id = 1
        and vmme.ggroup::int = 2
    group by
        1
) x
group by 1, 2




select distinct vmme.line_of_business_id, count(distinct vmme.member_id) nd from vw_member_month_eligibility vmme where vmme.mco_id = 2 group by 1


select
    vm.grp,
    v.member_id, v.bom,
    v.attd_pcs_appropriate_hrs,
    v.avg_auth_hrs_for_fake,
    v.attd_pcs_appropriate_hrs - v.attd_pcs_visit_hrs delta,
    v.attd_pcs_appropriate_hrs,
    v.attd_pcs_visit_hrs, v.attd_pcs_appropriate_hrs, v.attd_pcs_missed_hrs, v.visit_hrs,
    coalesce(vm.icfs_tc,0) icfs_tc,
    coalesce(vm.snfa_tc,0) snfa_tc,
    coalesce(vm.ipat_tc,0) ipat_tc
from
    vw_ds_visit_features_mm v
    join vw_ds_claims_mm vm on v.bom = vm.bom and v.member_id = vm.member_id
where
    v.mco_id = 1
    and v.bom between '01-01-2019' and '12-31-2019'
    --and v.member_id = 26836
    and (v.attd_pcs_appropriate_hrs - v.attd_pcs_visit_hrs) > 200
    and v.member_id <> 26937 --26836


select
    avg(v.attd_pcs_hrs)
from
    vw_ds_auth_mm v
where
    v.member_id = 26937
    and v.mco_id = 1
    and v.bom between '01-01-2019' and '12-31-2019'

select av.start_time - av.end_time
from appointments_visits av
where member_id = 26836
    and service_date between '2019-07-01' and '2019-07-31'
    and auth_procedure_code in ('s5125','t1019')
    and av.start_time != av.end_time

select start_time, end_time, service_date
from appointments_visits
where member_id = 22717
    and service_date between '2019-12-01' and '2019-12-31'
    and auth_procedure_code in ('s5125','t1019')
order by service_date, 1, 2




select
    round_half_down1(avg(lob_avg)) lob_avg_avg_half,
    case when _01_2018 then 1 else 0 end _01_2018,
    case when _02_2018 then 1 else 0 end _02_2018,
    case when _03_2018 then 1 else 0 end _03_2018,
    case when _04_2018 then 1 else 0 end _04_2018,
    case when _05_2018 then 1 else 0 end _05_2018,
    case when _06_2018 then 1 else 0 end _06_2018,
    case when _07_2018 then 1 else 0 end _07_2018,
    case when _08_2018 then 1 else 0 end _08_2018,
    case when _09_2018 then 1 else 0 end _09_2018,
    case when _10_2018 then 1 else 0 end _10_2018,
    case when _11_2018 then 1 else 0 end _11_2018,
    case when _12_2018 then 1 else 0 end _12_2018,
    case when _01_2019 then 1 else 0 end _01_2019,
    case when _02_2019 then 1 else 0 end _02_2019,
    case when _03_2019 then 1 else 0 end _03_2019,
    case when _04_2019 then 1 else 0 end _04_2019,
    case when _05_2019 then 1 else 0 end _05_2019,
    case when _06_2019 then 1 else 0 end _06_2019,
    case when _07_2019 then 1 else 0 end _07_2019,
    case when _08_2019 then 1 else 0 end _08_2019,
    case when _09_2019 then 1 else 0 end _09_2019,
    case when _10_2019 then 1 else 0 end _10_2019,
    case when _11_2019 then 1 else 0 end _11_2019,
    case when _12_2019 then 1 else 0 end _12_2019,
    count(x.member_id) N,
    count(distinct x.member_id) ND
from  (
    select
        es.member_id,
        avg(line_of_business_id) lob_avg,
        bool_or(('2018-12-31', '01-01-2020') overlaps (es.start_date, es.end_date)) all_2019,
        bool_or(('2017-12-31', '01-01-2019') overlaps (es.start_date, es.end_date)) all_2018,

        bool_or(('2019-11-30', '01-01-2020') overlaps (es.start_date, es.end_date)) _12_2019,
        bool_or(('2019-10-30', '12-01-2019') overlaps (es.start_date, es.end_date)) _11_2019,
        bool_or(('2019-09-30', '11-01-2019') overlaps (es.start_date, es.end_date)) _10_2019,
        bool_or(('2019-08-30', '10-01-2019') overlaps (es.start_date, es.end_date)) _09_2019,
        bool_or(('2019-07-30', '09-01-2019') overlaps (es.start_date, es.end_date)) _08_2019,
        bool_or(('2019-06-30', '08-01-2019') overlaps (es.start_date, es.end_date)) _07_2019,
        bool_or(('2019-05-30', '07-01-2019') overlaps (es.start_date, es.end_date)) _06_2019,
        bool_or(('2019-04-30', '06-01-2019') overlaps (es.start_date, es.end_date)) _05_2019,
        bool_or(('2019-03-30', '05-01-2019') overlaps (es.start_date, es.end_date)) _04_2019,
        bool_or(('2019-02-28', '04-01-2019') overlaps (es.start_date, es.end_date)) _03_2019,
        bool_or(('2019-01-31', '03-01-2019') overlaps (es.start_date, es.end_date)) _02_2019,
        bool_or(('2018-12-31', '02-01-2019') overlaps (es.start_date, es.end_date)) _01_2019, ----
        bool_or(('2018-11-30', '01-01-2019') overlaps (es.start_date, es.end_date)) _12_2018,
        bool_or(('2018-10-30', '12-01-2018') overlaps (es.start_date, es.end_date)) _11_2018,
        bool_or(('2018-09-30', '11-01-2018') overlaps (es.start_date, es.end_date)) _10_2018,
        bool_or(('2018-08-30', '10-01-2018') overlaps (es.start_date, es.end_date)) _09_2018,
        bool_or(('2018-07-30', '09-01-2018') overlaps (es.start_date, es.end_date)) _08_2018,
        bool_or(('2018-06-30', '08-01-2018') overlaps (es.start_date, es.end_date)) _07_2018,
        bool_or(('2018-05-30', '07-01-2018') overlaps (es.start_date, es.end_date)) _06_2018,
        bool_or(('2018-04-30', '06-01-2018') overlaps (es.start_date, es.end_date)) _05_2018,
        bool_or(('2018-03-30', '05-01-2018') overlaps (es.start_date, es.end_date)) _04_2018,
        bool_or(('2018-02-28', '04-01-2018') overlaps (es.start_date, es.end_date)) _03_2018,
        bool_or(('2018-01-31', '03-01-2018') overlaps (es.start_date, es.end_date)) _02_2018,
        bool_or(('2017-12-31', '02-01-2018') overlaps (es.start_date, es.end_date)) _01_2018
    from
        eligibility_segments es
    where
        es.mco_id = 2
        and replace(replace(es.ggroup, 'a', ''),'b','')::int = 2
    group by 1
) x
where
    all_2018
group by 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25



with yrs as (
    select * from (
        values
            ('2017-18', '01-01-2017'::date, '12-01-2017'::date, '01-01-2018'::date, '12-01-2018'::date ),
            ('2018-19', '01-01-2018'::date, '12-01-2018'::date, '01-01-2019'::date, '12-01-2019'::date )
    )x(time_period, boy1, eoy1, boy2, eoy2)
)
select
    time_period,
    grp_dec_y1,
    avg(lob_avg) lob_avg,
    avg(MM_y1) MM_y1,
    avg(MM_y2) MM_y2,
    count(distinct case when grp_dec_y2 = 2 then x.member_id end) nd_2_y2,
    count(distinct case when grp_dec_y2 = 3 then x.member_id end) nd_3_y2,
    avg(grp_3_n_mm_y2) grp_3_mm_avg_hope_zero_y2,
    max(grp_3_n_mm_y2) grp_3_mm_max_hope_zero_y2,
    count(distinct x.member_id) nd
from (
    select
        yrs.time_period,
        vmme.member_id,
        avg(line_of_business_id) lob_avg,
        max(case when vmme.mmonth = yrs.eoy1 then vmme.grp end)::int grp_dec_y1,
        max(case when vmme.mmonth = yrs.eoy2 then vmme.grp end)::int grp_dec_y2,
        count(case when vmme.mmonth between boy2 and eoy2 and vmme.grp::int = 3 then 1 end)::int grp_3_n_mm_y2,
        bool_or(vmme.mmonth = boy1) jan_y1,
        bool_or(vmme.mmonth = eoy1) dec_y1,
        bool_or(vmme.mmonth = boy2) jan_y2,
        bool_or(vmme.mmonth = eoy2) dec_y2,
        count(case when vmme.mmonth between yrs.boy1 and yrs.eoy1 then 1 end ) MM_y1,
        count(case when vmme.mmonth between yrs.boy2 and yrs.eoy2 then 1 end ) MM_y2
    from
        (select replace(replace(ggroup, 'a',''),'b','')::int grp, * from vw_member_month_eligibility) vmme
        cross join yrs
    where
        vmme.mco_id = 1
        and vmme.grp = 2
        --and vmme.line_of_business_id = 3
    group by 1,2
) x
where
    x.grp_dec_y1 = 2
    and MM_y1 > 6
    and MM_y2 > 6
group by 1,2



with x as (
    select
        es.member_id,
        extract(year from date_of_death) dod_yr,
        extract(month from date_of_death)::int dod_month,
        avg(line_of_business_id) lob_avg,
        max(term_reason) filter ( where ('2018-12-31', '01-01-2020') overlaps (es.start_date, es.end_date)) term_reason,
        bool_or(('2018-12-31', '01-01-2020') overlaps (es.start_date, es.end_date)) all_2019,
        bool_or(('2017-12-31', '01-01-2019') overlaps (es.start_date, es.end_date)) all_2018,
        bool_or(('2018-11-30', '01-01-2019') overlaps (es.start_date, es.end_date)) dec_2018,

        bool_or(('2019-11-30', '01-01-2020') overlaps (es.start_date, es.end_date)) _12_2019,
        bool_or(('2019-10-30', '12-01-2019') overlaps (es.start_date, es.end_date)) _11_2019,
        bool_or(('2019-09-30', '11-01-2019') overlaps (es.start_date, es.end_date)) _10_2019,
        bool_or(('2019-08-30', '10-01-2019') overlaps (es.start_date, es.end_date)) _09_2019,
        bool_or(('2019-07-30', '09-01-2019') overlaps (es.start_date, es.end_date)) _08_2019,
        bool_or(('2019-06-30', '08-01-2019') overlaps (es.start_date, es.end_date)) _07_2019,
        bool_or(('2019-05-30', '07-01-2019') overlaps (es.start_date, es.end_date)) _06_2019,
        bool_or(('2019-04-30', '06-01-2019') overlaps (es.start_date, es.end_date)) _05_2019,
        bool_or(('2019-03-30', '05-01-2019') overlaps (es.start_date, es.end_date)) _04_2019,
        bool_or(('2019-02-28', '04-01-2019') overlaps (es.start_date, es.end_date)) _03_2019,
        bool_or(('2019-01-31', '03-01-2019') overlaps (es.start_date, es.end_date)) _02_2019,
        bool_or(('2018-12-31', '02-01-2019') overlaps (es.start_date, es.end_date)) _01_2019, ----
        bool_or(('2018-11-30', '01-01-2019') overlaps (es.start_date, es.end_date)) _12_2018,
        bool_or(('2018-10-30', '12-01-2018') overlaps (es.start_date, es.end_date)) _11_2018,
        bool_or(('2018-09-30', '11-01-2018') overlaps (es.start_date, es.end_date)) _10_2018,
        bool_or(('2018-08-30', '10-01-2018') overlaps (es.start_date, es.end_date)) _09_2018,
        bool_or(('2018-07-30', '09-01-2018') overlaps (es.start_date, es.end_date)) _08_2018,
        bool_or(('2018-06-30', '08-01-2018') overlaps (es.start_date, es.end_date)) _07_2018,
        bool_or(('2018-05-30', '07-01-2018') overlaps (es.start_date, es.end_date)) _06_2018,
        bool_or(('2018-04-30', '06-01-2018') overlaps (es.start_date, es.end_date)) _05_2018,
        bool_or(('2018-03-30', '05-01-2018') overlaps (es.start_date, es.end_date)) _04_2018,
        bool_or(('2018-02-28', '04-01-2018') overlaps (es.start_date, es.end_date)) _03_2018,
        bool_or(('2018-01-31', '03-01-2018') overlaps (es.start_date, es.end_date)) _02_2018,
        bool_or(('2017-12-31', '02-01-2018') overlaps (es.start_date, es.end_date)) _01_2018
    from
        eligibility_segments es
        join members m on es.member_id = m.id and es.mco_id = es.mco_id
    where
        es.mco_id = 2
        --and es.line_of_business_id = 1 /*medicaid*/
        and replace(replace(es.ggroup, 'a', ''),'b','')::int = 2
    group by 1, 2, 3
)
select
    term_reason,
    x._12_2019,
    dod_yr,
    array_agg(distinct dod_month) dod_months,
    array_agg(distinct member_id) member_ids,
    count(distinct member_id) ND
from
    x
where
    x.all_2018
    and x.dec_2018
group by 1,2,3
order by 2,1,3
;
select
    round(lob_avg) lob,
    count(x.member_id)   N,
    array_agg(distinct term_reason) term_reasons,
    --count(case when x._12_2019 then member_id end) N_12_19,
    --count(case when x._11_2019 then member_id end) N_11_19,
    (count(case when x._07_2019 then member_id end) * 100.0 / count(x.member_id))::decimal(16,2) pct_07,
    (count(case when x._08_2019 then member_id end) * 100.0 / count(x.member_id))::decimal(16,2) pct_08,
    (count(case when x._09_2019 then member_id end) * 100.0 / count(x.member_id))::decimal(16,2) pct_09,
    (count(case when x._10_2019 then member_id end) * 100.0 / count(x.member_id))::decimal(16,2) pct_10,
    (count(case when x._11_2019 then member_id end) * 100.0 / count(x.member_id))::decimal(16,2) pct_11,
    (count(case when x._12_2019 then member_id end) * 100.0 / count(x.member_id))::decimal(16,2) pct_12
from x
where
    x.all_2018
group by 1



with yrs as (
    select * from (
        values
            (2017, '12-31-2016'::date, '01-01-2018'::date ),
            (2018, '12-31-2017'::date, '01-01-2019'::date ),
            (2019, '12-31-2018'::date, '01-01-2020'::date ),
            (2020, '12-31-2019'::date, '01-01-2021'::date )
    )x(yr, boy1, eoy1)
)
select
    yrs.yr,
    count(distinct case when replace(replace(es.ggroup, 'a', ''),'b','')::int = 3 then member_id end) nd_grp3,
    count(distinct case when replace(replace(es.ggroup, 'a', ''),'b','')::int = 2 then member_id end) nd_grp2,
    count(distinct es.member_id) nd,
    count(distinct case when extract(year from m.date_of_death) = yrs.yr then es.member_id end) nd_eol,
    (count(distinct case when extract(year from m.date_of_death) = yrs.yr then es.member_id end) * 100.0 / count(distinct es.member_id))::decimal(16,2) pct_eol,
    (count(distinct case when replace(replace(es.ggroup, 'a', ''),'b','')::int = 3 and extract(year from m.date_of_death) = yrs.yr then es.member_id end) * 100.0 / nullif(count(distinct  case when replace(replace(es.ggroup, 'a', ''),'b','')::int = 3 then es.member_id end),0))::decimal(16,2) pct_eol_grp3,
    (count(distinct case when replace(replace(es.ggroup, 'a', ''),'b','')::int = 2 and extract(year from m.date_of_death) = yrs.yr then es.member_id end) * 100.0 / nullif(count(distinct  case when replace(replace(es.ggroup, 'a', ''),'b','')::int = 2 then es.member_id end),0))::decimal(16,2) pct_eol_grp2
from
    eligibility_segments es
    join members m on es.member_id = m.id and es.mco_id = es.mco_id
    join yrs on (boy1, eoy1) overlaps (es.start_date, es.end_date)
where
    es.mco_id = 2
    --and es.line_of_business_id = 1 /*medicaid*/
    and exists ( select 1 from eligibility_segments es2 where es2.member_id = es.member_id
        and (boy1, eoy1) overlaps (es2.start_date, es2.end_date)
        and replace(replace(es2.ggroup, 'a', ''),'b','')::int = 1 )
group by
    1


select
    replace(replace(ggroup, 'a', ''), 'b', '') grp,
    count(distinct member_id)
from eligibility_segments es
where
    es.mco_id = 2 and
    es.ggroup ~ '1' and
    member_id in
    (38, 221, 253, 323, 330, 359, 382, 421, 560, 614, 645, 762, 793, 795, 839, 901, 910, 950, 968, 1003, 1022, 1047, 1077, 1212, 1217, 1256, 1261, 1316, 1361, 1406, 1437, 1475, 1517, 1553, 1562, 1580, 1595, 1596, 1597, 1607, 1613, 1621, 1639, 1644, 1648, 1660, 1671, 1682, 1724, 1734, 1735, 1738, 1739, 1757, 1778, 1787, 1794, 1859, 1869, 1910, 1946, 2034, 2048, 2092, 2099, 2147, 2154, 2173, 2214,
     2225, 2246, 2253, 2257, 2272, 2276, 2279, 2294, 2360, 2383, 2391, 2403, 2437, 2469, 2515, 2522, 2544, 2584, 2609, 2661, 2762, 2766, 2892, 2948, 2953, 2963, 2978, 3227, 3245, 3363, 3409, 3417, 3433, 3445, 3470, 3518, 3537, 3547, 3548, 3585, 3594, 3608, 3624, 3626, 3635, 3640, 3643, 3662, 3692, 3694, 3708, 3711, 3722, 4327, 4596, 4620, 4751, 4762, 5046, 5115, 5140, 5232, 5235, 5613, 5642, 5658,
     5697, 5704, 5776, 5779, 5862, 5949, 6015, 6050, 6109, 6302, 6430, 6451, 6461, 6552, 6554, 6564, 6735, 6790, 6837, 6860, 6944, 6978, 6986, 7158, 7234, 7239, 7241, 7244, 7811, 7908, 7916, 8036, 8092, 8279, 8402, 8515, 8572, 8589, 8895, 8971, 9192, 9439, 9450, 9633, 9649, 9653, 9707, 9839, 9942, 9964, 10060, 10283, 10304, 10354, 11030, 11109, 11157, 11213, 11217, 11337, 11382, 11710, 11847,
     11913, 11967, 11993, 12055, 12088, 12233, 12277, 12282, 12382, 12391, 12397, 12402, 12562, 12563, 12587, 12594, 12599, 12623, 12660, 12669, 12676, 12701, 12721, 12735, 12736, 12741, 12749, 12900, 13058, 13209, 13220, 13531, 13550, 13670, 13773, 13879, 13899, 13902, 13913, 13954, 13982, 14114, 14130, 14298, 14370, 14409, 14486, 14521, 14637, 14650, 14875, 14912, 14938, 15283, 15298, 15309,
     15448, 15522, 15767, 15915, 15977, 16021, 16313, 16671, 16723, 16782, 16831, 16983, 17215, 17243, 17576, 17751, 17835, 17839, 17948, 18273, 18325, 18677, 18698, 18723, 19019, 19123, 19437, 19945, 19959, 20012, 20019, 20120, 20223, 20361, 20432, 20684, 20913)
group by
    1