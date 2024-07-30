create temporary table duals as
select
    es.member_id, min(es.start_date) start_date, max(es.end_date) end_date
from
    cb.eligibility_segments es
where
    es.mco_id = 2
    and es.line_of_business_id = 2
group by 1;


select x.nd_months, count(distinct member_id) from
    (
        select
            d.member_id,
            count(distinct vmme.mmonth) nd_months
        from
            duals d
            join vw_member_month_eligibility vmme on d.member_id = vmme.member_id and vmme.mco_id = 2 and line_of_business_id between 2 and 3
        group by 1
    ) x
group by 1
order by 1


select
    count(distinct es.member_id) -- 4,118
from
    cb.eligibility_segments es
where
    es.mco_id = 2
    and es.line_of_business_id = 2


select
    x.nd_months > 5 _,
    count(1)
from
    (
        select
            d.member_id,
            count(distinct vmme.mmonth) nd_months
        from
            duals d
            join vw_member_month_eligibility vmme on d.member_id = vmme.member_id and vmme.mco_id = 2 and line_of_business_id between 2 and 3
        group by 1
    ) x
 group by 1

select
    y.mm > 11 and y.nd_months > 5,
    count(1) n,
    count(distinct member_id) nd
from (
    select
        vmme.member_id,
        x.nd_months,
        round_half_down1(avg(vmme.line_of_business_id)) lob,
        avg(replace(replace(vmme.ggroup,'a',''),'b','')::int) ggroup,
        count(vmme.mmonth) mm
    from
        (
            select
                d.member_id,
                count(distinct vmme.mmonth) nd_months
            from
                duals d
                join vw_member_month_eligibility vmme on d.member_id = vmme.member_id and vmme.mco_id = 2 and line_of_business_id between 2 and 3
            group by 1
        ) x
        join vw_member_month_eligibility vmme on vmme.member_id = x.member_id
        --where x.nd_months > 5
     group by 1,2
) y
group by 1


--- LIST DROP
create table uhc_mmr_request_2020_07_16 as
select
    upper(m.source_member_id) source_member_id, upper(m.mbi_id) mbi, d.start_date min_medicare_eligibility_start_date, d.end_date max_medicare_eligibility_emd_date, now() created_at
from (
    select
        vmme.member_id,
        x.nd_months,
        round_half_down1(avg(vmme.line_of_business_id)) lob,
        avg(replace(replace(vmme.ggroup,'a',''),'b','')::int) ggroup,
        count(vmme.mmonth) mm
    from
        (
            select
                d.member_id,
                count(distinct vmme.mmonth) nd_months
            from
                duals d
                join vw_member_month_eligibility vmme on d.member_id = vmme.member_id and vmme.mco_id = 2 and line_of_business_id between 2 and 3
            group by 1
        ) x
        join vw_member_month_eligibility vmme on vmme.member_id = x.member_id
        --where x.nd_months > 5
     group by 1,2
) y
join cb.members m on m.id = y.member_id and m.mco_id = 2
join duals d on m.id = d.member_id
where y.mm > 11 and y.nd_months > 5