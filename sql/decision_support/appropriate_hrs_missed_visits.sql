select
    extract(year from service_date) yr,
    reason_code_title,
    count(1)
from
    cb.appointments_visits zz
where
    extract(year from service_date) in (2018, 2019, 2020)
    and zz.mco_id = 1
    and zz.visit_status in ('missed') --, 'late')
    and zz.reason_code_type = 'provider initiated'
    --and zz.reason_code_title = 'member in a nursing facility'
    and zz.procedure_code_id not in (
        26547, -- s5150 respit
        26553, -- s5170 meal
        26650, -- s9125 respit
        27123 -- t2025 Waiver service, nos
    )
group by 1,2
order by 1,2


select
    zz.mco_id,
    --extract(year from service_date) yr,
    zz.reason_code_type,
    --reason_code_title,
    zz.resolution_status,
    zz.visit_status,
    zz.check_in_time is not null and zz.check_out_time is not null _was_visited,
    avg(coalesce(extract(year from m.date_of_death),2020) - extract(year from m.date_of_birth)) avg_age,
    array_agg(distinct auth_procedure_code ) auth_proc_codes,
    count(1) N
from cb.appointments_visits zz
     join cb.members m on zz.member_id = m.id and zz.mco_id = m.mco_id
where
    extract(year from service_date) in (2018, 2019, 2020)
    and zz.mco_id = 1
    --and zz.visit_status = 'missed'
    and zz.visit_status  in ('missed')
    and zz.reason_code_type = 'provider initiated'
    and m.date_of_death is not null
    --and zz.reason_code_title = 'member in a nursing facility'
    and zz.procedure_code_id not in (
        26547, -- s5150 respit
        26553, -- s5170 meal
        26650, -- s9125 respit
        27123 -- t2025 Waiver service, nos
    )
group by
    1, 2, 3, 4, 5
order by
    1, 2, 3, 4, 5
;


select
    zz.service_date,
    zz.reason_code_type,
    zz.reason_code_title,
    zz.resolution_status,
    array_agg(distinct (c.date_from, c.date_to))  from_to,
    sum(case when c.service_type_id =  1 then c.paid_amount end) ip_tc,
    sum(case when c.service_type_id =  2 then c.paid_amount end) ed_tc,
    sum(case when c.service_type_id =  4 then c.paid_amount end) snf_tc,
    sum(case when c.service_type_id =  6 then c.paid_amount end) amb_tc,
    sum(case when c.service_type_id = 11 then c.paid_amount end) icf_tc
from cb.appointments_visits zz
     join eligibility_segments es on es.member_id = zz.member_id
                                     and es.mco_id = 1
                                     and zz.service_date between es.start_date and es.end_date
                                     and es.line_of_business_id = 1
     --left join cb.member_days md on md.member_id = zz.member_id and zz.service_date = md.date
     left join cb.claims c on c.member_id = zz.member_id
                              and c.mco_id = 1
                              and zz.service_date between c.date_from and date_to
                              and c.service_type_id in (1, 2, 4, 11, 6)
where
    extract(year from service_date) in (2018, 2019, 2020)
    and zz.mco_id = 1
    and zz.visit_status = 'missed'
    and zz.reason_code_title = 'member in a nursing facility'
    and zz.procedure_code_id not in (
        26547, -- s5150 respit
        26553, -- s5170 meal
        26650, -- s9125 respit
        27123 -- t2025 Waiver service, nos
    )
group by 1,2,3,4
;
