select
    mco_id,
    extract(year from service_date) yr,
    visit_status,
    array_agg(distinct reason_code_type ) reason_code_types,
    array_agg(distinct reason_code_title) reason_code_title,
    count(1) n,
    count(case when reason_code_type = 'member initiated' then 1 end) N_mbr_init,
    count(case when reason_code_type = 'provider initiated' then 1 end) N_prv_init,
    count(case when reason_code_type = 'severe inclement weather or natural disaster' then 1 end) N_wthr_init,
    count(case when reason_code_type = 'technical issue' then 1 end) N_tech_init,
    count(case when reason_code_type is not null then 1 end) N_reason_code_type,
    count(case when reason_code_title is not null then 1 end) N_reason_code_title,
    count(case when zz.check_in_time is not null and zz.check_out_time is not null then 1 end) n_was_visited,
    count(1) - count(case when zz.check_in_time is not null and zz.check_out_time is not null then 1 end) n_delta
from
    cb.appointments_visits zz
where
    extract(year from service_date) in (2018, 2019, 2020)
    and zz.mco_id = 1
    and zz.visit_status in ('missed') --, 'late')
    --and zz.reason_code_title = 'member in a nursing facility'
    and zz.procedure_code_id not in (
        26547, -- s5150 respit
        26553, -- s5170 meal
        26650, -- s9125 respit
        27123 -- t2025 Waiver service, nos
    )
group by 1,2,3


with x as (
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
        es.mco_id = 1
        -- and es.line_of_business_id = 1 /*medicaid*/
        and replace(replace(es.ggroup, 'a', ''),'b','')::int = 2
    group by 1
)
select
    round(lob_avg) lob,
    count(x.member_id)                             N,
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

