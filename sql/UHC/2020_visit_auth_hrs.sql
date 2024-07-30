drop table _ff;
create temp table _ff as
select
    mco_id,
    extract(year from av.service_date) yr,
    extract(month from av.service_date) mnth,
    (_eom(av.service_date) - _bom(av.service_date) + 1) ddays,
    min(av.service_date) service_date_min,
    max(av.service_date) service_date_max,
    (sum(extract(epoch from av.check_out_time - av.check_in_time))/3600)::decimal(16,2) hrs,
    count(av.evv_member_id) N,
    count(distinct av.evv_member_id) ND
from
    appointments_visits av
where
    av.check_in_time is not null and av.check_out_time is not null
group by 1,2,3,4
having count(1) > 1
order by 1,2,3,4


select
    f.mco_id,
    yr,
    mnth,
    ddays,
    service_date_min,
    service_date_max,
    hrs,
    N,
    ND,
   (N   / f.ddays)::int norm_days,
   (hrs / f.ddays)::int norm_hrs
from
    _ff f
where
    f.yr in (2019,2020)

