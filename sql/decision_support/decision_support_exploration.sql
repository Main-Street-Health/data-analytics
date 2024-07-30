refresh materialized view cb.vw_hcbs_pcs_units_mm;
refresh materialized view cb.vw_hcbs_pcs_auths;
refresh materialized view cb.vw_hcbs_pcs_visit_hrs_mm;

-- random little things --
    select count(distinct member_id) from cb.hcbs_auths ha where '02-01-2020' between ha.start_date and ha.end_date

--------------------------

select
    a.bom,
    count(a.payer_member_id) N,
    count(distinct a.payer_member_id) ND,
    min(v.bom) min_month,
    max(v.bom) max_month,
    sum(a.hrs) auth_hrs,
    sum(v.hrs) visit_hrs,
    count(distinct case when a.hrs > v.hrs then a.payer_member_id end) ND_more_auth,
    count(distinct case when a.hrs = v.hrs then a.payer_member_id end) ND_eqll_auth,
    count(distinct case when a.hrs < v.hrs then a.payer_member_id end) ND_less_auth,
    sum(a.hrs) > sum(v.hrs) more_auth,
    sum(a.hrs) = sum(v.hrs) eql__auth,
    sum(a.hrs) < sum(v.hrs) less_auth
from cb.vw_hcbs_pcs_auth_units_mm a
    left join cb.vw_hcbs_pcs_visit_hrs_mm v on a.mco_id = v.mco_id
                                                   and a.payer_member_id = v.payer_member_id
                                                   and a.bom = v.bom
where
    a.bom >= '2017-07-01'
    and a.eom <= '2020-06-01'
group by 1;


with x as (
    select
        a.bom,
        a.payer_member_id,
        a.member_id,
        a.bom,
        a.eom,
        a.bom - interval '1 day' bom_md,
        a.eom + interval '1 day' eom_pd,
        a.hrs                                              auth_hrs,
        v.hrs                                              visit_hrs,
        (v.hrs - a.hrs)                                    hrs_over_auth,
        case when a.hrs > v.hrs then a.payer_member_id end _more_auth,
        case when a.hrs = v.hrs then a.payer_member_id end _eqll_auth,
        case when a.hrs < v.hrs then a.payer_member_id end _less_auth
    from
        cb.vw_hcbs_pcs_auth_units_mm a
        left join cb.vw_hcbs_pcs_visit_hrs_mm v on a.mco_id = v.mco_id
            and a.payer_member_id = v.payer_member_id
            and a.bom = v.bom
    where
          a.bom >= '2017-07-01'
      and a.eom <= '2020-06-01'
      and (v.hrs - a.hrs) > 13
          --and a.payer_member_id::bigint = 712276435
    order by
        hrs_over_auth desc
)
select
    ha.start_date,
    ha.end_date,
    ha.procedure_code,
    ha.member_id,
    ha.units / 4,
    ha.units_daily,
    ha.units,
    x.visit_hrs,
    x.auth_hrs
from hcbs_auths ha
    join x x on ha.payer_member_id::bigint = x.payer_member_id::bigint
where
    ha.payer_member_id::bigint = 722094554
    and is_hcbs_pcs
    and (ha.start_date, ha.end_date) overlaps (x.bom_md, x.eom_pd )
;

 --   and av.service_date between '2018-08-01'::date and '2018-08-31'::date






-- v > a ---
select vw.v_a_cat, count(*) from vw_tmp_av_util vw where coalesce(vw.v_hrs,0) > 0 group by 1;



select
    sum(units),
    sum(units) / 4 hrs
from hcbs_auths ha
where
    ha.member_id = 21010
    and is_hcbs_pcs


-- 3,227
-- 1939.75
-- 4623.25
select
    sum(cb.round_to_quarter((date_part('epoch'::text, (av.check_out_time - av.check_in_time)) * 1.0 / 3600.0)::numeric)) AS hrs
from cb.appointments_visits av
where
    av.payer_member_id::bigint = 722787158
    and av.check_in_time is not null
    and av.check_out_time is not null
    and (av.auth_procedure_code = ANY (ARRAY ['s5125'::text, 't1019'::text]))
    and av.check_in_time::date between '2019-01-01'::date and '2020-03-19'::date


select
    sum(v.hrs) hrs -- 1,309
from cb.vw_hcbs_pcs_units_mm2 v
where
    v.bom between '2019-01-01'::date and '2019-12-31'::date
--    and v.payer_member_id::bigint = 722787158
    and v.member_id = 21010
;




select
    --start_date,
    --end_date,
    sum(units/4) hrs -- 1,827
    --*
from hcbs_auths ha
where
    ha.member_id = 21010
    and ha.mco_id = 1
    AND is_hcbs_pcs
    AND ha.start_date <> ha.end_date
    AND (ha.start_date, ha.end_date) overlaps ('2019-01-01'::date, '2019-12-31'::date)
    AND ha.units_daily <= ha.units
-- order by 1, 2


select
    start_date,
    end_date,
    units/4 hrs,
    ha.units_daily,
    ha.units,
    ha.auth_days,
    date_part('day'::text, ha.end_date::timestamp without time zone - ha.start_date::timestamp without time zone) days,
    (ha.units::double precision / date_part('day'::text, ha.end_date::timestamp without time zone - ha.start_date::timestamp without time zone))::int calc_daily,
    (ha.units::double precision / date_part('day'::text, ha.end_date::timestamp without time zone - ha.start_date::timestamp without time zone)) > ha.units_daily::double precision,
    *
from hcbs_auths ha
where
    ha.member_id = 21010
    AND is_hcbs_pcs
    AND ha.start_date <> ha.end_date
    AND (ha.start_date, ha.end_date) overlaps ('2019-01-01'::date, '2019-12-31'::date)
    AND ha.units_daily <= ha.units
 order by 1, 2


SELECT
    --x.mco_id,
    --x.month,
    --x.bom,
    --x.eom,
    --x.payer_member_id,
    --x.evv_member_id,
    --x.member_id,
    sum(x.units/4) hrs,
    sum(x.units2/4) hrs2
    --x.units / 4 AS hrs
FROM
    (SELECT
        ha.mco_id,
        d.month,
        d.bom,
        d.eom,
        ha.payer_member_id,
        ha.evv_member_id,
        ha.member_id,
        sum(
            CASE
                WHEN (ha.units::double precision / date_part('day'::text, ha.end_date::timestamp without time zone - ha.start_date::timestamp without time zone)) > ha.units_daily::double precision
                    THEN ha.units_daily::double precision
                ELSE ha.units::double precision / date_part('day'::text, ha.end_date::timestamp without time zone - ha.start_date::timestamp without time zone)
            END
        )::integer AS units,
        sum(ha.units::double precision / date_part('day'::text, ha.end_date::timestamp without time zone - ha.start_date::timestamp without time zone)) units2
     FROM
         cb.hcbs_auths ha
         JOIN ref.dates d ON d.day >= ha.start_date AND d.day <= ha.end_date
                             --AND ha.auth_days ~ "left"(d.day_text, 3)
                             and d.bom between '2019-01-01'::date and '2019-12-31'::date
     WHERE
         ha.is_hcbs_pcs
         AND ha.payer_member_id::bigint <> 715724436
         AND ha.start_date <> ha.end_date
         AND ha.units_daily <= ha.units
        ----
        and ha.member_id = 21010
     GROUP BY d.month, d.bom, d.eom, ha.payer_member_id, ha.evv_member_id, ha.member_id, ha.mco_id) x;




SELECT
    --v.auth_procedure_code,
    --v.*,
    --date_part('epoch'::text, v.check_out_time - v.check_in_time) / 3600::double precision AS hrs
    count(v.source_appointment_id) N,
    count(distinct v.source_appointment_id) N
FROM
    cb.appointments_visits v
WHERE
    (v.auth_procedure_code = ANY (ARRAY ['s5125'::text, 't1019'::text]))
    AND v.check_in_time IS NOT NULL
    AND v.check_out_time IS NOT NULL
    AND v.payer_member_id::bigint   = 722787158


select * from cb.hcbs_auths ha where ha.payer_member_id = '724450989' and ha.is_hcbs_pcs
select * from cb.appointments_visits ha where ha.payer_member_id = '724450989'

select coalesce(trim(ha.auth_days),'') = '' and units_daily = 0, count(1), sum(units) units from cb.hcbs_auths ha group by 1

select extract(year from av.service_date) yr, av.reason_code_type, av.resolution_status, count(1) from appointments_visits av where av.check_in_time is null and av.service_date between '2018-01-01' and '2019-12-31'
group by 1, 2, 3
order by 1, 2, 3;

select distinct resolution_status from cb.appointments_visits av
select
   extract( year from service_date) yr,  (count( case when check_in_time is null then 1 end )  * 100.0 / count(1))::decimal(16,2)
from cb.appointments_visits av where av.service_date between '2017-01-01' and '2020-06-30'
and ( check_in_time is not null or coalesce(resolution_status,'') not in ('visit was made-up by paid staff', 'visit was made-up by unpaid support') )
group by 1

select 'uhc' _, count(1) from raw.uhc_appointment_visits_2020_07_13 x
union all
select 'agp' _, count(1) from raw.agp_healthstar_appointment_visits_with_endtime_2020_07_13 x;
select count(1) from raw.uhc_authorizations_2020_07_13 u;

drop table raw.uhc_appointment_visits_2020_07_13;

with visit_year as (
    select
        extract(year from v.bom) yr,
        v.payer_member_id,
        sum(v.hrs) hrs
    from
        vw_hcbs_pcs_visit_hrs_mm v
    group by
        1, 2
), auth_year as (
    select
        extract(year from a.bom) yr,
        a.member_id,
        a.payer_member_id,
        sum(a.hrs) hrs
    from
        vw_hcbs_pcs_auth_units_mm a
    group by
        1, 2, 3
),
ggroup as (
    select vmme.yearr, vmme.member_id, round_half_down1(avg(ggroup::int)) ggroup from vw_member_month_eligibility vmme where vmme.mco_id = 1 group by 1, 2
)
select
    gp.ggroup,
    ay.yr,
    --ay.payer_member_id,
    --ay.hrs auth_hrs,
    --vy.hrs visit_hrs,
    round(ay.hrs / 250,0) * 250 traunch,
    count(ay.payer_member_id) N,
    count(distinct ay.payer_member_id) ND,
    avg(coalesce((vy.hrs * 100.0 / ay.hrs)::decimal(16,2),0))::decimal(16,1) avg_auth_util,
    --min(coalesce((vy.hrs * 100.0 / ay.hrs)::decimal(16,2),0))::decimal(16,1) min_auth_util,
    max(coalesce((vy.hrs * 100.0 / ay.hrs)::decimal(16,2),0))::decimal(16,1) max_auth_util
from
    auth_year ay
    join ggroup gp on gp.member_id = ay.member_id and gp.yearr = ay.yr
    left join visit_year vy on ay.payer_member_id = vy.payer_member_id and ay.yr = vy.yr
where
    ay.yr between 2018 and 2019
group by 1, 2, 3
order by 1, 2, 3
    --and ay.hrs between 500 and 800
--order by 4 desc

;