-- AUTH MM
--create materialized view if not exists cb.vw_ds_auth_mm as
drop table if exists _auth_real_daily;
create temporary table _auth_real_daily AS
SELECT
    ha.id,
    ha.mco_id,
    ha.start_date,
    ha.end_date,
    ha.payer_member_id,
    ha.evv_member_id,
    ha.member_id,
    ha.procedure_code,
    ha.auth_days,
    ha.units,
    ha.units_daily,
    count(d.day)                                                     n_days,
    (CASE WHEN ha.units_daily > 0 AND (ha.units * 1.0 / count(d.day)) > ha.units_daily
              THEN ha.units_daily
          ELSE ha.units * 1.0 / count(d.day) END)::numeric(16, 2) AS real_units_daily
FROM
    cb.hcbs_auths ha
    JOIN ref.dates d ON d.day BETWEEN ha.start_date AND ha.end_date
        AND (ha.auth_days is null or ha.auth_days ~ "left"(d.day_text, 3))
WHERE
    (
            ha.start_date != ha.end_date
            OR ha.units_daily = ha.units
            OR (ha.units * 1.0 / (ha.units_daily + 0.0001)) < 1.5
            OR (ha.units <= 96 AND ha.units_daily = 0)
        ) AND
    COALESCE(ha.units_daily, 0) <= ha.units AND
    ha.units > 1
group by
    ha.id, ha.mco_id, ha.start_date, ha.end_date, ha.payer_member_id, ha.evv_member_id,
    ha.member_id, ha.procedure_code, ha.auth_days, ha.units, ha, units_daily;



insert into
    ds_auths_mm(ds_batch_id, mco_id, bom, eom, payer_member_id, evv_member_id, member_id, hcbs_auth_ids, auths_n, attd_auths_n, meal_auths_n, pcs_auths_n, resp_auths_n, units, hrs, attd_pcs_units, attd_pcs_hrs, attd_units, attd_hrs, meal_units, meal_hrs, pcs_units, pcs_hrs, resp_units, resp_hrs)
select
    1,
    x.mco_id,
    x.bom,
    x.eom,
    x.payer_member_id,
    x.evv_member_id,
    x.member_id,
    array_agg(x.id)                                                         as hcbs_auth_ids,
    COALESCE(count(x.id), 0)                                                as auths_n,
    COALESCE(sum(x.attd_auths), 0)                                          as attd_auths_n,
    COALESCE(sum(x.meal_auths), 0)                                          as meal_auths_n,
    COALESCE(sum(x.pcs__auths), 0)                                          as pcs_auths_n,
    COALESCE(sum(x.resp_auths), 0)                                          as resp_auths_n,
    COALESCE(sum(x.units), 0)                                               as units,
    COALESCE(sum(x.units), 0) / 4.0                                         as hrs,
    COALESCE(sum(x.attd_units), 0) + COALESCE(sum(x.pcs__units), 0)         as attd_pcs_units,
    (COALESCE(sum(x.attd_units), 0) + COALESCE(sum(x.pcs__units), 0)) / 4.0 as attd_pcs_hrs,
    COALESCE(sum(x.attd_units), 0)                                          as attd_units,
    COALESCE(sum(x.attd_units), 0) / 4.0                                    as attd_hrs,
    COALESCE(sum(x.meal_units), 0)                                          as meal_units,
    COALESCE(sum(x.meal_units), 0) / 4.0                                    as meal_hrs,
    COALESCE(sum(x.pcs__units), 0)                                          as pcs_units,
    COALESCE(sum(x.pcs__units), 0) / 4.0                                    as pcs_hrs,
    COALESCE(sum(x.resp_units), 0)                                          as resp_units,
    COALESCE(sum(x.resp_units), 0) / 4.0                                    as resp_hrs
from
    (select
         ha.id,
         ha.mco_id,
         ha.payer_member_id,
         ha.evv_member_id,
         ha.member_id,
         ha.procedure_code,
         ha.real_units_daily,
         d.bom,
         d.eom,
         (real_units_daily * count(d.day))                                                         as units,
         case when procedure_code = 's5125' then 1 end                                             as attd_auths,
         case when procedure_code = 's5125' then (real_units_daily * count(d.day)) end             as attd_units,
         case when procedure_code = 's5170' then 1 end                                             as meal_auths,
         case when procedure_code = 's5170' then (real_units_daily * count(d.day)) end             as meal_units,
         case when procedure_code = 't1019' then 1 end                                             as pcs__auths,
         case when procedure_code = 't1019' then (real_units_daily * count(d.day)) end             as pcs__units,
         case when procedure_code in ('s9125', 's5150') then 1 end                                 as resp_auths,
         case when procedure_code in ('s9125', 's5150') then (real_units_daily * count(d.day)) end as resp_units
     from
         _auth_real_daily ha
         join ref.dates d on d.day between ha.start_date and ha.end_date and
                             (ha.auth_days is null or ha.auth_days ~ "left"(d.day_text, 3))
     where ha.real_units_daily <= 96
     group by
         ha.id, ha.mco_id, ha.start_date, ha.end_date, ha.payer_member_id, ha.evv_member_id, ha.member_id,
         ha.procedure_code, ha.auth_days, ha.real_units_daily, d.bom, d.eom
    ) x
group by
    x.mco_id, x.bom, x.eom, x.payer_member_id, x.evv_member_id, x.member_id;

drop table if exists _foo;
create temp table _foo as
select
    dam.member_id,
    (sum(case when dvm.bom = '2019-01-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-01-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_01,
    (sum(case when dvm.bom = '2019-02-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-02-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_02,
    (sum(case when dvm.bom = '2019-03-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-03-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_03,
    (sum(case when dvm.bom = '2019-04-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-04-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_04,
    (sum(case when dvm.bom = '2019-05-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-05-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_05,
    (sum(case when dvm.bom = '2019-06-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-06-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_06,
    (sum(case when dvm.bom = '2019-07-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-07-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_07,
    (sum(case when dvm.bom = '2019-08-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-08-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_08,
    (sum(case when dvm.bom = '2019-09-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-09-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_09,
    (sum(case when dvm.bom = '2019-10-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-10-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_10,
    (sum(case when dvm.bom = '2019-11-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-11-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_11,
    (sum(case when dvm.bom = '2019-12-01' then dvm.attd_pcs_visit_hrs end) / nullif(sum(case when dam.bom = '2019-12-01' then dam.attd_pcs_hrs end), 0))::decimal(16, 2) hrs_12,
    (sum(dvm.attd_pcs_visit_hrs) / nullif(sum(dam.attd_pcs_hrs), 0))::decimal(16, 2)                                                                                     util,
    sum(dvm.attd_pcs_visit_hrs)                                                                                                                                          hrs,
    sum(dam.attd_pcs_hrs)                                                                                                                                                ath_hrs,
    max(case when dam.bom = '2019-01-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_01,
    max(case when dam.bom = '2019-02-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_02,
    max(case when dam.bom = '2019-03-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_03,
    max(case when dam.bom = '2019-04-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_04,
    max(case when dam.bom = '2019-05-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_05,
    max(case when dam.bom = '2019-06-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_06,
    max(case when dam.bom = '2019-07-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_07,
    max(case when dam.bom = '2019-08-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_08,
    max(case when dam.bom = '2019-09-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_09,
    max(case when dam.bom = '2019-10-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_10,
    max(case when dam.bom = '2019-11-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_11,
    max(case when dam.bom = '2019-12-01' and cmm.member_id is not null
                 then (case when vmme.ggroup is not null then vmme.ggroup::text else '9' end || case when cmm.ipat_$ > 0 then '1' else '0' end || case when cmm.snfa_$ > 0 then 1 else 0 end || case when cmm.icfs_$ > 0 then '1' else '0' end)::int
             else null end)                                                                                                                                              has_fac_12,
    max(case when cmm.member_id is not null then 1 else 0 end)                                                                                                           _has_fac,
    array_agg(vmme.line_of_business_id)                                                                                                                                  lob_agg,
    array_agg(vmme.line_of_business_id)                                                                                                                                  grp_agg,
    max(m.date_of_death)                                                                                                                                                 date_of_death
from
    ds_auths_mm dam
    join      members m on dam.member_id = m.id
    join      vw_member_month_eligibility vmme on dam.member_id = vmme.member_id and vmme.mmonth = dam.bom and vmme.mco_id = dam.mco_id
    left join ds_visits_mm dvm on dvm.member_id = vmme.member_id and dvm.mco_id = vmme.mco_id and dvm.bom = vmme.mmonth
    left join vw_claim_member_months cmm on dam.mco_id = cmm.mco_id and dam.bom = cmm.bom
        and dam.member_id = cmm.member_id
        and
                                            (
                                                cmm.ipat_$ > 0 or cmm.icfs_$ > 0 or cmm.snfa_$ > 0
                                                )
where
      dam.mco_id = 1
  and dam.bom between '2019-01-01'::date and '2019-12-01'
group by
    1
;

select *
from
    (
        select
            f.member_id,
            '0. hrs' hrs,
            f.hrs_01 m_01,
            f.hrs_02 m_02,
            f.hrs_03 m_03,
            f.hrs_04 m_04,
            f.hrs_05 m_05,
            f.hrs_06 m_06,
            f.hrs_07 m_07,
            f.hrs_08 m_08,
            f.hrs_09 m_09,
            f.hrs_10 m_10,
            f.hrs_11 m_11,
            f.hrs_12 m_12,
            f.ath_hrs,
            f.hrs,
            f.util,
            lob_agg,
            grp_agg,
            date_of_death
        from _foo f
        where f._has_fac = 1
        union all
        select
            f.member_id,
            '1. fac' hrs,
            f.has_fac_01,
            f.has_fac_02,
            f.has_fac_03,
            f.has_fac_04,
            f.has_fac_05,
            f.has_fac_06,
            f.has_fac_07,
            f.has_fac_08,
            f.has_fac_09,
            f.has_fac_10,
            f.has_fac_11,
            f.has_fac_12,
            f.ath_hrs,
            f.hrs,
            f.util,
            lob_agg,
            grp_agg,
            date_of_death
        from _foo f
        where f._has_fac = 1
    ) x
order by
    1, 2

select
    _has_fac,
    avg(f.ath_hrs)::decimal(16, 2) ath_hrs,
    avg(f.hrs)::decimal(16, 2)     hrs_avg,
    avg(f.util)::decimal(16, 2)    util_avg
from _foo f
group by
    1


WITH
    proc_codes AS (
        SELECT
            x.typee,
            x.proc_codes
        FROM
            (
                VALUES
                    ('all'::text, NULL::text[]),
                    ('attd'::text, ARRAY ['s5125'::text]),
                    ('pcs'::text, ARRAY ['t1019'::text]),
                    ('meal'::text, ARRAY ['s5170'::text]),
                    ('respite'::text, ARRAY ['s9125'::text, 's5150'::text])
            ) x(typee, proc_codes)
    ),
    types AS (
        SELECT
            v.mco_id,
            d.bom,
            d.eom,
            v.payer_member_id,
            v.evv_member_id,
            v.member_id,
            pc.typee,
            COALESCE(sum(v.hrs) FILTER (WHERE v.is_visit), 0)::numeric                                                                                                         AS visit_hrs,
            COALESCE(sum(v.hrs) FILTER (WHERE v.is_visit AND v.is_night), 0)::numeric                                                                                          AS visit_hrs_night,
            COALESCE(count(1) FILTER (WHERE v.is_visit), 0)::bigint                                                                                                            AS visit_n,
            COALESCE(count(1) FILTER (WHERE v.is_visit AND v.is_night), 0)::bigint                                                                                             AS visit_n_night,
            COALESCE(avg(v.hrs) FILTER (WHERE v.is_visit), 0)::numeric                                                                                                         AS visit_avg_hrs,
            COALESCE(avg(v.hrs) FILTER (WHERE v.is_visit AND v.is_night), 0)::numeric                                                                                          AS visit_avg_hrs_night,
            COALESCE(count(DISTINCT v.evv_caregiver_id) FILTER (WHERE v.is_visit), 0)::bigint                                                                                  AS visit_cg_nd,
            COALESCE(count(1) FILTER (WHERE v.is_missed), 0)::bigint                                                                                                           AS missed_n,
            COALESCE(sum(v.hrs_missed) FILTER (WHERE v.is_missed), 0)::numeric                                                                                                 AS missed_hrs,
            COALESCE(count(1) FILTER (WHERE v.is_missed AND v.reason_code_type = 'member initiated'), 0)::bigint                                                               AS missed_memb_init_n,
            COALESCE(sum(v.hrs_missed) FILTER (WHERE v.is_missed AND v.reason_code_type = 'member initiated'), 0)::numeric                                                     AS missed_memb_init_hrs,
            COALESCE(sum(v.hrs_missed) FILTER (WHERE v.is_missed AND v.reason_code_type = 'member initiated' AND (v.reason_code_title = ANY (ARRAY ['member in a nursing facility', 'member in a psychiatric facility', 'member in the hospital (not psychiatric facility)', 'member sick/at medical appointment']))
                AND v.resolution_status = 'visit was not made-up'), 0)::numeric                                                                                                AS missed_memb_init_needed_hrs,
            COALESCE(count(1) FILTER (WHERE v.is_missed AND v.reason_code_type = 'provider initiated'), 0)::bigint                                                             AS missed_prov_init_n,
            COALESCE(sum(v.hrs_missed) FILTER (WHERE v.is_missed AND v.reason_code_type = 'provider initiated'), 0)::numeric                                                   AS missed_prov_init_hrs,
            COALESCE(sum(v.hrs_missed) FILTER (WHERE v.is_missed AND v.reason_code_type = 'provider initiated' AND v.resolution_status = 'visit was not made-up'), 0)::numeric AS missed_prov_init_needed_hrs,
            COALESCE(sum(v.hrs_missed) FILTER (WHERE v.is_missed AND v.is_night_missed), 0)::numeric                                                                           AS missed_night_hrs,
            COALESCE(count(1) FILTER (WHERE v.is_missed AND v.is_night_missed), 0)::bigint                                                                                     AS missed_night_n
        FROM
            (SELECT
                 x.*,
                 cb.round_to_quarter((date_part('epoch'::text, x.check_out_cst - x.check_in_cst) * 1.0 / 3600.0)::numeric)  AS hrs,
                 (x.check_in_cst, x.check_out_cst) overlaps (x.night_start, x.night_end)                                    AS is_night,
                 cb.round_to_quarter((date_part('epoch'::text, x.end_time_cst - x.start_time_cst) * 1.0 / 3600.0)::numeric) AS hrs_missed,
                 (x.start_time_cst, x.end_time_cst) overlaps (x.night_start, x.night_end)                                   AS is_night_missed
             FROM
                 (SELECT
                      zz.*,
                      ('2020-01-01 '::text || cb._utc_2_cst(zz.check_in_time)::time::text)::timestamp              AS check_in_cst,
                      -- fix the check-in / out date thing
                      CASE WHEN cb._utc_2_cst(zz.check_out_time)::time >= cb._utc_2_cst(zz.check_in_time)::time
                               THEN ('2020-01-01 '::text || cb._utc_2_cst(zz.check_out_time)::time::text)::timestamp
                           ELSE ('2020-01-02 '::text || cb._utc_2_cst(zz.check_out_time)::time::text)::timestamp
                          END                                                                                      AS check_out_cst,
                      ('2020-01-01 '::text || cb._utc_2_cst(zz.start_time)::time::text)::timestamp                 AS start_time_cst,
                      CASE
                          WHEN cb._utc_2_cst(zz.end_time)::time >= cb._utc_2_cst(zz.start_time)::time
                              THEN ('2020-01-01 '::text || cb._utc_2_cst(zz.end_time)::time::text)::timestamp
                          ELSE ('2020-01-02 '::text || cb._utc_2_cst(zz.end_time)::time::text)::timestamp
                          END                                                                                      AS end_time_cst,
                      '2020-01-01 22:59:59'::timestamp                                                             AS night_start,
                      '2020-01-02 04:00:01'::timestamp                                                             AS night_end,
                      zz.check_in_time IS NOT NULL AND zz.check_out_time IS NOT NULL                               AS is_visit,
                      zz.visit_status = 'missed'::text AND (zz.check_in_time IS NULL OR zz.check_out_time IS NULL) AS is_missed
                  FROM cb.appointments_visits zz
                  WHERE zz.visit_status = 'missed'::text OR zz.check_in_time IS NOT NULL AND zz.check_out_time IS NOT NULL
                 ) x
             WHERE
                 (x.auth_procedure_code <> ANY (ARRAY ['s9125'::text, 's5150'::text]) OR x.is_visit) AND
                 (x.auth_procedure_code = ANY (ARRAY ['s5125'::text, 't1019'::text, 's5170'::text, 's9125'::text, 's5150'::text]))
            ) v
            CROSS JOIN proc_codes pc
            JOIN       ref.dates d ON d.day = v.service_date
        WHERE
            (pc.proc_codes IS NULL OR (v.auth_procedure_code IN (SELECT unnest(pc.proc_codes) AS unnest)))
        GROUP BY
            v.mco_id, d.bom, d.eom, v.payer_member_id, v.evv_member_id, v.member_id, pc.typee
    ),
    types2 AS (
        SELECT
            types.*,
            COALESCE(types.missed_prov_init_needed_hrs, 0)::numeric + COALESCE(types.missed_memb_init_needed_hrs, 0)::numeric + COALESCE(types.visit_hrs, 0)::numeric AS appropriate_hours
        FROM types
    ),
    final_visits AS (
        SELECT
            alll.mco_id,
            alll.bom,
            alll.eom,
            alll.payer_member_id,
            alll.evv_member_id,
            alll.member_id,
            COALESCE(alll.visit_hrs, 0)::numeric                                                       visit_hrs,
            (COALESCE(attd.visit_hrs, 0) + COALESCE(pcss.visit_hrs, 0))::numeric                       attd_pcs_visit_hrs,
            COALESCE(attd.visit_hrs, 0)::numeric                                                       attd_visit_hrs,
            COALESCE(pcss.visit_hrs, 0)::numeric                                                       pcs_visit_hrs,
            COALESCE(resp.visit_hrs, 0)::numeric                                                       resp_visit_hrs,
            COALESCE(meal.visit_hrs, 0)::numeric                                                       meal_visit_hrs,
            COALESCE(alll.visit_hrs_night, 0)::numeric                                                 night_visit_hrs,
            (COALESCE(attd.visit_hrs_night, 0) + COALESCE(pcss.visit_hrs_night, 0))::numeric           attd_pcs_night_visit_hrs,
            COALESCE(attd.visit_hrs_night, 0)::numeric                                                 attd_night_visit_hrs,
            COALESCE(pcss.visit_hrs_night, 0)::numeric                                                 pcs_night_visit_hrs,
            COALESCE(meal.visit_hrs_night, 0)::numeric                                                 meal_night_visit_hrs,
            COALESCE(resp.visit_hrs_night, 0)::numeric                                                 resp_night_visit_hrs,
            COALESCE(alll.visit_n, 0)::bigint                                                          visit_n,
            (COALESCE(attd.visit_n, 0) + COALESCE(pcss.visit_n, 0))::bigint                            attd_pcs_visit_n,
            COALESCE(attd.visit_n, 0)::bigint                                                          attd_visit_n,
            COALESCE(pcss.visit_n, 0)::bigint                                                          pcs_visit_n,
            COALESCE(resp.visit_n, 0)::bigint                                                          resp_visit_n,
            COALESCE(meal.visit_n, 0)::bigint                                                          meal_visit_n,
            COALESCE(alll.visit_n_night, 0)::bigint                                                    night_visit_n,
            (COALESCE(attd.visit_n_night, 0) + COALESCE(pcss.visit_n_night, 0))::bigint                attd_pcs_night_visit_n,
            COALESCE(attd.visit_n_night, 0)::bigint                                                    attd_night_visit_n,
            COALESCE(pcss.visit_n_night, 0)::bigint                                                    pcs_night_visit_n,
            COALESCE(resp.visit_n_night, 0)::bigint                                                    resp_night_visit_n,
            COALESCE(meal.visit_n_night, 0)::bigint                                                    meal_night_visit_n,
            COALESCE(alll.visit_avg_hrs, 0)::numeric                                                   avg_visit_hrs,
            COALESCE(alll.visit_avg_hrs_night, 0)::numeric                                             avg_night_visit_hrs,
            COALESCE(attd.visit_avg_hrs_night, 0)::numeric                                             attd_avg_night_visit_hrs,
            COALESCE(pcss.visit_avg_hrs_night, 0)::numeric                                             pcs_avg_night_visit_hrs,
            COALESCE(resp.visit_avg_hrs_night, 0)::numeric                                             resp_avg_night_visit_hrs,
            COALESCE(meal.visit_avg_hrs_night, 0)::numeric                                             meal_avg_night_visit_hrs,
            COALESCE(alll.visit_cg_nd, 0)::bigint                                                      cg_visit_nd,
            (COALESCE(attd.visit_cg_nd, 0) + COALESCE(pcss.visit_cg_nd, 0))::bigint                    attd_pcs_cg_visit_nd,
            COALESCE(attd.visit_cg_nd, 0)::bigint                                                      attd_cg_visit_nd,
            COALESCE(pcss.visit_cg_nd, 0)::bigint                                                      pcs_cg_visit_nd,
            COALESCE(meal.visit_cg_nd, 0)::bigint                                                      meal_cg_visit_nd,
            COALESCE(resp.visit_cg_nd, 0)::bigint                                                      resp_cg_visit_nd,
            COALESCE(alll.missed_n, 0)::bigint                                                         missed_n,
            (COALESCE(attd.missed_n, 0) + COALESCE(pcss.missed_n, 0))::bigint                          attd_pcs_missed_n,
            COALESCE(attd.missed_n, 0)::bigint                                                         attd_missed_n,
            COALESCE(pcss.missed_n, 0)::bigint                                                         pcs_missed_n,
            COALESCE(meal.missed_n, 0)::bigint                                                         meal_missed_n,
            COALESCE(alll.missed_hrs, 0)::numeric                                                      missed_hrs,
            (COALESCE(attd.missed_hrs, 0) + COALESCE(pcss.missed_hrs, 0))::numeric                     attd_pcs_missed_hrs,
            COALESCE(attd.missed_hrs, 0)::numeric                                                      attd_missed_hrs,
            COALESCE(pcss.missed_hrs, 0)::numeric                                                      pcs_missed_hrs,
            COALESCE(meal.missed_hrs, 0)::numeric                                                      meal_missed_hrs,
            COALESCE(alll.missed_memb_init_n, 0)::bigint                                               memb_init_missed_n,
            (COALESCE(attd.missed_memb_init_n, 0) + COALESCE(pcss.missed_memb_init_n, 0))::bigint      attd_pcs_memb_init_missed_n,
            COALESCE(attd.missed_memb_init_n, 0)::bigint                                               attd_memb_init_missed_n,
            COALESCE(pcss.missed_memb_init_n, 0)::bigint                                               pcs_memb_init_missed_n,
            COALESCE(meal.missed_memb_init_n, 0)::bigint                                               meal_memb_init_missed_n,
            COALESCE(alll.missed_memb_init_hrs, 0)::numeric                                            memb_init_missed_hrs,
            (COALESCE(attd.missed_memb_init_hrs, 0) + COALESCE(pcss.missed_memb_init_hrs, 0))::numeric attd_pcs_memb_init_missed_hrs,
            COALESCE(attd.missed_memb_init_hrs, 0)::numeric                                            attd_memb_init_missed_hrs,
            COALESCE(pcss.missed_memb_init_hrs, 0)::numeric                                            pcs_memb_init_missed_hrs,
            COALESCE(meal.missed_memb_init_hrs, 0)::numeric                                            meal_memb_init_missed_hrs,
            COALESCE(alll.missed_prov_init_n, 0)::bigint                                               prov_init_missed_n,
            (COALESCE(attd.missed_prov_init_n, 0) + COALESCE(pcss.missed_prov_init_n, 0))::bigint      attd_pcs_prov_init_missed_n,
            COALESCE(attd.missed_prov_init_n, 0)::bigint                                               attd_prov_init_missed_n,
            COALESCE(pcss.missed_prov_init_n, 0)::bigint                                               pcs_prov_init_missed_n,
            COALESCE(meal.missed_prov_init_n, 0)::bigint                                               meal_prov_init_missed_n,
            COALESCE(alll.missed_prov_init_hrs, 0)::numeric                                            prov_init_missed_hrs,
            (COALESCE(attd.missed_prov_init_hrs, 0) + COALESCE(pcss.missed_prov_init_hrs, 0))::numeric attd_pcs_prov_init_missed_hrs,
            COALESCE(attd.missed_prov_init_hrs, 0)::numeric                                            attd_prov_init_missed_hrs,
            COALESCE(pcss.missed_prov_init_hrs, 0)::numeric                                            pcs_prov_init_missed_hrs,
            COALESCE(meal.missed_prov_init_hrs, 0)::numeric                                            meal_prov_init_missed_hrs,
            COALESCE(alll.missed_night_hrs, 0)::numeric                                                night_missed_hrs,
            (COALESCE(attd.missed_night_hrs, 0) + COALESCE(pcss.missed_night_hrs, 0))::numeric         attd_pcs_night_missed_hrs,
            COALESCE(attd.missed_night_hrs, 0)::numeric                                                attd_night_missed_hrs,
            COALESCE(pcss.missed_night_hrs, 0)::numeric                                                pcs_night_missed_hrs,
            COALESCE(meal.missed_night_hrs, 0)::numeric                                                meal_night_missed_hrs,
            COALESCE(alll.missed_night_n, 0)::bigint                                                   night_missed_n,
            (COALESCE(attd.missed_night_n, 0) + COALESCE(pcss.missed_night_n, 0))::bigint              attd_pcs_night_missed_n,
            COALESCE(attd.missed_night_n, 0)::bigint                                                   attd_night_missed_n,
            COALESCE(pcss.missed_night_n, 0)::bigint                                                   pcs_night_missed_n,
            COALESCE(meal.missed_night_n, 0)::bigint                                                   meal_night_missed_n,
            (COALESCE(attd.appropriate_hours, 0) + COALESCE(pcss.appropriate_hours, 0))::numeric       attd_pcs_appropriate_hrs
        FROM
            types2 alll
            LEFT JOIN types2 attd ON alll.mco_id = attd.mco_id AND alll.bom = attd.bom AND alll.payer_member_id = attd.payer_member_id AND attd.typee = 'attd'::text
            LEFT JOIN types2 pcss ON alll.mco_id = pcss.mco_id AND alll.bom = pcss.bom AND alll.payer_member_id = pcss.payer_member_id AND pcss.typee = 'pcs'::text
            LEFT JOIN types2 resp ON alll.mco_id = resp.mco_id AND alll.bom = resp.bom AND alll.payer_member_id = resp.payer_member_id AND resp.typee = 'respite'::text
            LEFT JOIN types2 meal ON alll.mco_id = meal.mco_id AND alll.bom = meal.bom AND alll.payer_member_id = meal.payer_member_id AND meal.typee = 'meal'::text
        WHERE alll.typee = 'all'::text
    ),
    fake_hours AS (
        SELECT
            ha.member_id,
            round(avg(ha.attd_pcs_hrs)) AS visit_fake_facility_hrs
        FROM
            ds_auth_mm ha
            LEFT JOIN cb.vw_ds_claims_mm c ON c.member_id = ha.member_id AND c.bom = ha.bom AND c.mco_id = 1 AND (COALESCE(c.icfs_tc, 0::numeric) + COALESCE(c.snfa_tc, 0::numeric) + COALESCE(c.ipat_tc, 0::numeric)) > 0::numeric
        WHERE ha.mco_id = 1 AND ha.bom >= '2019-01-01'::date AND ha.bom <= '2019-12-31'::date AND ha.attd_pcs_hrs > 0::numeric AND c.member_id IS NULL
        GROUP BY ha.member_id
    ),
    fake_hours2 AS (
        SELECT
            cm.member_id,
            cm.bom,
            CASE
                WHEN COALESCE(a.attd_pcs_hrs, 0::numeric) < f.visit_fake_facility_hrs
                    THEN f.visit_fake_facility_hrs
                ELSE a.attd_pcs_hrs
                END                   AS real_plus_fake_attd_pcs_hrs,
            f.visit_fake_facility_hrs AS avg_auth_hrs_for_fake
        FROM
            cb.vw_ds_claims_mm cm
            LEFT JOIN cb.vw_ds_auth_mm a ON a.member_id = cm.member_id AND a.bom = cm.bom AND a.mco_id = 1
            LEFT JOIN fake_hours f ON f.member_id = cm.member_id
        WHERE
            cm.mco_id = 1 AND
            cm.bom >= '2019-01-01'::date AND
            cm.bom <= '2019-12-31'::date AND
            (EXISTS(SELECT
                        1
                    FROM cb.vw_ds_auth_mm ha2
                    WHERE ha2.mco_id = 1 AND ha2.member_id = cm.member_id AND ha2.attd_pcs_hrs > 0::numeric AND ha2.bom >= '2019-01-01'::date AND ha2.bom <= '2019-12-31'::date)) AND
            (COALESCE(cm.icfs_tc, 0::numeric) + COALESCE(cm.snfa_tc, 0::numeric) + COALESCE(cm.ipat_tc, 0::numeric)) > 0::numeric
    )
SELECT
    v2.*,
    COALESCE(f2.real_plus_fake_attd_pcs_hrs, v2.attd_pcs_appropriate_hrs) AS attd_pcs_appropriate_hrs,
    f2.real_plus_fake_attd_pcs_hrs,
    f2.avg_auth_hrs_for_fake
FROM
    final_visits v2
    LEFT JOIN fake_hours2 f2 ON f2.member_id = v2.member_id AND f2.bom = v2.bom;


create or replace view cb.vw_natural_support(member_id, natural_support) as
WITH
    clsfm AS (
        SELECT DISTINCT
            c.member_id
        FROM
            (
                SELECT
                    vmme.member_id,
                    vmme.yearr
                FROM cb.vw_member_month_eligibility vmme
                WHERE
                    vmme.yearr = 2019::double precision AND
                    vmme.mco_id = 1
            ) e
            LEFT JOIN cb.claims c ON e.member_id = c.member_id
                AND date_part('year'::text, c.date_from) = e.yearr
                AND c.procedure_code = 't2033'::text
    ),
    pcsp AS (
        SELECT
            p_1.member_id,
            max(CASE
                WHEN fm.member_id IS NOT NULL OR (p_1.concept_id = ANY (ARRAY [550440, 559705])) AND p_1.answer_int = 2
                    THEN 2
                WHEN (p_1.concept_id = ANY (ARRAY [550111, 551924])) AND p_1.answer_int = 1
                    THEN 1
                ELSE 0
                END) AS nat_suppt
        FROM
            cb.pcsp p_1
            LEFT JOIN clsfm fm ON fm.member_id = p_1.member_id
        WHERE p_1.mco_id = 1
        GROUP BY p_1.member_id
    )
SELECT
    a.member_id,
    COALESCE(COALESCE(CASE WHEN x.member_id IS NOT NULL THEN 2 ELSE NULL::integer END, p.nat_suppt), '-1'::integer) AS natural_support
FROM
    cb.vw_ds_all_visit_claims_auths_mm a
    LEFT JOIN clsfm x ON x.member_id = a.member_id
    LEFT JOIN pcsp p ON p.member_id = a.member_id
GROUP BY
    1, 2
ORDER BY
    1, 2
;


select *
from
    ds_visits_mm dvm


select
    pre_post,
    dcm.member_id,
    dcm.bom
from
    ds_claims_mm dcm
    cross join (values ('2019-01-01'::date)) x(bom)
    cross join generate_series(1, 12) srs
    join       (values (-1), (1)) y(pre_post) on dcm.bom = x.bom + interval '1 Month' * (y.pre_post * srs + case when pre_post > 0 then -1 else 0 end)
where
      dcm.mco_id = 2
  and dcm.member_id = 1
order by
    2, 3



select
    vmm.mco_id,
    count(distinct vmm.member_id)                                       nd,
    count(distinct vmm.member_id) filter (where vmm3.member_id is null) nd_gap
from
    vwm_eligibility_mm vmm
    join      vwm_eligibility_mm vmm2 on vmm.bom < vmm2.bom and vmm.member_id = vmm2.member_id
    left join vwm_eligibility_mm vmm3 on vmm.bom + interval '1 Month' = vmm3.bom and vmm.member_id = vmm3.member_id
group by
    1

select *
from vwm_eligibility_mm vem;
select *
from vwm_claims_mm vcm;
select *
from ds_visits_mm dvm;
select *
from ds_auths_mm dam;



select
    vec.mco_id,
    vec.has_facility_ddos,
    max(vec.auth_attd_pcs_util_pct)::decimal(16, 2)                      max_apc_util,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct) _75,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct ) _50,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct) _25,
    min(vec.auth_attd_pcs_util_pct)::decimal(16, 2)                      min_apc_util,
    avg(vec.auth_attd_pcs_util_pct)::decimal(16, 2)                      avg_apc_util
from
    vwm_elig_claims_visits_auths_mm vec
where
    vec.bom between '01-01-2019' and '12-31-2019'
group by
    1, 2
order by
    1, 2
;


        select
            vec.attd_pcs_appropriate_hrs < vec.attd_pcs_visit_hrs,
            count(1)
        from
            vwm_elig_claims_visits_auths_mm vec
        where
            vec.bom between '01-01-2019' and '12-31-2019'
            and vec.is_aligned = 1
            and ggroup in (2,3)
            and vec.attd_pcs_visit_hrs > 0
        group by
            1


with
    x as (
        select
            extract(year from bom)                                                                yr,
            vec.mco_id,
            vec.member_id,
            (sum(vec.attd_pcs_visit_hrs) / nullif(sum(vec.auth_attd_pcs_hrs), 0))::decimal(16, 2) auth_attd_pcs_util_pct,
            sum(vec.attd_pcs_visit_hrs)                                                           attd_pcs_hrs,
            sum(vec.auth_attd_pcs_hrs)                                                            auth_attd_pcs_hrs,
            count(distinct vec.bom) filter (where vec.auth_attd_hrs > 0 )                         mm,
            count(distinct vec.bom) filter (where vec.has_facility_ddos = 1 )                     mm_fac,
            vec.attd_pcs_appropriate_hrs
        from
            vwm_elig_claims_visits_auths_mm vec
        where
            vec.bom between '01-01-2019' and '12-31-2019'
            and vec.is_aligned = 1
            and ggroup in (2,3)
        group by 1, 2, 3
        --having sum(vec.hcbs_attend_care_tc) > 0 or sum(vec.hcbs_pers_care_tc) > 0
        having nullif(sum(vec.auth_attd_hrs), 0) is not null
    )
select
    mco_id,
    count(member_id) n,
    count(distinct member_id) nd,
    count(distinct member_id) filter(where x.mm_fac > 0)  nd_fac
from
    x
group by 1



with
    x as (
        select
            extract(year from bom)                                                                yr,
            vec.mco_id,
            vec.member_id,
            (sum(vec.attd_pcs_visit_hrs) / nullif(sum(vec.auth_attd_pcs_hrs), 0))::decimal(16, 2) auth_attd_pcs_util_pct,
            coalesce(sum(vec.auth_attd_pcs_hrs            ),0) auth_attd_pcs_hrs ,
            coalesce(sum(vec.attd_pcs_visit_hrs           ),0) attd_pcs_hrs      ,
            coalesce(sum(vec.attd_pcs_missed_hrs          ),0) missed_hrs        ,
            coalesce(sum(vec.attd_pcs_prov_init_missed_hrs),0) prov_init_miss_hrs,
            coalesce(sum(vec.attd_pcs_memb_init_missed_hrs),0) memb_init_miss_hrs,
            coalesce(sum(vec.attd_pcs_med_missed_hrs),0) med_miss_hrs,
            count(distinct vec.bom) filter (where vec.auth_attd_hrs     > 0 )                     mm,
            count(distinct vec.bom) filter (where vec.has_facility_ddos = 1 )                     mm_fac
        from
            vwm_elig_claims_visits_auths_mm vec
        where
            vec.bom between '01-01-2019' and '12-31-2019'
            and vec.is_aligned = 1
            and ggroup in (2,3)
        group by 1, 2, 3
        having nullif(sum(vec.auth_attd_hrs), 0) is not null
    )
select
    --vec.yr,
    mm_fac > 0 has_fac,
    case when auth_attd_pcs_hrs::int between    0 and  100 then  100
         when auth_attd_pcs_hrs::int between  101 and  500 then  500
         when auth_attd_pcs_hrs::int between  501 and 1000 then 1000
         when auth_attd_pcs_hrs::int between 1001 and 1500 then 1500
         when auth_attd_pcs_hrs::int between 1501 and 2000 then 2000
         when auth_attd_pcs_hrs::int between 2001 and 2500 then 2500
         when auth_attd_pcs_hrs::int between 2501 and 3000 then 3000
         when auth_attd_pcs_hrs::int between 3001 and 3500 then 3500
         else 9999 end hrs,
    vec.mco_id,
    max(vec.auth_attd_pcs_util_pct)::decimal(16, 2)                                        max_apc_util,
    (PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct))::decimal(16, 2) _90,
    (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct))::decimal(16, 2) _75,
    (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct))::decimal(16, 2) _50,
    (PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct))::decimal(16, 2) _25,
    (PERCENTILE_CONT(0.15) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct))::decimal(16, 2) _15,
    min(vec.auth_attd_pcs_util_pct)::decimal(16, 2)                                        min_apc_util,
    avg(vec.auth_attd_pcs_util_pct)::decimal(16, 2)                                        avg_apc_util,

    sum(auth_attd_pcs_hrs ) - sum(attd_pcs_hrs ) remain_hrs,

    (sum(missed_hrs        ) * 100.0 / nullif(sum(auth_attd_pcs_hrs ) - sum(attd_pcs_hrs ), 0))::int pct_missed,
    (sum(prov_init_miss_hrs) * 100.0 / nullif(sum(auth_attd_pcs_hrs ) - sum(attd_pcs_hrs ), 0))::int pct_prov_missed,
    (sum(memb_init_miss_hrs) * 100.0 / nullif(sum(auth_attd_pcs_hrs ) - sum(attd_pcs_hrs ), 0))::int pct_mbr_missed,
    (sum(med_miss_hrs)  * 100.0 / nullif(sum(auth_attd_pcs_hrs ) - sum(attd_pcs_hrs ), 0))::int pct_med_missed,

    count(member_id) N,
    count(distinct member_id) ND
from
    x vec
where
    mco_id = 2
    and vec.auth_attd_pcs_util_pct <= 1
    --and mm_fac = 0 -- no facility
group by
    1, 2, 3
order by
    1, 2, 3
;




/*
    - age
    - grp
    - dementia | alzheimers
    - paralysis
    - stroke
*/

/*
 case when vm._lob_1_days = greatest(vm._lob_1_days, vm._lob_2_days, vm._lob_3_days) then 1
     when vm._lob_2_days = greatest(vm._lob_1_days, vm._lob_2_days, vm._lob_3_days) then 2
else 3 end lob,
case when vm._grp_1_days = greatest(vm._grp_1_days, vm._grp_2_days, vm._grp_3_days, vm._grp_4_days, vm._grp_5_days, vm._grp_6_days, vm._grp_7_days, vm._grp_8_days) then 1
     when vm._grp_2_days = greatest(vm._grp_1_days, vm._grp_2_days, vm._grp_3_days, vm._grp_4_days, vm._grp_5_days, vm._grp_6_days, vm._grp_7_days, vm._grp_8_days) then 2
     when vm._grp_3_days = greatest(vm._grp_1_days, vm._grp_2_days, vm._grp_3_days, vm._grp_4_days, vm._grp_5_days, vm._grp_6_days, vm._grp_7_days, vm._grp_8_days) then 3
     when vm._grp_4_days = greatest(vm._grp_1_days, vm._grp_2_days, vm._grp_3_days, vm._grp_4_days, vm._grp_5_days, vm._grp_6_days, vm._grp_7_days, vm._grp_8_days) then 4
     when vm._grp_5_days = greatest(vm._grp_1_days, vm._grp_2_days, vm._grp_3_days, vm._grp_4_days, vm._grp_5_days, vm._grp_6_days, vm._grp_7_days, vm._grp_8_days) then 5
     when vm._grp_6_days = greatest(vm._grp_1_days, vm._grp_2_days, vm._grp_3_days, vm._grp_4_days, vm._grp_5_days, vm._grp_6_days, vm._grp_7_days, vm._grp_8_days) then 6
     when vm._grp_7_days = greatest(vm._grp_1_days, vm._grp_2_days, vm._grp_3_days, vm._grp_4_days, vm._grp_5_days, vm._grp_6_days, vm._grp_7_days, vm._grp_8_days) then 7
     when vm._grp_8_days = greatest(vm._grp_1_days, vm._grp_2_days, vm._grp_3_days, vm._grp_4_days, vm._grp_5_days, vm._grp_6_days, vm._grp_7_days, vm._grp_8_days) then 8
end grp,

 */
drop table if exists _xxx;
create temp table _xxx as
select * from cb.fn_elig_claims_visits_auths_agg(2, '2019-01-01'::date, '2019-12-31'::date)
union all
select * from cb.fn_elig_claims_visits_auths_agg(2, '2018-01-01'::date, '2018-12-31'::date)
;



select * from _xxx x where x.member_id = 5904;
/*
 grp	hrs_max	hrs_9	hrs_8	hrs_7	hrs_6	hrs_5	hrs_4	hrs_3	hrs_2	hrs_1	hrs_min	n	nd
  2	348	195	157	131	110	92	71	49	30	12	0	4359	4359
  3	121	60	54	49	44	39	33	28	19	8	0	989	989

 */
with hrs as (
    select
        x.grp,
        max((x.attd_pcs_appropriate_hrs/x.mm)::int ) hrs_max,
        (PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_9,
        (PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_8,
        (PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_7,
        (PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_6,
        (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_5,
        (PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_4,
        (PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_3,
        (PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_2,
        (PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_1,
        min((x.attd_pcs_appropriate_hrs/x.mm)::int ) hrs_min,
        count(x.member_id) n
        --count(distinct x.member_id) nd
    from _xxx x
    where
        x.is_aligned = 1
    group by 1
)
select
    'g(' || coalesce(x.grp::text,'x') || ')-hrs(' || (

            case when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_1  then '01'
                 when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_2  then '02'
                 when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_3  then '03'
                 when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_4  then '04'
                 when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_5  then '05'
                 when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_6  then '06'
                 when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_7  then '07'
                 when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_8  then '08'
                 when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_9  then '09'
                 when (attd_pcs_appropriate_hrs/x.mm)::int  <= h.hrs_max then '10'
                 else '11'
            end

    )::text || ')' binn
    , max(attd_pcs_appropriate_hrs) max_hrs
    , min(attd_pcs_appropriate_hrs) min_hrs
    --, min(lob) lob_min
    --, max(lob) lob_max
    , count(member_id) n
    , avg(age)::int age_avg
    --, count(distinct member_id) nd

    , (avg(coalesce(pcp_ddos         ,0) * 1.0 / mm))::decimal(16,2) pcp_avg
    , (avg(coalesce(copd_ddos        ,0) * 1.0 / mm))::decimal(16,2) copd_avg
    , (avg(coalesce(pulmonar_ddos    ,0) * 1.0 / mm))::decimal(16,2) pulm_avg
    , (avg(coalesce(chf_ddos         ,0) * 1.0 / mm))::decimal(16,2) chf_avg
    , (avg(coalesce(heart_ddos       ,0) * 1.0 / mm))::decimal(16,2) heart_avg
    , (avg(coalesce(cancer_ddos      ,0) * 1.0 / mm))::decimal(16,2) canc_avg
    , (avg(coalesce(ckd_ddos         ,0) * 1.0 / mm))::decimal(16,2) ckd_avg
    , (avg(coalesce(esrd_ddos        ,0) * 1.0 / mm))::decimal(16,2) esrd_avg
    , (avg(coalesce(hyperlipid_ddos  ,0) * 1.0 / mm))::decimal(16,2) lipi_avg
    , (avg(coalesce(diab_ddos        ,0) * 1.0 / mm))::decimal(16,2) diab_avg
    , (avg(coalesce(alzh_ddos        ,0) * 1.0 / mm))::decimal(16,2) alzh_avg
    , (avg(coalesce(dementia_ddos    ,0) * 1.0 / mm))::decimal(16,2) dem_avg
    , (avg(coalesce(stroke_ddos      ,0) * 1.0 / mm))::decimal(16,2) strk_avg
    , (avg(coalesce(paralysis_ddos   ,0) * 1.0 / mm))::decimal(16,2) para_avg
    , (avg(coalesce(hypertension_ddos,0) * 1.0 / mm))::decimal(16,2) hyper_avg
    , (avg(coalesce(fall_ddos        ,0) * 1.0 / mm))::decimal(16,2) fall_avg
    , (avg(coalesce(transplant_ddos  ,0) * 1.0 / mm))::decimal(16,2) trans_avg
    , (avg(coalesce(liver_ddos       ,0) * 1.0 / mm))::decimal(16,2) liver_avg
    , (avg(coalesce(hippfract_ddos   ,0) * 1.0 / mm))::decimal(16,2) hip_avg
    , (avg(coalesce(depression_ddos  ,0) * 1.0 / mm))::decimal(16,2) depre_avg
    , (avg(coalesce(psychosis_ddos   ,0) * 1.0 / mm))::decimal(16,2) psych_avg
    , (avg(coalesce(drug_ddos        ,0) * 1.0 / mm))::decimal(16,2) drug_avg
    , (avg(coalesce(alcohol_ddos     ,0) * 1.0 / mm))::decimal(16,2) alco_avg


    , (count(member_id) filter ( where pcp_ddos          > 2) * 1.0 / count(member_id))::decimal(16,2) pcp_pct
    , (count(member_id) filter ( where copd_ddos         > 2) * 1.0 / count(member_id))::decimal(16,2) copd_pct
    , (count(member_id) filter ( where pulmonar_ddos     > 2) * 1.0 / count(member_id))::decimal(16,2) pulm_pct
    , (count(member_id) filter ( where chf_ddos          > 2) * 1.0 / count(member_id))::decimal(16,2) chf_pct
    , (count(member_id) filter ( where heart_ddos        > 2) * 1.0 / count(member_id))::decimal(16,2) heart_pct
    , (count(member_id) filter ( where cancer_ddos       > 2) * 1.0 / count(member_id))::decimal(16,2) canc_pct
    , (count(member_id) filter ( where ckd_ddos          > 2) * 1.0 / count(member_id))::decimal(16,2) ckd_pct
    , (count(member_id) filter ( where esrd_ddos         > 2) * 1.0 / count(member_id))::decimal(16,2) esrd_pct
    , (count(member_id) filter ( where hyperlipid_ddos   > 2) * 1.0 / count(member_id))::decimal(16,2) lipi_pct
    , (count(member_id) filter ( where diab_ddos         > 2) * 1.0 / count(member_id))::decimal(16,2) diab_pct
    , (count(member_id) filter ( where alzh_ddos         > 2) * 1.0 / count(member_id))::decimal(16,2) alzh_pct
    , (count(member_id) filter ( where dementia_ddos     > 2) * 1.0 / count(member_id))::decimal(16,2) dem_pct
    , (count(member_id) filter ( where stroke_ddos       > 2) * 1.0 / count(member_id))::decimal(16,2) strk_pct
    , (count(member_id) filter ( where paralysis_ddos    > 2) * 1.0 / count(member_id))::decimal(16,2) para_pct
    , (count(member_id) filter ( where hypertension_ddos > 2) * 1.0 / count(member_id))::decimal(16,2) hyper_pct
    , (count(member_id) filter ( where fall_ddos         > 2) * 1.0 / count(member_id))::decimal(16,2) fall_pct
    , (count(member_id) filter ( where transplant_ddos   > 2) * 1.0 / count(member_id))::decimal(16,2) trans_pct
    , (count(member_id) filter ( where liver_ddos        > 2) * 1.0 / count(member_id))::decimal(16,2) liver_pct
    , (count(member_id) filter ( where hippfract_ddos    > 2) * 1.0 / count(member_id))::decimal(16,2) hip_pct
    , (count(member_id) filter ( where depression_ddos   > 2) * 1.0 / count(member_id))::decimal(16,2) depre_pct
    , (count(member_id) filter ( where psychosis_ddos    > 2) * 1.0 / count(member_id))::decimal(16,2) psych_pct
    , (count(member_id) filter ( where drug_ddos         > 2) * 1.0 / count(member_id))::decimal(16,2) drug_pct
    , (count(member_id) filter ( where alcohol_ddos      > 2) * 1.0 / count(member_id))::decimal(16,2) alco_pct

from _xxx x
join hrs h on h.grp = x.grp
where x.attd_pcs_appropriate_hrs >= 0 and x.grp in (2,3)
    and x.is_aligned = 1
    and x.night_visit_n > 5
group by 1
order by 1,2

select x.lob, x.grp, count(x.member_id) n, count(distinct x.member_id) from _xxx x group by 1, 2 order by 1, 2;
select
    x.grp,
    max((x.attd_pcs_appropriate_hrs/x.mm)::int ) hrs_max,
    (PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_9,
    (PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_8,
    (PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_7,
    (PERCENTILE_CONT(0.6) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_6,
    (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_5,
    (PERCENTILE_CONT(0.4) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_4,
    (PERCENTILE_CONT(0.3) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_3,
    (PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_2,
    (PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY (x.attd_pcs_appropriate_hrs/x.mm)::int ))::decimal(16,0) hrs_1,
    min((x.attd_pcs_appropriate_hrs/x.mm)::int ) hrs_min,
    count(x.member_id) n,
    count(distinct x.member_id) nd
from _xxx x
group by 1




drop function fn_elig_claims_visits_auths_agg;
create or replace function cb.fn_elig_claims_visits_auths_agg(_mco_id int, _bom date, _eom date)

returns table
    (
        mco_id smallint,
        member_id bigint,
        is_male bit,
        age smallint,
        lob           smallint,
        grp           smallint,
        is_aligned    smallint,
        is_unaligned  smallint,
        lob_max smallint,
        grp_max smallint,
        lob_1_mm int,
        lob_2_mm int,
        lob_3_mm int,
        grp_1_mm int,
        grp_2_mm int,
        grp_3_mm int,
        grp_4_mm int,
        grp_5_mm int,
        grp_6_mm int,
        grp_7_mm int,
        grp_8_mm int,
        aligned_mm int,
        unaligned_mm int,
        has_facility_ddos_mm int,
        mm int,
        auth_attd_pcs_util_pct_avg  decimal(16,2),
        auth_resp_util_pct_avg      decimal(16,2),
        appropriate_auth_pct_util   decimal(16,2),
        visit_auth_pct_util         decimal(16,2),
        auths_n int,
        auth_attd_n int,
        auth_meal_n int,
        auth_pc_n int,
        auth_resp_n int,
        auth_units int,
        auth_hrs int,
        auth_attd_pcs_hrs int,
        auth_attd_hrs int,
        auth_meal_hrs int,
        auth_pcs_hrs int,
        auth_resp_hrs int,
        visit_hrs int,
        attd_pcs_visit_hrs int,
        attd_visit_hrs int,
        pcs_visit_hrs int,
        resp_visit_hrs int,
        night_visit_hrs int,
        attd_pcs_night_visit_hrs int,
        attd_night_visit_hrs int,
        pcs_night_visit_hrs int,
        resp_night_visit_hrs int,
        visit_n int,
        attd_pcs_visit_n int,
        attd_visit_n int,
        pcs_visit_n int,
        resp_visit_n int,
        meal_visit_n int,
        night_visit_n int,
        attd_pcs_night_visit_n int,
        attd_night_visit_n int,
        pcs_night_visit_n int,
        resp_night_visit_n int,
        meal_night_visit_n int,
        avg_visit_hrs int,
        avg_night_visit_hrs int,
        attd_avg_night_visit_hrs int,
        pcs_avg_night_visit_hrs int,
        resp_avg_night_visit_hrs int,
        cg_visit_nd int,
        attd_pcs_cg_visit_nd int,
        attd_cg_visit_nd int,
        pcs_cg_visit_nd int,
        meal_cg_visit_nd int,
        resp_cg_visit_nd int,
        missed_n int,
        attd_pcs_missed_n int,
        attd_missed_n int,
        pcs_missed_n int,
        meal_missed_n int,
        missed_hrs int,
        attd_pcs_missed_hrs int,
        attd_missed_hrs int,
        pcs_missed_hrs int,
        meal_missed_hrs int,
        memb_init_missed_n int,
        attd_pcs_memb_init_missed_n int,
        attd_memb_init_missed_n int,
        pcs_memb_init_missed_n int,
        meal_memb_init_missed_n int,
        memb_init_missed_hrs int,
        attd_pcs_memb_init_missed_hrs int,
        attd_memb_init_missed_hrs int,
        pcs_memb_init_missed_hrs int,
        attd_pcs_med_missed_hrs int,
        prov_init_missed_n int,
        attd_pcs_prov_init_missed_n int,
        attd_prov_init_missed_n int,
        pcs_prov_init_missed_n int,
        prov_init_missed_hrs int,
        attd_pcs_prov_init_missed_hrs int,
        attd_prov_init_missed_hrs int,
        pcs_prov_init_missed_hrs int,
        night_missed_hrs int,
        attd_pcs_night_missed_hrs int,
        attd_night_missed_hrs int,
        pcs_night_missed_hrs int,
        night_missed_n int,
        attd_pcs_night_missed_n int,
        attd_night_missed_n int,
        pcs_night_missed_n int,
        meal_night_missed_n int,
        attd_pcs_appropriate_hrs int,
        attd_pcs_fake_hrs int,
        tc                     decimal(16,2),
        hcbs_tc                decimal(16,2),
        icf_tc                 decimal(16,2),
        ip_tc                  decimal(16,2),
        rx_tc                  decimal(16,2),
        ed_tc                  decimal(16,2),
        snf_tc                 decimal(16,2),
        out_tc                 decimal(16,2),
        pro_tc                 decimal(16,2),
        spfac_tc               decimal(16,2),
        amb_tc                 decimal(16,2),
        hh_tc                  decimal(16,2),
        hosp_tc                decimal(16,2),
        oth_tc                 decimal(16,2),
        hcbs_respite_tc        decimal(16,2),
        hcbs_fam_care_stip_tc  decimal(16,2),
        hcbs_com_trans_tc      decimal(16,2),
        hcbs_educ_train_tc     decimal(16,2),
        hcbs_com_liv_fam_tc    decimal(16,2),
        hcbs_com_liv_tc        decimal(16,2),
        hcbs_attend_care_tc    decimal(16,2),
        hcbs_com_trans_waiv_tc decimal(16,2),
        hcbs_home_meal_tc      decimal(16,2),
        hcbs_pers_care_tc      decimal(16,2),
        hcbs_ther_behav_tc     decimal(16,2),
        hcbs_unsk_respite_tc   decimal(16,2),
        hcbs_waiv_svc_tc       decimal(16,2),
        ddos              int,
        hcbs_ddos         int,
        icf_ddos          int,
        ip_ddos           int,
        rx_ddos           int,
        ed_ddos           int,
        snf_ddos          int,
        out_ddos          int,
        pro_ddos          int,
        spfac_ddos        int,
        amb_ddos          int,
        hh_ddos           int,
        hosp_ddos         int,
        oth_ddos          int,
        pcp_ddos          int,
        pulmonar_ddos     int,
        copd_ddos         int,
        chf_ddos          int,
        heart_ddos        int,
        cancer_ddos       int,
        ckd_ddos          int,
        esrd_ddos         int,
        hyperlipid_ddos   int,
        diab_ddos         int,
        alzh_ddos         int,
        dementia_ddos     int,
        stroke_ddos       int,
        hypertension_ddos int,
        fall_ddos         int,
        transplant_ddos   int,
        liver_ddos        int,
        hippfract_ddos    int,
        depression_ddos   int,
        psychosis_ddos    int,
        drug_ddos         int,
        alcohol_ddos      int,
        paralysis_ddos    int
)

AS $$
BEGIN

    return query
    select
          x.mco_id::smallint
        , x.member_id
        , x.is_male::bit
        , x.age::smallint
        , (case when x.lob_1_mm = greatest(x.lob_1_mm, x.lob_2_mm, x.lob_3_mm) then 1
                when x.lob_2_mm = greatest(x.lob_1_mm, x.lob_2_mm, x.lob_3_mm) then 2
                else 3 end)::smallint lob
        , (case when x.grp_1_mm = greatest(x.grp_1_mm, x.grp_2_mm, x.grp_3_mm, x.grp_4_mm, x.grp_5_mm, x.grp_6_mm, x.grp_7_mm, x.grp_8_mm, x.grp_x_mm) then 1
                when x.grp_2_mm = greatest(x.grp_1_mm, x.grp_2_mm, x.grp_3_mm, x.grp_4_mm, x.grp_5_mm, x.grp_6_mm, x.grp_7_mm, x.grp_8_mm, x.grp_x_mm) then 2
                when x.grp_3_mm = greatest(x.grp_1_mm, x.grp_2_mm, x.grp_3_mm, x.grp_4_mm, x.grp_5_mm, x.grp_6_mm, x.grp_7_mm, x.grp_8_mm, x.grp_x_mm) then 3
                when x.grp_4_mm = greatest(x.grp_1_mm, x.grp_2_mm, x.grp_3_mm, x.grp_4_mm, x.grp_5_mm, x.grp_6_mm, x.grp_7_mm, x.grp_8_mm, x.grp_x_mm) then 4
                when x.grp_5_mm = greatest(x.grp_1_mm, x.grp_2_mm, x.grp_3_mm, x.grp_4_mm, x.grp_5_mm, x.grp_6_mm, x.grp_7_mm, x.grp_8_mm, x.grp_x_mm) then 5
                when x.grp_6_mm = greatest(x.grp_1_mm, x.grp_2_mm, x.grp_3_mm, x.grp_4_mm, x.grp_5_mm, x.grp_6_mm, x.grp_7_mm, x.grp_8_mm, x.grp_x_mm) then 6
                when x.grp_7_mm = greatest(x.grp_1_mm, x.grp_2_mm, x.grp_3_mm, x.grp_4_mm, x.grp_5_mm, x.grp_6_mm, x.grp_7_mm, x.grp_8_mm, x.grp_x_mm) then 7
                when x.grp_8_mm = greatest(x.grp_1_mm, x.grp_2_mm, x.grp_3_mm, x.grp_4_mm, x.grp_5_mm, x.grp_6_mm, x.grp_7_mm, x.grp_8_mm, x.grp_x_mm) then 8
             else -1 end)::smallint grp
        , (case when x.aligned_mm   = greatest(x.aligned_mm, x.unaligned_mm) then 1 else 0 end)::smallint is_aligned
        , (case when x.unaligned_mm = greatest(x.aligned_mm, x.unaligned_mm) then 1 else 0 end)::smallint is_unaligned
        , x.lob_max::smallint
        , x.grp_max::smallint
        , x.lob_1_mm::int
        , x.lob_2_mm::int
        , x.lob_3_mm::int
        , x.grp_1_mm::int
        , x.grp_2_mm::int
        , x.grp_3_mm::int
        , x.grp_4_mm::int
        , x.grp_5_mm::int
        , x.grp_6_mm::int
        , x.grp_7_mm::int
        , x.grp_8_mm::int
        , x.aligned_mm::int
        , x.unaligned_mm::int
        , x.has_facility_ddos_mm::int
        , x.mm::int
        , x.auth_attd_pcs_util_pct_avg::decimal(16,2)
        , x.auth_resp_util_pct_avg::decimal(16,2)
        , x.appropriate_auth_pct_util::decimal(16,2)
        , x.visit_auth_pct_util::decimal(16,2)
        , x.auths_n::int
        , x.auth_attd_n::int
        , x.auth_meal_n::int
        , x.auth_pc_n::int
        , x.auth_resp_n::int
        , x.auth_units::int
        , x.auth_hrs::int
        , x.auth_attd_pcs_hrs::int
        , x.auth_attd_hrs::int
        , x.auth_meal_hrs::int
        , x.auth_pcs_hrs::int
        , x.auth_resp_hrs::int
        , x.visit_hrs::int
        , x.attd_pcs_visit_hrs::int
        , x.attd_visit_hrs::int
        , x.pcs_visit_hrs::int
        , x.resp_visit_hrs::int
        , x.night_visit_hrs::int
        , x.attd_pcs_night_visit_hrs::int
        , x.attd_night_visit_hrs::int
        , x.pcs_night_visit_hrs::int
        , x.resp_night_visit_hrs::int
        , x.visit_n::int
        , x.attd_pcs_visit_n::int
        , x.attd_visit_n::int
        , x.pcs_visit_n::int
        , x.resp_visit_n::int
        , x.meal_visit_n::int
        , x.night_visit_n::int
        , x.attd_pcs_night_visit_n::int
        , x.attd_night_visit_n::int
        , x.pcs_night_visit_n::int
        , x.resp_night_visit_n::int
        , x.meal_night_visit_n::int
        , x.avg_visit_hrs::int
        , x.avg_night_visit_hrs::int
        , x.attd_avg_night_visit_hrs::int
        , x.pcs_avg_night_visit_hrs::int
        , x.resp_avg_night_visit_hrs::int
        , x.cg_visit_nd::int
        , x.attd_pcs_cg_visit_nd::int
        , x.attd_cg_visit_nd::int
        , x.pcs_cg_visit_nd::int
        , x.meal_cg_visit_nd::int
        , x.resp_cg_visit_nd::int
        , x.missed_n::int
        , x.attd_pcs_missed_n::int
        , x.attd_missed_n::int
        , x.pcs_missed_n::int
        , x.meal_missed_n::int
        , x.missed_hrs::int
        , x.attd_pcs_missed_hrs::int
        , x.attd_missed_hrs::int
        , x.pcs_missed_hrs::int
        , x.meal_missed_hrs::int
        , x.memb_init_missed_n::int
        , x.attd_pcs_memb_init_missed_n::int
        , x.attd_memb_init_missed_n::int
        , x.pcs_memb_init_missed_n::int
        , x.meal_memb_init_missed_n::int
        , x.memb_init_missed_hrs::int
        , x.attd_pcs_memb_init_missed_hrs::int
        , x.attd_memb_init_missed_hrs::int
        , x.pcs_memb_init_missed_hrs::int
        , x.attd_pcs_med_missed_hrs::int
        , x.prov_init_missed_n::int
        , x.attd_pcs_prov_init_missed_n::int
        , x.attd_prov_init_missed_n::int
        , x.pcs_prov_init_missed_n::int
        , x.prov_init_missed_hrs::int
        , x.attd_pcs_prov_init_missed_hrs::int
        , x.attd_prov_init_missed_hrs::int
        , x.pcs_prov_init_missed_hrs::int
        , x.night_missed_hrs::int
        , x.attd_pcs_night_missed_hrs::int
        , x.attd_night_missed_hrs::int
        , x.pcs_night_missed_hrs::int
        , x.night_missed_n::int
        , x.attd_pcs_night_missed_n::int
        , x.attd_night_missed_n::int
        , x.pcs_night_missed_n::int
        , x.meal_night_missed_n::int
        , x.attd_pcs_appropriate_hrs::int
        , x.attd_pcs_fake_hrs::int
        , x.tc::decimal(16,2)
        , x.hcbs_tc::decimal(16,2)
        , x.icf_tc::decimal(16,2)
        , x.ip_tc::decimal(16,2)
        , x.rx_tc::decimal(16,2)
        , x.ed_tc::decimal(16,2)
        , x.snf_tc::decimal(16,2)
        , x.out_tc::decimal(16,2)
        , x.pro_tc::decimal(16,2)
        , x.spfac_tc::decimal(16,2)
        , x.amb_tc::decimal(16,2)
        , x.hh_tc::decimal(16,2)
        , x.hosp_tc::decimal(16,2)
        , x.oth_tc::decimal(16,2)
        , x.hcbs_respite_tc::decimal(16,2)
        , x.hcbs_fam_care_stip_tc::decimal(16,2)
        , x.hcbs_com_trans_tc::decimal(16,2)
        , x.hcbs_educ_train_tc::decimal(16,2)
        , x.hcbs_com_liv_fam_tc::decimal(16,2)
        , x.hcbs_com_liv_tc::decimal(16,2)
        , x.hcbs_attend_care_tc::decimal(16,2)
        , x.hcbs_com_trans_waiv_tc::decimal(16,2)
        , x.hcbs_home_meal_tc::decimal(16,2)
        , x.hcbs_pers_care_tc::decimal(16,2)
        , x.hcbs_ther_behav_tc::decimal(16,2)
        , x.hcbs_unsk_respite_tc::decimal(16,2)
        , x.hcbs_waiv_svc_tc::decimal(16,2)
        , x.ddos::int
        , x.hcbs_ddos::int
        , x.icf_ddos::int
        , x.ip_ddos::int
        , x.rx_ddos::int
        , x.ed_ddos::int
        , x.snf_ddos::int
        , x.out_ddos::int
        , x.pro_ddos::int
        , x.spfac_ddos::int
        , x.amb_ddos::int
        , x.hh_ddos::int
        , x.hosp_ddos::int
        , x.oth_ddos::int
        , x.pcp_ddos::int
        , x.pulmonar_ddos::int
        , x.copd_ddos::int
        , x.chf_ddos::int
        , x.heart_ddos::int
        , x.cancer_ddos::int
        , x.ckd_ddos::int
        , x.esrd_ddos::int
        , x.hyperlipid_ddos::int
        , x.diab_ddos::int
        , x.alzh_ddos::int
        , x.dementia_ddos::int
        , x.stroke_ddos::int
        , x.hypertension_ddos::int
        , x.fall_ddos::int
        , x.transplant_ddos::int
        , x.liver_ddos::int
        , x.hippfract_ddos::int
        , x.depression_ddos::int
        , x.psychosis_ddos::int
        , x.drug_ddos::int
        , x.alcohol_ddos::int
        , x.paralysis_ddos::int
    from
        (
            select
                vm.mco_id,
                vm.member_id,
                vm.is_male,
                max(vm.age)                                                                            age,
                max(vm.lob)                                                                            lob_max,
                max(vm.ggroup)                                                                         grp_max,
                count(1) filter ( where vm.lob = 1 )                                                   lob_1_mm,
                count(1) filter ( where vm.lob = 2 )                                                   lob_2_mm,
                count(1) filter ( where vm.lob = 3 )                                                   lob_3_mm,
                count(1) filter ( where vm.ggroup = 1 )                                                grp_1_mm,
                count(1) filter ( where vm.ggroup = 2 )                                                grp_2_mm,
                count(1) filter ( where vm.ggroup = 3 )                                                grp_3_mm,
                count(1) filter ( where vm.ggroup = 4 )                                                grp_4_mm,
                count(1) filter ( where vm.ggroup = 5 )                                                grp_5_mm,
                count(1) filter ( where vm.ggroup = 6 )                                                grp_6_mm,
                count(1) filter ( where vm.ggroup = 7 )                                                grp_7_mm,
                count(1) filter ( where vm.ggroup = 8 )                                                grp_8_mm,
                count(1) filter ( where coalesce(vm.ggroup,-1) not between 1 and 8 )                   grp_x_mm,
                count(1) filter ( where vm.is_aligned = 1 )                                            aligned_mm,
                count(1) filter ( where vm.is_unaligned = 1 )                                          unaligned_mm,
                count(1) filter ( where vm.has_facility_ddos = 1 )                                     has_facility_ddos_mm,
                count(1)                                                                            mm,
                avg(vm.auth_attd_pcs_util_pct)::decimal(16, 2)                                         auth_attd_pcs_util_pct_avg,
                avg(vm.auth_resp_util_pct)::decimal(16, 2)                                             auth_resp_util_pct_avg,
                (sum(vm.attd_pcs_appropriate_hrs) / nullif(sum(vm.auth_attd_pcs_hrs), 0))::decimal(16, 2) appropriate_auth_pct_util,
                (sum(vm.attd_pcs_visit_hrs)       / nullif(sum(vm.auth_attd_pcs_hrs), 0))::decimal(16, 2)       visit_auth_pct_util,
                sum(vm.auths_n)                                                                        auths_n,
                sum(vm.auth_attd_n)                                                                    auth_attd_n,
                sum(vm.auth_meal_n)                                                                    auth_meal_n,
                sum(vm.auth_pc_n)                                                                      auth_pc_n,
                sum(vm.auth_resp_n)                                                                    auth_resp_n,
                sum(vm.auth_units)                                                                     auth_units,
                sum(vm.auth_hrs)                                                                       auth_hrs,
                sum(vm.auth_attd_pcs_hrs)                                                              auth_attd_pcs_hrs,
                sum(vm.auth_attd_hrs)                                                                  auth_attd_hrs,
                sum(vm.auth_meal_hrs)                                                                  auth_meal_hrs,
                sum(vm.auth_pcs_hrs)                                                                   auth_pcs_hrs,
                sum(vm.auth_resp_hrs)                                                                  auth_resp_hrs,
                sum(vm.visit_hrs)                                                                      visit_hrs,
                sum(vm.attd_pcs_visit_hrs)                                                             attd_pcs_visit_hrs,
                sum(vm.attd_visit_hrs)                                                                 attd_visit_hrs,
                sum(vm.pcs_visit_hrs)                                                                  pcs_visit_hrs,
                sum(vm.resp_visit_hrs)                                                                 resp_visit_hrs,
                sum(vm.night_visit_hrs)                                                                night_visit_hrs,
                sum(vm.attd_pcs_night_visit_hrs)                                                       attd_pcs_night_visit_hrs,
                sum(vm.attd_night_visit_hrs)                                                           attd_night_visit_hrs,
                sum(vm.pcs_night_visit_hrs)                                                            pcs_night_visit_hrs,
                sum(vm.resp_night_visit_hrs)                                                           resp_night_visit_hrs,
                sum(vm.visit_n)                                                                        visit_n,
                sum(vm.attd_pcs_visit_n)                                                               attd_pcs_visit_n,
                sum(vm.attd_visit_n)                                                                   attd_visit_n,
                sum(vm.pcs_visit_n)                                                                    pcs_visit_n,
                sum(vm.resp_visit_n)                                                                   resp_visit_n,
                sum(vm.meal_visit_n)                                                                   meal_visit_n,
                sum(vm.night_visit_n)                                                                  night_visit_n,
                sum(vm.attd_pcs_night_visit_n)                                                         attd_pcs_night_visit_n,
                sum(vm.attd_night_visit_n)                                                             attd_night_visit_n,
                sum(vm.pcs_night_visit_n)                                                              pcs_night_visit_n,
                sum(vm.resp_night_visit_n)                                                             resp_night_visit_n,
                sum(vm.meal_night_visit_n)                                                             meal_night_visit_n,
                sum(vm.avg_visit_hrs)                                                                  avg_visit_hrs,
                sum(vm.avg_night_visit_hrs)                                                            avg_night_visit_hrs,
                sum(vm.attd_avg_night_visit_hrs)                                                       attd_avg_night_visit_hrs,
                sum(vm.pcs_avg_night_visit_hrs)                                                        pcs_avg_night_visit_hrs,
                sum(vm.resp_avg_night_visit_hrs)                                                       resp_avg_night_visit_hrs,
                sum(vm.cg_visit_nd)                                                                    cg_visit_nd,
                sum(vm.attd_pcs_cg_visit_nd)                                                           attd_pcs_cg_visit_nd,
                sum(vm.attd_cg_visit_nd)                                                               attd_cg_visit_nd,
                sum(vm.pcs_cg_visit_nd)                                                                pcs_cg_visit_nd,
                sum(vm.meal_cg_visit_nd)                                                               meal_cg_visit_nd,
                sum(vm.resp_cg_visit_nd)                                                               resp_cg_visit_nd,
                sum(vm.missed_n)                                                                       missed_n,
                sum(vm.attd_pcs_missed_n)                                                              attd_pcs_missed_n,
                sum(vm.attd_missed_n)                                                                  attd_missed_n,
                sum(vm.pcs_missed_n)                                                                   pcs_missed_n,
                sum(vm.meal_missed_n)                                                                  meal_missed_n,
                sum(vm.missed_hrs)                                                                     missed_hrs,
                sum(vm.attd_pcs_missed_hrs)                                                            attd_pcs_missed_hrs,
                sum(vm.attd_missed_hrs)                                                                attd_missed_hrs,
                sum(vm.pcs_missed_hrs)                                                                 pcs_missed_hrs,
                sum(vm.meal_missed_hrs)                                                                meal_missed_hrs,
                sum(vm.memb_init_missed_n)                                                             memb_init_missed_n,
                sum(vm.attd_pcs_memb_init_missed_n)                                                    attd_pcs_memb_init_missed_n,
                sum(vm.attd_memb_init_missed_n)                                                        attd_memb_init_missed_n,
                sum(vm.pcs_memb_init_missed_n)                                                         pcs_memb_init_missed_n,
                sum(vm.meal_memb_init_missed_n)                                                        meal_memb_init_missed_n,
                sum(vm.memb_init_missed_hrs)                                                           memb_init_missed_hrs,
                sum(vm.attd_pcs_memb_init_missed_hrs)                                                  attd_pcs_memb_init_missed_hrs,
                sum(vm.attd_memb_init_missed_hrs)                                                      attd_memb_init_missed_hrs,
                sum(vm.pcs_memb_init_missed_hrs)                                                       pcs_memb_init_missed_hrs,
                sum(vm.attd_pcs_med_missed_hrs)                                                        attd_pcs_med_missed_hrs,
                sum(vm.prov_init_missed_n)                                                             prov_init_missed_n,
                sum(vm.attd_pcs_prov_init_missed_n)                                                    attd_pcs_prov_init_missed_n,
                sum(vm.attd_prov_init_missed_n)                                                        attd_prov_init_missed_n,
                sum(vm.pcs_prov_init_missed_n)                                                         pcs_prov_init_missed_n,
                sum(vm.prov_init_missed_hrs)                                                           prov_init_missed_hrs,
                sum(vm.attd_pcs_prov_init_missed_hrs)                                                  attd_pcs_prov_init_missed_hrs,
                sum(vm.attd_prov_init_missed_hrs)                                                      attd_prov_init_missed_hrs,
                sum(vm.pcs_prov_init_missed_hrs)                                                       pcs_prov_init_missed_hrs,
                sum(vm.night_missed_hrs)                                                               night_missed_hrs,
                sum(vm.attd_pcs_night_missed_hrs)                                                      attd_pcs_night_missed_hrs,
                sum(vm.attd_night_missed_hrs)                                                          attd_night_missed_hrs,
                sum(vm.pcs_night_missed_hrs)                                                           pcs_night_missed_hrs,
                sum(vm.night_missed_n)                                                                 night_missed_n,
                sum(vm.attd_pcs_night_missed_n)                                                        attd_pcs_night_missed_n,
                sum(vm.attd_night_missed_n)                                                            attd_night_missed_n,
                sum(vm.pcs_night_missed_n)                                                             pcs_night_missed_n,
                sum(vm.meal_night_missed_n)                                                            meal_night_missed_n,
                sum(vm.attd_pcs_appropriate_hrs)                                                       attd_pcs_appropriate_hrs,
                sum(vm.attd_pcs_fake_hrs)                                                              attd_pcs_fake_hrs,
                sum(vm.tc)                                                                             tc,
                sum(vm.hcbs_tc)                                                                        hcbs_tc,
                sum(vm.icf_tc)                                                                         icf_tc,
                sum(vm.ip_tc)                                                                          ip_tc,
                sum(vm.rx_tc)                                                                          rx_tc,
                sum(vm.ed_tc)                                                                          ed_tc,
                sum(vm.snf_tc)                                                                         snf_tc,
                sum(vm.out_tc)                                                                         out_tc,
                sum(vm.pro_tc)                                                                         pro_tc,
                sum(vm.spfac_tc)                                                                       spfac_tc,
                sum(vm.amb_tc)                                                                         amb_tc,
                sum(vm.hh_tc)                                                                          hh_tc,
                sum(vm.hosp_tc)                                                                        hosp_tc,
                sum(vm.oth_tc)                                                                         oth_tc,
                sum(vm.hcbs_respite_tc)                                                                hcbs_respite_tc,
                sum(vm.hcbs_fam_care_stip_tc)                                                          hcbs_fam_care_stip_tc,
                sum(vm.hcbs_com_trans_tc)                                                              hcbs_com_trans_tc,
                sum(vm.hcbs_educ_train_tc)                                                             hcbs_educ_train_tc,
                sum(vm.hcbs_com_liv_fam_tc)                                                            hcbs_com_liv_fam_tc,
                sum(vm.hcbs_com_liv_tc)                                                                hcbs_com_liv_tc,
                sum(vm.hcbs_attend_care_tc)                                                            hcbs_attend_care_tc,
                sum(vm.hcbs_com_trans_waiv_tc)                                                         hcbs_com_trans_waiv_tc,
                sum(vm.hcbs_home_meal_tc)                                                              hcbs_home_meal_tc,
                sum(vm.hcbs_pers_care_tc)                                                              hcbs_pers_care_tc,
                sum(vm.hcbs_ther_behav_tc)                                                             hcbs_ther_behav_tc,
                sum(vm.hcbs_unsk_respite_tc)                                                           hcbs_unsk_respite_tc,
                sum(vm.hcbs_waiv_svc_tc)                                                               hcbs_waiv_svc_tc,
                sum(vm.ddos)                                                                           ddos,
                sum(vm.hcbs_ddos)                                                                      hcbs_ddos,
                sum(vm.icf_ddos)                                                                       icf_ddos,
                sum(vm.ip_ddos)                                                                        ip_ddos,
                sum(vm.rx_ddos)                                                                        rx_ddos,
                sum(vm.ed_ddos)                                                                        ed_ddos,
                sum(vm.snf_ddos)                                                                       snf_ddos,
                sum(vm.out_ddos)                                                                       out_ddos,
                sum(vm.pro_ddos)                                                                       pro_ddos,
                sum(vm.spfac_ddos)                                                                     spfac_ddos,
                sum(vm.amb_ddos)                                                                       amb_ddos,
                sum(vm.hh_ddos)                                                                        hh_ddos,
                sum(vm.hosp_ddos)                                                                      hosp_ddos,
                sum(vm.oth_ddos)                                                                       oth_ddos,
                sum(vm.pcp_ddos)                                                                       pcp_ddos,
                sum(vm.pulmonar_ddos)                                                                  pulmonar_ddos,
                sum(vm.copd_ddos)                                                                      copd_ddos,
                sum(vm.chf_ddos)                                                                       chf_ddos,
                sum(vm.heart_ddos)                                                                     heart_ddos,
                sum(vm.cancer_ddos)                                                                    cancer_ddos,
                sum(vm.ckd_ddos)                                                                       ckd_ddos,
                sum(vm.esrd_ddos)                                                                      esrd_ddos,
                sum(vm.hyperlipid_ddos)                                                                hyperlipid_ddos,
                sum(vm.diab_ddos)                                                                      diab_ddos,
                sum(vm.alzh_ddos)                                                                      alzh_ddos,
                sum(vm.dementia_ddos)                                                                  dementia_ddos,
                sum(vm.stroke_ddos)                                                                    stroke_ddos,
                sum(vm.hypertension_ddos)                                                              hypertension_ddos,
                sum(vm.fall_ddos)                                                                      fall_ddos,
                sum(vm.transplant_ddos)                                                                transplant_ddos,
                sum(vm.liver_ddos)                                                                     liver_ddos,
                sum(vm.hippfract_ddos)                                                                 hippfract_ddos,
                sum(vm.depression_ddos)                                                                depression_ddos,
                sum(vm.psychosis_ddos)                                                                 psychosis_ddos,
                sum(vm.drug_ddos)                                                                      drug_ddos,
                sum(vm.alcohol_ddos)                                                                   alcohol_ddos,
                sum(vm.paralysis_ddos)                                                                 paralysis_ddos
            from
                cb.vwm_elig_claims_visits_auths_mm vm
            where
                vm.bom between _bom and _eom
                and vm.mco_id = _mco_id
            group by
                1, 2, 3
        ) x;

END; $$ LANGUAGE 'plpgsql';




select
        'g' || ed.ggroup::text ||
        case when mp.dementia_ddos > 10 or mp.alzh_ddos > 10 then 'dem-1' else '' end || ' ' ||
        case when mp.paralysis_ddos > 0                    then 'para-1' else ''  end || ' ' ||
        case when mp.stroke_ddos    > 0                    then 'strk-1' else ''  end || ' ' ||
        case when age > 80              then 'a>80'
             else 'a<80' end || ' '
        --case when mp.fall_ddos > 0 then 'f1' else '' end
    BS,
    (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mp.alzh_ddos))::int alzheimer_ddos,

    count(distinct mp.member_id) ND,
    '_visit',
    avg(vm.attd_pcs_visit_hrs)::int hrs_avg,
    (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY attd_pcs_visit_hrs))::int hrs_med,
    stddev(attd_pcs_visit_hrs)::int std,
    max(attd_pcs_visit_hrs   )::int hrs_max,
    min(attd_pcs_visit_hrs   )::int hrs_min,

    '_approp' _,
    avg(vm.attd_pcs_appropriate_hrs)::int hrs_avg,
    (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY attd_pcs_appropriate_hrs))::int hrs_med,
    stddev(attd_pcs_appropriate_hrs)::int std,
    max(attd_pcs_appropriate_hrs   )::int hrs_max,
    min(attd_pcs_appropriate_hrs   )::int hrs_min

from
    vwm_mab_mbr_yr mp
    join eligibility_days ed on mp.id_date = ed.date and mp.member_id = ed.member_id and ed.mco_id = mp.mco_id and ed.is_unaligned = false
    join (
            select * from vwm_elig_claims_visits_auths_mm vm
        ) on vm.member_id = mp.member_id and vm.mco_id = mp.mco_id and vm.bom between mp.start_date and mp.end_date
where
    mp.mab_id = 5
    and ed.ggroup in (2,3)
    and attd_pcs_visit_hrs > 0
    -- and (
    --         ed.ggroup = 2 and vm.attd_pcs_appropriate_hrs > 50
    --         or
    --         ed.ggroup = 3 and vm.attd_pcs_appropriate_hrs > 10
    --     )
group by 1
order by 1






with x as (
    select ip.member_id, ip.bom,
           avg(pre.auth_attd_pcs_hrs)  as pre_monthly_hrs,
           avg(post.auth_attd_pcs_hrs) as post_monthly_hrs,
           avg(pre.auth_attd_pcs_hrs) < avg(post.auth_attd_pcs_hrs) as goes_up
    from
        vwm_elig_claims_visits_auths_mm ip
        join vwm_elig_claims_visits_auths_mm post on ip.mco_id = post.mco_id
                                                       and ip.member_id = post.member_id
                                                       and ip.bom < post.bom
                                                       and ip.bom + interval '1 year' >= post.bom
        join vwm_elig_claims_visits_auths_mm pre on ip.mco_id = pre.mco_id
                                                       and ip.member_id = pre.member_id
                                                       and ip.bom > pre.bom
                                                       and ip.bom - interval '1 year' <= pre.bom
    where
        ip.ip_tc > 0
        and post.auth_attd_pcs_hrs > 0 --nd coalesce(post.ip_tc,0) = 0
        and pre.auth_attd_pcs_hrs > 0  --and coalesce(pre.ip_tc,0) = 0
        and ip.mco_id = 2
    group by ip.member_id, ip.bom
    order by ip.member_id, ip.bom
)
select
    avg(x.post_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2) diff_avg,
    min(x.post_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2) diff_min,
    max(x.post_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2) diff_max,
    (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (x.post_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2))) diff_med
from
    x

;
with x as (
    select ip.member_id, ip.bom,
           avg(pre.auth_attd_pcs_hrs)  as pre_monthly_hrs,
           avg(post_3.auth_attd_pcs_hrs) as post3_monthly_hrs,
           avg(post_3plus.auth_attd_pcs_hrs) as post3plus_monthly_hrs
    from
        vwm_elig_claims_visits_auths_mm ip
        join vwm_elig_claims_visits_auths_mm post_3 on ip.mco_id = post_3.mco_id
                                                       and ip.member_id = post_3.member_id
                                                       and ip.bom < post_3.bom
                                                       and ip.bom + interval '3 month' >= post_3.bom
        join vwm_elig_claims_visits_auths_mm post_3plus on ip.mco_id = post_3plus.mco_id
                                                       and ip.member_id = post_3plus.member_id
                                                       and ip.bom + interval '3 month' < post_3plus.bom
                                                       and ip.bom + interval '12 month' >= post_3plus.bom
        join vwm_elig_claims_visits_auths_mm pre on ip.mco_id = pre.mco_id
                                                       and ip.member_id = pre.member_id
                                                       and ip.bom > pre.bom
                                                       and ip.bom - interval '1 year' <= pre.bom
    where
        ip.ip_tc > 0
        and ip.is_aligned = 1
        and ip.ggroup in (2,3)
        and post_3plus.ggroup in (2,3)
        and post_3.ggroup in (2,3)
        and post_3.auth_attd_pcs_hrs > 0 --nd coalesce(post.ip_tc,0) = 0
        and post_3plus.auth_attd_pcs_hrs > 0 --nd coalesce(post.ip_tc,0) = 0
        and pre.auth_attd_pcs_hrs > 0  --and coalesce(pre.ip_tc,0) = 0
        and ip.mco_id = 2
    group by ip.member_id, ip.bom
    order by ip.member_id, ip.bom
)
select
    avg(x.post3_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2) diff_avg_3,
    min(x.post3_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2) diff_min_3,
    max(x.post3_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2) diff_max_3,
    (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (x.post3_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2))) diff_med,

    '3+' _,
    avg(x.post3plus_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2) diff_avg_3plus,
    min(x.post3plus_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2) diff_min_3plus,
    max(x.post3plus_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2) diff_max_3plus,
    (PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (x.post3plus_monthly_hrs / x.pre_monthly_hrs)::decimal(16,2))) diff_med,

    count(1) n,
    count(distinct member_id) nd
from
    x
where
    --x.post3plus_monthly_hrs > x.post3_monthly_hrs
    x.pre_monthly_hrs > x.post3_monthly_hrs


with x as (
    select ip.member_id,
           ip.bom,
           avg(pre.auth_attd_pcs_hrs)                               as pre_monthly_hrs,
           avg(post.auth_attd_pcs_hrs)                              as post_monthly_hrs,
           avg(pre.auth_attd_pcs_hrs) < avg(post.auth_attd_pcs_hrs) as goes_up
    from vwm_elig_claims_visits_auths_mm ip
        join vwm_elig_claims_visits_auths_mm post on ip.mco_id = post.mco_id
        and ip.member_id = post.member_id
        and ip.bom < post.bom
        and ip.bom + interval '1 year' >= post.bom
        join vwm_elig_claims_visits_auths_mm pre on ip.mco_id = pre.mco_id
        and ip.member_id = pre.member_id
        and ip.bom > pre.bom
        and ip.bom - interval '1 year' <= pre.bom
    where ip.ip_tc > 0
      and post.auth_attd_pcs_hrs > 0 --and coalesce(post.ip_tc,0) = 0
      and pre.auth_attd_pcs_hrs > 0  --and coalesce(pre.ip_tc,0) = 0
      and ip.mco_id = 2
    group by ip.member_id, ip.bom
    order by ip.member_id, ip.bom
)
select goes_up, count(*)
from x
group by 1;



















select
    c.mco_id,
    count(distinct c.id) n,
    count(distinct c.id ) filter (where c.service_type_id in (1, 4, 11) and es.id is null) fac_no_elig,
    count(distinct c.id) filter (where es.id is not null) has_elig,
    count(distinct c.id) filter (where es.id is null) no_elig,
    (count(distinct c.id) filter (where es.id is null) * 100.0 / count(distinct c.id))::decimal(16,2) pct_no_elig,
    (count(distinct c.id ) filter (where c.service_type_id in (1, 4, 11)) * 100.0 / count(distinct c.id))::decimal(16,2) pct_fac,
    (count(distinct c.id ) filter (where c.service_type_id in (1, 4, 11) and es.id is null) * 100.0 / count(distinct c.id))::decimal(16,2) pct_fac_no_elig,
    (count(distinct c.id ) filter (where c.service_type_id in (1, 4, 11) and es.id is not null) * 100.0 / count(distinct c.id))::decimal(16,2) pct_fac_elig
from
    claims c
    left join eligibility_segments es on c.date_from between es.start_date and end_date and es.member_id = c.member_id and es.mco_id = c.mco_id
group by 1
order by 1
;


select * from ref.service_types;

---------------------------
---------------------------


select
    vcm.mco_id,
    vem.member_id is null is_empty_elig,
    case when vcm.icf_ddos > 0 or vcm.snf_ddos > 0 or vcm.ip_ddos > 0 then 1 else 0 end has_facility_ddos,
    count(*)
from vwm_claims_mm vcm
     left join vwm_eligibility_mm vem on vcm.member_id = vem.member_id and vcm.bom = vem.bom and vem.mco_id = vcm.mco_id
group by 1,2,3
order by 1,2,3;
























select
    extract(year from vec.bom)                                            yr,
    vec.has_facility_ddos,
    case when attd_pcs_hrs > 100
             then '1. >100'
         when attd_pcs_hrs > 50
             then '2. > 50'
         when attd_pcs_hrs > 20
             then '3. > 20'
         else '4. < 20' end                                               hrs_grp,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY attd_pcs_hrs)            _75_hrs,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY attd_pcs_hrs)            _50_hrs,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY attd_pcs_hrs)            _25_hrs,

    max(vec.auth_attd_pcs_util_pct)::decimal(16, 2)                       max_apc_util,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct)  _75,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct ) _50,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY auth_attd_pcs_util_pct)  _25,
    min(vec.auth_attd_pcs_util_pct)::decimal(16, 2)                       min_apc_util,
    avg(vec.auth_attd_pcs_util_pct)::decimal(16, 2)                       avg_apc_util
from
    vwm_elig_claims_visits_auths_mm vec
where
    auth_attd_pcs_util_pct <= 1
group by
    1, 2, 3
order by
    3, 2, 1



select
    vec.attd_pcs_appropriate_hrs > attd_pcs_hrs,
    count(1)
from vwm_elig_claims_visits_auths_mm vec
group by
    1


-----------------------

drop table _healthstar_mbr_adr;
create temp table _healthstar_mbr_adr as
select distinct
    mbr.patient_id,
    mbr.dob,
    ad.city,
    ad.state,
    ad.zip
from
    junk.uhc_members_20200811 mbr
    join junk.uhc_members_addresses_20200811 ad on ad.patient_id = mbr.patient_id
;
with
    member_clean_matches as (
        select
            m.id                               member_id,
            array_agg(distinct patient_id)     healthstar_ids,
            count(distinct hma.patient_id) = 1 is_clean_match,
            count(hma.patient_id)              n,
            count(distinct hma.patient_id)     nd
        from
            cb.members m
            join _healthstar_mbr_adr hma on hma.dob = m.date_of_birth and hma.city = m.city
        where m.mco_id = 2
        group by 1
    )
  , clean_ids as (
    select
        member_id,
        unnest(healthstar_ids) source_member_id
    from member_clean_matches
    where is_clean_match
)
update cb.hcbs_auths a
set
    member_id = i.member_id::bigint
from
    clean_ids i
    join junk.uhc_members_20200811 j on j.patient_id = i.source_member_id
where
    a.mco_id = 2 and
    lower(j.first_name) = a.first_name and
    lower(j.last_name) = a.last_name and
    j.dob = a.dob and
    a.evv_member_id::bigint = i.source_member_id;
update appointments_visits v
set
    member_id = a.member_id
from cb.hcbs_auths a
where
      a.mco_id = v.mco_id
  and a.evv_member_id = v.evv_member_id
  and v.source_authorization_id = a.source_authorization_id
  and v.mco_id = 2;