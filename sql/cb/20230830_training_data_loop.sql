select get_all_foo();
CREATE OR REPLACE FUNCTION get_all_foo() RETURNS void AS
$BODY$
DECLARE
    _mcos RECORD;
BEGIN
create temporary table _controls as
SELECT
    m.id                                                 mco_id
  , name                                                 mco_name
  , GREATEST(m.claims_start_date, '2017-01-01'::DATE)    start_date
  , COALESCE(m.claims_end_date, m.end_date_for_ds::DATE) end_date
FROM
    mcos m
WHERE
    m.claims_start_date IS NOT NULL
;

    FOR _mcos IN
        select mco_id from _controls ctrls where not exists(select 1 from junk.ip_features2_new n where n.mco_id = ctrls.mco_id)
    LOOP
        RAISE NOTICE '___MCO_ID___%', _mcos.mco_id;
        insert into junk.ip_features2_new
        select
            cc.mco_id,
            cc.mco_name,
            d.eom,
            ed_eom.member_id,
            ed_eom.line_of_business_id,
            ed_eom.ggroup,
            ed_eom.is_unaligned,
            extract(year from age(d.eom, m.date_of_birth))::int age,
            m.gender,
            1 cwmm,
            d.days_in_month,
            (count(distinct ed.id) * 1.0 / d.days_in_month)::decimal(16,2) cpmm,
            sum(paid_amount) filter (  where is_rx                )  rx_tc,
            sum(paid_amount) filter (  where service_type_id = 0  )  other_tc,
            sum(paid_amount) filter (  where service_type_id = 1  )  ip_tc,
            sum(paid_amount) filter (  where service_type_id = 2  )  er_tc,
            sum(paid_amount) filter (  where service_type_id = 3  )  out_tc,
            sum(paid_amount) filter (  where service_type_id = 4  )  snf_tc,
            sum(paid_amount) filter (  where service_type_id = 11 )  icf_tc,
            sum(paid_amount) filter (  where service_type_id = 5  )  hh_tc,
            sum(paid_amount) filter (  where service_type_id = 6  )  amb_tc,
            sum(paid_amount) filter (  where service_type_id = 7  )  hsp_tc,
            sum(paid_amount) filter (  where service_type_id = 8  )  pro_tc,
            sum(paid_amount) filter (  where service_type_id = 9  )  spc_fac_tc,
            sum(paid_amount) filter (  where service_type_id = 12 )  dme_tc,
            sum(paid_amount) filter (  where service_type_id = 13 )  cls_tc,
            sum(paid_amount) filter (  where service_type_id = 18 )  hha_tc,
            sum(paid_amount) filter (  where service_type_id = 17 )  hcbs_attdpcs_tc,
            sum(paid_amount) filter (  where service_type_id = 10 )  hcbs_other_tc,
            sum(paid_amount) filter (  where service_type_id = 15 )  hcbs_support_house_tc,
            sum(paid_amount) filter (  where service_type_id = 16 )  hcbs_adult_day_tc,

            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 1  )  ip_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 2  )  er_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 3  )  out_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 4  )  snf_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 11 )  icf_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 5  )  hh_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 6  )  amb_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 7  )  hsp_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 8  )  pro_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 9  )  spc_fac_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 12 )  dme_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 13 )  cls_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 18 )  hha_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 17 )  hcbs_attdpcs_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 10 )  hcbs_other_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 15 )  hcbs_support_house_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 16 )  hcbs_adult_day_ddos_span,
            sum((coalesce(c.date_to,c.date_from) - c.date_from + 1)) filter (  where service_type_id = 0  )  other_ddos_span,

            count(distinct c.date_from) filter (  where service_type_id = 1  )  ip_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 2  )  er_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 3  )  out_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 4  )  snf_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 11 )  icf_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 5  )  hh_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 6  )  amb_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 7  )  hsp_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 8  )  pro_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 9  )  spc_fac_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 12 )  dme_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 13 )  cls_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 18 )  hha_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 17 )  hcbs_attdpcs_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 10 )  hcbs_other_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 15 )  hcbs_support_house_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 16 )  hcbs_adult_day_ddos,
            count(distinct c.date_from) filter (  where service_type_id = 0  )  other_ddos
        from
            _controls  cc
            join members m on m.mco_id = cc.mco_id
            join junk.ip_keepers_new k on k.member_id = m.id
            join ref.dates d on d.day between cc.start_date and cc.end_date
            join eligibility_days ed_eom on ed_eom.member_id = m.id and ed_eom.mco_id = cc.mco_id and ed_eom.date = d.eom
            join eligibility_days ed     on ed.member_id     = m.id and ed.mco_id     = cc.mco_id and ed.date = d.day
            left join claims c           on c.member_id      = m.id and  c.mco_id     = cc.mco_id and c.date_from = d.day
        where
            cc.mco_id = _mcos.mco_id
        group by 1,2,3,4,5,6,7,8,9,10,11
    ;
    commit;
    END LOOP;
    RETURN;
END;
$BODY$
LANGUAGE plpgsql;


