CREATE or replace FUNCTION cb.fn_strat_predict_impactable_costs_v3(_mco_id bigint, _risk_strat_end_date date)
    RETURNS TABLE(strat_end_date date, member_id bigint, mco_id bigint, predicted_impactable_costs numeric)
    LANGUAGE plpgsql
AS
$$
declare
-- variable declaration
begin


    -- only stratify mcos with currently active risk members
    drop table if exists _controls;
    create temporary table _controls as
    with dt as (
        -- select '2022-07-01'::date risk_strat_end_date, 4 mco_id
        -- select mc.end_date_for_ds risk_strat_end_date, mc.id mco_id from mcos mc where mc.id = 5
        select _risk_strat_end_date risk_strat_end_date, _mco_id mco_id
    )
    select
        distinct
        rm.mco_id,
        mc.name mco_name,
        _bom((_eom(risk_strat_end_date) - interval '11 month')::date) start_date,
        _eom(risk_strat_end_date                                          ) end_date
    from
        cb_risk_batch_members rm
        join dt on dt.mco_id = rm.mco_id
        join mcos mc on mc.id = rm.mco_id
    where
        rm.is_active and rm.risk_program ~* '24_7'
    ;

    drop table if exists _trained_states;
    create temporary table _trained_states as
    select Array['ma', 'tn', 'fl', 'tx', 'ks', 'ia', 'va', 'oh', 'mn', 'az'] trained_states;

    drop table if exists _issue_need_to_train_model;
    create temporary table _issue_need_to_train_model as
    select
        x.trained_states,
        mc.*,
        'CB Risk Stratification: The model is not trained on state (' || upper(mc.state) || ')!!! (Alan & Brendon should train)' message
    from
        mcos mc
        join _controls c on c.mco_id = mc.id
        left join _trained_states x on mc.state = any(x.trained_states)
    where
        x.trained_states is null
    ;

    insert into public.messages(body, message_transport_id, inserted_at, updated_at, recipient_phone_numbers)
    select
        x.message, 34 /*sms*/, now(),  now(), Array[
            '+16154808909', -- Alan
            '+19084894555', -- Brendon
            '+18568851618', -- Andrew Z
            '+16154161780', -- Mary Lou Tays
            '+18165031949'  -- Alayna Diveney
        ]
    from
        _issue_need_to_train_model x
    ;

    -- throw exceptions only if there are risk (247) members and the model isn't trained on this state
    perform _if_true_raise_exception(x.state is not null, x.message  )
    from _issue_need_to_train_model x
    ;


    -- only let members in that are 'cb_eligible' at the eom of risk_strat_date
    -- TODO: ITC has unaligned members in the risk cohort, presumably must let them in, but perhaps not all unaligned members??
    drop table if exists _ip_keepers;
    create temporary table _ip_keepers as
    select
        x.*,
        coalesce(rm.is_active,false)                                   is_active_risk_member
    from (
        select
            cc.mco_id,
            cc.mco_name,
            ed_eom.member_id,
            ed_eom.line_of_business_id in (1,3)                            is_lob_ok,
            ed_eom.ggroup > 1                                              is_group_ok,
            ed_eom.is_unaligned is false                                   is_unaligned_ok,
            extract(year from age(cc.end_date, m.date_of_birth))::int > 20 is_age_ok
        from
            _controls cc
            join eligibility_days ed_eom on ed_eom.mco_id = (select c.mco_id from _controls c) and ed_eom.date =  (select c.end_date from _controls c) -- '2022-07-31'
                -- ed_eom.mco_id = cc.mco_id and ed_eom.date = cc.end_date
            join members m on ed_eom.member_id = m.id
    ) x
    left join cb_risk_batch_members rm on rm.member_id = x.member_id and rm.mco_id = x.mco_id and rm.is_active and rm.risk_program ~* '24_7'
    where
        (
            (x.is_lob_ok and x.is_group_ok and x.is_unaligned_ok and x.is_age_ok)
            or
            coalesce(rm.is_active,false) -- let in 247 risk members
        )
    ;
    create index idx_keeper_member on _ip_keepers(member_id);

    -- build the features for the model
    drop table if exists _ip_features2;
    create temporary table _ip_features2 as
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
        _controls cc
        join members m on m.mco_id = (select c.mco_id from _controls c) -- = cc.mco_id
        join _ip_keepers k on k.member_id = m.id
        join ref.dates d on d.day between (select c.start_date from _controls c) and (select c.end_date from _controls c)
        join eligibility_days ed_eom on ed_eom.member_id = m.id and ed_eom.mco_id = (select c.mco_id from _controls c) and ed_eom.date = d.eom
        join eligibility_days ed     on ed.member_id     = m.id and ed.mco_id     = (select c.mco_id from _controls c) and ed.date = d.day
        left join claims c           on c.member_id      = m.id and  c.mco_id     = (select c.mco_id from _controls c) and c.date_from = d.day
    group by 1,2,3,4,5,6,7,8,9,10,11
    ;


    ---------------------------------------------------------------------------------------------------------------------------------------
    ---- BUILD THE MODEL FEATURES (from Brendon's code) -----------------------------------------------------------------------------------
    ---------------------------------------------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS _months;
    CREATE TEMP TABLE _months AS
    SELECT
        x.eom
      , -1 +  ROW_NUMBER() OVER (ORDER BY x.eom) i
    FROM
        (
            SELECT DISTINCT
                d.eom
            FROM
                _controls c
                JOIN ref.dates d ON d.day BETWEEN c.start_date AND c.end_date
        ) x;

    DROP TABLE IF EXISTS _month_labelled_features ;
    CREATE TEMP TABLE _month_labelled_features AS
    SELECT
        mem.member_id
      , mon.i
      , mon.eom
      , COALESCE(fa.ip_ddos     , 0)      ip_ddos
      , COALESCE(fa.snf_ddos    , 0)     snf_ddos
      , COALESCE(fa.er_ddos     , 0)      er_ddos
      , COALESCE(fa.out_ddos    , 0)     out_ddos
      , COALESCE(fa.icf_ddos    , 0)     icf_ddos
      , COALESCE(fa.hh_ddos     , 0)      hh_ddos
      , COALESCE(fa.amb_ddos    , 0)     amb_ddos
      , COALESCE(fa.hsp_ddos    , 0)     hsp_ddos
      , COALESCE(fa.pro_ddos    , 0)     pro_ddos
      , COALESCE(fa.spc_fac_ddos, 0) spc_fac_ddos
      , COALESCE(fa.dme_ddos    , 0)     dme_ddos
      , COALESCE(fa.cls_ddos    , 0)     cls_ddos
      , COALESCE(fa.hha_ddos    , 0)     hha_ddos
    FROM
        _ip_keepers mem
        CROSS JOIN _months mon
        LEFT JOIN _ip_features2 fa ON fa.member_id = mem.member_id AND mon.eom = fa.eom
    ;

    create index idx_month_labelled_features_mbid_i on _month_labelled_features(member_id, i);

    DROP TABLE IF EXISTS _mom_features ;
    create TEMP table _mom_features as
    SELECT
        mlf0.member_id
      , mlf0.ip_ddos   ip_ddos_0, mlf0.snf_ddos   snf_ddos_0, mlf0.er_ddos   er_ddos_0, mlf0.out_ddos   out_ddos_0, mlf0.icf_ddos   icf_ddos_0, mlf0.hh_ddos   hh_ddos_0, mlf0.amb_ddos   amb_ddos_0, mlf0.hsp_ddos   hsp_ddos_0, mlf0.pro_ddos   pro_ddos_0, mlf0.spc_fac_ddos   spc_fac_ddos_0, mlf0.dme_ddos   dme_ddos_0, mlf0.cls_ddos   cls_ddos_0, mlf0.hha_ddos   hha_ddos_0
      , mlf1.ip_ddos   ip_ddos_1, mlf1.snf_ddos   snf_ddos_1, mlf1.er_ddos   er_ddos_1, mlf1.out_ddos   out_ddos_1, mlf1.icf_ddos   icf_ddos_1, mlf1.hh_ddos   hh_ddos_1, mlf1.amb_ddos   amb_ddos_1, mlf1.hsp_ddos   hsp_ddos_1, mlf1.pro_ddos   pro_ddos_1, mlf1.spc_fac_ddos   spc_fac_ddos_1, mlf1.dme_ddos   dme_ddos_1, mlf1.cls_ddos   cls_ddos_1, mlf1.hha_ddos   hha_ddos_1
      , mlf2.ip_ddos   ip_ddos_2, mlf2.snf_ddos   snf_ddos_2, mlf2.er_ddos   er_ddos_2, mlf2.out_ddos   out_ddos_2, mlf2.icf_ddos   icf_ddos_2, mlf2.hh_ddos   hh_ddos_2, mlf2.amb_ddos   amb_ddos_2, mlf2.hsp_ddos   hsp_ddos_2, mlf2.pro_ddos   pro_ddos_2, mlf2.spc_fac_ddos   spc_fac_ddos_2, mlf2.dme_ddos   dme_ddos_2, mlf2.cls_ddos   cls_ddos_2, mlf2.hha_ddos   hha_ddos_2
      , mlf3.ip_ddos   ip_ddos_3, mlf3.snf_ddos   snf_ddos_3, mlf3.er_ddos   er_ddos_3, mlf3.out_ddos   out_ddos_3, mlf3.icf_ddos   icf_ddos_3, mlf3.hh_ddos   hh_ddos_3, mlf3.amb_ddos   amb_ddos_3, mlf3.hsp_ddos   hsp_ddos_3, mlf3.pro_ddos   pro_ddos_3, mlf3.spc_fac_ddos   spc_fac_ddos_3, mlf3.dme_ddos   dme_ddos_3, mlf3.cls_ddos   cls_ddos_3, mlf3.hha_ddos   hha_ddos_3
      , mlf4.ip_ddos   ip_ddos_4, mlf4.snf_ddos   snf_ddos_4, mlf4.er_ddos   er_ddos_4, mlf4.out_ddos   out_ddos_4, mlf4.icf_ddos   icf_ddos_4, mlf4.hh_ddos   hh_ddos_4, mlf4.amb_ddos   amb_ddos_4, mlf4.hsp_ddos   hsp_ddos_4, mlf4.pro_ddos   pro_ddos_4, mlf4.spc_fac_ddos   spc_fac_ddos_4, mlf4.dme_ddos   dme_ddos_4, mlf4.cls_ddos   cls_ddos_4, mlf4.hha_ddos   hha_ddos_4
      , mlf5.ip_ddos   ip_ddos_5, mlf5.snf_ddos   snf_ddos_5, mlf5.er_ddos   er_ddos_5, mlf5.out_ddos   out_ddos_5, mlf5.icf_ddos   icf_ddos_5, mlf5.hh_ddos   hh_ddos_5, mlf5.amb_ddos   amb_ddos_5, mlf5.hsp_ddos   hsp_ddos_5, mlf5.pro_ddos   pro_ddos_5, mlf5.spc_fac_ddos   spc_fac_ddos_5, mlf5.dme_ddos   dme_ddos_5, mlf5.cls_ddos   cls_ddos_5, mlf5.hha_ddos   hha_ddos_5
      , mlf6.ip_ddos   ip_ddos_6, mlf6.snf_ddos   snf_ddos_6, mlf6.er_ddos   er_ddos_6, mlf6.out_ddos   out_ddos_6, mlf6.icf_ddos   icf_ddos_6, mlf6.hh_ddos   hh_ddos_6, mlf6.amb_ddos   amb_ddos_6, mlf6.hsp_ddos   hsp_ddos_6, mlf6.pro_ddos   pro_ddos_6, mlf6.spc_fac_ddos   spc_fac_ddos_6, mlf6.dme_ddos   dme_ddos_6, mlf6.cls_ddos   cls_ddos_6, mlf6.hha_ddos   hha_ddos_6
      , mlf7.ip_ddos   ip_ddos_7, mlf7.snf_ddos   snf_ddos_7, mlf7.er_ddos   er_ddos_7, mlf7.out_ddos   out_ddos_7, mlf7.icf_ddos   icf_ddos_7, mlf7.hh_ddos   hh_ddos_7, mlf7.amb_ddos   amb_ddos_7, mlf7.hsp_ddos   hsp_ddos_7, mlf7.pro_ddos   pro_ddos_7, mlf7.spc_fac_ddos   spc_fac_ddos_7, mlf7.dme_ddos   dme_ddos_7, mlf7.cls_ddos   cls_ddos_7, mlf7.hha_ddos   hha_ddos_7
      , mlf8.ip_ddos   ip_ddos_8, mlf8.snf_ddos   snf_ddos_8, mlf8.er_ddos   er_ddos_8, mlf8.out_ddos   out_ddos_8, mlf8.icf_ddos   icf_ddos_8, mlf8.hh_ddos   hh_ddos_8, mlf8.amb_ddos   amb_ddos_8, mlf8.hsp_ddos   hsp_ddos_8, mlf8.pro_ddos   pro_ddos_8, mlf8.spc_fac_ddos   spc_fac_ddos_8, mlf8.dme_ddos   dme_ddos_8, mlf8.cls_ddos   cls_ddos_8, mlf8.hha_ddos   hha_ddos_8
      , mlf9.ip_ddos   ip_ddos_9, mlf9.snf_ddos   snf_ddos_9, mlf9.er_ddos   er_ddos_9, mlf9.out_ddos   out_ddos_9, mlf9.icf_ddos   icf_ddos_9, mlf9.hh_ddos   hh_ddos_9, mlf9.amb_ddos   amb_ddos_9, mlf9.hsp_ddos   hsp_ddos_9, mlf9.pro_ddos   pro_ddos_9, mlf9.spc_fac_ddos   spc_fac_ddos_9, mlf9.dme_ddos   dme_ddos_9, mlf9.cls_ddos   cls_ddos_9, mlf9.hha_ddos   hha_ddos_9
      , mlf10.ip_ddos ip_ddos_10, mlf10.snf_ddos snf_ddos_10, mlf10.er_ddos er_ddos_10, mlf10.out_ddos out_ddos_10, mlf10.icf_ddos icf_ddos_10, mlf10.hh_ddos hh_ddos_10, mlf10.amb_ddos amb_ddos_10, mlf10.hsp_ddos hsp_ddos_10, mlf10.pro_ddos pro_ddos_10, mlf10.spc_fac_ddos spc_fac_ddos_10, mlf10.dme_ddos dme_ddos_10, mlf10.cls_ddos cls_ddos_10, mlf10.hha_ddos hha_ddos_10
      , mlf11.ip_ddos ip_ddos_11, mlf11.snf_ddos snf_ddos_11, mlf11.er_ddos er_ddos_11, mlf11.out_ddos out_ddos_11, mlf11.icf_ddos icf_ddos_11, mlf11.hh_ddos hh_ddos_11, mlf11.amb_ddos amb_ddos_11, mlf11.hsp_ddos hsp_ddos_11, mlf11.pro_ddos pro_ddos_11, mlf11.spc_fac_ddos spc_fac_ddos_11, mlf11.dme_ddos dme_ddos_11, mlf11.cls_ddos cls_ddos_11, mlf11.hha_ddos hha_ddos_11
    FROM
        _month_labelled_features mlf0
        JOIN _month_labelled_features mlf1 ON mlf1.i = 1 AND mlf1.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf2 ON mlf2.i = 2 AND mlf2.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf3 ON mlf3.i = 3 AND mlf3.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf4 ON mlf4.i = 4 AND mlf4.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf5 ON mlf5.i = 5 AND mlf5.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf6 ON mlf6.i = 6 AND mlf6.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf7 ON mlf7.i = 7 AND mlf7.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf8 ON mlf8.i = 8 AND mlf8.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf9 ON mlf9.i = 9 AND mlf9.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf10 ON mlf10.i = 10 AND mlf10.member_id = mlf0.member_id
        JOIN _month_labelled_features mlf11 ON mlf11.i = 11 AND mlf11.member_id = mlf0.member_id
    WHERE
            mlf0.i = 0
    ;

    -- TODO: will need to implement for other states
    -- select distinct state from mcos where state not in ('va', 'oh', 'dc')


    DROP TABLE IF EXISTS _dem_features ;
    CREATE TEMP TABLE _dem_features AS
    SELECT
        m.member_id
      , CASE WHEN mc.state = 'tn'          THEN 1 ELSE 0 END is_state_TN
      , CASE WHEN mc.state = 'fl'          THEN 1 ELSE 0 END is_state_FL
      , CASE WHEN mc.state = 'va'          THEN 1 ELSE 0 END is_state_VA
      , CASE WHEN mc.state = 'tx'          THEN 1 ELSE 0 END is_state_TX
      , CASE WHEN mc.state = 'az'          THEN 1 ELSE 0 END is_state_AZ
      , CASE WHEN mc.state = 'ks'          THEN 1 ELSE 0 END is_state_KS
      , CASE WHEN mc.state in ('ia', 'mn') THEN 1 ELSE 0 END is_state_IA
      , 0                                                    is_state_MN -- only 12 training samples so far TODO: retrain when we get more data
      , CASE WHEN mc.state = 'oh'          THEN 1 ELSE 0 END is_state_OH
      , CASE WHEN mc.state = 'ma'          THEN 1 ELSE 0 END is_state_MA
      , 0                                                    is_state_dc -- only ds right now TODO: retrain when we get more data
      -----------------------------------------------------------------------------------------
      , CASE WHEN fa.line_of_business_id = 1 THEN 1 ELSE 0 END    is_lob_1
      , CASE WHEN fa.line_of_business_id = 3 THEN 1 ELSE 0 END    is_lob_3
      , CASE WHEN fa.line_of_business_id = 2 THEN 1 ELSE 0 END    is_lob_2
      , CASE WHEN fa.line_of_business_id = 8 THEN 1 ELSE 0 END    is_lob_8
      , CASE WHEN fa.ggroup = 0     THEN 1 ELSE 0 END             is_group_0
      , CASE WHEN fa.ggroup = 3     THEN 1 ELSE 0 END             is_group_3
      , CASE WHEN fa.ggroup = 2     THEN 1 ELSE 0 END             is_group_2
      , CASE WHEN fa.ggroup = 1     THEN 1 ELSE 0 END             is_group_1
      , CASE WHEN fa.ggroup = -1    THEN 1 ELSE 0 END             is_group_neg1
      , CASE WHEN fa.ggroup = 5     THEN 1 ELSE 0 END             is_group_5
      , CASE WHEN fa.ggroup = 6     THEN 1 ELSE 0 END             is_group_6
      , CASE WHEN fa.ggroup = 4     THEN 1 ELSE 0 END             is_group_4
      , CASE WHEN fa.ggroup = 8     THEN 1 ELSE 0 END             is_group_8
      , CASE WHEN fa.ggroup = 14    THEN 1 ELSE 0 END             is_group_14
      , CASE WHEN fa.ggroup = 16    THEN 1 ELSE 0 END             is_group_16
      , CASE WHEN fa.ggroup = 7     THEN 1 ELSE 0 END             is_group_7
      , CASE WHEN fa.ggroup = 11    THEN 1 ELSE 0 END             is_group_11
      , CASE WHEN fa.ggroup = 15    THEN 1 ELSE 0 END             is_group_15
      , CASE WHEN fa.ggroup = 20    THEN 1 ELSE 0 END             is_group_20
      , CASE WHEN fa.ggroup = 12    THEN 1 ELSE 0 END             is_group_12
      , CASE WHEN fa.ggroup = 13    THEN 1 ELSE 0 END             is_group_13
      , CASE WHEN fa.ggroup = 9     THEN 1 ELSE 0 END             is_group_9
      , CASE WHEN fa.ggroup = 21    THEN 1 ELSE 0 END             is_group_21
      , CASE WHEN fa.ggroup = 17    THEN 1 ELSE 0 END             is_group_17
      , CASE WHEN fa.ggroup = 18    THEN 1 ELSE 0 END             is_group_18
      , CASE WHEN fa.ggroup = 10    THEN 1 ELSE 0 END             is_group_10
      , CASE WHEN fa.gender = 'm'   THEN 1 ELSE 0 END             is_male
      , CASE WHEN fa.gender = 'f'   THEN 1 ELSE 0 END             is_female
      , fa.age                                                    age
    FROM
        _ip_keepers m
        JOIN _controls c on c.mco_id = m.mco_id
        JOIN _ip_features2 fa ON m.member_id = fa.member_id AND fa.eom = c.end_date
        join mcos mc on mc.id = m.mco_id
    ;

                -- ADH DEPRECATED : 'sagemaker-scikit-learn-2022-02-27-02-55-15-188'
                --   ip_ddos_0, ip_ddos_1, ip_ddos_2, ip_ddos_3, ip_ddos_4, ip_ddos_5, ip_ddos_6, ip_ddos_7, ip_ddos_8, ip_ddos_9, ip_ddos_10, ip_ddos_11
                -- , er_ddos_0, er_ddos_1, er_ddos_2, er_ddos_3, er_ddos_4, er_ddos_5, er_ddos_6, er_ddos_7, er_ddos_8, er_ddos_9, er_ddos_10, er_ddos_11
                -- , out_ddos_0, out_ddos_1, out_ddos_2, out_ddos_3, out_ddos_4, out_ddos_5, out_ddos_6, out_ddos_7, out_ddos_8, out_ddos_9, out_ddos_10, out_ddos_11
                -- , snf_ddos_0, snf_ddos_1, snf_ddos_2, snf_ddos_3, snf_ddos_4, snf_ddos_5, snf_ddos_6, snf_ddos_7, snf_ddos_8, snf_ddos_9, snf_ddos_10, snf_ddos_11
                -- , icf_ddos_0, icf_ddos_1, icf_ddos_2, icf_ddos_3, icf_ddos_4, icf_ddos_5, icf_ddos_6, icf_ddos_7, icf_ddos_8, icf_ddos_9, icf_ddos_10, icf_ddos_11
                -- , hh_ddos_0, hh_ddos_1, hh_ddos_2, hh_ddos_3, hh_ddos_4, hh_ddos_5, hh_ddos_6, hh_ddos_7, hh_ddos_8, hh_ddos_9, hh_ddos_10, hh_ddos_11
                -- , amb_ddos_0, amb_ddos_1, amb_ddos_2, amb_ddos_3, amb_ddos_4, amb_ddos_5, amb_ddos_6, amb_ddos_7, amb_ddos_8, amb_ddos_9, amb_ddos_10, amb_ddos_11
                -- , hsp_ddos_0, hsp_ddos_1, hsp_ddos_2, hsp_ddos_3, hsp_ddos_4, hsp_ddos_5, hsp_ddos_6, hsp_ddos_7, hsp_ddos_8, hsp_ddos_9, hsp_ddos_10, hsp_ddos_11
                -- , pro_ddos_0, pro_ddos_1, pro_ddos_2, pro_ddos_3, pro_ddos_4, pro_ddos_5, pro_ddos_6, pro_ddos_7, pro_ddos_8, pro_ddos_9, pro_ddos_10, pro_ddos_11
                -- , spc_fac_ddos_0, spc_fac_ddos_1, spc_fac_ddos_2, spc_fac_ddos_3, spc_fac_ddos_4, spc_fac_ddos_5, spc_fac_ddos_6, spc_fac_ddos_7, spc_fac_ddos_8, spc_fac_ddos_9, spc_fac_ddos_10, spc_fac_ddos_11
                -- , dme_ddos_0, dme_ddos_1, dme_ddos_2, dme_ddos_3, dme_ddos_4, dme_ddos_5, dme_ddos_6, dme_ddos_7, dme_ddos_8, dme_ddos_9, dme_ddos_10, dme_ddos_11
                -- , cls_ddos_0, cls_ddos_1, cls_ddos_2, cls_ddos_3, cls_ddos_4, cls_ddos_5, cls_ddos_6, cls_ddos_7, cls_ddos_8, cls_ddos_9, cls_ddos_10, cls_ddos_11
                -- , hha_ddos_0, hha_ddos_1, hha_ddos_2, hha_ddos_3, hha_ddos_4, hha_ddos_5, hha_ddos_6, hha_ddos_7, hha_ddos_8, hha_ddos_9, hha_ddos_10, hha_ddos_11
                -- , is_state_TN, is_state_FL, is_state_TX, is_state_KS, is_state_IA
                -- , is_lob_1, is_lob_3, is_lob_2, is_lob_8
                -- , is_group_0, is_group_3, is_group_2, is_group_1, dm.is_group_neg1, is_group_5, is_group_6, is_group_4, is_group_8, is_group_14, is_group_16, is_group_7, is_group_11, is_group_15, is_group_20, is_group_12, is_group_13, is_group_9, is_group_21, is_group_18, is_group_10
                -- , is_male
                -- , age
                -- ADH DEPRECATED 2022-10-04 : 'sagemaker-scikit-learn-2022-08-05-14-58-34-149'
                -- ip_ddos_0, ip_ddos_1, ip_ddos_2, ip_ddos_3, ip_ddos_4, ip_ddos_5, ip_ddos_6, ip_ddos_7, ip_ddos_8, ip_ddos_9, ip_ddos_10, ip_ddos_11,
                -- er_ddos_0, er_ddos_1, er_ddos_2, er_ddos_3, er_ddos_4, er_ddos_5, er_ddos_6, er_ddos_7, er_ddos_8, er_ddos_9, er_ddos_10, er_ddos_11,
                -- out_ddos_0, out_ddos_1, out_ddos_2, out_ddos_3, out_ddos_4, out_ddos_5, out_ddos_6, out_ddos_7, out_ddos_8, out_ddos_9, out_ddos_10, out_ddos_11,
                -- snf_ddos_0, snf_ddos_1, snf_ddos_2, snf_ddos_3, snf_ddos_4, snf_ddos_5, snf_ddos_6, snf_ddos_7, snf_ddos_8, snf_ddos_9, snf_ddos_10, snf_ddos_11,
                -- icf_ddos_0, icf_ddos_1, icf_ddos_2, icf_ddos_3, icf_ddos_4, icf_ddos_5, icf_ddos_6, icf_ddos_7, icf_ddos_8, icf_ddos_9, icf_ddos_10, icf_ddos_11,
                -- hh_ddos_0, hh_ddos_1, hh_ddos_2, hh_ddos_3, hh_ddos_4, hh_ddos_5, hh_ddos_6, hh_ddos_7, hh_ddos_8, hh_ddos_9, hh_ddos_10, hh_ddos_11,
                -- amb_ddos_0, amb_ddos_1, amb_ddos_2, amb_ddos_3, amb_ddos_4, amb_ddos_5, amb_ddos_6, amb_ddos_7, amb_ddos_8, amb_ddos_9, amb_ddos_10, amb_ddos_11,
                -- hsp_ddos_0, hsp_ddos_1, hsp_ddos_2, hsp_ddos_3, hsp_ddos_4, hsp_ddos_5, hsp_ddos_6, hsp_ddos_7, hsp_ddos_8, hsp_ddos_9, hsp_ddos_10, hsp_ddos_11,
                -- pro_ddos_0, pro_ddos_1, pro_ddos_2, pro_ddos_3, pro_ddos_4, pro_ddos_5, pro_ddos_6, pro_ddos_7, pro_ddos_8, pro_ddos_9, pro_ddos_10, pro_ddos_11,
                -- spc_fac_ddos_0, spc_fac_ddos_1, spc_fac_ddos_2, spc_fac_ddos_3, spc_fac_ddos_4, spc_fac_ddos_5, spc_fac_ddos_6, spc_fac_ddos_7, spc_fac_ddos_8, spc_fac_ddos_9, spc_fac_ddos_10, spc_fac_ddos_11,
                -- dme_ddos_0, dme_ddos_1, dme_ddos_2, dme_ddos_3, dme_ddos_4, dme_ddos_5, dme_ddos_6, dme_ddos_7, dme_ddos_8, dme_ddos_9, dme_ddos_10, dme_ddos_11,
                -- cls_ddos_0, cls_ddos_1, cls_ddos_2, cls_ddos_3, cls_ddos_4, cls_ddos_5, cls_ddos_6, cls_ddos_7, cls_ddos_8, cls_ddos_9, cls_ddos_10, cls_ddos_11,
                -- hha_ddos_0, hha_ddos_1, hha_ddos_2, hha_ddos_3, hha_ddos_4, hha_ddos_5, hha_ddos_6, hha_ddos_7, hha_ddos_8, hha_ddos_9, hha_ddos_10, hha_ddos_11,
                -- is_state_TN, is_state_FL, is_state_TX, is_state_KS, is_state_IA, is_state_VA,
                -- is_lob_1, is_lob_3, is_lob_2, is_lob_8,
                -- is_group_0, is_group_3, is_group_2, is_group_1, is_group_neg1, is_group_5,
                -- is_group_6, is_group_4, is_group_8, is_group_14, is_group_16, is_group_7, is_group_11, is_group_15, is_group_20, is_group_12, is_group_13, is_group_9, is_group_21, is_group_18, is_group_10,
                -- is_male, age
                -- BP DEPRECATED 2023-09-07 : 'ep-xgboost-serverless-data-v11-model-v3-2022-10-04-17-20-24'
                -- ip_ddos_0, ip_ddos_1, ip_ddos_2, ip_ddos_3, ip_ddos_4, ip_ddos_5, ip_ddos_6, ip_ddos_7, ip_ddos_8, ip_ddos_9, ip_ddos_10, ip_ddos_11,
                -- er_ddos_0, er_ddos_1, er_ddos_2, er_ddos_3, er_ddos_4, er_ddos_5, er_ddos_6, er_ddos_7, er_ddos_8, er_ddos_9, er_ddos_10, er_ddos_11,
                -- out_ddos_0, out_ddos_1, out_ddos_2, out_ddos_3, out_ddos_4, out_ddos_5, out_ddos_6, out_ddos_7, out_ddos_8, out_ddos_9, out_ddos_10, out_ddos_11,
                -- snf_ddos_0, snf_ddos_1, snf_ddos_2, snf_ddos_3, snf_ddos_4, snf_ddos_5, snf_ddos_6, snf_ddos_7, snf_ddos_8, snf_ddos_9, snf_ddos_10, snf_ddos_11,
                -- icf_ddos_0, icf_ddos_1, icf_ddos_2, icf_ddos_3, icf_ddos_4, icf_ddos_5, icf_ddos_6, icf_ddos_7, icf_ddos_8, icf_ddos_9, icf_ddos_10, icf_ddos_11,
                -- hh_ddos_0, hh_ddos_1, hh_ddos_2, hh_ddos_3, hh_ddos_4, hh_ddos_5, hh_ddos_6, hh_ddos_7, hh_ddos_8, hh_ddos_9, hh_ddos_10, hh_ddos_11,
                -- amb_ddos_0, amb_ddos_1, amb_ddos_2, amb_ddos_3, amb_ddos_4, amb_ddos_5, amb_ddos_6, amb_ddos_7, amb_ddos_8, amb_ddos_9, amb_ddos_10, amb_ddos_11,
                -- hsp_ddos_0, hsp_ddos_1, hsp_ddos_2, hsp_ddos_3, hsp_ddos_4, hsp_ddos_5, hsp_ddos_6, hsp_ddos_7, hsp_ddos_8, hsp_ddos_9, hsp_ddos_10, hsp_ddos_11,
                -- pro_ddos_0, pro_ddos_1, pro_ddos_2, pro_ddos_3, pro_ddos_4, pro_ddos_5, pro_ddos_6, pro_ddos_7, pro_ddos_8, pro_ddos_9, pro_ddos_10, pro_ddos_11,
                -- spc_fac_ddos_0, spc_fac_ddos_1, spc_fac_ddos_2, spc_fac_ddos_3, spc_fac_ddos_4, spc_fac_ddos_5, spc_fac_ddos_6, spc_fac_ddos_7, spc_fac_ddos_8, spc_fac_ddos_9, spc_fac_ddos_10, spc_fac_ddos_11,
                -- dme_ddos_0, dme_ddos_1, dme_ddos_2, dme_ddos_3, dme_ddos_4, dme_ddos_5, dme_ddos_6, dme_ddos_7, dme_ddos_8, dme_ddos_9, dme_ddos_10, dme_ddos_11,
                -- cls_ddos_0, cls_ddos_1, cls_ddos_2, cls_ddos_3, cls_ddos_4, cls_ddos_5, cls_ddos_6, cls_ddos_7, cls_ddos_8, cls_ddos_9, cls_ddos_10, cls_ddos_11,
                -- hha_ddos_0, hha_ddos_1, hha_ddos_2, hha_ddos_3, hha_ddos_4, hha_ddos_5, hha_ddos_6, hha_ddos_7, hha_ddos_8, hha_ddos_9, hha_ddos_10, hha_ddos_11,
                -- is_state_FL, is_state_IA, is_state_KS, is_state_MA, is_state_OH, is_state_TN, is_state_TX, is_state_VA,
                -- is_lob_1, is_lob_3, is_lob_2, is_lob_8,
                -- is_group_2, is_group_5, is_group_3, is_group_neg1, is_group_1, is_group_0, is_group_6, is_group_4, is_group_8, is_group_14, is_group_16, is_group_7, is_group_10, is_group_11, is_group_15, is_group_20, is_group_12, is_group_13, is_group_17, is_group_9, is_group_21, is_group_18,
                -- is_male, age

    drop table if exists _scores;
    create temporary table _scores as
    with batch_size as (
        select
            case when count(1) % 1000 = 1  then 999 else 1000 end batch_size -- this is due to a sagemaker bug that errors if the # of records in the array is 1 :: ADH 2022-10-08
        from
            _mom_features mf
            JOIN _dem_features dm ON dm.member_id = mf.member_id
    )
    SELECT
        dm.member_id
      , aws_sagemaker.invoke_endpoint(
            --
            'ep-xgboost-serverless-data-v12-model-v3-2023-09-07-01-12-42'
            , (select bs.batch_size from batch_size bs) -- this is due to a sagemaker bug that errors if the # of records in the array is 1 :: ADH 2022-10-08
            , array [ ip_ddos_0, ip_ddos_1, ip_ddos_2, ip_ddos_3, ip_ddos_4, ip_ddos_5, ip_ddos_6, ip_ddos_7, ip_ddos_8, ip_ddos_9, ip_ddos_10, ip_ddos_11,
                      er_ddos_0, er_ddos_1, er_ddos_2, er_ddos_3, er_ddos_4, er_ddos_5, er_ddos_6, er_ddos_7, er_ddos_8, er_ddos_9, er_ddos_10, er_ddos_11,
                      out_ddos_0, out_ddos_1, out_ddos_2, out_ddos_3, out_ddos_4, out_ddos_5, out_ddos_6, out_ddos_7, out_ddos_8, out_ddos_9, out_ddos_10, out_ddos_11,
                      snf_ddos_0, snf_ddos_1, snf_ddos_2, snf_ddos_3, snf_ddos_4, snf_ddos_5, snf_ddos_6, snf_ddos_7, snf_ddos_8, snf_ddos_9, snf_ddos_10, snf_ddos_11,
                      icf_ddos_0, icf_ddos_1, icf_ddos_2, icf_ddos_3, icf_ddos_4, icf_ddos_5, icf_ddos_6, icf_ddos_7, icf_ddos_8, icf_ddos_9, icf_ddos_10, icf_ddos_11,
                      hh_ddos_0, hh_ddos_1, hh_ddos_2, hh_ddos_3, hh_ddos_4, hh_ddos_5, hh_ddos_6, hh_ddos_7, hh_ddos_8, hh_ddos_9, hh_ddos_10, hh_ddos_11,
                      amb_ddos_0, amb_ddos_1, amb_ddos_2, amb_ddos_3, amb_ddos_4, amb_ddos_5, amb_ddos_6, amb_ddos_7, amb_ddos_8, amb_ddos_9, amb_ddos_10, amb_ddos_11,
                      hsp_ddos_0, hsp_ddos_1, hsp_ddos_2, hsp_ddos_3, hsp_ddos_4, hsp_ddos_5, hsp_ddos_6, hsp_ddos_7, hsp_ddos_8, hsp_ddos_9, hsp_ddos_10, hsp_ddos_11,
                      pro_ddos_0, pro_ddos_1, pro_ddos_2, pro_ddos_3, pro_ddos_4, pro_ddos_5, pro_ddos_6, pro_ddos_7, pro_ddos_8, pro_ddos_9, pro_ddos_10, pro_ddos_11,
                      spc_fac_ddos_0, spc_fac_ddos_1, spc_fac_ddos_2, spc_fac_ddos_3, spc_fac_ddos_4, spc_fac_ddos_5, spc_fac_ddos_6, spc_fac_ddos_7, spc_fac_ddos_8, spc_fac_ddos_9, spc_fac_ddos_10, spc_fac_ddos_11,
                      dme_ddos_0, dme_ddos_1, dme_ddos_2, dme_ddos_3, dme_ddos_4, dme_ddos_5, dme_ddos_6, dme_ddos_7, dme_ddos_8, dme_ddos_9, dme_ddos_10, dme_ddos_11,
                      cls_ddos_0, cls_ddos_1, cls_ddos_2, cls_ddos_3, cls_ddos_4, cls_ddos_5, cls_ddos_6, cls_ddos_7, cls_ddos_8, cls_ddos_9, cls_ddos_10, cls_ddos_11,
                      hha_ddos_0, hha_ddos_1, hha_ddos_2, hha_ddos_3, hha_ddos_4, hha_ddos_5, hha_ddos_6, hha_ddos_7, hha_ddos_8, hha_ddos_9, hha_ddos_10, hha_ddos_11,
                      is_state_az, is_state_dc, is_state_fl, is_state_ia, is_state_ks, is_state_ma, is_state_mn, is_state_oh, is_state_tn, is_state_tx, is_state_va,
                      is_lob_1, is_lob_3, is_lob_2, is_lob_8,
                      is_group_0, is_group_3, is_group_2, is_group_1, is_group_neg1, is_group_5, is_group_8, is_group_4, is_group_6, is_group_7, is_group_10, is_group_9, is_group_11,
                      is_male, is_female, age
                    ])::decimal(16,2) predicted_impactable_spend
    FROM
        _mom_features mf
        JOIN _dem_features dm ON dm.member_id = mf.member_id
    ;

    -- DROP TABLE IF EXISTS _controls;      -- cant drop this as it is used in return
    DROP TABLE IF EXISTS _trained_states;
    DROP TABLE IF EXISTS _issue_need_to_train_model;
    DROP TABLE IF EXISTS _ip_keepers;
    DROP TABLE IF EXISTS _ip_features2;
    DROP TABLE IF EXISTS _months;
    DROP TABLE IF EXISTS _month_labelled_features;
    DROP TABLE IF EXISTS _mom_features;
    DROP TABLE IF EXISTS _dem_features;
    -- DROP TABLE IF EXISTS _scores;        -- cant drop this as it is used in return

    return  query
            select
                c.end_date,
                df.member_id,
                m.mco_id,
                df.predicted_impactable_spend
            from
                _scores df
                join members m on m.id = df.member_id
                join _controls c on c.mco_id = m.mco_id
    ;

end;
$$;

ALTER FUNCTION fn_strat_predict_impactable_costs(BIGINT, DATE) OWNER TO postgres;

