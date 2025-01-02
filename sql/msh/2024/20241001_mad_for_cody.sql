drop table if exists _temp_nephrology_claims;
create temp table _temp_nephrology_claims as
select
    cc.member_id,
    cc.payer_id,
    cc.date_from,
    cc.place_of_service_id,
    p.npi,
    p.provider_name,
    max(case when pt.npi is not null then 1 else 0 end) as nephrology_provider_flag
from
    dh.claim_category cc

    left join
        prd.providers p
        on cc.servicing_provider_id = p.id

    left join
        (select distinct npi from
        dh.provider_type_mapping pt
            where pt.provider_type = 'PRACTITIONER - NEPHROLOGY') pt
        on p.npi = pt.npi

where
    cc.date_from >= '1/1/2023'
    and cc.place_of_service_id in (11,12,13,19,21,22,31,32,33,34,49,50,65,71,72)

group by 1,2,3,4,5,6
having max(case when pt.npi is not null then 1 else 0 end) = 1
;

drop table if exists _temp_neph_patients;
create temp table _temp_neph_patients as
    select distinct member_id, payer_id, array_agg(distinct provider_name order by provider_name) as nephrologists_seen
    from _temp_nephrology_claims
    group by 1,2
;

drop table if exists _temp_neph_patients_geo;
create temp table _temp_neph_patients_geo as
    select
        ma.member_id,
        ma.payer_id,
        (array_agg(ma.city order by ma.input_id desc))[1] as city,
        (array_agg(ma.state order by ma.input_id desc))[1] as state,
        (array_agg(ma.zip order by ma.input_id desc))[1] as zip
    from
        prd.member_addresses ma

    where
        ma.member_id in (select member_id from _temp_neph_patients)

group by 1,2;

drop table if exists _temp_neph_patients_pz_attributes;
create temp table _temp_neph_patients_pz_attributes as
    select
        p.member_id,
        p.payer_id,
        pz.patient_id,
        m.first_name,
        m.last_name,
        m.date_of_birth,
        pz.attributed_rpo_id,
        pz.risk_rpo_id,
        pz.primary_rpo_id,
        coalesce(pz.attributed_rpo_id, pz.risk_rpo_id,pz.primary_rpo_id) as rpo_id_to_use,
        rpo.name as rpo_name,
        pz.patient_status,
        pz.patient_substatus,
        pz.attribution_status,
        pz.attribution_substatus,
        g.city,
        g.state,
        g.zip,
        p.nephrologists_seen
    from
        _temp_neph_patients p

        left join
            gmm.global_mco_member_mappings gmm
            on p.member_id = gmm.member_id
            and p.payer_id = gmm.payer_id

        left join
            gmm.global_members gm
            on gmm.global_member_id = gm.id

        left join
            fdw_member_doc.supreme_pizza pz
            on gm.patient_id = pz.patient_id

        left join
            fdw_member_doc.msh_referring_partner_organizations rpo
            on coalesce(pz.attributed_rpo_id, pz.risk_rpo_id,pz.primary_rpo_id) = rpo.id

        left join
            prd.members m
            on p.member_id = m.id
            and p.payer_id = m.payer_id

        left join
            _temp_neph_patients_geo g
            on p.member_id = g.member_id
            and p.payer_id = g.payer_id
;

SELECT
    n.member_id
  , n.payer_id
  , n.patient_id
  , n.first_name
  , n.last_name
  , n.date_of_birth
  , n.rpo_id_to_use                                      AS rpo_id
  , n.rpo_name
  , n.patient_status
  , n.patient_substatus
  , n.attribution_status
  , n.attribution_substatus
  , n.city
  , n.state
  , n.zip
  , n.nephrologists_seen
  , qpm.id IS NOT NULL                                      has_mad_measure
  , m.pdc_to_date
  , m.is_excluded
  , m.absolute_fail_date
  , m.adr
  , COUNT(DISTINCT CONCAT(c.member_id, c.payer_id, c.date_from, c.npi))
    FILTER ( WHERE EXTRACT(YEAR FROM date_from) = 2023 ) AS neph_visits_2023
  , COUNT(DISTINCT CONCAT(c.member_id, c.payer_id, c.date_from, c.npi))
    FILTER ( WHERE EXTRACT(YEAR FROM date_from) = 2024 ) AS neph_visits_2024

FROM
    _temp_neph_patients_pz_attributes n
    LEFT JOIN
        _temp_nephrology_claims c
        ON n.member_id = c.member_id
            AND n.payer_id = c.payer_id
    LEFT JOIN fdw_member_doc.qm_patient_measures qpm ON qpm.patient_id = n.patient_id
        AND qpm.measure_key = 'med_adherence_diabetes'
        AND qpm.operational_year = 2024
   left join fdw_member_doc.qm_pm_med_adh_metrics m on m.patient_measure_id = qpm.id
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17;

