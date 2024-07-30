






drop table if exists _pat_bookends;
create temporary table _pat_bookends as
select
    mdh.patient_id::bigint patient_id,
    min(effective_date     ) effective_date,
    max(mdh.expiration_date) expiration_date
from
    sure_scripts_med_history_details mdh
group by 1
;
select
    effective_date,
    expiration_date,
    count(1),
    count(distinct patient_id) nd
from _pat_bookends
group by 1,2
order by 1,2

drop table if exists _results;
create temporary table _results as
select
    patient_id,
    measure_id,
    nd_fills,
    effective_date,
    expiration_date,
    days_to_cover,
    days_covered,
    (days_covered * 1.0 /days_to_cover)::decimal(16,2) pct,
    sure_scripts_ids,
    patient_med_adherence_synth_period_ids
from (
    select
        mp.patient_id,
        mp.measure_id,
        pb.effective_date,
        pb.expiration_date,
        pb.expiration_date - min(mp.start_date) + 1 days_to_cover,
        count(distinct mp.start_date ) nd_fills,
        count(distinct d.day         ) filter ( where d.day between mp.start_date and mp.end_date ) days_covered,
        array_agg(mp.id) patient_med_adherence_synth_period_ids,
        ('{' || replace(replace(replace(array_agg(distinct mp.sure_scripts_ids::text)::text,'}',''),'{',''),'"','') || '}')::bigint[] sure_scripts_ids
    from
        _pat_bookends pb
        join prd.patient_med_adherence_synth_periods mp on pb.patient_id = mp.patient_id
        join ref.dates d on d.day between pb.effective_date and pb.expiration_date
    where
        mp.start_date between pb.effective_date and pb.expiration_date
    group by 1,2,3,4
) x
;

SELECT * FROM _results;




select
    r.measure_id,
    avg(pct)::decimal(16, 2) avg_pct,
    min(pct)::decimal(16, 2) min_pct,
    max(pct)::decimal(16, 2) max_pct
from
    _results r
where
    r.nd_fills > 1
group by 1
;

with pop as (
    select r.measure_id, count(distinct patient_id) nd from _results r group by 1
)
select
    r.measure_id,
    case when r.pct between 0 and .30 then '1. 0-30%'
         when r.pct between 0 and .60 then '2. 31-60%'
         when r.pct between 0 and .80 then '3. 61-80%'
         when r.pct between 0 and .90 then '4. 80-90%'
         when r.pct between 0 and 1.0 then '5. > 90%'
                                        else '6. fuck'
    end rng,
    count(1) n,
    count(distinct patient_id) nd,
    (count(distinct patient_id) * 1.0 / pop.nd)::decimal(16,2) pct
from
    _results r
    join pop on pop.measure_id = r.measure_id
where
    r.nd_fills > 0
group by 1,2,pop.nd
order by 1,2
;

select * from prd.patient_med_adherence_synth_period_batches btches

drop table if exists _med_adherence_outputs;
create temporary table _med_adherence_outputs as
select
    distinct
    py.name payer_name,
    py.id   payer_id,
    r.patient_id,
    r.measure_id,
    r.nd_fills,
    r.effective_date,
    r.expiration_date,
    r.days_to_cover,
    r.days_covered,
    r.pct pct_days_covered,
    rp.id rp_id,
    rp.name rp_name,
    rpo.id rpo_id,
    rpo.name rpo_name,
    p.first_name,
    p.last_name,
    p.dob,
    coalesce(mdh.sold_date, mdh.last_filled_date) rx_start_date,
    mdh.drug_description,
    mdh.days_supply,
    mdh.prescriber_first_name,
    mdh.prescriber_last_name,
    mdh.prescriber_npi
from
    _results r
    join fdw_member_doc.patients p on p.id = r.patient_id
    join fdw_member_doc.supreme_pizza sp on p.id = sp.patient_id
    join fdw_member_doc.referring_partners rp on rp.id = sp.primary_referring_partner_id
    join fdw_member_doc.payers py on sp.patient_payer_id = py.id
    join public.sure_scripts_med_history_details mdh on mdh.id = any(r.sure_scripts_ids)
                                                        and coalesce(mdh.sold_date, mdh.last_filled_date) >= '2023-01-01'::date
    left join fdw_member_doc.msh_referring_partner_organizations rpo on rpo.id = sp.primary_rpo_id
where
    sp.primary_referring_partner_id in (293, 297, 135, 143, 161, 285, 496, 310, 312, 134, 402, 403, 414, 115, 189, 329, 339, 330, 338, 356, 130, 248,
                                        300, 411, 464, 152, 239, 272, 281, 140, 133, 365, 193, 194, 276, 145, 230, 67, 262, 217, 224, 225, 213);
;


select
    case when doo.alll then '1. All'
                           else mo.measure_id
    end grpr,
    count(1) n,
    count(distinct mo.patient_id) nd
from
    _med_adherence_outputs mo
    cross join (select unnest(array[true,false]) alll) doo
where
    mo.nd_fills > 0
    and mo.pct_days_covered < 0.8
group by 1

with mds as (
    select
        p.id patient_id,
        md.full_name md_full_name
    from
        fdw_member_doc.patients p
        join fdw_member_doc.patient_contacts pc on p.id = pc.patient_id and pc.relationship = 'physician' and pc.is_primary and pc.status = 'active'
        join fdw_member_doc.msh_physicians md on md.contact_id = pc.contact_id
)
select
    mo.*,
    mds.md_full_name msh_physician_name
from
    _med_adherence_outputs mo
    left join mds on mds.patient_id = mo.patient_id
where
    mo.nd_fills > 1
    and mo.pct_days_covered < 0.8
order by 1,2,3,4,5




drop table if exists _med_adherence_outputs_compare;
create temporary table _med_adherence_outputs_compare as
select
    distinct
    py.name payer_name,
    py.id   payer_id,
    r.patient_id,
    r.measure_id,
    r.nd_fills,
    r.effective_date,
    r.expiration_date,
    r.days_to_cover,
    r.days_covered,
    r.pct pct_days_covered,
    rp.id rp_id,
    rp.name rp_name,
    rpo.id rpo_id,
    rpo.name rpo_name,
    p.first_name,
    p.last_name,
    p.dob
from
    _results r
    join fdw_member_doc.patients p on p.id = r.patient_id
    join fdw_member_doc.supreme_pizza sp on p.id = sp.patient_id
    join fdw_member_doc.referring_partners rp on rp.id = sp.primary_referring_partner_id
    join fdw_member_doc.payers py on sp.patient_payer_id = py.id
    left join fdw_member_doc.msh_referring_partner_organizations rpo on rpo.id = sp.primary_rpo_id
where
    r.nd_fills > 1
    and r.pct < 0.8
;


select * from raw.humana_rx_red_list;
select * from raw.elevance_rx_red_list;
select * from raw.uhc_rx_red_list;


select distinct measure_id from _med_adherence_outputs_compare mao
with total as (
    --select mao.patient_id, mao.measure_id from _med_adherence_outputs_compare mao
    --union
    select mao.patient_id, mao.our_measure measure_id  from junk.red_list_upload_2023_03 mao where mao.our_measure in ('PDC-DR','PDC-RASA','PDC-STA')
)
select
    t.measure_id,
    count(distinct t.patient_id) nd_rl_patients,
    count(distinct r.patient_id) nd_our_patients,
    count(distinct t.patient_id) filter ( where r.nd_fills > 1 and r.pct < 0.8 ) our_red_list,
    count(distinct t.patient_id) filter ( where not (r.nd_fills > 1 and r.pct < 0.8) ) nd_not_our_red_list,
    (count(distinct t.patient_id) filter ( where r.nd_fills > 1 and r.pct < 0.8 ) * 1.0 / count(distinct r.patient_id))::decimal(16,2) pct_agreement,
    '__' _,
    count(distinct t.patient_id) - count(distinct r.patient_id  ) nd_pat_not_associated_to_measure,
    count(distinct r.patient_id) filter ( where (r.nd_fills < 2) and r.pct < 0.8   ) nd_pat_not_have_2_fills,
    count(distinct r.patient_id) filter ( where (r.pct >= 0.8  ) and r.nd_fills > 1) nd_gt_80pct_cvg,
    count(distinct r.patient_id) filter ( where (r.pct >= 0.8  ) and r.nd_fills < 2) nd_neither_2fills_nor_gt80
from
    total t
    left join junk.red_list_upload_2023_03 rl on rl.patient_id = t.patient_id and rl.our_measure = t.measure_id
    left join _results r on r.patient_id = t.patient_id and r.measure_id = t.measure_id
group by 1


------------------------------------------------------------------------------------------------------------------------
/* TODO Question # 2 */
-- SELECT * FROM raw.patient_rx_adherence_roster_uhc r JOIN integrations.mco_patients mp on mp.payer_id = '' and mp ;
------------------------------------------------------------------------------------------------------------------------

SELECT * FROM junk.red_list_upload_2023_03 ; --1532
SELECT count(*) FROM _mco_red_list where patient_id ISNULL ; -- 2402 (280 null)
SELECT * FROM ;
;

------------------------------------------------------------------------------------------------------------------------
/* Question # 1 */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _mco_red_list;
CREATE TEMP TABLE _mco_red_list (
    mco        TEXT,
    patient_id BIGINT,
    measure_id TEXT,
    pat_key    TEXT
);


CREATE UNIQUE INDEX ON _mco_red_list(mco, patient_id, measure_id);

DROP TABLE IF EXISTS _mco_measure_mappings;
CREATE TEMP TABLE _mco_measure_mappings AS
SELECT *
FROM
    ( VALUES
          ('elevance', 'HTN', 'PDC-RASA'),
          ('elevance', 'STATIN', 'PDC-STA'),
          ('elevance', 'DM', 'PDC-DR'),
          ('humana', 'ADH-DIABETES', 'PDC-DR'),
          ('humana', 'ADH-STATINS', 'PDC-STA'),
          ('humana', 'ADH-ACE/ARB', 'PDC-RASA') ) x(mco, mco_measure, our_measure);
CREATE UNIQUE INDEX ON _mco_measure_mappings(mco, mco_measure, our_measure);



-- elevance missing counts
WITH
    overrides AS ( SELECT *
                   FROM
                       ( VALUES
                             ('WANDA HACKER 1952-01-03', 239668),
                             ('LARRY BRYANT 1946-04-06', 240460) ) x(name_dob, patient_id) )
SELECT
    COUNT(el.member_name_dob)            n_hp
  , COUNT(DISTINCT el.member_name_dob)   nd_hp
  , COUNT(COALESCE(ov.patient_id, p.id)) n_coop
  , COUNT(distinct COALESCE(ov.patient_id, p.id)) nd_coop
FROM
    raw.elevance_rx_red_list el
    LEFT JOIN overrides ov ON el.member_name_dob = ov.name_dob
    LEFT JOIN fdw_member_doc.patients p
              ON el.member_name_dob = UPPER(p.first_name || ' ' || p.last_name || ' ' || p.dob) AND
                 p.status != 'hard_delete'
                  AND ov.patient_id ISNULL;

DROP TABLE IF EXISTS _elevance_measures;
CREATE TEMP TABLE _elevance_measures AS
WITH
    overrides               AS ( SELECT *
                                 FROM
                                     ( VALUES
                                           ('WANDA HACKER 1952-01-03', 239668),
                                           ('LARRY BRYANT 1946-04-06', 240460) ) x(name_dob, patient_id) )
  , unnest_with_patient_ids AS ( SELECT
                                     COALESCE(ov.patient_id, p.id)                                      patient_id

                                   , UNNEST(REGEXP_SPLIT_TO_ARRAY(el.non_adherent_previous_year, '\|')) elev_measure
                                   , el.*
                                   , p.first_name
                                   , p.last_name
                                   , p.dob
                                 FROM
                                     raw.elevance_rx_red_list el
                                     LEFT JOIN overrides ov ON el.member_name_dob = ov.name_dob
                                     LEFT JOIN fdw_member_doc.patients p
                                               ON el.member_name_dob =
                                                  UPPER(p.first_name || ' ' || p.last_name || ' ' || p.dob) AND
                                                  p.status != 'hard_delete'
                                                   AND ov.patient_id ISNULL )

SELECT
    mmm.our_measure
  , upi.*
FROM
    unnest_with_patient_ids upi
    LEFT JOIN _mco_measure_mappings mmm ON mmm.mco = 'elevance' AND mmm.mco_measure = upi.elev_measure
WHERE
    NULLIF(elev_measure, '') IS NOT NULL
;

INSERT INTO _mco_red_list (mco, patient_id, measure_id, pat_key)
    select 'elevance', patient_id, our_measure, member_name_dob
    from _elevance_measures
-- WHERE patient_id is not null
;
SELECT * FROM _mco_red_list where mco = 'elevance';


-- humana missing 12
SELECT
     mp.patient_id
     , mmm.our_measure
     , r.*
FROM
    raw.humana_rx_red_list r
    LEFT JOIN integrations.mco_patients mp ON mp.payer_id = r.payer_id
        AND mp.mco_member_id = r.humana_patient_id
   left join _mco_measure_mappings mmm on mmm.mco = 'humana' and mmm.mco_measure = r.measure
WHERE
    r.humana_patient_id <> ''
and mp.patient_id isnull
    ;

INSERT
INTO
    _mco_red_list (mco, patient_id, measure_id, pat_key)
SELECT distinct
    'humana'
     , mp.patient_id
     , mmm.our_measure
, r.patient_name
FROM
    raw.humana_rx_red_list r
    LEFT JOIN integrations.mco_patients mp ON mp.payer_id = r.payer_id
        AND mp.mco_member_id = r.humana_patient_id
   left join _mco_measure_mappings mmm on mmm.mco = 'humana' and mmm.mco_measure = r.measure
where r.humana_patient_id <> ''
;

-- delete FROM _mco_red_list where mco = 'humana';
SELECT * FROM _mco_red_list where mco = 'humana';


--UHC

SELECT
    COUNT(CONCAT(red.first_name, red.last_name, red.dob))
  , COUNT(p.id)
FROM
    raw.uhc_rx_red_list red
    LEFT JOIN fdw_member_doc.patients p
              ON red.first_name = UPPER(p.first_name)
                  AND red.last_name = UPPER(p.last_name)
                  AND red.dob::DATE = p.dob
;

-- SELECT mam.measure_id, mam.value_set_id, vs.*
-- FROM
--     ref.med_adherence_value_sets vs
-- join ref.med_adherence_measures mam on mam.value_set_id = vs.value_set_id
-- WHERE
--     value_set_item ~* 'ATORVASTATIN';
;
--958 in red list, --841 in patients has dupes
--908 distinct pts in red list, 797 distinct pt ids
DROP TABLE IF EXISTS _mapped_uhc;
CREATE TEMP TABLE _mapped_uhc AS
    select p.id patient_id, red.* from (
        SELECT first_name, last_name, dob, 'PDC-DR' measure_id FROM raw.uhc_rx_red_list red where mad_2022 = 'Y'   UNION
        SELECT first_name, last_name, dob, 'PDC-RASA' measure_id FROM raw.uhc_rx_red_list red where mah_2022 = 'Y' UNION
        SELECT first_name, last_name, dob, 'PDC-STA' measure_id FROM raw.uhc_rx_red_list red where mac_2022 = 'Y'
    ) red
    left JOIN fdw_member_doc.patients p
    ON red.first_name = UPPER(p.first_name)
    AND red.last_name = UPPER(p.last_name)
    AND red.dob::date = p.dob
    AND p.status <> 'hard_delete'
    ;

-- missing uhc
SELECT red.*
FROM raw.uhc_rx_red_list red
LEFT JOIN fdw_member_doc.patients p
ON (red.first_name = UPPER(p.first_name)
    AND red.last_name = UPPER(p.last_name)
    AND red.dob::date = p.dob
    )
WHERE p.id is null
    ;

INSERT
INTO
    _mco_red_list (mco, patient_id, measure_id, pat_key)
SELECT 'uhc', patient_id, measure_id, first_name || last_name || dob
FROM
    _mapped_uhc;

SELECT
    mco
  , measure_id
  , COUNT(*)                             n
  , COUNT(patient_id)                    n_pats
  , (COUNT(patient_id) * 100.0 / COUNT(*))::DECIMAL(5,2) pct_pats_found
FROM
    _mco_red_list
GROUP BY
    1, 2
ORDER BY
    mco, measure_id
;


    ;
DROP TABLE IF EXISTS _pat_measure_super_set;
CREATE TEMP TABLE _pat_measure_super_set AS
SELECT
    mco
  , patient_id
  , measure_id
  , pat_key
FROM
    _mco_red_list mrl
UNION
SELECT
    pay.name mco
  , r.patient_id
  , r.measure_id
  , p.first_name || p.last_name || p.dob::TEXT
FROM
    _results r
    JOIN fdw_member_doc.patients p ON p.id = r.patient_id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = r.patient_id
    JOIN fdw_member_doc.payers pay ON pay.id = sp.patient_payer_id
;
SELECT mco, count(*)
FROM
     _pat_measure_super_set GROUP BY 1;

SELECT *
FROM
    _pat_measure_super_set pmss
    LEFT JOIN _mco_red_list mrl ON pmss.measure_id = mrl.measure_id
        AND (
                                               pmss.patient_id = mrl.patient_id
                                           OR (pmss.patient_id ISNULL AND pmss.pat_key = mrl.pat_key AND pmss.mco = mrl.mco)
                                       )
where mrl.patient_id ISNULL  and mrl.mco is not null
;

-- SELECT * FROM _red_list_super_set;
--
-- create table junk.red_list_super_set_20230321 as
-- SELECT *
-- FROM
--     _red_list_super_set;

    DROP TABLE IF EXISTS _red_list_super_set;
CREATE TEMP TABLE _red_list_super_set AS
WITH
    _one AS ( SELECT
                  pmss.mco
                , pmss.patient_id
                , pmss.measure_id
                , pmss.pat_key
                , mrl.pat_key IS NOT NULL                                                is_mco_red_list
                , sspp.first_name
                , sspp.last_name
                , sspp.dob
                , r.measure_id IS NOT NULL                                               has_measure_med_in_our_data
                , r.nd_fills > 1 AND r.pct < .80                                         has_measure_our_data
                , r.nd_fills
                , r.pct
                , r.sure_scripts_ids
                , MAX(sspp.id) IS NOT NULL                                               sure_scripts_sent
                , MAX(ssmhd.id) IS NOT NULL                                              sure_scripts_returned_data
                , ARRAY_AGG(DISTINCT ssmhd.note) FILTER ( WHERE ssmhd.note IS NOT NULL ) sure_scripts_notes
              FROM
                  _pat_measure_super_set pmss
                  LEFT JOIN _mco_red_list mrl ON pmss.measure_id = mrl.measure_id
                      AND (
                                                             pmss.patient_id = mrl.patient_id
                                                         OR (pmss.patient_id ISNULL AND pmss.pat_key = mrl.pat_key)
                                                     )
                  LEFT JOIN sure_scripts_panel_patients sspp ON sspp.patient_id = pmss.patient_id
                  LEFT JOIN sure_scripts_med_history_details ssmhd ON sspp.patient_id = ssmhd.patient_id::BIGINT
                  LEFT JOIN _results r ON pmss.patient_id = r.patient_id AND r.measure_id = pmss.measure_id
              GROUP BY
                  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 )
SELECT
    o.mco
  , o.patient_id
  , o.measure_id
  , o.pat_key
  , o.first_name
  , o.last_name
  , o.dob
  , o.has_measure_med_in_our_data
  , o.has_measure_our_data
  , o.nd_fills
  , o.pct
  , o.sure_scripts_sent
  , o.sure_scripts_returned_data
  , o.sure_scripts_notes
  , o.is_mco_red_list
  , ARRAY_AGG(DISTINCT mhd.product_code) meds
  , ARRAY_AGG(distinct mhd.pharmacy_name)         pharmacies
  , ARRAY_AGG(distinct mhd.prescriber_name)       prescribers
  , MAX(sold_date)                       sold_date
  , MAX(last_filled_date)                last_filled_date
FROM
    _one o
    LEFT JOIN sure_scripts_med_history_details mhd ON mhd.id = ANY (o.sure_scripts_ids)
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15;

SELECT *
FROM
    _red_list_super_set;
------------------------------------------------------------------------------------------------------------------------
/*  */
-----------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    sure_scripts_med_history_details
WHERE
    patient_id = '79588';

SELECT *
FROM
    prd.patient_med_adherence_synth_periods
WHERE
    patient_id = '79588';

SELECT *
FROM
    _red_list_super_set

WHERE
        patient_id = '79588';
;
