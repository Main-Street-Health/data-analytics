-- create table junk.patient_med_adherence_red_list_bak_20230324 as select * from prd.patient_med_adherence_red_list;
-- SELECT count(*) FROM prd.patient_med_adherence_red_list;
-- SELECT count(*) FROM junk.patient_med_adherence_red_list_bak_20230324;

-- SELECT count(*) FROM prd.patient_med_adherence_measures;
-- SELECT count(*) FROM junk.patient_med_adherence_measures_bak_20230324;

-- DROP TABLE IF EXISTS prd.patient_med_adherence_red_list;
CREATE TABLE prd.patient_med_adherence_red_list (
    id                                     BIGSERIAL PRIMARY KEY,
    patient_id                             BIGINT    NOT NULL,
    mco_id                                 BIGINT,
    measure_id                             TEXT      NOT NULL,
    year                                   INT       NOT NULL,
    is_mco_red_list                        bool      NOT NULL DEFAULT FALSE,
    is_sure_scripts_red_list               bool      NOT NULL DEFAULT FALSE,
    sure_scripts_ids                       BIGINT[],
    patient_med_adherence_synth_period_ids BIGINT[],
    days_supply                            INT,
    inserted_at                            TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at                             TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX ON prd.patient_med_adherence_red_list(patient_id);
CREATE INDEX ON prd.patient_med_adherence_red_list(measure_id);
CREATE UNIQUE INDEX ON prd.patient_med_adherence_red_list(year, measure_id, patient_id);

CREATE TABLE prd.patient_med_adherence_measures (
    id                                          BIGSERIAL PRIMARY KEY,
    patient_id                                  BIGINT                  NOT NULL,
    mco_id                                      BIGINT,
    measure_id                                  TEXT                    NOT NULL,
    year                                        INTEGER                 NOT NULL,
    fill_count                                  INTEGER,
    ipsd                                        DATE,
    next_fill_date                              DATE,
    absolute_fail_date                          DATE,
    sure_scripts_ids                            BIGINT[],
    patient_med_adherence_synth_period_ids      BIGINT[],
    patient_med_adherence_synth_period_batch_id BIGINT,
--     uhc_measure_ids bigint[],
    is_mco_measure                              BOOLEAN   DEFAULT FALSE NOT NULL,
    is_sure_scripts_measure                     BOOLEAN   DEFAULT FALSE NOT NULL,
    inserted_at                                 TIMESTAMP DEFAULT NOW() NOT NULL,
    updated_at                                  TIMESTAMP DEFAULT NOW() NOT NULL
);

-- alter table prd.patient_med_adherence_measures add uhc_measure_ids bigint[];

CREATE INDEX ON prd.patient_med_adherence_measures(patient_id);
CREATE INDEX ON prd.patient_med_adherence_measures(measure_id);
CREATE UNIQUE INDEX ON prd.patient_med_adherence_measures(year, measure_id, patient_id);

SELECT *
FROM
    prd.patient_med_adherence_measures where is_mco_measure;
------------------------------------------------------------------------------------------------------------------------
/* Lists are specific to pilot locations */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _pilot_rpl_ids;
CREATE TEMP TABLE _pilot_rpl_ids AS
SELECT
    rp.id, rp.name
FROM
    fdw_member_doc.referring_partners rp
WHERE
        rp.id IN
        (293, 297, 135, 143, 161, 285, 496, 310, 312, 134, 402, 403, 414, 115, 189, 329, 339, 330, 338, 356, 130, 248,
         300, 411, 464, 152, 239, 272, 281, 140, 133, 365, 193, 194, 276, 145, 230, 67, 262, 217, 224, 225, 213);

SELECT * FROM _pilot_rpl_ids;

------------------------------------------------------------------------------------------------------------------------
/* Red list
  MCO logic same as previous analysis
*/
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _mco_red_list;
CREATE TEMP TABLE _mco_red_list (
    mco        TEXT,
    mco_id bigint,
    patient_id BIGINT,
    measure_id TEXT,
    days_supply int,
    year int,
    source text,
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

SELECT * FROM _elevance_measures where patient_id ISNULL ;

INSERT INTO _mco_red_list (mco, mco_id, patient_id, measure_id, year, source, pat_key)
    select 'elevance', 2,  patient_id, our_measure, 2022, 'mco', member_name_dob
    from _elevance_measures
WHERE patient_id is not null
;
SELECT * FROM _mco_red_list where mco = 'elevance';



INSERT
INTO
    _mco_red_list (mco, mco_id, patient_id, measure_id, year, source, pat_key, days_supply)
SELECT distinct
    'humana'
              , 44
     , mp.patient_id
     , mmm.our_measure
              , 2022
              , 'mco'
, r.patient_name
, r.day_supply::int
FROM
    raw.humana_rx_red_list r
-- select * from raw.humana_rx_red_list r order by patient_name, measure
    LEFT JOIN integrations.mco_patients mp ON mp.payer_id = r.payer_id
        AND mp.mco_member_id = r.humana_patient_id
   left join _mco_measure_mappings mmm on mmm.mco = 'humana' and mmm.mco_measure = r.measure
where r.humana_patient_id <> ''
and mp.patient_id is not null
;

-- delete FROM _mco_red_list where mco = 'humana';
SELECT * FROM _mco_red_list where mco = 'humana';


--UHC


DROP TABLE IF EXISTS _mapped_uhc;
CREATE TEMP TABLE _mapped_uhc AS
    select p.id patient_id, red.* from (
        SELECT first_name, last_name, dob, mad_days_supply days_supply, 'PDC-DR' measure_id FROM raw.uhc_rx_red_list red where mad_2022 = 'Y'   UNION
        SELECT first_name, last_name, dob, mah_days_supply days_supply, 'PDC-RASA' measure_id FROM raw.uhc_rx_red_list red where mah_2022 = 'Y' UNION
        SELECT first_name, last_name, dob, mac_days_supply days_supply, 'PDC-STA' measure_id FROM raw.uhc_rx_red_list red where mac_2022 = 'Y'
    ) red
    left JOIN fdw_member_doc.patients p
    ON red.first_name = UPPER(p.first_name)
    AND red.last_name = UPPER(p.last_name)
    AND red.dob::date = p.dob
    AND p.status <> 'hard_delete'
    ;


INSERT
INTO
    _mco_red_list (mco, mco_id, patient_id, measure_id, year, source, pat_key, days_supply)
SELECT 'uhc', 47, patient_id, measure_id, 2022, 'mco', first_name || last_name || dob,m.days_supply::int
FROM
    _mapped_uhc m
where m.patient_id is not null
;

SELECT * FROM _mco_red_list;


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
order by 1,2;

drop table if exists _our_red_list;
create temporary table _our_red_list as
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
    and mp.batch_id = 2
    group by 1,2,3,4
) x
;


-- insert mco red list
INSERT
INTO
    prd.patient_med_adherence_red_list (patient_id, mco_id, measure_id, year, is_mco_red_list, days_supply)
SELECT
    patient_id
  , mco_id
  , measure_id
  , 2022 yr
  , TRUE is_mco_red_list
, days_supply
FROM
    _mco_red_list
    ;


-- upsert ss red list
INSERT
INTO
    prd.patient_med_adherence_red_list (patient_id, mco_id, measure_id, year, is_sure_scripts_red_list,
                                        sure_scripts_ids, patient_med_adherence_synth_period_ids)
SELECT
    rl.patient_id
  , sp.patient_payer_id
  , rl.measure_id
  , 2022
  , TRUE
  , sure_scripts_ids
  , patient_med_adherence_synth_period_ids
FROM
    _our_red_list rl
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = rl.patient_id
WHERE
      rl.nd_fills > 1
  AND rl.pct < .8
ON CONFLICT (year, measure_id, patient_id)
    DO UPDATE SET
                  is_sure_scripts_red_list = TRUE, updated_at = NOW(), sure_scripts_ids = EXCLUDED.sure_scripts_ids
                                                 , patient_med_adherence_synth_period_ids = EXCLUDED.patient_med_adherence_synth_period_ids;


SELECT
    is_sure_scripts_red_list
  , is_mco_red_list
  , COUNT(*)
FROM
    prd.patient_med_adherence_red_list
    group by 1, 2;


------------------------------------------------------------------------------------------------------------------------
/* current non compliant list */
------------------------------------------------------------------------------------------------------------------------
update prd.patient_med_adherence_measures set patient_med_adherence_synth_period_ids =
drop table if exists _our_current_measures;
create temporary table _our_current_measures as
select
    patient_id,
    measure_id,
    nd_fills,
    ipsd,
    days_to_cover,
    days_covered,
    last_covered_date,
    (days_covered * 1.0 /days_to_cover)::decimal(16,2) pct,
    '2023-12-31'::date - (days_needed_thru_eoy - days_covered)::int absolute_fail_date,
    days_needed_thru_eoy,
    sure_scripts_ids,
    patient_med_adherence_synth_period_ids
from (
    select
        mp.patient_id,
        mp.measure_id,
        min(mp.start_date) ipsd,
        max(end_date) last_covered_date,
        current_date - min(mp.start_date) + 1 days_to_cover,
        (('2023-12-31'::date - min(mp.start_date)) * .8)::int days_needed_thru_eoy,
        count(distinct mp.start_date ) nd_fills,
        count(distinct d.day         ) filter ( where d.day between mp.start_date and mp.end_date ) days_covered,
        array_agg(distinct mp.id) patient_med_adherence_synth_period_ids,
        ('{' || replace(replace(replace(array_agg(distinct mp.sure_scripts_ids::text)::text,'}',''),'{',''),'"','') || '}')::bigint[] sure_scripts_ids
    from
         prd.patient_med_adherence_synth_periods mp
        join ref.dates d on d.day between '2023-01-01'::date and current_date
    where
        mp.start_date between '2023-01-01'::date and current_date
    and mp.batch_id = 67 -- 2023 on
    group by 1,2
) x
;

SELECT * FROM _our_current_measures;

SELECT count(*)
FROM
    _our_current_measures
where current_date - last_covered_date  >= 5
;

-- layer in our measures first as there is more data available
INSERT
INTO
    prd.patient_med_adherence_measures (patient_id, mco_id, measure_id, year, fill_count, ipsd, next_fill_date,
                                        absolute_fail_date, is_sure_scripts_measure, sure_scripts_ids, patient_med_adherence_synth_period_ids)
SELECT
    cm.patient_id
  , sp.patient_payer_id
  , cm.measure_id
  , 2023
  , cm.nd_fills
  , cm.ipsd
  , cm.last_covered_date + 1
  , cm.absolute_fail_date
  , TRUE
  , sure_scripts_ids
  , patient_med_adherence_synth_period_ids
FROM
    _our_current_measures cm
    JOIN fdw_member_doc.supreme_pizza sp
         ON sp.patient_id = cm.patient_id;


CREATE TEMP TABLE _uhc_current_measures AS
SELECT DISTINCT
    mp.patient_id
  , CASE
        WHEN x.rx_category = 'MAC' THEN 'PDC-STA'
        WHEN x.rx_category = 'MAH' THEN 'PDC-RASA'
        WHEN x.rx_category = 'MAD' THEN 'PDC-DR'
        END                                                   measure_id
  , absolute_fail_date
    -- assume more than one fill if they are in this list and is_1x_fill=Yes
  , MAX(CASE WHEN TRIM(is_1x_fill) = 'Yes' THEN 1 ELSE 2 END) fill_cnt_approx
  , MAX(next_refill_due)                                      next_refill_due
, array_agg(distinct x.id) patient_rx_adherence_roster_uhc_ids
FROM
    raw.patient_rx_adherence_roster_uhc x
    JOIN integrations.mco_patients mp ON mp.payer_id = 47 AND mp.mco_member_id = x.patient_card_id
WHERE
        x.inbound_file_id =
        ( SELECT MAX(inbound_file_id) FROM raw.patient_rx_adherence_roster_uhc x2 WHERE x2.payer_id = 47 )
GROUP BY
    1, 2, 3
;
SELECT *
FROM
        raw.patient_rx_adherence_roster_uhc x
;



INSERT
INTO
    prd.patient_med_adherence_measures (patient_id, mco_id, measure_id, year, fill_count, ipsd, next_fill_date,
                                        absolute_fail_date, is_mco_measure, uhc_measure_ids)
SELECT DISTINCT
    u.patient_id
  , 47
  , u.measure_id
  , 2023
  , u.fill_cnt_approx
  , NULL::DATE
  , NULLIF(TRIM(u.next_refill_due), '')::DATE
  , NULLIF(TRIM(u.absolute_fail_date), '')::DATE
  , TRUE
  , patient_rx_adherence_roster_uhc_ids
FROM
    _uhc_current_measures u
WHERE
      u.measure_id IS NOT NULL
  AND u.patient_id IS NOT NULL
ON CONFLICT(year, measure_id, patient_id) DO UPDATE SET
                                                        is_mco_measure = TRUE, updated_at = NOW()
                                                                             , uhc_measure_ids = excluded.uhc_measure_ids;


SELECT *
FROM
    prd.patient_med_adherence_measures

    ;
------------------------------------------------------------------------------------------------------------------------
/* Pull together the two lists */
------------------------------------------------------------------------------------------------------------------------


drop table if exists _red_list_output;
CREATE TEMPORARY TABLE _red_list_output AS
SELECT DISTINCT
    py.name                   payer_name
  , py.id                     payer_id
  , r.patient_id
  , r.measure_id
  , rp.id                     rp_id
  , rp.name                   rp_name
  , rpo.id                    rpo_id
  , rpo.name                  rpo_name
  , pma.id IS NOT NULL        has_current_year_measure_med
  , nc.patient_id IS NOT NULL is_current_year_non_compliant
  , p.first_name
  , p.last_name
  , p.dob
FROM
    prd.patient_med_adherence_red_list r
    JOIN fdw_member_doc.patients p ON p.id = r.patient_id
    JOIN fdw_member_doc.supreme_pizza sp ON p.id = sp.patient_id
    JOIN fdw_member_doc.referring_partners rp ON rp.id = sp.primary_referring_partner_id
    JOIN fdw_member_doc.payers py ON sp.patient_payer_id = py.id
    LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = sp.primary_rpo_id
    LEFT JOIN prd.patient_med_adherence_measures pma
              ON pma.patient_id = r.patient_id AND pma.measure_id = r.measure_id AND pma.year = 2023

    LEFT JOIN _current_non_compliant_output nc
              ON nc.patient_id = r.patient_id AND nc.measure_id = r.measure_id
WHERE
    sp.primary_referring_partner_id IN ( SELECT id FROM _pilot_rpl_ids );


-- pilot_redlist_20230323
SELECT *
FROM
    _red_list_output;


-- pilot_non_compliant_20230323
SELECT *
FROM
    _current_non_compliant_output;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
drop table if exists _current_non_compliant_output;
CREATE TEMPORARY TABLE _current_non_compliant_output AS
SELECT DISTINCT
    py.name            payer_name
  , py.id              payer_id
  , m.patient_id
  , m.measure_id
  , rp.id              rp_id
  , rp.name            rp_name
  , rpo.id             rpo_id
  , rpo.name           rpo_name
  , p.first_name
  , p.last_name
  , p.dob
FROM
    prd.patient_med_adherence_measures m
    JOIN fdw_member_doc.patients p ON p.id = m.patient_id
    JOIN fdw_member_doc.supreme_pizza sp ON p.id = sp.patient_id
    JOIN fdw_member_doc.referring_partners rp ON rp.id = sp.primary_referring_partner_id
    JOIN fdw_member_doc.payers py ON sp.patient_payer_id = py.id
    LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = sp.primary_rpo_id
WHERE
    sp.primary_referring_partner_id IN ( SELECT id FROM _pilot_rpl_ids )
-- and current_date - m.next_fill_date >= 5
;
-- _med_adherence ma
--     JOIN fdw_member_doc.patients p ON p.id = ma.patient_id::bigint
--     LEFT JOIN fdw_member_doc.patient_referring_partners prp ON prp.patient_id = ma.patient_id::BIGINT
--     LEFT JOIN fdw_member_doc.referring_partners rp ON prp.referring_partner_id = rp.id
--     LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id
--     LEFT JOIN fdw_member_doc.msh_care_team_referring_partners ctrp ON rp.id = ctrp.referring_partner_id
--     LEFT JOIN fdw_member_doc.care_teams c ON ctrp.care_team_id = c.id
--     LEFT JOIN fdw_member_doc.care_team_members ctm ON c.id = ctm.care_team_id AND role = 'health_navigator'
--     LEFT JOIN fdw_member_doc.users u ON u.id = ctm.user_id

-- last fill meds for measure
drop table if exists _funs;
CREATE TEMPORARY TABLE _funs AS
SELECT DISTINCT ON (mhd.patient_id, ocm.measure_id, mhd.product_code)
    ocm.measure_id
  , mhd.*
  , COALESCE(mhd.sold_date, mhd.last_filled_date, mhd.written_date) start_date
  , pms.end_date                                                    adjusted_date

FROM
    prd.patient_med_adherence_measures ocm
    JOIN prd.patient_med_adherence_synth_periods pms ON pms.id = ANY (ocm.patient_med_adherence_synth_period_ids)
    JOIN public.sure_scripts_med_history_details mhd ON mhd.id = ANY (pms.sure_scripts_ids)
ORDER BY
    mhd.patient_id, ocm.measure_id, mhd.product_code
                  , COALESCE(mhd.sold_date, mhd.last_filled_date, mhd.written_date) DESC;

INSERT
INTO
    fdw_member_doc_stage.patient_medication_adherences (patient_id, measure_id, drug_name, ndc, days_supply,
                                                        next_fill_date, last_fill_date,adjusted_next_fill_date, remaining_refills, prescriber_name,
                                                        prescriber_npi, pharmacy_name, pharmacy_npi, pharmacy_phone,
                                                        failed_last_year, analytics_id, inserted_at, updated_at)
SELECT
    pmam.patient_id
  , pmam.measure_id
  , f.drug_description
  , f.product_code
  , f.days_supply::int
  , f.start_date + f.days_supply::int - 1
  , f.start_date
     , f.adjusted_date
  , f.refills_value::int remaining_refills
  , f.prescriber_name
  , f.prescriber_npi
  , f.pharmacy_name
  , f.pharmacy_npi
  , f.pharmacy_phone_number
  , rl.id is not null failed_last_year
  , pmam.id analytics_id
  , now()
  , now()
FROM
    prd.patient_med_adherence_measures pmam
    join _funs f on f.patient_id::bigint = pmam.patient_id
                and f.measure_id = pmam.measure_id
    left join prd.patient_med_adherence_red_list rl on rl.patient_id = pmam.patient_id
                                                   and rl.measure_id = pmam.measure_id
                                                   and rl.year = pmam.year - 1
where pmam.next_fill_date + 5 < current_date
  and pmam.is_sure_scripts_measure
;


-- call cb.x_util_create_fdw_member_doc();