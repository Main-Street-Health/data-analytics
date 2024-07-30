language_model_data_pulls.sql-- language model training
WITH
      encounter_level   AS ( SELECT DISTINCT
                                 member_id
                               , date_from
                               , array_agg(distinct c.id) claim_ids
                             FROM
                                 cb.claims c
                                 LEFT JOIN ref.place_of_services pos ON pos.id = c.place_of_service_id
                                 LEFT JOIN ref.service_types st ON st.id = c.service_type_id
                             WHERE
                                   c.mco_id = 2
                               AND c.member_id = 1
                               AND c.service_type_id NOT IN (12, 13, 17, 18, 10, 15, 16)
                               AND NOT c.is_rx
                             GROUP BY 1,2
                             )
    , lagged_encounters AS ( SELECT
                                 el.*
                               , LAG(date_from) OVER (PARTITION BY member_id ORDER BY date_from) prev_claim_date
                             FROM
                                 encounter_level el )
  SELECT
      le.member_id
    , c.date_from
    , le.date_from - prev_claim_date days_since_last_encounter
    , ARRAY_AGG(cd.diag ORDER BY c.claim_line_id, cd.diag_sequence) FILTER ( WHERE cd.diag IS NOT NULL) icds_by_seq
    , ARRAY_AGG(distinct cd.diag ORDER BY c.claim_line_id, cd.diag) FILTER ( WHERE cd.diag IS NOT NULL) icds_by_alpha
  FROM
      lagged_encounters le
      JOIN cb.claims c ON c.id = any(le.claim_ids)
      LEFT JOIN cb.claims_diagnosis cd ON c.id = cd.claim_id
  WHERE
        cd.mco_id = 2
    AND c.mco_id = 2
    AND c.member_id = 1
  GROUP BY
      1, 2, 3
  ORDER BY
      1, 2
;

SELECT *
FROM
    cb.mcos
where risk_batch_id is not null
ORDER BY
    id;


------------------------------------------------------------------------------------------------------------------------
/* Add target to test embeddings */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS junk.language_model_test20220919;
CREATE TABLE junk.language_model_test20220923 AS
WITH
    mems AS ( SELECT
                  member_id
                , mco_id
                , COUNT(eom) FILTER ( WHERE is_cb_eligible AND DATE_PART('year', eom) = 2020 )        n_elg_pre
                , COUNT(eom) FILTER ( WHERE is_cb_eligible AND DATE_PART('year', eom) = 2021 )        n_elg_post
                , (MAX(eom) FILTER ( WHERE is_cb_eligible AND eom = '2021-01-31'::DATE )) IS NOT NULL elg_at_id
                , SUM(COALESCE(ip_tc, 0) + COALESCE(er_tc, 0) + COALESCE(snf_tc, 0) + COALESCE(icf_tc, 0) +
                      COALESCE(amb_tc, 0))
                  FILTER (WHERE DATE_PART('year', eom) = 2020)                                        impactable_spend_pre
                , SUM(COALESCE(ip_tc, 0) + COALESCE(er_tc, 0) + COALESCE(snf_tc, 0) + COALESCE(icf_tc, 0) +
                      COALESCE(amb_tc, 0))
                  FILTER (WHERE DATE_PART('year', eom) = 2021)                                        impactable_spend_post
                , MAX(age)                                                                            age
                , MAX(gender)                                                                         gender

-- , count(*) n
-- , count(*) FILTER ( WHERE is_cb_eligible ) n_elg
              FROM
                  junk.ip_features_all_new
              WHERE
                    mco_id IN (1, 2, 4, 5, 6, 8, 9)
                AND DATE_PART('year', eom) IN (2020, 2021)
              GROUP BY
                  1, 2
              ORDER BY
                  1 )
SELECT
    m.member_id
  , m.mco_id
  , '2020-01-01'::DATE                                   pre_start
  , '2020-12-31'::DATE                                   pre_end
  , '2021-01-01'::DATE                                   post_start
  , '2021-12-31'::DATE                                   post_end
  , impactable_spend_pre::INT
  , impactable_spend_post::INT
  , PERCENT_RANK() OVER (ORDER BY impactable_spend_pre)  pre_impactable_spend_pct
  , PERCENT_RANK() OVER (ORDER BY impactable_spend_post) post_impactable_spend_pct
  , age
  , gender
FROM
    mems m
WHERE
      n_elg_post > 0
  AND n_elg_pre > 0
  AND elg_at_id
ORDER BY
    member_id
;

SELECT
    AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct < .5 ) pct_0_50
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .5 AND  post_impactable_spend_pct < .6 ) ::decimal(16,2)  pct_50_60
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .6 AND  post_impactable_spend_pct < .7 ) ::decimal(16,2)  pct_60_70
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .7 AND  post_impactable_spend_pct < .8 ) ::decimal(16,2)  pct_70_80
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .8 AND  post_impactable_spend_pct < .9 ) ::decimal(16,2)  pct_80_90
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .9 AND  post_impactable_spend_pct < .95 )::decimal(16,2)   pct_90_95
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .95 AND post_impactable_spend_pct < .96 )::decimal(16,2)   pct_95
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .96 AND post_impactable_spend_pct < .97 )::decimal(16,2)   pct_96
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .97 AND post_impactable_spend_pct < .98 )::decimal(16,2)   pct_97
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .98 AND post_impactable_spend_pct < .99 )::decimal(16,2)   pct_98
  , AVG(impactable_spend_post) FILTER ( WHERE post_impactable_spend_pct >= .99  )                             ::decimal(16,2)   pct_99
FROM
    junk.language_model_test20220923;

+--------+---------+---------+---------+---------+---------+--------+--------+--------+--------+---------+
|pct_0_50|pct_50_60|pct_60_70|pct_70_80|pct_80_90|pct_90_95|pct_95  |pct_96  |pct_97  |pct_98  |pct_99   |
+--------+---------+---------+---------+---------+---------+--------+--------+--------+--------+---------+
|0       |null     |93.23    |693.54   |4224.04  |17577.25 |28464.54|33200.64|40883.83|55437.42|116149.34|
+--------+---------+---------+---------+---------+---------+--------+--------+--------+--------+---------+

+----------------------+---------+---------+---------+---------+---------+--------+--------+--------+---------+---------+
|pct_0_50|pct_50_60|pct_60_70|pct_70_80|pct_80_90|pct_90_95|pct_95  |pct_96  |pct_97  |pct_98   |pct_99   |
+----------------------+---------+---------+---------+---------+---------+--------+--------+--------+---------+---------+
|0.08    |253.53   |1261.43  |4246.08  |16091.12 |37315.92 |55250.96|64900.31|80761.86|105782.43|194905.41|
+----------------------+---------+---------+---------+---------+---------+--------+--------+--------+---------+---------+

select
    sum(impactable_spend_post)
  , max(impactable_spend_post) filter(where post_impactable_spend_pct >= .95) very_high_max
  , min(impactable_spend_post) filter(where post_impactable_spend_pct >= .95) very_high_min
  , max(impactable_spend_post) filter(where post_impactable_spend_pct >= .80 and post_impactable_spend_pct < .95) high_max
  , min(impactable_spend_post) filter(where post_impactable_spend_pct >= .80 and post_impactable_spend_pct < .95) high_min
  , max(impactable_spend_post) filter(where post_impactable_spend_pct >= .50 and post_impactable_spend_pct < .8) mod_max
  , min(impactable_spend_post) filter(where post_impactable_spend_pct >= .50 and post_impactable_spend_pct < .8) mod_min
  , max(impactable_spend_post) filter(where post_impactable_spend_pct <  .50) low_max
  , min(impactable_spend_post) filter(where post_impactable_spend_pct <  .50) low_min
  , sum(impactable_spend_post) filter(where post_impactable_spend_pct >= .95) * 100.0 / sum(impactable_spend_post) very_high_pct_total_spend
  , sum(impactable_spend_post) filter(where post_impactable_spend_pct >= .80) * 100.0 / sum(impactable_spend_post) high_pct_total_spend
  , sum(impactable_spend_post) filter(where post_impactable_spend_pct >= .50) * 100.0 / sum(impactable_spend_post) mod_pct_total_spend
  , sum(impactable_spend_post) filter(where post_impactable_spend_pct <  .50) * 100.0 / sum(impactable_spend_post) low_pct_total_spend
from junk.language_model_test20220923;
+---------+-------------+-------------+--------+--------+-------+-------+-------+-------+-------------------------+--------------------+-------------------+----------------------+
|sum      |very_high_max|very_high_min|high_max|high_min|mod_max|mod_min|low_max|low_min|very_high_pct_total_spend|high_pct_total_spend|mod_pct_total_spend|low_pct_total_spend   |
+---------+-------------+-------------+--------+--------+-------+-------+-------+-------+-------------------------+--------------------+-------------------+----------------------+
|280008972|887125       |51243        |51210   |8139    |8138   |18     |17     |0      |55.3302984877213149      |93.6465982240026223 |99.9995521572073055|0.00044784279269451409|
+---------+-------------+-------------+--------+--------+-------+-------+-------+-------+-------------------------+--------------------+-------------------+----------------------+

-- classes
--     0-17
--     18-8138
--     8139-51242
--     51243+


    select count(*) from junk.language_model_test20220923
SELECT
    CASE
        WHEN impactable_spend_post >= 51243 THEN 0
        WHEN impactable_spend_post >= 8139  THEN 1
        WHEN impactable_spend_post >= 18    THEN 2
        ELSE 3
        END
  , COUNT(*)
  , COUNT(*) * 100.0 / 30875
FROM
    junk.language_model_test20220923
GROUP BY
    1
ORDER BY
    1;

SELECT now(), now() at TIME ZONE 'America/Chicago';

utc = cdt + 7
    18 + 7 = 25


+----+-----+-------------------+
|case|count|?column?           |
+----+-----+-------------------+
|0   |6093 |61.15|
|1   |1420 |14.25|
|2   |868  |8.711 |
|3   |645  |6.473 |
|4   |638  |6.403 |
+----+-----+-------------------+



    ------------------------------------------------------------------------------------------------------------------------
    /* try additional codes */
    ------------------------------------------------------------------------------------------------------------------------
    -- language model training
WITH
    encounter_level   AS ( SELECT DISTINCT
                               member_id
                             , date_from
                             , ARRAY_AGG(DISTINCT c.id) claim_ids
                           FROM
                               cb.claims c
                               LEFT JOIN ref.place_of_services pos ON pos.id = c.place_of_service_id
                               LEFT JOIN ref.service_types st ON st.id = c.service_type_id
                           WHERE
                                 c.mco_id = 2
                             AND c.member_id = 1
                             AND c.service_type_id NOT IN (12, 13, 17, 18, 10, 15, 16)
                             AND NOT c.is_rx
                           GROUP BY
                               1, 2 )
  , lagged_encounters AS ( SELECT
                               el.*
                             , LAG(date_from) OVER (PARTITION BY member_id ORDER BY date_from) prev_claim_date
                           FROM
                               encounter_level el )
SELECT
    le.member_id
  , c.date_from
  , c.source_claim_id
  , c.claim_line_id
  , c.procedure_code
  , c.rx_ndc_code
  , c.service_type_id
  , le.date_from - prev_claim_date                                                                    days_since_last_encounter
  , ARRAY_AGG(cd.diag ORDER BY cd.diag_sequence) FILTER ( WHERE cd.diag IS NOT NULL) icds_by_seq
  , ARRAY_AGG(DISTINCT cd.diag ORDER BY cd.diag) FILTER ( WHERE cd.diag IS NOT NULL) icds_by_alpha
FROM
    lagged_encounters le
    JOIN cb.claims c ON c.id = ANY (le.claim_ids)
    LEFT JOIN cb.claims_diagnosis cd ON c.id = cd.claim_id
WHERE
      cd.mco_id = 2
  AND c.mco_id = 2
  AND c.member_id = 1
GROUP BY
    1, 2, 3, 4, 5, 6, 7,8
ORDER BY
    1, 2,3,4



member_id,date_from,source_claim_id,claim_line_id,procedure_code,rx_ndc_code,service_type_id,days_since_last_encounter,icds_by_seq,icds_by_alpha
1,2017-05-22,17g612847500,1,99214,,8,,"{e1165,z794,e785,i10,e559}","{e1165,e559,e785,i10,z794}"

SELECT lower(regexp_replace(service_type_description, ' ', '_', 'g'))
FROM
    ref.service_types
                      where id NOT IN (12, 13, 17, 18, 10, 15, 16)
SELECT *
FROM
    ref.nd;

SELECT
    ndc.id ISNULL                 ref_ndc_missing
  , c.rx_ndc_code ISNULL          claim_ndc_missing
  , COUNT(*)                      n
  , COUNT(DISTINCT c.rx_ndc_code) nd_claim_ndcs
FROM
    cb.claims c
    LEFT JOIN ref.ndc_cder ndc ON c.rx_ndc_code = ndc.ndc
WHERE
      c.mco_id = 2
  AND c.is_rx
GROUP BY
    1, 2
ORDER BY
    1, 2;

SELECT
    c.rx_ndc_code ndc
  , COUNT(*)
FROM
    cb.claims c
    LEFT JOIN ref.ndc_cder ndc ON c.rx_ndc_code = ndc.ndc
WHERE
      c.mco_id = 2
  AND c.is_rx
  AND ndc.id ISNULL
GROUP BY
    1
ORDER BY
    2 DESC ;



-- with ref_ndc as (
create table junk.open_fda_ndc_formatted_20220925  as
WITH
    packages  AS ( SELECT
                       JSONB_ARRAY_ELEMENTS(packaging) package
                     , generic_name
                     , product_ndc
                   FROM
                       junk.open_fda_ndc_20220925 )
  , split_ndc AS ( SELECT
                       package['sample']
                     , package ->> 'description'                               descr
                     , generic_name
                     , product_ndc
                     , package ->> 'package_ndc'                               package_ndc_formatted
                     , REGEXP_REPLACE(package ->> 'package_ndc', '-', '', 'g') package_ndc
                     , REGEXP_SPLIT_TO_ARRAY(package ->> 'package_ndc', '-')   ndc_split
                     , package ->> 'marketing_start_date'                      marketing_date
                   FROM
                       packages )
  , padded    AS ( SELECT *
                        , LPAD(ndc_split[1], 5, '0') ndc_split_1
                        , LPAD(ndc_split[2], 4, '0') ndc_split_2
                        , LPAD(ndc_split[3], 2, '0') ndc_split_3
                   FROM
                       split_ndc )
--   , ref_ndc   AS (
  SELECT
                       descr
                     , generic_name
                     , product_ndc
                     , package_ndc_formatted
                     , package_ndc
                     , ndc_split_1 || ndc_split_2 || ndc_split_3         ndc
                     , LENGTH(ndc_split_1 || ndc_split_2 || ndc_split_3) ndc_len
                     , marketing_date
                   FROM
                       padded
                       ;
--                    )
SELECT
    c.rx_ndc_code
, c.created_input_id
, c.raw_id
FROM
    cb.claims c
    LEFT JOIN ref_ndc ndc ON c.rx_ndc_code = ndc.ndc
WHERE
      c.mco_id = 2
  AND c.is_rx
  AND ndc.product_ndc ISNULL
GROUP BY
    1, 2, 3
ORDER BY
    1;


create INDEX     junk_open_fda_ndc_formatted_20220925_ndc_idx on
    junk.open_fda_ndc_formatted_20220925(ndc);

SELECT *
FROM
    junk.open_fda_ndc_formatted_20220925
WHERE
    ndc IN (
 '53885024510'
,'65702040810'
,'53885024450'
,'99073070827'
,'65702040710'
,'50924097110'
,'99073070822'
,'53885027210'
,'99073013001'
,'08290320122'
,'08290320119'
,'08290320109'
,'53885000810'
,'69097022416'
,'53885027150'
,'53885013610'
,'08290329515'
,'65702071110'
,'60913000101'
,'65702028810'
,'99073070805'
,'91237000128'
,'53885044801'
,'65702071210'
,'00904759080'
,'08290320550'
,'00193731221'
,'53885059501'
,'11917004813'
,'57599000101'
,'53885001110'
,'65702010110'
,'65702072310'
,'90166083152'
,'98302000160'
,'80777027310'
,'00169185275'
,'08290328418'
,'53885039310'
,'59267100001'
,'56151173301'
,'98302000105'
,'90166043154'
,'00169185189'
,'53885014301'
,'98302000100'
,'65702049310'
,'08290328468'
,'91237000163'
,'00536100901'
,'11917002529'
,'00193731150'
,'11917004814'
,'00904759180'
,'49281040565'
,'57599000200'
,'08290306546'
,'90166011103'

        );



WITH
    encounter_level   AS ( SELECT DISTINCT
                               c.member_id
                             , c.date_from
                             , c.rx_ndc_code
                           FROM
                               cb.claims c
                           WHERE
--                                    c.mco_id = %(mco_id)s and c.member_id = %(member_id)s
                               mco_id = 2
                             AND member_id = 1
                             AND c.is_rx
                             AND c.rx_ndc_code IS NOT NULL 
                           )
  , lagged_encounters AS ( SELECT
                               el.*
                             , LAG(date_from) OVER (PARTITION BY member_id ORDER BY date_from) prev_claim_date
                           FROM
                               encounter_level el )

SELECT DISTINCT
    le.member_id
  , le.date_from
  , le.rx_ndc_code
  , le.date_from - prev_claim_date days_since_last_encounter
FROM
    lagged_encounters le
ORDER BY
    1, 2, 3
;
SELECT *
FROM
    cb.members WHERE mco_id = 5;


EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)
WITH
    encounter_level   AS ( SELECT
                               member_id
                             , mco_id
                             , date_from
--                              , ARRAY_AGG(DISTINCT c.id) claim_ids
                           FROM
                               cb.claims c
                           WHERE
                                 c.mco_id = 5
                             AND c.member_id = 317003
                             AND c.service_type_id NOT IN (12, 13, 17, 18, 10, 15, 16)
                             AND NOT c.is_rx
                           GROUP BY
                               1, 2, 3)
  , lagged_encounters AS ( SELECT
                               el.*
                             , LAG(date_from) OVER (PARTITION BY member_id ORDER BY date_from) prev_claim_date
                           FROM
                               encounter_level el )

SELECT
    le.member_id
  , c.date_from
  , c.source_claim_id
  , c.claim_line_id
  , c.procedure_code
  , c.rx_ndc_code
  , c.service_type_id
  , le.date_from - prev_claim_date                                                   days_since_last_encounter
  , ARRAY_AGG(cd.diag ORDER BY cd.diag_sequence) FILTER ( WHERE cd.diag IS NOT NULL) icds_by_seq
  , ARRAY_AGG(DISTINCT cd.diag ORDER BY cd.diag) FILTER ( WHERE cd.diag IS NOT NULL) icds_by_alpha
FROM
    lagged_encounters le
    JOIN cb.claims c ON c.date_from = le.date_from and c.member_id = le.member_id and c.mco_id = le.mco_id
    LEFT JOIN cb.claims_diagnosis cd ON c.id = cd.claim_id
WHERE
      c.mco_id = 5
  AND cd.mco_id = 5
  AND c.member_id = 317003
  AND NOT c.is_rx -- remove for ndc language model
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8
ORDER BY
    1, 2, 3, 4
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
WITH
      encounter_level   AS ( SELECT
                                 member_id
                               , mco_id
                               , date_from
                             FROM
                                 cb.claims c
                                 LEFT JOIN ref.service_types st ON st.id = c.service_type_id
                             WHERE
                                   c.mco_id = 2
                               and c.member_id = 1
                               AND c.service_type_id NOT IN (12, 13, 17, 18, 10, 15, 16)
                               AND NOT c.is_rx
                             GROUP BY 1,2,3
                             )
    , lagged_encounters AS ( SELECT
                                 el.*
                               , LAG(date_from) OVER (PARTITION BY member_id ORDER BY date_from) prev_claim_date
                             FROM
                                 encounter_level el )

  SELECT
    le.member_id
  , c.date_from
  , date_part('year', c.date_from) yr
  , c.source_claim_id
  , c.claim_line_id
  , c.procedure_code
  , c.service_type_id
  , le.date_from - prev_claim_date                                                                    days_since_last_encounter
  , ARRAY_AGG(DISTINCT cd.diag ORDER BY cd.diag) FILTER ( WHERE cd.diag IS NOT NULL) icds_by_alpha
FROM
    lagged_encounters le
    JOIN cb.claims c ON c.date_from = le.date_from and c.member_id = le.member_id and c.mco_id = le.mco_id
    LEFT JOIN cb.claims_diagnosis cd ON c.id = cd.claim_id
WHERE
       c.mco_id = 2
   and cd.mco_id = 2
   and c.member_id = 1
   and not c.is_rx -- remove for ndc language model
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8
ORDER BY
    1, 2,3,4;

SELECT
    mco_id
  , member_id
  , DATE_PART('year', eom)                       yr
  , COALESCE(SUM(ip_tc), 0)                      ip_tc
  , SUM(COALESCE(ip_tc, 0) + COALESCE(er_tc, 0)) ip_er_tc
  , COALESCE(SUM(ip_ddos_span), 0)               ip_ddos_span
FROM
    junk.ip_features_all_new
GROUP BY
    1, 2, 3



;