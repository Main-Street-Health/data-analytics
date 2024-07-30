DROP TABLE IF EXISTS _mems;
CREATE TEMP TABLE _mems AS
SELECT DISTINCT
    pre.member_id
  , pre.mco_id
  , '2020-01-01'::DATE pre_start
  , '2020-12-21'::DATE pre_end
  , daterange('[2020-01-01, 2020-12-21]') pre_rng
  , '2021-01-01'::DATE post_start
  , '2022-12-21'::DATE post_end
--   , ['2021-01-01'::DATE, '2021-12-21'::DATE]::daterange post_rng
  , daterange('[2021-01-01, 2021-12-21]') post_rng
FROM
    junk.ip_features_all_new pre
    JOIN junk.ip_features_all_new post ON post.member_id = pre.member_id
WHERE
      pre.is_cb_eligible
  AND pre.mco_id = 5
  AND pre.eom = '2020-12-31'
  AND post.is_cb_eligible
  AND post.mco_id = 5
  AND post.eom = '2021-01-31'
;
CREATE UNIQUE INDEX _mems_member_id_idxxxxx on _mems(member_id);
CREATE  INDEX _mems_mco_idxxxxx on _mems(mco_id);
CREATE  INDEX _mems_post_start_xxxxx on _mems(post_start);
CREATE INDEX _mems_pre_xxxxx  ON _mems USING GIST (pre_rng);
CREATE INDEX _mems_post_xxxxx  ON _mems USING GIST (post_rng);

SELECT count(*) FROM _mems;

DROP TABLE IF EXISTS _mem_features;
CREATE TEMP TABLE _mem_features AS
SELECT
    m.member_id
  , MAX(age)                 age
  , MIN(ggroup)              ggroup
  , MIN(line_of_business_id) line_of_business_id
  , MAX(gender)              gender
  , 0 nd_ip_admits
  , 0 hosp_los
  , 0 nd_er_admits
  , 0 nd_npi_seen
  , 0 nd_ndc
  , 0.0 ra_coef
  , 0 nd_hcc
  , '{}'::int[] hcc_list
  , 0.0::numeric ip_er_tc
FROM
    _mems m
    JOIN junk.ip_features_all_new pre ON pre.member_id = m.member_id
        AND pre.mco_id = m.mco_id
        AND m.pre_rng @> pre.eom
GROUP BY
    1
;

--
-- # admissions
-- # days in hosp
-- # ers
-- # distinct docs seen
-- # costs by service_type
-- # costs by pos
-- # distinct drugs
-- # HCCs



DROP TABLE IF EXISTS _pre_claims;
CREATE TEMP TABLE _pre_claims AS
SELECT
    m.member_id
  , date_from
  , LEAST(date_to, m.pre_end) - date_from los
  , paid_amount
  , st.service_type_description
  , pos.place_of_service_name
  , c.paid_date < m.post_start            include_in_lagged_claims
  , procedure_code
  , c.id
  , c.rx_ndc_code
  , COALESCE(LOWER(ndc.non_proprietary_name), c.rx_ndc_code)
  , c.servicing_provider_npi
  , c.admission_type
  , c.admission_source
  , c.admission_diag
FROM
    _mems m
    JOIN claims c ON c.member_id = m.member_id
        AND c.mco_id = 5
        AND m.mco_id = 5
        AND c.mco_id = m.mco_id
        AND m.pre_rng @> c.date_from
    LEFT JOIN ref.place_of_services pos ON c.place_of_service_id = pos.id
    LEFT JOIN ref.service_types st ON c.service_type_id = st.id
    LEFT JOIN ref.ndc_cder ndc ON c.rx_ndc_code_id = ndc.id
WHERE
    c.paid_amount >= 0
;

UPDATE _mem_features mf
SET
    nd_ip_admits = x.nd_ip_admits
FROM
    ( SELECT
          member_id
        , COUNT(DISTINCT date_from) nd_ip_admits
      FROM
          _pre_claims pc
      WHERE
          pc.service_type_description = 'Acute Inpatient'
      AND pc.include_in_lagged_claims
      GROUP BY 1
      ) x
WHERE
      x.member_id = mf.member_id
  AND x.nd_ip_admits > 0
;
UPDATE _mem_features mf
SET
    hosp_los = x.hosp_los
FROM
    ( SELECT
          member_id
        , sum(los) hosp_los
      FROM
          _pre_claims pc
      WHERE
          pc.service_type_description = 'Acute Inpatient'
      AND pc.include_in_lagged_claims
      GROUP BY 1
      ) x
WHERE
      x.member_id = mf.member_id
  AND x.hosp_los > 0
;
UPDATE _mem_features mf
SET
    nd_er_admits = x.nd_er_admits
FROM
    ( SELECT
          member_id
        , COUNT(DISTINCT date_from) nd_er_admits
      FROM
          _pre_claims pc
      WHERE
          pc.service_type_description = 'ER'
      AND pc.include_in_lagged_claims
      GROUP BY 1
      ) x
WHERE
      x.member_id = mf.member_id
  AND x.nd_er_admits > 0
;
UPDATE _mem_features mf
SET
    ip_er_tc = x.tc
FROM
    ( SELECT
          member_id
        , sum(paid_amount) tc
      FROM
          _pre_claims pc
      WHERE
          pc.service_type_description in ('ER', 'Acute Inpatient')
      AND pc.include_in_lagged_claims
      GROUP BY 1
      ) x
WHERE
      x.member_id = mf.member_id
  AND x.tc > 0
;


UPDATE _mem_features mf
SET
    nd_npi_seen = x.nd_npi_seen
FROM
    ( SELECT
          member_id
        , COUNT(DISTINCT servicing_provider_npi) nd_npi_seen
      FROM
          _pre_claims pc
      WHERE
        pc.include_in_lagged_claims
        AND pc.service_type_description NOT IN ( 'Other', 'Home DME', 'Community Living Supports',
                                                'HCBS Attendant & Personal Care', 'Home Health Aide', 'HCBS Other',
                                                'HCBS Supportive Housing (ALFGCF)', 'HCBS Adult Day Care'
          )
      GROUP BY 1
      ) x
WHERE
      x.member_id = mf.member_id
  AND x.nd_npi_seen > 0
;
UPDATE _mem_features mf
SET
    nd_ndc = x.nd_ndc
FROM
    ( SELECT
          member_id
        , COUNT(DISTINCT rx_ndc_code) nd_ndc
      FROM
          _pre_claims pc
      WHERE
          pc.include_in_lagged_claims
      GROUP BY 1
      ) x
WHERE
      x.member_id = mf.member_id
  AND x.nd_ndc > 0
;
UPDATE _mem_features mf
SET
    nd_hcc = x.nd_hcc
, hcc_list = x.hcc_list
FROM
    ( SELECT
          member_id
        , COUNT(DISTINCT hcc.id) nd_hcc

        , array_agg(DISTINCT hcc.id) filter (where hcc.id is not null) hcc_list
      FROM
          _pre_claims pc
      join claims_diagnosis cd ON pc.id = cd.claim_id and cd.mco_id = 5
      join ref.hcc_mappings hcc_icd on hcc_icd.icd10_id = cd.diag_id
      join ref.hcc_categories hcc on hcc.id = hcc_icd.hcc_id
      WHERE
          pc.include_in_lagged_claims
      GROUP BY 1
      ) x
WHERE
      x.member_id = mf.member_id
  AND x.nd_hcc > 0
;
UPDATE _mem_features mf
SET
    ra_coef = x.ra_coef
FROM
    ( SELECT
          member_id
        , SUM(hcc.v24_coefficient) ra_coef
      FROM
          _mem_features mf2
          join ref.hcc_categories hcc ON hcc.id = ANY (mf2.hcc_list)
      GROUP BY
          1 ) x
WHERE
      x.member_id = mf.member_id
  AND x.ra_coef > 0
;




SELECT
    member_id
  , age
  , ggroup
  , line_of_business_id
  , gender
  , nd_ip_admits
  , hosp_los
  , nd_er_admits
  , nd_npi_seen
  , nd_ndc
  , nd_hcc
--   , hcc_list
  , ip_er_tc
FROM
    _mem_features
WHERE
    _mem_features.nd_ip_admits > 0;


SELECT *
FROM
    _pre_claims ;
SELECT *
FROM
    ref.service_types;


DROP TABLE IF EXISTS _member_targets;
CREATE TEMP TABLE _member_targets AS
WITH post_claims AS (
     SELECT c.service_type_id, coalesce(c.paid_amount, 0.0) paid_amount, c.date_from, m.mco_id, m.member_id, m.post_start
     FROM _mems m
     LEFT JOIN claims c ON c.member_id = m.member_id
        AND c.mco_id = 5
        AND m.mco_id = 5
        AND c.mco_id = m.mco_id
        AND m.post_rng @> c.date_from
        AND c.paid_amount >= 0
    )
SELECT
    member_id
  , SUM(paid_amount)                                                           tc_12m
  , SUM(paid_amount) FILTER (          WHERE pc.service_type_id IN (1, 2) )    ip_er_tc_12m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.service_type_id = 1 )          ip_ddos_12m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.service_type_id = 2 )          er_ddos_12m

  , SUM(paid_amount) FILTER (          WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '2 months'::interval)                                      tc_1m_2m
  , SUM(paid_amount) FILTER (          WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '2 months'::interval and pc.service_type_id IN (1, 2) )    ip_er_tc_1m_2m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '2 months'::interval and pc.service_type_id = 2 )          er_ddos_1m_2m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '2 months'::interval and pc.service_type_id = 1 )          ip_ddos_1m_2m

  , SUM(paid_amount) FILTER (          WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '3 months'::interval)                                      tc_1m_3m
  , SUM(paid_amount) FILTER (          WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '3 months'::interval and pc.service_type_id IN (1, 2) )    ip_er_tc_1m_3m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '3 months'::interval and pc.service_type_id = 2 )          er_ddos_1m_3m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '3 months'::interval and pc.service_type_id = 1 )          ip_ddos_1m_3m

  , SUM(paid_amount) FILTER (          WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '6 months'::interval)                                      tc_1m_6m
  , SUM(paid_amount) FILTER (          WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '6 months'::interval and pc.service_type_id IN (1, 2) )    ip_er_tc_1m_6m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '6 months'::interval and pc.service_type_id = 2 )          er_ddos_1m_6m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from between pc.post_start + '1 months'::interval and pc.post_start + '6 months'::interval and pc.service_type_id = 1 )          ip_ddos_1m_6m

  , SUM(paid_amount) FILTER (          WHERE pc.date_from < pc.post_start + '2 months'::interval)                                      tc_2m
  , SUM(paid_amount) FILTER (          WHERE pc.date_from < pc.post_start + '2 months'::interval and pc.service_type_id IN (1, 2) )    ip_er_tc_2m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from < pc.post_start + '2 months'::interval and pc.service_type_id = 2 )          er_ddos_2m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from < pc.post_start + '2 months'::interval and pc.service_type_id = 1 )          ip_ddos_2m

  , SUM(paid_amount) FILTER (          WHERE pc.date_from < pc.post_start + '3 months'::interval)                                      tc_3m
  , SUM(paid_amount) FILTER (          WHERE pc.date_from < pc.post_start + '3 months'::interval and pc.service_type_id IN (1, 2) )    ip_er_tc_3m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from < pc.post_start + '3 months'::interval and pc.service_type_id = 2 )          er_ddos_3m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from < pc.post_start + '3 months'::interval and pc.service_type_id = 1 )          ip_ddos_3m

  , SUM(paid_amount) FILTER (          WHERE pc.date_from < pc.post_start + '6 months'::interval)                                      tc_6m
  , SUM(paid_amount) FILTER (          WHERE pc.date_from < pc.post_start + '6 months'::interval and pc.service_type_id IN (1, 2) )    ip_er_tc_6m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from < pc.post_start + '6 months'::interval and pc.service_type_id = 2 )          er_ddos_6m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from < pc.post_start + '6 months'::interval and pc.service_type_id = 1 )          ip_ddos_6m

  , SUM(paid_amount) FILTER (          WHERE pc.date_from < pc.post_start + '9 months'::interval)                                      tc_9m
  , SUM(paid_amount) FILTER (          WHERE pc.date_from < pc.post_start + '9 months'::interval and pc.service_type_id IN (1, 2) )    ip_er_tc_9m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from < pc.post_start + '9 months'::interval and pc.service_type_id = 2 )          er_ddos_9m
  , COUNT(DISTINCT date_from) FILTER ( WHERE pc.date_from < pc.post_start + '9 months'::interval and pc.service_type_id = 1 )          ip_ddos_9m
from post_claims pc

GROUP BY
    1
;

SELECT
    mf.member_id
  , age                                      ft_age
  , ggroup                                   ft_ggroup
  , line_of_business_id                      ft_line_of_business_id
  , CASE WHEN gender = 'm' THEN 1 ELSE 0 END ft_is_male
  , nd_ip_admits                             ft_nd_ip_admits
  , hosp_los                                 ft_hosp_los
  , nd_er_admits                             ft_nd_er_admits
  , nd_npi_seen                              ft_nd_npi_seen
  , nd_ndc                                   ft_nd_ndc
  , nd_hcc                                   ft_nd_hcc
  , mf.hcc_list                              ft_hcc_list
  , mf.ra_coef                               ft_ra_coef
  , mf.ip_er_tc                              ft_ip_er_tc
    --------------------------------------
  , COALESCE(mt.tc_12m, 0)                   tg_tc_12m
  , COALESCE(mt.ip_er_tc_12m, 0)             tg_ip_er_tc_12m
  , COALESCE(mt.er_ddos_12m, 0)              tg_er_ddos_12m
  , COALESCE(mt.ip_ddos_12m, 0)              tg_ip_ddos_12m
  , COALESCE(mt.tc_1m_2m, 0)                 tg_tc_1m_2m
  , COALESCE(mt.ip_er_tc_1m_2m, 0)           tg_ip_er_tc_1m_2m
  , COALESCE(mt.er_ddos_1m_2m, 0)            tg_er_ddos_1m_2m
  , COALESCE(mt.ip_ddos_1m_2m, 0)            tg_ip_ddos_1m_2m
  , COALESCE(mt.tc_1m_3m, 0)                 tg_tc_1m_3m
  , COALESCE(mt.ip_er_tc_1m_3m, 0)           tg_ip_er_tc_1m_3m
  , COALESCE(mt.er_ddos_1m_3m, 0)            tg_er_ddos_1m_3m
  , COALESCE(mt.ip_ddos_1m_3m, 0)            tg_ip_ddos_1m_3m
  , COALESCE(mt.tc_1m_6m, 0)                 tg_tc_1m_6m
  , COALESCE(mt.ip_er_tc_1m_6m, 0)           tg_ip_er_tc_1m_6m
  , COALESCE(mt.er_ddos_1m_6m, 0)            tg_er_ddos_1m_6m
  , COALESCE(mt.ip_ddos_1m_6m, 0)            tg_ip_ddos_1m_6m
  , COALESCE(mt.tc_2m, 0)                    tg_tc_2m
  , COALESCE(mt.ip_er_tc_2m, 0)              tg_ip_er_tc_2m
  , COALESCE(mt.er_ddos_2m, 0)               tg_er_ddos_2m
  , COALESCE(mt.ip_ddos_2m, 0)               tg_ip_ddos_2m
  , COALESCE(mt.tc_3m, 0)                    tg_tc_3m
  , COALESCE(mt.ip_er_tc_3m, 0)              tg_ip_er_tc_3m
  , COALESCE(mt.er_ddos_3m, 0)               tg_er_ddos_3m
  , COALESCE(mt.ip_ddos_3m, 0)               tg_ip_ddos_3m
  , COALESCE(mt.tc_6m, 0)                    tg_tc_6m
  , COALESCE(mt.ip_er_tc_6m, 0)              tg_ip_er_tc_6m
  , COALESCE(mt.er_ddos_6m, 0)               tg_er_ddos_6m
  , COALESCE(mt.ip_ddos_6m, 0)               tg_ip_ddos_6m
  , COALESCE(mt.tc_9m, 0)                    tg_tc_9m
  , COALESCE(mt.ip_er_tc_9m, 0)              tg_ip_er_tc_9m
  , COALESCE(mt.er_ddos_9m, 0)               tg_er_ddos_9m
  , COALESCE(mt.ip_ddos_9m, 0)               tg_ip_ddos_9m
FROM
    _mem_features mf
    JOIN _member_targets mt ON mf.member_id = mt.member_id
;


------------------------------------------------------------------------------------------------------------------------
/* Results */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM junk.uhc_tx_risk_model_ml_20221001;

;

    ;
