------------------------------------------------------------------------------------------------------------------------
/* Training data build */
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
/*
    Step 1: find good (enough) member periods that have (enough) pre and post
    Limit to payer 81 CCLF initially
*/
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _controls;
CREATE TEMP TABLE _controls AS
SELECT
    id payer_id
  , name
  , claim_start_date
  , claim_end_date
FROM
    fdw_member_doc.payers
WHERE
    id = 81;

SELECT * FROM _controls;


DROP TABLE IF EXISTS _training_periods;
CREATE TEMP TABLE _training_periods AS
SELECT
    payer_id
  , ROW_NUMBER() OVER ()                 period_number
  , bom                                  pre_start
  , (bom + '6 months'::INTERVAL)::DATE   pre_mid
  , (bom - 1 + '1 year'::INTERVAL)::DATE pre_end
  , (bom + '1 year'::INTERVAL)::DATE     post_start
  , (bom - 1 + '2 year'::INTERVAL)::DATE post_end
FROM
    analytics.ref.dates d
    JOIN _controls c ON d.bom = d.day AND d.day BETWEEN c.claim_start_date AND c.claim_end_date - '2 year'::interval
ORDER BY
    bom
;


SELECT *
FROM
    _training_periods
ORDER BY
    pre_start
;

SELECT *
FROM
    prd.eligibility_months where payer_id = 81;

-- for each of the training periods find what members have enough eligibility
-- arbitrary 6months in 2H pre, 6months total in post

DROP TABLE IF EXISTS junk.ml_member_periods_pre_20250210;
CREATE TABLE junk.ml_member_periods_pre_20250210 AS
SELECT
    tp.payer_id
  , tp.period_number
  , em.member_id
  , CASE
        WHEN em.bom BETWEEN tp.pre_start AND tp.pre_mid - 1 THEN 'pre_1h'
        WHEN em.bom BETWEEN tp.pre_mid AND tp.pre_end       THEN 'pre_2h' END period
  , SUM(em.n_elig_days)                                                       n_elig_days
  , SUM(em.p_mm)                                                              p_mm
  , BOOL_OR(em.eom = pre_end)                                                 elg_at_end_of_pre
FROM
    _training_periods tp
    JOIN prd.eligibility_months em ON em.payer_id = tp.payer_id
        AND em.bom BETWEEN tp.pre_start AND tp.pre_end
GROUP BY
    1, 2, 3, 4
;
create INDEX  on junk.ml_member_periods_pre_20250210(payer_id, period_number, member_id);

SELECT *
FROM
    junk.ml_member_periods_pre_20250210 where member_id = 20362;


DROP TABLE IF EXISTS junk.ml_member_periods_post_20250210;
CREATE TABLE junk.ml_member_periods_post_20250210 AS
SELECT
    tp.payer_id
  , tp.period_number
  , em.member_id
  , 'post'              period
  , SUM(em.n_elig_days) n_elig_days
  , SUM(em.p_mm)        p_mm
FROM
    _training_periods tp
    JOIN prd.eligibility_months em ON em.payer_id = tp.payer_id
        AND em.bom BETWEEN tp.post_start AND tp.post_end
GROUP BY
    1, 2, 3, 4
;
create INDEX  on junk.ml_member_periods_post_20250210 (payer_id, period_number, member_id);

-- check counts for training periods 
DROP TABLE IF EXISTS _period_member_counts;
CREATE TEMP TABLE _period_member_counts AS
SELECT
    pre.period_number
  , pre.payer_id
  , COUNT(DISTINCT pre.member_id)                                               nd_mem_pre
  , COUNT(DISTINCT post.member_id)                                              nd_mem_post
  , COUNT(DISTINCT pre.member_id) FILTER ( WHERE pre.p_mm >= 6 )                nd_mem_pre_six_month_elg
  , COUNT(DISTINCT post.member_id) FILTER ( WHERE post.p_mm >= 6 )              nd_mem_post_six_month_elg
  , COUNT(DISTINCT post.member_id)
    FILTER ( WHERE pre.elg_at_end_of_pre AND pre.p_mm >= 6 AND post.p_mm >= 6 ) nd_mem_elg_for_period
FROM
    junk.ml_member_periods_pre_20250210 pre
    LEFT JOIN junk.ml_member_periods_post_20250210 post
              ON pre.payer_id = post.payer_id
                  AND pre.period_number = post.period_number
                  AND pre.member_id = post.member_id
WHERE
    pre.period = 'pre_2h'
GROUP BY
    1, 2
ORDER BY
    1, 2
;

SELECT * FROM _period_member_counts;

    ;

-- save off member period eligibility
drop table if exists junk.ml_member_periods_20250210;
CREATE TABLE junk.ml_member_periods_20250210 AS
SELECT
    pre.payer_id
  , pre.period_number
  , pre.member_id
  , EXTRACT('year' FROM AGE(tp.pre_end, date_of_birth)) age
  , m.gender
FROM
    junk.ml_member_periods_pre_20250210 pre
    JOIN junk.ml_member_periods_post_20250210 post
         ON pre.payer_id = post.payer_id
             AND pre.period_number = post.period_number
             AND pre.member_id = post.member_id
    JOIN _training_periods tp ON tp.payer_id = pre.payer_id AND tp.period_number = pre.period_number
    JOIN prd.members m ON m.payer_id = pre.payer_id AND m.id = pre.member_id
WHERE
      pre.period = 'pre_2h'
  AND pre.elg_at_end_of_pre
  AND pre.p_mm >= 6
  AND post.p_mm >= 6;

CREATE UNIQUE INDEX on junk.ml_member_periods_20250210(payer_id, period_number, member_id);

SELECT * FROM junk.ml_member_periods_20250210;

-- get min max range per patient, pull all the data, can then group how we want in python


DROP TABLE IF EXISTS _member_bookends;
CREATE TEMP TABLE _member_bookends AS
SELECT
    mp.payer_id
  , mp.member_id
  , MIN(tp.pre_start) first_start
  , MAX(tp.post_end)  last_end
FROM
    junk.ml_member_periods_20250210 mp
    JOIN _training_periods tp
ON tp.payer_id = mp.payer_id AND tp.period_number = mp.period_number
GROUP BY 1, 2;
CREATE UNIQUE INDEX  on _member_bookends(payer_id, member_id);

-- member months
SELECT
    id
  , mm.payer_id
  , mm.member_id
  , bom
--   , eom
--   , line_of_business_id
--   , market
--   , ma_product_key
--   , provider_id
--   , provider_tax_id
  , n_elig_days
  , p_mm
--   , last_elig_date

  , tc
  , medical_tc
  , nrx_tc
  , rx_tc
  , ip_tc
  , ed_tc
  , snf_tc
  , icf_tc
  , hh_tc
  , out_tc
  , pro_tc
  , hcbs_tc
  , sphs_tc
  , amb_tc
  , dme_tc
  , hosp_tc
  , oth_tc

--   , ortho_tc
--   , general_ortho_tc
--   , hand_ortho_tc
--   , foot_ankle_ortho_tc
--   , spine_ortho_tc
--   , shoulder_elbow_ortho_tc
--   , hip_knee_ortho_tc
--
--   , ddos
--   , rx_ddos
--   , ip_ddos
--   , ed_ddos
--   , snf_ddos
--   , icf_ddos
--   , hh_ddos
--   , out_ddos
--   , pro_ddos
--   , hcbs_ddos
--   , sphs_ddos
--   , amb_ddos
--   , dme_ddos
--   , hosp_ddos
--   , oth_ddos
--   , pcp_ddos

  , dialysis_ddos
  , pulmonar_ddos
  , copd_ddos
  , chf_ddos
  , heart_ddos
  , cancer_ddos
  , ckd_ddos
  , esrd_ddos
  , hyperlipid_ddos
  , diab_ddos
  , alzh_ddos
  , dementia_ddos
  , neurocognitive_ddos
  , stroke_ddos
  , hypertension_ddos
  , fall_ddos
  , transplant_ddos
  , liver_ddos
  , hippfract_ddos
  , depression_ddos
  , psychosis_ddos
  , drug_ddos
  , alcohol_ddos
  , paralysis_ddos
  , hemophilia_ddos
  , pressure_ulcer_ddos
  , tbi_ddos
  , obese_ddos

--   , year
--   , month
--   , month_text
--   , inserted_at
--   , ip_span_ddos
--   , snf_span_ddos
--   , snf_admits
--   , partb_tc
--   , partb_ddos
FROM
    analytics.prd.member_months mm
    JOIN _member_bookends mb ON mb.payer_id = mm.payer_id
        AND mm.member_id = mb.member_id
        AND mm.bom BETWEEN mb.first_start AND mb.last_end
;

-- member_training_periods_20250210
SELECT
    mp.payer_id
  , mp.period_number
  , mp.member_id
  , mp.age
  , mp.gender
  , tp.pre_start
  , tp.pre_mid
  , tp.pre_end
  , tp.post_start
  , tp.post_end
FROM
    junk.ml_member_periods_20250210 mp
    JOIN _training_periods tp ON tp.payer_id = mp.payer_id AND tp.period_number = mp.period_number
;

------------------------------------------------------------------------------------------------------------------------
/* build actual training samples */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS _pre;
CREATE TEMP TABLE _pre AS
SELECT
    -- features
    mm.payer_id
  , mp.period_number
  , mm.member_id
  , mp.age
  , mp.gender
  , SUM(n_elig_days)         n_elig_days_pre
  , SUM(ip_tc)               ip_tc_pre
  , SUM(ed_tc)               ed_tc_pre
  , SUM(snf_tc)              snf_tc_pre
  , SUM(icf_tc)              icf_tc_pre
  , SUM(hh_tc)               hh_tc_pre
  , SUM(out_tc)              out_tc_pre
  , SUM(pro_tc)              pro_tc_pre
  , SUM(hcbs_tc)             hcbs_tc_pre
  , SUM(sphs_tc)             sphs_tc_pre
  , SUM(amb_tc)              amb_tc_pre
  , SUM(dme_tc)              dme_tc_pre
  , SUM(hosp_tc)             hosp_tc_pre
  , SUM(dialysis_ddos)       dialysis_ddos_pre
  , SUM(pulmonar_ddos)       pulmonar_ddos_pre
  , SUM(copd_ddos)           copd_ddos_pre
  , SUM(chf_ddos)            chf_ddos_pre
  , SUM(heart_ddos)          heart_ddos_pre
  , SUM(cancer_ddos)         cancer_ddos_pre
  , SUM(ckd_ddos)            ckd_ddos_pre
  , SUM(esrd_ddos)           esrd_ddos_pre
  , SUM(hyperlipid_ddos)     hyperlipid_ddos_pre
  , SUM(diab_ddos)           diab_ddos_pre
  , SUM(alzh_ddos)           alzh_ddos_pre
  , SUM(dementia_ddos)       dementia_ddos_pre
  , SUM(neurocognitive_ddos) neurocognitive_ddos_pre
  , SUM(stroke_ddos)         stroke_ddos_pre
  , SUM(hypertension_ddos)   hypertension_ddos_pre
  , SUM(fall_ddos)           fall_ddos_pre
  , SUM(transplant_ddos)     transplant_ddos_pre
  , SUM(liver_ddos)          liver_ddos_pre
  , SUM(hippfract_ddos)      hippfract_ddos_pre
  , SUM(depression_ddos)     depression_ddos_pre
  , SUM(psychosis_ddos)      psychosis_ddos_pre
  , SUM(drug_ddos)           drug_ddos_pre
  , SUM(alcohol_ddos)        alcohol_ddos_pre
  , SUM(paralysis_ddos)      paralysis_ddos_pre
  , SUM(hemophilia_ddos)     hemophilia_ddos_pre
  , SUM(pressure_ulcer_ddos) pressure_ulcer_ddos_pre
  , SUM(tbi_ddos)            tbi_ddos_pre
  , SUM(obese_ddos)          obese_ddos_pre
FROM
    junk.ml_member_periods_20250210 mp
    JOIN _training_periods tp ON tp.payer_id = mp.payer_id AND tp.period_number = mp.period_number
    JOIN analytics.prd.member_months mm ON mm.payer_id = mp.payer_id
        AND mm.member_id = mp.member_id
        AND mm.bom BETWEEN tp.pre_start AND tp.pre_end
GROUP BY
    1, 2, 3, 4, 5
    ;
create UNIQUE INDEX on _pre(payer_id, period_number, member_id);

DROP TABLE IF EXISTS _post;
CREATE TEMP TABLE _post AS
SELECT
    -- features
    mm.payer_id
  , mp.period_number
  , mm.member_id
  , SUM(n_elig_days) n_elig_days_pre
  , SUM(tc)          tc
FROM
    junk.ml_member_periods_20250210 mp
    JOIN _training_periods tp ON tp.payer_id = mp.payer_id AND tp.period_number = mp.period_number
    JOIN analytics.prd.member_months mm ON mm.payer_id = mp.payer_id
        AND mm.member_id = mp.member_id
        AND mm.bom BETWEEN tp.post_start AND tp.post_end
GROUP BY
    1, 2, 3
    ;
create UNIQUE INDEX on _post(payer_id, period_number, member_id);

drop TABLE if exists junk.ml_training_samples_20250210;
CREATE TABLE junk.ml_training_samples_20250210 AS
SELECT
    pre.payer_id
  , pre.period_number
  , pre.member_id
  , pre.n_elig_days_pre                                                                   pre_elg_days
  , pre.age                                                                               age_ft
  , CASE WHEN pre.gender = 'm' THEN 1 ELSE 0 END                                          is_male_ft
  , CASE WHEN pre.gender = 'f' THEN 1 ELSE 0 END                                          is_female_ft

  , COALESCE(pre.ip_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)               ip_tc_pre_pmpm_ft
  , COALESCE(pre.ed_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)               ed_tc_pre_pmpm_ft
  , COALESCE(pre.snf_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)              snf_tc_pre_pmpm_ft
  , COALESCE(pre.icf_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)              icf_tc_pre_pmpm_ft
  , COALESCE(pre.hh_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)               hh_tc_pre_pmpm_ft
  , COALESCE(pre.out_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)              out_tc_pre_pmpm_ft
  , COALESCE(pre.pro_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)              pro_tc_pre_pmpm_ft
  , COALESCE(pre.hcbs_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)             hcbs_tc_pre_pmpm_ft
  , COALESCE(pre.sphs_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)             sphs_tc_pre_pmpm_ft
  , COALESCE(pre.amb_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)              amb_tc_pre_pmpm_ft
  , COALESCE(pre.dme_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)              dme_tc_pre_pmpm_ft
  , COALESCE(pre.hosp_tc_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)             hosp_tc_pre_pmpm_ft
  , COALESCE(pre.dialysis_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)       dialysis_ddos_pre_pmpm_ft
  , COALESCE(pre.pulmonar_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)       pulmonar_ddos_pre_pmpm_ft
  , COALESCE(pre.copd_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)           copd_ddos_pre_pmpm_ft
  , COALESCE(pre.chf_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)            chf_ddos_pre_pmpm_ft
  , COALESCE(pre.heart_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)          heart_ddos_pre_pmpm_ft
  , COALESCE(pre.cancer_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)         cancer_ddos_pre_pmpm_ft
  , COALESCE(pre.ckd_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)            ckd_ddos_pre_pmpm_ft
  , COALESCE(pre.esrd_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)           esrd_ddos_pre_pmpm_ft
  , COALESCE(pre.hyperlipid_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)     hyperlipid_ddos_pre_pmpm_ft
  , COALESCE(pre.diab_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)           diab_ddos_pre_pmpm_ft
  , COALESCE(pre.alzh_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)           alzh_ddos_pre_pmpm_ft
  , COALESCE(pre.dementia_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)       dementia_ddos_pre_pmpm_ft
  , COALESCE(pre.neurocognitive_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2) neurocognitive_ddos_pre_pmpm_ft
  , COALESCE(pre.stroke_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)         stroke_ddos_pre_pmpm_ft
  , COALESCE(pre.hypertension_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)   hypertension_ddos_pre_pmpm_ft
  , COALESCE(pre.fall_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)           fall_ddos_pre_pmpm_ft
  , COALESCE(pre.transplant_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)     transplant_ddos_pre_pmpm_ft
  , COALESCE(pre.liver_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)          liver_ddos_pre_pmpm_ft
  , COALESCE(pre.hippfract_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)      hippfract_ddos_pre_pmpm_ft
  , COALESCE(pre.depression_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)     depression_ddos_pre_pmpm_ft
  , COALESCE(pre.psychosis_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)      psychosis_ddos_pre_pmpm_ft
  , COALESCE(pre.drug_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)           drug_ddos_pre_pmpm_ft
  , COALESCE(pre.alcohol_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)        alcohol_ddos_pre_pmpm_ft
  , COALESCE(pre.paralysis_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)      paralysis_ddos_pre_pmpm_ft
  , COALESCE(pre.hemophilia_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)     hemophilia_ddos_pre_pmpm_ft
  , COALESCE(pre.pressure_ulcer_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2) pressure_ulcer_ddos_pre_pmpm_ft
  , COALESCE(pre.tbi_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)            tbi_ddos_pre_pmpm_ft
  , COALESCE(pre.obese_ddos_pre * 30.0 / pre.n_elig_days_pre, 0)::DECIMAL(16, 2)          obese_ddos_pre_pmpm_ft

  , post.n_elig_days_pre                                                                  post_elig_days
  , post.tc                                                                               tc_tg
  , COALESCE(post.tc * 3.0 / post.n_elig_days_pre, 0)::DECIMAL(16, 2)                     tc_pmpm_tg
FROM
    _pre pre
    JOIN _post post
         ON pre.payer_id = post.payer_id
             AND pre.period_number = post.period_number
             AND pre.member_id = post.member_id
;




SELECT * FROM junk.ml_training_samples_20250210;









------------------------------------------------------------------------------------------------------------------------
/* simple features
  age
  gender
  pmpms for service types
  ddos for diseases
*/
------------------------------------------------------------------------------------------------------------------------