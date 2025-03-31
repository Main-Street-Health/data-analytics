SELECT
    COUNT(*)                                                                             n
  , COUNT(*) FILTER ( WHERE full_risk_2023 = 1 AND full_risk_2024 = 1 )                  n_full_risk
  , COUNT(DISTINCT member_id)                                                            nd_member
  , COUNT(DISTINCT member_id) FILTER ( WHERE full_risk_2023 = 1 AND full_risk_2024 = 1 ) nd_member_full_risk
FROM
    dh.cost_prediction_model_member_months;



------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _member_periods;
CREATE TEMP TABLE _member_periods AS
SELECT
    m.payer_id
  , m.member_id
  , gm.gender
  , EXTRACT('year' FROM AGE('2023-12-31'::DATE, gm.date_of_birth)) age
  , '2023-01-01'::DATE                                             pre_start
  , '2023-12-31'::DATE                                             pre_end
  , '2024-01-01'::DATE                                             post_start
  , '2024-12-31'::DATE                                             post_end
FROM
    dh.cost_prediction_model_member_months m
    JOIN gmm.global_mco_member_mappings gmm ON m.member_id = gmm.member_id AND m.payer_id = gmm.payer_id
    JOIN gmm.global_members gm ON gmm.global_member_id = gm.id AND gm.is_duplicate IS FALSE
WHERE
      m.full_risk_2023 = 1
  AND m.full_risk_2024 = 1
;



CREATE UNIQUE INDEX  on _member_periods(payer_id, member_id);

------------------------------------------------------------------------------------------------------------------------
/* build actual training samples */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS _pre;
CREATE TEMP TABLE _pre AS
SELECT
    -- features
    mm.payer_id
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
    _member_periods mp
    JOIN analytics.prd.member_months mm ON mm.payer_id = mp.payer_id
        AND mm.member_id = mp.member_id
        AND mm.bom BETWEEN mp.pre_start AND mp.pre_end
GROUP BY
    1, 2, 3, 4
    ;
create UNIQUE INDEX on _pre(payer_id, member_id);

DROP TABLE IF EXISTS _post;
CREATE TEMP TABLE _post AS
SELECT
    -- features
    mm.payer_id
  , mm.member_id
  , SUM(n_elig_days) n_elig_days_pre
  , SUM(tc)          tc
FROM
    _member_periods mp
    JOIN analytics.prd.member_months mm ON mm.payer_id = mp.payer_id
        AND mm.member_id = mp.member_id
        AND mm.bom BETWEEN mp.post_start AND mp.post_end
GROUP BY
    1, 2
    ;
create UNIQUE INDEX on _post(payer_id, member_id);

drop TABLE if exists junk.ml_training_samples_20250326;
CREATE TABLE junk.ml_training_samples_20250326 AS
SELECT
    pre.payer_id
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
             AND pre.member_id = post.member_id
;




SELECT * FROM junk.ml_training_samples_20250326
