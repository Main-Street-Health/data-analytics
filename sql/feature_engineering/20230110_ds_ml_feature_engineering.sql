DROP TABLE IF EXISTS _ass_liv_cpts;
CREATE TEMP TABLE _ass_liv_cpts AS
SELECT
    dc.payer_id
  , dc.analytics_category
  , cpt.procedure_code
  , cpt.procedure_mod1
FROM
    ds_categories dc
    JOIN ds_category_cpt_mappings cpt ON dc.id = cpt.ds_category_id
WHERE
      dc.is_current
  AND cpt.is_current
  AND dc.analytics_category = 'Community and Assisted Living Supports';

-- training so only want finished sferes
DROP TABLE IF EXISTS _sferes ;
CREATE TEMP TABLE _sferes AS
SELECT
    ARRAY_AGG(s.id) _sfere_ids
FROM
    sferes s
join patients p on p.id = s.patient_id
WHERE
      s.status IN ('completed', 'submitted')
  AND s.type = 'sfere'
  and p.payer_id = 8 --itc
;

DROP TABLE IF EXISTS _sfere_features;
CREATE TEMP TABLE _sfere_features AS
SELECT *
FROM
    fn_ds_ml_sfere_features((select _sfere_ids from _sferes));


-- itc_knn_sfere_features_20230130
SELECT
    sfere_id, patient_id, payer_id_ft, group_id_ft, age_ft, transfer_bed_to_chair_ft, transfer_chair_to_standing_ft, mobility_ft, dressing_ft, bathing_ft, eating_ft, grooming_ft, toileting_ft, turn_change_position_ft, has_incontinence_ft, incontinence_type_ft, incontinence_frequency_ft, calling_friends_and_family_ft, articulating_needs_ft, meal_prep_ft, shopping_ft, medication_management_ft, finances_ft, housework_ft, transportation_ft, daily_routine_decisions_ft, comprehension_ft, member_opinion_ft, cleaning_ft, laundry_ft, change_bed_ft, clean_kitchen_ft, clean_home_ft, medical_appointments_ft, work_school_socialize_ft, driving_ft, alert_oriented_self_ft, alert_oriented_place_ft, alert_oriented_day_time_ft, has_alzheimers_ft, has_dementia_ft, reporting_current_hrs_ft, reporting_rec_hrs_tg
FROM
    ( SELECT
                  ROW_NUMBER()
                  OVER (PARTITION BY sf.patient_id ORDER BY COALESCE(s.submitted_at, s.inserted_at) DESC ) rn_most_patient_sfere
        ,         sf.*
      FROM
          _sfere_features sf
          JOIN sferes s ON sf.sfere_id = s.id
      WHERE
            reporting_rec_hrs_tg IS NOT NULL
        AND reporting_current_hrs_ft != 0
      ORDER BY
          sf.patient_id ) x
WHERE
    rn_most_patient_sfere = 1


;




--     JOIN patients p ON p.id = sf.patient_id
--     JOIN analytics_patients ap ON ap.analytics_member_id = p.analytics_member_id




;

DROP TABLE IF EXISTS _a_pats;
CREATE TEMP TABLE _a_pats AS
SELECT DISTINCT
    p.analytics_member_id
  , p.payer_id
  , s.id                                             sfere_id
  , (s.submitted_at - '1 year' ::INTERVAL) ::DATE AS pre_start
  , (s.submitted_at) ::DATE                       AS pre_end
FROM
    _sfere_features sf
    JOIN sferes s ON sf.sfere_id = s.id
    JOIN patients p ON sf.patient_id = p.id

WHERE
    submitted_at IS NOT NULL
;

DROP TABLE IF EXISTS _ass_live_to_exclude;
CREATE TEMP TABLE _ass_live_to_exclude AS
SELECT distinct
    a.analytics_member_id, a.payer_id, a.pre_start, a.sfere_id
FROM
    _a_pats a
    JOIN analytics_claims ac ON a.analytics_member_id = ac.analytics_member_id
        AND a.payer_id = ac.mco_id
        AND ac.service_date_from BETWEEN a.pre_start AND a.pre_end
    JOIN _ass_liv_cpts alc ON a.payer_id = alc.payer_id
        AND alc.procedure_code = ac.procedure_code
        AND alc.procedure_mod1 IS NOT DISTINCT FROM ac.procedure_mod1;

SELECT count(*) FROM _ass_live_to_exclude;
SELECT * FROM _ass_live_to_exclude;

-- exclude assisted living
DELETE
FROM
    _sfere_features sf
WHERE
    EXISTS(SELECT 1 FROM _ass_live_to_exclude ex WHERE ex.sfere_id = sf.sfere_id);


-- training. Not sure why these rec's are null
DELETe
-- select s.*
FROM
    _sfere_features sf
-- join sferes s on sf.sfere_id = s.id
WHERE
    sf.reporting_rec_hrs_tg ISNULL ;


DROP TABLE IF EXISTS junk.sfere_claim_chronic_conditions_20230110;
CREATE TABLE junk.sfere_claim_chronic_conditions_20230110 AS
SELECT
    s.id                                                                                          sfere_id
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'glaucoma')          glaucoma_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'cataract')          cataract_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'behavioral_health') behavioral_health_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'osteoporosis')      osteoporosis_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'hiv')               hiv_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'transplants')       transplants_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'obesity')           obesity_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'cancer')            cancer_ddos
  , COUNT(DISTINCT ac.service_date_from)
    FILTER (WHERE ni.chronic_category = 'hip_pelvic_fracture')                                    hip_pelvic_fracture_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'sclerosis')         sclerosis_ddos
  , COUNT(DISTINCT ac.service_date_from)
    FILTER (WHERE ni.chronic_category = 'rheumatoid_arthritis')                                   rheumatoid_arthritis_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'ckd')               ckd_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'hyperlipidemia')    hyperlipidemia_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'diabetes')          diabetes_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'hypothyroidism')    hypothyroidism_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'pressure_ulcer')    pressure_ulcer_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'weight_loss')       weight_loss_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'heart')             heart_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'peptic_ulcer')      peptic_ulcer_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'anemia')            anemia_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'substance_abuse')   substance_abuse_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'liver')             liver_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'disabled')          disabled_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'fall')              fall_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'stroke')            stroke_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'paralysis')         paralysis_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'hypertension')      hypertension_ddos
  , COUNT(DISTINCT ac.service_date_from)
    FILTER (WHERE ni.chronic_category = 'peripheral_vascular')                                    peripheral_vascular_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'coagulation')       coagulation_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'fluid')             fluid_ddos
  , COUNT(DISTINCT ac.service_date_from)
    FILTER (WHERE ni.chronic_category = 'benign_prostatic_hyperplasia')                           benign_prostatic_hyperplasia_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'tbi')               tbi_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'neurocognitive')    neurocognitive_ddos
  , COUNT(DISTINCT ac.service_date_from) FILTER (WHERE ni.chronic_category = 'pulmonary')         pulmonary_ddos
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'glaucoma'), 0)              glaucoma_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'cataract'), 0)              cataract_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'behavioral_health'), 0)     behavioral_health_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'osteoporosis'), 0)          osteoporosis_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'hiv'), 0)                   hiv_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'transplants'), 0)           transplants_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'obesity'), 0)               obesity_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'cancer'), 0)                cancer_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'hip_pelvic_fracture'), 0)   hip_pelvic_fracture_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'sclerosis'), 0)             sclerosis_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'rheumatoid_arthritis'), 0)  rheumatoid_arthritis_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'ckd'), 0)                   ckd_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'hyperlipidemia'), 0)        hyperlipidemia_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'diabetes'), 0)              diabetes_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'hypothyroidism'), 0)        hypothyroidism_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'pressure_ulcer'), 0)        pressure_ulcer_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'weight_loss'), 0)           weight_loss_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'heart'), 0)                 heart_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'peptic_ulcer'), 0)          peptic_ulcer_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'anemia'), 0)                anemia_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'substance_abuse'), 0)       substance_abuse_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'liver'), 0)                 liver_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'disabled'), 0)              disabled_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'fall'), 0)                  fall_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'stroke'), 0)                stroke_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'paralysis'), 0)             paralysis_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'hypertension'), 0)          hypertension_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'peripheral_vascular'), 0)   peripheral_vascular_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'coagulation'), 0)           coagulation_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'fluid'), 0)                 fluid_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'benign_prostatic_hyperplasia'), 0)
                                                                                                  benign_prostatic_hyperplasia_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'tbi'), 0)                   tbi_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'neurocognitive'), 0)        neurocognitive_tc
  , COALESCE(SUM(ac.paid_amount) FILTER (WHERE ni.chronic_category = 'pulmonary'), 0)             pulmonary_tc

FROM
    sferes s
    JOIN _sfere_features sf ON s.id = sf.sfere_id
    JOIN patients p ON s.patient_id = p.id
    JOIN analytics_claims ac ON p.analytics_member_id = ac.analytics_member_id
        AND p.payer_id = ac.mco_id
        AND ac.service_date_from BETWEEN s.submitted_at - '13 months'::INTERVAL AND s.submitted_at - '1 month'::INTERVAL
        AND ac.service_type !~* 'hcbs|home'
        AND ac.paid_amount > 0
        AND NOT ac.is_rx
    JOIN analytics_claim_diagnoses acd ON ac.uuid = acd.analytics_claim_uuid
    JOIN junk.ref_chronics_20221109 ni ON ni.icd10 = acd.diagnosis_code
GROUP BY
    1;




-- lots of claims missing in uhc fl
SELECT
    sf.payer_id_ft
  , COUNT(sf.sfere_id)                              n_sferes
  , COUNT(cc.sfere_id)                              n_seres_w_claims
  , COUNT(cc.sfere_id) * 100.0 / COUNT(sf.sfere_id) n_seres_w_claims
FROM
    _sfere_features sf
    LEFT JOIN junk.sfere_claim_chronic_conditions_20230110 cc ON cc.sfere_id = sf.sfere_id
GROUP BY
    1
ORDER BY
    1;

/*
Missing claims

| payer\_id\_ft | n\_sferes | n\_seres\_w\_claims | n\_seres\_w\_claims |
| :--- | :--- | :--- | :--- |
| 1 | 201 | 185 | 92.0398009950248756 |
| 2 | 2842 | 2406 | 84.6586910626319493 |
| 4 | 2609 | 1805 | 69.1835952472211575 |
| 5 | 2786 | 2527 | 90.7035175879396985 |
| 6 | 2417 | 2292 | 94.82829954489036 |
| 7 | 2 | 1 | 50 |
| 8 | 1800 | 1580 | 87.7777777777777778 |
| 9 | 1287 | 1254 | 97.4358974358974359 |
| 12 | 788 | 733 | 93.0203045685279188 |
| 13 | 978 | 968 | 98.9775051124744376 |

*/

SELECT *
FROM
    sferes s
    JOIN patients p ON p.id = s.patient_id
WHERE
      p.payer_id = 5
  AND s.status IN ('completed', 'submitted')
ORDER BY
    submitted_at DESC
;

-- ds_rec_hrs_modelling_w_claims_20230110
SELECT
    sf.sfere_id
  , sf.patient_id
  , payer_id_ft
  , age_ft
  , transfer_bed_to_chair_ft
  , transfer_chair_to_standing_ft
  , mobility_ft
  , dressing_ft
  , bathing_ft
  , eating_ft
  , grooming_ft
  , toileting_ft
  , turn_change_position_ft
  , has_incontinence_ft
  , incontinence_type_ft
  , incontinence_frequency_ft
  , calling_friends_and_family_ft
  , articulating_needs_ft
  , meal_prep_ft
  , shopping_ft
  , medication_management_ft
  , finances_ft
  , housework_ft
  , transportation_ft
  , daily_routine_decisions_ft
  , comprehension_ft
  , member_opinion_ft
  , cleaning_ft
  , laundry_ft
  , change_bed_ft
  , clean_kitchen_ft
  , clean_home_ft
  , medical_appointments_ft
  , work_school_socialize_ft
  , driving_ft
  , alert_oriented_self_ft
  , alert_oriented_place_ft
  , alert_oriented_day_time_ft
  , has_alzheimers_ft
  , has_dementia_ft
  , reporting_current_hrs_ft
  , reporting_rec_hrs_tg
  , ap.grp_id                                     group_id_ft
    ------ Claims features
  , glaucoma_ddos                                 glaucoma_ddos_ft
  , cataract_ddos                                 cataract_ddos_ft
  , behavioral_health_ddos                        behavioral_health_ddos_ft
  , osteoporosis_ddos                             osteoporosis_ddos_ft
  , hiv_ddos                                      hiv_ddos_ft
  , transplants_ddos                              transplants_ddos_ft
  , obesity_ddos                                  obesity_ddos_ft
  , cancer_ddos                                   cancer_ddos_ft
  , hip_pelvic_fracture_ddos                      hip_pelvic_fracture_ddos_ft
  , sclerosis_ddos                                sclerosis_ddos_ft
  , rheumatoid_arthritis_ddos                     rheumatoid_arthritis_ddos_ft
  , ckd_ddos                                      ckd_ddos_ft
  , hyperlipidemia_ddos                           hyperlipidemia_ddos_ft
  , diabetes_ddos                                 diabetes_ddos_ft
  , hypothyroidism_ddos                           hypothyroidism_ddos_ft
  , pressure_ulcer_ddos                           pressure_ulcer_ddos_ft
  , weight_loss_ddos                              weight_loss_ddos_ft
  , heart_ddos                                    heart_ddos_ft
  , peptic_ulcer_ddos                             peptic_ulcer_ddos_ft
  , anemia_ddos                                   anemia_ddos_ft
  , substance_abuse_ddos                          substance_abuse_ddos_ft
  , liver_ddos                                    liver_ddos_ft
  , disabled_ddos                                 disabled_ddos_ft
  , fall_ddos                                     fall_ddos_ft
  , stroke_ddos                                   stroke_ddos_ft
  , paralysis_ddos                                paralysis_ddos_ft
  , hypertension_ddos                             hypertension_ddos_ft
  , peripheral_vascular_ddos                      peripheral_vascular_ddos_ft
  , coagulation_ddos                              coagulation_ddos_ft
  , fluid_ddos                                    fluid_ddos_ft
  , benign_prostatic_hyperplasia_ddos             benign_prostatic_hyperplasia_ddos_ft
  , tbi_ddos                                      tbi_ddos_ft
  , neurocognitive_ddos                           neurocognitive_ddos_ft
  , pulmonary_ddos                                pulmonary_ddos_ft
  , glaucoma_tc                                   glaucoma_tc_ft
  , cataract_tc                                   cataract_tc_ft
  , behavioral_health_tc                          behavioral_health_tc_ft
  , osteoporosis_tc                               osteoporosis_tc_ft
  , hiv_tc                                        hiv_tc_ft
  , transplants_tc                                transplants_tc_ft
  , obesity_tc                                    obesity_tc_ft
  , cancer_tc                                     cancer_tc_ft
  , hip_pelvic_fracture_tc                        hip_pelvic_fracture_tc_ft
  , sclerosis_tc                                  sclerosis_tc_ft
  , rheumatoid_arthritis_tc                       rheumatoid_arthritis_tc_ft
  , ckd_tc                                        ckd_tc_ft
  , hyperlipidemia_tc                             hyperlipidemia_tc_ft
  , diabetes_tc                                   diabetes_tc_ft
  , hypothyroidism_tc                             hypothyroidism_tc_ft
  , pressure_ulcer_tc                             pressure_ulcer_tc_ft
  , weight_loss_tc                                weight_loss_tc_ft
  , heart_tc                                      heart_tc_ft
  , peptic_ulcer_tc                               peptic_ulcer_tc_ft
  , anemia_tc                                     anemia_tc_ft
  , substance_abuse_tc                            substance_abuse_tc_ft
  , liver_tc                                      liver_tc_ft
  , disabled_tc                                   disabled_tc_ft
  , fall_tc                                       fall_tc_ft
  , stroke_tc                                     stroke_tc_ft
  , paralysis_tc                                  paralysis_tc_ft
  , hypertension_tc                               hypertension_tc_ft
  , peripheral_vascular_tc                        peripheral_vascular_tc_ft
  , coagulation_tc                                coagulation_tc_ft
  , fluid_tc                                      fluid_tc_ft
  , benign_prostatic_hyperplasia_tc               benign_prostatic_hyperplasia_tc_ft
  , tbi_tc                                        tbi_tc_ft
  , neurocognitive_tc                             neurocognitive_tc_ft
  , pulmonary_tc                                  pulmonary_tc_ft
  , s.submitted_at >= NOW() - '30 days'::INTERVAL submitted_last_30d
FROM
    _sfere_features sf
    JOIN sferes s ON s.id = sf.sfere_id
    JOIN patients p ON p.id = sf.patient_id
    LEFT JOIN analytics_patients ap ON ap.analytics_member_id = p.analytics_member_id
    LEFT JOIN junk.sfere_claim_chronic_conditions_20230110 cc ON sf.sfere_id = cc.sfere_id
;






-- ds_sfere_claim_chronic_conditions_20230110
SELECT * FROM junk.sfere_claim_chronic_conditions_20230110;

------------------------------------------------------------------------------------------------------------------------
/* Debug 0hr recs */
------------------------------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS _spend_cats;
CREATE TEMP TABLE _spend_cats AS
SELECT
    sf.sfere_id
  , p.payer_id
  , sf.patient_id
  , dc.analytics_category
  , SUM(ac.paid_amount) spend
FROM
    _sfere_features sf
    JOIN sferes s ON sf.sfere_id = s.id
    JOIN patients p ON s.patient_id = p.id
    JOIN analytics_claims ac ON ac.analytics_member_id = p.analytics_member_id
        AND ac.mco_id = p.payer_id
        AND ac.service_date_from BETWEEN s.submitted_at - '1 year'::INTERVAL AND s.submitted_at
    JOIN ds_category_cpt_mappings cpt ON cpt.payer_id = ac.mco_id
        AND cpt.payer_id = p.payer_id
        AND cpt.procedure_code = ac.procedure_code
        AND cpt.procedure_code IS NOT DISTINCT FROM ac.procedure_code
        AND cpt.is_current
--         and not cpt.is_deleted
    JOIN ds_categories dc ON dc.id = cpt.ds_category_id AND dc.is_current --and not dc.is_deleted
WHERE
      sf.reporting_rec_hrs_tg = 0
  AND p.payer_id NOT IN (1, 6, 7, 9)
GROUP BY
    1, 2, 3, 4
;


WITH
    tots AS ( SELECT
                  payer_id
                , SUM(spend) total
              FROM
                  _spend_cats
              GROUP BY 1 )
SELECT
    sc.payer_id
  , sc.analytics_category
  , (SUM(sc.spend) * 100.0 / t.total)::DECIMAL(16, 2) spend_pct
FROM
    _spend_cats sc
    JOIN tots t ON t.payer_id = sc.payer_id

GROUP BY
    1, 2, t.total
ORDER BY
    1, 3 DESC, 2
;



