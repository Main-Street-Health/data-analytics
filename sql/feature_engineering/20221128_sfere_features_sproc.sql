SELECT * FROM fn_ds_ml_sfere_features();


DROP TABLE IF EXISTS _scores;
CREATE TEMP TABLE _scores AS
SELECT
    sfere_id,
    aws_sagemaker.invoke_endpoint(
            'ep-ds-rec-hrs-v1-serverless-20221202-185746',
            100,
            ARRAY [
                payer_id_ft,
                alert_oriented_day_time_ft,
                has_dementia_ft,
                driving_ft,
                age_ft,
                transfer_bed_to_chair_ft,
                transfer_chair_to_standing_ft,
                mobility_ft,
                dressing_ft,
                bathing_ft,
                eating_ft,
                grooming_ft,
                toileting_ft,
                meal_prep_ft,
                shopping_ft,
                medication_management_ft,
                finances_ft,
                housework_ft,
                transportation_ft,
                comprehension_ft,
--                 0
                laundry_ft
                ]
        )::DECIMAL(16,2) as score
FROM
    fn_ds_ml_sfere_features()
;

SELECT
    AVG(ABS(sc.score - r.reporting_rec_hrs))
FROM
    _scores sc
    JOIN cpt.ds_recommendation_totals r ON r.sfere_id = sc.sfere_id
;

DROP TABLE IF EXISTS _sfere_features;
CREATE TEMP TABLE _sfere_features AS
select * from fn_ds_ml_sfere_features()

-- add in claims features for combo model
-- ds_rec_hrs_modelling_w_claims_20221205
SELECT
    sf.*
  , s.submitted_at >= NOW() - '30 days'::INTERVAL submitted_last_30d
  , cc.glaucoma_ddos                              glaucoma_ddos_ft
  , cc.cataract_ddos                              cataract_ddos_ft
  , cc.behavioral_health_ddos                     behavioral_health_ddos_ft
  , cc.osteoporosis_ddos                          osteoporosis_ddos_ft
  , cc.hiv_ddos                                   hiv_ddos_ft
  , cc.transplants_ddos                           transplants_ddos_ft
  , cc.obesity_ddos                               obesity_ddos_ft
  , cc.cancer_ddos                                cancer_ddos_ft
  , cc.hip_pelvic_fracture_ddos                   hip_pelvic_fracture_ddos_ft
  , cc.sclerosis_ddos                             sclerosis_ddos_ft
  , cc.rheumatoid_arthritis_ddos                  rheumatoid_arthritis_ddos_ft
  , cc.ckd_ddos                                   ckd_ddos_ft
  , cc.hyperlipidemia_ddos                        hyperlipidemia_ddos_ft
  , cc.diabetes_ddos                              diabetes_ddos_ft
  , cc.hypothyroidism_ddos                        hypothyroidism_ddos_ft
  , cc.pressure_ulcer_ddos                        pressure_ulcer_ddos_ft
  , cc.weight_loss_ddos                           weight_loss_ddos_ft
  , cc.heart_ddos                                 heart_ddos_ft
  , cc.peptic_ulcer_ddos                          peptic_ulcer_ddos_ft
  , cc.anemia_ddos                                anemia_ddos_ft
  , cc.substance_abuse_ddos                       substance_abuse_ddos_ft
  , cc.liver_ddos                                 liver_ddos_ft
  , cc.disabled_ddos                              disabled_ddos_ft
  , cc.fall_ddos                                  fall_ddos_ft
  , cc.stroke_ddos                                stroke_ddos_ft
  , cc.paralysis_ddos                             paralysis_ddos_ft
  , cc.hypertension_ddos                          hypertension_ddos_ft
  , cc.peripheral_vascular_ddos                   peripheral_vascular_ddos_ft
  , cc.coagulation_ddos                           coagulation_ddos_ft
  , cc.fluid_ddos                                 fluid_ddos_ft
  , cc.benign_prostatic_hyperplasia_ddos          benign_prostatic_hyperplasia_ddos_ft
  , cc.tbi_ddos                                   tbi_ddos_ft
  , cc.neurocognitive_ddos                        neurocognitive_ddos_ft
  , cc.pulmonary_ddos                             pulmonary_ddos_ft
  , cc.glaucoma_tc                                glaucoma_tc_ft
  , cc.cataract_tc                                cataract_tc_ft
  , cc.behavioral_health_tc                       behavioral_health_tc_ft
  , cc.osteoporosis_tc                            osteoporosis_tc_ft
  , cc.hiv_tc                                     hiv_tc_ft
  , cc.transplants_tc                             transplants_tc_ft
  , cc.obesity_tc                                 obesity_tc_ft
  , cc.cancer_tc                                  cancer_tc_ft
  , cc.hip_pelvic_fracture_tc                     hip_pelvic_fracture_tc_ft
  , cc.sclerosis_tc                               sclerosis_tc_ft
  , cc.rheumatoid_arthritis_tc                    rheumatoid_arthritis_tc_ft
  , cc.ckd_tc                                     ckd_tc_ft
  , cc.hyperlipidemia_tc                          hyperlipidemia_tc_ft
  , cc.diabetes_tc                                diabetes_tc_ft
  , cc.hypothyroidism_tc                          hypothyroidism_tc_ft
  , cc.pressure_ulcer_tc                          pressure_ulcer_tc_ft
  , cc.weight_loss_tc                             weight_loss_tc_ft
  , cc.heart_tc                                   heart_tc_ft
  , cc.peptic_ulcer_tc                            peptic_ulcer_tc_ft
  , cc.anemia_tc                                  anemia_tc_ft
  , cc.substance_abuse_tc                         substance_abuse_tc_ft
  , cc.liver_tc                                   liver_tc_ft
  , cc.disabled_tc                                disabled_tc_ft
  , cc.fall_tc                                    fall_tc_ft
  , cc.stroke_tc                                  stroke_tc_ft
  , cc.paralysis_tc                               paralysis_tc_ft
  , cc.hypertension_tc                            hypertension_tc_ft
  , cc.peripheral_vascular_tc                     peripheral_vascular_tc_ft
  , cc.coagulation_tc                             coagulation_tc_ft
  , cc.fluid_tc                                   fluid_tc_ft
  , cc.benign_prostatic_hyperplasia_tc            benign_prostatic_hyperplasia_tc_ft
  , cc.tbi_tc                                     tbi_tc_ft
  , cc.neurocognitive_tc                          neurocognitive_tc_ft
  , cc.pulmonary_tc                               pulmonary_tc_ft
FROM
    _sfere_features sf
    JOIN sferes s ON sf.sfere_id = s.id
    LEFT JOIN junk.sfere_claim_chronic_conditions_20221205 cc ON sf.sfere_id = cc.sfere_id
WHERE
      sf.reporting_rec_hrs_tg IS NOT NULL
  AND s.status IN ('completed', 'submitted')
  AND s.type IN ('sfere', 'scl_assessment')
;




SELECT
    pay.name
-- s.type
  , COUNT(*)                                      n
  , COUNT(*) FILTER ( WHERE cc.sfere_id ISNULL  ) n_missing_cc
  , COUNT(*) FILTER ( WHERE cc.sfere_id ISNULL  ) * 100.0 / count(*) n_missing_cc
-- , sas.bathing
-- , fn_ds_map_adl_iadl_assistance_level(sas.bathing)
FROM
    sferes s
    JOIN patients p ON p.id = s.patient_id
        join payers pay on p.payer_id = pay.id
    LEFT JOIN sfere_adls_sections sas ON s.id = sas.sfere_id
    LEFT JOIN sfere_iadls_sections sis ON s.id = sis.sfere_id
    LEFT JOIN junk.sfere_claim_chronic_conditions_20221205 cc ON s.id = cc.sfere_id
WHERE
      s.status IN ('completed', 'submitted')
GROUP BY 1
ORDER BY  2 DESC
;

--   fn_ds_map_adl_iadl_assistance_level(sas.bathing) is not null
-- and nullif(sas.bathing, '') ISNULL
;
SELECT status, count(*)
FROM
    sferes GROUP BY 1;





;
CREATE TABLE ml_models (
    id                  BIGSERIAL PRIMARY KEY,
    name                TEXT,
    description         TEXT,
    features            TEXT[],
    sage_maker_endpoint TEXT,
    inserted_at         TIMESTAMP,
    updated_at          TIMESTAMP
);


-- Question: Json or full schema by feature set type ie ml_sfere_features
CREATE TABLE ml_features (
    id          BIGSERIAL PRIMARY KEY ,
    patient_id  BIGINT REFERENCES patients(id),
    type        TEXT, -- sfere, ...
    data        JSONB,
    inserted_at TIMESTAMP,
    updated_at  TIMESTAMP
);


CREATE TABLE ml_scores (
    id            BIGSERIAL PRIMARY KEY ,
    patient_id    BIGINT REFERENCES patients(id),
    ml_model_id   BIGINT REFERENCES ml_models(id),
    ml_feature_id BIGINT REFERENCES ml_features(id),
    score         NUMERIC,
    inserted_at   TIMESTAMP,
    updated_at    TIMESTAMP
);





CREATE FUNCTION fn_sfere_ml_features (sfere_id BIGINT)
RETURNS TABLE(
    payer_id                   BIGINT,
    patient_id                 BIGINT,
    analytics_member_id        BIGINT,
    sfere_id                   BIGINT,
    total_impairment_sum       BIGINT,
    adl_impairment_sum         BIGINT,
    transfer_bed_to_chair      INTEGER,
    transfer_chair_to_standing INTEGER,
    mobility                   INTEGER,
    dressing                   INTEGER,
    bathing                    INTEGER,
    eating                     INTEGER,
    grooming                   INTEGER,
    toileting                  INTEGER,
    has_incontinence           TEXT,
    incontinence_type          TEXT,
    incontinence_frequency     TEXT,
    turn_change_position       INTEGER,
    calling_friends_and_family INTEGER,
    articulating_needs         INTEGER,
    meal_prep                  INTEGER,
    shopping                   INTEGER,
    medication_management      INTEGER,
    finances                   INTEGER,
    housework                  INTEGER,
    transportation             INTEGER,
    driving                    INTEGER,
    daily_routine_decisions    INTEGER,
    comprehension              INTEGER,
    member_opinion             INTEGER,
    cleaning                   INTEGER,
    laundry                    INTEGER,
    change_bed                 INTEGER,
    clean_kitchen              INTEGER,
    clean_home                 INTEGER,
    medical_appointments       INTEGER,
    work_school_socialize      INTEGER,
    alert_oriented_self        INTEGER,
    alert_oriented_place       INTEGER,
    alert_oriented_day_time    INTEGER,
    best_in_class_assessor     BOOLEAN,
    assessor                   TEXT,
    reporting_current_hrs      NUMERIC,
    reporting_rec_hrs          NUMERIC,
    submitted_at               TIMESTAMP,
    submitted_last_30d         BOOLEAN,
    age                        NUMERIC
) AS $$
    SELECT $1 + tab.y, $1 * tab.y FROM tab;
$$ LANGUAGE plpgsql;


CREATE FUNCTION sp_build_cpt_ds_current_services_totals() RETURNS void
    LANGUAGE plpgsql
AS
$$
/***********************************************************************************************************************
    Author : Ruben Perez
    Creation Date : 2022-10-289
    Description :
    Notes :


    Revision History :
    --------------------------------------------------------------------------------------------
    Date            Author                  Comment
    --------------------------------------------------------------------------------------------
    2022-10-24      TSelf                   Widening table to include all current service sources & add reporting logic

***********************************************************************************************************************/
DECLARE
BEGIN
    drop table if exists cpt.ds_current_service_totals;
    create table cpt.ds_current_service_totals as
