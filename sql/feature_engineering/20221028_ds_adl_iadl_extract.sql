-----------------------------
---------- adls -------------
-----------------------------
drop table if exists _adls;
create temporary table _adls as (
    select
       sfere_id,
       unnest(array['transfer_bed_to_chair',     'transfer_chair_to_standing',      'mobility',                 'dressing',       'bathing',                 'eating',       'grooming',       'toileting',                 'turn_change_position'])         as adl,
       unnest(array[transfer_bed_to_chair,        transfer_chair_to_standing,        mobility,                   dressing,         bathing,                   eating,         grooming,         toileting,                   turn_change_position])          as assistance_level,
       unnest(array[transfer_bed_to_chair_support,transfer_chair_to_standing_support,mobility_support,           dressing_support, bathing_support,           eating_support, grooming_support, toileting_support,           ''])                            as supports,
       unnest(array['',                           '',                                mobility_assistive_devices, '',               bathing_assistive_devices, '',             '',               toileting_assistive_devices, turn_change_assistive_devices]) as assistive_devices,
       unnest(array['',                           '',                                '',                         '',               '',                        '',             '',               has_incontinence::text,      ''])                            as has_incontinence,
       unnest(array['',                           '',                                '',                         '',               '',                        '',             '',               incontinence_type,           ''])                            as incontinence_type,
       unnest(array['',                           '',                                '',                         '',               '',                        '',             '',               incontinence_frequency,      ''])                            as incontinence_frequency
    from sfere_adls_sections
    group by 1
);

drop table if exists _iadls;
create temporary table _iadls as (
    select
       sfere_id,
       unnest(array['calling_friends_and_family','articulating_needs','meal_prep','shopping','medication_management','finances','housework','transportation','driving',    'daily_routine_decisions','comprehension','member_opinion','cleaning','laundry','change_bed','clean_kitchen','clean_home','medical_appointments','work_school_socialize']) as iadl,
       unnest(array[ calling_friends_and_family,  articulating_needs,  meal_prep,  shopping,  medication_management,  finances,  housework,  transportation,  driving::text,daily_routine_decisions,  comprehension,  member_opinion,  cleaning,  laundry,  change_bed,  clean_kitchen,  clean_home,  medical_appointments,  work_school_socialize]) as assistance_level
    from sfere_iadls_sections
    group by 1
);
SELECT * FROM _iadls;

DROP TABLE IF EXISTS _out;
CREATE TEMP TABLE _out AS
-- adl
SELECT
    p.payer_id
  , p.id  AS patient_id
  , p.analytics_member_id
  , a.sfere_id
  , 'adl' AS type
  , a.adl AS section
  , a.assistance_level
  , a.supports
  , a.assistive_devices
  , a.has_incontinence
  , a.incontinence_type
  , a.incontinence_frequency
  , s.completed_at
FROM
    _adls a
    JOIN sferes s ON s.id = a.sfere_id
    JOIN patients p ON p.id = s.patient_id
UNION ALL
--iadl
SELECT
    p.payer_id
  , p.id    AS patient_id
  , p.analytics_member_id
  , ia.sfere_id
  , 'iadl'  AS type
  , ia.iadl AS section
  , ia.assistance_level
  , ''
  , ''
  , ''
  , ''
  , ''
  , s.completed_at
FROM
    _iadls ia
    JOIN sferes s ON s.id = ia.sfere_id
    JOIN patients p ON p.id = s.patient_id
ORDER BY
    1, 2, 4, 5;


------------------------------------------------------------------------------------------------------------------------
/* NEW */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS _assistance_map;
CREATE TEMP TABLE _assistance_map AS
SELECT *
FROM
    ( VALUES
          ('total_assistance', 3),
          ('supervision_and_standby_assist', 1),
          ('some_assistance', 2),
          ('set_up', 1),
          ('requires_little_assistance_paid_caregiver', 1),
          ('requires_little_assistance_natural_support_and_paid_caregiver', 1),
          ('requires_little_assistance_natural_support', 1),
          ('requires_assistance_paid_caregiver', 1),
          ('requires_assistance_natural_support_and_paid_caregiver', 1),
          ('requires_assistance_natural_support', 1),
          ('requires_a_lot_assistance_paid_caregiver', 2),
          ('requires_a_lot_assistance_natural_support_and_paid_caregiver', 2),
          ('requires_a_lot_assistance_natural_support', 2),
          ('paid_caregiver_required_to_manage', 2),
          ('adult_foster_care_manages', 2),
          ('natural_support_manages', 0),
          ('natural_support_and_paid_caregiver_required_to_manage', 1),
          ('modified_independent', 0),
          ('moderate_assist', 2),
          ('minimal_assist', 2),
          ('member_manages', 0),
          ('maximal_total_assist', 3),
          ('contact_guard_assist', 1),
          ('independent', 0),
          ('decreasing', NULL),  -- member_mood
          ('increasing', NULL),  -- member_mood
          ('maintaining', NULL), -- member_mood
          ('FALSE', NULL), -- only for driving
          ('TRUE', NULL)   -- only for driving
          ) x(ass_level_text, ass_level_int);

DROP TABLE IF EXISTS _wide_out;
CREATE TEMP TABLE _wide_out AS
SELECT
    o.payer_id
  , o.patient_id
  , o.analytics_member_id
  , o.sfere_id
  , coalesce(sum(am.ass_level_int), 0)                                           total_impairment_sum
  , coalesce(sum(am.ass_level_int) filter (
          where section in ('transfer_bed_to_chair','transfer_chair_to_standing',
                            'mobility','dressing','bathing','eating','grooming',
                            'toileting','turn_change_position')
  ), 0)                                                                          adl_impairment_sum
  , MAX(am.ass_level_int) FILTER (WHERE section = 'transfer_bed_to_chair' )      transfer_bed_to_chair
  , MAX(am.ass_level_int) FILTER (WHERE section = 'transfer_chair_to_standing' ) transfer_chair_to_standing
  , MAX(am.ass_level_int) FILTER (WHERE section = 'mobility' )                   mobility
  , MAX(am.ass_level_int) FILTER (WHERE section = 'dressing' )                   dressing
  , MAX(am.ass_level_int) FILTER (WHERE section = 'bathing' )                    bathing
  , MAX(am.ass_level_int) FILTER (WHERE section = 'eating' )                     eating
  , MAX(am.ass_level_int) FILTER (WHERE section = 'grooming' )                   grooming
  , MAX(am.ass_level_int) FILTER (WHERE section = 'toileting' )                  toileting
  , MAX(o.has_incontinence) FILTER (WHERE section = 'toileting' )                has_incontinence
  , MAX(o.incontinence_type) FILTER (WHERE section = 'toileting' )               incontinence_type
  , MAX(o.incontinence_frequency) FILTER (WHERE section = 'toileting' )          incontinence_frequency
  , MAX(am.ass_level_int) FILTER (WHERE section = 'turn_change_position' )       turn_change_position
  , MAX(am.ass_level_int) FILTER (WHERE section = 'calling_friends_and_family' ) calling_friends_and_family
  , MAX(am.ass_level_int) FILTER (WHERE section = 'articulating_needs' )         articulating_needs
  , MAX(am.ass_level_int) FILTER (WHERE section = 'meal_prep' )                  meal_prep
  , MAX(am.ass_level_int) FILTER (WHERE section = 'shopping' )                   shopping
  , MAX(am.ass_level_int) FILTER (WHERE section = 'medication_management' )      medication_management
  , MAX(am.ass_level_int) FILTER (WHERE section = 'finances' )                   finances
  , MAX(am.ass_level_int) FILTER (WHERE section = 'housework' )                  housework
  , MAX(am.ass_level_int) FILTER (WHERE section = 'transportation' )             transportation
  , MAX(am.ass_level_int) FILTER (WHERE section = 'driving' )                    driving
  , MAX(am.ass_level_int) FILTER (WHERE section = 'daily_routine_decisions' )    daily_routine_decisions
  , MAX(am.ass_level_int) FILTER (WHERE section = 'comprehension' )              comprehension
  , MAX(am.ass_level_int) FILTER (WHERE section = 'member_opinion' )             member_opinion
  , MAX(am.ass_level_int) FILTER (WHERE section = 'cleaning' )                   cleaning
  , MAX(am.ass_level_int) FILTER (WHERE section = 'laundry' )                    laundry
  , MAX(am.ass_level_int) FILTER (WHERE section = 'change_bed' )                 change_bed
  , MAX(am.ass_level_int) FILTER (WHERE section = 'clean_kitchen' )              clean_kitchen
  , MAX(am.ass_level_int) FILTER (WHERE section = 'clean_home' )                 clean_home
  , MAX(am.ass_level_int) FILTER (WHERE section = 'medical_appointments' )       medical_appointments
  , MAX(am.ass_level_int) FILTER (WHERE section = 'work_school_socialize' )      work_school_socialize
FROM
    _out o
    JOIN _assistance_map am ON am.ass_level_text = o.assistance_level
GROUP BY
    o.payer_id
  , o.patient_id
  , o.analytics_member_id
  , o.sfere_id
;


-- best in class assessors
DROP TABLE IF EXISTS _best_ass;
CREATE TEMP TABLE _best_ass AS
SELECT
    u.id user_id
  , u.first_name
  , u.last_name
  , x.fn
  , x.ln
FROM
    ( VALUES
          ('Danielle', 'Spangler'),
          ('Patricia', 'Gilmore'),
          ('Beth', 'Saunders'),
          ('Karen', 'Kruse'),
          ('Billi Jo', 'Ehrenberger'),
          ('Megan', 'Laher'),
          ('Kelly', 'Nash'),
          ('Brandy', 'Richardson'),
          ('Lauren', 'Jezek') ) x(fn, ln)
    LEFT JOIN users u ON u.first_name ~* x.fn AND u.last_name ~* x.ln
;



-- DROP TABLE IF EXISTS junk.sfere_claim_chronic_conditions_20221118;
-- CREATE  TABLE junk.sfere_claim_chronic_conditions_20221121 AS
CREATE  TABLE junk.sfere_claim_chronic_conditions_20221205 AS
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
    JOIN patients p ON s.patient_id = p.id
    JOIN analytics_claims ac ON p.analytics_member_id = ac.analytics_member_id
        AND p.payer_id = ac.mco_id
        AND ac.service_date_from BETWEEN s.submitted_at - '13 months'::INTERVAL AND s.submitted_at - '1 month'::INTERVAL
        AND ac.service_type !~* 'hcbs|home'
        AND ac.paid_amount > 0
        and not ac.is_rx
    JOIN analytics_claim_diagnoses acd ON ac.uuid = acd.analytics_claim_uuid
    JOIN junk.ref_chronics_20221109 ni ON ni.icd10 = acd.diagnosis_code
GROUP BY
    1
;
-- create table junk.sfere_claim_chronic_conditions_20221109 as select * from _claim_condis;
-- cached in junk table, takes a while
-- SELECT * FROM junk.sfere_claim_chronic_conditions_20221121;

DROP TABLE IF EXISTS _final_output;
CREATE TEMP TABLE _final_output AS
SELECT distinct
    wo.*
  , CASE WHEN alert_oriented_self THEN 1 WHEN NOT alert_oriented_self THEN 0 END         alert_oriented_self
  , CASE WHEN alert_oriented_place THEN 1 WHEN NOT alert_oriented_place THEN 0 END       alert_oriented_place
  , CASE WHEN alert_oriented_day_time THEN 1 WHEN NOT alert_oriented_day_time THEN 0 END alert_oriented_day_time
  , ba.user_id IS NOT NULL                       AS                                      best_in_class_assessor
  , u.full_name                                                                          assessor
  , cur.reporting_current_hrs
  , rec.reporting_rec_hrs
  , s.submitted_at
  , s.submitted_at > NOW() - '30 days'::INTERVAL AS                                      submitted_last_30d
  , EXTRACT(YEARS FROM AGE(p.dob))                                                       age
FROM
    _wide_out wo
    JOIN sferes s ON s.id = wo.sfere_id
    LEFT JOIN sfere_cognitive_sections cs ON cs.sfere_id = s.id
    JOIN cpt.ds_current_service_totals cur ON wo.sfere_id = cur.sfere_id
    JOIN cpt.ds_recommendation_totals rec ON rec.sfere_id = wo.sfere_id
    JOIN patients p ON p.id = wo.patient_id
    LEFT JOIN _best_ass ba ON ba.user_id = s.submitted_by_id
    JOIN users u ON u.id = s.submitted_by_id
;
SELECT distinct member_opinion
FROM
    sfere_iadls_sections;

delete FROM _final_output
WHERE sfere_id  in  ( 16704,7616,18074,14420,14723,10413,14496,11442,14532 ) -- dupes
;


-- CREATE TABLE junk.ds_patient_matching_features_20221118
-- AS
-- SELECT * FROM _final_output;


-- ds_rec_hrs_modelling_20221118
SELECT *
FROM
    junk.ds_patient_matching_features_20221118;
SELECT count(DISTINCT patient_id), count(distinct sfere_id)
FROM
    junk.ds_patient_matching_features_20221118

;
SELECT *
FROM
    payers;


-- ds_rec_hrs_modelling_w_claims_20221205
select fo.*
  , cc.glaucoma_ddos
  , cc.cataract_ddos
  , cc.behavioral_health_ddos
  , cc.osteoporosis_ddos
  , cc.hiv_ddos
  , cc.transplants_ddos
  , cc.obesity_ddos
  , cc.cancer_ddos
  , cc.hip_pelvic_fracture_ddos
  , cc.sclerosis_ddos
  , cc.rheumatoid_arthritis_ddos
  , cc.ckd_ddos
  , cc.hyperlipidemia_ddos
  , cc.diabetes_ddos
  , cc.hypothyroidism_ddos
  , cc.pressure_ulcer_ddos
  , cc.weight_loss_ddos
  , cc.heart_ddos
  , cc.peptic_ulcer_ddos
  , cc.anemia_ddos
  , cc.substance_abuse_ddos
  , cc.liver_ddos
  , cc.disabled_ddos
  , cc.fall_ddos
  , cc.stroke_ddos
  , cc.paralysis_ddos
  , cc.hypertension_ddos
  , cc.peripheral_vascular_ddos
  , cc.coagulation_ddos
  , cc.fluid_ddos
  , cc.benign_prostatic_hyperplasia_ddos
  , cc.tbi_ddos
  , cc.neurocognitive_ddos
  , cc.pulmonary_ddos
  , cc.glaucoma_tc
  , cc.cataract_tc
  , cc.behavioral_health_tc
  , cc.osteoporosis_tc
  , cc.hiv_tc
  , cc.transplants_tc
  , cc.obesity_tc
  , cc.cancer_tc
  , cc.hip_pelvic_fracture_tc
  , cc.sclerosis_tc
  , cc.rheumatoid_arthritis_tc
  , cc.ckd_tc
  , cc.hyperlipidemia_tc
  , cc.diabetes_tc
  , cc.hypothyroidism_tc
  , cc.pressure_ulcer_tc
  , cc.weight_loss_tc
  , cc.heart_tc
  , cc.peptic_ulcer_tc
  , cc.anemia_tc
  , cc.substance_abuse_tc
  , cc.liver_tc
  , cc.disabled_tc
  , cc.fall_tc
  , cc.stroke_tc
  , cc.paralysis_tc
  , cc.hypertension_tc
  , cc.peripheral_vascular_tc
  , cc.coagulation_tc
  , cc.fluid_tc
  , cc.benign_prostatic_hyperplasia_tc
  , cc.tbi_tc
  , cc.neurocognitive_tc
  , cc.pulmonary_tc
from _final_output fo
LEFT JOIN junk.sfere_claim_chronic_conditions_20221205 cc ON fo.sfere_id = cc.sfere_id;

SELECT avg(abs(fo.reporting_rec_hrs -  dl.pred))
, avg(fo.reporting_rec_hrs)
, avg(dl.pred)
FROM
    junk.ds_hrs_lm_sfere_preds_20221115 dl
join _final_output fo ON dl.sfere_id = fo.sfere_id
;
SELECT sfere_id, count(*)
FROM
    cpt.ds_recommendation_totals
GROUP BY 1 having count(*) > 1
;