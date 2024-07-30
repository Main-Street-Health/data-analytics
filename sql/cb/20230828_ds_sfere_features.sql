drop function fn_ds_ml_sfere_features_v2_fl;
CREATE or replace FUNCTION fn_ds_ml_sfere_features_v2_fl(_sfere_ids bigint[] DEFAULT NULL::bigint[])
    RETURNS TABLE(sfere_id bigint, sfere_type int, patient_id bigint, payer_id_ft bigint, group_id_ft bigint, age_ft numeric, transfer_bed_to_chair_ft integer, mobility_ft integer, dressing_ft integer, bathing_ft integer, eating_ft integer, grooming_ft integer, toileting_ft integer, turn_change_position_ft integer, has_incontinence_ft integer, incontinence_type_ft integer, incontinence_frequency_ft integer,
     bathing_support_days_natural_ft int,dressing_support_days_natural_ft int,eating_support_days_natural_ft int,grooming_support_days_natural_ft int,toileting_support_days_natural_ft int,mobility_support_days_natural_ft int,transfer_bed_to_chair_support_days_natural_ft int,turn_change_position_support_days_natural_ft int,bathing_transfers_support_days_natural_ft int,dressing_lower_support_days_natural_ft int,hair_support_days_natural_ft int,mobility_outside_support_days_natural_ft int,
     calling_friends_and_family_ft integer, articulating_needs_ft integer, meal_prep_ft integer, shopping_ft integer, medication_management_ft integer, finances_ft integer, housework_ft integer, transportation_ft integer, daily_routine_decisions_ft integer, comprehension_ft integer, member_opinion_ft integer, cleaning_ft integer, laundry_ft integer, change_bed_ft integer, clean_kitchen_ft integer, clean_home_ft integer, medical_appointments_ft integer, work_school_socialize_ft integer, driving_ft integer,
     calling_friends_and_family_support_days_natural_ft int,finances_support_days_natural_ft int,laundry_support_days_natural_ft int,housework_support_days_natural_ft int,meal_prep_support_days_natural_ft int,medication_management_support_days_natural_ft int,shopping_support_days_natural_ft int,
     alert_oriented_self_ft integer, alert_oriented_place_ft integer, alert_oriented_day_time_ft integer, has_alzheimers_ft integer, has_dementia_ft integer, reporting_current_hrs_ft numeric, reporting_rec_hrs_tg numeric)
    LANGUAGE plpgsql


AS
$$
BEGIN

    IF _sfere_ids ISNULL
    THEN
        _sfere_ids := ( SELECT ARRAY_AGG(id) FROM sferes );
    END IF;

--     SELECT status, type, count(*) FROM sferes GROUP BY 1,2 order by 3 desc;
    DROP TABLE IF EXISTS _sferes_of_interest;
    CREATE TEMP TABLE _sferes_of_interest AS
--         SELECT s.id, s.type  FROM sferes s join patients p on s.patient_id = p.id and p.payer_id in (28,4,21) -- FL ONLY where s.status in ('submitted', 'completed') and s.type in ('sfere', 'sfere_v2', 'sfere_v3') ;
--     AND s.status IN ('submitted', 'completed')
--       AND s.type IN ('sfere', 'sfere_v2', 'sfere_v3');
    SELECT
        s.id
      , s.type
    FROM
        sferes s
    join patients p on s.patient_id = p.id and p.payer_id in (28,4,21) -- FL ONLY
    WHERE
          s.id = ANY (_sfere_ids)
      AND s.status IN ('submitted', 'completed')
      AND s.type IN ('sfere', 'sfere_v2', 'sfere_v3');

    create index on _sferes_of_interest(id);

--     SELECT type, count(*) FROM _sferes_of_interest GROUP BY 1;
    ------------------------------------------------------------------------------------------------------------------------
    /* sfere v1 */
    ------------------------------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS _adls;
    CREATE TEMP TABLE _adls AS
    SELECT
        adls.sfere_id
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := transfer_bed_to_chair)      transfer_bed_to_chair
--       , fn_ds_map_adl_iadl_assistance_level(assistance_level := transfer_chair_to_standing) transfer_chair_to_standing
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := mobility)                   mobility
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := dressing)                   dressing
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := bathing)                    bathing
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := eating)                     eating
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := grooming)                   grooming
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := toileting)                  toileting
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := turn_change_position)       turn_change_position
      , CASE WHEN has_incontinence THEN 1 WHEN has_incontinence IS FALSE THEN 0 END         has_incontinence
      , CASE WHEN incontinence_type = 'bladder'           THEN 0
             WHEN incontinence_type = 'bowel'             THEN 1
             WHEN incontinence_type = 'bladder_and_bowel' THEN 2
            END                                                                             incontinence_type
      , CASE
            WHEN incontinence_frequency = 'occasional_incontinence' THEN 0
            WHEN incontinence_frequency = 'nighttime_incontinence'  THEN 1
            WHEN incontinence_frequency = 'always_incontinent'      THEN 2 END              incontinence_frequency
      , transfer_bed_to_chair_support_days_natural
      , mobility_support_days_natural
      , mobility_outside_support_days_natural
      , dressing_support_days_natural
      , dressing_lower_support_days_natural
      , bathing_support_days_natural
      , bathing_transfers_support_days_natural
      , eating_support_days_natural
      , grooming_support_days_natural
      , hair_support_days_natural
      , toileting_support_days_natural
      , turn_change_position_support_days_natural
    FROM
        sfere_adls_sections adls
    join _sferes_of_interest soi on soi.id = adls.sfere_id and soi.type = 'sfere'
    ;

    DROP TABLE IF EXISTS _iadls;
    CREATE TEMP TABLE _iadls AS
    SELECT
        iadls.sfere_id
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := calling_friends_and_family) calling_friends_and_family
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := articulating_needs)         articulating_needs
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := meal_prep)                  meal_prep
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := shopping)                   shopping
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := medication_management)      medication_management
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := finances)                   finances
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := housework)                  housework
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := transportation)             transportation
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := daily_routine_decisions)    daily_routine_decisions
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := comprehension)              comprehension
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := cleaning)                   cleaning
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := laundry)                    laundry
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := change_bed)                 change_bed
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := clean_kitchen)              clean_kitchen
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := clean_home)                 clean_home
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := medical_appointments)       medical_appointments
      , fn_dsml_map_adl_iadl_assistance_level(assistance_level := work_school_socialize)      work_school_socialize
      , CASE WHEN driving THEN 1 WHEN driving IS FALSE THEN 0 END                           driving
      , CASE WHEN member_opinion = 'decreasing'  THEN 0
             WHEN member_opinion = 'maintaining' THEN 1
             WHEN member_opinion = 'increasing'  THEN 2 END                                 member_opinion
      , calling_friends_and_family_support_days_natural
      , meal_prep_support_days_natural
      , shopping_support_days_natural
      , medication_management_support_days_natural
      , finances_support_days_natural
      , housework_support_days_natural
      , laundry_support_days_natural
      , medical_appointments_support_days_natural
    FROM
        sfere_iadls_sections iadls
        join _sferes_of_interest soi on soi.id = iadls.sfere_id and soi.type = 'sfere'
;

    ------------------------------------------------------------------------------------------------------------------------
    /* sfere v2 v3*/
    ------------------------------------------------------------------------------------------------------------------------

    ------------------------------------------------------------------------------------------------------------------------
    /* MAKE ADLs table up top with all columns */
    ------------------------------------------------------------------------------------------------------------------------
    WITH
        tall AS ( SELECT DISTINCT
                      s.id
                    , ddt.id category
                    , ddi.score
                    , lt.support_days_natural
                  FROM
                      _sferes_of_interest s
                      JOIN sfere_daily_living_tasks lt ON s.id = lt.sfere_id
                      JOIN ds_dl_payer_task_impairments pti ON lt.payer_task_impairment_id = pti.id
                      JOIN public.ds_dl_impairments ddi ON pti.impairment_id = ddi.id
                      JOIN public.ds_dl_payer_tasks ddpt ON lt.payer_task_id = ddpt.id
                      JOIN public.ds_dl_tasks ddt ON ddpt.task_id = ddt.id
                  WHERE
                        s.type in ('sfere_v2', 'sfere_v3')
                    AND lt.type = 'adl' )
    , wide as (
        SELECT
            soi.id                                                                              sfere_id
          , bathing.score                                                                       bathing
          , dressing.score                                                                      dressing
          , eating.score                                                                        eating
          , grooming.score                                                                      grooming
          , toileting.score                                                                     toileting
          , mobility.score                                                                      mobility
          , transfer_bed_to_chair.score                                                         transfer_bed_to_chair
          , turn_change_position.score                                                          turn_change_position
          , bathing_grooming_hygiene.score                                                      bathing_grooming_hygiene
          , bathing_transfers.score                                                             bathing_transfers
          , dressing_lower.score                                                                dressing_lower
          , hair.score                                                                          hair
          , mobility_outside.score                                                              mobility_outside
          , transferring_ambulation_mobility.score                                              transferring_ambulation_mobility
          , wound_care.score                                                                    wound_care
          , bathing.support_days_natural                                                        bathing_support_days_natural
          , dressing.support_days_natural                                                       dressing_support_days_natural
          , eating.support_days_natural                                                         eating_support_days_natural
          , grooming.support_days_natural                                                       grooming_support_days_natural
          , toileting.support_days_natural                                                      toileting_support_days_natural
          , mobility.support_days_natural                                                       mobility_support_days_natural
          , transfer_bed_to_chair.support_days_natural                                          transfer_bed_to_chair_support_days_natural
          , turn_change_position.support_days_natural                                           turn_change_position_support_days_natural
          , bathing_grooming_hygiene.support_days_natural                                       bathing_grooming_hygiene_support_days_natural
          , bathing_transfers.support_days_natural                                              bathing_transfers_support_days_natural
          , dressing_lower.support_days_natural                                                 dressing_lower_support_days_natural
          , hair.support_days_natural                                                           hair_support_days_natural
          , mobility_outside.support_days_natural                                               mobility_outside_support_days_natural
          , transferring_ambulation_mobility.support_days_natural                               transferring_ambulation_mobility_support_days_natural
          , wound_care.support_days_natural                                                     wound_care_support_days_natural
          , CASE WHEN sts.has_incontinence THEN 1 WHEN sts.has_incontinence IS FALSE THEN 0 END has_incontinence
          , CASE WHEN sts.incontinence_type = 'bladder'           THEN 0
                 WHEN sts.incontinence_type = 'bowel'             THEN 1
                 WHEN sts.incontinence_type = 'bladder_and_bowel' THEN 2
                END                                                                             incontinence_type
          , CASE
                WHEN sts.incontinence_frequency = 'occasional_incontinence' THEN 0
                WHEN sts.incontinence_frequency = 'nighttime_incontinence'  THEN 1
                WHEN sts.incontinence_frequency = 'always_incontinent'      THEN 2 END          incontinence_frequency
        FROM
            _sferes_of_interest soi
            LEFT JOIN sfere_therapies_sections sts on sts.sfere_id = soi.id
            LEFT JOIN tall bathing ON bathing.id = soi.id                                                      AND bathing.category                          = 'bathing'
            LEFT JOIN tall dressing ON dressing.id = soi.id                                                    AND dressing.category                         = 'dressing'
            LEFT JOIN tall eating ON eating.id = soi.id                                                        AND eating.category                           = 'eating'
            LEFT JOIN tall grooming ON grooming.id = soi.id                                                    AND grooming.category                         = 'grooming'
            LEFT JOIN tall toileting ON toileting.id = soi.id                                                  AND toileting.category                        = 'toileting'
            LEFT JOIN tall mobility ON mobility.id = soi.id                                                    AND mobility.category                         = 'mobility'
            LEFT JOIN tall transfer_bed_to_chair ON transfer_bed_to_chair.id = soi.id                          AND transfer_bed_to_chair.category            = 'transfer_bed_to_chair'
            LEFT JOIN tall turn_change_position ON turn_change_position.id = soi.id                            AND turn_change_position.category             = 'turn_change_position'
            LEFT JOIN tall bathing_grooming_hygiene ON bathing_grooming_hygiene.id = soi.id                    AND bathing_grooming_hygiene.category         = 'bathing_grooming_hygiene'
            LEFT JOIN tall bathing_transfers ON bathing_transfers.id = soi.id                                  AND bathing_transfers.category                = 'bathing_transfers'
            LEFT JOIN tall dressing_lower ON dressing_lower.id = soi.id                                        AND dressing_lower.category                   = 'dressing_lower'
            LEFT JOIN tall hair ON hair.id = soi.id                                                            AND hair.category                             = 'hair'
            LEFT JOIN tall mobility_outside ON mobility_outside.id = soi.id                                    AND mobility_outside.category                 = 'mobility_outside'
            LEFT JOIN tall transferring_ambulation_mobility ON transferring_ambulation_mobility.id = soi.id    AND transferring_ambulation_mobility.category = 'transferring_ambulation_mobility'
            LEFT JOIN tall wound_care ON wound_care.id = soi.id                                                AND wound_care.category                       = 'wound_care'
        WHERE soi.type in ('sfere_v2', 'sfere_v3')
    )
    INSERT
    INTO
        _adls (sfere_id, transfer_bed_to_chair,  mobility, dressing, bathing, eating, grooming, toileting, turn_change_position, has_incontinence, incontinence_type, incontinence_frequency, bathing_support_days_natural,dressing_support_days_natural,eating_support_days_natural,grooming_support_days_natural,toileting_support_days_natural,mobility_support_days_natural,transfer_bed_to_chair_support_days_natural,turn_change_position_support_days_natural,bathing_transfers_support_days_natural,dressing_lower_support_days_natural,hair_support_days_natural,mobility_outside_support_days_natural)
    select
        w.sfere_id,
        w.transfer_bed_to_chair,
        w.mobility,
        w.dressing,
        w.bathing,
        w.eating,
        w.grooming,
        w.toileting,
        w.turn_change_position,
        w.has_incontinence,
        w.incontinence_type,
        w.incontinence_frequency,
        w.bathing_support_days_natural,
        w.dressing_support_days_natural,
        w.eating_support_days_natural,
        w.grooming_support_days_natural,
        w.toileting_support_days_natural,
        w.mobility_support_days_natural,
        w.transfer_bed_to_chair_support_days_natural,
        w.turn_change_position_support_days_natural,
        w.bathing_transfers_support_days_natural,
        w.dressing_lower_support_days_natural,
        w.hair_support_days_natural,
        w.mobility_outside_support_days_natural
    from wide w
    ;

    ------------------------------------------------------------------------------------------------------------------------
    /* iadls v2+3 */
    ------------------------------------------------------------------------------------------------------------------------

    WITH
        tall AS ( SELECT DISTINCT
                      s.id
                    , ddt.id category
                    , ddi.score
                    , lt.support_days_natural
                  FROM
                      _sferes_of_interest s
                      JOIN sfere_daily_living_tasks lt ON s.id = lt.sfere_id
                      JOIN ds_dl_payer_task_impairments pti ON lt.payer_task_impairment_id = pti.id
                      JOIN public.ds_dl_impairments ddi ON pti.impairment_id = ddi.id
                      JOIN public.ds_dl_payer_tasks ddpt ON lt.payer_task_id = ddpt.id
                      JOIN public.ds_dl_tasks ddt ON ddpt.task_id = ddt.id
                  WHERE
                        s.type in ('sfere_v2', 'sfere_v3')
                    AND lt.type = 'iadl' )
    , wide as (
        SELECT
            soi.id                                           sfere_id
          , calling_friends_and_family.score                 calling_friends_and_family
          , finances.score                                   finances
          , laundry.score                                    laundry
          , housework.score                                  housework
          , meal_prep.score                                  meal_prep
          , medication_management.score                      medication_management
          , shopping.score                                   shopping
          , transportation.score                             transportation
          , calling_friends_and_family.support_days_natural  calling_friends_and_family_support_days_natural
          , finances.support_days_natural                    finances_support_days_natural
          , laundry.support_days_natural                     laundry_support_days_natural
          , housework.support_days_natural                   housework_support_days_natural
          , meal_prep.support_days_natural                   meal_prep_support_days_natural
          , medication_management.support_days_natural       medication_management_support_days_natural
          , shopping.support_days_natural                    shopping_support_days_natural
          , transportation.support_days_natural              transportation_support_days_natural
        FROM
            _sferes_of_interest soi
            LEFT JOIN tall calling_friends_and_family ON calling_friends_and_family.id = soi.id AND calling_friends_and_family.category = 'calling_friends_and_family'
            LEFT JOIN tall finances ON finances.id = soi.id                                     AND finances.category = 'finances'
            LEFT JOIN tall laundry ON laundry.id = soi.id                                       AND laundry.category = 'heavy_chores'
            LEFT JOIN tall housework ON housework.id = soi.id                                   AND housework.category = 'housework'
            LEFT JOIN tall meal_prep ON meal_prep.id = soi.id                                   AND meal_prep.category = 'meal_prep'
            LEFT JOIN tall medication_management ON medication_management.id = soi.id           AND medication_management.category = 'medication_management'
            LEFT JOIN tall shopping ON shopping.id = soi.id                                     AND shopping.category = 'shopping'
            LEFT JOIN tall transportation ON transportation.id = soi.id                         AND transportation.category = 'transportation'
        WHERE soi.type in ('sfere_v2', 'sfere_v3')
    )
    INSERT
    INTO
        _iadls (sfere_id, calling_friends_and_family, meal_prep, shopping, medication_management, finances, housework, transportation, laundry, calling_friends_and_family_support_days_natural,finances_support_days_natural,laundry_support_days_natural,housework_support_days_natural,meal_prep_support_days_natural,medication_management_support_days_natural,shopping_support_days_natural)



    select
    w.sfere_id, calling_friends_and_family, meal_prep, shopping, medication_management,
                finances, housework, transportation, laundry
    , w.calling_friends_and_family_support_days_natural
    , w.finances_support_days_natural
    , w.laundry_support_days_natural
    , w.housework_support_days_natural
    , w.meal_prep_support_days_natural
    , w.medication_management_support_days_natural
    , w.shopping_support_days_natural
    FROM wide w
;

    ------------------------------------------------------------------------------------------------------------------------
    /* cognitive is same across v1,2,3 */
    ------------------------------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS _cog;
    CREATE TEMP TABLE _cog AS
    SELECT
        c.sfere_id
      , CASE WHEN alert_oriented_self THEN 1 WHEN alert_oriented_self IS FALSE THEN 0 END   alert_oriented_self
      , CASE WHEN alert_oriented_place THEN 1 WHEN alert_oriented_place IS FALSE THEN 0 END alert_oriented_place
      , CASE WHEN alert_oriented_day_time          THEN 1
             WHEN alert_oriented_day_time IS FALSE THEN 0 END                               alert_oriented_day_time
      , CASE WHEN has_alzheimers THEN 1 WHEN has_alzheimers IS FALSE THEN 0 END             has_alzheimers
      , CASE WHEN has_dementia THEN 1 WHEN has_dementia IS FALSE THEN 0 END                 has_dementia
    FROM
        sfere_cognitive_sections c
        join _sferes_of_interest soi on soi.id = c.sfere_id
;

    -- may need mapping: state payer group to generic group id
--     DROP TABLE IF EXISTS _grps_encoding;
--     CREATE TEMP TABLE _grps_encoding AS
--     SELECT *
--     FROM
--         ( VALUES
--               -- ITC
--               (8, -1, -1), -- 	not incl, Medicare
--               (8, 0, 0),    -- 	other, Other Medicaid
--               (8, 1, 0),    -- 	other, Other Medicaid
--               (8, 3, 0),    -- 	other, 3 - health and disability
--               (8, 5, 0),    -- 	other, 5 - physical disability
--               (8, 7, 0),    -- 	other, 7 - physical disability/health and disability
--               (8, 8, 0),    -- 	other, 8 - physical disability/health and disability/aids
--               (8, 4, 1),    -- 	scl, 4 - intellectual disability
--               (8, 2, 1),    -- 	scl, 2 - brain injury
--               (8, 6, 2)     -- 	elderly, 6 - elderly
--         ) x(payer_id, grp_id, encoded_grp_id);



    RETURN QUERY
        SELECT
            s.id
          , case when s.type = 'sfere' then 1 when s.type = 'sfere_v2' then 2 when s.type = 'sfere_v3' then 3 end sfere_type
          , p.id
          , p.payer_id
          , ap.grp_id
          , EXTRACT(YEARS FROM AGE(p.dob))
            -- adls
          , a.transfer_bed_to_chair
          , a.mobility
          , a.dressing
          , a.bathing
          , a.eating
          , a.grooming
          , a.toileting
          , a.turn_change_position
          , a.has_incontinence
          , a.incontinence_type
          , a.incontinence_frequency
          , a.bathing_support_days_natural
          , a.dressing_support_days_natural
          , a.eating_support_days_natural
          , a.grooming_support_days_natural
          , a.toileting_support_days_natural
          , a.mobility_support_days_natural
          , a.transfer_bed_to_chair_support_days_natural
          , a.turn_change_position_support_days_natural
          , a.bathing_transfers_support_days_natural
          , a.dressing_lower_support_days_natural
          , a.hair_support_days_natural
          , a.mobility_outside_support_days_natural
            -- iadls
          , ia.calling_friends_and_family
          , ia.articulating_needs
          , ia.meal_prep
          , ia.shopping
          , ia.medication_management
          , ia.finances
          , ia.housework
          , ia.transportation
          , ia.daily_routine_decisions
          , ia.comprehension
          , ia.member_opinion
          , ia.cleaning
          , ia.laundry
          , ia.change_bed
          , ia.clean_kitchen
          , ia.clean_home
          , ia.medical_appointments
          , ia.work_school_socialize
          , ia.driving
          , ia.calling_friends_and_family_support_days_natural
          , ia.finances_support_days_natural
          , ia.laundry_support_days_natural
          , ia.housework_support_days_natural
          , ia.meal_prep_support_days_natural
          , ia.medication_management_support_days_natural
          , ia.shopping_support_days_natural
            -- cog
          , cog.alert_oriented_self
          , cog.alert_oriented_place
          , cog.alert_oriented_day_time
          , cog.has_alzheimers
          , cog.has_dementia
          , cur.reporting_current_hrs
          , rec.reporting_rec_hrs
        FROM
            sferes s
            JOIN _sferes_of_interest soi on soi.id = s.id
            JOIN patients p ON p.id = s.patient_id
            JOIN analytics_patients ap on ap.analytics_member_id = p.analytics_member_id
            LEFT JOIN _adls a ON s.id = a.sfere_id
            LEFT JOIN _iadls ia ON s.id = ia.sfere_id
            LEFT JOIN _cog cog ON s.id = cog.sfere_id
                -- TODO: Move cpt.ds_current_service_totals to authorized hours once app dev is complete
            LEFT JOIN cpt.ds_current_service_totals cur ON s.id = cur.sfere_id
                -- TODO: Move this to separate sproc specific to training data
            LEFT JOIN cpt.ds_recommendation_totals rec ON s.id = rec.sfere_id
        WHERE
              s.id = ANY (_sfere_ids)
              -- remove dupes in rec
          AND s.id NOT IN (16704, 7016, 7616, 18074, 14420, 14723, 10413, 14496, 11442, 14532, 17758, 14670, 17256);



    --     IF ass_level_int ISNULL AND assistance_level IS NOT NULL
--     THEN
--         RAISE EXCEPTION 'Nonexistent assistance_level --> %', assistance_level
--             USING HINT = 'If new level it must be added to fn_ds_map_adl_iadl_assistance_level';
--     ELSE
--         RETURN ass_level_int;
--     END IF;
END;

$$;

ALTER FUNCTION fn_dsml_sfere_features(BIGINT[]) OWNER TO postgres;

GRANT EXECUTE ON FUNCTION fn_dsml_sfere_features(BIGINT[]) TO member_doc_readwrite;

DROP TABLE IF EXISTS _sfere_features;
CREATE TEMP TABLE _sfere_features AS
SELECT * FROM fn_ds_ml_sfere_features_v2_fl();
-- 88k
SELECT sfere_type, count(*)
FROM
    _sfere_features GROUP BY 1;

with mults as ( SELECT
                    patient_id
                  , COUNT(*)
                FROM
                    _sfere_features
                GROUP BY 1
                HAVING
                    COUNT(*) > 1 )
SELECT *
FROM
    _sfere_features sf
join mults m on m.patient_id = sf.patient_id
order by sf.patient_id
;

SELECT *
FROM
    _sfere_features;

SELECT *
FROM
    ;

SELECT *
FROM
    _sfere_features w;
select * from fn_ds_ml_sfere_features_v2_fl(array[76130]);
