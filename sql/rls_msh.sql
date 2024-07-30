SELECT
    distinct r.*
FROM
     roles r
WHERE
    r.name = 'galen_user';

-- SET session_replication_role = replica;
-- SET session_replication_role = DEFAULT;

CREATE OR REPLACE FUNCTION access_patient_ids() RETURNS TABLE ( patient_id BIGINT) AS $$
DECLARE
    _is_galen_user BOOLEAN := FALSE;
BEGIN

    _is_galen_user := ( SELECT
                            COUNT(1)
                        FROM
                            users u
                            JOIN user_roles ur ON u.id = ur.user_id
                            JOIN roles r ON r.id = ur.role_id
                        WHERE
--                               r.id = 5452  -- prd
                              r.name = 'galen_user'
                          AND u.id = CURRENT_SETTING('security.user_id'):: BIGINT ) > 0;

    IF _is_galen_user
    THEN
        RETURN QUERY SELECT
                         p.id
                     FROM
                         patients p
                         JOIN patient_referring_partners prp ON p.id = prp.patient_id
                         JOIN referring_partners rp ON prp.referring_partner_id = rp.id
                     WHERE
--                          rp.id = 279; -- prd
        rp.name = 'Galen Medical';
    ELSE
        RETURN QUERY SELECT id FROM patients;
    END IF;

END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;


SELECT DISTINCT
    rp.id, name
FROM
--     patients p
--     JOIN patient_referring_partners prp ON p.id = prp.patient_id
    referring_partners rp
order by id desc
-- WHERE
--     rp.name = 'Galen Medical';



ALTER TABLE patients ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE patients disABLE ROW LEVEL SECURITY;
-- DROP POLICY user_patients ON patients;
-- DROP FUNCTION access_patient_ids;

CREATE POLICY user_patients ON patients TO application_user
USING (id IN (SELECT access_patient_ids()));

CREATE POLICY msh_pg_admin_patients ON patients TO msh_pg_admins USING (true);
CREATE POLICY msh_pg_readonly_patients ON patients TO msh_pg_readonly USING (true);
CREATE POLICY msh_pg_reporting_patients ON patients TO msh_pg_reporting USING (true);
CREATE POLICY tableau_patients ON patients TO tableau USING (true);

SELECT *
FROM
    pg_roles;

UPDATE pg_catalog.pg_roles SET rolname = 'msh_pg_readonly';
UPDATE pg_catalog.pg_roles SET rolname = 'msh_pg_reporting';



BEGIN;
SELECT set_config('security.user_id', '1550', true);
-- SELECT set_config('security.user_id', '4834', true);
-- select  CURRENT_SETTING('security.user_id');
SELECT *
FROM
    access_patient_ids();

SET LOCAL ROLE application_user;
END;
ROLLBACK ;

grant usage on schema rpt to application_user;
grant all on all tables in schema rpt to application_user;
grant usage on all sequences in schema rpt to application_user;
alter default privileges in schema rpt grant all on tables to application_user;
alter default privileges in schema rpt grant usage on sequences to application_user;


begin;
SET LOCAL ROLE application_user;
SELECT *
FROM
    rpt.ds_waterfall_alignment;
END;

ROLLBACK ;

------------------------------------------------------------------------------------------------------------------------
/* dev setup */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS _galen_users;
CREATE TEMP TABLE _galen_users AS
WITH
    ins AS (
        INSERT INTO users(first_name, last_name, email, phone, username, created_by_id, modified_by_id, is_deleted,
                          deleted_at,
                          deleted_by_id, inserted_at, updated_at, image_url, personal_meeting_id, time_zone,
                          zoom_user_id, okta_id,
                          prefix, suffix, contract, reporting_group, emr_origin_user_id, scheduling_notes)
            SELECT
                'galen'                                 first_name
              , 'user_' || i::TEXT                      last_name
              , 'galen_user_' || i::TEXT || '@test.com' email
              , '6154808909'                            phone
              , 'galen_user_' || i::TEXT || '@test.com' username
              , 2                                       created_by_id
              , 2                                       modified_by_id
              , FALSE                                   is_deleted
              , NULL                                    deleted_at
              , NULL                                    deleted_by_id
              , NOW()                                   inserted_at
              , NOW()                                   updated_at
              , NULL                                    image_url
              , NULL                                    personal_meeting_id
              , 'America/Chicago'                       time_zone
              , NULL                                    zoom_user_id
              , NULL                                    okta_id
              , NULL                                    prefix
              , NULL                                    suffix
              , FALSE                                   contract
              , NULL                                    reporting_group
              , NULL                                    emr_origin_user_id
              , NULL                                    scheduling_notes
            FROM
                    ( SELECT GENERATE_SERIES(1, 30) i ) i
            RETURNING * )
SELECT *
FROM
    ins;

SELECT count(*)
FROM
    users;
INSERT
INTO
    user_roles (user_id, role_id, inserted_at, updated_at)
select id, 2411, now(), now()
from _galen_users;

SELECT *
FROM
    care_teams;
-- galen 67
-- alan 5

drop table if exists _patients;
        create temporary table _patients as
        select
            x.first_name || '_galen_' || i.i first_name,
            x.last_name  || '_galen_' || i.i last_name,
            x.first_name || '_galen_' || i.i preferred_name,
            i.i                      ctrl_num,
            x.dob::date              dob,
            x.gender                 gender,
            gen_random_uuid()        notes,
            67                    care_team_id, -- tbd
            '2023-01-01'       enroll_date,
            1                        payer_id,
            'America/Chicago'        time_zone,
            'vendor'                 segment_source,
            'initial_load'           source,
            2                        created_by_id,
            now()                    inserted_at,
            now()                    updated_at,
            'unknown_segment'        segment, --?
            '?'                      status,
            '?'                      substatus,
            0                        emr_patient_id,
--             ctc.ctrl_ab              ctrl_ab,
            null::bigint             patient_id,
--             u.user_id                user_id,
            137        referring_partner_id,
            x.program,
            x.insurance_classification_id,
            x.assistance_classification_id,
            x.admit_type,
            x.schedule_visit,
            x.appointment_offset_mins,
            coalesce(x.extra_data_type, -1) extra_data_type,
            null::bigint patient_contact_phone_id,
            null::bigint patient_contact_id
        from
            (
                values
                ('Melisa',    'Rasper',       'Female',     'A', '2/25/1952'  ,            'ma'            ,    'group_ma'                   ,          'assistance_unknown', 'recent_er'        ,         true  ,                      0  ,    1),
                ('Ada',       'Fulford',      'Female',     'A', '4/1/2001'   ,            'ma'            ,    'ma'                         ,          'assistance_unknown', 'er_discharge_hosp',         true  ,                     30  , null),
                ('Rose',      'Kemson',       'Female',     'A', '3/25/1923'  ,            'ma'            ,    'medicare_soon_late_retiree' ,          'assistance_unknown', 'ip_admission_3d'  ,         false ,                     45  , null),
                ('Bart',      'Flaunders',    'Male',       'A', '4/27/1945'  ,            'ma'            ,    'om'                         ,          'assistance_unknown', 'ip_admission_gt3d',         false ,                     75  , null),
                ('Milo',      'Stenson',      'Male',       'A', '5/4/1967'   ,            'ma'            ,    'om_addl'                    ,          'assistance_unknown', 'ip_discharge'     ,         true  ,                     90  , null),
                ('Sher',      'Zanre',        'Female',     'A', '3/1/1954'   ,            'ma'            ,    'om_other_dno'               ,          'assistance_unknown', 'recent_er'        ,         false ,                      0  , null),
                ('Derick',    'MacCague',     'Male',       'A', '2/28/1953'  ,            'ma'            ,    'unknown'                    ,          'assistance_unknown', 'er_discharge'     ,         true  ,                     60  , null),
                ('Mason',     'Burgoin',      'Male',       'A', '1/16/1972'  ,            'ma'            ,    'group_ma'                   ,          'assistance_unknown', 'ip_admission_3d'  ,         false ,                    180  , null),
                ('Gan',       'Wise',         'Male',       'A', '2/29/1956'  ,            'ma'            ,    'ma'                         ,          'assistance_unknown', 'ip_admission_gt3d',         false ,                    270  , null),
                ('Madeline',  'Wormell',      'Female',     'A', '10/19/1959' ,            'ma'            ,    'medicare_soon_late_retiree' ,          'assistance_unknown', 'ip_discharge'     ,         true  ,                    360  , null),
                ('Lester',    'Boatwright',   'Male',       'A', '3/20/1962'  ,            'ma'            ,    'om'                         ,          'assistance_unknown', ''                 ,         true  ,                    240  ,    2), -- 'recent_er'
                ('Regiona',   'Daft',         'Female',     'A', '7/9/1941'  ,             'ma'            ,    'om_addl'                    ,          'assistance_unknown', ''                 ,         true  ,                    300  , null), -- 'er_discharge_hosp'
                ('Angel',     'Leason',       'Male',       'A', '11/9/1963'  ,            'ma'            ,    'om_other_dno'               ,          'assistance_unknown', ''                 ,         true  ,                      0  , null), -- 'ip_admission_3d'
                ('Lyn',       'Dobbs',        'Male',       'A', '7/10/1943'  ,            'ma'            ,    'unknown'                    ,          'assistance_unknown', ''                 ,         true  ,                     30  , null), -- 'ip_admission_gt3d'
                ('Rolfe',     'Kenda',        'Male',       'A', '1/16/1972'  ,            'ma'            ,    'group_ma'                   ,          'assistance_unknown', ''                 ,         true  ,                     45  , null), -- 'ip_discharge'
                ('Elizabeth', 'Castle',       'Female',     'B', '10/2/1973'   ,           'ma'            ,    'ma'                         ,          'assistance_unknown', ''                 ,         false ,                     75  , null), -- 'recent_er'
                ('Lamar',     'Rouge',        'Male',       'B', '12/9/1967'   ,           'ma'            ,    'medicare_soon_late_retiree' ,          'assistance_unknown', ''                 ,         false ,                     90  , null), -- 'er_discharge'
                ('Elliot',    'Menger',       'Male',       'B', '11/16/1951' ,            'ma'            ,    'om'                         ,          'assistance_unknown', ''                 ,         false ,                      0  , null), -- 'ip_admission_3d'
                ('Lonny',     'Kinzinger',    'Male',       'B', '4/16/1960'  ,            'ma'            ,    'om_addl'                    ,          'assistance_unknown', ''                 ,         false ,                     60  , null), -- 'ip_admission_gt3d'
                ('Griz',      'Rog',          'Male',       'B', '9/3/1955'  ,             'om_dual'       ,    'om_other_dno'               ,          'assistance_unknown', ''                 ,         false ,                    180  , null), -- 'ip_discharge'
                ('Abby',      'Dank',         'Female',     'B', '3/4/1947'   ,            'medicare_soon' ,    'medicare_soon_age_in'       ,          'assistance_unknown', ''                 ,         false ,                    270  , null), -- 'recent_er'
                ('Rob',       'Perrelli',     'Male',       'B', '8/4/1984'   ,            'medicare_soon' ,    'medicare_soon_late_retiree' ,          'assistance_unknown', ''                 ,         false ,                    360  , null), -- 'er_discharge_hosp'
                ('Dora',      'Chavez',       'Female',     'B', '8/22/1962' ,             'om_dual'       ,    'om'                         ,          'assistance_unknown', ''                 ,         false ,                    240  , null), -- 'ip_admission_3d'
                ('Magdalena', 'Peppard',      'Female',     'B', '12/25/1945'  ,           'om_dual'       ,    'om_med_supp_pdp'            ,          'assistance_unknown', ''                 ,         true  ,                    300  , null), -- 'ip_admission_gt3d'
                ('Jan',       'Labman',       'Female',     'B', '11/3/1926'  ,            'om_dual'       ,    'unknown'                    ,          'assistance_unknown', ''                 ,         true  ,                    270  , null), -- 'ip_discharge'
                ('Kaleb',     'Walters',      'Male',       'B', '9/19/1990' ,             'om_non_dual'   ,    'om_addl'                    ,          'assistance_unknown', ''                 ,         false ,                    360  , null), -- 'recent_er'
                ('Irvin',     'Brock',        'Male',       'B', '7/27/1938'  ,            'om_non_dual'   ,    'om_med_supp'                ,          'assistance_unknown', ''                 ,         true  ,                    240  , null), -- 'er_discharge'
                ('Deanna',    'Sandman',      'Female',     'B', '9/17/2001' ,             'om_non_dual'   ,    'unknown'                    ,          'assistance_unknown', ''                 ,         false ,                    300  , null), -- 'ip_admission_3d'
                ('Cory',      'Hurl',         'Male',       'B', '11/30/1952' ,            'undetermined'  ,    'unknown'                    ,          'assistance_unknown', ''                 ,         true  ,                      0  , null), -- 'ip_admission_gt3d'
                ('Becky',     'Linn',         'Female',     'B', '5/20/2009'  ,            'om_dual'       ,    'om_med_supp'                ,          'assistance_unknown', ''                 ,         true  ,                     30  , null)  -- 'ip_discharge'
            ) x (first_name, last_name,         gender, ctrl_ab,           dob,            program         ,    insurance_classification_id  ,  assistance_classification_id,          admit_type, schedule_visit, appointment_offset_mins , extra_data_type)
            cross join generate_series(1, 10000) i
--             join _users u on u.i = i.i
--             join _care_teams ctc on ctc.ctrl_num = i.i and ctc.ctrl_ab = x.ctrl_ab
--             join care_teams ct on ct.emergency_sms = ctc.emergency_sms::text
        ;
SELECT count(*)
FROM
    _patients;
    ---------------------------------------------
    -- patients
    insert into patients(first_name, last_name, preferred_name, dob, gender, notes, care_team_id, enroll_date, payer_id, time_zone, segment_source, source, created_by_id, inserted_at, updated_at, segment, status, substatus) -- emr_patient_id)
    select first_name, last_name, preferred_name, dob, gender, notes, care_team_id, enroll_date::date, payer_id, time_zone, segment_source, source, created_by_id, inserted_at, updated_at, segment, status, substatus from _patients p
    ;

UPDATE _patients p
SET
    patient_id = id
FROM
    patients p2
WHERE
      p2.first_name = p.first_name
  AND p2.last_name = p.last_name
  AND p2.preferred_name LIKE '%_galen_%';



    insert into patient_referring_partners(patient_id, referring_partner_id, external_patient_id, "primary", inserted_at, updated_at )
    select
        p.patient_id, p.referring_partner_id, null, true, now(), now()
    from
        _patients p

------------------------------------------------------------------------------------------------------------------------
/* non galen users */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    care_teams;
drop table if exists _non_galen_patients;
        create temporary table _non_galen_patients as
        select
            x.first_name || '_not_galen_' || i.i first_name,
            x.last_name  || '_not_galen_' || i.i last_name,
            x.first_name || '_not_galen_' || i.i preferred_name,
            i.i                      ctrl_num,
            x.dob::date              dob,
            x.gender                 gender,
            gen_random_uuid()        notes,
            5                    care_team_id, -- tbd
            '2023-01-01'       enroll_date,
            1                        payer_id,
            'America/Chicago'        time_zone,
            'vendor'                 segment_source,
            'initial_load'           source,
            2                        created_by_id,
            now()                    inserted_at,
            now()                    updated_at,
            'unknown_segment'        segment, --?
            '?'                      status,
            '?'                      substatus,
            0                        emr_patient_id,
--             ctc.ctrl_ab              ctrl_ab,
            null::bigint             patient_id,
--             u.user_id                user_id,
            136        referring_partner_id,
            x.program,
            x.insurance_classification_id,
            x.assistance_classification_id,
            x.admit_type,
            x.schedule_visit,
            x.appointment_offset_mins,
            coalesce(x.extra_data_type, -1) extra_data_type,
            null::bigint patient_contact_phone_id,
            null::bigint patient_contact_id
        from
            (
                values
                ('Melisa',    'Rasper',       'Female',     'A', '2/25/1952'  ,            'ma'            ,    'group_ma'                   ,          'assistance_unknown', 'recent_er'        ,         true  ,                      0  ,    1),
                ('Ada',       'Fulford',      'Female',     'A', '4/1/2001'   ,            'ma'            ,    'ma'                         ,          'assistance_unknown', 'er_discharge_hosp',         true  ,                     30  , null),
                ('Rose',      'Kemson',       'Female',     'A', '3/25/1923'  ,            'ma'            ,    'medicare_soon_late_retiree' ,          'assistance_unknown', 'ip_admission_3d'  ,         false ,                     45  , null),
                ('Bart',      'Flaunders',    'Male',       'A', '4/27/1945'  ,            'ma'            ,    'om'                         ,          'assistance_unknown', 'ip_admission_gt3d',         false ,                     75  , null),
                ('Milo',      'Stenson',      'Male',       'A', '5/4/1967'   ,            'ma'            ,    'om_addl'                    ,          'assistance_unknown', 'ip_discharge'     ,         true  ,                     90  , null),
                ('Sher',      'Zanre',        'Female',     'A', '3/1/1954'   ,            'ma'            ,    'om_other_dno'               ,          'assistance_unknown', 'recent_er'        ,         false ,                      0  , null),
                ('Derick',    'MacCague',     'Male',       'A', '2/28/1953'  ,            'ma'            ,    'unknown'                    ,          'assistance_unknown', 'er_discharge'     ,         true  ,                     60  , null),
                ('Mason',     'Burgoin',      'Male',       'A', '1/16/1972'  ,            'ma'            ,    'group_ma'                   ,          'assistance_unknown', 'ip_admission_3d'  ,         false ,                    180  , null),
                ('Gan',       'Wise',         'Male',       'A', '2/29/1956'  ,            'ma'            ,    'ma'                         ,          'assistance_unknown', 'ip_admission_gt3d',         false ,                    270  , null),
                ('Madeline',  'Wormell',      'Female',     'A', '10/19/1959' ,            'ma'            ,    'medicare_soon_late_retiree' ,          'assistance_unknown', 'ip_discharge'     ,         true  ,                    360  , null),
                ('Lester',    'Boatwright',   'Male',       'A', '3/20/1962'  ,            'ma'            ,    'om'                         ,          'assistance_unknown', ''                 ,         true  ,                    240  ,    2), -- 'recent_er'
                ('Regiona',   'Daft',         'Female',     'A', '7/9/1941'  ,             'ma'            ,    'om_addl'                    ,          'assistance_unknown', ''                 ,         true  ,                    300  , null), -- 'er_discharge_hosp'
                ('Angel',     'Leason',       'Male',       'A', '11/9/1963'  ,            'ma'            ,    'om_other_dno'               ,          'assistance_unknown', ''                 ,         true  ,                      0  , null), -- 'ip_admission_3d'
                ('Lyn',       'Dobbs',        'Male',       'A', '7/10/1943'  ,            'ma'            ,    'unknown'                    ,          'assistance_unknown', ''                 ,         true  ,                     30  , null), -- 'ip_admission_gt3d'
                ('Rolfe',     'Kenda',        'Male',       'A', '1/16/1972'  ,            'ma'            ,    'group_ma'                   ,          'assistance_unknown', ''                 ,         true  ,                     45  , null), -- 'ip_discharge'
                ('Elizabeth', 'Castle',       'Female',     'B', '10/2/1973'   ,           'ma'            ,    'ma'                         ,          'assistance_unknown', ''                 ,         false ,                     75  , null), -- 'recent_er'
                ('Lamar',     'Rouge',        'Male',       'B', '12/9/1967'   ,           'ma'            ,    'medicare_soon_late_retiree' ,          'assistance_unknown', ''                 ,         false ,                     90  , null), -- 'er_discharge'
                ('Elliot',    'Menger',       'Male',       'B', '11/16/1951' ,            'ma'            ,    'om'                         ,          'assistance_unknown', ''                 ,         false ,                      0  , null), -- 'ip_admission_3d'
                ('Lonny',     'Kinzinger',    'Male',       'B', '4/16/1960'  ,            'ma'            ,    'om_addl'                    ,          'assistance_unknown', ''                 ,         false ,                     60  , null), -- 'ip_admission_gt3d'
                ('Griz',      'Rog',          'Male',       'B', '9/3/1955'  ,             'om_dual'       ,    'om_other_dno'               ,          'assistance_unknown', ''                 ,         false ,                    180  , null), -- 'ip_discharge'
                ('Abby',      'Dank',         'Female',     'B', '3/4/1947'   ,            'medicare_soon' ,    'medicare_soon_age_in'       ,          'assistance_unknown', ''                 ,         false ,                    270  , null), -- 'recent_er'
                ('Rob',       'Perrelli',     'Male',       'B', '8/4/1984'   ,            'medicare_soon' ,    'medicare_soon_late_retiree' ,          'assistance_unknown', ''                 ,         false ,                    360  , null), -- 'er_discharge_hosp'
                ('Dora',      'Chavez',       'Female',     'B', '8/22/1962' ,             'om_dual'       ,    'om'                         ,          'assistance_unknown', ''                 ,         false ,                    240  , null), -- 'ip_admission_3d'
                ('Magdalena', 'Peppard',      'Female',     'B', '12/25/1945'  ,           'om_dual'       ,    'om_med_supp_pdp'            ,          'assistance_unknown', ''                 ,         true  ,                    300  , null), -- 'ip_admission_gt3d'
                ('Jan',       'Labman',       'Female',     'B', '11/3/1926'  ,            'om_dual'       ,    'unknown'                    ,          'assistance_unknown', ''                 ,         true  ,                    270  , null), -- 'ip_discharge'
                ('Kaleb',     'Walters',      'Male',       'B', '9/19/1990' ,             'om_non_dual'   ,    'om_addl'                    ,          'assistance_unknown', ''                 ,         false ,                    360  , null), -- 'recent_er'
                ('Irvin',     'Brock',        'Male',       'B', '7/27/1938'  ,            'om_non_dual'   ,    'om_med_supp'                ,          'assistance_unknown', ''                 ,         true  ,                    240  , null), -- 'er_discharge'
                ('Deanna',    'Sandman',      'Female',     'B', '9/17/2001' ,             'om_non_dual'   ,    'unknown'                    ,          'assistance_unknown', ''                 ,         false ,                    300  , null), -- 'ip_admission_3d'
                ('Cory',      'Hurl',         'Male',       'B', '11/30/1952' ,            'undetermined'  ,    'unknown'                    ,          'assistance_unknown', ''                 ,         true  ,                      0  , null), -- 'ip_admission_gt3d'
                ('Becky',     'Linn',         'Female',     'B', '5/20/2009'  ,            'om_dual'       ,    'om_med_supp'                ,          'assistance_unknown', ''                 ,         true  ,                     30  , null)  -- 'ip_discharge'
            ) x (first_name, last_name,         gender, ctrl_ab,           dob,            program         ,    insurance_classification_id  ,  assistance_classification_id,          admit_type, schedule_visit, appointment_offset_mins , extra_data_type)
            cross join generate_series(1, 10000) i
--             join _users u on u.i = i.i
--             join _care_teams ctc on ctc.ctrl_num = i.i and ctc.ctrl_ab = x.ctrl_ab
--             join care_teams ct on ct.emergency_sms = ctc.emergency_sms::text
        ;
SELECT count(*)
FROM
    _non_galen_patients;
    ---------------------------------------------
    -- patients
    insert into patients(first_name, last_name, preferred_name, dob, gender, notes, care_team_id, enroll_date, payer_id, time_zone, segment_source, source, created_by_id, inserted_at, updated_at, segment, status, substatus) -- emr_patient_id)
    select first_name, last_name, preferred_name, dob, gender, notes, care_team_id, enroll_date::date, payer_id, time_zone, segment_source, source, created_by_id, inserted_at, updated_at, segment, status, substatus from _non_galen_patients p
    ;

UPDATE _non_galen_patients p
SET
    patient_id = id
FROM
    patients p2
WHERE
      p2.first_name = p.first_name
  AND p2.last_name = p.last_name
  AND p2.preferred_name LIKE '%_not_galen_%';



    insert into patient_referring_partners(patient_id, referring_partner_id, external_patient_id, "primary", inserted_at, updated_at )
    select
        p.patient_id, p.referring_partner_id, null, true, now(), now()
    from
        _non_galen_patients  p

;
SELECT count(*)
FROM
    patients;