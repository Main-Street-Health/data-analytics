------------------------------------------------------------------------------------------------------------------------
/* Ref */
------------------------------------------------------------------------------------------------------------------------

CREATE TABLE rn_ref_referral_statuses (
    key         TEXT PRIMARY KEY,
    name        TEXT,
    is_deleted  BOOLEAN DEFAULT FALSE,
    inserted_at TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
-- Open
-- S1: Pending Pre-Visit Documentation
-- S2: Pending Visit
-- S3: Pending Post-Visit Documentation
-- S4: Complete
-- Closed
);


CREATE TABLE rn_ref_specialty_types (
    key         TEXT PRIMARY KEY,
    name        TEXT,
    is_deleted  BOOLEAN DEFAULT FALSE,
    inserted_at TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
-- Mammogram, Dexa Scan, Colonoscopy etc
-- Cardiology Orthopedics GI Etc..
);

CREATE TABLE rn_ref_referral_activities (
    key         TEXT PRIMARY KEY,
    name        TEXT,
    is_deleted  BOOLEAN DEFAULT FALSE,
    inserted_at TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
-- referral_created
-- referral_updated?
-- appointment_scheduled
-- appointment_rescheduled
-- pre_visit_documentation_sent
-- post_visit_documentation_received
);

CREATE TABLE rn_ref_non_preferred_reasons (
    key         TEXT PRIMARY KEY,
    name        TEXT,
    is_deleted  BOOLEAN DEFAULT FALSE,
    inserted_at TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
-- PCP Preference
-- Patient Preference
-- Patient Insurance
-- First Available
);


------------------------------------------------------------------------------------------------------------------------
/* End ref */
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
/* config rpl referral facilities */
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE rn_facilities (
    id             BIGSERIAL PRIMARY KEY,
    name           TEXT      NOT NULL,
    phone          TEXT,
    ext            TEXT,
    address_line_1 TEXT,
    address_line_2 TEXT,
    city           TEXT,
    state          TEXT,
    zip            TEXT,
    website        TEXT,
    npi            TEXT,
    updated_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    inserted_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE rn_providers (
    id         BIGSERIAL PRIMARY KEY,
    first_name TEXT,
    last_name  TEXT,
    npi        TEXT,
    updated_at              TIMESTAMP                                NOT NULL DEFAULT NOW(),
    inserted_at             TIMESTAMP                                NOT NULL DEFAULT NOW()
);

-- I think this is mainly needed for referrals that won't have a provider ie x-ray's, ct's etc
CREATE TABLE rn_facility_specialties (
    id                 BIGSERIAL PRIMARY KEY,
    rn_facility_id     BIGINT REFERENCES rn_facilities        NOT NULL,
    specialty_type_key TEXT REFERENCES rn_ref_specialty_types NOT NULL,
    is_preferred       BOOLEAN,
    updated_at         TIMESTAMP                              NOT NULL DEFAULT NOW(),
    inserted_at        TIMESTAMP                              NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX ON rn_facility_specialties (specialty_type_key, rn_facility_id);


CREATE TABLE rn_facility_providers (
    id                       BIGSERIAL PRIMARY KEY,
    rn_facility_id     BIGINT REFERENCES rn_facilities        NOT NULL,
    rn_provider_id     BIGINT REFERENCES rn_providers         NOT NULL,
    specialty_type_key TEXT REFERENCES rn_ref_specialty_types NOT NULL,
    is_preferred             BOOLEAN,
    updated_at               TIMESTAMP                      NOT NULL DEFAULT NOW(),
    inserted_at              TIMESTAMP                      NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX ON rn_facility_providers(specialty_type_key, rn_facility_id, rn_provider_id);


SELECT f.name facility_name, p.last_name doc, fp.specialty_type_key, fp.is_preferred
FROM
    rn_facilities f
    JOIN rn_facility_providers fp ON f.id = fp.rn_facility_id
    JOIN rn_providers p ON p.id = fp.rn_provider_id
where fp.specialty_type_key = 'mamogram'
;


-- Q) is the network consistent across rpl's?
CREATE TABLE rn_facility_rpl (
    rn_facility_id       BIGINT REFERENCES rn_facilities,
    referring_partner_id BIGINT REFERENCES referring_partners NOT NULL,
    updated_at           TIMESTAMP                            NOT NULL DEFAULT NOW(),
    inserted_at          TIMESTAMP                            NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX ON rn_facility_rpl(rn_facility_id, referring_partner_id);
------------------------------------------------------------------------------------------------------------------------
/* end rpl-dest config */
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
/* main patient referral tables */
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE rn_patient_referrals (
    id                          BIGSERIAL PRIMARY KEY,
    patient_id                  BIGINT REFERENCES patients               NOT NULL,
    referring_partner_id        BIGINT REFERENCES referring_partners     NOT NULL,
    referral_status_key         TEXT REFERENCES rn_ref_referral_statuses NOT NULL,
    specialty_type_key          TEXT REFERENCES rn_ref_specialty_types   NOT NULL,
    rn_facility_id              BIGINT REFERENCES rn_facilities,
    rn_provider_id              BIGINT REFERENCES rn_providers,
    is_active                   BOOLEAN                                  NOT NULL DEFAULT TRUE,
--     patient_not_found? -- not sure if we create pat or what, look at sdoh

    assigned_to_id              BIGINT REFERENCES users,
    referral_date               DATE,
    icd_10_id                   BIGINT REFERENCES icd10s,

    is_preferred                BOOLEAN,
    non_preferred_reason        TEXT REFERENCES rn_ref_non_preferred_reasons,
    appointment_date            DATE,
    pre_visit_docs_sent         BOOLEAN                                  NOT NULL DEFAULT FALSE,
    pre_visit_docs_sent_at      TIMESTAMP,
    post_visit_docs_received    BOOLEAN                                  NOT NULL DEFAULT FALSE,
    post_visit_docs_received_at TIMESTAMP,
    notes                       TEXT,

    updated_at                  TIMESTAMP                                NOT NULL DEFAULT NOW(),
    inserted_at                 TIMESTAMP                                NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX ON rn_patient_referrals(patient_id, specialty_type_key) WHERE is_active;


CREATE TABLE rn_pr_nav_blocks (
    id                          BIGSERIAL PRIMARY KEY,
    rn_patient_referral_id      BIGINT REFERENCES rn_patient_referrals NOT NULL,
    patient_task_id             BIGINT REFERENCES patient_tasks        NOT NULL,

    rn_facility_id              BIGINT REFERENCES rn_facilities,
    rn_provider_id              BIGINT REFERENCES rn_providers,
    specialty_type_key          TEXT REFERENCES rn_ref_specialty_types NOT NULL,

    assigned_to_id              BIGINT REFERENCES users,
    referral_date               DATE,
    icd_10_id                   BIGINT REFERENCES icd10s,

    is_preferred                BOOLEAN,
    non_preferred_reason        TEXT REFERENCES rn_ref_non_preferred_reasons,
    appointment_date            DATE,
    pre_visit_docs_sent         BOOLEAN                                NOT NULL DEFAULT FALSE,
    pre_visit_docs_sent_at      TIMESTAMP,
    post_visit_docs_received    BOOLEAN                                NOT NULL DEFAULT FALSE,
    post_visit_docs_received_at TIMESTAMP,
    notes                       TEXT,

    created_by_id               BIGINT REFERENCES users,
    updated_by_id               BIGINT REFERENCES users,
    updated_at                  TIMESTAMP                              NOT NULL DEFAULT NOW(),
    inserted_at                 TIMESTAMP                              NOT NULL DEFAULT NOW()
);


CREATE INDEX ON rn_pr_nav_blocks(rn_patient_referral_id);
CREATE INDEX ON rn_pr_nav_blocks(patient_task_id);


CREATE TABLE rn_pr_status_periods (
    id                     BIGSERIAL PRIMARY KEY,
    rn_patient_referral_id BIGINT REFERENCES rn_patient_referrals   NOT NULL,
    referral_status_key    TEXT REFERENCES rn_ref_referral_statuses NOT NULL,
    start_why              TEXT,
    start_at               TIMESTAMP                                NOT NULL DEFAULT NOW(),
    end_at                 TIMESTAMP,
    updated_at             TIMESTAMP                                NOT NULL DEFAULT NOW(),
    inserted_at            TIMESTAMP                                NOT NULL DEFAULT NOW()
);
CREATE INDEX ON rn_pr_status_periods(rn_patient_referral_id);


CREATE TABLE rn_pr_activities (
    id                       BIGSERIAL PRIMARY KEY,
    rn_patient_referral_id   BIGINT REFERENCES rn_patient_referrals NOT NULL,
    activity_key             TEXT REFERENCES rn_ref_referral_activities,
    activity_by_id           BIGINT REFERENCES users,
    status_period_id         BIGINT REFERENCES rn_pr_status_periods,
    patient_task_id          BIGINT REFERENCES patient_tasks,
    patient_appointment_date DATE,
    is_no_show               BOOLEAN,
    description              TEXT,
    updated_at               TIMESTAMP                              NOT NULL DEFAULT NOW(),
    inserted_at              TIMESTAMP                              NOT NULL DEFAULT NOW()
);
CREATE INDEX ON rn_pr_activities(rn_patient_referral_id);
CREATE INDEX ON rn_pr_activities(patient_task_id);

CREATE TABLE rn_pr_tasks (
    id                     BIGSERIAL PRIMARY KEY,
    rn_patient_referral_id BIGINT REFERENCES rn_patient_referrals NOT NULL,
    patient_task_id        BIGINT REFERENCES patient_tasks        NOT NULL,
    updated_at             TIMESTAMP                              NOT NULL DEFAULT NOW(),
    inserted_at            TIMESTAMP                              NOT NULL DEFAULT NOW()
);
CREATE INDEX ON rn_pr_tasks(rn_patient_referral_id);
CREATE INDEX ON rn_pr_tasks(patient_task_id);

CREATE TABLE rn_pr_scheduled_events (
    id                     BIGSERIAL PRIMARY KEY,
    rn_patient_referral_id BIGINT REFERENCES rn_patient_referrals NOT NULL,
    rescheduled_event_id   BIGINT REFERENCES rn_pr_scheduled_events,
    status_period_id       BIGINT REFERENCES rn_pr_status_periods,
    scheduled_date         DATE,
    outcome_key            TEXT,
    outcome_reason_key     TEXT,
    outcome_note           TEXT,
    note                   TEXT,
    location               TEXT,
    inserted_at            TIMESTAMP                           NOT NULL,
    updated_at             TIMESTAMP                           NOT NULL
);
CREATE INDEX ON rn_pr_scheduled_events(rn_patient_referral_id);

------------------------------------------------------------------------------------------------------------------------
/* Portal logging tables */
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE rn_pr_searches (
    id                   BIGSERIAL PRIMARY KEY,
    user_id              BIGINT REFERENCES users,
    referring_partner_id BIGINT REFERENCES referring_partners,
    specialty_type_key   TEXT REFERENCES rn_ref_specialty_types,

    inserted_at          TIMESTAMP NOT NULL DEFAULT now()
);

------------------------------------------------------------------------------------------------------------------------
/* example happy path

   Doc scribbles referral needed for patient to get an MRI
   Health Nav (HN) creates the rn_patient_referrals record entering the patient, referral type, etc ...
   HN looks at drop down of facilities with mri for rpl and selects preferred dest
       - (check no existing open referal of same type for patient)
       - status period is created: open
       - activity: referral_created
       - task to schedule
   HN contacts ref_dest, schedules patient (or does patient schedule themselves?)
       - status period: scheduled
       - activity: scheduled, set appt date
       - scheduled_event created
       - task to upload docs
   HN sends ref_dest pre visit docs
       (how does HN know what needs to be sent? how different are the docs that need to be sent by proc/spec?)
       ( what is the method of sending docs? do we have a way to confirm they've been sent (out side of user input)?)
   Visit date passes current date
       - create task for HN to check if visit happened
   HN contacts ref dest to see if visit happened, it has
       - status period: completed
       - activity: visit completed
       - scheduled_event updated
       - task to upload docs post visit docs
   HN uploads post visit docs
        - status period: closed (or is "closed" more "it didn't happen" and we need a separate status between scheduled and completed?)
        - activity: visit closed
*/
------------------------------------------------------------------------------------------------------------------------

-- config
SELECT *
FROM
    referring_partners
WHERE
    name ~* 'Rhea';
;
SELECT * FROM junk.rpl_rhea_provider_directory_manual_v1_20240617;

-- update junk.rpl_rhea_provider_directory_manual_v1_20240617 set provider = null where rpl_rhea_provider_directory_manual_v1_20240617.provider ~* ' list providers here, all audiologists only. Could we just have the practice selected in this instance?'

create table junk.rpl_rhea_provider_directory_manual_v1_cleaned_up_20240617 as
SELECT
    rpl                                              rpl_name
  , NULL                                             rpl_id
  , specialty
  , facility                                         facility_name
  , line1                                            facility_adddres_line1
  , line2                                            facility_address_line2
  , city                                             facility_address_city
  , state                                            facility_address_state
  , zip                                              facility_address_zip
  , facility_phone
  , fax
  , provider                                         provider_full_name
  , SPLIT_PART(provider, ' ', 1)                     provider_first_name
  , SPLIT_PART(SPLIT_PART(provider, ' ', 2), ',', 1) provider_last_name
  , SPLIT_PART(provider, ', ', 2)                    provider_title
  , preferred
  , provider_instructions
  , insurance_accepted
FROM
    ( VALUES
          ('988 Oak Ridge Turnpike Ste 200 Oak Ridge, TN 37830', '988 Oak Ridge Turnpike', 'Ste 200', 'Oak Ridge', 'TN',
           '37830'),
          ('623 South Congress Parkway Athens, TN 37303', '623 South Congress Parkway', NULL, 'Athens', 'TN', '37303'),
          ('144 Concord Road Knoxville, Tennessee 37934', '144 Concord Road', NULL, 'Knoxville', 'Tennessee', '37934'),
          ('1930 Alcoa Highway Knoxville, TN 37920', '1930 Alcoa Highway', NULL, 'Knoxville', 'TN', '37920'),
          ('4980 Alpha Lane, Hixson, TN 37343', '4980 Alpha Lane', NULL, 'Hixson', 'TN', '37343'),
          ('10800 Parkside Drive, Knoxville, TN 37934', '10800 Parkside Drive', NULL, 'Knoxville', 'TN', '37934'),
          ('700 S. Illinois Ave, Suite A104 Oak Ridge, TN 37830', '700 S. Illinois Ave', 'Suite A104', 'Oak Ridge',
           'TN', '37830'),
          ('901 Riverfront Parkway, Suite 300 Chattanooga, TN 37402', '901 Riverfront Parkway', 'Suite 300',
           'Chattanooga', 'TN', '37402'),
          ('2565 Business Park Dr. NE, Cleveland, TN 37311', '2565 Business Park Dr. NE', NULL, 'Cleveland', 'TN',
           '37311'),
          ('2404 Chambliss Avenue Northwest Cleveland, TN 37311', '2404 Chambliss Avenue Northwest', NULL, 'Cleveland',
           'TN', '37311'),
          ('2415 Chambliss Avenue Cleveland, TN 37311', '2415 Chambliss Avenue', NULL, 'Cleveland', 'TN', '37311'),
          ('1724 Hamill Road Oasis Park Building Suite 102 Hixson, TN 37343', '1724 Hamill Road',
           'Oasis Park Building Suite 102', 'Hixson', 'TN', '37343'),
          ('1450 Dowell Springs Blvd. Suite 100 Knoxville, TN 37909', '1450 Dowell Springs Blvd.', 'Suite 100',
           'Knoxville', 'TN', '37909'),
          ('9309 Apison Pike, Ooltewah, TN 37363', '9309 Apison Pike', NULL, 'Ooltewah', 'TN', '37363'),
          ('979 East Third Street Suite C-0630 Chattanooga, TN 37403', '979 East Third Street', 'Suite C-0630',
           'Chattanooga', 'TN', '37403'),
          ('2515 Desales Ave Suite 206 Chattanooga TN 37404', '2515 Desales Ave', 'Suite 206', 'Chattanooga', 'TN',
           '37404'),
          ('Fort Loudoun Medical Center Drive Suite 200 Lenoir City, TN 37772', 'Fort Loudoun Medical Center Drive',
           'Suite 200', 'Lenoir City', 'TN', '37772'),
          ('5441 Highway 153 Hixson, TN 37343', '5441 Highway 153', NULL, 'Hixson', 'TN', '37343'),
          ('4700 Battlefield Pkwy #130, Ringgold, GA 30736', '4700 Battlefield Pkwy', '#130', 'Ringgold', 'GA',
           '30736'),
          ('49 Cleveland St., Suite 220 Crossville, TN 38555', '49 Cleveland St.', 'Suite 220', 'Crossville', 'TN',
           '38555'),
          ('109 Independence Lane, Lafollette TN 37766', '109 Independence Lane', NULL, 'Lafollette', 'TN', '37766'),
          ('304 Wright Street Sweetwater, TN 37874', '304 Wright Street', NULL, 'Sweetwater', 'TN', '37874'),
          ('Physicians Plaza of Roane County 1855 Tanner Way, Suite 130 Harriman, TN',
           'Physicians Plaza of Roane County 1855 Tanner Way', 'Suite 130', 'Harriman', 'TN', ''),
          ('2200 East Third Street Suite 200 Chattanooga, TN37404', '2200 East Third Street', 'Suite 200',
           'Chattanooga', 'TN', '37404'),
          ('800 Oak Ridge Turnpike, Suite C-100 Oak Ridge, TN 37830', '800 Oak Ridge Turnpike', 'Suite C-100',
           'Oak Ridge', 'TN', '37830'),
          ('2290 Ogletree Ave 108 Chattanooga, TN 37421', '2290 Ogletree Ave', '108', 'Chattanooga', 'TN', '37421'),
          ('725 Glenwood Drive, Suite 488, Chattanooga, TN 37404', '725 Glenwood Drive', 'Suite 488', 'Chattanooga',
           'TN', '37404'),
          ('9309 Apison Pike, Ooltewah, TN 37363', '9309 Apison Pike', NULL, 'Ooltewah', 'TN',
           '37363') ) AS x(original, line1, line2, city, state, zip)
    JOIN
        junk.rpl_rhea_provider_directory_manual_v1_20240617 j ON j.facility_address = x.original
;



SELECT *
FROM
    junk.rpl_rhea_provider_directory_manual_v1_cleaned_up_20240617
 ;
    update junk.rpl_rhea_provider_directory_manual_v1_cleaned_up_20240617 set facility_name = null where facility_name = '<None>';

