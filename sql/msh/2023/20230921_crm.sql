SELECT
    m0."id"
  , m0."name"
  , u4."full_name"
  , m0."warmth"
  , m0."score"
  , m0."status"
  , m0."broker_status"
  , m0."county"
  , m2."market"
  , s5."due_date"
  , m1."score"
  , m1."stage"
  , m0."updated_at"
  , COALESCE(m0."ma_patient_count", 0)
  , COALESCE(m0."om_patient_count", 0)
  , COALESCE(m0."total_patient_count", 0)
  , COALESCE(s6."completed_interactions", 0)
  , m7 IS NULL
  , m0."city"
  , s5."activity"
  , m0."opportunity_notes"
  , m7."notes"
  , m0."percentage"
  , m0."cin_participation"
  , m0."expected_close_date"
FROM
    "msh_referring_partner_leads" AS m0
    LEFT OUTER JOIN "msh_referring_partner_opportunities" AS m1
                    ON (m1."lead_id" = m0."id") AND (m1."deleted_at" IS NULL)
    LEFT OUTER JOIN "markets" AS m2 ON m2."id" = m0."market_id"
    LEFT OUTER JOIN "market_leadership_teams" AS m3 ON m3."market_id" = m2."id"
    LEFT OUTER JOIN "public"."users" AS u4 ON u4."id" = m0."lead_owner_id"
    LEFT OUTER JOIN ( SELECT DISTINCT ON (sm0."lead_id")
                          sm0."id"              AS "id"
                        , sm0."activity"        AS "activity"
                        , sm0."assigned_to_id"  AS "assigned_to_id"
                        , sm0."due_date"        AS "due_date"
                        , sm0."status"          AS "status"
                        , sm0."created_by_id"   AS "created_by_id"
                        , sm0."updated_by_id"   AS "updated_by_id"
                        , sm0."deleted_by_id"   AS "deleted_by_id"
                        , sm0."deleted_at"      AS "deleted_at"
                        , sm0."lead_id"         AS "lead_id"
                        , sm0."location_id"     AS "location_id"
                        , sm0."organization_id" AS "organization_id"
                        , sm0."inserted_at"     AS "inserted_at"
                        , sm0."updated_at"      AS "updated_at"
                      FROM
                          "msh_next_steps" AS sm0
                      WHERE
                          (sm0."status" = 'open')
                      ORDER BY sm0."lead_id", sm0."due_date" ) AS s5 ON s5."lead_id" = m0."id"
    LEFT OUTER JOIN ( SELECT
                          sm0."referring_partner_lead_id" AS "lead_id"
                        , COUNT(*)                        AS "completed_interactions"
                      FROM
                          "msh_interactions" AS sm0
                      WHERE
                            (sm0."status" = 'completed')
                        AND (NOT (sm0."referring_partner_lead_id" IS NULL))
                      GROUP BY sm0."referring_partner_lead_id" ) AS s6 ON s6."lead_id" = m0."id"
    LEFT OUTER JOIN "msh_product_stages" AS m7 ON (m7."lead_id" = m0."id") AND m7."status" IN ('lost', 'snoozed')
WHERE
      (m0."deleted_at" IS NULL)
--   AND (m0."lead_owner_id" = 2)
  AND (m0."status" = ANY ('{open}'))
-- LIMIT $3 OFFSET $4 [2, ["open"], 100, 0];
;
SELECT * FROM msh_referring_partner_opportunities;
SELECT * FROM referring_partner_lea;
SELECT *
FROM
    docu;
SELECT * FROM msh_next_steps;
SELECT * FROM msh_interactions;
SELECT * FROM msh_product_stages;
SELECT product_id, stage, terms, count(*) FROM msh_product_stages GROUP BY 1,2,3 order by 4 desc;

SELECT
    m0."id"
  , m0."name"
  , u4."full_name"
  , m0."warmth"
  , m0."score"
  , m0."status"
  , m0."broker_status"
  , m0."county"
  , m2."market"
  , s5."due_date"
  , m1."score"
  , m1."stage"
  , m0."updated_at"
  , COALESCE(m0."ma_patient_count", 0)
  , COALESCE(m0."om_patient_count", 0)
  , COALESCE(m0."total_patient_count", 0)
  , COALESCE(s6."completed_interactions", 0)
  , m7 IS NULL
  , m0."city"
  , s5."activity"
  , m0."opportunity_notes"
  , m7."notes"
  , m0."percentage"
  , m0."cin_participation"
  , m0."expected_close_date"
FROM
    "msh_referring_partner_leads" AS m0
    LEFT OUTER JOIN "msh_referring_partner_opportunities" AS m1
                    ON (m1."lead_id" = m0."id") AND (m1."deleted_at" IS NULL)
    LEFT OUTER JOIN "markets" AS m2 ON m2."id" = m0."market_id"
    LEFT OUTER JOIN "market_leadership_teams" AS m3 ON m3."market_id" = m2."id"
    LEFT OUTER JOIN "public"."users" AS u4 ON u4."id" = m0."lead_owner_id"
    LEFT OUTER JOIN ( SELECT DISTINCT ON (sm0."lead_id")
                          sm0."id"              AS "id"
                        , sm0."activity"        AS "activity"
                        , sm0."assigned_to_id"  AS "assigned_to_id"
                        , sm0."due_date"        AS "due_date"
                        , sm0."status"          AS "status"
                        , sm0."created_by_id"   AS "created_by_id"
                        , sm0."updated_by_id"   AS "updated_by_id"
                        , sm0."deleted_by_id"   AS "deleted_by_id"
                        , sm0."deleted_at"      AS "deleted_at"
                        , sm0."lead_id"         AS "lead_id"
                        , sm0."location_id"     AS "location_id"
                        , sm0."organization_id" AS "organization_id"
                        , sm0."inserted_at"     AS "inserted_at"
                        , sm0."updated_at"      AS "updated_at"
                      FROM
                          "msh_next_steps" AS sm0
                      WHERE
                          (sm0."status" = 'open')
                      ORDER BY sm0."lead_id", sm0."due_date" ) AS s5 ON s5."lead_id" = m0."id"
    LEFT OUTER JOIN ( SELECT
                          sm0."referring_partner_lead_id" AS "lead_id"
                        , COUNT(*)                        AS "completed_interactions"
                      FROM
                          "msh_interactions" AS sm0
                      WHERE
                            (sm0."status" = 'completed')
                        AND (NOT (sm0."referring_partner_lead_id" IS NULL))
                      GROUP BY sm0."referring_partner_lead_id" ) AS s6 ON s6."lead_id" = m0."id"
    LEFT OUTER JOIN "msh_product_stages" AS m7 ON (m7."lead_id" = m0."id") AND m7."status" IN ('lost', 'snoozed')
WHERE
      (m0."deleted_at" IS NULL)
  AND (m0."lead_owner_id" = $1)
  AND (m1."status" = ANY ($2))
LIMIT $3 OFFSET $4 [2, ["open"], 100, 0];

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE msh_next_steps (
    id              BIGSERIAL PRIMARY KEY,
    activity        TEXT,
    assigned_to_id  BIGINT REFERENCES users,
    due_date        DATE,
    status          VARCHAR(255),
    created_by_id   BIGINT REFERENCES users,
    updated_by_id   BIGINT REFERENCES users,
    deleted_by_id   BIGINT REFERENCES users,
    deleted_at      TIMESTAMP(0),
    lead_id         BIGINT REFERENCES msh_referring_partner_leads,
    location_id     BIGINT REFERENCES referring_partners,
    organization_id BIGINT REFERENCES msh_referring_partner_organizations,
    inserted_at     TIMESTAMP(0) NOT NULL,
    updated_at      TIMESTAMP(0) NOT NULL
);

CREATE TABLE msh_referring_partner_leads (
    id                                 BIGSERIAL PRIMARY KEY,
    external_entity_id                 INTEGER REFERENCES external_entities,
    source_type_id                     BIGINT REFERENCES msh_referring_partner_lead_source_types,
    business_development_team_id       BIGINT REFERENCES market_leadership_teams,
    status                             TEXT                            NOT NULL,
    score                              INTEGER,
    warmth                             TEXT,
    notes                              TEXT,
    closed_date                        DATE,
    created_by_id                      BIGINT                          NOT NULL REFERENCES users,
    updated_by_id                      BIGINT                          NOT NULL REFERENCES users,
    inserted_at                        TIMESTAMP(0)                    NOT NULL,
    updated_at                         TIMESTAMP(0)                    NOT NULL,
    market_id                          BIGINT REFERENCES markets,
    npi                                TEXT,
    type                               TEXT,
    emr_system                         TEXT,
    emr_system_other                   TEXT,
    emr_system_version                 TEXT,
    pm_system                          TEXT,
    pm_system_other                    TEXT,
    pm_system_version                  TEXT,
    ma_patient_count                   INTEGER,
    total_patient_count                INTEGER,
    existing_value_based_payments      BOOLEAN,
    existing_value_based_payment_notes TEXT,
    fte                                DOUBLE PRECISION,
    single_location_only               TEXT,
    broker_status                      VARCHAR(255),
    opportunity_notes                  TEXT,
    programs_under_consideration       TEXT[] DEFAULT ARRAY []::TEXT[] NOT NULL,
    programs_under_consideration_other TEXT,
    region_type                        TEXT,
    percentage                         NUMERIC,
    source_type_other                  TEXT,
    lead_owner_id                      BIGINT REFERENCES users,
    om_patient_count                   INTEGER,
    name                               TEXT,
    email                              TEXT,
    website                            TEXT,
    phone1                             TEXT,
    phone2                             TEXT,
    tin                                TEXT CONSTRAINT tin_format CHECK (tin ~ '^\d{2}\-\d{7}$'::TEXT),
    address1                           TEXT,
    address2                           TEXT,
    city                               TEXT,
    state                              TEXT,
    zip                                TEXT,
    county                             TEXT,
    latitude                           DOUBLE PRECISION,
    longitude                          DOUBLE PRECISION,
    deleted_at                         TIMESTAMP(0),
    deleted_by_id                      BIGINT REFERENCES users,
    expected_close_date                DATE,
    status_reason                      TEXT,
    status_reason_other                TEXT,
    other_type                         TEXT,
    cin_participation                  TEXT
);

CREATE TABLE msh_referring_partner_opportunities (
    id                  BIGSERIAL PRIMARY KEY,
    lead_id             BIGINT       NOT NULL REFERENCES msh_referring_partner_leads,
    status              TEXT         NOT NULL,
    title               TEXT,
    score               TEXT,
    stage               TEXT,
    notes               TEXT,
    initiated_date      DATE         NOT NULL,
    closed_date         DATE,
    created_by_id       BIGINT       NOT NULL REFERENCES users,
    updated_by_id       BIGINT       NOT NULL REFERENCES users,
    inserted_at         TIMESTAMP(0) NOT NULL,
    updated_at          TIMESTAMP(0) NOT NULL,
    deleted_at          TIMESTAMP,
    status_reason       TEXT,
    status_reason_other TEXT
);

CREATE TABLE msh_interactions (
    id                                BIGSERIAL PRIMARY KEY,
    appointment_id                    INTEGER REFERENCES appointments,
    referring_partner_lead_id         INTEGER REFERENCES msh_referring_partner_leads,
    medium                            TEXT         NOT NULL,
    is_successful                     BOOLEAN,
    outcome                           TEXT,
    interaction_date                  TIMESTAMP,
    inserted_at                       TIMESTAMP(0) NOT NULL,
    updated_at                        TIMESTAMP(0) NOT NULL,
    status                            TEXT,
    type_id                           TEXT REFERENCES msh_interaction_types,
    created_by_id                     BIGINT REFERENCES users,
    updated_by_id                     BIGINT REFERENCES users,
    referring_partner_location_id     BIGINT REFERENCES referring_partners,
    referring_partner_organization_id BIGINT REFERENCES msh_referring_partner_organizations
);

CREATE TABLE msh_product_stages (
    id              BIGSERIAL PRIMARY KEY,
    product_id      TEXT,
    stage           TEXT,
    terms           TEXT[],
    lead_id         BIGINT REFERENCES msh_referring_partner_leads,
    organization_id BIGINT REFERENCES msh_referring_partner_organizations,
    status          TEXT,
    notes           TEXT,
    closed_reason   TEXT,
    deleted_at      TIMESTAMP(0) DEFAULT NULL::TIMESTAMP WITHOUT TIME ZONE,
    created_by_id   BIGINT REFERENCES users,
    updated_by_id   BIGINT REFERENCES users,
    inserted_at     TIMESTAMP(0) NOT NULL,
    updated_at      TIMESTAMP(0) NOT NULL,
    reason_detail   TEXT
);

SELECT product_id, stage, count(*)
FROM
    msh_product_stages GROUP BY 1,2 ORDER BY 3 desc;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    msh_products;
select * from msh_referring_partner_leads;
select * from msh_referring_partner_lead_source_types;
select * from msh_product_stages;
select * from referring_partner_pac_ids;
select * from msh_referring_partner_opportunities;
select * from msh_referring_partner_lead_locations;
select * from msh_referring_partner_implementations;
select * from msh_efficiency_scores;
select * from msh_referring_partner_proposals;
select * from msh_referring_partner_negotiations;
select * from msh_referring_partner_evaluations;
select * from market_leadership_teams;
select * from msh_interactions;
select * from msh_interaction_types;
select * from msh_interaction_contacts;
select * from referring_partner_lead_documents;
SELECT *
FROM
    referring_partner_lead_documents ld
join public.documents d ON ld.document_id = d.id
;