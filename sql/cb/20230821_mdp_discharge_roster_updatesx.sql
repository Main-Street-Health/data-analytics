-- update existing by adding two new columns
CREATE OR REPLACE VIEW _v_md_portals_rosters
            ( patient_org_source_id, patient_first_name, patient_middle_name, patient_last_name, patient_dob
            , patient_sex, patient_phone, patient_address, patient_city, patient_state, patient_zip
            , provider_direct_address, provider_name, provider_group_name, icd_codes, next_apt_date,last_visit_date, discharge_date)
AS
WITH
    pats      AS ( SELECT
                       p_1.id
                     , p_1.first_name
                     , p_1.last_name
                     , p_1.dob
                     , p_1.gender
                     , rs.label AS risk_level
                   FROM
                       patients p_1
                       JOIN patient_programs pp ON p_1.id = pp.patient_id
                       JOIN analytics_risk_strat_info rs ON rs.score = p_1.risk_strat
                   WHERE
                         pp.end_date IS NULL
                     AND pp.program = '24_7'::TEXT
                     AND (p_1.status = ANY (ARRAY ['engaged'::TEXT, 'attributed'::TEXT])) )
  , phones    AS ( SELECT
                       p_1.id AS patient_id
                     , cp.phone_number
                     , ROW_NUMBER() OVER (PARTITION BY p_1.id ORDER BY (
        CASE
            WHEN cp.type = 'mobile'::TEXT THEN 1
            WHEN cp.type = 'home'::TEXT   THEN 2
            ELSE 3
            END))             AS rn
                   FROM
                       pats p_1
                       JOIN patient_contacts pc
                            ON p_1.id = pc.patient_id AND pc.relationship = 'self'::TEXT AND pc.status = 'active'::TEXT
                       JOIN contact_phones cp ON pc.contact_id = cp.contact_id AND cp.status = 'active'::TEXT )
  , addresses AS ( SELECT
                       p_1.id                                                              AS patient_id
                     , pa.line1 || COALESCE(pa.line2, ''::TEXT)                            AS street
                     , pa.city
                     , pa.state
                     , pa.postal_code
                     , ROW_NUMBER() OVER (PARTITION BY p_1.id ORDER BY pa.updated_at DESC) AS rn
                   FROM
                       pats p_1
                       JOIN patient_addresses pa ON p_1.id = pa.patient_id
                   WHERE
                       pa.type = 'home'::TEXT )
  , next_cca  AS ( SELECT
                       v_emr_visits.patient_id
                     , v_emr_visits.appt_date
                     , v_emr_visits.appt_type
                     , v_emr_visits.appt_status
                     , ROW_NUMBER() OVER (PARTITION BY v_emr_visits.patient_id ORDER BY v_emr_visits.appt_date) AS rn
                   FROM
                       rpt.v_emr_visits
                   WHERE
                         v_emr_visits.is_cca
                     AND v_emr_visits.appt_date >= NOW()::DATE
                     AND v_emr_visits.appt_status = 'scheduled'::TEXT )
SELECT
    p.id                                                                                   AS patient_org_source_id
  , p.first_name                                                                           AS patient_first_name
  , NULL::TEXT                                                                             AS patient_middle_name
  , p.last_name                                                                            AS patient_last_name
  , p.dob                                                                                  AS patient_dob
  , "left"(p.gender, 1)                                                                    AS patient_sex
  , ph.phone_number                                                                        AS patient_phone
  , a.street                                                                               AS patient_address
  , a.city                                                                                 AS patient_city
  , a.state                                                                                AS patient_state
  , a.postal_code                                                                          AS patient_zip
  , NULL::TEXT                                                                             AS provider_direct_address
  , NULL::TEXT                                                                             AS provider_name
  , NULL::TEXT                                                                             AS provider_group_name
  , STRING_AGG(DISTINCT i10s.code_formatted, ','::TEXT) FILTER (WHERE i10s.id IS NOT NULL) AS icd_codes
  , nc.appt_date                                                                           AS next_apt_date
  , NULL                                                                                   AS last_visit_date
  , NULL                                                                                   AS discharge_date
FROM
    pats p
    LEFT JOIN patient_hccs phcc ON phcc.patient_id = p.id AND phcc.yr::DOUBLE PRECISION = DATE_PART('year'::TEXT, NOW())
    LEFT JOIN patient_hcc_captured_icd10s phc ON phc.patient_hcc_id = phcc.id
    LEFT JOIN hcc_icd10s hi ON phc.hcc_icd10_id = hi.id
    LEFT JOIN icd10s i10s ON hi.icd10_id = i10s.id
    LEFT JOIN addresses a ON p.id = a.patient_id AND a.rn = 1
    LEFT JOIN phones ph ON ph.patient_id = p.id AND ph.rn = 1
    LEFT JOIN next_cca nc ON nc.patient_id = p.id AND nc.rn = 1 AND nc.appt_date >= (NOW() - '15 days'::INTERVAL)
WHERE
     nc.patient_id IS NOT NULL
  OR NOT (EXISTS ( SELECT
                       1
                   FROM
                       md_portal_roster_patients mdp_rp
                   WHERE
                       mdp_rp.patient_id = p.id ))
GROUP BY
    p.id, p.first_name, NULL::TEXT, p.last_name, p.dob, ("left"(p.gender, 1)), ph.phone_number, a.street, a.city
        , a.state, a.postal_code, nc.appt_date;

-- create new view for discharge roster
CREATE OR REPLACE VIEW _v_md_portals_discharge_rosters
            ( patient_org_source_id, patient_first_name, patient_middle_name, patient_last_name, patient_dob
            , patient_sex, patient_phone, patient_address, patient_city, patient_state, patient_zip
            , provider_direct_address, provider_name, provider_group_name, icd_codes, next_apt_date,last_visit_date, discharge_date)
AS
WITH
    pats      AS ( SELECT
                       p_1.id
                     , p_1.first_name
                     , p_1.last_name
                     , p_1.gender
                     , p_1.dob
                     , rs.label AS risk_level
                     , max(h.discharge_date) discharge_date
                   FROM
                       patients p_1
                       JOIN patient_programs pp ON p_1.id = pp.patient_id
                       JOIN analytics_risk_strat_info rs ON rs.score = p_1.risk_strat
                       JOIN hospitalizations h ON p_1.id = h.patient_id
                                                      AND h.discharge_date >= NOW()::DATE - '5 days'::INTERVAL
                       JOIN cb_transitions_of_care toc on h.admission_series_id = toc.admission_series_id
                           and toc.deleted_at ISNULL
                           AND toc.status = 'open'
                   WHERE
                         pp.end_date IS NULL
                     AND pp.program = '24_7'::TEXT
                     AND (p_1.status = ANY (ARRAY ['engaged'::TEXT, 'attributed'::TEXT]))
                   GROUP BY 1,2,3,4,5,6
                   )
  , phones    AS ( SELECT
                       p_1.id AS patient_id
                     , cp.phone_number
                     , ROW_NUMBER() OVER (PARTITION BY p_1.id ORDER BY (
        CASE
            WHEN cp.type = 'mobile'::TEXT THEN 1
            WHEN cp.type = 'home'::TEXT   THEN 2
            ELSE 3
            END))             AS rn
                   FROM
                       pats p_1
                       JOIN patient_contacts pc
                            ON p_1.id = pc.patient_id AND pc.relationship = 'self'::TEXT AND pc.status = 'active'::TEXT
                       JOIN contact_phones cp ON pc.contact_id = cp.contact_id AND cp.status = 'active'::TEXT )
  , addresses AS ( SELECT
                       p_1.id                                                              AS patient_id
                     , pa.line1 || COALESCE(pa.line2, ''::TEXT)                            AS street
                     , pa.city
                     , pa.state
                     , pa.postal_code
                     , ROW_NUMBER() OVER (PARTITION BY p_1.id ORDER BY pa.updated_at DESC) AS rn
                   FROM
                       pats p_1
                       JOIN patient_addresses pa ON p_1.id = pa.patient_id
                   WHERE
                       pa.type = 'home'::TEXT )
SELECT
    p.id                                                                                   AS patient_org_source_id
  , p.first_name                                                                           AS patient_first_name
  , NULL::TEXT                                                                             AS patient_middle_name
  , p.last_name                                                                            AS patient_last_name
  , p.dob                                                                                  AS patient_dob
  , "left"(p.gender, 1)                                                                    AS patient_sex
  , ph.phone_number                                                                        AS patient_phone
  , a.street                                                                               AS patient_address
  , a.city                                                                                 AS patient_city
  , a.state                                                                                AS patient_state
  , a.postal_code                                                                          AS patient_zip
  , NULL::TEXT                                                                             AS provider_direct_address
  , NULL::TEXT                                                                             AS provider_name
  , NULL::TEXT                                                                             AS provider_group_name
  , STRING_AGG(DISTINCT i10s.code_formatted, ','::TEXT) FILTER (WHERE i10s.id IS NOT NULL) AS icd_codes
  , null                                                                                   AS next_apt_date
  , NULL                                                                                   AS last_visit_date
  , p.discharge_date                                                                       AS discharge_date
FROM
    pats p
    LEFT JOIN patient_hccs phcc ON phcc.patient_id = p.id AND phcc.yr::DOUBLE PRECISION = DATE_PART('year'::TEXT, NOW())
    LEFT JOIN patient_hcc_captured_icd10s phc ON phc.patient_hcc_id = phcc.id
    LEFT JOIN hcc_icd10s hi ON phc.hcc_icd10_id = hi.id
    LEFT JOIN icd10s i10s ON hi.icd10_id = i10s.id
    LEFT JOIN addresses a ON p.id = a.patient_id AND a.rn = 1
    LEFT JOIN phones ph ON ph.patient_id = p.id AND ph.rn = 1
GROUP BY
    p.id, p.first_name, NULL::TEXT, p.last_name, p.dob, ("left"(p.gender, 1)), ph.phone_number, a.street, a.city
        , a.state, a.postal_code, p.discharge_date;

CREATE VIEW _v_md_portals_full_rosters
            ( patient_org_source_id, patient_first_name, patient_middle_name, patient_last_name, patient_dob
            , patient_sex, patient_phone, patient_address, patient_city, patient_state, patient_zip
            , provider_direct_address, provider_name, provider_group_name, icd_codes, next_apt_date, last_visit_date
            , discharge_date)
AS
WITH
    pats      AS ( SELECT
                       p_1.id
                     , p_1.first_name
                     , p_1.last_name
                     , p_1.dob
                     , p_1.gender
                     , rs.label AS risk_level
                   FROM
                       patients p_1
                       JOIN patient_programs pp ON p_1.id = pp.patient_id
                       JOIN analytics_risk_strat_info rs ON rs.score = p_1.risk_strat
                   WHERE
                         pp.end_date IS NULL
                     AND pp.program = '24_7'::TEXT
                     AND (p_1.status = ANY (ARRAY ['engaged'::TEXT, 'attributed'::TEXT])) )
  , phones    AS ( SELECT
                       p_1.id AS patient_id
                     , cp.phone_number
                     , ROW_NUMBER() OVER (PARTITION BY p_1.id ORDER BY (
        CASE
            WHEN cp.type = 'mobile'::TEXT THEN 1
            WHEN cp.type = 'home'::TEXT   THEN 2
            ELSE 3
            END))             AS rn
                   FROM
                       pats p_1
                       JOIN patient_contacts pc
                            ON p_1.id = pc.patient_id AND pc.relationship = 'self'::TEXT AND pc.status = 'active'::TEXT
                       JOIN contact_phones cp ON pc.contact_id = cp.contact_id AND cp.status = 'active'::TEXT )
  , addresses AS ( SELECT
                       p_1.id                                                              AS patient_id
                     , pa.line1 || COALESCE(pa.line2, ''::TEXT)                            AS street
                     , pa.city
                     , pa.state
                     , pa.postal_code
                     , ROW_NUMBER() OVER (PARTITION BY p_1.id ORDER BY pa.updated_at DESC) AS rn
                   FROM
                       pats p_1
                       JOIN patient_addresses pa ON p_1.id = pa.patient_id
                   WHERE
                       pa.type = 'home'::TEXT )
  , next_cca  AS ( SELECT
                       v_emr_visits.patient_id
                     , v_emr_visits.appt_date
                     , v_emr_visits.appt_type
                     , v_emr_visits.appt_status
                     , ROW_NUMBER() OVER (PARTITION BY v_emr_visits.patient_id ORDER BY v_emr_visits.appt_date) AS rn
                   FROM
                       rpt.v_emr_visits
                   WHERE
                         v_emr_visits.is_cca
                     AND v_emr_visits.appt_date >= NOW()::DATE
                     AND v_emr_visits.appt_status = 'scheduled'::TEXT )
SELECT
    p.id                                                                                   AS patient_org_source_id
  , p.first_name                                                                           AS patient_first_name
  , NULL::TEXT                                                                             AS patient_middle_name
  , p.last_name                                                                            AS patient_last_name
  , p.dob                                                                                  AS patient_dob
  , "left"(p.gender, 1)                                                                    AS patient_sex
  , ph.phone_number                                                                        AS patient_phone
  , a.street                                                                               AS patient_address
  , a.city                                                                                 AS patient_city
  , a.state                                                                                AS patient_state
  , a.postal_code                                                                          AS patient_zip
  , NULL::TEXT                                                                             AS provider_direct_address
  , NULL::TEXT                                                                             AS provider_name
  , NULL::TEXT                                                                             AS provider_group_name
  , STRING_AGG(DISTINCT i10s.code_formatted, ','::TEXT) FILTER (WHERE i10s.id IS NOT NULL) AS icd_codes
  , nc.appt_date                                                                           AS next_apt_date
  , NULL::TEXT                                                                             AS last_visit_date
  , NULL::TEXT                                                                             AS discharge_date
FROM
    pats p
    LEFT JOIN patient_hccs phcc ON phcc.patient_id = p.id AND phcc.yr::DOUBLE PRECISION = DATE_PART('year'::TEXT, NOW())
    LEFT JOIN patient_hcc_captured_icd10s phc ON phc.patient_hcc_id = phcc.id
    LEFT JOIN hcc_icd10s hi ON phc.hcc_icd10_id = hi.id
    LEFT JOIN icd10s i10s ON hi.icd10_id = i10s.id
    LEFT JOIN addresses a ON p.id = a.patient_id AND a.rn = 1
    LEFT JOIN phones ph ON ph.patient_id = p.id AND ph.rn = 1
    LEFT JOIN next_cca nc ON nc.patient_id = p.id AND nc.rn = 1 AND nc.appt_date >= (NOW() - '15 days'::INTERVAL)
GROUP BY
    p.id, p.first_name, NULL::TEXT, p.last_name, p.dob, ("left"(p.gender, 1)), ph.phone_number, a.street, a.city
        , a.state, a.postal_code, nc.appt_date;


SELECT date_trunc('month', now())
, (date_trunc('month', now()) + '1 month'::interval)::date
    ;

-- delete from oban_crons where name = 'md_portal_roster_submission';
-- INSERT
-- INTO
--     public.oban_crons (name, expression, worker, opts, paused, lock_version, inserted_at, updated_at)
-- VALUES
--     ('md_portal_daily_roster_submission', '7 23 * * *', 'MD.MDPortals.MDPortalsRosterWorker', '{ "args": { "type": "daily" } }', FALSE, 1, now(), now()),
--     ('md_portal_discharge_roster_submission', '7 23 * * *', 'MD.MDPortals.MDPortalsRosterWorker', '{ "args": { "type": "discharge" } }', FALSE, 1, now(), now())
--     ;
INSERT
INTO
    public.oban_crons (name, expression, worker, opts, paused, lock_version, inserted_at, updated_at)
VALUES
    ('md_portal_docs_retrieval', '7 5 * * *', 'MD.MDPortals.MDPortalsDocumentWorker', '{}', FALSE, 1, now(), now())
    ;
;
SELECT *
FROM
    public.oban_crons where name ~* 'md_p';
SELECT * FROM md_portal_patients;
SELECT *
FROM
    oban_jobs where worker = 'MD.MDPortals.MDPortalsDocumentWorker';

SELECT
    COUNT(*)
  , COUNT(DISTINCT p_1.id)
FROM
    patients p_1
    JOIN patient_programs pp ON p_1.id = pp.patient_id
    JOIN analytics_risk_strat_info rs ON rs.score = p_1.risk_strat
WHERE
      pp.end_date IS NULL
  AND pp.program = '24_7'::TEXT
  AND (p_1.status = ANY (ARRAY ['engaged'::TEXT, 'attributed'::TEXT]));

