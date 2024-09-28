-- select * from junk.md_portal_suspects_max_pat_batch;
-- 3220279
SELECT backed_up_date, count(1) FROM analytics.bk_up.bk_md_portal_suspects GROUP BY 1 order by 1;
SELECT *
FROM
    pg_stat_activity ;


SELECT inbound_file_id < 3220279, count(*)
FROM
     rawrp.md_portal_suspects
GROUP BY 1;
83,474,834
928,815,740
-- where
--     inbound_file_id <= 3220279;

WITH
    ins AS (
        INSERT INTO
            bk_up.bk_md_portal_suspects(id, inbound_file_id, job_run_step_id, patient_id,
                                        organization_source_patient_id,
                                        first_name, last_name, date_of_birth, icd_10_code, icd_10_code_desc,
                                        hcc_category,
                                        hcc_category_desc, reason, source_fact_name, source_supportive_evidence,
                                        source_excerpt, source_date, source_author, source_description, source_location,
                                        compendium_url, meta, inserted_at, backed_up_date)
            SELECT
                id
              , inbound_file_id
              , job_run_step_id
              , patient_id
              , organization_source_patient_id
              , first_name
              , last_name
              , date_of_birth
              , icd_10_code
              , icd_10_code_desc
              , hcc_category
              , hcc_category_desc
              , reason
              , source_fact_name
              , source_supportive_evidence
              , source_excerpt
              , source_date
              , source_author
              , source_description
              , source_location
              , compendium_url
              , meta
              , inserted_at
              , NOW() backed_up_date
            FROM
                rawrp.md_portal_suspects
            WHERE
                inbound_file_id < 3220279
            RETURNING id )
  , del AS (
    DELETE FROM rawrp.md_portal_suspects s
        USING ins b
        WHERE s.id = b.id
        RETURNING s.id )
SELECT
    COUNT(*) total_deleted
FROM
    del;

;




WITH
    ins AS (
        INSERT INTO
            bk_up.bk_md_portal_suspects(id, inbound_file_id, job_run_step_id, patient_id,
                                        organization_source_patient_id,
                                        first_name, last_name, date_of_birth, icd_10_code, icd_10_code_desc,
                                        hcc_category,
                                        hcc_category_desc, reason, source_fact_name, source_supportive_evidence,
                                        source_excerpt, source_date, source_author, source_description, source_location,
                                        compendium_url, meta, inserted_at, backed_up_date)
            SELECT
                id
              , inbound_file_id
              , job_run_step_id
              , patient_id
              , organization_source_patient_id
              , first_name
              , last_name
              , date_of_birth
              , icd_10_code
              , icd_10_code_desc
              , hcc_category
              , hcc_category_desc
              , reason
              , source_fact_name
              , source_supportive_evidence
              , source_excerpt
              , source_date
              , source_author
              , source_description
              , source_location
              , compendium_url
              , meta
              , inserted_at
              , NOW() backed_up_date
            FROM
                rawrp.md_portal_suspects s
            join junk.md_portal_suspects_max_pat_batch b on s.patient_id = b.ord
            WHERE
                inbound_file_id < 3220279
            RETURNING id )
  , del AS (
    DELETE FROM rawrp.md_portal_suspects s
        USING ins b
        WHERE s.id = b.id
        RETURNING s.id )
SELECT
    COUNT(*) total_deleted
FROM
    del;

;


------------------------------------------------------------------------------------------------------------------------
/* create new, drop table, recreate */
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE rawrp.md_portal_suspects_new (
    id                             bigserial PRIMARY KEY,
    inbound_file_id                BIGINT,
    job_run_step_id                BIGINT,
    patient_id                     TEXT,
    organization_source_patient_id TEXT,
    first_name                     TEXT,
    last_name                      TEXT,
    date_of_birth                  TEXT,
    icd_10_code                    TEXT,
    icd_10_code_desc               TEXT,
    hcc_category                   TEXT,
    hcc_category_desc              TEXT,
    reason                         TEXT,
    source_fact_name               TEXT,
    source_supportive_evidence     TEXT,
    source_excerpt                 TEXT,
    source_date                    TEXT,
    source_author                  TEXT,
    source_description             TEXT,
    source_location                TEXT,
    compendium_url                 TEXT,
    last_updated                   TEXT,
    meta                           jsonb,
    inserted_at                    TIMESTAMP DEFAULT NOW()                                                 NOT NULL,
    source_evidence_type           TEXT,
    source_document_type           TEXT,
    source_document_type_loinc     TEXT,
    source_document_title          TEXT,
    source_encounter_type          TEXT,
    source_encounter_organization  TEXT,
    source_organization_name       TEXT,
    source_author_software         TEXT,
    md_portals_batch_id            BIGINT,
    table_name                     TEXT GENERATED ALWAYS AS ((tableoid)::regclass) STORED,
    control_suspects_code_count    TEXT,
    source_text                    TEXT
);
create INDEX  on junk.md_portal_suspects_max_pat_batch(patient_id, inbound_file_id);

-- create table of just latest patient suspects
INSERT
INTO
    rawrp.md_portal_suspects_new (id, inbound_file_id, job_run_step_id, patient_id, organization_source_patient_id,
                                  first_name, last_name, date_of_birth, icd_10_code, icd_10_code_desc, hcc_category,
                                  hcc_category_desc, reason, source_fact_name, source_supportive_evidence,
                                  source_excerpt, source_date, source_author, source_description, source_location,
                                  compendium_url, last_updated, meta, inserted_at, source_evidence_type,
                                  source_document_type, source_document_type_loinc, source_document_title,
                                  source_encounter_type, source_encounter_organization, source_organization_name,
                                  source_author_software, md_portals_batch_id, control_suspects_code_count, source_text)
SELECT
    id
  , s.inbound_file_id
  , job_run_step_id
  , s.patient_id
  , s.organization_source_patient_id
  , first_name
  , last_name
  , date_of_birth
  , icd_10_code
  , icd_10_code_desc
  , hcc_category
  , hcc_category_desc
  , reason
  , source_fact_name
  , source_supportive_evidence
  , source_excerpt
  , source_date
  , source_author
  , source_description
  , source_location
  , compendium_url
  , last_updated
  , meta
  , inserted_at
  , source_evidence_type
  , source_document_type
  , source_document_type_loinc
  , source_document_title
  , source_encounter_type
  , source_encounter_organization
  , source_organization_name
  , source_author_software
  , md_portals_batch_id
  , control_suspects_code_count
  , source_text
FROM
    rawrp.md_portal_suspects s
    JOIN junk.md_portal_suspects_max_pat_batch j
         ON j.organization_source_patient_id  = s.organization_source_patient_id AND j.inbound_file_id <= s.inbound_file_id;
-- create index on junk.md_portal_suspects_max_pat_batch(organization_source_patient_id, inbound_file_id);
SELECT * FROM junk.md_portal_suspects_max_pat_batch;


SELECT
    mqm_rev.*
FROM
    prd.member_rx_adherence mqm_latest
    JOIN prd.member_rx_adherence mqm_rev ON mqm_latest.member_id = mqm_rev.member_id
        AND mqm_latest.measure_key = mqm_rev.measure_key
        AND mqm_latest.measure_year = mqm_rev.measure_year
        AND mqm_latest.signal_date > mqm_rev.signal_date
        AND mqm_latest.next_fill_date < mqm_rev.signal_date
WHERE
      mqm_latest.measure_year = 2024
  AND mqm_rev.measure_year = 2024
;


-- DROP TABLE IF EXISTS junk.md_portal_suspects_max_pat_batch;
-- create table junk.md_portal_suspects_max_pat_batch as
SELECT *
FROM
    junk.md_portal_suspects_max_pat_batch;
-- create UNIQUE INDEX on junk.md_portal_suspects_max_pat_batch(organization_source_patient_id, inbound_file_id);

-- temporary, should be able to remove after one run
        -- delete out suspects not used
        WITH
            ins AS (
                INSERT INTO
                    bk_up.bk_md_portal_suspects(id, inbound_file_id, job_run_step_id, patient_id,
                                                organization_source_patient_id,
                                                first_name, last_name, date_of_birth, icd_10_code, icd_10_code_desc,
                                                hcc_category,
                                                hcc_category_desc, reason, source_fact_name, source_supportive_evidence,
                                                source_excerpt, source_date, source_author, source_description,
                                                source_location,
                                                compendium_url, meta, inserted_at, backed_up_date)
                    SELECT
                        s.id
                      , s.inbound_file_id
                      , s.job_run_step_id
                      , s.patient_id
                      , s.organization_source_patient_id
                      , s.first_name
                      , s.last_name
                      , s.date_of_birth
                      , s.icd_10_code
                      , s.icd_10_code_desc
                      , s.hcc_category
                      , s.hcc_category_desc
                      , s.reason
                      , s.source_fact_name
                      , s.source_supportive_evidence
                      , s.source_excerpt
                      , s.source_date
                      , s.source_author
                      , s.source_description
                      , s.source_location
                      , s.compendium_url
                      , s.meta
                      , s.inserted_at
                      , NOW() backed_up_date
                    FROM
                        rawrp.md_portal_suspects s
                    left join junk.md_portal_suspects_max_pat_batch mb on mb.organization_source_patient_id = s.organization_source_patient_id
                    and mb.inbound_file_id = s.inbound_file_id
                    WHERE
                        md_portals_batch_id ISNULL
                    and mb.inbound_file_id ISNULL
                    RETURNING id )
        DELETE
        FROM
            rawrp.md_portal_suspects s
            USING ins b
        WHERE
            s.id = b.id;

-- 16277137
SELECT
    MAX(inbound_file_id) max_inbound_file_id_processed
FROM
    rawrp.md_portal_suspects
WHERE
    md_portals_batch_id IS NOT NULL;




call cb.sp_process_md_portal_suspects();

SELECT count(*)
FROM
    rawrp.md_portal_suspects; WHERE md_portals_batch_id ISNULL;
VACUUM ANALYSE rawrp.md_portal_suspects;

SELECT * FROM fdw_member_doc_stage.msh_md_portal_suspects
;
-- 927,661,201
-- 19,885,774;

-- move original table to bk_up
alter TABLE rawrp.md_portal_suspects SET SCHEMA bk_up;
vacuum full ANALYSE bk_up.md_portal_suspects;

-- create new empty table
CREATE TABLE rawrp.md_portal_suspects (
    id                             bigserial PRIMARY KEY ,
    inbound_file_id                BIGINT,
    job_run_step_id                BIGINT,
    patient_id                     TEXT,
    organization_source_patient_id TEXT,
    first_name                     TEXT,
    last_name                      TEXT,
    date_of_birth                  TEXT,
    icd_10_code                    TEXT,
    icd_10_code_desc               TEXT,
    hcc_category                   TEXT,
    hcc_category_desc              TEXT,
    reason                         TEXT,
    source_fact_name               TEXT,
    source_supportive_evidence     TEXT,
    source_excerpt                 TEXT,
    source_date                    TEXT,
    source_author                  TEXT,
    source_description             TEXT,
    source_location                TEXT,
    compendium_url                 TEXT,
    last_updated                   TEXT,
    meta                           jsonb,
    inserted_at                    TIMESTAMP DEFAULT NOW()                                                 NOT NULL,
    source_evidence_type           TEXT,
    source_document_type           TEXT,
    source_document_type_loinc     TEXT,
    source_document_title          TEXT,
    source_encounter_type          TEXT,
    source_encounter_organization  TEXT,
    source_organization_name       TEXT,
    source_author_software         TEXT,
    md_portals_batch_id            BIGINT,
    table_name                     TEXT GENERATED ALWAYS AS ((tableoid)::regclass) STORED,
    control_suspects_code_count    TEXT,
    source_text                    TEXT
);


CREATE INDEX md_portal_suspects_organization_source_patient_id_idx1
    ON rawrp.md_portal_suspects(organization_source_patient_id);

CREATE INDEX md_portal_suspects_inbound_file_id_idx1
    ON rawrp.md_portal_suspects(inbound_file_id);

CREATE INDEX md_portal_suspects_inbound_file_id_organization_source_pati_idx
    ON rawrp.md_portal_suspects(inbound_file_id, organization_source_patient_id)
    WHERE (md_portals_batch_id IS NULL);