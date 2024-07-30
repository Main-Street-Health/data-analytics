------------------------------------------------------------------------------------------------------------------------
/* Reveleer Two Oh */
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
/* existing tables */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM reveleer_files;
SELECT * FROM reveleer_chase_file_details;
SELECT * FROM reveleer_attribute_file_details;
SELECT * FROM reveleer_compliance_file_details;

SELECT * FROM reveleer_cca_pdfs;
SELECT * FROM reveleer_projects;

------------------------------------------------------------------------------------------------------------------------
/* new table to abstract chases
   Avoid directly tying reveleer_chase_file_details directly to pqm's
   Allows one pqm to tie to multiple payers/revleer projects
   Would allow us to avoid creating pqm, patients, et al when doing an EOY scramble alla qm chase '23
*/
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE reveleer_chases (
    id                    BIGSERIAL PRIMARY KEY,
    patient_id            BIGINT                  NOT NULL,
    measure_key           TEXT                    NOT NULL,
    due_date              DATE                    NOT NULL,
    yr                    INT                     NOT NULL, -- not sure about this one
    reveleer_project_id   BIGINT                  NOT NULL,
    qm_patient_measure_id BIGINT,
    external_chase_id     TEXT,                             -- not sure if this should be a bigint
    is_active             bool      DEFAULT TRUE  NOT NULL, -- I think we need this one so we know to stop sending the measure, not sure how we'd populate though
    inserted_at           TIMESTAMP DEFAULT NOW() NOT NULL,
    updated_at            TIMESTAMP DEFAULT NOW() NOT NULL
);


CREATE UNIQUE INDEX ON reveleer_chases(patient_id, measure_key, due_date, yr, reveleer_project_id);


------------------------------------------------------------------------------------------------------------------------
/* update existing detail tables to:
   1. switch from state_payer_id to reveleer_project_id to link detail records to rev proj
   2. link detail tables to the main chase
*/
------------------------------------------------------------------------------------------------------------------------
-- cfd
ALTER TABLE reveleer_chase_file_details add reveleer_project_id bigint;
UPDATE reveleer_chase_file_details cfd
SET
    reveleer_project_id = rp.id
FROM
    reveleer_projects rp
WHERE
    rp.state_payer_id = cfd.state_payer_id;

ALTER TABLE reveleer_chase_file_details DROP state_payer_id;

-- attr
ALTER TABLE reveleer_attribute_file_details add reveleer_project_id bigint;
UPDATE reveleer_attribute_file_details cfd
SET
    reveleer_project_id = rp.id
FROM
    reveleer_projects rp
WHERE
    rp.state_payer_id = cfd.state_payer_id;

ALTER TABLE reveleer_attribute_file_details DROP state_payer_id;

-- comp
ALTER TABLE reveleer_compliance_file_details add reveleer_project_id bigint;
UPDATE reveleer_compliance_file_details cfd
SET
    reveleer_project_id = rp.id
FROM
    reveleer_projects rp
WHERE
    rp.state_payer_id = cfd.state_payer_id;

ALTER TABLE reveleer_chase_file_details DROP state_payer_id;

-- add link to main rev chase record
ALTER TABLE reveleer_chase_file_details add reveleer_chase_id bigint;
ALTER TABLE reveleer_attribute_file_details add reveleer_chase_id bigint;
ALTER TABLE reveleer_compliance_file_details add reveleer_chase_id bigint;


-- make projects based on yr
ALTER TABLE reveleer_projects
    ADD yr INT NOT NULL DEFAULT 2023;

ALTER TABLE reveleer_projects
    ADD is_active bool NOT NULL DEFAULT TRUE;
update reveleer_projects set is_active = false;
------------------------------------------------------------------------------------------------------------------------
/* New process */
------------------------------------------------------------------------------------------------------------------------
-- 1. pull all pqm's that are in a state/status that should be shipped to reveleer (another option is qm workflow signalling specifically to hit reveleer)
-- 2. create reveleer_chases records for each payer+pqm if not already created
-- 3. Using now populated reveleer_chases table stage the detail records for incremental or refresh
   -- refresh (aka we've already sent the payer pqm)
     -- only send if we have a new attribute we haven't previously sent
     -- populate compliance detail if status = "system_closed"
   -- incremental (aka new payer pqm)
     -- populate cfd + any required attributes
-- 4. Oban job to trigger deus to pull all the detail records without file_id's
  -- create files, (first refresh, then incremental, can likely pass this as oban job args)
  -- zip and ship
  -- update detail records with file id
-- 5. Ship all non shipped docs for patients with any chases
    
    
------------------------------------------------------------------------------------------------------------------------
/* wish list */
------------------------------------------------------------------------------------------------------------------------
-- 6. Return files
  -- process row level return files
    -- mark the external_chase_id on the main chase record if not already populated
    -- flag detail record as successfully ingested or not
  -- process patient docs sent
    -- patient_id, file_name, imported_at, error


-- chase detail response file wishlist
-- would want similar for attr + compliance
-- would require revleer to accept a unique id with every detail record
CREATE TABLE reveleer_detail_responses (
    reveleer_chase_file_detail_id BIGINT,
    reveleer_chase_id             BIGINT,
    external_chase_id             TEXT,
    successfully_imported         bool                    NOT NULL,
    error                         TEXT,
    imported_at                   TIMESTAMP,
    inserted_at                   TIMESTAMP DEFAULT NOW() NOT NULL
);


CREATE TABLE reveleer_patient_document_responses (
    patient_id            BIGINT,
    file_name             TEXT,
    imported_at           TIMESTAMP,
    successfully_imported bool NOT NULL,
    error                 TEXT
);



------------------------------------------------------------------------------------------------------------------------
/* next steps */
------------------------------------------------------------------------------------------------------------------------
-- deus
    -- move the table changes to migration
    -- refactor out sproc call
    -- make file generator an oban job
-- sql
    -- re write the data stager sproc on new qm2 tables
    -- pdf sproc can remove the pdf guard