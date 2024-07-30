-- delete from sure_scripts_med_history_details WHERE sure_scripts_panel_id = 35
-- delete from sure_scripts_med_histories WHERE sure_scripts_panel_id = 35
SELECT count(1) FROM sure_scripts_med_history_details;
-- UPDATE sure_scripts_med_history_details mhd
-- SET
--     patient_id = pp.patient_id
-- FROM
--     sure_scripts_panel_patients pp
-- WHERE
--       mhd.patient_id::bigint = pp.id
--   AND pp.sure_scripts_panel_id = 35
--   AND mhd.sure_scripts_panel_id = 35;
-- CREATE PROCEDURE msh_roster_build()
--     LANGUAGE plpgsql
-- AS
-- $$
-- BEGIN

SELECT *
FROM
    ref.med_adherence_value_sets;

DROP TABLE IF EXISTS _patient_meds;
CREATE TEMP TABLE _patient_meds AS
SELECT DISTINCT
    mhd.patient_id
  , mhd.patient_date_of_birth                                                                                                                  dob
  , mhd.drug_description
  , mhd.product_code                                                                                                                           ndc
  , mhd.days_supply
  , mhd.refills_value
  , mhd.refills_qualifier -- REM remaining number of refills
  , COALESCE(mhd.sold_date, last_filled_date, written_date)                                                                                    start_date
  , mhd.written_date
  , mhd.last_filled_date
  , mhd.sold_date
  , COALESCE(mhd.sold_date, last_filled_date, written_date) + days_supply::INT - 1                                                             last_day_of_meds
  , COALESCE(mhd.sold_date, last_filled_date, written_date) + days_supply::INT                                                                 next_fill_date
  , nc.non_proprietary_name
  , ROW_NUMBER()
    OVER (PARTITION BY patient_id, COALESCE(nc.non_proprietary_name, mhd.product_code) ORDER BY COALESCE(mhd.sold_date, mhd.last_filled_date)) rn
FROM
    sure_scripts_med_histories mh
    JOIN sure_scripts_med_history_details mhd ON mh.id = mhd.sure_scripts_med_history_id
    JOIN ref.med_adherence_value_sets vs on vs.code = mhd.product_code
    JOIN ref.med_adherence_measures m on m.value_set_id = vs.value_set_id and m.measure_id in ('PDC-DR', 'PDC-RASA', 'PDC-STA','SUPD')
    LEFT JOIN ref.ndc_cder nc ON nc.ndc = mhd.product_code
WHERE
      COALESCE(mhd.sold_date, mhd.last_filled_date, mhd.written_date) >= '2023-01-01'
  AND mhd.product_code_qualifier = 'ND'; -- only have ndc's

create index _patient_meds_ndc_idx on _patient_meds(ndc);
create index _patient_meds_patient_id_idx on _patient_meds(patient_id);


-- weird case of 2 NDC's same generic name but picked up on same days
SELECT
    patient_id
  , ndc
  , non_proprietary_name
  , rn
  , days_supply
  , refills_value
  , sold_date
FROM
    _patient_meds
-- WHERE
--     patient_id = '37390'
ORDER BY
    patient_id, non_proprietary_name, rn, ndc;

-- A1
-- |--------|
-- A2
-- |--------|
--           A3
--           |---|
--                A4
--                |--------|




-- SELECT * FROM _patient_meds;
-- SELECT count(*) FROM _patient_meds
;

------------------------------------------------------------------------------------------------------------------------
/* PDC-DR Medication Adherence for Patients with Diabetes All Class (MAD). Part D Measure. */
------------------------------------------------------------------------------------------------------------------------
--
-- A 90d supply -|
-- |-------------|
--         A
--         |------------|
--     B
--     |------------|
--              B
--              |-----------|


-- SELECT * FROM sure_scripts_responses where hdr_load_status_description = 'Failed to load file.  No data was processed.';

DROP TABLE IF EXISTS _med_adherence;
CREATE TEMP TABLE _med_adherence AS
SELECT
    pm.patient_id
  , 'PDC-DR'                                                                                        measure_id
  , ARRAY_AGG(DISTINCT pm.drug_description)                                                         drugs
  , ARRAY_AGG(DISTINCT pm.ndc)                                                                      ndcs
  , COUNT(DISTINCT pm.ndc)                                                                          nd_rx
  , COUNT(DISTINCT pm.sold_date)                                                                    nd_sold_dates
  , MIN(pm.start_date)                                                                               ipsd
  , MAX(pm.last_day_of_meds)                                                                        run_out_date
  , COUNT(DISTINCT d.day)                                                                           n_days_covered
  , COUNT(DISTINCT d_treatment_period.day)                                                          n_days_treatment_to_eoy
  , COUNT(DISTINCT d_treatment_period.day) FILTER ( WHERE d_treatment_period.day <= current_date )  n_days_treatment_to_today
  , MAX(pm.last_day_of_meds) - MIN(pm.start_date) + 1                                                total_days_since_ipsd_to_last_day_of_meds
FROM
    ref.med_adherence_measures m
    JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
    JOIN _patient_meds pm ON pm.ndc = vs.code AND vs.code_type = 'NDC' and pm.start_date BETWEEN vs.from_date and coalesce(vs.thru_date, now()::date)
    LEFT JOIN ref.dates d ON d.day BETWEEN pm.start_date AND pm.last_day_of_meds
    LEFT JOIN ref.dates d_treatment_period ON d_treatment_period.day BETWEEN pm.start_date AND '2023-12-31'::date
WHERE
      m.measure_id = 'PDC-DR'
  AND m.measure_version = '2023' -- NOTE: vsn numb was null on import, assumed 2022
  AND m.table_id IN ('BG', 'SFU', 'TZD', 'DPP4', 'GLP1', 'MEG', 'SGLT2')
      -- exclude anyone with any insulin rx
  AND NOT EXISTS(SELECT
                     1
                 FROM
                     ref.med_adherence_measures im
                     JOIN ref.med_adherence_value_sets ivs ON ivs.value_set_id = im.value_set_id
                     JOIN _patient_meds ipm ON ipm.ndc = ivs.code AND ivs.code_type = 'NDC' and ipm.start_date BETWEEN ivs.from_date and coalesce(ivs.thru_date, now()::date)
                 WHERE
                       im.measure_id = 'PDC-DR'
                   AND im.table_id = 'INSULINS'
                   AND im.measure_version = '2022' -- NOTE: vsn numb was null on import, assumed 2022
                   AND ipm.patient_id = pm.patient_id
    )
GROUP BY
    1
;


create unique index _med_adherence_patient_id_measure_id_idx on _med_adherence(patient_id, measure_id);

------------------------------------------------------------------------------------------------------------------------
/* PDC-RASA: Medication Adherence for Patients on Renin Angiotensin System (“RAS”) Antagonists (Hypertension/MAH). Part D Measure. */
------------------------------------------------------------------------------------------------------------------------
-- SELECT distinct measure_id
-- FROM
--     ref.med_adherence_measures m
-- --     JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
-- --     JOIN _patient_meds pm ON pm.ndc = vs.code AND vs.code_type = 'NDC'
-- --     LEFT JOIN ref.dates d ON d.day BETWEEN pm.sold_date AND pm.last_day_of_meds
-- WHERE
--       m.measure_name ~* 'renin'
--     ;
INSERT
INTO
    _med_adherence (patient_id, measure_id, drugs, ndcs, nd_rx, nd_sold_dates, ipsd, run_out_date, n_days_covered, n_days_treatment_to_eoy, n_days_treatment_to_today, total_days_since_ipsd_to_last_day_of_meds)
SELECT
    pm.patient_id
  , 'PDC-RASA'
  , ARRAY_AGG(DISTINCT pm.drug_description)          drugs
  , ARRAY_AGG(DISTINCT pm.ndc)                       ndcs
  , COUNT(DISTINCT pm.ndc)                           nd_rx
  , COUNT(DISTINCT pm.sold_date)                     nd_rx_sold
  , MIN(pm.start_date)                                ipsd
  , MAX(pm.last_day_of_meds)                         run_out_date
  , COUNT(DISTINCT d.day)                            n_days_covered
  , COUNT(DISTINCT d_treatment_period.day)                                                          n_days_treatment_to_eoy
  , COUNT(DISTINCT d_treatment_period.day) FILTER ( WHERE d_treatment_period.day <= current_date )  n_days_treatment_to_today
  , MAX(pm.last_day_of_meds) - MIN(pm.start_date) + 1 total_days_since_ipsd_to_last_day_of_meds
FROM
    ref.med_adherence_measures m
    JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
    JOIN _patient_meds pm ON pm.ndc = vs.code AND vs.code_type = 'NDC' and pm.start_date BETWEEN vs.from_date and coalesce(vs.thru_date, now()::date)
    LEFT JOIN ref.dates d ON d.day BETWEEN pm.start_date AND pm.last_day_of_meds
    LEFT JOIN ref.dates d_treatment_period ON d_treatment_period.day BETWEEN pm.start_date AND '2023-12-31'::date
WHERE
--       m.measure_name ~* 'renin'
    m.measure_id = 'PDC-RASA'
  AND m.table_id = 'RASA'
  AND m.measure_version = '2022'
  -- exclude any with  sacubitril/valsartan rx
  AND NOT EXISTS(
        SELECT
            1
        FROM
            ref.med_adherence_measures xm
            JOIN ref.med_adherence_value_sets xvs ON xvs.value_set_id = xm.value_set_id
            JOIN _patient_meds xpm ON xpm.ndc = xvs.code AND xvs.code_type = 'NDC' and pm.start_date BETWEEN vs.from_date and coalesce(vs.thru_date, now()::date)
        WHERE
              xm.measure_id = 'PDC-RASA'
          AND xm.table_id = 'SAC-VAL'
          AND xm.measure_version = '2022'
          AND xpm.patient_id = pm.patient_id
    )
GROUP BY
    1;

------------------------------------------------------------------------------------------------------------------------
/* PDC-STA  */
------------------------------------------------------------------------------------------------------------------------

INSERT
INTO
    _med_adherence (patient_id, measure_id, drugs, ndcs, nd_rx, nd_sold_dates, ipsd, run_out_date, n_days_covered, n_days_treatment_to_eoy, n_days_treatment_to_today, total_days_since_ipsd_to_last_day_of_meds)
SELECT
    pm.patient_id
  , 'PDC-STA'
  , ARRAY_AGG(DISTINCT pm.drug_description)          drugs
  , ARRAY_AGG(DISTINCT pm.ndc)                       ndcs
  , COUNT(DISTINCT pm.ndc)                           nd_rx
  , COUNT(DISTINCT pm.sold_date)                     nd_rx_sold
  , MIN(pm.start_date)                                ipsd
  , MAX(pm.last_day_of_meds)                         run_out_date
  , COUNT(DISTINCT d.day)                            n_days_covered
  , COUNT(DISTINCT d_treatment_period.day)                                                          n_days_treatment_to_eoy
  , COUNT(DISTINCT d_treatment_period.day) FILTER ( WHERE d_treatment_period.day <= current_date )  n_days_treatment_to_today
  , MAX(pm.last_day_of_meds) - MIN(pm.start_date) + 1 total_days_since_ipsd_to_last_day_of_meds
FROM
    ref.med_adherence_measures m
    JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
    JOIN _patient_meds pm ON pm.ndc = vs.code AND vs.code_type = 'NDC' and pm.start_date BETWEEN vs.from_date and coalesce(vs.thru_date, now()::date)
    LEFT JOIN ref.dates d ON d.day BETWEEN pm.start_date AND pm.last_day_of_meds
    LEFT JOIN ref.dates d_treatment_period ON d_treatment_period.day BETWEEN pm.start_date AND '2023-12-31'::date
WHERE
      m.measure_id = 'PDC-STA'
  AND table_id = 'STATINS'
GROUP BY
    1;

------------------------------------------------------------------------------------------------------------------------
/* SUPD Statin Use in Persons with Diabetes */
------------------------------------------------------------------------------------------------------------------------
INSERT
INTO
    _med_adherence (patient_id, measure_id, drugs, ndcs, nd_rx, nd_sold_dates, ipsd, run_out_date, n_days_covered, n_days_treatment_to_eoy, n_days_treatment_to_today, total_days_since_ipsd_to_last_day_of_meds)
WITH
--     ≥2 prescription claims on different dates of service for any diabetes medication (Medication Table DIABETES) during the measurement year.
diabetics AS ( SELECT
                   pm.patient_id
               FROM
                   ref.med_adherence_measures m
                   JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
                   JOIN _patient_meds pm ON pm.ndc = vs.code AND vs.code_type = 'NDC' and pm.start_date BETWEEN vs.from_date and coalesce(vs.thru_date, now()::date)
               WHERE
                     m.measure_id = 'SUPD'
                 AND m.table_id = 'DIABETES'
                 AND m.measure_version = '2022'
                     -- fertility exclusion
                 AND NOT EXISTS(
                       SELECT
                           1
                       FROM
                           ref.med_adherence_measures xm
                           JOIN ref.med_adherence_value_sets xvs ON xvs.value_set_id = xm.value_set_id
                           JOIN _patient_meds xpm ON xpm.ndc = xvs.code AND xvs.code_type = 'NDC' and xpm.start_date BETWEEN xvs.from_date and coalesce(xvs.thru_date, now()::date)
                       WHERE
                             xm.measure_id = 'SUPD'
                         AND xm.is_med = 'Y'
                         AND xm.is_exclusion = 'Y'
                         AND xm.measure_version = '2022'
                         AND xpm.patient_id = pm.patient_id
                   )
               GROUP BY 1
               HAVING
                   COUNT(DISTINCT pm.written_date) > 1 )
SELECT
    pm.patient_id
  , 'SUPD'
  , ARRAY_AGG(DISTINCT pm.drug_description)          drugs
  , ARRAY_AGG(DISTINCT pm.ndc)                       ndcs
  , COUNT(DISTINCT pm.ndc)                           nd_rx
  , COUNT(DISTINCT pm.sold_date)                     nd_rx_sold
  , MIN(pm.start_date)                                ipsd
  , MAX(pm.last_day_of_meds)                         run_out_date
  , COUNT(DISTINCT d.day)                            n_days_covered
  , COUNT(DISTINCT d_treatment_period.day)                                                          n_days_treatment_to_eoy
  , COUNT(DISTINCT d_treatment_period.day) FILTER ( WHERE d_treatment_period.day <= current_date )  n_days_treatment_to_today
  , MAX(pm.last_day_of_meds) - MIN(pm.start_date) + 1 total_days_since_ipsd_to_last_day_of_meds
FROM
    ref.med_adherence_measures m
    JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
    JOIN _patient_meds pm ON pm.ndc = vs.code AND vs.code_type = 'NDC' and pm.start_date BETWEEN vs.from_date and coalesce(vs.thru_date, now()::date)
    JOIN diabetics db ON db.patient_id = pm.patient_id
    LEFT JOIN ref.dates d ON d.day BETWEEN pm.start_date AND pm.last_day_of_meds
    LEFT JOIN ref.dates d_treatment_period ON d_treatment_period.day BETWEEN pm.start_date AND '2023-12-31'::date
WHERE
      m.measure_id = 'SUPD'
  AND m.table_id = 'STATINS'
  AND m.measure_version = '2022'
GROUP BY
    1;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
-- rough_med_adherence_measures_v2
SELECT DISTINCT
    ma.patient_id
  , p.first_name
  , p.last_name
  , p.dob
  , ma.measure_id
  , ma.drugs
  , ma.ndcs
  , ma.nd_rx
  , ma.nd_sold_dates
  , ma.ipsd
  , ma.run_out_date
  , ma.n_days_covered
  , ma.total_days_since_ipsd_to_last_day_of_meds
  , py.name payer_name
--   , string_agg(distinct rp.name , ',' )                             referring_partner
--   , string_agg(distinct rpo.name, ',' )                             referring_partner_organization
--   , STRING_AGG(DISTINCT u.full_name, ',') health_navigators
FROM
    _med_adherence ma
    JOIN fdw_member_doc.patients p ON p.id = ma.patient_id::BIGINT
    LEFT JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = p.id
    LEFT JOIN fdw_member_doc.payers py ON py.id = sp.patient_payer_id;

--     LEFT JOIN fdw_member_doc.patient_referring_partners prp ON prp.patient_id = ma.patient_id::BIGINT
--     LEFT JOIN fdw_member_doc.referring_partners rp ON prp.referring_partner_id = rp.id
--     LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id
--     LEFT JOIN fdw_member_doc.msh_care_team_referring_partners ctrp ON rp.id = ctrp.referring_partner_id
--     LEFT JOIN fdw_member_doc.care_teams c ON ctrp.care_team_id = c.id
--     LEFT JOIN fdw_member_doc.care_team_members ctm ON c.id = ctm.care_team_id AND role = 'health_navigator'
--     LEFT JOIN fdw_member_doc.users u ON u.id = ctm.user_id
-- GROUP BY
--     1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
;
SELECT * FROM ref.med_adherence_measures;
SELECT * FROM ref.med_adherence_value_sets;
;
-- END
--     $$

SELECT * FROM _med_adherence WHERE patient_id::int = 148899;

select current_date - '2023-03-12';

SELECT
    measure_id
  , COUNT(*)
FROM
    _med_adherence
GROUP BY
    1
;
------------------------------------------------------------------------------------------------------------------------
/*
-- How do we want to do dx based exclusions, should we use external_dx?

*/
------------------------------------------------------------------------------------------------------------------------


-- same medication for the same patient, written by the same provider, dispensed on the same day at the same pharmacy
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
-- missing 366 ndcs in ref
-- SELECT
--     COUNT(DISTINCT product_code)
--   , COUNT(DISTINCT product_code) FILTER ( WHERE rn.id ISNULL)
-- FROM
--     sure_scripts_med_history_details mhd
--     LEFT JOIN ref.ndc_cder rn ON mhd.product_code = rn.ndc
-- WHERE
--     mhd.note IS NULL
;

-- CREATE TABLE pma_patient_meds (
--     id                     BIGSERIAL PRIMARY KEY,
--     patient_id             BIGSERIAL NOT NULL,
--     ndc                    TEXT      NOT NULL,
--     prescriber_npi         TEXT,
--     drug_description       TEXT,
--     product_code           TEXT,
--     product_code_qualifier TEXT,
--     strength               TEXT,
--     quantity_prescribed    TEXT,
--     days_supply            TEXT,
--     directions             TEXT,
--     refills_value          TEXT,
--     written_date           DATE,
--     last_filled_date       DATE,
--     sold_date              DATE,
--     last_day_of_fill       DATE,
--     ncpdpid                TEXT,
--     pharmacy_npi           TEXT,
--     yr                     INT,
--     source                 TEXT, -- sure scripts, claims, manually entered?
-- --     is_current             BOOLEAN   NOT NULL DEFAULT TRUE, -- next fill gets new row?
--     inserted_at            TIMESTAMP NOT NULL DEFAULT NOW(),
--     updated_at             TIMESTAMP NOT NULL DEFAULT NOW()
-- );
--
-- create index pma_patient_meds_patient_id_idx on pma_patient_meds(patient_id);
-- create index pma_patient_meds_ndc_idx on pma_patient_meds(ndc);
--
-- CREATE TABLE pma_measures (
--     id                         BIGSERIAL PRIMARY KEY,
--     yr                         INT       NOT NULL,
--     patient_id                 BIGINT    NOT NULL,
--     measure_id                 TEXT      NOT NULL,
--     n_fills                    INT,
--     ndcs                       TEXT[],
--     nd_rx                      INT,
--     ipsd                       DATE,
--     run_out_date               DATE,
--     covered_days               INT,
--     treatment_period_days      INT,
--     proportion_of_days_covered DECIMAL(5, 2),
--     is_excluded_by_machine     BOOLEAN   NOT NULL DEFAULT FALSE,
--     is_excluded_by_user        BOOLEAN   NOT NULL DEFAULT FALSE,
--     excluded_by_id             BIGINT,
--     excluded_at                TIMESTAMP,
--     machine_exclusions         TEXT[],
--     user_exclusions            TEXT[],
--     inserted_at                TIMESTAMP NOT NULL DEFAULT NOW(),
--     updated_at                 TIMESTAMP NOT NULL DEFAULT NOW()
-- );
--
-- CREATE UNIQUE INDEX pma_measures_patient_id_measure_id_yr ON pma_measures(yr, patient_id, measure_id);
--
-- -- Maybe do this instead of ndcs array?
-- CREATE TABLE pma_measure_meds (
--     id                 BIGSERIAL PRIMARY KEY,
--     pma_patient_med_id BIGINT,
--     pma_measure_id     BIGINT
-- );
--
--
-- CREATE INDEX pma_measure_meds_patient_med_id_idx ON pma_measure_meds(pma_patient_med_id);
-- CREATE INDEX pma_measure_meds_measure_id_idx ON pma_measure_meds(pma_measure_id);
--
--
--
-- SELECT *
-- FROM
--     ref.med_adherence_measures;
--
-- -- CREATE TABLE pharmacies ( id                      BIGSERIAL PRIMARY KEY, ncpdpid                 TEXT, pharmacy_npi            TEXT, pharmacy_name           TEXT, pharmacy_address_line_1 TEXT, pharmacy_address_line_2 TEXT, pharmacy_city           TEXT, pharmacy_state          TEXT, pharmacy_zip            TEXT, pharmacy_phone_number   TEXT, pharmacy_fax_number     TEXT ); CREATE TABLE prescribers ( id                      BIGSERIAL PRIMARY KEY, ncpdpid                 TEXT, pharmacy_npi            TEXT, pharmacy_name           TEXT, pharmacy_address_line_1 TEXT, pharmacy_address_line_2 TEXT, pharmacy_city           TEXT, pharmacy_state          TEXT, pharmacy_zip            TEXT, pharmacy_phone_number   TEXT, pharmacy_fax_number     TEXT );
--
-- SELECT
--     id
--   , drug_description
--   , product_code
--   , product_code_qualifier
--   , strength
--   , quantity_prescribed
--   , days_supply
--   , directions
--   , refills_value
--   , written_date
--   , last_filled_date
--   , sold_date
-- FROM
--     sure_scripts_med_history_details
-- WHERE
--     note IS NULL;
--
--
--
--
-- -- and note is not null
--
-- -- create table patient_medications