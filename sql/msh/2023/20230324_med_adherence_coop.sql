drop table stage.patient_medication_adherences;
CREATE TABLE stage.patient_medication_adherences (
    patient_id        BIGINT,
    measure_id        TEXT,
    drug_name         TEXT,
    ndc               text,
    days_supply       INT,
    next_fill_date    DATE,
    last_fill_date    DATE,
    adjusted_next_fill_date date,
    remaining_refills INT,
    prescriber_name   TEXT,
    prescriber_npi    TEXT,
    pharmacy_name     TEXT,
    pharmacy_npi      TEXT,
    pharmacy_phone    TEXT,
    failed_last_year  bool,
    analytics_id      bigint,
    inserted_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP NOT NULL DEFAULT NOW()
);
SELECT distinct quality_measure_id
FROM
    patient_medication_adherences;
SELECT *
FROM
    quality_measures where id in (15, 16, 17);


ALTER TABLE patient_medication_adherences
    ADD prescriber_npi          TEXT,
    ADD ndc                     TEXT,
    ADD analytics_id            BIGINT,
    ADD adjusted_next_fill_date DATE;




SELECT DISTINCT
    id
  , patient_id
  , quality_measure_id
  , year
  , drug_name
  , last_fill_date
  , next_fill_date
  , pharmacy
  , prescribing_provider_name
  , created_by_id
  , updated_by_id
  , inserted_at
  , updated_at
  , rx_days_supply
  , failed_last_year
FROM
    patient_medication_adherences
ORDER BY
    patient_id, quality_measure_id, drug_name
LIMIT 10 ;

SELECT *
FROM
    ( VALUES
          ('PDC-STA', 'med_adherence_cholesterol'),
          ('PDC-DR', 'med_adherence_diabetes'),
          ('PDC-RASA', 'med_adherence_hypertension') ) x(measure_id, measure_name);


SELECT * FROM medication_adherence_patient_task;
