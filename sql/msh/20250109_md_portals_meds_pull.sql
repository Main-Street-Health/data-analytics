-- Date, Medication, Dosage, DosageUnit, StartDate, StopDate, StopReason, Active, DoseQuantity, DoseUnit, Dispense, DispenseUnit, Refills, NdcCode, DrugCode, PharmacyId, IsPrescription, MappedMedication, Srcstatus
-- add patient identifier and provider info columns as well for matching


-- meds_last_30_days_20250109
SELECT
    pmf.inserted_at    signal_date
  , pmf.days_supply
  , pmf.start_date
  , pmf.end_date
  , pmf.quantity
  , pmf.refills_remaining
  , pmf.drug_description
  , pmf.ndc
  , pmf.dispenser_name
  , pmf.dispenser_ncpdpid
  , pmf.dispenser_npi
  , pmf.prescriber_name
  , pmf.prescriber_npi
  , pmf.prescriber_phone
  , pmf.patient_id
  , 'sure_scripts' source
FROM
    patient_medication_fills pmf
join supreme_pizza rp on rp.patient_id = pmf.patient_id and rp.is_md_portal_full
-- WHERE
--     inserted_at > NOW() - '30 days'::INTERVAL
UNION
SELECT
    pmf.signal_date
  , pmf.days_supply
  , pmf.last_fill_date
  , pmf.next_fill_date
  , pmf.quantity
  , NULL                 refills_remaining
  , drug_name            drug_description
  , ndc
  , pharmacy_name        dispenser_name
  , NULL                 dispenser_ncpdpid
  , NULL                 dispenser_npi
  , prescribing_provider prescriber_name
  , prescriber_npi
  , NULL                 prescriber_phone
  , pmf.patient_id
  , 'mco'                source
FROM
    stage.qm_pm_med_adh_mco_measures pmf
    join supreme_pizza rp on rp.patient_id = pmf.patient_id and rp.is_md_portal_full
WHERE
    not is_reversal
--     pmf.signal_date > NOW() - '30 days'::INTERVAL
    ;
SELECT * from stage._md_portals_medications_outbound();
-- drop function stage._md_portals_medications_outbound();
CREATE OR REPLACE FUNCTION stage._md_portals_medications_outbound()
    RETURNS TABLE (
        signal_date       TIMESTAMP,
        days_supply       INTEGER,
        start_date        DATE,
        end_date          DATE,
        quantity          NUMERIC,
        refills_remaining INTEGER,
        drug_description  TEXT,
        ndc               TEXT,
        dispenser_name    TEXT,
        dispenser_ncpdpid TEXT,
        dispenser_npi     TEXT,
        prescriber_name   TEXT,
        prescriber_npi    TEXT,
        prescriber_phone  TEXT,
        patient_id        bigint,
        source            TEXT
    )
AS
$$
BEGIN
    RETURN QUERY
        SELECT
            pmf.inserted_at
          , pmf.days_supply
          , pmf.start_date
          , pmf.end_date
          , pmf.quantity::numeric
          , pmf.refills_remaining
          , pmf.drug_description
          , pmf.ndc
          , pmf.dispenser_name
          , pmf.dispenser_ncpdpid
          , pmf.dispenser_npi
          , pmf.prescriber_name
          , pmf.prescriber_npi
          , pmf.prescriber_phone
          , pmf.patient_id
          , 'sure_scripts'::TEXT AS source
        FROM
            patient_medication_fills pmf
            JOIN supreme_pizza rp ON rp.patient_id = pmf.patient_id AND rp.is_md_portal_full
--     WHERE inserted_at > NOW() - '30 days'::INTERVAL
        UNION
        SELECT
            pmf.signal_date
          , pmf.days_supply
          , pmf.last_fill_date
          , pmf.next_fill_date
          , pmf.quantity
          , NULL::INTEGER
          , pmf.drug_name
          , pmf.ndc
          , pmf.pharmacy_name
          , NULL::TEXT
          , NULL::TEXT
          , pmf.prescribing_provider
          , pmf.prescriber_npi
          , NULL::TEXT
          , pmf.patient_id
          , 'mco'::TEXT
        FROM
            stage.qm_pm_med_adh_mco_measures pmf
            JOIN supreme_pizza rp ON rp.patient_id = pmf.patient_id AND rp.is_md_portal_full
        WHERE
            NOT is_reversal
--     AND pmf.signal_date > NOW() - '30 days'::INTERVAL
    ;
END;
$$ LANGUAGE plpgsql;


-- INSERT
-- INTO
--     oban.oban_jobs (queue, worker, args, max_attempts, scheduled_at, state)
-- VALUES
--     ('deus_outbound', 'Deus.Outbound.OutboundFileWorker', '{
--       "sql": "select * from stage._md_portals_medications_outbound();",
--       "file_name_pattern": "meds_last_30_days_{{datetime:%Y%m%d}}.csv",
--       "database_connection_id": 34,
--       "ftp_server_name": "md_portal_nlp_issues",
--       "s3_bucket": "msh-analytics-us-east-1-prd",
--       "s3_path": "outbound/md_portals/patient_medications",
--       "timeout": 14400000
--     }', 1, NOW(), 'available')
-- returning *
-- ;
SELECT * FROM member_doc.oban.oban_jobs where id = 203401886;
SELECT *
FROM
    file_router.pushed_files order by id desc;

SELECT *
FROM
    qm_patient_measures WHERE patient_id = 165377;