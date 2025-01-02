-- Date, Medication, Dosage, DosageUnit, StartDate, StopDate, StopReason, Active, DoseQuantity, DoseUnit, Dispense, DispenseUnit, Refills, NdcCode, DrugCode, PharmacyId, IsPrescription, MappedMedication, Srcstatus
-- add patient identifier and provider info columns as well for matching
SELECT
    inserted_at    signal_date
  , days_supply
  , start_date
  , end_date
  , quantity
  , refills_remaining
  , drug_description
  , ndc
  , dispenser_name
  , dispenser_ncpdpid
  , dispenser_npi
  , prescriber_name
  , prescriber_npi
  , prescriber_phone
  , pmf.patient_id
  , 'sure_scripts' source
FROM
    patient_medication_fills pmf
WHERE
    inserted_at > NOW() - '30 days'::INTERVAL
UNION
SELECT
    signal_date
  , days_supply
  , last_fill_date
  , next_fill_date
  , quantity
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
WHERE
    pmf.signal_date > NOW() - '30 days'::INTERVAL
    ;
