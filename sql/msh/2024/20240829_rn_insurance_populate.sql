
SELECT * FROM rn_insurance_carriers;
-- load all ideon_provider_networks's  carriers into rn_insurance_carriers
INSERT
INTO
    rn_insurance_carriers (name, inserted_at, updated_at)
SELECT DISTINCT
    carrier_name
  , NOW()
  , NOW()
FROM
    ideon_provider_networks n
WHERE
    NOT EXISTS( SELECT 1
                FROM rn_insurance_carriers c
                WHERE c.name = n.carrier_name )
ORDER BY
    n.carrier_name;

SELECT *
FROM rn_provider_insurances
    ;
SELECT *
FROM
    rn_ref_insurance_types;
-- key
-- om
-- medicaid
-- ma
-- commercial
-- uninsured
-- other

select distinct line_of_coverage from     ideon_provider_networks ipn;

-- load all ideon_provider_networks to rn_provider_insurances (comercial+medicaid, ma done (verify))
INSERT
INTO
    rn_provider_insurances AS i(rn_provider_id, rn_insurance_carrier_id, insurance_type_key, source)
SELECT DISTINCT
    p.id
  , ic.id
  , CASE WHEN ipn.line_of_coverage = 'medicaid'           THEN 'medicaid'
         WHEN ipn.line_of_coverage = 'medical'            THEN 'commercial'
         WHEN ipn.line_of_coverage = 'medicare_advantage' THEN 'ma' END insurance_type_key
  , 'ideon'
FROM
    ideon_provider_networks ipn
    JOIN rn_providers p ON p.npi = ipn.search_npi
    JOIN rn_insurance_carriers ic ON ic.name = ipn.carrier_name
WHERE
    NOT EXISTS( SELECT
                    1
                FROM
                    rn_provider_insurances pi
                WHERE
                      pi.rn_provider_id = p.id
                  AND pi.rn_insurance_carrier_id = ic.id
                  AND pi.insurance_type_key = CASE WHEN ipn.line_of_coverage = 'medicaid'           THEN 'medicaid'
                                                   WHEN ipn.line_of_coverage = 'medical'            THEN 'commercial'
                                                   WHEN ipn.line_of_coverage = 'medicare_advantage' THEN 'ma' END )
;
SELECT *
FROM
    payers;

INSERT
INTO
    rn_insurance_carriers (name, inserted_at, updated_at)
VALUES
    ('Medicare', now(), now()) RETURNING  *;
-- id = 370
-- cms data in rn_insurance_carriers, rn_provider_insurances for om
INSERT
INTO
    rn_provider_insurances (rn_provider_id, rn_insurance_carrier_id, insurance_type_key, source)
SELECT DISTINCT
    rp.id
  , 370
  , 'om'
  , 'cms'
FROM
    junk.cms_ppef_enrollment_extract_2024_07_20240812 cms
    JOIN rn_providers rp ON rp.npi = cms.npi
    ;


SELECT
    c.name
  , f.name
  , p.full_name
  , fp.specialty_type_key
FROM
    rn_insurance_carriers c
    JOIN rn_provider_insurances i ON c.id = i.rn_insurance_carrier_id
    JOIN rn_providers p ON i.rn_provider_id = p.id
    JOIN rn_facility_providers fp ON fp.rn_provider_id = p.id
    JOIN rn_facilities f ON fp.rn_facility_id = f.id
WHERE
      c.name = 'Humana'
  AND i.insurance_type_key = 'ma'
  AND specialty_type_key = 'gi'
;