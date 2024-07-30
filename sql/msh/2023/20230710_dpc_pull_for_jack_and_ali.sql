DROP TABLE IF EXISTS _all_hits;
CREATE TEMP TABLE _all_hits AS
SELECT
    dp.mbi
  , dp.source_id                               patient_id
  , c.billable_period_start                    dos
  , DATE_PART('year', c.billable_period_start::date) yr
  , dcl.procedure_code
FROM
    dpoc_claims c
    JOIN dpoc_claim_lines dcl ON c.id = dcl.dpoc_claim_id
    JOIN dpoc_patients dp ON dp.bene_id = c.patient
WHERE
        dcl.procedure_code IN
        ('J9271', 'J9299', 'J2505', 'J2506', 'J9022', 'J9173', 'J9145', 'J9306', 'J0897', 'J9305', 'Q5117', 'Q5107',
         'J0881', 'J9355', 'J1439', 'J9228', 'J1300', 'J9035', 'J9264', 'J9354', 'J2353', 'Q5115', 'J2796', 'J9041',
         'J9312', 'J9311', 'J9144', 'Q5114', 'Q5112', 'Q5116', 'Q5118', 'Q5126')
;
SELECT count(*)
FROM
    _all_hits;
-- 72848


DROP TABLE IF EXISTS _first_hits;
CREATE TEMP TABLE _first_hits AS
SELECT DISTINCT on(patient_id, procedure_code) *
FROM
    _all_hits
order by patient_id, procedure_code, dos
;

SELECT
    ah.*
  , pa.postal_code patient_zip
  , pa.county patient_county
  , fh.yr IS NOT NULL is_first_hit
FROM
    _all_hits ah
    LEFT JOIN fdw_member_doc.patient_addresses pa ON pa.patient_id = ah.patient_id
    LEFT JOIN _first_hits fh
              ON ah.patient_id = fh.patient_id AND fh.dos = ah.dos AND fh.procedure_code = ah.procedure_code
;

DROP TABLE IF EXISTS _all_pats;
CREATE TEMP TABLE _all_pats AS
SELECT distinct
    dp.mbi
  , dp.source_id                                      patient_id
  , DATE_PART('year', dc.billable_period_start::DATE) yr
FROM
    dpoc_patients dp
    JOIN dpoc_claims dc ON dc.patient = dp.bene_id
;

SELECT
    ah.*
  , pa.postal_code patient_zip
  , pa.county patient_county
FROM
    _all_pats ah
    LEFT JOIN fdw_member_doc.patient_addresses pa ON pa.patient_id = ah.patient_id
;