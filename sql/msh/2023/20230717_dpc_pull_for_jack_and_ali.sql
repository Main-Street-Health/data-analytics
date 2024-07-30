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
        ('J1303','J1930','J1569','J0185','J9042','J9047','J9034','J9303','J9119','J9395','J9308','J9301','J1561','J1950','J9179','J9070','J9309','J9205','J9055','Q0138','J9176','J9043','Q5111','J1442','J0129','J2350','J3380','J1745','J0717','J2357','J1602','J3241','J3262','Q5119','J3357','Q5103','Q5104','Q5121','J3358')


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