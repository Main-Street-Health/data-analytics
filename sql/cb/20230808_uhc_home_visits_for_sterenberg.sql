DROP TABLE IF EXISTS _house_calls;
CREATE TEMP TABLE _house_calls AS
SELECT mco_id, member_id, procedure_code, date_from
FROM
    cb.claims
WHERE
    billing_provider_npi = '1033737069'
or servicing_provider_npi = '1033737069'
;

SELECT *
FROM
    _house_calls;
SELECT *
FROM
    cb.claims where billing_provider_npi is not null;