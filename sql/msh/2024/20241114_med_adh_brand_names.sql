SELECT *
FROM
    junk.med_adh_brand_names_20241114
;
CREATE index on junk.med_adh_brand_names_20241114(code);


WITH
    duped AS ( SELECT
                   code
                 , COUNT(1)
               FROM
                   junk.med_adh_brand_names_20241114
               WHERE
                   "Brand Name" IS NOT NULL
               GROUP BY code
               HAVING
                   COUNT(*) > 1 )
SELECT
    j.*
FROM
    duped d
    JOIN junk.med_adh_brand_names_20241114 j
         ON j.code = d.code
;
-- delete out one of the dupes
DELETE
FROM
    junk.med_adh_brand_names_20241114
WHERE
      measure_id LIKE 'PDC-DR'
  AND value_set_id LIKE 'BIGUANIDES'
  AND code LIKE '50458054291'
  AND description LIKE 'CANAGLIFLOZIN 150 MG / METFORMIN 500 MG TABLET, FILM COATED [INVOKAMET]'
  AND "Brand Name" LIKE 'INVOKAMET';


-- fix missing 0s issue
SELECT
    measure_id
  , value_set_id
  , description
  , code
  , "Brand Name"
  , LPAD(code, 11, '0')
  , length(LPAD(code, 11, '0'))
FROM
    junk.med_adh_brand_names_20241114
WHERE
      "Brand Name" IS NOT NULL
  AND LENGTH(code) != 11;


UPDATE
    junk.med_adh_brand_names_20241114
SET
    code = LPAD(code, 11, '0')
WHERE
      "Brand Name" IS NOT NULL
  AND LENGTH(code) != 11;



-- order by length(code)


SELECT distinct code, lower(trim(string_to_table("Brand Name",',')))
FROM
    junk.med_adh_brand_names_20241114
WHERE
    "Brand Name" IS NOT NULL
  
and description ~* 'metformin'
;
SELECT *
FROM
    junk.med_adh_brand_names_20241114 j
WHERE
    description ~* 'lisino'
and "Brand Name" is not null


SELECT DISTINCT ON ( drug_description )
    drug_description
  , ndc
FROM
    patient_medication_fills pm
    JOIN junk.med_adh_brand_names_20241114 j ON j.code = pm.ndc AND j."Brand Name" IS NOT NULL
WHERE
    drug_description IN (
                         'METFORMIN 500MG TAB',
                         'ATORVASTATIN 40MG TAB',
                         'LISINOPRIL 20MG TAB'
        )
ORDER BY
    drug_description;


INSERT
INTO
    qm_pm_med_adh_brand_name_drugs (ndc, brand_name)
SELECT DISTINCT
    code
  , LOWER(TRIM(string_to_table("Brand Name", ',')))
FROM
    junk.med_adh_brand_names_20241114
WHERE
    "Brand Name" IS NOT NULL


;

DROP TABLE IF EXISTS _still_need_brand_names;
CREATE TEMP TABLE _still_need_brand_names AS
SELECT DISTINCT ON (pm.ndc)
    pm.ndc
  , pm.drug_description
  , pm.measure_key
FROM
    patient_medication_fills pm
WHERE
      pm.measure_key IS NOT NULL
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      qm_pm_med_adh_brand_name_drugs bn
                  WHERE
                      bn.ndc = pm.ndc )

;
SELECT *
FROM
    _still_need_brand_names ;


