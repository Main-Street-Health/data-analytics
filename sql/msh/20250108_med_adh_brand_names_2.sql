
SELECT * FROM junk.still_need_brand_names_20250108;

CREATE index on junk.still_need_brand_names_20250108(code);


WITH
    duped AS ( SELECT
                   code
                 , COUNT(1)
               FROM
                   junk.still_need_brand_names_20250108
               WHERE
                   brand_name IS NOT NULL
               GROUP BY code
               HAVING
                   COUNT(*) > 1 )
SELECT
    j.*
FROM
    duped d
    JOIN junk.still_need_brand_names_20250108 j
         ON j.code = d.code
;
-- no dupes do delete out


-- fix missing 0s issue
SELECT
    measure_key, code
  , brand_name
  , LPAD(code, 11, '0')
  , length(LPAD(code, 11, '0'))
FROM
    junk.still_need_brand_names_20250108
WHERE
      brand_name IS NOT NULL
  AND LENGTH(code) != 11;


UPDATE
    junk.still_need_brand_names_20250108
SET
    code = LPAD(code, 11, '0')
WHERE
      brand_name IS NOT NULL
  AND LENGTH(code) != 11;



SELECT DISTINCT ON ( drug_description )
    drug_description
  , ndc
FROM
    patient_medication_fills pm
    JOIN junk.still_need_brand_names_20250108 j ON j.code = pm.ndc AND j.brand_name IS NOT NULL
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
  , LOWER(TRIM(string_to_table(brand_name, ',')))
FROM
    junk.still_need_brand_names_20250108
WHERE
    brand_name IS NOT NULL
ON CONFLICT DO NOTHING


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


