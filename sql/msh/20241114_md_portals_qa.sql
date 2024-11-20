------------------------------------------------------------------------------------------------------------------------
/* mdportals */
------------------------------------------------------------------------------------------------------------------------
Patient ID 291098 - I20.9
nitro, NITROSTAT\n9/9/24 Upstate Medical University: NITROSTAT\n8/8/24 Upstate Medical University: nitro\n7/16/24 Upstate Medical University: NITROSTAT\n6/25/24 Upstate Medical University: nitro\n6/10/24 Upstate Medical University: nitro\n4/30/24 Trinity Health: NITROSTAT\n4/18/24 Upstate Medical University: nitro\n11/14/23 Trinity Health: NITROSTAT\n7/25/23 Upstate Medical University: nitro

nitro, NITROSTAT\n9/9/24 Upstate Medical University: NITROSTAT\n8/8/24 Upstate Medical University: nitro\n7/16/24 Upstate Medical University: NITROSTAT\n6/25/24 Upstate Medical University: nitro\n6/10/24 Upstate Medical University: nitro\n4/30/24 Trinity Health: NITROSTAT\n4/18/24 Upstate Medical University: nitro\n11/14/23 Trinity Health: NITROSTAT\n7/25/23 Upstate Medical University: nitro
SELECT *
FROM
    msh_external_emr_diagnoses xdx
    JOIN icd10s i ON xdx.icd10_id = i.id
WHERE
      patient_id = 291098
  AND i.code_formatted = 'I20.9'
-- and xdx.cms_contract_year = 2024
;

-- always distinct on source fact name
-- medications use the source_text if its not null
SELECT DISTINCT ON (LOWER(source_fact_name))
    source_fact_name, source_date, source_text
FROM
    stage.msh_md_portal_suspects_history
WHERE
      golgi_patient_id = 291098
  AND icd_10_code = 'I20.9'
ORDER BY
    LOWER(source_fact_name), source_date DESC;


-- and xdx.cms_contract_year = 2024
;


SELECT *
FROM
    member_doc.stage.msh_md_portal_suspects_history;

| source\_fact\_name |
| :--- |
| Atrial fibrillation |
| Warfarin Sodium |
| BMI |
| Clotrimazole-Betamethasone |
| Diabetes type II |
| montelukast |
| Diabetes |
| BMI |
| Congestive heart failure |
| risperiDONE |
| Lipitor |
| Clotrimazole-Betamethasone |
| COPD |
