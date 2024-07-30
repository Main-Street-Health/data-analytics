
-- 1/31, 2/7
SELECT m.measure_id, m.is_exclusion, vs.*
FROM
    ref.med_adherence_value_sets vs
join ref.med_adherence_measures m on m.value_set_id = vs.value_set_id
-- where measure_id ~* 'SPC'
;
    ;
SELECT * FROM ref.hedis_value_sets vs WHERE yr = 2023;


SELECT * FROM prd.claims;


SELECT
    distinct measure_name, qm.*
--     j.*, qm.code
FROM
    junk.qm_chase_2023 j
join fdw_member_doc.quality_measures qm on qm.id = j.measure_id
where j.measure_name ~* 'osteo'
;
17,SPC, spc
18,SPD, supd
50,SPD, spd
12,OMW


SELECT * FROM junk.qm_chase_2023 WHERE measure_id = 50 ; --med adh
SELECT * FROM junk.qm_chase_2023_icd_exclusions;

-- INSERT
-- INTO
--     junk.qm_chase_2023_icd_exclusions      (icd10_code_unformatted, measure_id, measure_key)
-- SELECT
--    i.code_unformatted, 17, 'spc'
-- FROM
--     analytics.junk.spc_excl_2024022 j
--      JOIN fdw_member_doc.icd10s i ON i.code_formatted = j.icd
-- ;

-- INSERT
-- INTO
--     junk.qm_chase_2023_icd_exclusions      (icd10_code_unformatted, measure_id, measure_key)
-- SELECT
--    i.code_unformatted, 18, 'supd'
-- FROM
--         junk.supd_excl_202402 j
--      JOIN fdw_member_doc.icd10s i ON i.code_formatted = j.icd
-- ;

--
-- -- Create claims tables for patients in qm chase file
--         -- DPC (claims + claimdx)
--         -- EMR (dx and procs in one table)
--         -- MCO (claims + claimdx)
--
--     drop table if exists _qm_pats;
--     create temp table _qm_pats as
--     select distinct patient_id from junk.qm_chase_2023;
--     create unique index udx__qm_pats on _qm_pats(patient_id);
--
-- --     drop table if exists junk.qm_chase_2023_dpc_claims;
--     select
--         qm.patient_id,
--         dc.patient,
--         dc.claim_id,
--         cl.line,
--         cl.procedure_code,
--         cl.service_period_end,
--         cl.mod_1,
--         cl.mod_2,
--         cl.mod_3,
--         cl.mod_4,
--         dc.primary_prvdr_npi,
--         dc.assisting_prvdr_npi,
--         dc.facility_npi,
--         dc.organization_npi
--     into junk.qm_chase_2023_dpc_claims
--     from
--         dpoc_claims dc
--         join dpoc_claim_lines cl on cl.dpoc_claim_id = dc.id
--         join dpoc_patients dp on dp.bene_id = dc.patient
--         join _qm_pats qm on qm.patient_id = dp.source_id
--     ;
--
-- --     drop table if exists junk.qm_chase_2023_dpc_dx;
--     select
--         qm.patient_id,
--         dc.claim_id,
--         dc.billable_period_end,
--         dx.type,
--         dx.code,
--         dx.sequence
--     into junk.qm_chase_2023_dpc_dx
--     from
--         dpoc_claims dc
--         join dpoc_claim_dx dx on dx.dpoc_claim_id = dc.id
--         join dpoc_patients dp on dp.bene_id = dc.patient
--         join _qm_pats qm on qm.patient_id = dp.source_id
--     ;
--
--     drop table if exists _rpos;
--     create temp table _rpos as
--     select id rpo_id, name from fdw_member_doc.msh_referring_partner_organizations;
--     create unique index udx__rpos on _rpos(rpo_id);
--
--     drop table if exists junk.qm_chase_2023_emr_claims;
--     select
--         qp.patient_id,
--         rpo.name rpo_name,
--         ec.encounter_date,
--         ec.icd10_code,
--         ec.procedure_code,
--         pxw.provider_npi,
--         pxw.provider_first_name,
--         pxw.provider_last_name
--     into junk.qm_chase_2023_emr_claims
--     from
--         integrations.emr_charges ec
--         join gmm.global_emr_patient_mappings epm on epm.patient_external_id = ec.patient_external_id
--         join gmm.global_members gm on gm.id = epm.global_member_id
--         join _qm_pats qp on qp.patient_id = gm.patient_id
--         left join integrations.provider_id_provider_name_xwalk pxw on pxw.id = ec.rendering_provider_xw_id
--         join cb.integration_points ip on ip.id = ec.integration_point_id
--         join _rpos rpo on rpo.rpo_id = ip.rpo_ids[1]
--     ;
--
--     drop table if exists junk.qm_chase_2023_mco_claims;
--     select
--         qp.patient_id,
--         gmmm.member_id,
--         gmmm.payer_id,
--         clm.billing_provider_npi,
--         clm.servicing_provider_npi,
--         clm.date_to encounter_date,
--         clm.place_of_service,
--         clm.procedure_code,
--         clm.procedure_mod1,
--         clm.procedure_mod2,
--         clm.procedure_mod3,
--         clm.procedure_mod4
--     into junk.qm_chase_2023_mco_claims
--     from
--         prd.claims clm
--         join gmm.global_mco_member_mappings gmmm on gmmm.member_id = clm.member_id
--         join gmm.global_members gm on gm.id = gmmm.global_member_id
--         join _qm_pats qp on qp.patient_id = gm.patient_id
--     ;
--
--     drop table if exists junk.qm_chase_2023_mco_claim_dx;
--     select
--         qp.patient_id,
--         gmmm.member_id,
--         gmmm.payer_id,
--         cd.claim_id,
--         cd.diag,
--         cd.diag_sequence
--     into junk.qm_chase_2023_mco_claim_dx
--     from
--         prd.claims clm
--         join gmm.global_mco_member_mappings gmmm on gmmm.member_id = clm.member_id
--         join gmm.global_members gm on gm.id = gmmm.global_member_id
--         join prd.claims_diagnosis cd on cd.claim_id = clm.id
--         join _qm_pats qp on qp.patient_id = gm.patient_id
--     ;

select * from junk.qm_chase_2023_icd_exclusions; WHERE icd10_code_unformatted = 'K7030';
select * from junk.qm_chase_2023_cpts;


select * from junk.qm_chase_2023_mco_claim_dx;
select * from junk.qm_chase_2023_mco_claims;
select * from junk.qm_chase_2023_emr_claims;
select * from junk.qm_chase_2023_dpc_dx;
select * from junk.qm_chase_2023_dpc_claims;
select * from junk.qm_chase_2023_icd_exclusions;

------------------------------------------------------------------------------------------------------------------------
/* SUPD */
------------------------------------------------------------------------------------------------------------------------
SELECT
    j.patient_id
  , j.measure_id
  , j.measure_name
  , c.rpo_name
  , c.encounter_date
  , c.icd10_code
  , c.procedure_code
  , c.provider_npi
  , c.provider_first_name
  , c.provider_last_name
  , 'excluded' hit
, 'emr_claims' src
FROM
    junk.qm_chase_2023 j
    JOIN junk.qm_chase_2023_emr_claims c ON j.patient_id = c.patient_id
        AND c.encounter_date BETWEEN '2022-01-01' AND '2023-12-31'
    JOIN junk.qm_chase_2023_icd_exclusions e ON e.measure_id = j.measure_id AND e.icd10_code_unformatted = c.icd10_code
WHERE
    j.measure_id = 18
;


-- not hits on mco
SELECT
    j.patient_id
  , j.measure_id
  , j.measure_name
  , c.encounter_date
  , 'excluded' hit
  , 'mco_claims' src
FROM
    junk.qm_chase_2023 j
    JOIN junk.qm_chase_2023_mco_claim_dx c ON j.patient_id = c.patient_id
        AND c.encounter_date BETWEEN '2022-01-01' AND '2023-12-31'
    JOIN junk.qm_chase_2023_icd_exclusions e ON e.measure_id = j.measure_id AND e.icd10_code_unformatted = c.diag
WHERE
    j.measure_id = 18
;
------------------------------------------------------------------------------------------------------------------------
/* SPC */
------------------------------------------------------------------------------------------------------------------------
SELECT
    j.patient_id
  , j.measure_id
  , j.measure_name
  , c.rpo_name
  , c.encounter_date
  , c.icd10_code
  , c.procedure_code
  , c.provider_npi
  , c.provider_first_name
  , c.provider_last_name
  , 'excluded' hit
, 'emr_claims' src
FROM
    junk.qm_chase_2023 j
    JOIN junk.qm_chase_2023_emr_claims c ON j.patient_id = c.patient_id
        AND c.encounter_date BETWEEN '2022-01-01' AND '2023-12-31'
    JOIN junk.qm_chase_2023_icd_exclusions e ON e.measure_id = j.measure_id AND e.icd10_code_unformatted = c.icd10_code
WHERE
    j.measure_id = 17
;


-- not hits on mco
SELECT
    j.patient_id
  , j.measure_id
  , j.measure_name
  , c.encounter_date
  , 'excluded' hit
  , 'mco_claims' src
FROM
    junk.qm_chase_2023 j
    JOIN junk.qm_chase_2023_mco_claim_dx c ON j.patient_id = c.patient_id
        AND c.encounter_date BETWEEN '2022-01-01' AND '2023-12-31'
    JOIN junk.qm_chase_2023_icd_exclusions e ON e.measure_id = j.measure_id AND e.icd10_code_unformatted = c.diag
WHERE
    j.measure_id = 17
;

SELECT
    j.patient_id
  , j.measure_id
  , j.measure_name
  , c.medication_list_name
  , pm.ndc
  , pm.drug_description
  , pm.start_date
  , pm.days_supply
  , pm.end_date
  , pm.quantity
  , 'statin_med'   hit
  , 'sure_scripts' src
FROM
    junk.qm_chase_2023 j
    JOIN prd.patient_medications pm
         ON pm.patient_id = j.patient_id AND pm.start_date BETWEEN '2023-01-01' AND '2023-12-31'
    JOIN ref.hedis_med_list_to_codes c ON c.code = pm.ndc and c.yr = 2023
WHERE
      j.measure_id = 17
  AND c.medication_list_name IN (
                                 'Simvastatin Moderate Intensity Medications',
                                 'Simvastatin High Intensity Medications',
                                 'Rosuvastatin Moderate Intensity Medications',
                                 'Rosuvastatin High Intensity Medications',
                                 'Pravastatin Moderate Intensity Medications',
                                 'Pitavastatin Moderate Intensity Medications',
                                 'Lovastatin Moderate Intensity Medications',
                                 'Fluvastatin Moderate Intensity Medications',
                                 'Ezetimibe Simvastatin Moderate Intensity Medications',
                                 'Ezetimibe Simvastatin High Intensity Medications',
                                 'Atorvastatin Moderate Intensity Medications',
                                 'Atorvastatin High Intensity Medications',
                                 'Amlodipine Atorvastatin Moderate Intensity Medications',
                                 'Amlodipine Atorvastatin High Intensity Medications');



------------------------------------------------------------------------------------------------------------------------
/* OMW 12 */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    ref.hedis_measure_to_med_list
WHERE
      measure_id = 'OMW'
  AND medication_list_name = 'Osteoporosis Medications'
    ;

SELECT
    j.patient_id
  , j.measure_id
  , j.measure_name
  , c.medication_list_name
  , pm.ndc
  , pm.drug_description
  , pm.start_date
  , pm.days_supply
  , pm.end_date
  , pm.quantity
  , 'osteo_med'   hit
  , 'sure_scripts' src
FROM
    junk.qm_chase_2023 j
    JOIN prd.patient_medications pm
         ON pm.patient_id = j.patient_id AND pm.start_date BETWEEN '2023-01-01' AND '2023-12-31'
    JOIN ref.hedis_med_list_to_codes c ON c.code = pm.ndc
WHERE
      j.measure_id = 12
  AND c.medication_list_name = 'Osteoporosis Medications'
;
SELECT *
FROM
    analytics.ref.hedis_value_sets
WHERE
      measure_id = 'OMW'
  AND value_set_name = 'Frailty Diagnosis'
and code_system = 'icd10cm'
;

SELECT * FROM ref.med_adherence_measure_names;