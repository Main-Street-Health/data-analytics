-- for https://github.com/Main-Street-Health/member-doc/issues/7579

DROP TABLE IF EXISTS _patients;
CREATE TEMP TABLE _patients AS
SELECT DISTINCT
    p.patient_id
  , p.mbi
  , dpat.id dpoc_patient_id
  , dpat.bene_id
FROM
    fdw_member_doc_ent.patients p
    LEFT JOIN dpoc_patients dpat ON dpat.mbi = p.mbi
    JOIN fdw_member_doc.patient_referring_partners prp
         ON prp.patient_id = p.patient_id AND prp."primary"
    JOIN fdw_member_doc.referring_partners rp ON prp.referring_partner_id = rp.id
    JOIN fdw_member_doc.msh_patient_risk_attributions attr ON attr.patient_id = p.patient_id
WHERE
      rp.id IN (
              275 -- CovenantCare - Hillcrest
            , 274 -- CovenantCare - Dover
            , 276 -- CovenantCare - Sango
            , 273 -- CovenantCare - Erin
            , 227 -- CCWV - Big Otter
            , 213 -- CCWV - Clarksburg
            , 225 -- CCWV - Clay
     );
-- part of retro, commented out below
--   AND attr.is_om
--   AND p.substatus <> 'deceased'
--   AND NOT EXISTS( SELECT
--                       1
--                   FROM
--                       fdw_member_doc.msh_patient_classifications mpc
--                   WHERE
--                           insurance_classification_id IN ('om_other', 'om_other_dno', 'om_other_outreach')
--                     AND   mpc.patient_id = p.patient_id );



CREATE UNIQUE INDEX ON _patients(patient_id);

-- remove patients without dpc coverage information
DELETE
FROM _patients p
where not exists(
    select 1 from prd.dpc_eligibility e
             where e.patient_id = p.patient_id
);
-- SELECT * FROM _patients;
-- SELECT count(*) FROM _patients;



DROP TABLE IF EXISTS _dpc_milliman_med_claims;
CREATE TEMP TABLE _dpc_milliman_med_claims AS
WITH
    claim_ids AS ( SELECT distinct
                       p2.patient_id,
                       c.id claim_id
                   FROM
                       _patients p2
                       JOIN dpoc_claims c ON c.patient = p2.bene_id
                                            and c.status = 'active'
                                            AND c.eob_type != 'PDE' )
  , drgs      AS ( SELECT
                       c.claim_id
                     , cdx.code
                   FROM
                       claim_ids c
                       JOIN dpoc_claim_dx cdx ON c.claim_id = cdx.dpoc_claim_id
                   WHERE
                       cdx.type = 'drg' )
  , dxes      AS ( SELECT
                       c.claim_id
                     , dx1.code  dx_1
                     , dx2.code  dx_2
                     , dx3.code  dx_3
                     , dx4.code  dx_4
                     , dx5.code  dx_5
                     , dx6.code  dx_6
                     , dx7.code  dx_7
                     , dx8.code  dx_8
                     , dx9.code  dx_9
                     , dx10.code dx_10
                     , dx11.code dx_11
                     , dx12.code dx_12
                     , dx13.code dx_13
                     , dx14.code dx_14
                     , dx15.code dx_15
                   FROM
                       claim_ids c
                       LEFT JOIN dpoc_claim_dx dx1 ON dx1.dpoc_claim_id = c.claim_id AND dx1.type = 'icd10' AND dx1.sequence = '1'
                       LEFT JOIN dpoc_claim_dx dx2 ON dx2.dpoc_claim_id = c.claim_id AND dx2.type = 'icd10' AND dx2.sequence = '2'
                       LEFT JOIN dpoc_claim_dx dx3 ON dx3.dpoc_claim_id = c.claim_id AND dx3.type = 'icd10' AND dx3.sequence = '3'
                       LEFT JOIN dpoc_claim_dx dx4 ON dx4.dpoc_claim_id = c.claim_id AND dx4.type = 'icd10' AND dx4.sequence = '4'
                       LEFT JOIN dpoc_claim_dx dx5 ON dx5.dpoc_claim_id = c.claim_id AND dx5.type = 'icd10' AND dx5.sequence = '5'
                       LEFT JOIN dpoc_claim_dx dx6 ON dx6.dpoc_claim_id = c.claim_id AND dx6.type = 'icd10' AND dx6.sequence = '6'
                       LEFT JOIN dpoc_claim_dx dx7 ON dx7.dpoc_claim_id = c.claim_id AND dx7.type = 'icd10' AND dx7.sequence = '7'
                       LEFT JOIN dpoc_claim_dx dx8 ON dx8.dpoc_claim_id = c.claim_id AND dx8.type = 'icd10' AND dx8.sequence = '8'
                       LEFT JOIN dpoc_claim_dx dx9 ON dx9.dpoc_claim_id = c.claim_id AND dx9.type = 'icd10' AND dx9.sequence = '9'
                       LEFT JOIN dpoc_claim_dx dx10 ON dx10.dpoc_claim_id = c.claim_id AND dx10.type = 'icd10' AND dx10.sequence = '10'
                       LEFT JOIN dpoc_claim_dx dx11 ON dx11.dpoc_claim_id = c.claim_id AND dx11.type = 'icd10' AND dx11.sequence = '11'
                       LEFT JOIN dpoc_claim_dx dx12 ON dx12.dpoc_claim_id = c.claim_id AND dx12.type = 'icd10' AND dx12.sequence = '12'
                       LEFT JOIN dpoc_claim_dx dx13 ON dx13.dpoc_claim_id = c.claim_id AND dx13.type = 'icd10' AND dx13.sequence = '13'
                       LEFT JOIN dpoc_claim_dx dx14 ON dx14.dpoc_claim_id = c.claim_id AND dx14.type = 'icd10' AND dx14.sequence = '14'
                       LEFT JOIN dpoc_claim_dx dx15 ON dx15.dpoc_claim_id = c.claim_id AND dx15.type = 'icd10' AND dx15.sequence = '15' )

SELECT
    c.claim_id                                                     "ClaimID"
  , cl.line                                                        "LineNum"
  , p.patient_id                                                   "MemberId"
  , c.coverage                                                     "PlanId"
  , coalesce(cl.service_period_start, c.billable_period_start)     "FromDate"
  , coalesce(cl.service_period_end, c.billable_period_end)         "ToDate"
  , NULL                                                           "AdmitDate"
  , NULL                                                           "DischDate"
  , drgs.code                                                      "DRG"
  , c.revenue_code                                                 "RevCode"
  , cl.procedure_code                                              "HCPCS"
  , cl.mod_1                                                       "Modifier"
  , cl.mod_2                                                       "Modifier2"
  , cl.place_of_service                                            "POS"
  , c.primary_prvdr_spclty                                         "srcSpecialty"
  , c.primary_prvdr_npi                                            "ServicingProviderID"
  , c.organization_npi                                             "BillingProviderID"
  , cl.line_sbmtd_chrg_amt                                         "Billed"
  , COALESCE(cl.line_prmry_alowd_chrg_amt, cl.line_alowd_chrg_amt) "Allowed"
  , cl.line_bene_prmry_pyr_pd_amt                                  "Paid"
  , cl.line_bene_ptb_ddctbl_amt                                    "Deductible"
  , cl.ptnt_pay_amt                                                "Copay"
  , cl.line_coinsrnc_amt                                           "Coinsurance"
  , NULL                                                           "Days"
  , NULL                                                           "Units"
  , NULL                                                           "DischargeStatus"
  , 1                                                              "ICDVersion"
  , dx.dx_1                                                        "ICDDiag1"
  , dx.dx_2                                                        "ICDDiag2"
  , dx.dx_3                                                        "ICDDiag3"
  , dx.dx_4                                                        "ICDDiag4"
  , dx.dx_5                                                        "ICDDiag5"
  , dx.dx_6                                                        "ICDDiag6"
  , dx.dx_7                                                        "ICDDiag7"
  , dx.dx_8                                                        "ICDDiag8"
  , dx.dx_9                                                        "ICDDiag9"
  , dx.dx_10                                                       "ICDDiag10"
  , dx.dx_11                                                       "ICDDiag11"
  , dx.dx_12                                                       "ICDDiag12"
  , dx.dx_13                                                       "ICDDiag13"
  , dx.dx_14                                                       "ICDDiag14"
  , dx.dx_15                                                       "ICDDiag15"
, null                                                             "ICDProc1"
, null                                                             "ICDProc2"
, null                                                             "ICDProc3"
, null                                                             "ICDProc4"
, null                                                             "ICDProc5"
, null                                                             "ICDProc6"
, null                                                             "ICDProc7"
, null                                                             "ICDProc8"
, null                                                             "ICDProc9"
, null                                                             "ICDProc10"
, null                                                             "ICDProc11"
, null                                                             "ICDProc12"
, null                                                             "ICDProc13"
, null                                                             "ICDProc14"
, null                                                             "ICDProc15"
  , 'P'                                                            "ClaimLineStatus"
FROM
    _patients p
    JOIN claim_ids ci on ci.patient_id = p.patient_id
    JOIN dpoc_claims c ON p.bene_id = c.patient and c.id = ci.claim_id
    LEFT JOIN dpoc_claim_lines cl ON c.id = cl.dpoc_claim_id
    LEFT JOIN drgs ON drgs.claim_id = c.id
    LEFT JOIN dxes dx ON c.id = dx.claim_id
; -- 4m20s
-- SELECT * FROM _dpc_milliman_med_claims;


DROP TABLE IF EXISTS _dpc_milliman_rx_claims;
CREATE TEMP TABLE _dpc_milliman_rx_claims AS
SELECT
    c.claim_group                                                                               "ClaimID"
  , p.patient_id                                                                                "MemberId"
  , COALESCE(cl.serviced_date::text, cl.service_period_start, c.billable_period_start)          "FromDate"
  , cl.ndc                                                                                      "NDC"
  , c.primary_prvdr_npi                                                                         "PrescriberID"
  , c.phrmcy_npi                                                                                "ProviderID"
  , cl.line_sbmtd_chrg_amt                                                                      "Billed"
  , COALESCE(cl.line_prmry_alowd_chrg_amt, cl.line_alowd_chrg_amt)                              "Allowed"
  , cl.line_bene_prmry_pyr_pd_amt                                                               "Paid"
  , COALESCE(cl.line_bene_ptb_ddctbl_amt, cl.rev_cntr_ptnt_rspnsblty_pmt, cl.line_bene_pmt_amt) "Deductible"
  , cl.ptnt_pay_amt                                                                             "Copay"
  , cl.line_coinsrnc_amt                                                                        "Coinsurance"
  , cl.days_supply_num                                                                          "DaysSupply"
  , cl.quantity                                                                                 "QuantityDispensed"
  , 'P'                                                                                         "ClaimLineStatus"
FROM
    _patients p
    JOIN dpoc_claims c ON p.bene_id = c.patient AND c.eob_type = 'PDE' AND c.status = 'active'
    LEFT JOIN dpoc_claim_lines cl ON c.id = cl.dpoc_claim_id
WHERE
    c.status = 'active';
;
INSERT
INTO
    _dpc_milliman_rx_claims ("ClaimID", "MemberId", "FromDate", "NDC", "PrescriberID", "ProviderID", "Billed",
                             "Allowed", "Paid", "Deductible", "Copay", "Coinsurance", "DaysSupply", "QuantityDispensed",
                             "ClaimLineStatus")
SELECT "ClaimID" , "MemberId" , "FromDate" , "NDC" , "PrescriberID" , "ProviderID" , "Billed" , "Allowed" , "Paid" , "Deductible" , "Copay" , "Coinsurance" , "DaysSupply" , "QuantityDispensed" , "ClaimLineStatus"
FROM
    ( SELECT
          h.id                                                      "ClaimID"
        , h.patient_id::bigint                                      "MemberId"
        , COALESCE(h.sold_date, h.last_filled_date, h.written_date) "FromDate"
        , h.product_code                                            "NDC"
        , h.prescriber_npi                                          "PrescriberID"
        , h.pharmacy_npi                                            "ProviderID"
        , NULL::NUMERIC                                             "Billed"
        , NULL::NUMERIC                                             "Allowed"
        , NULL::NUMERIC                                             "Paid"
        , NULL::NUMERIC                                             "Deductible"
        , NULL::NUMERIC                                             "Copay"
        , NULL::NUMERIC                                             "Coinsurance"
        , h.days_supply::integer                                    "DaysSupply"
        , h.quantity_prescribed::numeric                            "QuantityDispensed"
        , 'P'                                                       "ClaimLineStatus"
      FROM
          sure_scripts_med_history_details h
      JOIN _patients p on p.patient_id::text = h.patient_id
      WHERE
          h.sure_scripts_panel_id = 5281
      AND not exists (select 1 from _dpc_milliman_rx_claims rx where rx."MemberId"::text = h.patient_id)
      ) x
;


DROP TABLE IF EXISTS _eligibility;
CREATE TEMP TABLE _eligibility AS
SELECT DISTINCT ON (e.patient_id)
    e.patient_id
  , is_part_c
  , is_part_d
  , cost_share_code
  , dual_status_code
  , CASE
        WHEN is_part_d and part_d_contract_id != 'N' THEN CONCAT_WS('_', part_d_contract_id, part_d_pbp_id, segment_id)
        WHEN is_part_c and part_c_contract_id != 'N' THEN CONCAT_WS('_', part_c_contract_id, part_c_pbp_id)
        END plan_id
FROM
    prd.dpc_eligibility e
    JOIN _patients p ON e.patient_id = p.patient_id
where e.most_recent_coverage
order by e.patient_id, e.eom DESC
;
-- SELECT count(*), count(plan_id) FROM _eligibility;

DROP TABLE IF EXISTS _cost_share_lookup;
CREATE TEMP TABLE _cost_share_lookup AS
-- Value	Definition		PremiumPct	PartDCostSharingType
SELECT *
FROM
    ( VALUES
          ('00', 'Not Medicare enrolled for the month', NULL, NULL),
          ('01', 'Beneficiary enrolled in Parts A and/or B, and Part D; deemed eligible for LIS with 100% premium subsidy and no copayment',     '0', 'NoCostSharing'),
          ('02', 'Beneficiary enrolled in Parts A and/or B, and Part D; deemed eligible for LIS with 100% premium subsidy and low copayment',    '0', 'FBDE'),
          ('03', 'Beneficiary enrolled in Parts A and/or B, and Part D; deemed eligible for LIS with 100% premium subsidy and high copayment',   '0', 'NonFBDE'),
          ('04', 'Beneficiary enrolled in Parts A and/or B, and Part D; enrolled in LIS with 100% premium subsidy and high copayment',           '0', 'NonFBDE'),
          ('05', 'Beneficiary enrolled in Parts A and/or B, and Part D; enrolled in LIS with 100% premium subsidy and 15% copayment',            '0', 'FifteenPercentCostSharing'),
          ('06', 'Beneficiary enrolled in Parts A and/or B, and Part D; enrolled in LIS with 75% premium subsidy and 15% copayment',             '0', 'FifteenPercentCostSharing'),
          ('07', 'Beneficiary enrolled in Parts A and/or B, and Part D; enrolled in LIS with 50% premium subsidy and 15% copayment',             '0', 'FifteenPercentCostSharing'),
          ('08', 'Beneficiary enrolled in Parts A and/or B, and Part D; enrolled in LIS with 25% premium subsidy and 15% copayment',             '0', 'FifteenPercentCostSharing'),
          ('09', 'Beneficiary enrolled in Parts A and/or B, and Part D; no premium or cost sharing subsidy', NULL, NULL),
          ('10', 'Beneficiary enrolled in Parts A and/or B, but not Part D enrolled',                       NULL, NULL),
          ('13', 'Beneficiary enrolled in Parts A and/or B, but not Part D; employer receives RDS subsidy', NULL, NULL)
  ) x(cost_share_dpc_val, definition, premium_pct, part_d_cost_sharing_type);

DROP TABLE IF EXISTS _dual_status_lookup;
CREATE TEMP TABLE _dual_status_lookup AS
   SELECT * FROM (
     VALUES
       ('NA', 'Non-Medicaid',                                                                                              '0'),
       ('00', 'Not enrolled in Medicare for the month',                                                                    '0'),
       ('01', 'Qualified Medicare Beneficiary (QMB)-only',                                                                 '1'),
       ('02', 'QMB and full Medicaid coverage, including prescription drugs',                                              '1'),
       ('03', 'Specified Low-Income Medicare Beneficiary (SLMB)-only',                                                     '0'),
       ('04', 'SLMB and full Medicaid coverage, including prescription drugs',                                             '1'),
       ('05', 'Qualified Disabled Working Individual (QDWI)',                                                              '0'),
       ('06', 'Qualifying individuals (QI)',                                                                               '0'),
       ('08', 'Other dual eligible (not QMB, SLMB, QWDI, or QI) with full Medicaid coverage, including prescription Drugs','1'),
       ('09', 'Other dual eligible, but without Medicaid coverage',                                                        '0'),
       ('99', 'Unknown',                                                                                                   '0')
     ) x(dual_status_dpc_val, definition, medical_help);



DROP TABLE IF EXISTS _dpc_milliman_contacts;
CREATE TEMP TABLE _dpc_milliman_contacts AS
SELECT
    p.patient_id                                                           "MemberId"
  , NULL                                                                   "GroupNumber"
  , e.plan_id                                                              "PlanId"
  , dp.first_name                                                          "FirstName"
  , dp.last_name                                                           "LastName"
  , dp.dob                                                                 "DOB"
  , upper(left(dp.gender, 1))                                              "Gender"
  , pa.line1                                                               "Address1"
  , pa.line2                                                               "Address2"
  , pa.city                                                                "City"
  , pa.state                                                               "State"
  , pa.postal_code                                                         "Zip"
  , NULL                                                                   "FIPSCounty"
  , NULL                                                                   "EffectiveDate"
  , NULL                                                                   "TermDate"
  , NULL                                                                   "Smoker"
  , NULL                                                                   "PCP"
  , NULL                                                                   "LineOfBusiness"
  , NULL                                                                   "Premium"
  , csl.premium_pct                                                        "PremiumPct" --(numeric value or null, informed by cstshrXX
  , csl.part_d_cost_sharing_type                                           "PartDCostSharingType" -- (text, informed by cstshrXX)
  , CASE WHEN csl.premium_pct IS NOT NULL
    AND csl.part_d_cost_sharing_type IS NOT NULL THEN dsl.medical_help END "MedicalHelp" --(0 or 1, informed by dual_XX)
  , cost_share_code
  , dual_status_code
  , rp.id rpl_id
  , rp.name rpl_name
FROM
    _patients p
    JOIN fdw_member_doc.patient_addresses pa ON pa.patient_id = p.patient_id
    JOIN dpoc_patients dp ON dp.source_id = p.patient_id
    JOIN _eligibility e ON p.patient_id = e.patient_id
    LEFT JOIN _cost_share_lookup csl ON csl.cost_share_dpc_val = e.cost_share_code
    LEFT JOIN _dual_status_lookup dsl ON e.dual_status_code = dsl.dual_status_dpc_val
    JOIN fdw_member_doc.supreme_pizza sp on sp.patient_id = p.patient_id
    JOIN fdw_member_doc.referring_partners rp on rp.id = sp.primary_referring_partner_id
;

DROP TABLE IF EXISTS _jan;
CREATE TEMP TABLE _jan AS
SELECT
    dual_status_code
  , cost_share_code
  , CASE
        WHEN is_part_d AND part_d_contract_id != 'N' THEN CONCAT_WS('_', part_d_contract_id, part_d_pbp_id, segment_id)
        WHEN is_part_c AND part_c_contract_id != 'N' THEN CONCAT_WS('_', part_c_contract_id, part_c_pbp_id)
        END plan_id
, jan.patient_id
FROM
    analytics.prd.dpc_eligibility jan
WHERE
      bom = '2024-01-01'
  AND EXISTS( SELECT 1 FROM _patients p WHERE p.patient_id = jan.patient_id )
;

DROP TABLE IF EXISTS _dec;
CREATE TEMP TABLE _dec AS 
SELECT
    dual_status_code
  , cost_share_code
  , CASE
        WHEN is_part_d AND part_d_contract_id != 'N' THEN CONCAT_WS('_', part_d_contract_id, part_d_pbp_id, segment_id)
        WHEN is_part_c AND part_c_contract_id != 'N' THEN CONCAT_WS('_', part_c_contract_id, part_c_pbp_id)
        END plan_id
, dec.patient_id
FROM
    analytics.prd.dpc_eligibility dec
WHERE
      bom = '2023-12-01'
  AND EXISTS( SELECT 1 FROM _patients p WHERE p.patient_id = dec.patient_id );

SELECT
    c."MemberId"
  , c."GroupNumber"
  , c."PlanId"
  , c."FirstName"
  , c."LastName"
  , c."DOB"
  , c."Gender"
  , c."Address1"
  , c."Address2"
  , c."City"
  , c."State"
  , c."Zip"
  , c."FIPSCounty"
  , c."EffectiveDate"
  , c."TermDate"
  , c."Smoker"
  , c."PCP"
  , c."LineOfBusiness"
  , c."Premium"
  , c."PremiumPct"
  , c."PartDCostSharingType"
  , c."MedicalHelp"
  , c.cost_share_code
  , c.dual_status_code
  , c.rpl_id
  , c.rpl_name
  , d.dual_status_code dec_dual_status_code
  , d.cost_share_code  dec_cost_share_code
  , d.plan_id          dec_plan_id
  , j.dual_status_code jan_dual_status_code
  , j.cost_share_code  jan_cost_share_code
  , j.plan_id          jan_plan_id
FROM
    _dpc_milliman_contacts c
    LEFT JOIN _dec d ON d.patient_id = c."MemberId"
    LEFT JOIN _jan j ON j.patient_id = c."MemberId"
;








SELECT * FROM _dpc_milliman_med_claims;
SELECT * FROM _dpc_milliman_rx_claims ;


INSERT
INTO
    bk_up.dpc_milliman_contacts ("MemberId", "GroupNumber", "PlanId", "FirstName", "LastName", "DOB", "Gender",
                                 "Address1", "Address2", "City", "State", "Zip", "FIPSCounty", "EffectiveDate",
                                 "TermDate", "Smoker", "PCP", "LineOfBusiness", "Premium", "PremiumPct",
                                 "PartDCostSharingType", "MedicalHelp", cost_share_code, dual_status_code)
select
 "MemberId", "GroupNumber", "PlanId", "FirstName", "LastName", "DOB", "Gender",
 "Address1", "Address2", "City", "State", "Zip", "FIPSCounty", "EffectiveDate",
 "TermDate", "Smoker", "PCP", "LineOfBusiness", "Premium", "PremiumPct",
 "PartDCostSharingType", "MedicalHelp", cost_share_code, dual_status_code
from _dpc_milliman_contacts;

INSERT
INTO
    bk_up.dpc_milliman_med_claims ("ClaimID", "LineNum", "MemberId", "PlanId", "FromDate", "ToDate", "AdmitDate",
                                   "DischDate", "DRG", "RevCode", "HCPCS", "Modifier", "Modifier2", "POS",
                                   "srcSpecialty", "ServicingProviderID", "BillingProviderID", "Billed", "Allowed",
                                   "Paid", "Deductible", "Copay", "Coinsurance", "Days", "Units", "DischargeStatus",
                                   "ICDVersion", "ICDDiag1", "ICDDiag2", "ICDDiag3", "ICDDiag4", "ICDDiag5", "ICDDiag6",
                                   "ICDDiag7", "ICDDiag8", "ICDDiag9", "ICDDiag10", "ICDDiag11", "ICDDiag12",
                                   "ICDDiag13", "ICDDiag14", "ICDDiag15", "ICDProc1", "ICDProc2", "ICDProc3",
                                   "ICDProc4", "ICDProc5", "ICDProc6", "ICDProc7", "ICDProc8", "ICDProc9", "ICDProc10",
                                   "ICDProc11", "ICDProc12", "ICDProc13", "ICDProc14", "ICDProc15", "ClaimLineStatus")
select
"ClaimID", "LineNum", "MemberId", "PlanId", "FromDate", "ToDate", "AdmitDate",
"DischDate", "DRG", "RevCode", "HCPCS", "Modifier", "Modifier2", "POS",
"srcSpecialty", "ServicingProviderID", "BillingProviderID", "Billed", "Allowed",
"Paid", "Deductible", "Copay", "Coinsurance", "Days", "Units", "DischargeStatus",
"ICDVersion", "ICDDiag1", "ICDDiag2", "ICDDiag3", "ICDDiag4", "ICDDiag5", "ICDDiag6",
"ICDDiag7", "ICDDiag8", "ICDDiag9", "ICDDiag10", "ICDDiag11", "ICDDiag12",
"ICDDiag13", "ICDDiag14", "ICDDiag15", "ICDProc1", "ICDProc2", "ICDProc3",
"ICDProc4", "ICDProc5", "ICDProc6", "ICDProc7", "ICDProc8", "ICDProc9", "ICDProc10",
"ICDProc11", "ICDProc12", "ICDProc13", "ICDProc14", "ICDProc15", "ClaimLineStatus"
from _dpc_milliman_med_claims;

INSERT
INTO
    bk_up.dpc_milliman_rx_claims ("ClaimID", "MemberId", "FromDate", "NDC", "PrescriberID", "ProviderID", "Billed",
                                  "Allowed", "Paid", "Deductible", "Copay", "Coinsurance", "DaysSupply",
                                  "QuantityDispensed", "ClaimLineStatus")
select
"ClaimID", "MemberId", "FromDate", "NDC", "PrescriberID", "ProviderID", "Billed",
"Allowed", "Paid", "Deductible", "Copay", "Coinsurance", "DaysSupply",
"QuantityDispensed", "ClaimLineStatus"
from _dpc_milliman_rx_claims;

DROP TABLE IF EXISTS _dpc_milliman_med_claims
DROP TABLE IF EXISTS _dpc_milliman_contacts
DROP TABLE IF EXISTS _dpc_milliman_rx_claims

DROP TABLE IF EXISTS _dpc_milliman_rx_claims;
CREATE TEMP TABLE _dpc_milliman_rx_claims AS


SELECT count(distinct "MemberId")
FROM
    _dpc_milliman_rx_claims;
INSERT
INTO
    bk_up.dpc_milliman_rx_claims ("ClaimID", "MemberId", "FromDate", "NDC", "PrescriberID", "ProviderID", "Billed",
                                  "Allowed", "Paid", "Deductible", "Copay", "Coinsurance", "DaysSupply",
                                  "QuantityDispensed", "ClaimLineStatus")
SELECT
    "ClaimID"
  , "MemberId" ::bigint
  , "FromDate"
  , "NDC"
  , "PrescriberID"
  , "ProviderID"
  , "Billed"::numeric
  , "Allowed"::numeric
  , "Paid"::numeric
  , "Deductible"::numeric
  , "Copay"::numeric
  , "Coinsurance"::numeric
  , "DaysSupply"::int
  , "QuantityDispensed"::numeric
  , "ClaimLineStatus"
FROM
    _dpc_milliman_rx_claims;

------------------------------------------------------------------------------------------------------------------------
/* for the reporting */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _patients;
CREATE TEMP TABLE _patients AS
SELECT DISTINCT
    p.patient_id
  , p.mbi
  , dpat.id dpoc_patient_id
  , dpat.bene_id
, attr.is_om
, attr.is_ma
, rp.name
FROM
    fdw_member_doc_ent.patients p
    LEFT JOIN dpoc_patients dpat ON dpat.mbi = p.mbi
    JOIN fdw_member_doc.patient_referring_partners prp
         ON prp.patient_id = p.patient_id AND prp."primary"
    JOIN fdw_member_doc.referring_partners rp ON prp.referring_partner_id = rp.id
    JOIN fdw_member_doc.msh_patient_risk_attributions attr ON attr.patient_id = p.patient_id
WHERE
    -- removed burton creek rp 188 per leslie 20230914
      rp.id IN (
              275 -- CovenantCare - Hillcrest
            , 274 -- CovenantCare - Dover
            , 276 -- CovenantCare - Sango
            , 273 -- CovenantCare - Erin
            , 227 -- CCWV - Big Otter
            , 213 -- CCWV - Clarksburg
            , 225 -- CCWV - Clay
  )
  AND attr.is_om
  AND p.substatus <> 'deceased'
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      fdw_member_doc.msh_patient_classifications mpc
                  WHERE
                          insurance_classification_id IN ('om_other', 'om_other_dno', 'om_other_outreach')
                    AND   mpc.patient_id = p.patient_id );

SELECT p.name,
FROM
    _patients p
join
;