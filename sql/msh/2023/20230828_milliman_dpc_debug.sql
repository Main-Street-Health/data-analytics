DROP TABLE IF EXISTS _patients;
CREATE TEMP TABLE _patients AS
SELECT DISTINCT
    p.patient_id
  , p.mbi
  , dpat.id dpoc_patient_id
  , dpat.bene_id
  , p.patient_first_name
  , p.patient_last_name
  , p.date_of_birth
  , rp.id   rp_id
  , rp.name
FROM
    fdw_member_doc_ent.patients p
    LEFT JOIN dpoc_patients dpat ON dpat.mbi = p.mbi
    JOIN fdw_member_doc.patient_referring_partners prp
         ON prp.patient_id = p.patient_id AND prp."primary"
    JOIN fdw_member_doc.referring_partners rp ON prp.referring_partner_id = rp.id
    JOIN fdw_member_doc.msh_patient_risk_attributions attr ON attr.patient_id = p.patient_id
WHERE
      rp.id IN (276, 273, 274, 275, 188, 225, 227)
  AND attr.is_om
  AND p.substatus <> 'deceased'
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      fdw_member_doc.msh_patient_classifications mpc
                  WHERE
                          insurance_classification_id IN ('om_other', 'om_other_dno', 'om_other_outreach')
                    AND   mpc.patient_id = p.patient_id );

CREATE UNIQUE INDEX ON _patients(patient_id);

select p.*, e.*
FROM
    prd.dpc_eligibility e
    JOIN _patients p ON e.patient_id = p.patient_id
where e.most_recent_coverage
order by e.patient_id, e.eom DESC

SELECT
    de.is_dual
  , de.bom
  , dc.id
  , dc.dpoc_id
  , dc.patient
  , dc.period_end
  , dc.period_start
  , dc.status
  , dc.sub_group
  , dc.sub_plan
  , dc.a_trm_cd
  , dc.b_trm_cd
  , dc.crec
  , dc.esrd_ind
  , dc.ms_cd
  , dc.orec
  , dc.rfrnc_yr
  , dc.type_code
  , dc.type_system
  , dc.dual_01
  , dc.dual_02
  , dc.dual_03
  , dc.dual_04
  , dc.dual_05
  , dc.dual_06
  , dc.dual_07
  , dc.dual_08
  , dc.dual_09
  , dc.dual_10
  , dc.dual_11
  , dc.dual_12
  , dc.ptc_cntrct_id_01
  , dc.ptc_cntrct_id_02
  , dc.ptc_cntrct_id_03
  , dc.ptc_cntrct_id_04
  , dc.ptc_cntrct_id_05
  , dc.ptc_cntrct_id_06
  , dc.ptc_cntrct_id_07
  , dc.ptc_cntrct_id_08
  , dc.ptc_cntrct_id_09
  , dc.ptc_cntrct_id_10
  , dc.ptc_cntrct_id_11
  , dc.ptc_cntrct_id_12
FROM
    dpoc_patients dp
    JOIN dpoc_coverage dc ON dc.patient = dp.bene_id
    JOIN prd.dpc_eligibility de ON de.patient_id = dp.source_id AND de.most_recent_coverage
WHERE
      dp.source_id IN (193535, 193586, 193603, 193621, 193628)
  AND dc.sub_plan = 'Part C'
ORDER BY
    dp.source_id
;

SELECT DISTINCT
--     e.is_dual
--   , e.part_c_contract_id
--   , e.dual_status_code
dp.source_id patient_id, dp.first_name, dp.last_name, dp.dob, dp.dod
  , dc.dual_01
  , dc.dual_02
  , dc.dual_03
  , dc.dual_04
  , dc.dual_05
  , dc.dual_06
  , dc.dual_07
  , dc.dual_08
  , dc.dual_09
  , dc.updated_at ::date
FROM
    dpoc_patients dp
    JOIN dpoc_coverage dc ON dc.patient = dp.bene_id
    JOIN prd.dpc_eligibility e ON e.patient_id = dp.source_id AND e.most_recent_coverage
WHERE
--       dual_08 = 'NA'
--   AND dual_07 != 'NA'
--   AND ptc_cntrct_id_08 != '0'
-- and
dp.source_id IN
(104300, 96905, 85031, 87150, 85506, 86562, 84707, 91044, 90986, 103532, 102252, 195343, 195178, 193676, 196712, 319736,
 343383)


;

drop table if exists _dual_status;
CREATE TEMP TABLE _dual_status AS
WITH
    cte   AS ( SELECT patient_id, bom FROM prd.dpc_eligibility WHERE most_recent_coverage )
  , codes AS ( SELECT
                   de.patient_id
                 , MAX(dual_status_code)
                   FILTER ( WHERE dual_status_code <> 'NA' AND is_populated_month ) most_recent_valid_dual_l3m
                 , MAX(dual_status_code) FILTER ( WHERE most_recent_coverage )      current_dual
               FROM
                   prd.dpc_eligibility de
                   JOIN cte ON cte.patient_id = de.patient_id
               WHERE
                   de.bom BETWEEN cte.bom - INTERVAL '3 months' AND cte.bom
               GROUP BY 1 )
SELECT *
     , CASE WHEN current_dual <> 'NA' THEN current_dual
            ELSE COALESCE(most_recent_valid_dual_l3m, current_dual) END current_dual_status_code
FROM
    codes
    ;

select * from _dual_status;



------------------------------------------------------------------------------------------------------------------------
/* OLD below */
------------------------------------------------------------------------------------------------------------------------


-- remove patients without dpc coverage information
DELETE
FROM _patients p
where not exists(
    select 1 from prd.dpc_eligibility e
             where e.patient_id = p.patient_id
);
-- SELECT * FROM _patients;



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
  , p.patient_id                                                                                   "MemberId"
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
-- null_plan_ids_20230306
-- SELECT
--     dp.mbi
--   , dc.id, dc.dpoc_id, dc.patient, dc.period_end, dc.period_start, dc.status, dc.sub_group, dc.sub_plan, dc.a_trm_cd, dc.b_trm_cd, dc.crec, dc.esrd_ind, dc.ms_cd, dc.orec, dc.rfrnc_yr, dc.type_code, dc.type_system, dc.buyin01, dc.buyin02, dc.buyin03, dc.buyin04, dc.buyin05, dc.buyin06, dc.buyin07, dc.buyin08, dc.buyin09, dc.buyin10, dc.buyin11, dc.buyin12, dc.dual_01, dc.dual_02, dc.dual_03, dc.dual_04, dc.dual_05, dc.dual_06, dc.dual_07, dc.dual_08, dc.dual_09, dc.dual_10, dc.dual_11, dc.dual_12, dc.ptc_cntrct_id_01, dc.ptc_cntrct_id_02, dc.ptc_cntrct_id_03, dc.ptc_cntrct_id_04, dc.ptc_cntrct_id_05, dc.ptc_cntrct_id_06, dc.ptc_cntrct_id_07, dc.ptc_cntrct_id_08, dc.ptc_cntrct_id_09, dc.ptc_cntrct_id_10, dc.ptc_cntrct_id_11, dc.ptc_cntrct_id_12, dc.ptc_pbp_id_01, dc.ptc_pbp_id_02, dc.ptc_pbp_id_03, dc.ptc_pbp_id_04, dc.ptc_pbp_id_05, dc.ptc_pbp_id_06, dc.ptc_pbp_id_07, dc.ptc_pbp_id_08, dc.ptc_pbp_id_09, dc.ptc_pbp_id_10, dc.ptc_pbp_id_11, dc.ptc_pbp_id_12, dc.ptc_plan_type_cd_01, dc.ptc_plan_type_cd_02, dc.ptc_plan_type_cd_03, dc.ptc_plan_type_cd_04, dc.ptc_plan_type_cd_05, dc.ptc_plan_type_cd_06, dc.ptc_plan_type_cd_07, dc.ptc_plan_type_cd_08, dc.ptc_plan_type_cd_09, dc.ptc_plan_type_cd_10, dc.ptc_plan_type_cd_11, dc.ptc_plan_type_cd_12, dc.hmo_ind_01, dc.hmo_ind_02, dc.hmo_ind_03, dc.hmo_ind_04, dc.hmo_ind_05, dc.hmo_ind_06, dc.hmo_ind_07, dc.hmo_ind_08, dc.hmo_ind_09, dc.hmo_ind_10, dc.hmo_ind_11, dc.hmo_ind_12, dc.ptdcntrct01, dc.ptdcntrct02, dc.ptdcntrct03, dc.ptdcntrct04, dc.ptdcntrct05, dc.ptdcntrct06, dc.ptdcntrct07, dc.ptdcntrct08, dc.ptdcntrct09, dc.ptdcntrct10, dc.ptdcntrct11, dc.ptdcntrct12, dc.ptdpbpid01, dc.ptdpbpid02, dc.ptdpbpid03, dc.ptdpbpid04, dc.ptdpbpid05, dc.ptdpbpid06, dc.ptdpbpid07, dc.ptdpbpid08, dc.ptdpbpid09, dc.ptdpbpid10, dc.ptdpbpid11, dc.ptdpbpid12, dc.sgmtid01, dc.sgmtid02, dc.sgmtid03, dc.sgmtid04, dc.sgmtid05, dc.sgmtid06, dc.sgmtid07, dc.sgmtid08, dc.sgmtid09, dc.sgmtid10, dc.sgmtid11, dc.sgmtid12, dc.cstshr01, dc.cstshr02, dc.cstshr03, dc.cstshr04, dc.cstshr05, dc.cstshr06, dc.cstshr07, dc.cstshr08, dc.cstshr09, dc.cstshr10, dc.cstshr11, dc.cstshr12, dc.rdsind01, dc.rdsind02, dc.rdsind03, dc.rdsind04, dc.rdsind05, dc.rdsind06, dc.rdsind07, dc.rdsind08, dc.rdsind09, dc.rdsind10, dc.rdsind11, dc.rdsind12, dc.dpoc_last_updated
-- FROM
--     _eligibility e
--     JOIN dpoc_patients dp ON e.patient_id = dp.source_id
--     JOIN dpoc_coverage dc ON dc.patient = dp.bene_id
-- WHERE
--     plan_id ISNULL
-- ORDER BY
--     dc.patient, dc.type_code;




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
          ('06', 'Beneficiary enrolled in Parts A and/or B, and Part D; enrolled in LIS with 75% premium subsidy and 15% copayment',             '0.25', 'FifteenPercentCostSharing'),
          ('07', 'Beneficiary enrolled in Parts A and/or B, and Part D; enrolled in LIS with 50% premium subsidy and 15% copayment',             '0.50', 'FifteenPercentCostSharing'),
          ('08', 'Beneficiary enrolled in Parts A and/or B, and Part D; enrolled in LIS with 25% premium subsidy and 15% copayment',             '0.75', 'FifteenPercentCostSharing'),
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
FROM
    _patients p
    JOIN fdw_member_doc.patient_addresses pa ON pa.patient_id = p.patient_id
    JOIN dpoc_patients dp ON dp.source_id = p.patient_id
    JOIN _eligibility e ON p.patient_id = e.patient_id
    LEFT JOIN _cost_share_lookup csl ON csl.cost_share_dpc_val = e.cost_share_code
    LEFT JOIN _dual_status_lookup dsl ON e.dual_status_code = dsl.dual_status_dpc_val
;

SELECT * FROM _dpc_milliman_contacts ;
SELECT * FROM _dpc_milliman_med_claims;
SELECT * FROM _dpc_milliman_rx_claims ;

    ;
create table junk.dpc_mil_contacts as select * from _dpc_milliman_contacts limit 0;
create table junk.dpc_mil_med_claims as select * from _dpc_milliman_med_claims limit 0 ;
create table junk.dpc_mil_rx_claims as select * from _dpc_milliman_rx_claims limit 0 ;

drop table junk.dpc_mil_contacts;
drop table junk.dpc_mil_med_claims;
drop table junk.dpc_mil_rx_claims;

CREATE TABLE bk_up.dpc_milliman_contacts (
    id bigserial PRIMARY KEY ,
    pulled_at timestamp not null default now(),
    "MemberId"             BIGINT,
    "GroupNumber"          TEXT,
    "PlanId"               TEXT,
    "FirstName"            TEXT,
    "LastName"             TEXT,
    "DOB"                  DATE,
    "Gender"               TEXT,
    "Address1"             TEXT,
    "Address2"             TEXT,
    "City"                 TEXT,
    "State"                TEXT,
    "Zip"                  TEXT,
    "FIPSCounty"           TEXT,
    "EffectiveDate"        TEXT,
    "TermDate"             TEXT,
    "Smoker"               TEXT,
    "PCP"                  TEXT,
    "LineOfBusiness"       TEXT,
    "Premium"              TEXT,
    "PremiumPct"           TEXT,
    "PartDCostSharingType" TEXT,
    "MedicalHelp"          TEXT,
    cost_share_code        TEXT,
    dual_status_code       TEXT
);

CREATE TABLE bk_up.dpc_milliman_med_claims (
    id bigserial PRIMARY KEY ,
    pulled_at timestamp not null default now(),
    "ClaimID"             TEXT,
    "LineNum"             INTEGER,
    "MemberId"            BIGINT,
    "PlanId"              TEXT,
    "FromDate"            VARCHAR,
    "ToDate"              VARCHAR,
    "AdmitDate"           TEXT,
    "DischDate"           TEXT,
    "DRG"                 TEXT,
    "RevCode"             TEXT,
    "HCPCS"               VARCHAR(255),
    "Modifier"            TEXT,
    "Modifier2"           TEXT,
    "POS"                 VARCHAR(255),
    "srcSpecialty"        TEXT,
    "ServicingProviderID" TEXT,
    "BillingProviderID"   TEXT,
    "Billed"              NUMERIC(16, 2),
    "Allowed"             NUMERIC(16, 2),
    "Paid"                NUMERIC(16, 2),
    "Deductible"          NUMERIC(16, 2),
    "Copay"               NUMERIC(16, 2),
    "Coinsurance"         NUMERIC(16, 2),
    "Days"                TEXT,
    "Units"               TEXT,
    "DischargeStatus"     TEXT,
    "ICDVersion"          INTEGER,
    "ICDDiag1"            TEXT,
    "ICDDiag2"            TEXT,
    "ICDDiag3"            TEXT,
    "ICDDiag4"            TEXT,
    "ICDDiag5"            TEXT,
    "ICDDiag6"            TEXT,
    "ICDDiag7"            TEXT,
    "ICDDiag8"            TEXT,
    "ICDDiag9"            TEXT,
    "ICDDiag10"           TEXT,
    "ICDDiag11"           TEXT,
    "ICDDiag12"           TEXT,
    "ICDDiag13"           TEXT,
    "ICDDiag14"           TEXT,
    "ICDDiag15"           TEXT,
    "ICDProc1"            TEXT,
    "ICDProc2"            TEXT,
    "ICDProc3"            TEXT,
    "ICDProc4"            TEXT,
    "ICDProc5"            TEXT,
    "ICDProc6"            TEXT,
    "ICDProc7"            TEXT,
    "ICDProc8"            TEXT,
    "ICDProc9"            TEXT,
    "ICDProc10"           TEXT,
    "ICDProc11"           TEXT,
    "ICDProc12"           TEXT,
    "ICDProc13"           TEXT,
    "ICDProc14"           TEXT,
    "ICDProc15"           TEXT,
    "ClaimLineStatus"     TEXT
);

CREATE TABLE bk_up.dpc_milliman_rx_claims (
    id bigserial PRIMARY KEY ,
    pulled_at timestamp not null default now(),
    "ClaimID"           TEXT,
    "MemberId"          BIGINT,
    "FromDate"          TEXT,
    "NDC"               TEXT,
    "PrescriberID"      TEXT,
    "ProviderID"        TEXT,
    "Billed"            NUMERIC(16, 2),
    "Allowed"           NUMERIC(16, 2),
    "Paid"              NUMERIC(16, 2),
    "Deductible"        NUMERIC(16, 2),
    "Copay"             NUMERIC(16, 2),
    "Coinsurance"       NUMERIC(16, 2),
    "DaysSupply"        INTEGER,
    "QuantityDispensed" NUMERIC,
    "ClaimLineStatus"   TEXT
);

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
