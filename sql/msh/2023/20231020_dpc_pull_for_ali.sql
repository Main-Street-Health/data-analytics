We were hoping to get your help with a DPC cut for some more infusion / specialty analysis we’re working on.
Would it be possible to pull full claims data
  for patients that have a claim with one of the attached procedure codes?
     E.g., if a patient goes on Keytruda (J9271), we’d want to see their prior claims.
     We were hoping to pull this for
    all OM patients that show up in the following zip codes:


;
DROP TABLE IF EXISTS _patients;
CREATE TEMP TABLE _patients AS 
SELECT
    distinct sp.patient_id
FROM
    fdw_member_doc.supreme_pizza sp
JOIN fdw_member_doc.patient_addresses pa on pa.patient_id = sp.patient_id
WHERE
      sp.is_om
  AND pa.postal_code in ('37732', '37733', '37755', '37756', '37762', '37819', '37841', '37847', '37852', '37891', '37892', '38504', '38556', '40769', '42631', '42635', '42638', '42647', '42649', '42653') ;

create UNIQUE INDEX  on _patients(patient_id);

DROP TABLE IF EXISTS _proc_code_hit;
CREATE TEMP TABLE _proc_code_hit AS
SELECT p.id dpoc_patient_id
FROM
    dpoc_claim_lines cl
join dpoc_claims c on cl.dpoc_claim_id = c.id
join dpoc_patients p on p.bene_id = c.patient
WHERE cl.procedure_code in ('J9271','J9299','J2506','J9022','J9173','J9145','J9306','J9305','Q5117','Q5107','J0881','J9355','J1439','J9228','J1300','J9035','J9264','J9354','J2353','Q5115','J2796','J9041','J9312','J9311','J1303','J1930','J1569','J0185','J9042','J9047','J9034','J9303','J9119','J9395','J9308','J9301','J1561','J1950','J9179','J9070','J9309','J9205','J9055','Q0138','J9176','J9043','Q5111','J1442','J0897','J0129','J2350','J3380','J1745','J0717','J2357','J1602','J3241','J3262','Q5119','J3357','J2182','J2323','J0517','J2507','J3245','J0256','J0490','J0222','J0485','Q5103','J0221','J0180','Q5104','J1786','J0584','J1823','J0257','J3032','J2786','J3240','J0223','Q5121','J1306','J2356','J2329','J0225','J2327','J1417','J0202','J0638','J0219','J0224','J0491','J1459','J1554','J1556','J1557','J1566','J1568','J1572','J2505','J8501','J8540','J8610','J9023','J9144','J9316','J9332','Q5112','Q5114','Q5116','Q5118','Q5127','J1437','J8999')
AND exists(select 1 from _patients pats where pats.patient_id = p.source_id)
;

create index on _proc_code_hit(dpoc_patient_id);


DROP TABLE IF EXISTS _output;
CREATE TEMP TABLE _output AS
SELECT
    p.mbi
  , p.source_id     coop_patient_id
  , c.id            internal_id
  , c.dpoc_id
  , c.ex_claim_type
  , c.eob_type
  , c.claim_id
  , c.primary_prvdr_npi
  , c.primary_prvdr_spclty
  , c.primary_prvdr_spclty_display
  , c.assisting_prvdr_npi
  , c.assisting_prvdr_spclty
  , c.assisting_prvdr_spclty_display
  , c.supervisor_prvdr_spclty
  , c.supervisor_prvdr_spclty_display
  , c.supervisor_prvdr_npi
  , c.other_prvdr_spclty
  , c.other_prvdr_spclty_display
  , c.other_prvdr_npi
  , c.organization_name
  , c.organization_npi
  , c.organization_ncpdp
  , c.organization_tax_id_number
  , c.facility_name
  , c.facility_npi
  , c.clm_fac_type_cd
  , c.clm_fac_type_display
  , c.phrmcy_name
  , c.phrmcy_npi
  , c.phrmcy_ncpdp
  , c.phrmcy_srvc_type_cd
  , c.phrmcy_srvc_type_display
  , c.billable_period_start
  , c.billable_period_end
  , c.drg
  , c.paid
  , c.coverage
  , c.disposition
  , c.clm_srvc_clsfctn_type_cd
  , c.revenue_code
  , c.pde_id
  , c.rx_srvc_rfrnc_num
  , 'claim<-->line' start_line_data
  , dcl.place_of_service
  , dcl.service_period_start
  , dcl.service_period_end
  , dcl.serviced_date
  , dcl.location_address_state
  , dcl.procedure_code
  , dcl.mod_1
  , dcl.mod_2
  , dcl.mod_3
  , dcl.mod_4
  , dcl.quantity
  , dcl.ndc
  , dcl.ndc_display_name
  , dcl.fill_num
  , dcl.days_supply_num
  , dcl.rev_cntr_ncvrd_chrg_amt
  , dcl.rev_cntr_tot_chrg_amt
  , dcl.rev_cntr_rate_amt
  , dcl.rev_cntr_bene_pmt_amt
  , dcl.rev_cntr_pmt_amt_amt
  , dcl.rev_cntr_prvdr_pmt_amt
  , dcl.carr_line_rdcd_pmt_phys_astn_c
  , dcl.line_bene_pmt_amt
  , dcl.line_prvdr_pmt_amt
  , dcl.line_bene_ptb_ddctbl_amt
  , dcl.line_bene_prmry_pyr_pd_amt
  , dcl.line_coinsrnc_amt
  , dcl.line_sbmtd_chrg_amt
  , dcl.line_alowd_chrg_amt
  , dcl.line_prcsg_ind_cd
  , dcl.line_pmt_80_100_cd
  , dcl.line_pmt_80_100_display
  , dcl.line_nch_pmt_amt
  , dcl.carr_line_rdcd_pmt_phys_astn_c_display
  , dcl.rev_cntr_blood_ddctbl_amt
  , dcl.drug_cvrg_stus_cd
  , dcl.cvrd_d_plan_pd_amt
  , dcl.tot_rx_cst_amt
  , dcl.line_dme_prchs_price_amt
  , dcl.gdc_blw_oopt_amt
  , dcl.lics_amt
  , dcl.plro_amt
  , dcl.rev_cntr_coinsrnc_wge_adjstd_c
  , dcl.rev_cntr_1st_msp_pd_amt
  , dcl.rev_cntr_ptnt_rspnsblty_pmt
  , dcl.gdc_abv_oopt_amt
  , dcl.ptnt_pay_amt
  , dcl.rev_cntr_cash_ddctbl_amt
  , dcl.line_prmry_alowd_chrg_amt
  , dcl.rev_cntr_2nd_msp_pd_amt
  , dcl.rev_cntr_1st_ansi_cd
  , dcl.othr_troop_amt
  , dcl.rptd_gap_dscnt_num
  , dcl.rev_cntr_rdcd_coinsrnc_amt
  , dcl.rev_cntr_stus_ind_cd
  , dcl.rev_cntr_stus_ind_display
FROM
    dpoc_patients p
    JOIN dpoc_claims c ON p.bene_id = c.patient
    JOIN dpoc_claim_lines dcl ON c.id = dcl.dpoc_claim_id
WHERE
      EXISTS( SELECT 1 FROM _proc_code_hit pats WHERE pats.dpoc_patient_id = p.id )
--   AND COALESCE(c.billable_period_start ::DATE, dcl.serviced_date ::DATE, dcl.service_period_start ::DATE) >=
--       '2021-01-01';

--   AND   c.eob_type IN ('CARRIER', 'OUTPATIENT');
-- Timeframe: 2021 & 2022
-- Claim types: Carrier & Outpatient only

-- | eob\_type |
-- | :--- |
-- | CARRIER |
-- | DME |
-- | HHA |
-- | HOSPICE |
-- | INPATIENT |
-- | OUTPATIENT |
-- | PDE |
-- | SNF |
-- SELECT distinct eob_type
-- FROM
--     dpoc_claims;
-- Carrier, Inpatient, Outpatient, and PDE

-- 20231020_dpc_pull_for_ali
SELECT *
FROM
    _output;




