-- carrier_ip_op_pde_claims_20230525
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
    JOIN junk.dpc_request_20230525 j ON p.mbi = j.mbi
WHERE
        COALESCE(c.billable_period_start ::DATE, dcl.serviced_date ::DATE, dcl.service_period_start ::DATE) between
        '2019-01-01' and '2022-12-31'
  AND   c.eob_type IN ('CARRIER', 'INPATIENT', 'OUTPATIENT', 'PDE');

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



