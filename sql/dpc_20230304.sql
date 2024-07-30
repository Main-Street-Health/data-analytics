
------------------------------------------------------------------------------------------------------------------------
/* V2 for Charlie */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _infusion_procs;
CREATE TEMP TABLE _infusion_procs AS
SELECT *
FROM
    ( VALUES
          ('J9271'),
          ('J0897'),
          ('J9299'),
          ('J9144'),
          ('J0129'),
          ('J9312'),
          ('J9022'),
          ('J1300'),
          ('J2350'),
          ('J3380'),
          ('J2505'),
          ('J1745'),
          ('J0717'),
          ('J9173'),
          ('J9228'),
          ('J2357'),
          ('Q5107'),
          ('J9305'),
          ('Q5117'),
          ('J9035'),
          ('J0894'),
          ('J1454'),
          ('J9263'),
          ('J2469'),
          ('J9181'),
          ('J9201'),
          ('Q5103'),
          ('J2930'),
          ('J1569'),
          ('J1568'),
          ('J1439'),
          ('J0565'),
          ('J9041'),
          ('J0640'),
          ('J0490'),
          ('J2997'),
          ('J1644'),
          ('Q0138'),
          ('J1756'),
          ('J7999'),
          ('J1940'),
          ('C9142'),
          ('J9023'),
          ('J9355'),
          ('J9119'),
          ('Q5114'),
          ('Q5112'),
          ('J9316'),
          ('J9311'),
          ('Q5119'),
          ('Q5116'),
          ('Q5115'),
          ('Q5111'),
          ('Q5118'),
          ('J3262'),
          ('J0256'),
          ('Q5121'),
          ('J1786'),
          ('J2786'),
          ('J0584'),
          ('J1743'),
          ('J0180'),
          ('J0517'),
          ('J0223'),
          ('J0257'),
          ('J0638'),
          ('J3245'),
          ('J2507'),
          ('J0202'),
          ('J0221'),
          ('J2182'),
          ('J0485'),
          ('J0222'),
          ('Q5104'),
          ('J0491'),
          ('J1602'),
          ('J3357'),
          ('J3241'),
          ('J2356'),
          ('J3240'),
          ('J2323'),
          ('J1303'),
          ('J1823'),
          ('J3032'),
          ('C9257'),
          ('J0690'),
          ('J1270'),
          ('J2916'),
          ('Q4081'),
          ('Q5106'),
          ('J0887'),
          ('Q0243'),
          ('Q0245'),
          ('Q0239'),
          ('Q0240'),
          ('J2543'),
          ('J0585'),
          ('Q5120'),
          ('J0178'),
          ('J2778'),
          ('J7312'),
          ('J7318'),
          ('Q4206'),
          ('J7324'),
          ('J3304'),
          ('J7320'),
          ('J7327'),
          ('J3111'),
          ('J9217') ) x(cpt);



DROP TABLE IF EXISTS _inf_claims;
CREATE TEMP TABLE _inf_claims AS
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
    dpoc_claims c
    JOIN dpoc_patients p ON p.bene_id = c.patient
    JOIN dpoc_claim_lines dcl ON c.id = dcl.dpoc_claim_id
WHERE
        COALESCE(c.billable_period_start::date, dcl.service_period_start::date, dcl.serviced_date::date, NOW()::DATE) >= '2019-01-01'::DATE
  AND   EXISTS(SELECT
                   1
               FRM
                   dpoc_claim_lines cl2
                   JOIN _infusion_procs inf_proc ON inf_proc.cpt = cl2.procedure_code
               WHERE
                   cl2.dpoc_claim_id = c.id
            )
    ;

DROP TABLE IF EXISTS _all_mems;
CREATE TEMP TABLE _all_mems AS
SELECT DISTINCT
    mbi
FROM
    dpoc_claims c
    JOIN dpoc_patients p ON p.bene_id = c.patient
    JOIN dpoc_claim_lines dcl ON c.id = dcl.dpoc_claim_id
WHERE
        COALESCE(c.billable_period_start::DATE, dcl.service_period_start::DATE, dcl.serviced_date::DATE, NOW()::DATE) >= '2019-01-01'::DATE
;


SELECT count(*) FROM _all_mems; -- 62693
SELECT count(*) FROM _inf_claims; -- 880 458
SELECT count(distinct mbi) FROM _inf_claims; -- 10024

SELECT * FROM _all_mems;
SELECT * FROM _inf_claims;
