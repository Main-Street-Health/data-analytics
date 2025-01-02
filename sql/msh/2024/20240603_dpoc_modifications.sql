SELECT *
FROM
    dpoc_claims c
WHERE
    eob_type = 'INPATIENT'
-- and id = 271010573
and drg ISNULL
LIMIT 10 ;

SELECT * FROM dpoc_claim_lines WHERE dpoc_claim_id = 271010573;
-- INSERT INTO public.dpoc_claim_lines (id, line, line_cms_type_srvc_cd, place_of_service, procedure_code, quantity, service_period_end, service_period_start, dpoc_claim_id, inserted_at, updated_at, ndc, ndc_display_name, fill_num, days_supply_num, rev_cntr_ncvrd_chrg_amt, rev_cntr_tot_chrg_amt, rev_cntr_rate_amt, rev_cntr_bene_pmt_amt, rev_cntr_pmt_amt_amt, rev_cntr_prvdr_pmt_amt, carr_line_rdcd_pmt_phys_astn_c, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt, line_sbmtd_chrg_amt, line_alowd_chrg_amt, line_prcsg_ind_cd, line_pmt_80_100_cd, line_pmt_80_100_display, line_nch_pmt_amt, carr_line_rdcd_pmt_phys_astn_c_display, rev_cntr_blood_ddctbl_amt, drug_cvrg_stus_cd, cvrd_d_plan_pd_amt, tot_rx_cst_amt, line_dme_prchs_price_amt, gdc_blw_oopt_amt, lics_amt, plro_amt, rev_cntr_coinsrnc_wge_adjstd_c, rev_cntr_1st_msp_pd_amt, rev_cntr_ptnt_rspnsblty_pmt, gdc_abv_oopt_amt, ptnt_pay_amt, rev_cntr_cash_ddctbl_amt, line_prmry_alowd_chrg_amt, rev_cntr_2nd_msp_pd_amt, othr_troop_amt, rptd_gap_dscnt_num, rev_cntr_rdcd_coinsrnc_amt, line_place_of_srvc_display, serviced_date, location_address_state, rev_cntr_stus_ind_cd, rev_cntr_stus_ind_display, mod_1, mod_2, mod_3, mod_4, rev_cntr_2nd_ansi_cd, ncvrd_plan_pd_amt, rev_cntr_1st_ansi_cd, rev_cntr_3rd_ansi_cd) VALUES (435858250, 1, null, null, 'A0603', 1, null, null, 271010573, '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327', null, null, null, null, 0, 0, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'AR', null, null, null, null, null, null, null, null, null, null);
-- INSERT INTO public.dpoc_claim_lines (id, line, line_cms_type_srvc_cd, place_of_service, procedure_code, quantity, service_period_end, service_period_start, dpoc_claim_id, inserted_at, updated_at, ndc, ndc_display_name, fill_num, days_supply_num, rev_cntr_ncvrd_chrg_amt, rev_cntr_tot_chrg_amt, rev_cntr_rate_amt, rev_cntr_bene_pmt_amt, rev_cntr_pmt_amt_amt, rev_cntr_prvdr_pmt_amt, carr_line_rdcd_pmt_phys_astn_c, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt, line_sbmtd_chrg_amt, line_alowd_chrg_amt, line_prcsg_ind_cd, line_pmt_80_100_cd, line_pmt_80_100_display, line_nch_pmt_amt, carr_line_rdcd_pmt_phys_astn_c_display, rev_cntr_blood_ddctbl_amt, drug_cvrg_stus_cd, cvrd_d_plan_pd_amt, tot_rx_cst_amt, line_dme_prchs_price_amt, gdc_blw_oopt_amt, lics_amt, plro_amt, rev_cntr_coinsrnc_wge_adjstd_c, rev_cntr_1st_msp_pd_amt, rev_cntr_ptnt_rspnsblty_pmt, gdc_abv_oopt_amt, ptnt_pay_amt, rev_cntr_cash_ddctbl_amt, line_prmry_alowd_chrg_amt, rev_cntr_2nd_msp_pd_amt, othr_troop_amt, rptd_gap_dscnt_num, rev_cntr_rdcd_coinsrnc_amt, line_place_of_srvc_display, serviced_date, location_address_state, rev_cntr_stus_ind_cd, rev_cntr_stus_ind_display, mod_1, mod_2, mod_3, mod_4, rev_cntr_2nd_ansi_cd, ncvrd_plan_pd_amt, rev_cntr_1st_ansi_cd, rev_cntr_3rd_ansi_cd) VALUES (435858251, 2, null, null, null, 14, null, null, 271010573, '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327', null, null, null, null, 0, 14658, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'AR', null, null, null, null, null, null, null, null, null, null);
-- INSERT INTO public.dpoc_claim_lines (id, line, line_cms_type_srvc_cd, place_of_service, procedure_code, quantity, service_period_end, service_period_start, dpoc_claim_id, inserted_at, updated_at, ndc, ndc_display_name, fill_num, days_supply_num, rev_cntr_ncvrd_chrg_amt, rev_cntr_tot_chrg_amt, rev_cntr_rate_amt, rev_cntr_bene_pmt_amt, rev_cntr_pmt_amt_amt, rev_cntr_prvdr_pmt_amt, carr_line_rdcd_pmt_phys_astn_c, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt, line_sbmtd_chrg_amt, line_alowd_chrg_amt, line_prcsg_ind_cd, line_pmt_80_100_cd, line_pmt_80_100_display, line_nch_pmt_amt, carr_line_rdcd_pmt_phys_astn_c_display, rev_cntr_blood_ddctbl_amt, drug_cvrg_stus_cd, cvrd_d_plan_pd_amt, tot_rx_cst_amt, line_dme_prchs_price_amt, gdc_blw_oopt_amt, lics_amt, plro_amt, rev_cntr_coinsrnc_wge_adjstd_c, rev_cntr_1st_msp_pd_amt, rev_cntr_ptnt_rspnsblty_pmt, gdc_abv_oopt_amt, ptnt_pay_amt, rev_cntr_cash_ddctbl_amt, line_prmry_alowd_chrg_amt, rev_cntr_2nd_msp_pd_amt, othr_troop_amt, rptd_gap_dscnt_num, rev_cntr_rdcd_coinsrnc_amt, line_place_of_srvc_display, serviced_date, location_address_state, rev_cntr_stus_ind_cd, rev_cntr_stus_ind_display, mod_1, mod_2, mod_3, mod_4, rev_cntr_2nd_ansi_cd, ncvrd_plan_pd_amt, rev_cntr_1st_ansi_cd, rev_cntr_3rd_ansi_cd) VALUES (435858252, 3, null, null, null, 89, null, null, 271010573, '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327', null, null, null, null, 0, 366.78, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'AR', null, null, null, null, null, null, null, null, null, null);
-- INSERT INTO public.dpoc_claim_lines (id, line, line_cms_type_srvc_cd, place_of_service, procedure_code, quantity, service_period_end, service_period_start, dpoc_claim_id, inserted_at, updated_at, ndc, ndc_display_name, fill_num, days_supply_num, rev_cntr_ncvrd_chrg_amt, rev_cntr_tot_chrg_amt, rev_cntr_rate_amt, rev_cntr_bene_pmt_amt, rev_cntr_pmt_amt_amt, rev_cntr_prvdr_pmt_amt, carr_line_rdcd_pmt_phys_astn_c, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt, line_sbmtd_chrg_amt, line_alowd_chrg_amt, line_prcsg_ind_cd, line_pmt_80_100_cd, line_pmt_80_100_display, line_nch_pmt_amt, carr_line_rdcd_pmt_phys_astn_c_display, rev_cntr_blood_ddctbl_amt, drug_cvrg_stus_cd, cvrd_d_plan_pd_amt, tot_rx_cst_amt, line_dme_prchs_price_amt, gdc_blw_oopt_amt, lics_amt, plro_amt, rev_cntr_coinsrnc_wge_adjstd_c, rev_cntr_1st_msp_pd_amt, rev_cntr_ptnt_rspnsblty_pmt, gdc_abv_oopt_amt, ptnt_pay_amt, rev_cntr_cash_ddctbl_amt, line_prmry_alowd_chrg_amt, rev_cntr_2nd_msp_pd_amt, othr_troop_amt, rptd_gap_dscnt_num, rev_cntr_rdcd_coinsrnc_amt, line_place_of_srvc_display, serviced_date, location_address_state, rev_cntr_stus_ind_cd, rev_cntr_stus_ind_display, mod_1, mod_2, mod_3, mod_4, rev_cntr_2nd_ansi_cd, ncvrd_plan_pd_amt, rev_cntr_1st_ansi_cd, rev_cntr_3rd_ansi_cd) VALUES (435858253, 4, null, null, null, 14, null, null, 271010573, '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327', null, null, null, null, 0, 666, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'AR', null, null, null, null, null, null, null, null, null, null);
-- INSERT INTO public.dpoc_claim_lines (id, line, line_cms_type_srvc_cd, place_of_service, procedure_code, quantity, service_period_end, service_period_start, dpoc_claim_id, inserted_at, updated_at, ndc, ndc_display_name, fill_num, days_supply_num, rev_cntr_ncvrd_chrg_amt, rev_cntr_tot_chrg_amt, rev_cntr_rate_amt, rev_cntr_bene_pmt_amt, rev_cntr_pmt_amt_amt, rev_cntr_prvdr_pmt_amt, carr_line_rdcd_pmt_phys_astn_c, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt, line_sbmtd_chrg_amt, line_alowd_chrg_amt, line_prcsg_ind_cd, line_pmt_80_100_cd, line_pmt_80_100_display, line_nch_pmt_amt, carr_line_rdcd_pmt_phys_astn_c_display, rev_cntr_blood_ddctbl_amt, drug_cvrg_stus_cd, cvrd_d_plan_pd_amt, tot_rx_cst_amt, line_dme_prchs_price_amt, gdc_blw_oopt_amt, lics_amt, plro_amt, rev_cntr_coinsrnc_wge_adjstd_c, rev_cntr_1st_msp_pd_amt, rev_cntr_ptnt_rspnsblty_pmt, gdc_abv_oopt_amt, ptnt_pay_amt, rev_cntr_cash_ddctbl_amt, line_prmry_alowd_chrg_amt, rev_cntr_2nd_msp_pd_amt, othr_troop_amt, rptd_gap_dscnt_num, rev_cntr_rdcd_coinsrnc_amt, line_place_of_srvc_display, serviced_date, location_address_state, rev_cntr_stus_ind_cd, rev_cntr_stus_ind_display, mod_1, mod_2, mod_3, mod_4, rev_cntr_2nd_ansi_cd, ncvrd_plan_pd_amt, rev_cntr_1st_ansi_cd, rev_cntr_3rd_ansi_cd) VALUES (435858254, 5, null, null, null, 17, null, null, 271010573, '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327', null, null, null, null, 0, 1952, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'AR', null, null, null, null, null, null, null, null, null, null);
-- INSERT INTO public.dpoc_claim_lines (id, line, line_cms_type_srvc_cd, place_of_service, procedure_code, quantity, service_period_end, service_period_start, dpoc_claim_id, inserted_at, updated_at, ndc, ndc_display_name, fill_num, days_supply_num, rev_cntr_ncvrd_chrg_amt, rev_cntr_tot_chrg_amt, rev_cntr_rate_amt, rev_cntr_bene_pmt_amt, rev_cntr_pmt_amt_amt, rev_cntr_prvdr_pmt_amt, carr_line_rdcd_pmt_phys_astn_c, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt, line_sbmtd_chrg_amt, line_alowd_chrg_amt, line_prcsg_ind_cd, line_pmt_80_100_cd, line_pmt_80_100_display, line_nch_pmt_amt, carr_line_rdcd_pmt_phys_astn_c_display, rev_cntr_blood_ddctbl_amt, drug_cvrg_stus_cd, cvrd_d_plan_pd_amt, tot_rx_cst_amt, line_dme_prchs_price_amt, gdc_blw_oopt_amt, lics_amt, plro_amt, rev_cntr_coinsrnc_wge_adjstd_c, rev_cntr_1st_msp_pd_amt, rev_cntr_ptnt_rspnsblty_pmt, gdc_abv_oopt_amt, ptnt_pay_amt, rev_cntr_cash_ddctbl_amt, line_prmry_alowd_chrg_amt, rev_cntr_2nd_msp_pd_amt, othr_troop_amt, rptd_gap_dscnt_num, rev_cntr_rdcd_coinsrnc_amt, line_place_of_srvc_display, serviced_date, location_address_state, rev_cntr_stus_ind_cd, rev_cntr_stus_ind_display, mod_1, mod_2, mod_3, mod_4, rev_cntr_2nd_ansi_cd, ncvrd_plan_pd_amt, rev_cntr_1st_ansi_cd, rev_cntr_3rd_ansi_cd) VALUES (435858255, 6, null, null, null, 840, null, null, 271010573, '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327', null, null, null, null, 0, 4248, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'AR', null, null, null, null, null, null, null, null, null, null);
-- INSERT INTO public.dpoc_claim_lines (id, line, line_cms_type_srvc_cd, place_of_service, procedure_code, quantity, service_period_end, service_period_start, dpoc_claim_id, inserted_at, updated_at, ndc, ndc_display_name, fill_num, days_supply_num, rev_cntr_ncvrd_chrg_amt, rev_cntr_tot_chrg_amt, rev_cntr_rate_amt, rev_cntr_bene_pmt_amt, rev_cntr_pmt_amt_amt, rev_cntr_prvdr_pmt_amt, carr_line_rdcd_pmt_phys_astn_c, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt, line_sbmtd_chrg_amt, line_alowd_chrg_amt, line_prcsg_ind_cd, line_pmt_80_100_cd, line_pmt_80_100_display, line_nch_pmt_amt, carr_line_rdcd_pmt_phys_astn_c_display, rev_cntr_blood_ddctbl_amt, drug_cvrg_stus_cd, cvrd_d_plan_pd_amt, tot_rx_cst_amt, line_dme_prchs_price_amt, gdc_blw_oopt_amt, lics_amt, plro_amt, rev_cntr_coinsrnc_wge_adjstd_c, rev_cntr_1st_msp_pd_amt, rev_cntr_ptnt_rspnsblty_pmt, gdc_abv_oopt_amt, ptnt_pay_amt, rev_cntr_cash_ddctbl_amt, line_prmry_alowd_chrg_amt, rev_cntr_2nd_msp_pd_amt, othr_troop_amt, rptd_gap_dscnt_num, rev_cntr_rdcd_coinsrnc_amt, line_place_of_srvc_display, serviced_date, location_address_state, rev_cntr_stus_ind_cd, rev_cntr_stus_ind_display, mod_1, mod_2, mod_3, mod_4, rev_cntr_2nd_ansi_cd, ncvrd_plan_pd_amt, rev_cntr_1st_ansi_cd, rev_cntr_3rd_ansi_cd) VALUES (435858256, 7, null, null, null, 840, null, null, 271010573, '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327', null, null, null, null, 0, 4308, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'AR', null, null, null, null, null, null, null, null, null, null);
-- INSERT INTO public.dpoc_claim_lines (id, line, line_cms_type_srvc_cd, place_of_service, procedure_code, quantity, service_period_end, service_period_start, dpoc_claim_id, inserted_at, updated_at, ndc, ndc_display_name, fill_num, days_supply_num, rev_cntr_ncvrd_chrg_amt, rev_cntr_tot_chrg_amt, rev_cntr_rate_amt, rev_cntr_bene_pmt_amt, rev_cntr_pmt_amt_amt, rev_cntr_prvdr_pmt_amt, carr_line_rdcd_pmt_phys_astn_c, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt, line_sbmtd_chrg_amt, line_alowd_chrg_amt, line_prcsg_ind_cd, line_pmt_80_100_cd, line_pmt_80_100_display, line_nch_pmt_amt, carr_line_rdcd_pmt_phys_astn_c_display, rev_cntr_blood_ddctbl_amt, drug_cvrg_stus_cd, cvrd_d_plan_pd_amt, tot_rx_cst_amt, line_dme_prchs_price_amt, gdc_blw_oopt_amt, lics_amt, plro_amt, rev_cntr_coinsrnc_wge_adjstd_c, rev_cntr_1st_msp_pd_amt, rev_cntr_ptnt_rspnsblty_pmt, gdc_abv_oopt_amt, ptnt_pay_amt, rev_cntr_cash_ddctbl_amt, line_prmry_alowd_chrg_amt, rev_cntr_2nd_msp_pd_amt, othr_troop_amt, rptd_gap_dscnt_num, rev_cntr_rdcd_coinsrnc_amt, line_place_of_srvc_display, serviced_date, location_address_state, rev_cntr_stus_ind_cd, rev_cntr_stus_ind_display, mod_1, mod_2, mod_3, mod_4, rev_cntr_2nd_ansi_cd, ncvrd_plan_pd_amt, rev_cntr_1st_ansi_cd, rev_cntr_3rd_ansi_cd) VALUES (435858257, 8, null, null, null, 660, null, null, 271010573, '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327', null, null, null, null, 0, 3072, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'AR', null, null, null, null, null, null, null, null, null, null);
-- INSERT INTO public.dpoc_claim_lines (id, line, line_cms_type_srvc_cd, place_of_service, procedure_code, quantity, service_period_end, service_period_start, dpoc_claim_id, inserted_at, updated_at, ndc, ndc_display_name, fill_num, days_supply_num, rev_cntr_ncvrd_chrg_amt, rev_cntr_tot_chrg_amt, rev_cntr_rate_amt, rev_cntr_bene_pmt_amt, rev_cntr_pmt_amt_amt, rev_cntr_prvdr_pmt_amt, carr_line_rdcd_pmt_phys_astn_c, line_bene_pmt_amt, line_prvdr_pmt_amt, line_bene_ptb_ddctbl_amt, line_bene_prmry_pyr_pd_amt, line_coinsrnc_amt, line_sbmtd_chrg_amt, line_alowd_chrg_amt, line_prcsg_ind_cd, line_pmt_80_100_cd, line_pmt_80_100_display, line_nch_pmt_amt, carr_line_rdcd_pmt_phys_astn_c_display, rev_cntr_blood_ddctbl_amt, drug_cvrg_stus_cd, cvrd_d_plan_pd_amt, tot_rx_cst_amt, line_dme_prchs_price_amt, gdc_blw_oopt_amt, lics_amt, plro_amt, rev_cntr_coinsrnc_wge_adjstd_c, rev_cntr_1st_msp_pd_amt, rev_cntr_ptnt_rspnsblty_pmt, gdc_abv_oopt_amt, ptnt_pay_amt, rev_cntr_cash_ddctbl_amt, line_prmry_alowd_chrg_amt, rev_cntr_2nd_msp_pd_amt, othr_troop_amt, rptd_gap_dscnt_num, rev_cntr_rdcd_coinsrnc_amt, line_place_of_srvc_display, serviced_date, location_address_state, rev_cntr_stus_ind_cd, rev_cntr_stus_ind_display, mod_1, mod_2, mod_3, mod_4, rev_cntr_2nd_ansi_cd, ncvrd_plan_pd_amt, rev_cntr_1st_ansi_cd, rev_cntr_3rd_ansi_cd) VALUES (435858258, 9, null, null, null, 0, null, null, 271010573, '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327', null, null, null, null, 0, 29270.78, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 'AR', null, null, null, null, null, null, null, null, null, null);


SELECT * FROM dpoc_claim_dx WHERE dpoc_claim_id = 271010573;
-- INSERT INTO public.dpoc_claim_dx (id, dpoc_claim_id, sequence, code, type, inserted_at, updated_at) VALUES (263222964, 271010573, '1', '093', 'drg', '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327');
-- INSERT INTO public.dpoc_claim_dx (id, dpoc_claim_id, sequence, code, type, inserted_at, updated_at) VALUES (263222963, 271010573, '2', 'G7289', 'icd10', '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327');
-- INSERT INTO public.dpoc_claim_dx (id, dpoc_claim_id, sequence, code, type, inserted_at, updated_at) VALUES (263222962, 271010573, '3', 'E785', 'icd10', '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327');
-- INSERT INTO public.dpoc_claim_dx (id, dpoc_claim_id, sequence, code, type, inserted_at, updated_at) VALUES (263222961, 271010573, '4', 'G609', 'icd10', '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327');
-- INSERT INTO public.dpoc_claim_dx (id, dpoc_claim_id, sequence, code, type, inserted_at, updated_at) VALUES (263222960, 271010573, '5', 'D696', 'icd10', '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327');
-- INSERT INTO public.dpoc_claim_dx (id, dpoc_claim_id, sequence, code, type, inserted_at, updated_at) VALUES (263222959, 271010573, '6', 'C678', 'icd10', '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327');
-- INSERT INTO public.dpoc_claim_dx (id, dpoc_claim_id, sequence, code, type, inserted_at, updated_at) VALUES (263222958, 271010573, '7', 'I10', 'icd10', '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327');
-- INSERT INTO public.dpoc_claim_dx (id, dpoc_claim_id, sequence, code, type, inserted_at, updated_at) VALUES (263222957, 271010573, '8', 'R21', 'icd10', '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327');
-- INSERT INTO public.dpoc_claim_dx (id, dpoc_claim_id, sequence, code, type, inserted_at, updated_at) VALUES (263222956, 271010573, '9', 'S80811D', 'icd10', '2023-08-11 02:07:17.262772', '2024-01-10 02:34:19.386327');

-- delete FROM dpoc_claim_lines WHERE dpoc_claim_id = 271010573;
-- delete FROM dpoc_claim_dx WHERE dpoc_claim_id = 271010573;


SELECT *
FROM
    dpoc_claims c
    LEFT JOIN dpoc_claim_dx dx ON c.id = dx.dpoc_claim_id AND dx.type = 'drg'
WHERE
      c.eob_type = 'INPATIENT'
-- and id = 271010573
  AND c.drg ISNULL
  AND dx.id ISNULL
;

WITH
    claim_drgs AS (
    SELECT
                        dx.dpoc_claim_id
                      , dx.code
                      , dx.id dx_to_del_id
                    FROM
                        dpoc_claims c
                        JOIN dpoc_claim_dx dx ON c.id = dx.dpoc_claim_id AND dx.sequence = 1 AND dx.type = 'drg'
                    WHERE
                          c.eob_type = 'INPATIENT'
                      AND c.drg ISNULL
                    )
  , clm_upd    AS (
    UPDATE dpoc_claims c
        SET
            drg = cd.code, updated_at = NOW()
        FROM
            claim_drgs cd
        WHERE
            c.eob_type = 'INPATIENT'
                AND c.id = cd.dpoc_claim_id
                AND c.drg ISNULL )
  , dx_del     AS (
    DELETE
        FROM
            dpoc_claim_dx dx
            USING
                claim_drgs cd
            WHERE
                cd.dx_to_del_id = dx.id )
UPDATE dpoc_claim_dx dx
SET
    sequence   = sequence - 1
  , updated_at = NOW()
FROM
    claim_drgs cd
WHERE
    cd.dpoc_claim_id = dx.dpoc_claim_id

;
------------------------------------------------------------------------------------------------------------------------
/* with tmp tables */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS junk._claim_drgs;
CREATE TABLE junk._claim_drgs AS
SELECT
    dx.dpoc_claim_id
  , dx.code
  , dx.id dx_to_del_id
FROM
    dpoc_claims c
    JOIN dpoc_claim_dx dx ON c.id = dx.dpoc_claim_id
                                 AND dx.sequence = 1
                                 AND dx.type = 'drg'
WHERE
  c.drg ISNULL
;
CREATE INDEX on junk._claim_drgs(dpoc_claim_id);
CREATE INDEX on junk._claim_drgs(dx_to_del_id);


UPDATE dpoc_claims c
SET
    drg = cd.code, updated_at = NOW()
FROM
    junk._claim_drgs cd
WHERE c.id = cd.dpoc_claim_id
  AND c.drg ISNULL
            ;
DELETE
FROM
    dpoc_claim_dx dx
    USING
        junk._claim_drgs cd
WHERE
    cd.dx_to_del_id = dx.id;

UPDATE dpoc_claim_dx dx
SET
    sequence   = sequence - 1
  , updated_at = NOW()
FROM
    junk._claim_drgs cd
WHERE
    cd.dpoc_claim_id = dx.dpoc_claim_id
;


create index dpoc_claim_dx_dpoc_claim_id_index
    on public.dpoc_claim_dx (dpoc_claim_id);

create unique index dpoc_claim_dx_dpoc_claim_id_sequence_index
    on public.dpoc_claim_dx (dpoc_claim_id, sequence);

SELECT * FROM dpoc_claim_dx WHERE type = 'drg';

VACUUM FULL ANALYZE dpoc_claims;
VACUUM FULL ANALYZE dpoc_claim_lines;
VACUUM FULL ANALYZE dpoc_claim_dx;
