drop table if exists _patients;
create temp table _patients as
select distinct sp.patient_id
from fdw_member_doc.supreme_pizza sp
join fdw_member_doc.patient_addresses pa
  on pa.patient_id = sp.patient_id
where sp.is_om
and pa.postal_code in
--      winchester polaski
--     ('37398', '37318', '37324', '37375', '37330', '37388', '37352', '37359', '37335', '37342', '37366', '37356', '37376', '37306', '37345', '37328', '37348', '37387', '37355', '37334', '38478', '38456', '37160', '38449', '38464', '37091', '35611', '38401', '38472', '37047', '38459', '38453', '38477', '38455', '38460', '38473', '38457', '38468', '37019', '38469', '35610', '35648', '38488', '37144', '38451', '38474', '38483', '38486', '38481', '35647', '35620', '35739', '35613', '35749', '35757', '35773', '35750', '37360', '35772', '35740', '35745', '35768', '35746', '35766', '35761', '37380')

-- macon
('37083','37066','37150','37148','38563','42167','37087','37030','38551','42164','42140','42153','42120','37186','38588','37145','37057','37074','37022','37031','42133','42157','42151','38575','37151','38562','42124','42731','42166','42130');

--bsf
-- ('37732','37733','37755','37756','37762','37819','37841','37847','37852','37891','37892','38504','38556','40769','42631','42635','42638','42647','42649','42653')

-- rhea
-- ('37874','37826','37846','37774','37880','37742','37801','37885','37737','37777','37772','37771','37763','37854','37337','37381','37370','37329','37385','37354','37303','37322','37331','37748','37361','37804','37627','37367','37343','38555','37321','37327','37338','38585','37373','37308','37336','37379','37312','37311','37310','38571','37307','37317','37313','37909','37921','37920','37803')


create unique index  on _patients(patient_id);

drop table if exists _proc_code_hit;
create temp table _proc_code_hit as
select p.id dpoc_patient_id
from dpoc_claim_lines cl
join dpoc_claims c
  on cl.dpoc_claim_id = c.id
join dpoc_patients p
  on p.bene_id = c.patient
where cl.procedure_code in ( 'J9271', 'J9299', 'J2506', 'J9022', 'J9173', 'J9145', 'J9306', 'J9305', 'Q5117', 'Q5107', 'J0881', 'J9355', 'J1439', 'J9228', 'J1300', 'J9035', 'J9264', 'J9354', 'J2353', 'Q5115', 'J2796', 'J9041', 'J9312', 'J9311', 'J1303', 'J1930', 'J1569', 'J0185', 'J9042', 'J9047', 'J9034', 'J9303', 'J9119', 'J9395', 'J9308', 'J9301', 'J1561', 'J1950', 'J9179', 'J9070', 'J9309', 'J9205', 'J9055', 'Q0138', 'J9176', 'J9043', 'Q5111', 'J1442', 'J0897', 'J0129', 'J2350', 'J3380', 'J1745', 'J0717', 'J2357', 'J1602', 'J3241', 'J3262', 'Q5119', 'J3357', 'J2182', 'J2323', 'J0517', 'J2507', 'J3245', 'J0256', 'J0490', 'J0222', 'J0485', 'Q5103', 'J0221', 'J0180', 'Q5104', 'J1786', 'J0584', 'J1823', 'J0257', 'J3032', 'J2786', 'J3240', 'J0223', 'Q5121', 'J1306', 'J2356', 'J2329', 'J0225', 'J2327', 'J1417', 'J0638', 'J0219', 'J0224', 'J0491', 'J9332', 'J1743', 'J0202', 'J1756', 'J9144', 'Q5112', 'Q5114', 'Q5116', 'Q5118', 'Q5126', 'J1554', 'J9316', 'Q5127', 'J9023', 'J1459', 'J1556', 'J1557', 'J1566', 'J1568', 'J1572', 'J2505', 'J9020', 'J9040', 'J9039', 'J1740', 'J9045', 'J9050', 'J9999', 'J9060', 'J9065', 'J1555', 'J9100', 'J9130', 'J0894', 'J9150', 'J9171', 'J9170', 'J9000', 'Q2050', 'J9178', 'J1438', 'J0885', 'J0888', 'J9019', 'J0207', 'J9181', 'J1744', 'J9155', 'J9185', 'J9190', 'J1560', 'J1460', 'J9201', 'J1595', 'J1447', 'J1573', 'J1559', 'J0135', 'J0152', 'J9351', 'J1575', 'J9211', 'J9208', 'J9214', 'J1826', 'J9216', 'J9206', 'J1750', 'J9315', 'J9207', 'J9302', 'J0640', 'J9218', 'J0641', 'J9217', 'J9371', 'J9209', 'J9260', 'J3590', 'J9293', 'J9203', 'J9261', 'J9266', 'J9263', 'J9267', 'J2430', 'J1599', 'J9295', 'Q2043', 'J3489', 'J2354', 'J3358', 'J9328', 'J9340', 'J9330', 'J9033', 'J1628', 'J9017', 'J9025', 'J9360', 'J9370', 'J9390', 'J1562', 'J3385', 'JKI11', 'J1558', 'J9352', 'J9202', 'J3111', 'J7179', 'J8501', 'J8540', 'J8610', 'J1437', 'J8999')
and exists(
  select 1 from _patients pats where pats.patient_id = p.source_id
)
;

create index on _proc_code_hit(dpoc_patient_id);

DROP TABLE IF EXISTS _output;
CREATE TEMP TABLE _output AS
select
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
from dpoc_patients p
join dpoc_claims c
  on p.bene_id = c.patient
join dpoc_claim_lines dcl
  on c.id = dcl.dpoc_claim_id
where exists( select 1 from _proc_code_hit pats where pats.dpoc_patient_id = p.id )
;
-- 20231027_dpc_ali_pull_winchester_polaski
-- 20231027_dpc_ali_pull_macon
SELECT *
FROM
    _output;
------------------------------------------------------------------------------------------------------------------------
/* zip counts */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _zips;
CREATE TEMP TABLE _zips AS
SELECT *
FROM
    ( VALUES
          ('37398', 'win/polaski'),
          ('37318', 'win/polaski'),
          ('37324', 'win/polaski'),
          ('37375', 'win/polaski'),
          ('37330', 'win/polaski'),
          ('37388', 'win/polaski'),
          ('37352', 'win/polaski'),
          ('37359', 'win/polaski'),
          ('37335', 'win/polaski'),
          ('37342', 'win/polaski'),
          ('37366', 'win/polaski'),
          ('37356', 'win/polaski'),
          ('37376', 'win/polaski'),
          ('37306', 'win/polaski'),
          ('37345', 'win/polaski'),
          ('37328', 'win/polaski'),
          ('37348', 'win/polaski'),
          ('37387', 'win/polaski'),
          ('37355', 'win/polaski'),
          ('37334', 'win/polaski'),
          ('38478', 'win/polaski'),
          ('38456', 'win/polaski'),
          ('37160', 'win/polaski'),
          ('38449', 'win/polaski'),
          ('38464', 'win/polaski'),
          ('37091', 'win/polaski'),
          ('35611', 'win/polaski'),
          ('38401', 'win/polaski'),
          ('38472', 'win/polaski'),
          ('37047', 'win/polaski'),
          ('38459', 'win/polaski'),
          ('38453', 'win/polaski'),
          ('38477', 'win/polaski'),
          ('38455', 'win/polaski'),
          ('38460', 'win/polaski'),
          ('38473', 'win/polaski'),
          ('38457', 'win/polaski'),
          ('38468', 'win/polaski'),
          ('37019', 'win/polaski'),
          ('38469', 'win/polaski'),
          ('35610', 'win/polaski'),
          ('35648', 'win/polaski'),
          ('38488', 'win/polaski'),
          ('37144', 'win/polaski'),
          ('38451', 'win/polaski'),
          ('38474', 'win/polaski'),
          ('38483', 'win/polaski'),
          ('38486', 'win/polaski'),
          ('38481', 'win/polaski'),
          ('35647', 'win/polaski'),
          ('35620', 'win/polaski'),
          ('35739', 'win/polaski'),
          ('35613', 'win/polaski'),
          ('35749', 'win/polaski'),
          ('35757', 'win/polaski'),
          ('35773', 'win/polaski'),
          ('35750', 'win/polaski'),
          ('37360', 'win/polaski'),
          ('35772', 'win/polaski'),
          ('35740', 'win/polaski'),
          ('35745', 'win/polaski'),
          ('35768', 'win/polaski'),
          ('35746', 'win/polaski'),
          ('35766', 'win/polaski'),
          ('35761', 'win/polaski'),
          ('37380', 'win/polaski'),
          ('37083', 'macon'),
          ('37066', 'macon'),
          ('37150', 'macon'),
          ('37148', 'macon'),
          ('38563', 'macon'),
          ('42167', 'macon'),
          ('37087', 'macon'),
          ('37030', 'macon'),
          ('38551', 'macon'),
          ('42164', 'macon'),
          ('42140', 'macon'),
          ('42153', 'macon'),
          ('42120', 'macon'),
          ('37186', 'macon'),
          ('38588', 'macon'),
          ('37145', 'macon'),
          ('37057', 'macon'),
          ('37074', 'macon'),
          ('37022', 'macon'),
          ('37031', 'macon'),
          ('42133', 'macon'),
          ('42157', 'macon'),
          ('42151', 'macon'),
          ('38575', 'macon'),
          ('37151', 'macon'),
          ('38562', 'macon'),
          ('42124', 'macon'),
          ('42731', 'macon'),
          ('42166', 'macon'),
          ('42130', 'macon'),
          ('37732', 'bsf'),
          ('37733', 'bsf'),
          ('37755', 'bsf'),
          ('37756', 'bsf'),
          ('37762', 'bsf'),
          ('37819', 'bsf'),
          ('37841', 'bsf'),
          ('37847', 'bsf'),
          ('37852', 'bsf'),
          ('37891', 'bsf'),
          ('37892', 'bsf'),
          ('38504', 'bsf'),
          ('38556', 'bsf'),
          ('40769', 'bsf'),
          ('42631', 'bsf'),
          ('42635', 'bsf'),
          ('42638', 'bsf'),
          ('42647', 'bsf'),
          ('42649', 'bsf'),
          ('42653', 'bsf'),
          ('37874', 'rhea'),
          ('37826', 'rhea'),
          ('37846', 'rhea'),
          ('37774', 'rhea'),
          ('37880', 'rhea'),
          ('37742', 'rhea'),
          ('37801', 'rhea'),
          ('37885', 'rhea'),
          ('37737', 'rhea'),
          ('37777', 'rhea'),
          ('37772', 'rhea'),
          ('37771', 'rhea'),
          ('37763', 'rhea'),
          ('37854', 'rhea'),
          ('37337', 'rhea'),
          ('37381', 'rhea'),
          ('37370', 'rhea'),
          ('37329', 'rhea'),
          ('37385', 'rhea'),
          ('37354', 'rhea'),
          ('37303', 'rhea'),
          ('37322', 'rhea'),
          ('37331', 'rhea'),
          ('37748', 'rhea'),
          ('37361', 'rhea'),
          ('37804', 'rhea'),
          ('37627', 'rhea'),
          ('37367', 'rhea'),
          ('37343', 'rhea'),
          ('38555', 'rhea'),
          ('37321', 'rhea'),
          ('37327', 'rhea'),
          ('37338', 'rhea'),
          ('38585', 'rhea'),
          ('37373', 'rhea'),
          ('37308', 'rhea'),
          ('37336', 'rhea'),
          ('37379', 'rhea'),
          ('37312', 'rhea'),
          ('37311', 'rhea'),
          ('37310', 'rhea'),
          ('38571', 'rhea'),
          ('37307', 'rhea'),
          ('37317', 'rhea'),
          ('37313', 'rhea'),
          ('37909', 'rhea'),
          ('37921', 'rhea'),
          ('37920', 'rhea'),
          ('37803', 'rhea') ) x(zip, zips_name);


drop table if exists _patients;
create temp table _patients as
select distinct sp.patient_id, z.zips_name
from fdw_member_doc.supreme_pizza sp
join fdw_member_doc.patient_addresses pa
  on pa.patient_id = sp.patient_id
join _zips z on z.zip = pa.postal_code
where sp.is_om;

create unique index  on _patients(patient_id);


drop table if exists _claim_hit;
CREATE TEMP TABLE _claim_hit AS
SELECT
    pats.*
FROM
    _patients pats
WHERE
    EXISTS ( SELECT
                 1
             FROM
                 dpoc_patients p
                 JOIN dpoc_claims c ON c.patient = p.bene_id
             WHERE pats.patient_id = p.source_id );

-- 20231027_dpc_ali_pull_zip_counts
SELECT
    pats.zips_name
  , COUNT(ch.patient_id) total_w_claims
FROM
    _patients pats
    LEFT JOIN _claim_hit ch ON pats.patient_id = ch.patient_id
GROUP BY
    1;
