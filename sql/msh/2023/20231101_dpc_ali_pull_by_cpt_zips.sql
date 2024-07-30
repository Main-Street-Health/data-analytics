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

drop table if exists _proc_code_hit;
create temp table _proc_code_hit as
select distinct c.id dpoc_claim_id, p.id dpoc_patient_id
from dpoc_claim_lines cl
join dpoc_claims c
  on cl.dpoc_claim_id = c.id
join dpoc_patients p
  on p.bene_id = c.patient
where cl.procedure_code in ('11600','11601','11602','11603','11604','11606','11621','11622','11623','11626','11640','11641','11642','11644','11646','17260','17261','17262','17263','17264','17266','17270','17271','17272','17273','17274','17280','17281','17282','17283','19081','19083','19084','19085','19120','19125','19296','19297','19301','19302','21016','31535','31536','31541','31622','31623','31624','31625','31626','31627','31628','31629','31630','31632','31633','31641','31645','31652','31653','31654','32480','32650','32656','32666','32701','38500','38505','38510','38525','38570','38572','38770','43200','43202','43229','43232','43236','43237','43242','43245','43246','43251','43260','43261','43270','44143','44160','44188','44205','44207','44213','44360','44361','45338','45378','45380','45381','45382','45384','45385','45388','45390','45398','46600','46606','47120','48140','49205','50230','50543','50545','50593','55700','55706','55821','56605','56820','56821','57420','57421','57452','58100','58150','58210','58262','58263','71271','71275','74175','74261','76090','76091','76604','76641','76642','76932','77021','77063','78195','78811','78812','78813','78814','78815','78816','84152','84153','84154','88309','3126F','3340F','3341F','3342F','3343F','3344F','3350F','C1830','C8903','C8908','G0101','G0102','G0103','G0104','G0105','G0121','G0202','G0204','G0328','G6002','G6012','G6013','G9418','G9422','Q0091')
    and exists(
      select 1 from _patients pats where pats.patient_id = p.source_id
    )
;

create index on _proc_code_hit(dpoc_patient_id);
create index on _proc_code_hit(dpoc_claim_id);

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
from
    _proc_code_hit pch
join dpoc_patients p on pch.dpoc_patient_id = p.id
join dpoc_claims c on pch.dpoc_claim_id = c.id and p.bene_id = c.patient
join dpoc_claim_lines dcl on c.id = dcl.dpoc_claim_id
;

-- 20231101_dpc_ali_pull
SELECT * FROM _output order by claim_id;

DROP TABLE IF EXISTS _output;
DROP TABLE IF EXISTS _proc_code_hit;
DROP TABLE IF EXISTS _patients;
DROP TABLE IF EXISTS _zips
