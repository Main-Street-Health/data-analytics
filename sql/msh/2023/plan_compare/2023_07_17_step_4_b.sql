-- Script for Milliman Providers-> Step 4b
-- Rohan

-- inclusions - exclude Old claims over 12 months

DROP TABLE IF EXISTS _milliman_pc_provider_claim_inclusions;

CREATE TEMP TABLE _milliman_pc_provider_claim_inclusions AS

SELECT id
FROM raw.milliman_plan_compare_providers
WHERE inbound_file_id = 10
  and datelastseen::date >= '2022-07-01';

-- inclusions - Medicare Assignment (exclude N and null values);
-- Temporarily leave this out
DROP TABLE IF EXISTS _milliman_pc_provider_med_assignment_inclusions;

CREATE TEMP TABLE _milliman_pc_provider_med_assignment_inclusions AS

SELECT id
FROM raw.milliman_plan_compare_providers
WHERE inbound_file_id = 10
  and medicareassignment = 'Y';

--  Specialty codes Rules

DROP TABLE IF EXISTS _milliman_pc_provider_specialty_rules;

CREATE TEMP TABLE _milliman_pc_provider_specialty_rules AS

SELECT *
FROM (VALUES ('00', '00', 'Carrier wide    ', 'EXCLUDE'),
             ('01', '01', 'General practice    ', 'EXCLUDE'),
             ('02', '02', 'General surgery ', 'EXCLUDE'),
             ('03', '03', 'Allergy / immunology   ', ''),
             ('04', '04', 'Otolaryngology ', ''),
             ('05', '05', 'Anesthesiology  ', 'EXCLUDE'),
             ('06', '06', 'Cardiology ', ''),
             ('07', '07', 'Dermatology', ''),
             ('08', '08', 'Family practice ', 'EXCLUDE'),
             ('09', '09', 'Interventional Pain Management ', ''),
             ('10', '10', 'Gastroenterology   ', ''),
             ('11', '11', 'Internal medicine   ', 'EXCLUDE'),
             ('12', '12', 'Osteopathic manipulative therapy   ', ''),
             ('13', '13', 'Neurology  ', ''),
             ('14', '14', 'Neurosurgery   ', ''),
             ('15', '15', 'Speech Language Pathologists   ', ''),
             ('16', '16', 'Obstetrics/gynecology  ', ''),
             ('17', '17', 'Hospice and Palliative Care ', 'EXCLUDE'),
             ('18', '18', 'Ophthalmology   ', 'EXCLUDE'),
             ('19', '19', 'Oral Surgery (dentists only)    ', 'EXCLUDE'),
             ('20', '20', 'Orthopedic surgery ', ''),
             ('21', '21', 'Cardiac Electrophysiology  ', ''),
             ('22', '22', 'Pathology   ', 'EXCLUDE'),
             ('23', '23', 'Sports Medicine', ''),
             ('24', '24', 'Plastic and reconstructive surgery ', ''),
             ('25', '25', 'Physical medicine and rehabilitation   ', ''),
             ('26', '26', 'Psychiatry ', ''),
             ('27', '27', 'Geriatric psychiatry   ', ''),
             ('28', '28', 'Colorectal surgery ', ''),
             ('29', '29', 'Pulmonary disease  ', ''),
             ('30', '30', 'Diagnostic radiology    ', 'EXCLUDE'),
             ('31', '31', 'Intensive Cardiac Rehabilitation   ', ''),
             ('32', '32', 'Anesthesiologist Assistant  ', 'EXCLUDE'),
             ('33', '33', 'Thoracic surgery   ', ''),
             ('34', '34', 'Urology', ''),
             ('35', '35', 'Chiropractic   ', ''),
             ('36', '36', 'Nuclear medicine   ', ''),
             ('37', '37', 'Pediatric medicine ', ''),
             ('38', '38', 'Geriatric medicine  ', 'EXCLUDE'),
             ('39', '39', 'Nephrology ', ''),
             ('40', '40', 'Hand surgery   ', ''),
             ('41', '41', 'Optometrist ', 'EXCLUDE'),
             ('42', '42', 'Certified nurse midwife ', 'EXCLUDE'),
             ('43', '43', 'CRNA, anesthesia assistant  ', 'EXCLUDE'),
             ('44', '44', 'Infectious disease ', ''),
             ('45', '45', 'Mammography screening center    ', 'EXCLUDE'),
             ('46', '46', 'Endocrinology  ', ''),
             ('47', '47', 'Independent Diagnostic Testing Facility (IDTF)  ', 'EXCLUDE'),
             ('48', '48', 'Podiatry   ', ''),
             ('49', '49', 'Ambulatory surgical center  ', 'EXCLUDE'),
             ('50', '50', 'Nurse practitioner  ', 'EXCLUDE'),
             ('51', '51', 'Medical supply company with certified orthotist ', 'EXCLUDE'),
             ('52', '52', 'Medical supply company with certified prosthetist   ', 'EXCLUDE'),
             ('53', '53', 'Medical supply company with certified prosthetist-orthotist ', 'EXCLUDE'),
             ('54', '54', 'Medical supply company for DMERC    ', 'EXCLUDE'),
             ('55', '55', 'Individual certified orthotist  ', 'EXCLUDE'),
             ('56', '56', 'Individual certified prosthetist    ', 'EXCLUDE'),
             ('57', '57', 'Individual certified prosthetist-orthotist  ', 'EXCLUDE'),
             ('58', '58', 'Supply company with registered pharmacist   ', 'EXCLUDE'),
             ('59', '59', 'Ambulance service supplier  ', 'EXCLUDE'),
             ('60', '60', 'Public health or welfare agencies   ', 'EXCLUDE'),
             ('61', '61', 'Voluntary health or charitable agencies ', 'EXCLUDE'),
             ('62', '62', 'Psychologist (billing independently)   ', ''),
             ('63', '63', 'Portable X-ray supplier ', 'EXCLUDE'),
             ('64', '64', 'Audiologist (billing independently)', ''),
             ('65', '65', 'Physical therapist (independently practicing)  ', ''),
             ('66', '66', 'Rheumatology   ', ''),
             ('67', '67', 'Occupational therapist (independently practicing)  ', ''),
             ('68', '68', 'Clinical psychologist  ', ''),
             ('69', '69', 'Clinical laboratory (billing independently) ', 'EXCLUDE'),
             ('70', '70', 'Multispecialty clinic or group practice', ''),
             ('71', '71', 'Registered Dietician / Nutrition Professional   ', 'EXCLUDE'),
             ('72', '72', 'Pain Management', ''),
             ('73', '73', 'Mass Immunization Roster Billers    ', 'EXCLUDE'),
             ('74', '74', 'Radiation Therapy Center   ', ''),
             ('75', '75', 'Slide Preparation Facility  ', 'EXCLUDE'),
             ('76', '76', 'Peripheral vascular disease', ''),
             ('77', '77', 'Vascular surgery   ', ''),
             ('78', '78', 'Cardiac surgery', ''),
             ('79', '79', 'Addiction medicine ', ''),
             ('80', '80', 'Licensed clinical social worker', ''),
             ('81', '81', 'Critical care (intensivists)    ', 'EXCLUDE'),
             ('82', '82', 'Hematology ', ''),
             ('83', '83', 'Hematology/oncology', ''),
             ('84', '84', 'Preventive medicine ', 'EXCLUDE'),
             ('85', '85', 'Maxillofacial surgery  ', ''),
             ('86', '86', 'Neuropsychiatry', ''),
             ('87', '87', 'All other suppliers ', 'EXCLUDE'),
             ('88', '88', 'Unknown provider specialty  ', 'EXCLUDE'),
             ('89', '89', 'Certified clinical nurse specialist', ''),
             ('90', '90', 'Medical oncology   ', ''),
             ('91', '91', 'Surgical oncology  ', ''),
             ('92', '92', 'Radiation oncology ', ''),
             ('93', '93', 'Emergency medicine  ', 'EXCLUDE'),
             ('94', '94', 'Interventional Radiology    ', 'EXCLUDE'),
             ('95', '95', 'Unknown Supplier    ', 'EXCLUDE'),
             ('96', '96', 'Optician    ', 'EXCLUDE'),
             ('97', '97', 'Physician assistant ', 'EXCLUDE'),
             ('98', '98', 'Gynecologist/oncologist', ''),
             ('99', '99', 'Unknown physician specialty', ''),
             ('A0', 'A0', 'Hospital*   ', 'EXCLUDE'),
             ('A1', 'A1', 'Skilled Nursing Facility   ', ''),
             ('A2', 'A2', 'Intermediate care nursing facility ', ''),
             ('A3', 'A3', 'Nursing facility, other', ''),
             ('A4', 'A4', 'Home Health Agency ', ''),
             ('A5', 'A5', 'Pharmacy    ', 'EXCLUDE'),
             ('A6', 'A6', 'Medical supply company with respiratory therapist   ', 'EXCLUDE'),
             ('A7', 'A7', 'Department Store    ', 'EXCLUDE'),
             ('A8', 'A8', 'Grocery Store   ', 'EXCLUDE'),
             ('A9', 'A9', 'Indian Health Service, tribe and tribal organizations   ', 'EXCLUDE'),
             ('B1', 'B1', 'Supplier of oxygen and/or oxygen related equipment  ', 'EXCLUDE'),
             ('B2', 'B2', 'Pedorthic Personnel ', 'EXCLUDE'),
             ('B3', 'B3', 'Medical Supply Company with Pedorthic Personnel ', 'EXCLUDE'),
             ('B4', 'B4', 'Rehabilitation Agency  ', ''),
             ('B5', 'B5', 'Ocularist   ', 'EXCLUDE'),
             ('C0', 'C0', 'Sleep Medicine ', ''),
             ('C1', 'C1', 'Centralized Flu ', 'EXCLUDE'),
             ('C2', 'C2', 'Indirect Payment Procedure  ', 'EXCLUDE'),
             ('C3', 'C3', 'Interventional Cardiology  ', ''),
             ('C4', 'C4', 'Restricted Use  ', 'EXCLUDE'),
             ('C5', 'C5', 'Dentist ', 'EXCLUDE'),
             ('C6', 'C6', 'Hospitalist ', 'EXCLUDE'),
             ('C7', 'C7', 'Advanced Heart Failure and Transplant Cardiology   ', ''),
             ('C8', 'C8', 'Medical Toxicology ', ''),
             ('C9', 'C9', 'Hematopoietic Cell Transplantation and Cellular Therapy', ''),
             ('D1', 'D1', 'Medicare Diabetes Preventive Program    ', 'EXCLUDE'),
             ('D2', 'D2', 'Restricted Use  ', 'EXCLUDE'),
             ('D3', 'D3', 'Medical Genetics and Genomics   ', 'EXCLUDE'),
             ('D4', 'D4', 'Undersea and Hyperbaric Medicine    ', 'EXCLUDE'),
             ('D5', 'D5', 'Opioid Treatment Program   ', ''),
             ('D6', 'D6', 'Home Infusion Therapy Services ', ''),
             ('D7', 'D7', 'Micrographic Dermatologic Surgery  ', ''),
             ('D8', 'D8', 'Adult Congenital Heart Disease ', ''),
             ('X0', 'X0', 'Dentist ', 'EXCLUDE'),
             ('X1', 'X1', 'Otology', ''),
             ('X2', 'X2', 'Home Infusion Therapy  ', ''),
             ('X3', 'X3', 'Urgent Care ', 'EXCLUDE'),
             ('X4', 'X4', 'Alternative Medicine    ', 'EXCLUDE'),
             ('X5', 'X5', 'Speech Therapy ', ''),
             ('X6', 'X6', 'Genetics    ', 'EXCLUDE'),
             ('X7', 'X7', 'Nurse, Non Practitioner ', 'EXCLUDE'),
             ('X8', 'X8', 'Birthing Center ', 'EXCLUDE'),
             ('X9', 'X9', 'Dialysis Center', ''),
             ('XX', 'XX', 'UNK ', 'EXCLUDE'),
             ('Y0', 'Y0', 'Diagnostic Radiology Center ', 'EXCLUDE'),
             ('Y1', 'Y1', 'Hospice ', 'EXCLUDE'),
             ('Y2', 'Y2', 'Psych/Mental Health Facility   ', ''),
             ('Y3', 'Y3', 'Rehabilitation Center  ', ''),
             ('Y4', 'Y4', 'Alcohol/Drug Abuse Treatment Facility  ', ''),
             ('Y5', 'Y5', 'Neonatology ', 'EXCLUDE'),
             ('Y7', 'Y7', 'Sports Medicine', ''),
             ('Y8', 'Y8', 'Naturopath  ', 'EXCLUDE'),
             ('Y9', 'Y9', 'Homeopath   ', 'EXCLUDE'),
             ('Z0', 'Z0', 'Assistant Surgeon   ',
              'EXCLUDE')) x(specialty, specialty_code, speciality_description, exclusion);

-- inclusions - filter out Specialty codes where exclusion ='EXCLUDE';

DROP TABLE IF EXISTS _milliman_pc_provider_specialty_inclusions;

CREATE TEMP TABLE _milliman_pc_provider_specialty_inclusions AS

select *
from _milliman_pc_provider_specialty_rules
where exclusion = '';


-- providers
DROP TABLE IF EXISTS _milliman_pc_specialty_providers_for_ideon;

CREATE TEMP TABLE _milliman_pc_specialty_providers_for_ideon AS
select distinct mpcp.id,
                mpcp.memberid,
                mpcp.contactid,
                mpcp.npi,
                mpcp.providerfirstname,
                mpcp.providermiddlename,
                mpcp.providerlastname,
                mpcp.providercredentialtext,
                mpcp.providerorganizationname,
                mpcp.addressline1,
                mpcp.addressline2,
                mpcp.cityname,
                mpcp.statename,
                mpcp.postalcode,
                mpcp.countrycodeoutofus,
                mpcp.datelastseen::date,
                mpcp.primaryspecialty,
                mpcp.specialtycode,
                mpcp.specialtytype,
                mpcp.medicareassignment
from raw.milliman_plan_compare_providers mpcp
         join _milliman_pc_provider_claim_inclusions mpci on mpci.id = mpcp.id
    -- temporarily suppress med assignment
-- join _milliman_pc_provider_med_assignment_inclusions mmi on mmi.id = mpcp.id
         join _milliman_pc_provider_specialty_inclusions mpsi on mpsi.specialty_code = mpcp.specialtycode
where inbound_file_id = 10
order by memberid;



-- possible hospitals
DROP TABLE IF EXISTS _milliman_pc_specialty_hospitals_for_ideon;

CREATE TEMP TABLE _milliman_pc_specialty_hospitals_for_ideon AS

select distinct mpcp.*
from raw.milliman_plan_compare_providers mpcp
         join _milliman_pc_provider_claim_inclusions mpci on mpci.id = mpcp.id
         join _milliman_pc_provider_specialty_rules mr
              on mr.specialty_code = mpcp.specialtycode and mr.specialty_code = 'A0'
where inbound_file_id = 10;

-- 39

DROP TABLE IF EXISTS _milliman_pc_pcp_for_ideon;

CREATE TEMP TABLE _milliman_pc_pcp_for_ideon AS
select p.id                 patient_id,
       p.full_name,
       pa.postal_code       patient_zipcode,
       rp.id                referring_partner_id,
       rp.name              referring_partner,
       rp.zip               referring_partner_zip,
       c.name               primary_pcp,
       mp.id                pcp_msh_physician_id,
       mp.npi               primary_pcp_npi,
       mp_primary.id        partner_physician_msh_physician_id,
       mp_primary.full_name partner_physician,
       mp_primary.npi       partner_physician_npi
from prd.plan_compare_dpc_milliman_contacts mc
         join fdw_member_doc.patients p on mc.patient_id = p.id
         left join fdw_member_doc.patient_addresses pa on pa.patient_id = p.id and pa.type = 'home'
         join fdw_member_doc.patient_referring_partners prp on p.id = prp.patient_id and prp."primary"
         join fdw_member_doc.referring_partners rp on rp.id = prp.referring_partner_id
         left join fdw_member_doc.care_teams ct on ct.id = p.care_team_id
         left join fdw_member_doc.patient_contacts pc
                   on p.id = pc.patient_id and pc.relationship = 'physician' and pc.is_primary
         left join fdw_member_doc.contacts c on pc.contact_id = c.id
         left join fdw_member_doc.msh_physicians mp on mp.contact_id = c.id
         left join fdw_member_doc.msh_physicians mp_primary on rp.default_physician_id = mp_primary.id
order by rp.id, patient_id;



DROP TABLE IF EXISTS _milliman_specialists_ideon;

CREATE TEMP TABLE _milliman_specialists_ideon AS

select mpcp.patient_id,
       array_agg(msp.npi)                    specialist_npis,
       array_agg(msp.id)                     specialist_ids,
       json_object_agg(msp.id::int, msp.npi) specialist_ids_npis
from _milliman_pc_pcp_for_ideon mpcp
         join _milliman_pc_specialty_providers_for_ideon msp on msp.memberid::int = mpcp.patient_id
group by 1;


DROP TABLE IF EXISTS _milliman_pt_hospitals_ideon;

CREATE TEMP TABLE _milliman_pt_hospitals_ideon AS

select mpcp.patient_id,
       array_agg(msp.npi)                    hospital_specialist_npis,
       array_agg(msp.id)                     hospital_specialist_ids,
       json_object_agg(msp.id::int, msp.npi) hospital_specialist_ids_npis
from _milliman_pc_pcp_for_ideon mpcp
         join _milliman_pc_specialty_hospitals_for_ideon msp on msp.memberid::int = mpcp.patient_id
group by 1;



--
-- select *
-- from _milliman_pc_pcp_for_ideon msp
--          left join _milliman_specialists_ideon msi on msi.patient_id = msp.patient_id
--          left join _milliman_pt_hospitals_ideon mpi on mpi.patient_id = msp.patient_id
-- where msi.patient_id is null
--   and referring_partner_id in (30, 135, 138);
--
-- select referring_partner_id, referring_partner, count(1)
-- from _milliman_pc_pcp_for_ideon msp
-- where referring_partner_id in (30, 135, 138)
-- group by 1, 2;

--
-- insert into fdw_member_doc_stage.plan_compare_patient_ideon_request(patient_id,
--                                                                     full_name,
--                                                                     patient_zipcode,
--                                                                     referring_partner_id,
--                                                                     referring_partner,
--                                                                     referring_partner_zip,
--                                                                     primary_pcp,
--                                                                     pcp_msh_physician_id,
--                                                                     primary_pcp_npi,
--                                                                     partner_physician_msh_physician_id,
--                                                                     partner_physician,
--                                                                     partner_physician_npi,
--                                                                     specialist_ids,
--                                                                     specialist_npis,
--                                                                     hospital_specialist_ids,
--                                                                     hospital_specialist_npis,
--                                                                     inbound_file_id)
select distinct msp.patient_id,
                full_name,
                patient_zipcode,
                referring_partner_id,
                referring_partner,
                referring_partner_zip,
                primary_pcp,
                pcp_msh_physician_id,
                primary_pcp_npi,
                partner_physician_msh_physician_id,
                partner_physician,
                partner_physician_npi,
                msi.specialist_ids,
                msi.specialist_npis,
                mpi.hospital_specialist_ids,
                mpi.hospital_specialist_npis,
                --todo pull from inbound file
10 inbound_file_id
from _milliman_pc_pcp_for_ideon msp
         left join _milliman_specialists_ideon msi on msi.patient_id = msp.patient_id
         left join _milliman_pt_hospitals_ideon mpi on mpi.patient_id = msp.patient_id;



----- QA section
select memberid, inbound_file_id, count(1)
from raw.milliman_plan_compare_providers
where inbound_file_id = 10
group by 1, 2;

--4437

select count(distinct (patient_id))
from prd.plan_compare_dpc_milliman_contacts mc;

select count(distinct (memberid))
from raw.milliman_plan_compare_providers pcp
where inbound_file_id = 10;

select patient_id, count(referring_partner_id)
from _milliman_pc_pcp_for_ideon
group by 1
having count(referring_partner_id) > 1;



select distinct patient_id, rp.memberid
from prd.plan_compare_dpc_milliman_contacts mc
         left join raw.milliman_plan_compare_providers rp on rp.memberid::int = mc.patient_id
where rp.memberid::int is null
  and inbound_file_id = 10;

select distinct memberid, patient_id
from raw.milliman_plan_compare_providers rp
         left join prd.plan_compare_dpc_milliman_contacts mc on rp.memberid::int = mc.patient_id
where mc.patient_id is null
  and inbound_file_id = 10;

select *
from fdw_member_doc.patient_addresses pa
where pa.patient_id = 656
  and pa.type = 'home';

select * from fdw_member_doc_stage.plan_compare_patient_ideon_request;
