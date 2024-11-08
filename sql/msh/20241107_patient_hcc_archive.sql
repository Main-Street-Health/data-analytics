SELECT * FROM patient_hcc_captured_icd10s order by id desc;
SELECT * FROM patient_hccs;
SELECT * FROM patient_hcc_recapture_icd10s order by id desc;
SELECT * FROM hcc_suspect_reasons order by id desc;

create table archive.patient_hcc_captured_icd10s as
SELECT * FROM patient_hcc_captured_icd10s order by id;
create table archive.patient_hccs as
SELECT * FROM patient_hccs order by id ;
create table archive.patient_hcc_recapture_icd10s as
SELECT * FROM patient_hcc_recapture_icd10s order by id ;
create table archive.hcc_suspect_reasons as
SELECT * FROM hcc_suspect_reasons order by id ;
create table archive.hcc_capture_statuses as
select * from public.hcc_capture_statuses order by id;
create table archive.hcc_notes as
select * from public.hcc_notes order by id;


drop table patient_hcc_captured_icd10s;
drop table patient_hcc_recapture_icd10s;
drop table hcc_suspect_reasons;
drop table hcc_notes;
drop table hcc_capture_statuses;
drop table patient_hccs;

