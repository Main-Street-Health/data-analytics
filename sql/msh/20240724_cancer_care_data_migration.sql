INSERT
INTO
    cc_locations (id, name, referring_partner_id, created_by_id, updated_by_id, inserted_at, updated_at)
SELECT id , name , referring_partner_id , created_by_id , updated_by_id , inserted_at , updated_at FROM rn_locations;

SELECT pg_catalog.setval('public.cc_locations_id_seq', (select max(id) from cc_locations), true);

INSERT
INTO
    cc_patients (id, first_name, last_name, dob, phone, insurance_carrier_name, insurance_type_other, insurance_type_key,
                 patient_id, created_by_id, updated_by_id)
SELECT id, first_name, last_name, dob, phone, insurance_carrier_name, insurance_type_other, insurance_type_key,
                 patient_id, created_by_id, updated_by_id FROM rn_patients;
SELECT pg_catalog.setval('public.cc_patients_id_seq', (select max(id) from cc_patients ), true);

INSERT
INTO
    cc_facilities (id, name, phone, ext, fax, address_line_1, address_line_2, city, state, zip, website, npi,
                   inserted_at, updated_at)
SELECT id, name, phone, ext, fax, address_line_1, address_line_2, city, state, zip, website, npi,
                   inserted_at, updated_at
FROM rn_facilities f
where f.name = 'Rhea Cancer Center'
;
SELECT pg_catalog.setval('public.cc_facilities_id_seq', (select max(id) from cc_facilities ), true);

INSERT
INTO
    cc_providers (id, first_name, last_name, npi, created_by_id, updated_by_id, msh_physician_id)
SELECT id, first_name, last_name, npi, created_by_id, updated_by_id, msh_physician_id
FROM rn_providers p
where (first_name = 'Stephen' and last_name = 'Schleicher')
or (exists(select 1 from rn_patient_referrals pr where pr.referring_provider_id = p.id))
;
SELECT pg_catalog.setval('public.cc_providers_id_seq', (select max(id) from cc_providers ), true);
-- delete from cc_providers;


INSERT
INTO
    cc_configs (name, value, inserted_at, updated_at)
SELECT name, value, inserted_at, updated_at FROM rn_configs;

INSERT
INTO
    cc_facility_providers (id, is_preferred, scheduling_notes, pre_visit_notes, post_visit_notes, cc_facility_id, cc_provider_id, inserted_at, updated_at)
SELECT
    id, is_preferred, scheduling_notes, pre_visit_notes, post_visit_notes, rn_facility_id, rn_provider_id, inserted_at, updated_at
FROM rn_facility_providers p
where exists(select 1 from cc_providers cc where cc.id = p.rn_provider_id)
and exists(select 1 from cc_facilities f where f.id = p.rn_facility_id)
;
SELECT pg_catalog.setval('public.cc_facility_providers_id_seq', (select max(id) from cc_facility_providers ), true);
-- delete from cc_facility_providers;

INSERT
INTO
    cc_patient_referrals (id, is_active, is_preferred, referral_date, notes, reason, priority, referral_status_key,
                          cc_patient_id, cc_referred_by_location_id, cc_facility_id, referring_provider_id,
                          specialist_provider_id, created_by_id, updated_by_id, assigned_to_id, inserted_at, updated_at)
SELECT id, is_active, is_preferred, referral_date, notes, reason, priority, referral_status_key,
                          rn_patient_id, rn_referred_by_location_id, rn_facility_id, referring_provider_id,
                          specialist_provider_id, created_by_id, updated_by_id, assigned_to_id, inserted_at, updated_at
FROM rn_patient_referrals pr
where exists(select 1 from cc_providers cc where cc.id = pr.specialist_provider_id)
  and exists(select 1 from cc_facilities f where f.id = pr.rn_facility_id)
;
SELECT pg_catalog.setval('public.cc_patient_referrals_id_seq', (select max(id) from cc_patient_referrals ), true);
-- delete from cc_patient_referrals;

INSERT
INTO
    cc_referral_status_periods (id, start_why, start_at, end_at, cc_patient_referral_id, referral_status_key,
                                inserted_at, updated_at)
SELECT id, start_why, start_at, end_at, rn_patient_referral_id, referral_status_key,
                                inserted_at, updated_at
FROM rn_pr_status_periods p
where exists(select 1 from rn_patient_referrals pr where pr.id = p.rn_patient_referral_id)
;
SELECT pg_catalog.setval('public.cc_referral_status_periods_id_seq', (select max(id) from cc_referral_status_periods ), true);

INSERT
INTO
    cc_referral_activities (id, description, cc_referral_status_period_id, cc_patient_referral_id, activity_by_id, activity_type_key, inserted_at, updated_at)
SELECT id, description, rn_pr_status_period_id, rn_patient_referral_id, activity_by_id, activity_type_key, inserted_at, updated_at FROM rn_pr_activities;
SELECT pg_catalog.setval('public.cc_referral_activities_id_seq', (select max(id) from public.cc_referral_activities ), true);

INSERT
INTO
    cc_referral_wfs (id, appointment_date, pre_visit_docs_sent, pre_visit_docs_sent_at, pre_visit_docs_verified,
                     pre_visit_docs_verified_at, notes, scheduling_status_key, cc_patient_referral_id, assigned_to_id,
                     created_by_id, updated_by_id, inserted_at, updated_at)
SELECT id, appointment_date, pre_visit_docs_sent, pre_visit_docs_sent_at, pre_visit_docs_verified,
       pre_visit_docs_verified_at, notes, scheduling_status_key, rn_patient_referral_id, assigned_to_id,
       created_by_id, updated_by_id, inserted_at, updated_at FROM rn_pr_internal_wfs;
SELECT pg_catalog.setval('public.cc_referral_wfs_id_seq', (select max(id) from public.cc_referral_wfs ), true);

INSERT
INTO
    cc_referral_nav_blocks (id, appointment_date, pre_visit_docs_sent, pre_visit_docs_sent_at, pre_visit_docs_verified,
                            pre_visit_docs_verified_at, notes, scheduling_status_key, cc_referral_wf_id,
                            cc_patient_referral_id, assigned_to_id, created_by_id, updated_by_id, inserted_at,
                            updated_at)
SELECT id, appointment_date, pre_visit_docs_sent, pre_visit_docs_sent_at, pre_visit_docs_verified,
                            pre_visit_docs_verified_at, notes, scheduling_status_key, rn_pr_internal_wf_id,
                            rn_patient_referral_id, assigned_to_id, created_by_id, updated_by_id, inserted_at,
                            updated_at FROM rn_pr_internal_nav_blocks;
SELECT pg_catalog.setval('public.cc_referral_nav_blocks_id_seq', (select max(id) from public.cc_referral_nav_blocks ), true);

INSERT
INTO
    cc_calls (id, call_id, cc_patient_referral_id, inserted_at, updated_at)
SELECT id, call_id, rn_patient_referral_id, inserted_at, updated_at FROM rn_calls;
SELECT pg_catalog.setval('public.cc_calls_id_seq', (select max(id) from public.cc_calls ), true);


-- ?????? not used anymore??
-- SELECT * FROM rn_facility_locations;
-- SELECT * FROM rn_facility_specialties;
-- SELECT * FROM rn_insurance_carriers;
-- SELECT * FROM rn_pr_external_nav_blocks;
-- SELECT * FROM rn_pr_external_wfs;
-- SELECT * FROM rn_pr_scheduled_events;
-- SELECT * FROM rn_pr_searches;
