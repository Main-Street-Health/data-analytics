CREATE PROCEDURE sp_load_ability_inbound()
    LANGUAGE plpgsql
AS
$$
begin
    -- Need to map cb file fields to msh file fields
    INSERT
    INTO
        ability.ability_all (id, inbound_file_id, patient_visit_number, patient_first_name, patient_last_name,
                             patient_middle_name, patient_suffix, patient_gender, patient_address_1, patient_address_2,
                             patient_city, patient_state, patient_zip, date_of_service, facility_name, patient_dob,
                             patient_ssn, payor_name, payor_code, status, subscriber_id, policy_start_date,
                             policy_end_date, added_date, date_of_death, health_insurance_claim_number, grp_number,
                             plan_sponsor, medicare_part_a_start, medicare_part_a_end, medicare_part_b_start,
                             medicare_part_b_end, additional_coverage, additional_coverage_policy_number,
                             additional_coverage_contact_number, additional_coverage_start_date,
                             additional_coverage_end_date, additional_coverage_address_1, additional_coverage_address_2,
                             additional_coverage_city, additional_coverage_state, additional_coverage_zip,
                             received_first_name, received_last_name, received_suffix, received_middle_name,
                             received_dob, received_ssn, received_gender, received_address_1, received_address_2,
                             received_city, received_state, received_zip, dep_first_name, dep_last_name, dep_suffix,
                             dep_middle_name, dep_dob, dep_ssn, dep_gender, dep_address_1, dep_address_2, dep_city,
                             dep_state, dep_zip, medicare_replacement_payor, medicare_replacement_plan_name,
                             medicare_supplemental_plan_name, hospice_start_date, hospice_end_date, hospice_npi,
                             payor_address_1, payor_address_2, payor_city, payor_state, payor_zip, payor_identifier,
                             county_code, ipa_identifier, ipa_description, tpa_name, tpa_subscriber_id,
                             electronic_verification_code, plan_network_identification_number,
                             managed_care_subscriber_id, qualified_medicare_beneficiary, medicare_part_a_only,
                             insurance_plan, insurance_type, deductible, deductible_remaining, out_of_pocket,
                             out_of_pocketremaining, life_time_limit, life_time_limit_remaining, spend_down,
                             spend_down_amount, managed_care_plan, managed_care_program, related_entities,
                             medicare_a_deductible, medicare_a_deductible_time_period, medicare_a_deductible_remaining,
                             medicare_b_deductible, medicare_b_deductible_time_period, medicare_b_deductible_remaining,
                             medicare_subscriber_id, medicaid_hic_number, mismatch_first_name, mismatch_last_name,
                             mismatch_ssn, mismatch_dob, mismatch_state, confidence_score, confidence_score_reason,
                             payor_name_2, payor_code_2, status_2, subscriber_id_2, policy_start_date_2,
                             policy_end_date_2, added_date_2, date_of_death_2, health_insurance_claim_number_2,
                             grp_number_2, plan_sponsor_2, medicare_part_a_start_2, medicare_part_a_end_2,
                             medicare_part_b_start_2, medicare_part_b_end_2, additional_coverage_2,
                             additional_coverage_policy_number_2, additional_coverage_contact_number_2,
                             additional_coverage_start_date_2, additional_coverage_end_date_2,
                             additional_coverage_address_1_2, additional_coverage_address_2_2,
                             additional_coverage_city_2, additional_coverage_state_2, additional_coverage_zip_2,
                             received_first_name_2, received_last_name_2, received_suffix_2, received_middle_name_2,
                             received_dob_2, received_ssn_2, received_gender_2, received_address_1_2,
                             received_address_2_2, received_city_2, received_state_2, received_zip_2, dep_first_name_2,
                             dep_last_name_2, dep_suffix_2, dep_middle_name_2, dep_dob_2, dep_ssn_2, dep_gender_2,
                             dep_address_1_2, dep_address_2_2, dep_city_2, dep_state_2, dep_zip_2,
                             medicare_replacement_payor_2, medicare_replacement_plan_name_2,
                             medicare_supplemental_plan_name_2, hospice_start_date_2, hospice_end_date_2, hospice_npi_2,
                             payor_address_1_2, payor_address_2_2, payor_city_2, payor_state_2, payor_zip_2,
                             payor_identifier_2, county_code_2, ipa_identifier_2, ipa_description_2, tpa_name_2,
                             tpa_subscriber_id_2, electronic_verification_code_2, plan_network_identification_number_2,
                             managed_care_subscriber_id_2, qualified_medicare_beneficiary_2, medicare_part_a_only_2,
                             insurance_plan_2, insurance_type_2, deductible_2, deductible_remaining_2, out_of_pocket_2,
                             out_of_pocketremaining_2, life_time_limit_2, life_time_limit_remaining_2, spend_down_2,
                             spend_down_amount_2, managed_care_plan_2, managed_care_program_2, related_entities_2,
                             medicare_a_deductible_2, medicare_a_deductible_time_period_2,
                             medicare_a_deductible_remaining_2, medicare_b_deductible_2,
                             medicare_b_deductible_time_period_2, medicare_b_deductible_remaining_2,
                             medicare_subscriber_id_2, medicaid_hic_number_2, mismatch_first_name_2,
                             mismatch_last_name_2, mismatch_ssn_2, mismatch_dob_2, mismatch_state_2, confidence_score_2,
                             confidence_score_reason_2, payor_name_3, payor_code_3, status_3, subscriber_id_3,
                             policy_start_date_3, policy_end_date_3, added_date_3, date_of_death_3,
                             health_insurance_claim_number_3, grp_number_3, plan_sponsor_3, medicare_part_a_start_3,
                             medicare_part_a_end_3, medicare_part_b_start_3, medicare_part_b_end_3,
                             additional_coverage_3, additional_coverage_policy_number_3,
                             additional_coverage_contact_number_3, additional_coverage_start_date_3,
                             additional_coverage_end_date_3, additional_coverage_address_1_3,
                             additional_coverage_address_2_3, additional_coverage_city_3, additional_coverage_state_3,
                             additional_coverage_zip_3, received_first_name_3, received_last_name_3, received_suffix_3,
                             received_middle_name_3, received_dob_3, received_ssn_3, received_gender_3,
                             received_address_1_3, received_address_2_3, received_city_3, received_state_3,
                             received_zip_3, dep_first_name_3, dep_last_name_3, dep_suffix_3, dep_middle_name_3,
                             dep_dob_3, dep_ssn_3, dep_gender_3, dep_address_1_3, dep_address_2_3, dep_city_3,
                             dep_state_3, dep_zip_3, medicare_replacement_payor_3, medicare_replacement_plan_name_3,
                             medicare_supplemental_plan_name_3, hospice_start_date_3, hospice_end_date_3, hospice_npi_3,
                             payor_address_1_3, payor_address_2_3, payor_city_3, payor_state_3, payor_zip_3,
                             payor_identifier_3, county_code_3, ipa_identifier_3, ipa_description_3, tpa_name_3,
                             tpa_subscriber_id_3, electronic_verification_code_3, plan_network_identification_number_3,
                             managed_care_subscriber_id_3, qualified_medicare_beneficiary_3, medicare_part_a_only_3,
                             insurance_plan_3, insurance_type_3, deductible_3, deductible_remaining_3, out_of_pocket_3,
                             out_of_pocketremaining_3, life_time_limit_3, life_time_limit_remaining_3, spend_down_3,
                             spend_down_amount_3, managed_care_plan_3, managed_care_program_3, related_entities_3,
                             medicare_a_deductible_3, medicare_a_deductible_time_period_3,
                             medicare_a_deductible_remaining_3, medicare_b_deductible_3,
                             medicare_b_deductible_time_period_3, medicare_b_deductible_remaining_3,
                             medicare_subscriber_id_3, medicaid_hic_number_3, mismatch_first_name_3,
                             mismatch_last_name_3, mismatch_ssn_3, mismatch_dob_3, mismatch_state_3, confidence_score_3,
                             confidence_score_reason_3)
    SELECT
        id
      , inbound_file_id
      , NULL                                 patient_visit_number
      , cb.firstname                         patient_first_name
      , cb.lastname                          patient_last_name
      , cb.middlename                        patient_middle_name
      , cb.suffix                            patient_suffix
      , cb.gender                            patient_gender
      , cb.address1                          patient_address_1
      , cb.address2                          patient_address_2
      , cb.city                              patient_city
      , cb.state                             patient_state
      , cb.zip                               patient_zip
      , cb.dateofservice                     date_of_service
      , cb.facilityname                      facility_name
      , cb.dob                               patient_dob
      , cb.ssn                               patient_ssn
      , cb.payername                         payor_name
      , cb.payercode                         payor_code
      , cb.status                            status
      , cb.subscriberid                      subscriber_id
      , cb.policystartdate                   policy_start_date
      , cb.policyenddate                     policy_end_date
      , cb.addeddate                         added_date
      , cb.dateofdeath                       date_of_death
      , cb.healthinsuranceclaimnumber        health_insurance_claim_number
      , cb.grpnumber                         grp_number
      , cb.plansponsor                       plan_sponsor
      , cb.medicarepartastart                medicare_part_a_start
      , cb.medicarepartaend                  medicare_part_a_end
      , cb.medicarepartbstart                medicare_part_b_start
      , cb.medicarepartbend                  medicare_part_b_end
      , cb.additionalcoverage                additional_coverage
      , cb.additionalcoveragepolicynumber    additional_coverage_policy_number
      , cb.additionalcoveragecontactnumber   additional_coverage_contact_number
      , cb.additionalcoveragestartdate       additional_coverage_start_date
      , cb.additionalcoverageenddate         additional_coverage_end_date
      , cb.additionalcoverageaddress         additional_coverage_address_1
      , cb.additionalcoverageaddress2        additional_coverage_address_2
      , cb.additionalcoveragecity            additional_coverage_city
      , cb.additionalcoveragestate           additional_coverage_state
      , cb.additionalcoveragezip             additional_coverage_zip
      , cb.receivedfirstname                 received_first_name
      , cb.receivedlastname                  received_last_name
      , cb.receivedsuffix                    received_suffix
      , cb.receivedmiddlename                received_middle_name
      , cb.receiveddob                       received_dob
      , cb.receivedssn                       received_ssn
      , cb.receivedgender                    received_gender
      , cb.receivedaddress1                  received_address_1
      , cb.receivedaddress2                  received_address_2
      , cb.receivedcity                      received_city
      , cb.receivedstate                     received_state
      , cb.receivedzip                       received_zip
      , cb.depfirstname                      dep_first_name
      , cb.deplastname                       dep_last_name
      , cb.depsuffix                         dep_suffix
      , cb.depmiddlename                     dep_middle_name
      , cb.depdob                            dep_dob
      , cb.depssn                            dep_ssn
      , cb.depgender                         dep_gender
      , cb.depaddress1                       dep_address_1
      , cb.depaddress2                       dep_address_2
      , cb.depcity                           dep_city
      , cb.depstate                          dep_state
      , cb.depzip                            dep_zip
      , cb.medicarereplacementpayor          medicare_replacement_payor
      , cb.medicarereplacementplanname       medicare_replacement_plan_name
      , cb.medicaresupplementalplanname      medicare_supplemental_plan_name
      , cb.hospicestartdate                  hospice_start_date
      , cb.hospiceenddate                    hospice_end_date
      , cb.hospicenpi                        hospice_npi
      , cb.payoraddress1                     payor_address_1
      , cb.payoraddress2                     payor_address_2
      , cb.payorcity                         payor_city
      , cb.payorstate                        payor_state
      , cb.payorzip                          payor_zip
      , cb.payoridentifier                   payor_identifier
      , cb.countycode                        county_code
      , cb.ipaidentifier                     ipa_identifier
      , cb.ipadescription                    ipa_description
      , cb.tpaname                           tpa_name
      , cb.tpasubscriberid                   tpa_subscriber_id
      , cb.electronicverificationcode        electronic_verification_code
      , cb.plannetworkidentificationnumber   plan_network_identification_number
      , cb.managedcaresubscriberid           managed_care_subscriber_id
      , NULL                                 qualified_medicare_beneficiary ------------- ???????????????????????????
      , cb.medicarepartaonly                 medicare_part_a_only
      , cb.insuranceplan                     insurance_plan
      , cb.insurancetype                     insurance_type
      , cb.deductible                        deductible
      , cb.deductibleremaining               deductible_remaining
      , cb.outofpocket                       out_of_pocket
      , cb.outofpocketremaining              out_of_pocketremaining
      , cb.lifetimelimit                     life_time_limit
      , cb.lifetimelimitremaining            life_time_limit_remaining
      , cb.spenddown                         spend_down
      , cb.spenddownamount                   spend_down_amount
      , cb.managedcareplan                   managed_care_plan
      , cb.managedcareprogram                managed_care_program
      , cb.relatedentities                   related_entities
      , cb.medicareadeductible               medicare_a_deductible
      , cb.medicareadeductibletimeperiod     medicare_a_deductible_time_period
      , cb.medicareadeductibleremaining      medicare_a_deductible_remaining
      , cb.medicarebdeductible               medicare_b_deductible
      , cb.medicarebdeductibletimeperiod     medicare_b_deductible_time_period
      , cb.medicarebdeductibleremaining      medicare_b_deductible_remaining
      , cb.medicaresubscriberid              medicare_subscriber_id
      , cb.medicaidhicnumber                 medicaid_hic_number
      , cb.mismatchfirstname                 mismatch_first_name
      , cb.mismatchlastname                  mismatch_last_name
      , cb.mismatchssn                       mismatch_ssn
      , cb.mismatchdob                       mismatch_dob
      , cb.mismatchstate                     mismatch_state
      , cb.confidencescore                   confidence_score
      , cb.confidencescorereason             confidence_score_reason
      , cb.payername_2                       payor_name_2
      , cb.payercode_2                       payor_code_2
      , cb.status_2                          status_2
      , cb.subscriberid_2                    subscriber_id_2
      , cb.policystartdate_2                 policy_start_date_2
      , cb.policyenddate_2                   policy_end_date_2
      , cb.addeddate_2                       added_date_2
      , cb.dateofdeath_2                     date_of_death_2
      , cb.healthinsuranceclaimnumber_2      health_insurance_claim_number_2
      , cb.grpnumber_2                       grp_number_2
      , cb.plansponsor_2                     plan_sponsor_2
      , cb.medicarepartastart_2              medicare_part_a_start_2
      , cb.medicarepartaend_2                medicare_part_a_end_2
      , cb.medicarepartbstart_2              medicare_part_b_start_2
      , cb.medicarepartbend_2                medicare_part_b_end_2
      , cb.additionalcoverage_2              additional_coverage_2
      , cb.additionalcoveragepolicynumber_2  additional_coverage_policy_number_2
      , cb.additionalcoveragecontactnumber_2 additional_coverage_contact_number_2
      , cb.additionalcoveragestartdate_2     additional_coverage_start_date_2
      , cb.additionalcoverageenddate_2       additional_coverage_end_date_2
      , cb.additionalcoverageaddress_2       additional_coverage_address_1_2
      , cb.additionalcoverageaddress2_2      additional_coverage_address_2_2
      , cb.additionalcoveragecity_2          additional_coverage_city_2
      , cb.additionalcoveragestate_2         additional_coverage_state_2
      , cb.additionalcoveragezip_2           additional_coverage_zip_2
      , cb.receivedfirstname_2               received_first_name_2
      , cb.receivedlastname_2                received_last_name_2
      , cb.receivedsuffix_2                  received_suffix_2
      , cb.receivedmiddlename_2              received_middle_name_2
      , cb.receiveddob_2                     received_dob_2
      , cb.receivedssn_2                     received_ssn_2
      , cb.receivedgender_2                  received_gender_2
      , cb.receivedaddress1_2                received_address_1_2
      , cb.receivedaddress2_2                received_address_2_2
      , cb.receivedcity_2                    received_city_2
      , cb.receivedstate_2                   received_state_2
      , cb.receivedzip_2                     received_zip_2
      , cb.depfirstname_2                    dep_first_name_2
      , cb.deplastname_2                     dep_last_name_2
      , cb.depsuffix_2                       dep_suffix_2
      , cb.depmiddlename_2                   dep_middle_name_2
      , cb.depdob_2                          dep_dob_2
      , cb.depssn_2                          dep_ssn_2
      , cb.depgender_2                       dep_gender_2
      , cb.depaddress1_2                     dep_address_1_2
      , cb.depaddress2_2                     dep_address_2_2
      , cb.depcity_2                         dep_city_2
      , cb.depstate_2                        dep_state_2
      , cb.depzip_2                          dep_zip_2
      , cb.medicarereplacementpayor_2        medicare_replacement_payor_2
      , cb.medicarereplacementplanname_2     medicare_replacement_plan_name_2
      , cb.medicaresupplementalplanname_2    medicare_supplemental_plan_name_2
      , cb.hospicestartdate_2                hospice_start_date_2
      , cb.hospiceenddate_2                  hospice_end_date_2
      , cb.hospicenpi_2                      hospice_npi_2
      , cb.payoraddress1_2                   payor_address_1_2
      , cb.payoraddress2_2                   payor_address_2_2
      , cb.payorcity_2                       payor_city_2
      , cb.payorstate_2                      payor_state_2
      , cb.payorzip_2                        payor_zip_2
      , cb.payoridentifier_2                 payor_identifier_2
      , cb.countycode_2                      county_code_2
      , cb.ipaidentifier_2                   ipa_identifier_2
      , cb.ipadescription_2                  ipa_description_2
      , cb.tpaname_2                         tpa_name_2
      , cb.tpasubscriberid_2                 tpa_subscriber_id_2
      , cb.electronicverificationcode_2      electronic_verification_code_2
      , cb.plannetworkidentificationnumber_2 plan_network_identification_number_2
      , cb.managedcaresubscriberid_2         managed_care_subscriber_id_2
      , NULL                                 qualified_medicare_beneficiary_2 ----------------------- ????????????????????????????????????????/
      , cb.medicarepartaonly_2               medicare_part_a_only_2
      , cb.insuranceplan_2                   insurance_plan_2
      , cb.insurancetype_2                   insurance_type_2
      , cb.deductible_2                      deductible_2
      , cb.deductibleremaining_2             deductible_remaining_2
      , cb.outofpocket_2                     out_of_pocket_2
      , cb.outofpocketremaining_2            out_of_pocketremaining_2
      , cb.lifetimelimit_2                   life_time_limit_2
      , cb.lifetimelimitremaining_2          life_time_limit_remaining_2
      , cb.spenddown_2                       spend_down_2
      , cb.spenddownamount_2                 spend_down_amount_2
      , cb.managedcareplan_2                 managed_care_plan_2
      , cb.managedcareprogram_2              managed_care_program_2
      , cb.relatedentities_2                 related_entities_2
      , cb.medicareadeductible_2             medicare_a_deductible_2
      , cb.medicareadeductibletimeperiod_2   medicare_a_deductible_time_period_2
      , cb.medicareadeductibleremaining_2    medicare_a_deductible_remaining_2
      , cb.medicarebdeductible_2             medicare_b_deductible_2
      , cb.medicarebdeductibletimeperiod_2   medicare_b_deductible_time_period_2
      , cb.medicarebdeductibleremaining_2    medicare_b_deductible_remaining_2
      , cb.medicaresubscriberid_2            medicare_subscriber_id_2
      , cb.medicaidhicnumber_2               medicaid_hic_number_2
      , cb.mismatchfirstname_2               mismatch_first_name_2
      , cb.mismatchlastname_2                mismatch_last_name_2
      , cb.mismatchssn_2                     mismatch_ssn_2
      , cb.mismatchdob_2                     mismatch_dob_2
      , cb.mismatchstate_2                   mismatch_state_2
      , cb.confidencescore_2                 confidence_score_2
      , cb.confidencescorereason_2           confidence_score_reason_2
      , cb.payername_3                       payor_name_3
      , cb.payercode_3                       payor_code_3
      , cb.status_3                          status_3
      , cb.subscriberid_3                    subscriber_id_3
      , cb.policystartdate_3                 policy_start_date_3
      , cb.policyenddate_3                   policy_end_date_3
      , cb.addeddate_3                       added_date_3
      , cb.dateofdeath_3                     date_of_death_3
      , cb.healthinsuranceclaimnumber_3      health_insurance_claim_number_3
      , cb.grpnumber_3                       grp_number_3
      , cb.plansponsor_3                     plan_sponsor_3
      , cb.medicarepartastart_3              medicare_part_a_start_3
      , cb.medicarepartaend_3                medicare_part_a_end_3
      , cb.medicarepartbstart_3              medicare_part_b_start_3
      , cb.medicarepartbend_3                medicare_part_b_end_3
      , cb.additionalcoverage_3              additional_coverage_3
      , cb.additionalcoveragepolicynumber_3  additional_coverage_policy_number_3
      , cb.additionalcoveragecontactnumber_3 additional_coverage_contact_number_3
      , cb.additionalcoveragestartdate_3     additional_coverage_start_date_3
      , cb.additionalcoverageenddate_3       additional_coverage_end_date_3
      , cb.additionalcoverageaddress_3       additional_coverage_address_1_3
      , cb.additionalcoverageaddress2_3      additional_coverage_address_2_3
      , cb.additionalcoveragecity_3          additional_coverage_city_3
      , cb.additionalcoveragestate_3         additional_coverage_state_3
      , cb.additionalcoveragezip_3           additional_coverage_zip_3
      , cb.receivedfirstname_3               received_first_name_3
      , cb.receivedlastname_3                received_last_name_3
      , cb.receivedsuffix_3                  received_suffix_3
      , cb.receivedmiddlename_3              received_middle_name_3
      , cb.receiveddob_3                     received_dob_3
      , cb.receivedssn_3                     received_ssn_3
      , cb.receivedgender_3                  received_gender_3
      , cb.receivedaddress1_3                received_address_1_3
      , cb.receivedaddress2_3                received_address_2_3
      , cb.receivedcity_3                    received_city_3
      , cb.receivedstate_3                   received_state_3
      , cb.receivedzip_3                     received_zip_3
      , cb.depfirstname_3                    dep_first_name_3
      , cb.deplastname_3                     dep_last_name_3
      , cb.depsuffix_3                       dep_suffix_3
      , cb.depmiddlename_3                   dep_middle_name_3
      , cb.depdob_3                          dep_dob_3
      , cb.depssn_3                          dep_ssn_3
      , cb.depgender_3                       dep_gender_3
      , cb.depaddress1_3                     dep_address_1_3
      , cb.depaddress2_3                     dep_address_2_3
      , cb.depcity_3                         dep_city_3
      , cb.depstate_3                        dep_state_3
      , cb.depzip_3                          dep_zip_3
      , cb.medicarereplacementpayor_3        medicare_replacement_payor_3
      , cb.medicarereplacementplanname_3     medicare_replacement_plan_name_3
      , cb.medicaresupplementalplanname_3    medicare_supplemental_plan_name_3
      , cb.hospicestartdate_3                hospice_start_date_3
      , cb.hospiceenddate_3                  hospice_end_date_3
      , cb.hospicenpi_3                      hospice_npi_3
      , cb.payoraddress1_3                   payor_address_1_3
      , cb.payoraddress2_3                   payor_address_2_3
      , cb.payorcity_3                       payor_city_3
      , cb.payorstate_3                      payor_state_3
      , cb.payorzip_3                        payor_zip_3
      , cb.payoridentifier_3                 payor_identifier_3
      , cb.countycode_3                      county_code_3
      , cb.ipaidentifier_3                   ipa_identifier_3
      , cb.ipadescription_3                  ipa_description_3
      , cb.tpaname_3                         tpa_name_3
      , cb.tpasubscriberid_3                 tpa_subscriber_id_3
      , cb.electronicverificationcode_3      electronic_verification_code_3
      , cb.plannetworkidentificationnumber_3 plan_network_identification_number_3
      , cb.managedcaresubscriberid_3         managed_care_subscriber_id_3
      , NULL                                 qualified_medicare_beneficiary_3
      , cb.medicarepartaonly_3               medicare_part_a_only_3
      , cb.insuranceplan_3                   insurance_plan_3
      , cb.insurancetype_3                   insurance_type_3
      , cb.deductible_3                      deductible_3
      , cb.deductibleremaining_3             deductible_remaining_3
      , cb.outofpocket_3                     out_of_pocket_3
      , cb.outofpocketremaining_3            out_of_pocketremaining_3
      , cb.lifetimelimit_3                   life_time_limit_3
      , cb.lifetimelimitremaining_3          life_time_limit_remaining_3
      , cb.spenddown_3                       spend_down_3
      , cb.spenddownamount_3                 spend_down_amount_3
      , cb.managedcareplan_3                 managed_care_plan_3
      , cb.managedcareprogram_3              managed_care_program_3
      , cb.relatedentities_3                 related_entities_3
      , cb.medicareadeductible_3             medicare_a_deductible_3
      , cb.medicareadeductibletimeperiod_3   medicare_a_deductible_time_period_3
      , cb.medicareadeductibleremaining_3    medicare_a_deductible_remaining_3
      , cb.medicarebdeductible_3             medicare_b_deductible_3
      , cb.medicarebdeductibletimeperiod_3   medicare_b_deductible_time_period_3
      , cb.medicarebdeductibleremaining_3    medicare_b_deductible_remaining_3
      , cb.medicaresubscriberid_3            medicare_subscriber_id_3
      , cb.medicaidhicnumber_3               medicaid_hic_number_3
      , cb.mismatchfirstname_3               mismatch_first_name_3
      , cb.mismatchlastname_3                mismatch_last_name_3
      , cb.mismatchssn_3                     mismatch_ssn_3
      , cb.mismatchdob_3                     mismatch_dob_3
      , cb.mismatchstate_3                   mismatch_state_3
      , cb.confidencescore_3                 confidence_score_3
      , cb.confidencescorereason_3           confidence_score_reason_3
    FROM
        ability.ability_all_cb cb
    WHERE
        NOT EXISTS( SELECT 1 FROM ability.ability_all aa WHERE aa.id = cb.id )
    ;





    insert into ability.ability_inbound (inbound_file_id, raw_id, patient_id, patient_first_name, patient_last_name,
                                         patient_middle_name, patient_suffix, patient_gender, patient_address_1,
                                         patient_address_2, patient_city, patient_state, patient_zip, date_of_service,
                                         facility_name, patient_dob, patient_ssn, payor_name, payor_code, status,
                                         subscriber_id, policy_start_date, policy_end_date, added_date, date_of_death,
                                         health_insurance_claim_number, grp_number, plan_sponsor, medicare_part_a_start,
                                         medicare_part_a_end, medicare_part_b_start, medicare_part_b_end,
                                         additional_coverage, additional_coverage_policy_number,
                                         additional_coverage_contact_number, additional_coverage_start_date,
                                         additional_coverage_end_date, additional_coverage_address_1,
                                         additional_coverage_address_2, additional_coverage_city,
                                         additional_coverage_state, additional_coverage_zip, received_first_name,
                                         received_last_name, received_suffix, received_middle_name, received_dob,
                                         received_ssn, received_gender, received_address_1, received_address_2,
                                         received_city, received_state, received_zip, dep_first_name, dep_last_name,
                                         dep_suffix, dep_middle_name, dep_dob, dep_ssn, dep_gender, dep_address_1,
                                         dep_address_2, dep_city, dep_state, dep_zip, medicare_replacement_payor,
                                         medicare_replacement_plan_name, medicare_supplemental_plan_name,
                                         hospice_start_date, hospice_end_date, hospice_npi, payor_address_1,
                                         payor_address_2, payor_city, payor_state, payor_zip, payor_identifier,
                                         county_code, ipa_identifier, ipa_description, tpa_name, tpa_subscriber_id,
                                         electronic_verification_code, plan_network_identification_number,
                                         managed_care_subscriber_id, qualified_medicare_beneficiary,
                                         medicare_part_a_only, insurance_plan, insurance_type, deductible,
                                         deductible_remaining, out_of_pocket, out_of_pocketremaining, life_time_limit,
                                         life_time_limit_remaining, spend_down, spend_down_amount, managed_care_plan,
                                         managed_care_program, related_entities, medicare_a_deductible,
                                         medicare_a_deductible_time_period, medicare_a_deductible_remaining,
                                         medicare_b_deductible, medicare_b_deductible_time_period,
                                         medicare_b_deductible_remaining, medicare_subscriber_id, medicaid_hic_number,
                                         mismatch_first_name, mismatch_last_name, mismatch_ssn, mismatch_dob,
                                         mismatch_state, confidence_score, confidence_score_reason, payor_name_2,
                                         payor_code_2, status_2, subscriber_id_2, policy_start_date_2,
                                         policy_end_date_2, added_date_2, date_of_death_2,
                                         health_insurance_claim_number_2, grp_number_2, plan_sponsor_2,
                                         medicare_part_a_start_2, medicare_part_a_end_2, medicare_part_b_start_2,
                                         medicare_part_b_end_2, additional_coverage_2,
                                         additional_coverage_policy_number_2, additional_coverage_contact_number_2,
                                         additional_coverage_start_date_2, additional_coverage_end_date_2,
                                         additional_coverage_address_1_2, additional_coverage_address_2_2,
                                         additional_coverage_city_2, additional_coverage_state_2,
                                         additional_coverage_zip_2, received_first_name_2, received_last_name_2,
                                         received_suffix_2, received_middle_name_2, received_dob_2, received_ssn_2,
                                         received_gender_2, received_address_1_2, received_address_2_2, received_city_2,
                                         received_state_2, received_zip_2, dep_first_name_2, dep_last_name_2,
                                         dep_suffix_2, dep_middle_name_2, dep_dob_2, dep_ssn_2, dep_gender_2,
                                         dep_address_1_2, dep_address_2_2, dep_city_2, dep_state_2, dep_zip_2,
                                         medicare_replacement_payor_2, medicare_replacement_plan_name_2,
                                         medicare_supplemental_plan_name_2, hospice_start_date_2, hospice_end_date_2,
                                         hospice_npi_2, payor_address_1_2, payor_address_2_2, payor_city_2,
                                         payor_state_2, payor_zip_2, payor_identifier_2, county_code_2,
                                         ipa_identifier_2, ipa_description_2, tpa_name_2, tpa_subscriber_id_2,
                                         electronic_verification_code_2, plan_network_identification_number_2,
                                         managed_care_subscriber_id_2, qualified_medicare_beneficiary_2,
                                         medicare_part_a_only_2, insurance_plan_2, insurance_type_2, deductible_2,
                                         deductible_remaining_2, out_of_pocket_2, out_of_pocketremaining_2,
                                         life_time_limit_2, life_time_limit_remaining_2, spend_down_2,
                                         spend_down_amount_2, managed_care_plan_2, managed_care_program_2,
                                         related_entities_2, medicare_a_deductible_2,
                                         medicare_a_deductible_time_period_2, medicare_a_deductible_remaining_2,
                                         medicare_b_deductible_2, medicare_b_deductible_time_period_2,
                                         medicare_b_deductible_remaining_2, medicare_subscriber_id_2,
                                         medicaid_hic_number_2, mismatch_first_name_2, mismatch_last_name_2,
                                         mismatch_ssn_2, mismatch_dob_2, mismatch_state_2, confidence_score_2,
                                         confidence_score_reason_2, payor_name_3, payor_code_3, status_3,
                                         subscriber_id_3, policy_start_date_3, policy_end_date_3, added_date_3,
                                         date_of_death_3, health_insurance_claim_number_3, grp_number_3, plan_sponsor_3,
                                         medicare_part_a_start_3, medicare_part_a_end_3, medicare_part_b_start_3,
                                         medicare_part_b_end_3, additional_coverage_3,
                                         additional_coverage_policy_number_3, additional_coverage_contact_number_3,
                                         additional_coverage_start_date_3, additional_coverage_end_date_3,
                                         additional_coverage_address_1_3, additional_coverage_address_2_3,
                                         additional_coverage_city_3, additional_coverage_state_3,
                                         additional_coverage_zip_3, received_first_name_3, received_last_name_3,
                                         received_suffix_3, received_middle_name_3, received_dob_3, received_ssn_3,
                                         received_gender_3, received_address_1_3, received_address_2_3, received_city_3,
                                         received_state_3, received_zip_3, dep_first_name_3, dep_last_name_3,
                                         dep_suffix_3, dep_middle_name_3, dep_dob_3, dep_ssn_3, dep_gender_3,
                                         dep_address_1_3, dep_address_2_3, dep_city_3, dep_state_3, dep_zip_3,
                                         medicare_replacement_payor_3, medicare_replacement_plan_name_3,
                                         medicare_supplemental_plan_name_3, hospice_start_date_3, hospice_end_date_3,
                                         hospice_npi_3, payor_address_1_3, payor_address_2_3, payor_city_3,
                                         payor_state_3, payor_zip_3, payor_identifier_3, county_code_3,
                                         ipa_identifier_3, ipa_description_3, tpa_name_3, tpa_subscriber_id_3,
                                         electronic_verification_code_3, plan_network_identification_number_3,
                                         managed_care_subscriber_id_3, qualified_medicare_beneficiary_3,
                                         medicare_part_a_only_3, insurance_plan_3, insurance_type_3, deductible_3,
                                         deductible_remaining_3, out_of_pocket_3, out_of_pocketremaining_3,
                                         life_time_limit_3, life_time_limit_remaining_3, spend_down_3,
                                         spend_down_amount_3, managed_care_plan_3, managed_care_program_3,
                                         related_entities_3, medicare_a_deductible_3,
                                         medicare_a_deductible_time_period_3, medicare_a_deductible_remaining_3,
                                         medicare_b_deductible_3, medicare_b_deductible_time_period_3,
                                         medicare_b_deductible_remaining_3, medicare_subscriber_id_3,
                                         medicaid_hic_number_3, mismatch_first_name_3, mismatch_last_name_3,
                                         mismatch_ssn_3, mismatch_dob_3, mismatch_state_3, confidence_score_3,
                                         confidence_score_reason_3)
    select distinct
--     select distinct on (patient_visit_number)
        inbound_file_id,
        id                                                  raw_id,
        patient_visit_number::bigint                        patient_id,
        nullif(patient_first_name, '')                      patient_first_name,
        nullif(patient_last_name, '')                       patient_last_name,
        nullif(patient_middle_name, '')                     patient_middle_name,
        nullif(patient_suffix, '')                          patient_suffix,
        nullif(patient_gender, '')                          patient_gender,
        nullif(patient_address_1, '')                       patient_address_1,
        nullif(patient_address_2, '')                       patient_address_2,
        nullif(patient_city, '')                            patient_city,
        nullif(patient_state, '')                           patient_state,
        nullif(patient_zip, '')                             patient_zip,
        nullif(date_of_service, '')::date                   date_of_service,
        nullif(facility_name, '')                           facility_name,
        nullif(patient_dob, '')::date                       patient_dob,
        nullif(patient_ssn, '')                             patient_ssn,
        nullif(payor_name, '')                              payor_name,
        nullif(payor_code, '')                              payor_code,
        nullif(status, '')                                  status,
        nullif(subscriber_id, '')                           subscriber_id,
        nullif(policy_start_date, '')::date                 policy_start_date,
        nullif(policy_end_date, '')::date                   policy_end_date,
        nullif(added_date, '')::date                        added_date,
        nullif(date_of_death, '')::date                     date_of_death,
        nullif(health_insurance_claim_number, '')           health_insurance_claim_number,
        nullif(grp_number, '')                              grp_number,
        nullif(plan_sponsor, '')                            plan_sponsor,
        nullif(medicare_part_a_start, '')::date             medicare_part_a_start,
        nullif(medicare_part_a_end, '')::date               medicare_part_a_end,
        nullif(medicare_part_b_start, '')::date             medicare_part_b_start,
        nullif(medicare_part_b_end, '')::date               medicare_part_b_end,
        nullif(additional_coverage, '')                     additional_coverage,
        nullif(additional_coverage_policy_number, '')       additional_coverage_policy_number,
        nullif(additional_coverage_contact_number, '')      additional_coverage_contact_number,
        nullif(additional_coverage_start_date, '')::date    additional_coverage_start_date,
        nullif(additional_coverage_end_date, '')::date      additional_coverage_end_date,
        nullif(additional_coverage_address_1, '')           additional_coverage_address_1,
        nullif(additional_coverage_address_2, '')           additional_coverage_address_2,
        nullif(additional_coverage_city, '')                additional_coverage_city,
        nullif(additional_coverage_state, '')               additional_coverage_state,
        nullif(additional_coverage_zip, '')                 additional_coverage_zip,
        nullif(received_first_name, '')                     received_first_name,
        nullif(received_last_name, '')                      received_last_name,
        nullif(received_suffix, '')                         received_suffix,
        nullif(received_middle_name, '')                    received_middle_name,
        nullif(received_dob, '')::date                      received_dob,
        nullif(received_ssn, '')                            received_ssn,
        nullif(received_gender, '')                         received_gender,
        nullif(received_address_1, '')                      received_address_1,
        nullif(received_address_2, '')                      received_address_2,
        nullif(received_city, '')                           received_city,
        nullif(received_state, '')                          received_state,
        nullif(received_zip, '')                            received_zip,
        nullif(dep_first_name, '')                          dep_first_name,
        nullif(dep_last_name, '')                           dep_last_name,
        nullif(dep_suffix, '')                              dep_suffix,
        nullif(dep_middle_name, '')                         dep_middle_name,
        nullif(dep_dob, '')::date                           dep_dob,
        nullif(dep_ssn, '')                                 dep_ssn,
        nullif(dep_gender, '')                              dep_gender,
        nullif(dep_address_1, '')                           dep_address_1,
        nullif(dep_address_2, '')                           dep_address_2,
        nullif(dep_city, '')                                dep_city,
        nullif(dep_state, '')                               dep_state,
        nullif(dep_zip, '')                                 dep_zip,
        nullif(medicare_replacement_payor, '')              medicare_replacement_payor,
        nullif(medicare_replacement_plan_name, '')          medicare_replacement_plan_name,
        nullif(medicare_supplemental_plan_name, '')         medicare_supplemental_plan_name,
        nullif(hospice_start_date, '')::date                hospice_start_date,
        nullif(hospice_end_date, '')::date                  hospice_end_date,
        nullif(hospice_npi, '')                             hospice_npi,
        nullif(payor_address_1, '')                         payor_address_1,
        nullif(payor_address_2, '')                         payor_address_2,
        nullif(payor_city, '')                              payor_city,
        nullif(payor_state, '')                             payor_state,
        nullif(payor_zip, '')                               payor_zip,
        nullif(payor_identifier, '')                        payor_identifier,
        nullif(county_code, '')                             county_code,
        nullif(ipa_identifier, '')                          ipa_identifier,
        nullif(ipa_description, '')                         ipa_description,
        nullif(tpa_name, '')                                tpa_name,
        nullif(tpa_subscriber_id, '')                       tpa_subscriber_id,
        nullif(electronic_verification_code, '')            electronic_verification_code,
        nullif(plan_network_identification_number, '')      plan_network_identification_number,
        nullif(managed_care_subscriber_id, '')              managed_care_subscriber_id,
        nullif(qualified_medicare_beneficiary, '')          qualified_medicare_beneficiary,
        nullif(medicare_part_a_only, '')                    medicare_part_a_only,
        nullif(insurance_plan, '')                          insurance_plan,
        nullif(insurance_type, '')                          insurance_type,
        nullif(deductible, '')                              deductible,
        nullif(deductible_remaining, '')                    deductible_remaining,
        nullif(out_of_pocket, '')                           out_of_pocket,
        nullif(out_of_pocketremaining, '')                  out_of_pocketremaining,
        nullif(life_time_limit, '')                         life_time_limit,
        nullif(life_time_limit_remaining, '')               life_time_limit_remaining,
        nullif(spend_down, '')                              spend_down,
        nullif(spend_down_amount, '')                       spend_down_amount,
        nullif(managed_care_plan, '')                       managed_care_plan,
        nullif(managed_care_program, '')                    managed_care_program,
        nullif(related_entities, '')                        related_entities,
        nullif(medicare_a_deductible, '')                   medicare_a_deductible,
        nullif(medicare_a_deductible_time_period, '')       medicare_a_deductible_time_period,
        nullif(medicare_a_deductible_remaining, '')         medicare_a_deductible_remaining,
        nullif(medicare_b_deductible, '')                   medicare_b_deductible,
        nullif(medicare_b_deductible_time_period, '')       medicare_b_deductible_time_period,
        nullif(medicare_b_deductible_remaining, '')         medicare_b_deductible_remaining,
        nullif(medicare_subscriber_id, '')                  medicare_subscriber_id,
        nullif(medicaid_hic_number, '')                     medicaid_hic_number,
        nullif(mismatch_first_name, '')                     mismatch_first_name,
        nullif(mismatch_last_name, '')                      mismatch_last_name,
        nullif(mismatch_ssn, '')                            mismatch_ssn,
        nullif(mismatch_dob, '')                            mismatch_dob,
        nullif(mismatch_state, '')                          mismatch_state,
        nullif(confidence_score, '')                        confidence_score,
        nullif(confidence_score_reason, '')                 confidence_score_reason,
        nullif(payor_name_2, '')                            payor_name_2,
        nullif(payor_code_2, '')                            payor_code_2,
        nullif(status_2, '')                                status_2,
        nullif(subscriber_id_2, '')                         subscriber_id_2,
        nullif(policy_start_date_2, '')::date               policy_start_date_2,
        nullif(policy_end_date_2, '')::date                 policy_end_date_2,
        nullif(added_date_2, '')::date                      added_date_2,
        nullif(date_of_death_2, '')::date                   date_of_death_2,
        nullif(health_insurance_claim_number_2, '')         health_insurance_claim_number_2,
        nullif(grp_number_2, '')                            grp_number_2,
        nullif(plan_sponsor_2, '')                          plan_sponsor_2,
        nullif(medicare_part_a_start_2, '')::date           medicare_part_a_start_2,
        nullif(medicare_part_a_end_2, '')::date             medicare_part_a_end_2,
        nullif(medicare_part_b_start_2, '')::date           medicare_part_b_start_2,
        nullif(medicare_part_b_end_2, '')::date             medicare_part_b_end_2,
        nullif(additional_coverage_2, '')                   additional_coverage_2,
        nullif(additional_coverage_policy_number_2, '')     additional_coverage_policy_number_2,
        nullif(additional_coverage_contact_number_2, '')    additional_coverage_contact_number_2,
        nullif(additional_coverage_start_date_2, '')::date  additional_coverage_start_date_2,
        nullif(additional_coverage_end_date_2, '')::date    additional_coverage_end_date_2,
        nullif(additional_coverage_address_1_2, '')         additional_coverage_address_1_2,
        nullif(additional_coverage_address_2_2, '')         additional_coverage_address_2_2,
        nullif(additional_coverage_city_2, '')              additional_coverage_city_2,
        nullif(additional_coverage_state_2, '')             additional_coverage_state_2,
        nullif(additional_coverage_zip_2, '')               additional_coverage_zip_2,
        nullif(received_first_name_2, '')                   received_first_name_2,
        nullif(received_last_name_2, '')                    received_last_name_2,
        nullif(received_suffix_2, '')                       received_suffix_2,
        nullif(received_middle_name_2, '')                  received_middle_name_2,
        nullif(received_dob_2, '')::date                    received_dob_2,
        nullif(received_ssn_2, '')                          received_ssn_2,
        nullif(received_gender_2, '')                       received_gender_2,
        nullif(received_address_1_2, '')                    received_address_1_2,
        nullif(received_address_2_2, '')                    received_address_2_2,
        nullif(received_city_2, '')                         received_city_2,
        nullif(received_state_2, '')                        received_state_2,
        nullif(received_zip_2, '')                          received_zip_2,
        nullif(dep_first_name_2, '')                        dep_first_name_2,
        nullif(dep_last_name_2, '')                         dep_last_name_2,
        nullif(dep_suffix_2, '')                            dep_suffix_2,
        nullif(dep_middle_name_2, '')                       dep_middle_name_2,
        nullif(dep_dob_2, '')::date                         dep_dob_2,
        nullif(dep_ssn_2, '')                               dep_ssn_2,
        nullif(dep_gender_2, '')                            dep_gender_2,
        nullif(dep_address_1_2, '')                         dep_address_1_2,
        nullif(dep_address_2_2, '')                         dep_address_2_2,
        nullif(dep_city_2, '')                              dep_city_2,
        nullif(dep_state_2, '')                             dep_state_2,
        nullif(dep_zip_2, '')                               dep_zip_2,
        nullif(medicare_replacement_payor_2, '')            medicare_replacement_payor_2,
        nullif(medicare_replacement_plan_name_2, '')        medicare_replacement_plan_name_2,
        nullif(medicare_supplemental_plan_name_2, '')       medicare_supplemental_plan_name_2,
        nullif(hospice_start_date_2, '')::date              hospice_start_date_2,
        nullif(hospice_end_date_2, '')::date                hospice_end_date_2,
        nullif(hospice_npi_2, '')                           hospice_npi_2,
        nullif(payor_address_1_2, '')                       payor_address_1_2,
        nullif(payor_address_2_2, '')                       payor_address_2_2,
        nullif(payor_city_2, '')                            payor_city_2,
        nullif(payor_state_2, '')                           payor_state_2,
        nullif(payor_zip_2, '')                             payor_zip_2,
        nullif(payor_identifier_2, '')                      payor_identifier_2,
        nullif(county_code_2, '')                           county_code_2,
        nullif(ipa_identifier_2, '')                        ipa_identifier_2,
        nullif(ipa_description_2, '')                       ipa_description_2,
        nullif(tpa_name_2, '')                              tpa_name_2,
        nullif(tpa_subscriber_id_2, '')                     tpa_subscriber_id_2,
        nullif(electronic_verification_code_2, '')          electronic_verification_code_2,
        nullif(plan_network_identification_number_2, '')    plan_network_identification_number_2,
        nullif(managed_care_subscriber_id_2, '')            managed_care_subscriber_id_2,
        nullif(qualified_medicare_beneficiary_2, '')        qualified_medicare_beneficiary_2,
        nullif(medicare_part_a_only_2, '')                  medicare_part_a_only_2,
        nullif(insurance_plan_2, '')                        insurance_plan_2,
        nullif(insurance_type_2, '')                        insurance_type_2,
        nullif(deductible_2, '')                            deductible_2,
        nullif(deductible_remaining_2, '')                  deductible_remaining_2,
        nullif(out_of_pocket_2, '')                         out_of_pocket_2,
        nullif(out_of_pocketremaining_2, '')                out_of_pocketremaining_2,
        nullif(life_time_limit_2, '')                       life_time_limit_2,
        nullif(life_time_limit_remaining_2, '')             life_time_limit_remaining_2,
        nullif(spend_down_2, '')                            spend_down_2,
        nullif(spend_down_amount_2, '')                     spend_down_amount_2,
        nullif(managed_care_plan_2, '')                     managed_care_plan_2,
        nullif(managed_care_program_2, '')                  managed_care_program_2,
        nullif(related_entities_2, '')                      related_entities_2,
        nullif(medicare_a_deductible_2, '')                 medicare_a_deductible_2,
        nullif(medicare_a_deductible_time_period_2, '')     medicare_a_deductible_time_period_2,
        nullif(medicare_a_deductible_remaining_2, '')       medicare_a_deductible_remaining_2,
        nullif(medicare_b_deductible_2, '')                 medicare_b_deductible_2,
        nullif(medicare_b_deductible_time_period_2, '')     medicare_b_deductible_time_period_2,
        nullif(medicare_b_deductible_remaining_2, '')       medicare_b_deductible_remaining_2,
        nullif(medicare_subscriber_id_2, '')                medicare_subscriber_id_2,
        nullif(medicaid_hic_number_2, '')                   medicaid_hic_number_2,
        nullif(mismatch_first_name_2, '')                   mismatch_first_name_2,
        nullif(mismatch_last_name_2, '')                    mismatch_last_name_2,
        nullif(mismatch_ssn_2, '')                          mismatch_ssn_2,
        nullif(mismatch_dob_2, '')                          mismatch_dob_2,
        nullif(mismatch_state_2, '')                        mismatch_state_2,
        nullif(confidence_score_2, '')                      confidence_score_2,
        nullif(confidence_score_reason_2, '')               confidence_score_reason_2,
        nullif(payor_name_3, '')                            payor_name_3,
        nullif(payor_code_3, '')                            payor_code_3,
        nullif(status_3, '')                                status_3,
        nullif(subscriber_id_3, '')                         subscriber_id_3,
        nullif(policy_start_date_3, '')::date               policy_start_date_3,
        nullif(policy_end_date_3, '')::date                 policy_end_date_3,
        nullif(added_date_3, '')::date                      added_date_3,
        nullif(date_of_death_3, '')::date                   date_of_death_3,
        nullif(health_insurance_claim_number_3, '')         health_insurance_claim_number_3,
        nullif(grp_number_3, '')                            grp_number_3,
        nullif(plan_sponsor_3, '')                          plan_sponsor_3,
        nullif(medicare_part_a_start_3, '')::date           medicare_part_a_start_3,
        nullif(medicare_part_a_end_3, '')::date             medicare_part_a_end_3,
        nullif(medicare_part_b_start_3, '')::date           medicare_part_b_start_3,
        nullif(medicare_part_b_end_3, '')::date             medicare_part_b_end_3,
        nullif(additional_coverage_3, '')                   additional_coverage_3,
        nullif(additional_coverage_policy_number_3, '')     additional_coverage_policy_number_3,
        nullif(additional_coverage_contact_number_3, '')    additional_coverage_contact_number_3,
        nullif(additional_coverage_start_date_3, '')::date  additional_coverage_start_date_3,
        nullif(additional_coverage_end_date_3, '')::date    additional_coverage_end_date_3,
        nullif(additional_coverage_address_1_3, '')         additional_coverage_address_1_3,
        nullif(additional_coverage_address_2_3, '')         additional_coverage_address_2_3,
        nullif(additional_coverage_city_3, '')              additional_coverage_city_3,
        nullif(additional_coverage_state_3, '')             additional_coverage_state_3,
        nullif(additional_coverage_zip_3, '')               additional_coverage_zip_3,
        nullif(received_first_name_3, '')                   received_first_name_3,
        nullif(received_last_name_3, '')                    received_last_name_3,
        nullif(received_suffix_3, '')                       received_suffix_3,
        nullif(received_middle_name_3, '')                  received_middle_name_3,
        nullif(received_dob_3, '')::date                    received_dob_3,
        nullif(received_ssn_3, '')                          received_ssn_3,
        nullif(received_gender_3, '')                       received_gender_3,
        nullif(received_address_1_3, '')                    received_address_1_3,
        nullif(received_address_2_3, '')                    received_address_2_3,
        nullif(received_city_3, '')                         received_city_3,
        nullif(received_state_3, '')                        received_state_3,
        nullif(received_zip_3, '')                          received_zip_3,
        nullif(dep_first_name_3, '')                        dep_first_name_3,
        nullif(dep_last_name_3, '')                         dep_last_name_3,
        nullif(dep_suffix_3, '')                            dep_suffix_3,
        nullif(dep_middle_name_3, '')                       dep_middle_name_3,
        nullif(dep_dob_3, '')::date                         dep_dob_3,
        nullif(dep_ssn_3, '')                               dep_ssn_3,
        nullif(dep_gender_3, '')                            dep_gender_3,
        nullif(dep_address_1_3, '')                         dep_address_1_3,
        nullif(dep_address_2_3, '')                         dep_address_2_3,
        nullif(dep_city_3, '')                              dep_city_3,
        nullif(dep_state_3, '')                             dep_state_3,
        nullif(dep_zip_3, '')                               dep_zip_3,
        nullif(medicare_replacement_payor_3, '')            medicare_replacement_payor_3,
        nullif(medicare_replacement_plan_name_3, '')        medicare_replacement_plan_name_3,
        nullif(medicare_supplemental_plan_name_3, '')       medicare_supplemental_plan_name_3,
        nullif(hospice_start_date_3, '')::date              hospice_start_date_3,
        nullif(hospice_end_date_3, '')::date                hospice_end_date_3,
        nullif(hospice_npi_3, '')                           hospice_npi_3,
        nullif(payor_address_1_3, '')                       payor_address_1_3,
        nullif(payor_address_2_3, '')                       payor_address_2_3,
        nullif(payor_city_3, '')                            payor_city_3,
        nullif(payor_state_3, '')                           payor_state_3,
        nullif(payor_zip_3, '')                             payor_zip_3,
        nullif(payor_identifier_3, '')                      payor_identifier_3,
        nullif(county_code_3, '')                           county_code_3,
        nullif(ipa_identifier_3, '')                        ipa_identifier_3,
        nullif(ipa_description_3, '')                       ipa_description_3,
        nullif(tpa_name_3, '')                              tpa_name_3,
        nullif(tpa_subscriber_id_3, '')                     tpa_subscriber_id_3,
        nullif(electronic_verification_code_3, '')          electronic_verification_code_3,
        nullif(plan_network_identification_number_3, '')    plan_network_identification_number_3,
        nullif(managed_care_subscriber_id_3, '')            managed_care_subscriber_id_3,
        nullif(qualified_medicare_beneficiary_3, '')        qualified_medicare_beneficiary_3,
        nullif(medicare_part_a_only_3, '')                  medicare_part_a_only_3,
        nullif(insurance_plan_3, '')                        insurance_plan_3,
        nullif(insurance_type_3, '')                        insurance_type_3,
        nullif(deductible_3, '')                            deductible_3,
        nullif(deductible_remaining_3, '')                  deductible_remaining_3,
        nullif(out_of_pocket_3, '')                         out_of_pocket_3,
        nullif(out_of_pocketremaining_3, '')                out_of_pocketremaining_3,
        nullif(life_time_limit_3, '')                       life_time_limit_3,
        nullif(life_time_limit_remaining_3, '')             life_time_limit_remaining_3,
        nullif(spend_down_3, '')                            spend_down_3,
        nullif(spend_down_amount_3, '')                     spend_down_amount_3,
        nullif(managed_care_plan_3, '')                     managed_care_plan_3,
        nullif(managed_care_program_3, '')                  managed_care_program_3,
        nullif(related_entities_3, '')                      related_entities_3,
        nullif(medicare_a_deductible_3, '')                 medicare_a_deductible_3,
        nullif(medicare_a_deductible_time_period_3, '')     medicare_a_deductible_time_period_3,
        nullif(medicare_a_deductible_remaining_3, '')       medicare_a_deductible_remaining_3,
        nullif(medicare_b_deductible_3, '')                 medicare_b_deductible_3,
        nullif(medicare_b_deductible_time_period_3, '')     medicare_b_deductible_time_period_3,
        nullif(medicare_b_deductible_remaining_3, '')       medicare_b_deductible_remaining_3,
        nullif(medicare_subscriber_id_3, '')                medicare_subscriber_id_3,
        nullif(medicaid_hic_number_3, '')                   medicaid_hic_number_3,
        nullif(mismatch_first_name_3, '')                   mismatch_first_name_3,
        nullif(mismatch_last_name_3, '')                    mismatch_last_name_3,
        nullif(mismatch_ssn_3, '')                          mismatch_ssn_3,
        nullif(mismatch_dob_3, '')                          mismatch_dob_3,
        nullif(mismatch_state_3, '')                        mismatch_state_3,
        nullif(confidence_score_3, '')                      confidence_score_3,
        nullif(confidence_score_reason_3, '')               confidence_score_reason_3
    from
        ability.ability_all
--     where
--         patient_visit_number !~* '[a-z]'
--     order by patient_visit_number, date_of_service desc, id desc
    on conflict (patient_id)
        do update
        set
            raw_id = excluded.raw_id,
            patient_id = excluded.patient_id,
            patient_first_name = excluded.patient_first_name,
            patient_last_name = excluded.patient_last_name,
            patient_middle_name = excluded.patient_middle_name,
            patient_suffix = excluded.patient_suffix,
            patient_gender = excluded.patient_gender,
            patient_address_1 = excluded.patient_address_1,
            patient_address_2 = excluded.patient_address_2,
            patient_city = excluded.patient_city,
            patient_state = excluded.patient_state,
            patient_zip = excluded.patient_zip,
            date_of_service = excluded.date_of_service,
            facility_name = excluded.facility_name,
            patient_dob = excluded.patient_dob,
            patient_ssn = excluded.patient_ssn,
            payor_name = excluded.payor_name,
            payor_code = excluded.payor_code,
            status = excluded.status,
            subscriber_id = excluded.subscriber_id,
            policy_start_date = excluded.policy_start_date,
            policy_end_date = excluded.policy_end_date,
            added_date = excluded.added_date,
            date_of_death = excluded.date_of_death,
            health_insurance_claim_number = excluded.health_insurance_claim_number,
            grp_number = excluded.grp_number,
            plan_sponsor = excluded.plan_sponsor,
            medicare_part_a_start = excluded.medicare_part_a_start,
            medicare_part_a_end = excluded.medicare_part_a_end,
            medicare_part_b_start = excluded.medicare_part_b_start,
            medicare_part_b_end = excluded.medicare_part_b_end,
            additional_coverage = excluded.additional_coverage,
            additional_coverage_policy_number = excluded.additional_coverage_policy_number,
            additional_coverage_contact_number = excluded.additional_coverage_contact_number,
            additional_coverage_start_date = excluded.additional_coverage_start_date,
            additional_coverage_end_date = excluded.additional_coverage_end_date,
            additional_coverage_address_1 = excluded.additional_coverage_address_1,
            additional_coverage_address_2 = excluded.additional_coverage_address_2,
            additional_coverage_city = excluded.additional_coverage_city,
            additional_coverage_state = excluded.additional_coverage_state,
            additional_coverage_zip = excluded.additional_coverage_zip,
            received_first_name = excluded.received_first_name,
            received_last_name = excluded.received_last_name,
            received_suffix = excluded.received_suffix,
            received_middle_name = excluded.received_middle_name,
            received_dob = excluded.received_dob,
            received_ssn = excluded.received_ssn,
            received_gender = excluded.received_gender,
            received_address_1 = excluded.received_address_1,
            received_address_2 = excluded.received_address_2,
            received_city = excluded.received_city,
            received_state = excluded.received_state,
            received_zip = excluded.received_zip,
            dep_first_name = excluded.dep_first_name,
            dep_last_name = excluded.dep_last_name,
            dep_suffix = excluded.dep_suffix,
            dep_middle_name = excluded.dep_middle_name,
            dep_dob = excluded.dep_dob,
            dep_ssn = excluded.dep_ssn,
            dep_gender = excluded.dep_gender,
            dep_address_1 = excluded.dep_address_1,
            dep_address_2 = excluded.dep_address_2,
            dep_city = excluded.dep_city,
            dep_state = excluded.dep_state,
            dep_zip = excluded.dep_zip,
            medicare_replacement_payor = excluded.medicare_replacement_payor,
            medicare_replacement_plan_name = excluded.medicare_replacement_plan_name,
            medicare_supplemental_plan_name = excluded.medicare_supplemental_plan_name,
            hospice_start_date = excluded.hospice_start_date,
            hospice_end_date = excluded.hospice_end_date,
            hospice_npi = excluded.hospice_npi,
            payor_address_1 = excluded.payor_address_1,
            payor_address_2 = excluded.payor_address_2,
            payor_city = excluded.payor_city,
            payor_state = excluded.payor_state,
            payor_zip = excluded.payor_zip,
            payor_identifier = excluded.payor_identifier,
            county_code = excluded.county_code,
            ipa_identifier = excluded.ipa_identifier,
            ipa_description = excluded.ipa_description,
            tpa_name = excluded.tpa_name,
            tpa_subscriber_id = excluded.tpa_subscriber_id,
            electronic_verification_code = excluded.electronic_verification_code,
            plan_network_identification_number = excluded.plan_network_identification_number,
            managed_care_subscriber_id = excluded.managed_care_subscriber_id,
            qualified_medicare_beneficiary = excluded.qualified_medicare_beneficiary,
            medicare_part_a_only = excluded.medicare_part_a_only,
            insurance_plan = excluded.insurance_plan,
            insurance_type = excluded.insurance_type,
            deductible = excluded.deductible,
            deductible_remaining = excluded.deductible_remaining,
            out_of_pocket = excluded.out_of_pocket,
            out_of_pocketremaining = excluded.out_of_pocketremaining,
            life_time_limit = excluded.life_time_limit,
            life_time_limit_remaining = excluded.life_time_limit_remaining,
            spend_down = excluded.spend_down,
            spend_down_amount = excluded.spend_down_amount,
            managed_care_plan = excluded.managed_care_plan,
            managed_care_program = excluded.managed_care_program,
            related_entities = excluded.related_entities,
            medicare_a_deductible = excluded.medicare_a_deductible,
            medicare_a_deductible_time_period = excluded.medicare_a_deductible_time_period,
            medicare_a_deductible_remaining = excluded.medicare_a_deductible_remaining,
            medicare_b_deductible = excluded.medicare_b_deductible,
            medicare_b_deductible_time_period = excluded.medicare_b_deductible_time_period,
            medicare_b_deductible_remaining = excluded.medicare_b_deductible_remaining,
            medicare_subscriber_id = excluded.medicare_subscriber_id,
            medicaid_hic_number = excluded.medicaid_hic_number,
            mismatch_first_name = excluded.mismatch_first_name,
            mismatch_last_name = excluded.mismatch_last_name,
            mismatch_ssn = excluded.mismatch_ssn,
            mismatch_dob = excluded.mismatch_dob,
            mismatch_state = excluded.mismatch_state,
            confidence_score = excluded.confidence_score,
            confidence_score_reason = excluded.confidence_score_reason,
            payor_name_2 = excluded.payor_name_2,
            payor_code_2 = excluded.payor_code_2,
            status_2 = excluded.status_2,
            subscriber_id_2 = excluded.subscriber_id_2,
            policy_start_date_2 = excluded.policy_start_date_2,
            policy_end_date_2 = excluded.policy_end_date_2,
            added_date_2 = excluded.added_date_2,
            date_of_death_2 = excluded.date_of_death_2,
            health_insurance_claim_number_2 = excluded.health_insurance_claim_number_2,
            grp_number_2 = excluded.grp_number_2,
            plan_sponsor_2 = excluded.plan_sponsor_2,
            medicare_part_a_start_2 = excluded.medicare_part_a_start_2,
            medicare_part_a_end_2 = excluded.medicare_part_a_end_2,
            medicare_part_b_start_2 = excluded.medicare_part_b_start_2,
            medicare_part_b_end_2 = excluded.medicare_part_b_end_2,
            additional_coverage_2 = excluded.additional_coverage_2,
            additional_coverage_policy_number_2 = excluded.additional_coverage_policy_number_2,
            additional_coverage_contact_number_2 = excluded.additional_coverage_contact_number_2,
            additional_coverage_start_date_2 = excluded.additional_coverage_start_date_2,
            additional_coverage_end_date_2 = excluded.additional_coverage_end_date_2,
            additional_coverage_address_1_2 = excluded.additional_coverage_address_1_2,
            additional_coverage_address_2_2 = excluded.additional_coverage_address_2_2,
            additional_coverage_city_2 = excluded.additional_coverage_city_2,
            additional_coverage_state_2 = excluded.additional_coverage_state_2,
            additional_coverage_zip_2 = excluded.additional_coverage_zip_2,
            received_first_name_2 = excluded.received_first_name_2,
            received_last_name_2 = excluded.received_last_name_2,
            received_suffix_2 = excluded.received_suffix_2,
            received_middle_name_2 = excluded.received_middle_name_2,
            received_dob_2 = excluded.received_dob_2,
            received_ssn_2 = excluded.received_ssn_2,
            received_gender_2 = excluded.received_gender_2,
            received_address_1_2 = excluded.received_address_1_2,
            received_address_2_2 = excluded.received_address_2_2,
            received_city_2 = excluded.received_city_2,
            received_state_2 = excluded.received_state_2,
            received_zip_2 = excluded.received_zip_2,
            dep_first_name_2 = excluded.dep_first_name_2,
            dep_last_name_2 = excluded.dep_last_name_2,
            dep_suffix_2 = excluded.dep_suffix_2,
            dep_middle_name_2 = excluded.dep_middle_name_2,
            dep_dob_2 = excluded.dep_dob_2,
            dep_ssn_2 = excluded.dep_ssn_2,
            dep_gender_2 = excluded.dep_gender_2,
            dep_address_1_2 = excluded.dep_address_1_2,
            dep_address_2_2 = excluded.dep_address_2_2,
            dep_city_2 = excluded.dep_city_2,
            dep_state_2 = excluded.dep_state_2,
            dep_zip_2 = excluded.dep_zip_2,
            medicare_replacement_payor_2 = excluded.medicare_replacement_payor_2,
            medicare_replacement_plan_name_2 = excluded.medicare_replacement_plan_name_2,
            medicare_supplemental_plan_name_2 = excluded.medicare_supplemental_plan_name_2,
            hospice_start_date_2 = excluded.hospice_start_date_2,
            hospice_end_date_2 = excluded.hospice_end_date_2,
            hospice_npi_2 = excluded.hospice_npi_2,
            payor_address_1_2 = excluded.payor_address_1_2,
            payor_address_2_2 = excluded.payor_address_2_2,
            payor_city_2 = excluded.payor_city_2,
            payor_state_2 = excluded.payor_state_2,
            payor_zip_2 = excluded.payor_zip_2,
            payor_identifier_2 = excluded.payor_identifier_2,
            county_code_2 = excluded.county_code_2,
            ipa_identifier_2 = excluded.ipa_identifier_2,
            ipa_description_2 = excluded.ipa_description_2,
            tpa_name_2 = excluded.tpa_name_2,
            tpa_subscriber_id_2 = excluded.tpa_subscriber_id_2,
            electronic_verification_code_2 = excluded.electronic_verification_code_2,
            plan_network_identification_number_2 = excluded.plan_network_identification_number_2,
            managed_care_subscriber_id_2 = excluded.managed_care_subscriber_id_2,
            qualified_medicare_beneficiary_2 = excluded.qualified_medicare_beneficiary_2,
            medicare_part_a_only_2 = excluded.medicare_part_a_only_2,
            insurance_plan_2 = excluded.insurance_plan_2,
            insurance_type_2 = excluded.insurance_type_2,
            deductible_2 = excluded.deductible_2,
            deductible_remaining_2 = excluded.deductible_remaining_2,
            out_of_pocket_2 = excluded.out_of_pocket_2,
            out_of_pocketremaining_2 = excluded.out_of_pocketremaining_2,
            life_time_limit_2 = excluded.life_time_limit_2,
            life_time_limit_remaining_2 = excluded.life_time_limit_remaining_2,
            spend_down_2 = excluded.spend_down_2,
            spend_down_amount_2 = excluded.spend_down_amount_2,
            managed_care_plan_2 = excluded.managed_care_plan_2,
            managed_care_program_2 = excluded.managed_care_program_2,
            related_entities_2 = excluded.related_entities_2,
            medicare_a_deductible_2 = excluded.medicare_a_deductible_2,
            medicare_a_deductible_time_period_2 = excluded.medicare_a_deductible_time_period_2,
            medicare_a_deductible_remaining_2 = excluded.medicare_a_deductible_remaining_2,
            medicare_b_deductible_2 = excluded.medicare_b_deductible_2,
            medicare_b_deductible_time_period_2 = excluded.medicare_b_deductible_time_period_2,
            medicare_b_deductible_remaining_2 = excluded.medicare_b_deductible_remaining_2,
            medicare_subscriber_id_2 = excluded.medicare_subscriber_id_2,
            medicaid_hic_number_2 = excluded.medicaid_hic_number_2,
            mismatch_first_name_2 = excluded.mismatch_first_name_2,
            mismatch_last_name_2 = excluded.mismatch_last_name_2,
            mismatch_ssn_2 = excluded.mismatch_ssn_2,
            mismatch_dob_2 = excluded.mismatch_dob_2,
            mismatch_state_2 = excluded.mismatch_state_2,
            confidence_score_2 = excluded.confidence_score_2,
            confidence_score_reason_2 = excluded.confidence_score_reason_2,
            payor_name_3 = excluded.payor_name_3,
            payor_code_3 = excluded.payor_code_3,
            status_3 = excluded.status_3,
            subscriber_id_3 = excluded.subscriber_id_3,
            policy_start_date_3 = excluded.policy_start_date_3,
            policy_end_date_3 = excluded.policy_end_date_3,
            added_date_3 = excluded.added_date_3,
            date_of_death_3 = excluded.date_of_death_3,
            health_insurance_claim_number_3 = excluded.health_insurance_claim_number_3,
            grp_number_3 = excluded.grp_number_3,
            plan_sponsor_3 = excluded.plan_sponsor_3,
            medicare_part_a_start_3 = excluded.medicare_part_a_start_3,
            medicare_part_a_end_3 = excluded.medicare_part_a_end_3,
            medicare_part_b_start_3 = excluded.medicare_part_b_start_3,
            medicare_part_b_end_3 = excluded.medicare_part_b_end_3,
            additional_coverage_3 = excluded.additional_coverage_3,
            additional_coverage_policy_number_3 = excluded.additional_coverage_policy_number_3,
            additional_coverage_contact_number_3 = excluded.additional_coverage_contact_number_3,
            additional_coverage_start_date_3 = excluded.additional_coverage_start_date_3,
            additional_coverage_end_date_3 = excluded.additional_coverage_end_date_3,
            additional_coverage_address_1_3 = excluded.additional_coverage_address_1_3,
            additional_coverage_address_2_3 = excluded.additional_coverage_address_2_3,
            additional_coverage_city_3 = excluded.additional_coverage_city_3,
            additional_coverage_state_3 = excluded.additional_coverage_state_3,
            additional_coverage_zip_3 = excluded.additional_coverage_zip_3,
            received_first_name_3 = excluded.received_first_name_3,
            received_last_name_3 = excluded.received_last_name_3,
            received_suffix_3 = excluded.received_suffix_3,
            received_middle_name_3 = excluded.received_middle_name_3,
            received_dob_3 = excluded.received_dob_3,
            received_ssn_3 = excluded.received_ssn_3,
            received_gender_3 = excluded.received_gender_3,
            received_address_1_3 = excluded.received_address_1_3,
            received_address_2_3 = excluded.received_address_2_3,
            received_city_3 = excluded.received_city_3,
            received_state_3 = excluded.received_state_3,
            received_zip_3 = excluded.received_zip_3,
            dep_first_name_3 = excluded.dep_first_name_3,
            dep_last_name_3 = excluded.dep_last_name_3,
            dep_suffix_3 = excluded.dep_suffix_3,
            dep_middle_name_3 = excluded.dep_middle_name_3,
            dep_dob_3 = excluded.dep_dob_3,
            dep_ssn_3 = excluded.dep_ssn_3,
            dep_gender_3 = excluded.dep_gender_3,
            dep_address_1_3 = excluded.dep_address_1_3,
            dep_address_2_3 = excluded.dep_address_2_3,
            dep_city_3 = excluded.dep_city_3,
            dep_state_3 = excluded.dep_state_3,
            dep_zip_3 = excluded.dep_zip_3,
            medicare_replacement_payor_3 = excluded.medicare_replacement_payor_3,
            medicare_replacement_plan_name_3 = excluded.medicare_replacement_plan_name_3,
            medicare_supplemental_plan_name_3 = excluded.medicare_supplemental_plan_name_3,
            hospice_start_date_3 = excluded.hospice_start_date_3,
            hospice_end_date_3 = excluded.hospice_end_date_3,
            hospice_npi_3 = excluded.hospice_npi_3,
            payor_address_1_3 = excluded.payor_address_1_3,
            payor_address_2_3 = excluded.payor_address_2_3,
            payor_city_3 = excluded.payor_city_3,
            payor_state_3 = excluded.payor_state_3,
            payor_zip_3 = excluded.payor_zip_3,
            payor_identifier_3 = excluded.payor_identifier_3,
            county_code_3 = excluded.county_code_3,
            ipa_identifier_3 = excluded.ipa_identifier_3,
            ipa_description_3 = excluded.ipa_description_3,
            tpa_name_3 = excluded.tpa_name_3,
            tpa_subscriber_id_3 = excluded.tpa_subscriber_id_3,
            electronic_verification_code_3 = excluded.electronic_verification_code_3,
            plan_network_identification_number_3 = excluded.plan_network_identification_number_3,
            managed_care_subscriber_id_3 = excluded.managed_care_subscriber_id_3,
            qualified_medicare_beneficiary_3 = excluded.qualified_medicare_beneficiary_3,
            medicare_part_a_only_3 = excluded.medicare_part_a_only_3,
            insurance_plan_3 = excluded.insurance_plan_3,
            insurance_type_3 = excluded.insurance_type_3,
            deductible_3 = excluded.deductible_3,
            deductible_remaining_3 = excluded.deductible_remaining_3,
            out_of_pocket_3 = excluded.out_of_pocket_3,
            out_of_pocketremaining_3 = excluded.out_of_pocketremaining_3,
            life_time_limit_3 = excluded.life_time_limit_3,
            life_time_limit_remaining_3 = excluded.life_time_limit_remaining_3,
            spend_down_3 = excluded.spend_down_3,
            spend_down_amount_3 = excluded.spend_down_amount_3,
            managed_care_plan_3 = excluded.managed_care_plan_3,
            managed_care_program_3 = excluded.managed_care_program_3,
            related_entities_3 = excluded.related_entities_3,
            medicare_a_deductible_3 = excluded.medicare_a_deductible_3,
            medicare_a_deductible_time_period_3 = excluded.medicare_a_deductible_time_period_3,
            medicare_a_deductible_remaining_3 = excluded.medicare_a_deductible_remaining_3,
            medicare_b_deductible_3 = excluded.medicare_b_deductible_3,
            medicare_b_deductible_time_period_3 = excluded.medicare_b_deductible_time_period_3,
            medicare_b_deductible_remaining_3 = excluded.medicare_b_deductible_remaining_3,
            medicare_subscriber_id_3 = excluded.medicare_subscriber_id_3,
            medicaid_hic_number_3 = excluded.medicaid_hic_number_3,
            mismatch_first_name_3 = excluded.mismatch_first_name_3,
            mismatch_last_name_3 = excluded.mismatch_last_name_3,
            mismatch_ssn_3 = excluded.mismatch_ssn_3,
            mismatch_dob_3 = excluded.mismatch_dob_3,
            mismatch_state_3 = excluded.mismatch_state_3,
            confidence_score_3 = excluded.confidence_score_3,
            confidence_score_reason_3 = excluded.confidence_score_reason_3,
            updated_at = now()
    where
        ability_inbound.date_of_service is distinct from excluded.date_of_service
    ;

    drop table if exists _cms_payer_records;
    create temp table _cms_payer_records as
    select
        id ability_inbound_id,
        patient_id,
        date_of_service,
        'primary' rank,
        payor_name,
        payor_code,
        status,
        coalesce(
            staging.return_valid_mbi(subscriber_id),
            staging.return_valid_mbi(medicare_subscriber_id),
            staging.return_valid_mbi(medicaid_hic_number)
            ) mbi,
        medicare_part_a_start,
        medicare_part_b_start,
        medicare_replacement_payor,
        case when qualified_medicare_beneficiary = 'Y' then true else false end qualified_medicare_beneficiary,
        case when medicare_part_a_only = 'Y' then true else false end medicare_part_a_only,
        insurance_type,
        related_entities,
        confidence_score,
        confidence_score_reason
    from
        ability.ability_inbound
    where
        payor_code = '00472'
    union all
    select
        id,
        patient_id,
        date_of_service,
        'secondary' rank,
        payor_name_2,
        payor_code_2,
        status_2,
        coalesce(
            staging.return_valid_mbi(subscriber_id_2),
            staging.return_valid_mbi(medicare_subscriber_id_2),
            staging.return_valid_mbi(medicaid_hic_number_2)
            ) mbi,
        medicare_part_a_start_2,
        medicare_part_b_start_2,
        medicare_replacement_payor_2,
        case when qualified_medicare_beneficiary_2 = 'Y' then true else false end,
        case when medicare_part_a_only_2 = 'Y' then true else false end,
        insurance_type_2,
        related_entities_2,
        confidence_score_2,
        confidence_score_reason_2
    from
        ability.ability_inbound
    where
        payor_code_2 = '00472'
    union all
    select
        id,
        patient_id,
        date_of_service,
        'tertiary' rank,
        payor_name_3,
        payor_code_3,
        status_3,
        coalesce(
            staging.return_valid_mbi(subscriber_id_3),
            staging.return_valid_mbi(medicare_subscriber_id_3),
            staging.return_valid_mbi(medicaid_hic_number_3)
            ) mbi,
        medicare_part_a_start_3,
        medicare_part_b_start_3,
        medicare_replacement_payor_3,
        case when qualified_medicare_beneficiary_3 = 'Y' then true else false end,
        case when medicare_part_a_only_3 = 'Y' then true else false end,
        insurance_type_3,
        related_entities_3,
        confidence_score_3,
        confidence_score_reason_3
    from
        ability.ability_inbound
    where
        payor_code_3 = '00472'
    ;

    insert into ability.cms_payer_record (ability_inbound_id, date_of_service, patient_id, rank, payor_name, payor_code,
                                          status, mbi, medicare_part_a_start, medicare_part_b_start,
                                          medicare_replacement_payor, medicare_replacement_payer_id, medicare_replacement_payer_name,
                                          qualified_medicare_beneficiary,
                                          medicare_part_a_only, insurance_type, related_entities, confidence_score,
                                          confidence_score_reason)
    select
        ability_inbound_id,
        date_of_service,
        patient_id,
        rank,
        payor_name,
        payor_code,
        status,
        mbi,
        medicare_part_a_start,
        medicare_part_b_start,
        medicare_replacement_payor,
        mrp.payer_id,
        mrp.payer_name,
        qualified_medicare_beneficiary,
        medicare_part_a_only,
        insurance_type,
        related_entities,
        confidence_score,
        confidence_score_reason
    from
        _cms_payer_records cpr
        left join ability.medicare_replacement_payers mrp on mrp.name = cpr.medicare_replacement_payor
    on conflict (patient_id) do update
        set
            ability_inbound_id             = excluded.ability_inbound_id,
            date_of_service                = excluded.date_of_service,
            rank                           = excluded.rank,
            payor_name                     = excluded.payor_name,
            payor_code                     = excluded.payor_code,
            status                         = excluded.status,
            mbi                            = excluded.mbi,
            medicare_part_a_start          = excluded.medicare_part_a_start,
            medicare_part_b_start          = excluded.medicare_part_b_start,
            medicare_replacement_payor     = excluded.medicare_replacement_payor,
            qualified_medicare_beneficiary = excluded.qualified_medicare_beneficiary,
            medicare_part_a_only           = excluded.medicare_part_a_only,
            insurance_type                 = excluded.insurance_type,
            related_entities               = excluded.related_entities,
            confidence_score               = excluded.confidence_score,
            confidence_score_reason        = excluded.confidence_score_reason,
            updated_at                     = now()
    where
        excluded.ability_inbound_id is distinct from cms_payer_record.ability_inbound_id
    ;
    
    drop table if exists _all_pdp;
    create temp table _all_pdp as
    select
        id ability_inbound_id,
        patient_id,
        date_of_service,
        'primary' rank,
        payor_name,
        payor_code,
        status,
        subscriber_id,
        policy_start_date,
        grp_number,
        plan_network_identification_number,
        insurance_plan,
        insurance_type,
        replace(substring(insurance_plan from 'S\d{4}-?\d{3}'), '-', '') cms_contract_number,
        confidence_score,
        confidence_score_reason
    from
        ability.ability_inbound
    where
        plan_network_identification_number ~* 'PDG|PDA' -- searching insurance plan for pdp/S#### is identical
    union all
    select
        id ability_inbound_id,
        patient_id,
        date_of_service,
        'secondary' rank,
        payor_name_2,
        payor_code_2,
        status_2,
        subscriber_id_2,
        policy_start_date_2,
        grp_number_2,
        plan_network_identification_number_2,
        insurance_plan_2,
        insurance_type_2,
        replace(substring(insurance_plan_2 from 'S\d{4}-?\d{3}'), '-', '') cms_contract_number,
        confidence_score_2,
        confidence_score_reason_2
    from
        ability.ability_inbound
    where
        plan_network_identification_number_2 ~* 'PDG|PDA' -- searching insurance plan for pdp/S#### is identical
    union all
    select
        id ability_inbound_id,
        patient_id,
        date_of_service,
        'tertiary' rank,
        payor_name_3,
        payor_code_3,
        status_3,
        subscriber_id_3,
        policy_start_date_3,
        grp_number_3,
        plan_network_identification_number_3,
        insurance_plan_3,
        insurance_type_3,
        replace(substring(insurance_plan_3 from 'S\d{4}-?\d{3}'), '-', '') cms_contract_number,
        confidence_score_3,
        confidence_score_reason_3
    from
        ability.ability_inbound
    where
        plan_network_identification_number_3 ~* 'PDG|PDA' -- searching insurance plan for pdp/S#### is identical
    ;

    insert into ability.pdp_records (ability_inbound_id, patient_id, date_of_service, rank, payor_name, payor_code,
                                     status, subscriber_id, policy_start_date, grp_number,
                                     plan_network_identification_number, insurance_plan, insurance_type,
                                     cms_contract_number, confidence_score, confidence_score_reason)
    select distinct on (patient_id)
        ability_inbound_id,
        patient_id,
        date_of_service,
        rank,
        payor_name,
        payor_code,
        status,
        subscriber_id,
        policy_start_date,
        grp_number,
        plan_network_identification_number,
        insurance_plan,
        insurance_type,
        cms_contract_number,
        confidence_score,
        confidence_score_reason
    from
        _all_pdp ap
    order by patient_id,
             confidence_score = 'Y' desc, -- take Y before R
             case when rank = 'primary' then 1 when rank = 'secondary' then 2 else 3 end
    on conflict (patient_id) do update
        set
            ability_inbound_id                 = excluded.ability_inbound_id,
            date_of_service                    = excluded.date_of_service,
            rank                               = excluded.rank,
            payor_name                         = excluded.payor_name,
            payor_code                         = excluded.payor_code,
            status                             = excluded.status,
            subscriber_id                      = excluded.subscriber_id,
            policy_start_date                  = excluded.policy_start_date,
            grp_number                         = excluded.grp_number,
            plan_network_identification_number = excluded.plan_network_identification_number,
            insurance_plan                     = excluded.insurance_plan,
            insurance_type                     = excluded.insurance_type,
            cms_contract_number                = excluded.cms_contract_number,
            confidence_score                   = excluded.confidence_score,
            confidence_score_reason            = excluded.confidence_score_reason,
            updated_at                         = now()
    where
        excluded.ability_inbound_id is distinct from pdp_records.ability_inbound_id
    ;

    drop table if exists _all_med_supp;
    create temp table _all_med_supp as
    select
        id ability_inbound_id,
        patient_id,
        date_of_service,
        'primary' rank,
        payor_name,
        payor_code,
        status,
        subscriber_id,
        policy_start_date,
        grp_number,
        plan_sponsor,
        ipa_identifier,
        ipa_description,
        additional_coverage,
        insurance_plan,
        insurance_type,
        related_entities,
        confidence_score,
        confidence_score_reason
    from
        ability.ability_inbound
    where
        insurance_plan ~* 'supp' or insurance_type ~* 'SP'
    union all
    select
        id ability_inbound_id,
        patient_id,
        date_of_service,
        'secondary' rank,
        payor_name_2,
        payor_code_2,
        status_2,
        subscriber_id_2,
        policy_start_date_2,
        grp_number_2,
        plan_sponsor_2,
        ipa_identifier_2,
        ipa_description_2,
        additional_coverage_2,
        insurance_plan_2,
        insurance_type_2,
        related_entities_2,
        confidence_score_2,
        confidence_score_reason_2
    from
        ability.ability_inbound
    where
        insurance_plan_2 ~* 'supp' or insurance_type_2 ~* 'SP'
    union all
    select
        id ability_inbound_id,
        patient_id,
        date_of_service,
        'tertiary' rank,
        payor_name_3,
        payor_code_3,
        status_3,
        subscriber_id_3,
        policy_start_date_3,
        grp_number_3,
        plan_sponsor_3,
        ipa_identifier_3,
        ipa_description_3,
        additional_coverage_3,
        insurance_plan_3,
        insurance_type_3,
        related_entities_3,
        confidence_score_3,
        confidence_score_reason_3
    from
        ability.ability_inbound
    where
        insurance_plan_3 ~* 'supp' or insurance_type_3 ~* 'SP'
    ;

    insert into ability.med_supp_records (ability_inbound_id, patient_id, date_of_service, rank, payor_name, payor_code,
                                          status, subscriber_id, policy_start_date, grp_number, plan_sponsor,
                                          ipa_identifier, ipa_description, additional_coverage, insurance_plan,
                                          insurance_type, related_entities, confidence_score, confidence_score_reason)
    select distinct on (patient_id)
        ability_inbound_id,
        patient_id,
        date_of_service,
        rank,
        payor_name,
        payor_code,
        status,
        subscriber_id,
        policy_start_date,
        grp_number,
        plan_sponsor,
        ipa_identifier,
        ipa_description,
        additional_coverage,
        insurance_plan,
        insurance_type,
        related_entities,
        confidence_score,
        confidence_score_reason
    from
        _all_med_supp ams
    order by patient_id,
             confidence_score = 'Y' desc, -- take Y before R
             case when rank = 'primary' then 1 when rank = 'secondary' then 2 else 3 end
    on conflict (patient_id) do update
        set
            ability_inbound_id = excluded.ability_inbound_id,
            date_of_service = excluded.date_of_service,
            rank = excluded.rank,
            payor_name = excluded.payor_name,
            payor_code = excluded.payor_code,
            status = excluded.status,
            subscriber_id = excluded.subscriber_id,
            policy_start_date = excluded.policy_start_date,
            grp_number = excluded.grp_number,
            plan_sponsor = excluded.plan_sponsor,
            ipa_identifier = excluded.ipa_identifier,
            ipa_description = excluded.ipa_description,
            additional_coverage = excluded.additional_coverage,
            insurance_plan = excluded.insurance_plan,
            insurance_type = excluded.insurance_type,
            related_entities = excluded.related_entities,
            confidence_score = excluded.confidence_score,
            confidence_score_reason = excluded.confidence_score_reason,
            updated_at = now()
    where
        excluded.ability_inbound_id is distinct from med_supp_records.ability_inbound_id
    ;

--     call ability.sp_process_ability_to_coop();

--     -- Do this outside of the transaction to ensure staging sproc has access to the data
--     drop table if exists _trashy998;
--     create temporary table _trashy998 as
--     select 1 from dblink_exec('cb_member_doc', 'call stage.sp_process_ability_data()');

end;
$$;

ALTER PROCEDURE sp_load_ability_inbound() OWNER TO postgres;

