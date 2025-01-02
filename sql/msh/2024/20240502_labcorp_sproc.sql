CREATE PROCEDURE sp_lab_corp_stage_roster_patients()
    LANGUAGE plpgsql
AS
$$
BEGIN


    SELECT
        first_name
      , NULL                                                   middle_name
      , last_name
      , dob
      , LEFT(gender, 1)                                        gender
      , NULL                                                   race_code
      , NULL                                                   drivers_license_number
      , NULL                                                   drivers_license_state
      , NULL                                                   ssn9
      , NULL                                                   ssn4
      , NULL                                                   mrn
      , NULL                                                   ordering_account_number
      , pa.line1
      , pa.line2
      , pa.city
      , pa.state
      , LEFT(pa.postal_code, 5)                                zip5
      , NULL                                                   zip4
      , 'USA'                                                  country
      , COALESCE(mobile_cp.phone_number, home_cp.phone_number) primary_phone
      , mobile_cp.phone_number                                 cell_phone
      , home_cp.phone_number                                   home_phone
      , NULL                                                   email
      , pay.name
      , NULL                                                   payer_grp_numb
      , NULL                                                   member_id
      , sp.subscriber_id
      , sp.patient_id
      , 'N'                                                    record_indicator
    FROM
        fdw_member_doc.patients p
        JOIN fdw_member_doc.supreme_pizza sp ON p.id = sp.patient_id
        JOIN fdw_member_doc.patient_addresses pa ON p.id = pa.patient_id
        JOIN fdw_member_doc.payers pay ON p.payer_id = pay.id
        LEFT JOIN fdw_member_doc.patient_contact_phones pcp ON pcp.patient_id = p.id AND pcp."primary"
        LEFT JOIN fdw_member_doc.contact_phones home_cp
                  ON pcp.contact_phone_id = home_cp.id AND home_cp.status = 'active' AND home_cp.type = 'home'
        LEFT JOIN fdw_member_doc.contact_phones mobile_cp
                  ON pcp.contact_phone_id = mobile_cp.id AND mobile_cp.status = 'active' AND mobile_cp.type = 'mobile'
    WHERE
        sp.is_md_portal_full
    ;

END;
$$;

