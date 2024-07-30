select
    p.PATID,
    p.patid patient_id,
    p.firstname first_name,
    p.middlename middle_name,
    p.lastname last_name,
    p.email email,
    p.notes notes,
    p.gender,
    p.active is_active,
    p.dob,
    p.number_primary payer_member_id,
    p.number_secondary payer_member_id_2,
    p.number_other     payer_member_id_3,
    p.region         ,
    p.jurisdiction_id,
    p.last_patient_status_id_deprecated depricated_payer_id,
    p.elig_begin_date,
    p.elig_end_date,
    p.program_id,
    p.program_other,
    p.last_patient_status_id,
    p.deceased_error,
    p.duplicate_error,
    p.eligible_method_id,
    p.choices_group,
    l.lmid         address_id,
    l.radius       radius,
    l.`primary`    is_primary,
    replace(l.address,',','')      address,
    l.city         city,
    l.county       county,
    l.state        state,
    l.zip          zip,
    l.ttimezone    time_zone,
    l.locale       locale,
    l.dst          dst,
    l.active       is_active,
    l.deleted      is_deleted,
    l.geocoded     is_geocoded
from
    PATIENTS p
    join LANDMARKS l on l.PATID = p.PATID
;



