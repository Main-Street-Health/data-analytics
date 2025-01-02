
select * from qm_pm_blood_pressures_handoff
select * from qm_pm_breast_cancer_screening_handoff
select * from qm_pm_colorectal_screening_handoff
select * from qm_pm_depression_handoff
select * from qm_pm_eye_exam_handoff
select * from qm_pm_functional_assessment_handoff
select * from qm_pm_hbd_handoff
select * from qm_pm_kidney_health_evaluations_handoff
select * from qm_pm_medication_reviews_handoff
select * from qm_pm_osteoporosis_management_handoff
select * from qm_pm_pain_assessment_handoff
;

select
    is_quality_measures,
    count(1) n,
    count(distinct p.id) nd
from
    patients p
    join supreme_pizza sp on p.id = sp.patient_id
group by 1

select
    -- patient_status,
    -- patient_substatus,
    p.status,
    p.substatus,
    is_quality_measures,
    is_proxy_gaps,
    count(distinct sp.patient_id) nd
from
    patients p
    join patient_referring_partners prp on p.id = prp.patient_id and prp."primary"
    join referring_partners rp on prp.referring_partner_id = rp.id
    join supreme_pizza sp on p.id = sp.patient_id
    join care_teams ct on p.care_team_id = ct.id
    join care_team_members ctm on ct.id = ctm.care_team_id and ctm.role = 'health_navigator'
group by 1,2,3,4
;
drop table if exists junk.qm_patients_to_test;
create table junk.qm_patients_to_test as
select * from (
    select
        p.id        patient_id,
        rp.id       primary_referring_partner_id,
        true        is_quality_measures,
        true        is_proxy_gaps,
        p.status    patient_status,
        p.substatus patient_substatus,
        row_number() over ( order by random() ) rn
    from
        patients p
        join patient_referring_partners prp on p.id = prp.patient_id and prp."primary"
        join referring_partners rp on prp.referring_partner_id = rp.id
        join supreme_pizza sp on p.id = sp.patient_id
        join care_teams ct on p.care_team_id = ct.id
        join care_team_members ctm on ct.id = ctm.care_team_id and ctm.role = 'health_navigator'
    where
        not exists (select 1 from qm_patient_measures qpm where qpm.patient_id = p.id)
        and p.status in ('active', 'potential')
) x
where rn <= 10000
;
    -- QA
    select count(1) n, count(distinct patient_id) from junk.qm_patients_to_test qp

update supreme_pizza sp
set
    is_quality_measures          = qp.is_quality_measures,
    is_proxy_gaps                = qp.is_proxy_gaps,
    primary_referring_partner_id = qp.primary_referring_partner_id,
    patient_status               = qp.patient_status,
    patient_substatus            = qp.patient_substatus
from
    junk.qm_patients_to_test qp
where
    qp.patient_id = sp.patient_id
;

create table junk.qm_5k_patients_to_use_2024 as
select
    patient_id,
    2024                        measure_year,
    'proxy'                     measure_source_key,
    'pending_scheduling_clinic' measure_status_key,
    null::timestamp             processed_at,
    now()                       inserted_at,
    true                        is_active_patient_measure
from
    junk.qm_patients_to_test qp
where
    qp.rn <= 5000
;


insert into qm_pm_blood_pressures_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_breast_cancer_screening_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_colorectal_screening_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_depression_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_eye_exam_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_functional_assessment_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_hbd_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_kidney_health_evaluations_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_medication_reviews_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_osteoporosis_management_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure, must_close_by_date
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure, current_date + 170 must_close_by_date
from
    junk.qm_5k_patients_to_use_2024
;

insert into qm_pm_pain_assessment_handoff(
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
)
select
    patient_id, measure_year, measure_source_key, measure_status_key, processed_at, inserted_at, is_active_patient_measure
from
    junk.qm_5k_patients_to_use_2024
;


select 'qm_pm_blood_pressures_handoff' _,           (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_blood_pressures_handoff           where inserted_at::date >= current_date union all
select 'qm_pm_breast_cancer_screening_handoff' _,   (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_breast_cancer_screening_handoff   where inserted_at::date >= current_date union all
select 'qm_pm_colorectal_screening_handoff' _,      (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_colorectal_screening_handoff      where inserted_at::date >= current_date union all
select 'qm_pm_depression_handoff' _,                (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_depression_handoff                where inserted_at::date >= current_date union all
select 'qm_pm_eye_exam_handoff' _,                  (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_eye_exam_handoff                  where inserted_at::date >= current_date union all
select 'qm_pm_functional_assessment_handoff' _,     (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_functional_assessment_handoff     where inserted_at::date >= current_date union all
select 'qm_pm_hbd_handoff' _,                       (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_hbd_handoff                       where inserted_at::date >= current_date union all
select 'qm_pm_kidney_health_evaluations_handoff' _, (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_kidney_health_evaluations_handoff where inserted_at::date >= current_date union all
select 'qm_pm_medication_reviews_handoff' _,        (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_medication_reviews_handoff        where inserted_at::date >= current_date union all
select 'qm_pm_osteoporosis_management_handoff' _,   (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_osteoporosis_management_handoff   where inserted_at::date >= current_date union all
select 'qm_pm_pain_assessment_handoff' _,           (max(processed_at) - min(processed_at))  time_to_process, count(distinct patient_id) nd, count(1) n, count(distinct patient_id) filter ( where processed_at is not null ) nd_processed from qm_pm_pain_assessment_handoff           where inserted_at::date >= current_date
order by time_to_process desc
;