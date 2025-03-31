------------------------------------------------------------------------------------------------------------------------
/* from sean */
------------------------------------------------------------------------------------------------------------------------
drop table if exists _pats;
create temp table _pats as
with cte as (
select
    x.inbound_file_id,
    x.id raw_id,
    gm.patient_id,
    x.bene_mbi_id mbi,
    x.payer_id,
    smm.member_id,
    substring(x.meta->'extra_args'->>'original_name' from 'A\d{4}') contract_id,
    (substring(x.meta->'extra_args'->>'original_name' from '.*(\d{4})Q'))::int measure_year,
    coalesce(htn_age = '1' and htn_dx = '1' and htn_encounter = '1' and htn_exclusion = '.',false) is_cbp,
    coalesce(dep_age = '1' and dep_encounter = '1' and dep_exclusion = '.',false) is_depression,
    coalesce(dm_age = '1' and dm_dx = '1' and dm_encounter = '1' and dm_exclusion = '.',false) is_hbd

from
    raw.mssp_alr_mcqm x
        left join prd.source_member_mappings smm on smm.payer_member_id = lower(trim(x.bene_mbi_id))
            and smm.is_mbi
            and smm.payer_id = 81
        left join gmm.global_mco_member_mappings mco on mco.member_id = smm.member_id
        left join gmm.global_members gm on gm.id = mco.global_member_id and gm.patient_id is not null
where
    inbound_file_id >= 20936004 /*2024Q4*/), filt as (
select *
from cte
where is_cbp or is_depression or is_hbd)
select distinct on (mbi, measure_key)
    raw_id,
    inbound_file_id,
    patient_id,
    measure_year,
    mbi,
    payer_id,
    member_id,
    contract_id,
    measure_key,
    'proxy' measure_source_key,
    'pending_scheduling_clinic' measure_status_key
from filt
        cross join lateral ( values
            ('cbp_controlling_high_blood_pressure', is_cbp),
            ('hbd_hemoglobin_a1c_control_for_patients_with_diabetes', is_hbd),
            ('dsf_depression', is_depression)
            ) dx (measure_key, is_active)
where
    is_active
order by mbi, measure_key, contract_id
;

drop table if exists _qms;
create temp table _qms as
select * from fdw_member_doc.qm_patient_measures where operational_year = 2024;

drop table if exists _chase;
create temp table _chase as
select distinct unnest(qm_patient_measure_ids) qm_patient_measure_id, external_chase_id, id msh_chase_id
from public.reveleer_chases;
create unique index udx__chase on _chase(qm_patient_measure_id, external_chase_id);
create index on _chase( msh_chase_id);

drop table if exists _unpivoted;
CREATE TEMP TABLE _unpivoted AS
SELECT distinct on (qpm.id)
--     p.raw_id
--   , p.inbound_file_id
    p.patient_id
--   , p.measure_year
  , p.mbi
--   , p.payer_id
--   , p.member_id
  , p.contract_id
--   , p.measure_key
  , qpm.id               qm_patient_measure_id
--      , qpm.is_active
--   , ch.external_chase_id rev_chase_id -- NEED THIS
--   , ch.msh_chase_id
FROM
    _pats p
    JOIN _qms qpm ON qpm.patient_id = p.patient_id
        AND qpm.measure_key = p.measure_key
    order by qpm.id
--     LEFT JOIN _chase ch ON ch.qm_patient_measure_id = qpm.id
;
    create index on _unpivoted(qm_patient_measure_id);
    ---


CREATE TABLE junk.mssp_for_quest_20250328 AS
SELECT DISTINCT
    pqm.patient_id
FROM
    fdw_member_doc.qm_patient_measures pqm
    JOIN _unpivoted u ON u.qm_patient_measure_id = pqm.id;

CREATE UNIQUE INDEX ON junk.mssp_for_quest_20250328(patient_id);

SELECT count(*)
FROM
    junk.mssp_for_quest_20250328;
-- JOIN junk.mssp_for_quest_20250328 j on j.patient_id = qpm.patient_id

-- DELETE
select count(*)
FROM
    quest_roster_patients
WHERE quest_roster_id ISNULL ;