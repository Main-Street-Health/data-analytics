--         - ss_med_hist_details -> <<patient_medications -(synth)-> pmam>> -> stage.data

DROP TABLE IF EXISTS _pilot_med_adherences;
CREATE TEMP TABLE _pilot_med_adherences AS
SELECT *
FROM
    fdw_member_doc_stage.patient_medication_adherences;

-- pilot_stage_med_adherences_20230328
SELECT *
FROM
    _pilot_med_adherences;

-- pilot_prd_med_adherences_20230328
SELECT pmam.next_fill_date, stg.next_fill_date, stg.adjusted_next_fill_date
FROM
    _pilot_med_adherences stg
join prd.patient_med_adherence_measures pmam on pmam.id = stg.analytics_id;

-- pilot_synth_periods_med_adherences_20230328
SELECT distinct pmam.id analytics_id, sp.*
FROM
    _pilot_med_adherences stg
join prd.patient_med_adherence_measures pmam on pmam.id = stg.analytics_id
join prd.patient_med_adherence_synth_periods sp on sp.id = any(pmam.patient_med_adherence_synth_period_ids)

-- pilot_patient_medications_med_adherences_20230328
SELECT distinct pmam.id analytics_id, pm.*
FROM
    _pilot_med_adherences stg
join prd.patient_med_adherence_measures pmam on pmam.id = stg.analytics_id
join prd.patient_medications pm on pm.id = any(pmam.patient_medication_ids)

-- pilot_ss_med_hist_detail_med_adherences_20230328
WITH
    alll AS ( SELECT
                  pmam.id            analytics_id
                , UNNEST(pm.sources) src
              FROM
                  _pilot_med_adherences stg
                  JOIN prd.patient_med_adherence_measures pmam ON pmam.id = stg.analytics_id
                  JOIN prd.patient_medications pm ON pm.id = ANY (pmam.patient_medication_ids) )
SELECT DISTINCT
    a.analytics_id
  , mhd.*
FROM
    alll a
    JOIN public.sure_scripts_med_history_details mhd ON mhd.id = (src -> 'id')::BIGINT
;

SELECT *
FROM
    sure_scripts_med_history_details
WHERE
      patient_id = '103010'
  AND sure_scripts_med_history_id = 133
ORDER BY
    COALESCE(sold_date, last_filled_date, written_date) DESC
;


SELECT * FROM sure_scripts_med_histories;





-- CREATE OR REPLACE VIEW v_sure_scripts_patients AS
SELECT  DISTINCT ON (pids.patient_id)
    pids.patient_id
--   , REGEXP_REPLACE(TRIM(p.last_name), '[\n\r]+', ' ', 'g')  last_name
--   , REGEXP_REPLACE(TRIM(p.first_name), '[\n\r]+', ' ', 'g') first_name
--   , NULL                                                    middle_name
--   , NULL                                                    prefix
--   , NULL                                                    suffix
--   , REGEXP_REPLACE(TRIM(pa.line1), '[\n\r]+', ' ', 'g')     address_line_1
--   , REGEXP_REPLACE(TRIM(pa.line2), '[\n\r]+', ' ', 'g')     address_line_2
--   , REGEXP_REPLACE(TRIM(pa.city), '[\n\r]+', ' ', 'g')      city
--   , REGEXP_REPLACE(TRIM(pa.state), '[\n\r]+', ' ', 'g')     state
--   , pa.postal_code                                          zip
--   , p.dob
--   , LEFT(p.gender, 1)                                       gender
--   , mp.npi::TEXT                                            npi
FROM
--     junk.sure_scripts_red_list_20230308 jp
    junk.sure_scripts_pids_to_refresh_20230327 pids
join fdw_member_doc.layer_cake_patients lcp on lcp.patient_id = pids.patient_id
JOIN fdw_member_doc.patients p ON lcp.patient_id = p.id
JOIN fdw_member_doc.patient_addresses pa ON p.id = pa.patient_id
JOIN fdw_member_doc.patient_contacts pc ON p.id = pc.patient_id AND pc.relationship = 'physician'
JOIN fdw_member_doc.msh_physicians mp ON mp.contact_id = pc.contact_id AND mp.npi IS NOT NULL
WHERE lcp.is_medication_adherence
-- and pids.patient_id = 119120
-- and lcp.is_medication_adherence
ORDER BY
    pids.patient_id, CASE WHEN COALESCE(pc.is_primary, FALSE) THEN -1 ELSE pc.id END;

-- SELECT * FROM junk.sure_scripts_pids_to_refresh_20230327 where patient_id = 119120;

SELECT * FROM prd.patient_med_adherence_measures WHERE patient_id = 119120 ;
SELECT * FROM prd.patient_medications WHERE patient_id = 119120 and id = 122809;

SELECT * FROM sure_scripts_panel_patients WHERE patient_id = 119120;

SELECT inserted_at AT TIME ZONE 'america/chicago', * FROM fdw_member_doc.patient_medication_adherences WHERE patient_id = 119120;


SELECT DISTINCT
    pt.patient_id
FROM
    fdw_member_doc.patient_tasks pt
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pt.patient_id
    JOIN fdw_member_doc.referring_partners rp ON rp.id = sp.primary_referring_partner_id
    LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rp.organization_id = rpo.id
    JOIN fdw_member_doc.users u ON pt.assigned_to_id = u.id
    JOIN fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
    JOIN fdw_member_doc.patient_medication_adherences pma ON pma.id = mapt.patient_medication_adherence_id
    JOIN fdw_member_doc.patients p ON pt.patient_id = p.id
    JOIN fdw_member_doc.care_teams ct ON ct.id = p.care_team_id
WHERE
        pt.task_type IN ('med_adherence_cholesterol', 'med_adherence_diabetes', 'med_adherence_hypertension')
  AND   NOT EXISTS(SELECT
                       1
                   FROM
                       sure_scripts_panel_patients pp
                   WHERE
                         pp.patient_id = pt.patient_id
                     AND pp.inserted_at > NOW() - '3 days'::INTERVAL)
--   AND   pt.patient_id = 119120;

    ;
with qq  as (
    SELECT DISTINCT
    pt.patient_id
FROM
    fdw_member_doc.patient_tasks pt
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pt.patient_id
    JOIN fdw_member_doc.referring_partners rp ON rp.id = sp.primary_referring_partner_id
    LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rp.organization_id = rpo.id
    JOIN fdw_member_doc.users u ON pt.assigned_to_id = u.id
    JOIN fdw_member_doc.medication_adherence_patient_task mapt ON pt.id = mapt.patient_task_id
    JOIN fdw_member_doc.patient_medication_adherences pma ON pma.id = mapt.patient_medication_adherence_id
    JOIN fdw_member_doc.patients p ON pt.patient_id = p.id
    JOIN fdw_member_doc.care_teams ct ON ct.id = p.care_team_id
WHERE
        pt.task_type IN ('med_adherence_cholesterol', 'med_adherence_diabetes', 'med_adherence_hypertension')
)
SELECT *
FROM
    qq
    left join junk.sure_scripts_pids_to_refresh_20230327 j on j.patient_id = qq.patient_id
where j.patient_id ISNULL
;
-- call sp_populate_sure_scripts_panel_patients()

SELECT *
FROM
--     prd.patient_med_adherence_measures
--     sure_scripts_med_history_details
    sure_scripts_panel_patients
WHERE
    patient_id = '117666 '
SELECT *
FROM
    prd.patient_med_adherence_measures;
------------------------------------------------------------------------------------------------------------------------
/* not sent to ss  */
------------------------------------------------------------------------------------------------------------------------
drop table if exists _not_sent_to_ss;
create temp table _not_sent_to_ss as
select p.id patient_id, p.full_name, p.dob, mp.npi, pct.care_team_id
from
    fdw_member_doc.patients p
left join fdw_member_doc.patient_contacts pc on pc.patient_id = p.id and pc.relationship = 'physician' and pc.is_primary
left join fdw_member_doc.msh_physicians mp on mp.contact_id = pc.contact_id
left join junk.med_adherence_pilot_care_teams_20230327 pct on pct.care_team_id = p.care_team_id
where p.id in (951,3445,3484,4414,5759,6189,6215,6386,6453,6499,6779,9317,11281,12513,14751,17887,19166,27687,31414,34558,36968,37997,38016,38210,38219,38319,38505,43892,44011,44633,48674,50210,63580,64544,65558,66110,66786,67899,70043,77087,83212,88078,94650,95184,95681,96621,144325,146857,151647,151825,154525,154536,156011,196808,197594,199180,209827,211215,250149,270213,308711,308717,308720,308901,310065,311274,318655,318775,319032,319657,319926,320503,320782,321693,322007,333241,333972,334733,334790,334913,335685,335725,336389,336452,336467,336637,342197,342644,342653,342655,342672,342674,342679,344541,344748,345315,345320,345973,346136,346396,348162,349319,349324,349328,349329,349349,349376,349381,349395,349417,349422,349428,349429,349437,349441,349461,349464,349465,349480,349484,349492,349495,349512,349519,349523,349526,349527,354122,354123,354124,354125,354126,354127,354128,354130,354131,354134,354137,354139,354144,354147,354154,354155,354158,354159,354162,354163,354164,354166,354168,354173,354174,354175,354177,354178,354180,354185,354189,354190,354191,354199,354202,354205,354209,354210,354213,354221,354222,354227,354229,354239,354240,354241,354247,354251)
;


with
    facts as ( select
                   ns.patient_id
                 , ns.full_name
                 , ns.dob
                 , ns.npi
                 , lcp.patient_id is not null    is_in_layer_cake
                 , ic.patient_id is not null     is_roster_conf
                 , ns.care_team_id is not null   is_in_pilot
                 , max(pp.sure_scripts_panel_id) panel_id
               from
                   _not_sent_to_ss ns
                   left join sure_scripts_panel_patients pp on pp.patient_id = ns.patient_id
                   left join fdw_member_doc.layer_cake_patients lcp
                             on lcp.patient_id = ns.patient_id and lcp.is_medication_adherence
                   left join fdw_member_doc.msh_patient_integration_configs ic on lcp.patient_id = ic.patient_id
               group by
                   ns.patient_id
                 , ns.full_name
                 , ns.dob
                 , ns.npi
                 , lcp.patient_id
                 , ic.patient_id
                 , ns.care_team_id )
  , agged as ( select
                   f.patient_id
                 , f.full_name
                 , f.dob
                 , case when panel_id is not null then 'is_in_panel'
                        when not is_roster_conf   then 'not_in_roster'
                        when not is_in_layer_cake then 'not_in_layer_cake'
                        when not is_in_pilot      then 'not_in_pilot'
                        else 'unknown' end reason
               from
                   facts f )
-- select * from agged a where reason = 'unknown';
select
    a.reason
  , count(patient_id)          n
  , count(distinct patient_id) nd
from
    agged a
group by
    1
;

;
38016
38210


;


SELECT *
FROM
    sure_scripts_panel_patients
WHERE
    patient_id = '4414';
SELECT * FROM sure_scripts_med_history_details WHERE sure_scripts_panel_id = 35 and patient_id = '4414';
SELECT * FROM sure_scripts_response_details WHERE sure_scripts_panel_id = 35 and patient_id = 4414;
