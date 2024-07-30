-- drop TABLE junk.sure_scripts_refresh_20230327;
-- DROP TABLE IF EXISTS junk.sure_scripts_pids_to_refresh_20230327;
CREATE  TABLE junk.sure_scripts_pids_to_refresh_20230327 AS
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
    ;
SELECT *
FROM
    junk.sure_scripts_pids_to_refresh_20230327 WHERE patient_id = 119120;;


-- CREATE OR REPLACE VIEW v_sure_scripts_patients AS
SELECT DISTINCT ON (lcp.patient_id)
    lcp.patient_id
  , REGEXP_REPLACE(TRIM(p.last_name), '[\n\r]+', ' ', 'g')  last_name
  , REGEXP_REPLACE(TRIM(p.first_name), '[\n\r]+', ' ', 'g') first_name
  , NULL                                                    middle_name
  , NULL                                                    prefix
  , NULL                                                    suffix
  , REGEXP_REPLACE(TRIM(pa.line1), '[\n\r]+', ' ', 'g')     address_line_1
  , REGEXP_REPLACE(TRIM(pa.line2), '[\n\r]+', ' ', 'g')     address_line_2
  , REGEXP_REPLACE(TRIM(pa.city), '[\n\r]+', ' ', 'g')      city
  , REGEXP_REPLACE(TRIM(pa.state), '[\n\r]+', ' ', 'g')     state
  , pa.postal_code                                          zip
  , p.dob
  , LEFT(p.gender, 1)                                       gender
  , mp.npi::TEXT                                            npi
FROM
--     junk.sure_scripts_red_list_20230308 jp
    junk.sure_scripts_pids_to_refresh_20230327 pids
join fdw_member_doc.layer_cake_patients lcp on lcp.patient_id = pids.patient_id
JOIN fdw_member_doc.patients p ON lcp.patient_id = p.id
JOIN fdw_member_doc.patient_addresses pa ON p.id = pa.patient_id
JOIN fdw_member_doc.patient_contacts pc ON p.id = pc.patient_id AND pc.relationship = 'physician'
JOIN fdw_member_doc.msh_physicians mp ON mp.contact_id = pc.contact_id AND mp.npi IS NOT NULL
WHERE lcp.is_medication_adherence
ORDER BY
    lcp.patient_id, CASE WHEN COALESCE(pc.is_primary, FALSE) THEN -1 ELSE pc.id END;


SELECT *
FROM
    sure_scripts_panels p
join sure_scripts_panel_patients pp on p.id = pp.sure_scripts_panel_id
where p.id = 166
and pp.patient_id = 68957
;
SELECT *
FROM
    sure_scripts_responses r
left join sure_scripts_response_details rd on r.id = rd.sure_scripts_response_id
WHERE
    r.sure_scripts_panel_id = 166;

SELECT *
FROM
    sure_scripts_med_histories mh
    LEFT JOIN sure_scripts_med_history_details mhd ON mh.id = mhd.sure_scripts_med_history_id
WHERE
    mh.sure_scripts_panel_id = 166;

SELECT count(distinct npi)
FROM
    fdw_member_doc.msh_physicians;
