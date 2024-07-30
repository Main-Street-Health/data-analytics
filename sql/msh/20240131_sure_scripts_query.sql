
DROP TABLE IF EXISTS _potential_patients;
CREATE TEMP TABLE _potential_patients AS
SELECT
    p.id                                                                                patient_id
  , pmay.measure_id                                                                     measure_id
  , pmay.id IS NOT NULL                                                                 has_measure
  , pmay.next_fill_date                                                                 next_fill_date
  , pmay.adr                                                                            adr
  , MAX(pp.inserted_at) FILTER ( WHERE ssr.hdr_load_status_description !=
                                       'Failed to load file.  No data was processed.' ) last_panel_created_at
  , MAX(pt.id) FILTER (WHERE pt.status IN ('new', 'in_progress')) IS NOT NULL           has_open_task
  , min(pt.inserted_at) FILTER (WHERE pt.status IN ('new', 'in_progress'))              open_task_inserted_at
  , MAX(med_ad_pat_task.visit_date) FILTER (WHERE pt.status IN ('new', 'in_progress'))  open_task_visit_date
  , MAX(med_ad_pat_task.expected_discharge_date)
      FILTER (WHERE pt.status = 'completed'
                AND NOT med_ad_pat_task.is_system_verified_closed)                      closed_task_discharge_date
  , MAX(pt.updated_at) FILTER (WHERE pt.status = 'completed'
                                   AND NOT med_ad_pat_task.is_system_verified_closed)   closed_non_verified_task_updated_at
  , MAX(mhd.id) IS NOT NULL                                                             pulled_but_no_ss_hit_previously
  , ARRAY_AGG(DISTINCT mhd.note)                                                        previous_ss_notes
    -- prescription fill tasks fields below
  , min(pqmt.inserted_at) FILTER (WHERE pqmt.status IN ('new', 'in_progress'))          open_rx_fill_task_inserted_at
  , MAX(pqmt.updated_at) FILTER (WHERE pqmt.status = 'completed'
                                        AND NOT pfpt.is_system_verified_closed)         closed_non_verified_rx_fill_task_updated_at
  , string_agg(distinct pqmt.task_type, ',')
                                 FILTER ( WHERE pqmt.status in ('new', 'in_progress') ) open_rx_fill_task_types
  , string_agg(distinct pqmt.task_type, ',') FILTER ( WHERE pqmt.status = 'completed' ) closed_rx_fill_task_types
-- external osteo order
  , max(pt_ext_osteo.updated_at)                                                        max_upd_ext_osteo_complete_at
FROM
--         junk.med_adherence_pilot_care_teams_20230327 pct
--         join fdw_member_doc.patients p  on pct.care_team_id = p.care_team_id
    fdw_member_doc.patients p
    JOIN fdw_member_doc.supreme_pizza sp on sp.patient_id = p.id and sp.is_medication_adherence
    JOIN fdw_member_doc.referring_partners rp on rp.id = sp.primary_referring_partner_id and rp.organization_id != 7
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pt.patient_id = p.id
        AND pt.task_type IN ('med_adherence_cholesterol', 'med_adherence_diabetes', 'med_adherence_hypertension')
    LEFT JOIN fdw_member_doc.medication_adherence_patient_task med_ad_pat_task
              ON pt.id = med_ad_pat_task.patient_task_id
    LEFT JOIN prd.patient_med_adherence_year_measures pmay ON pmay.patient_id = p.id
        AND pmay.year = EXTRACT('year' FROM NOW())
    LEFT JOIN public.sure_scripts_panel_patients pp ON pp.patient_id = p.id
    LEFT JOIN public.sure_scripts_responses ssr ON ssr.sure_scripts_panel_id = pp.sure_scripts_panel_id
    LEFT JOIN public.sure_scripts_med_history_details mhd ON p.id = mhd.patient_id::BIGINT
        AND mhd.note IS NOT NULL
    LEFT JOIN fdw_member_doc.patient_tasks pqmt on pqmt.patient_id = p.id
        AND pqmt.task_type IN ('prescription_fill_osteoporosis', 'prescription_fill_statin_diabetes', 'prescription_fill_statin_cvd') --prescription_fill_osteoporosis -- 12 prescription_fill_statin_diabetes -- 18 prescription_fill_statin_cvd -- 17
    left JOIN fdw_member_doc.prescription_fill_patient_task pfpt on pfpt.patient_task_id = pqmt.id
    left join fdw_member_doc.patient_tasks pt_ext_osteo on pt_ext_osteo.patient_id = p.id
                                                       and pt_ext_osteo.task_type = 'recon_osteoporosis_external_order'
                                                       and pt_ext_osteo.status = 'completed'
GROUP BY
    1,2,3,4,5;


-- SELECT *
-- FROM
--     _patients_to_pull;
-- INSERT
-- INTO
--     _patients_to_pull (patient_id, reason)
-- SELECT
--     patient_id, 'initial_2023_query_to_hit_ss_minimum_pilot_locs' reason
-- --     COUNT(*)
-- FROM
--     fdw_member_doc.supreme_pizza
-- WHERE
--       primary_referring_partner_id IN (135, 345, 829, 133, 133, 275, 266, 302, 67, 162)
--   AND is_medication_adherence
-- ON CONFLICT DO NOTHING
-- ;

DROP TABLE IF EXISTS _patients_to_pull;
CREATE TEMP TABLE _patients_to_pull (
    patient_id BIGINT NOT NULL,
    reason     TEXT   NOT NULL,
    UNIQUE (patient_id)
);
SELECT count(*) FROM _potential_patients;
SELECT
    COUNT(*)
FROM
    _potential_patients pp
WHERE
    NOT EXISTS( SELECT
                    1
                FROM
                    sure_scripts_panel_patients pp2
                WHERE
                      pp.patient_id = pp2.patient_id
                  AND pp2.inserted_at > NOW() - '1 week'::INTERVAL );

SELECT pp.inserted_at::date, count(*), count(distinct pp.patient_id)
FROM
    sure_scripts_panel_patients  pp
GROUP BY 1
order by 1 desc
limit 10
;

INSERT
INTO
    _patients_to_pull (patient_id, reason)
SELECT
    x.patient_id
  , 'initial_2023_query_to_hit_ss_minimum' reason
FROM
    ( SELECT
          pp.patient_id
        , MIN(y.absolute_fail_date) absolute_fail_date
      FROM
          _potential_patients pp
          LEFT JOIN prd.patient_med_adherence_year_measures y ON y.patient_id = pp.patient_id
              AND y.fill_count > 1
      WHERE
          NOT EXISTS( SELECT
                          1
                      FROM
                          sure_scripts_panel_patients pp2
                      WHERE
                            pp.patient_id = pp2.patient_id
                        AND pp2.inserted_at > NOW() - '1 week'::INTERVAL )
      GROUP BY 1
      ORDER BY 2
      LIMIT 70000 ) x
;

;

-- DELETE from _patients_to_pull ptp where exists(select 1 from sure_scripts_panel_patients pp where pp.patient_id = ptp.patient_id and pp.sure_scripts_panel_id isnull)



INSERT
INTO
    public.sure_scripts_panel_patients (patient_id, sequence, last_name, first_name, middle_name, prefix,
                                        suffix, address_line_1, address_line_2, city, state, zip, dob, gender, npi,
                                        updated_at, inserted_at, reason_for_query)
SELECT DISTINCT
    ptp.patient_id
  , ROW_NUMBER() OVER (ORDER BY ptp.patient_id)           sequence
  , REGEXP_REPLACE(p.last_name, E'[\\n\\r]+', '', 'g')    last_name
  , REGEXP_REPLACE(p.first_name, E'[\\n\\r]+', '', 'g')   first_name
  , NULL                                                  middle_name
  , NULL                                                  prefix
  , NULL                                                  suffix
  , REGEXP_REPLACE(pa.line1, E'[\\n\\r]+', '', 'g')       address_line_1
  , REGEXP_REPLACE(pa.line2, E'[\\n\\r]+', '', 'g')       address_line_2
  , REGEXP_REPLACE(pa.city, E'[\\n\\r]+', '', 'g')        city
  , REGEXP_REPLACE(pa.state, E'[\\n\\r]+', '', 'g')       state
  , REGEXP_REPLACE(pa.postal_code, E'[\\n\\r]+', '', 'g') zip
  , p.dob
  , LEFT(p.gender, 1)                                     gender
  , coalesce(mp.npi::TEXT, '1023087954')                  npi
  , NOW()                                                 updated_at
  , NOW()                                                 inserted_at
  , ptp.reason
FROM
    _patients_to_pull ptp
    JOIN fdw_member_doc.patients p ON ptp.patient_id = p.id
    JOIN fdw_member_doc.patient_addresses pa ON p.id = pa.patient_id
    LEFT JOIN fdw_member_doc.patient_contacts pc
              ON p.id = pc.patient_id AND pc.relationship = 'physician' AND pc.is_primary
    LEFT JOIN fdw_member_doc.msh_physicians mp ON mp.contact_id = pc.contact_id AND mp.npi IS NOT NULL
WHERE
      -- don't add if patient already exists
      NOT EXISTS(SELECT
                     1
                 FROM
                     public.sure_scripts_panel_patients sspp
                 WHERE
                       sspp.sure_scripts_panel_id ISNULL
                   AND sspp.patient_id = ptp.patient_id)
      and length(p.first_name) >= 2 -- SS requires Two of a person's names (Last Name, First Name, Middle Name) must have 2 or more characters.
      and length(p.last_name) >= 2
--       and length(REGEXP_REPLACE(pa.city, E'[\\n\\r]+', '', 'g')) >= 2
;

SELECT count(*) FROM sure_scripts_panel_patients WHERE sure_scripts_panel_id ISNULL ;

SELECT * FROM analytics.oban.oban_crons WHERE name ~* 'sure';

INSERT
INTO
    oban.oban_jobs (queue, worker, args, max_attempts, inserted_at, scheduled_at, state)
VALUES
    ('sure_scripts', 'Deus.SureScripts.SureScriptsWorker', '{
      "action": "generate_panel"
    }', 1, NOW(), NOW(), 'available')
returning *
;

SELECT * FROM oban.oban_jobs where id = 99228798;
SELECT * FROM oban.oban_jobs where id = 99329057;

SELECT * FROM oban.oban_jobs where queue = 'sure_scripts';
SELECT * FROM fdw_file_router.ftp_servers WHERE name ~* 'sure_scripts';

SELECT * FROM sure_scripts_responses order by id desc;

-- TODO: address these errors, add guards
SELECT error_description, count(*)
FROM
    sure_scripts_response_details
WHERE
    sure_scripts_response_id = 10297
group by 1
order by 2 desc
;
-- ORDER BY
--     id DESC;

SELECT *
FROM
    sure_scripts_med_histories
WHERE
    id > 9406
ORDER BY
    id DESC;

SELECT *
FROM
    sure_scripts_med_history_details where sure_scripts_med_history_id > 9406;


-- SELECT * FROM fdw_file_router.retrieval_batches WHERE ftp_server_id = 486 order by id desc;
SELECT *
FROM
    fdw_file_router.external_files
WHERE
    ftp_server_id = 486
and s3_key is not null
ORDER BY
    id DESC;

SELECT
    p.id
   , p.inserted_at ::DATE
  , r.inserted_at - p.inserted_at resp_time
  , h.inserted_at - p.inserted_at med_hist_time
   , now() - p.inserted_at med_hist_time
FROM
    sure_scripts_panels p
    left JOIN sure_scripts_responses r ON p.id = r.sure_scripts_panel_id
    left JOIN sure_scripts_med_histories h ON p.id = h.sure_scripts_panel_id
where p.inserted_at > '2024-01-01' or h.id is not null
ORDER BY
    4 DESC
;


