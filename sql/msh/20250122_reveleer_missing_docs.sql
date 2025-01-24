SELECT *
FROM
    reveleer_chases rc
-- join reveleer_cca_pdfs rcp ON rc.id = rcp.reveleer_chase_id
where rc.yr = 2024
and rc.measure_code = 'DSF'
;

DROP TABLE IF EXISTS _dsf_pats;
CREATE TEMP TABLE _dsf_pats AS
SELECT distinct patient_id
              FROM
                  reveleer_chases rc
              WHERE
                    rc.yr = 2024
                AND rc.measure_code = 'DSF'
;
CREATE UNIQUE INDEX on _dsf_pats(patient_id);

SELECT count(*) FROM _dsf_pats; --2895

DROP TABLE IF EXISTS _ccas;
CREATE TEMP TABLE _ccas AS
SELECT
    dp.patient_id
, d.id document_id
FROM
--     reveleer_cca_pdfs
fdw_member_doc.documents d
JOIN fdw_member_doc.msh_cca_worksheets mcw ON d.id = mcw.document_id
JOIN fdw_member_doc.visits v ON mcw.visit_id = v.id AND DATE_PART('year', v.date) = 2024
join _dsf_pats dp ON d.patient_id = dp.patient_id
where d.deleted_at ISNULL;

SELECT count(*) FROM _ccas; -- 2790

SELECT count(*)
FROM
    _ccas c
join reveleer_cca_pdfs p on p.document_id = c.document_id
;

select * from inbound_file_config where ftp_server_name = 'cigna_quality_pcr_report';

------------------------------------------------------------------------------------------------------------------------
/* configure FMC, MRP and PED for Viva and Wellmark */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    fdw_member_doc.payers p where p.name ~* 'viva|wellmark';
id,name
50,Wellmark
48,Viva
SELECT distinct unnest(measures_to_send)
FROM
    reveleer_projects rp
order by 1
;
SELECT rp.*
FROM
    fdw_member_doc.qm_mco_patient_measures mco
join fdw_member_doc.qm_patient_measures pm on pm.mco_patient_measure_id = mco.id
join fdw_member_doc.qm_ref_patient_measure_statuses ms on ms.key = pm.measure_status_key and ms.send_to_reveleer
join fdw_member_doc.supreme_pizza sp on sp.patient_id = pm.patient_id and sp.is_quality_measures
join reveleer_projects rp ON mco.payer_id = rp.payer_id and pm.measure_key = any(rp.measures_to_send)
join reveleer_chases rc on pm.id = any(rc.qm_patient_measure_ids)
WHERE
      mco.payer_id IN (48, 50)
  AND mco.measure_key IN (
                      'fmc_follow_up_after_ed_visit_multiple_chronic_conditions',
                      'trc_mrp_medication_reconciliation_post_discharge',
                      'trc_peid_patient_engagement_after_ip_discharge'
    )
and mco.measure_year = 2024
and pm.measure_source_key = 'mco'
and pm.is_active
;
select *
FROM
        fdw_member_doc.qm_patient_measures pqm
        JOIN fdw_member_doc.qm_mco_patient_measures mpm ON pqm.mco_patient_measure_id = mpm.id
    and mpm.payer_id IN (48, 50)
        JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--         JOIN public.reveleer_projects ptr ON mpm.payer_id = ptr.payer_id
        JOIN (select id, unnest(measures_to_send) measures_to_send, payer_id from public.reveleer_projects) ptr ON mpm.payer_id = ptr.payer_id
        JOIN fdw_member_doc.payers pay ON pay.id = mpm.payer_id and pay.id IN (48, 50)
        JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
            and m.key in (
                      'fmc_follow_up_after_ed_visit_multiple_chronic_conditions',
                      'trc_mrp_medication_reconciliation_post_discharge',
                      'trc_peid_patient_engagement_after_ip_discharge'
    )
        JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
    WHERE
          pqm.operational_year = 2024
      AND pqm.measure_source_key = 'mco'
      AND pqm.is_active
      AND sp.is_quality_measures
      -- need to include closed system for compliance file
      AND (st.send_to_reveleer or pqm.measure_status_key = 'closed_system')
      AND pqm.measure_key = ptr.measures_to_send


SELECT *
FROM
    reveleer_projects
WHERE
      yr = 2024
  AND payer_id IN (50, 48);

SELECT *
FROM
    reveleer_chase_file_details
where reveleer_project_id in (235, 237) and measure_id in ('TRC', 'FMC');

call sp_reveleer_data_stager();
