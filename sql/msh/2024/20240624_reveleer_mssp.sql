-- SELECT * FROM analytics.public.reveleer_projects where yr = 2024;
-- SELECT * FROM fdw_member_doc.payers where name ~* 'medicare';
-- SELECT * FROM fdw_member_doc.qm_ref_measures WHERE code in ('CBP', 'HBD', 'DSF');
-- INSERT
-- INTO
--     public.reveleer_projects (name, payer_id, state_payer_id, reveleer_id, inserted_at, updated_at,
--                               measures_to_send, yr, is_active)
-- VALUES
--     ('mssp_cqm_aco', 81, NULL, '2250', now(), now(),
--      '{cbp_controlling_high_blood_pressure,hbd_hemoglobin_a1c_control_for_patients_with_diabetes,dsf_depression}', 2024, TRUE);

SELECT * FROM reveleer_projects where payer_id = 81;


DROP TABLE IF EXISTS _pats;
CREATE TEMP TABLE _pats AS
SELECT r.*, sp.patient_id ISNULL  missing_in_coop
FROM
   raw.reveleer_mssp_cqm_aco_membrs_20240624 r
left join fdw_member_doc.supreme_pizza sp on sp.patient_id = r.patient_id
;
SELECT count(*)
FROM
    _pats p
-- join fdw_member_doc.patients p
WHERE
   not missing_in_coop;
patient_id
748562
748579


-- MSSP
DROP TABLE IF EXISTS _patient_measures;
CREATE TEMP TABLE _patient_measures AS
SELECT DISTINCT
    sp.patient_id
  , NULL::BIGINT                             chase_id
  , sp.patient_mbi
  , sp.program
  , pay.name                                 payer_name
  , ptr.id                                   reveleer_project_id
  , CASE
        WHEN m.code = 'HBD' THEN 'A1C9'
        ELSE m.code
        END                                  measure_code
  , pqm.measure_key
  , pqm.id                                   patient_quality_measure_id
  , pqm.operational_year
  , pqm.measure_source_key
  , pqm.must_close_by_date
  , sp.subscriber_id
  , pqm.measure_status_key
--   , pqm.measure_status_key = 'closed_system' is_closed_system
FROM
    fdw_member_doc.qm_patient_measures pqm
--         JOIN fdw_member_doc.qm_mco_patient_measures mpm ON pqm.mco_patient_measure_id = mpm.id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--         JOIN public.reveleer_projects ptr ON mpm.payer_id = ptr.payer_id
    JOIN ( SELECT id, UNNEST(measures_to_send) measures_to_send, payer_id FROM public.reveleer_projects ) ptr
         ON sp.patient_payer_id = ptr.payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = sp.patient_payer_id
    JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
    JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
    JOIN raw.reveleer_mssp_cqm_aco_membrs_20240624 r ON r.patient_id = sp.patient_id
WHERE
      pqm.operational_year = 2024-- ( SELECT yr FROM _controls )
  AND pqm.measure_source_key = 'proxy'
  AND pqm.is_active
  AND sp.is_quality_measures
      -- need to include closed system for compliance file
  AND (st.send_to_reveleer OR pqm.measure_status_key = 'closed_system')
  AND pqm.measure_key = ptr.measures_to_send
  AND ptr.id = 298 -- mssp cqm aco
  and pqm.measure_key != 'dsf_depression'
    ;


SELECT *
FROM
    _patient_measures
;
SELECT count(*)
FROM
    reveleer_cca_pdfs WHERE inserted_at::date = now()::date - 1;

call sp_reveleer_data_stager();

SELECT * FROM rawrp.md_portal_suspects;
call cb.sp_process_md_portal_suspects()