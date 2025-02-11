
-- DROP TABLE IF EXISTS _patient_measures;
-- CREATE TEMP TABLE _patient_measures AS
SELECT DISTINCT
    pqm.id
--     sp.patient_id
--   , NULL::BIGINT                             chase_id
--   , sp.patient_mbi
-- --   , pay.name                                 payer_name
-- --   , ptr.id                                   reveleer_project_id
--   , CASE
--         WHEN m.code = 'HBD'                          THEN 'A1C9'
--         WHEN m.code IN ('MRP', 'NIA', 'PEID', 'RDI') THEN 'TRC'
--         ELSE m.code
--         END                                  measure_code
--   , pqm.measure_key
--   , pqm.id                                   patient_quality_measure_id
--   , pqm.operational_year
--   , pqm.measure_source_key
--   , pqm.must_close_by_date
-- --   , mpm.subscriber_id
--   , pqm.measure_status_key = 'closed_system' is_closed_system
--   , pqm.measure_source_key
FROM
    fdw_member_doc.qm_patient_measures pqm
    JOIN fdw_member_doc.qm_mco_patient_measures mpm ON pqm.mco_patient_measure_id = mpm.id
--     JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--     JOIN ( SELECT id, UNNEST(measures_to_send) measures_to_send, payer_id FROM public.reveleer_projects ) ptr
--          ON mpm.payer_id = ptr.payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = mpm.payer_id
    JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
    JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
WHERE
      pqm.operational_year = (2024)
  AND pqm.measure_source_key = 'mco'
--   AND pqm.is_active
--   AND sp.is_quality_measures
  AND mpm.payer_id = 50
--   AND sp.patient_payer_id = 50
      -- need to include closed system for compliance file
--   AND (st.send_to_reveleer OR pqm.measure_status_key = 'closed_system')
--   AND pqm.measure_key = ptr.measures_to_send
  AND pqm.measure_key IN (
                          'fmc_follow_up_after_ed_visit_multiple_chronic_conditions',
                          'trc_mrp_medication_reconciliation_post_discharge',
                          'trc_peid_patient_engagement_after_ip_discharge'
    );

select *
from fdw_member_doc.qm_patient_measures qpm
where exists
      (
      select 1
      from fdw_member_doc.qm_mco_patient_measures qmpm
      where qmpm.payer_id = 50 and qmpm.measure_year = 2024 and
            qmpm.measure_key in ('trc_mrp_medication_reconciliation_post_discharge',
                                 'trc_peid_patient_engagement_after_ip_discharge',
                                 'fmc_follow_up_after_ed_visit_multiple_chronic_conditions'
                ) and qpm.mco_patient_measure_id = qmpm.id
      );

DROP TABLE IF EXISTS _mco;
CREATE TEMP TABLE _mco AS
SELECT *
FROM
    public.qm_mco_patient_measures
WHERE
      payer_id = 50
  AND measure_year = 2024
  AND measure_key IN ('fmc_follow_up_after_ed_visit_multiple_chronic_conditions',
                      'trc_mrp_medication_reconciliation_post_discharge',
                      'trc_peid_patient_engagement_after_ip_discharge'
    )
  AND fall_off_status_key = 'on_latest_file'
;



SELECT
    COUNT(pm.patient_id)
  , COUNT(m.patient_id)
FROM
    _patient_measures pm
    LEFT JOIN _mco m ON pm.measure_key = m.measure_key
        AND pm.patient_id = m.patient_id
--         AND pm.must_close_by_date = m.measure_due_date
;
SELECT patient_id, measure_key, count(*)
FROM
    _mco
GROUP BY  1,2
having count(*) > 1
;

;

SELECT * FROM fdw_member_doc.patient_sure_scripts_panels;
SELECT *
FROM
    reveleer_chase_file_details
ORDER BY
    id DESC;


------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _patient_measures;
    CREATE TEMP TABLE _patient_measures AS
    SELECT DISTINCT
        pqm.patient_id
      , NULL::BIGINT chase_id
      , upper(mpm.member_key) patient_mbi
      , pay.name     payer_name
      , ptr.id       reveleer_project_id
      , CASE
            WHEN m.code = 'HBD' THEN 'A1C9'
            WHEN m.code in ('MRP', 'NIA', 'PEID', 'RDI') THEN 'TRC'
            ELSE m.code
            END      measure_code
      , pqm.measure_key
      , pqm.id       patient_quality_measure_id
      , pqm.operational_year
      , pqm.measure_source_key
      , pqm.must_close_by_date
      , mpm.subscriber_id
      , pqm.measure_status_key = 'closed_system' is_closed_system
    FROM
        fdw_member_doc.qm_patient_measures pqm
        JOIN fdw_member_doc.qm_mco_patient_measures mpm ON pqm.mco_patient_measure_id = mpm.id
--         JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--         JOIN public.reveleer_projects ptr ON mpm.payer_id = ptr.payer_id
        JOIN (select id, unnest(measures_to_send) measures_to_send, payer_id from public.reveleer_projects) ptr ON mpm.payer_id = ptr.payer_id
        JOIN fdw_member_doc.payers pay ON pay.id = mpm.payer_id
        JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
        JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
    WHERE
          pqm.operational_year = ( SELECT yr FROM _controls )
      and mpm.payer_id = 50
      AND pqm.measure_source_key = 'mco'
--       AND pqm.is_active
--       AND sp.is_quality_measures
      -- need to include closed system for compliance file
--       AND (st.send_to_reveleer or pqm.measure_status_key = 'closed_system')
      AND pqm.measure_key = ptr.measures_to_send
      and pqm.measure_key in ('trc_mrp_medication_reconciliation_post_discharge',
                                 'trc_peid_patient_engagement_after_ip_discharge',
                                 'fmc_follow_up_after_ed_visit_multiple_chronic_conditions'
                )
    ;
