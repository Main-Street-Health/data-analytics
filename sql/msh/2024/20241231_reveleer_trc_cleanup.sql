SELECT *
FROM
    analytics.public.reveleer_chases rc
    JOIN reveleer_compliance_file_details comp ON rc.id = comp.reveleer_chase_id
--     JOIN reveleer_chase_file_details cfd ON rc.id = cfd.reveleer_chase_id
WHERE
    rc.external_chase_id = '27476488';

SELECT pm.*
FROM
    analytics.public.reveleer_chases rc
--     JOIN reveleer_compliance_file_details comp ON rc.id = comp.reveleer_chase_id
join fdw_member_doc.qm_patient_measures pm on pm.id = any(rc.qm_patient_measure_ids)
WHERE
    rc.external_chase_id = '27476488';
SELECT *
FROM
    fdw_member_doc.qm_pm_status_periods WHERE patient_measure_id = 1600626;

SELECT *
FROM
    reveleer_projects where id = 233;
SELECT *
FROM
    fdw_member_doc.quality_measure_config;

WITH
    analytics   AS ( SELECT
                         id
                       , name
                       , payer_id
                       , state_payer_id
                       , reveleer_id
                       , UNNEST(measures_to_send) measure_key
                     FROM
                         reveleer_projects
                     WHERE
                         yr = 2024 )
  , payer_level AS ( SELECT
                         c.payer_id
                       , c.payer_name
                       , c.measure_key
                       , c.measure_contracted
                       , a.id IS NOT NULL is_configured
                     FROM
                         fdw_member_doc_ent.quality_measure_config c
                         LEFT JOIN analytics a ON a.measure_key = c.measure_key AND a.payer_id = c.payer_id
                     WHERE
                           c.measure_year = 2024
                       AND a.id ISNULL
                       AND c.measure_key !~* 'med_adh'
                       AND c.measure_key NOT IN ('spd_statin_use_for_patients_with_diabetes',
                                                 'spc_statin_therapy_for_patients_with_cardiovascular_disease') )
  , to_add      AS ( SELECT
                         measure_key
                       , pl.payer_id
                     --   , COUNT(*)
--   , ARRAY_AGG(payer_name) payers
                     FROM
                         payer_level pl
                     WHERE
                           EXISTS( SELECT 1 FROM analytics a WHERE a.payer_id = pl.payer_id )
                       AND measure_key ~* 'trc_'
                       AND measure_key = 'trc_rdi_receipt_of_discharge_information'
-- and measure_key != 'trc_avg_average_score'
-- and measure_key != 'trc_pe7_patient_engagement_after_inpatient_discharge_7_days'
                       AND EXISTS( SELECT 1 FROM analytics a WHERE a.measure_key = pl.measure_key )
                     -- GROUP BY
--     1
                     ORDER BY
                         2 DESC )
UPDATE
    reveleer_projects rp
SET
    measures_to_send = ARRAY_APPEND(rp.measures_to_send, ta.measure_key), updated_at = NOW()
FROM
    to_add ta
WHERE
      ta.payer_id = rp.payer_id
  AND rp.yr = 2024;



SELECT rp.id, rp.name, rp.payer_id, rp.measures_to_send, ta.measure_key, array_append(rp.measures_to_send, ta.measure_key)
FROM
    reveleer_projects rp
join to_add ta on ta.payer_id = rp.payer_id
where rp.yr = 2024
;

DROP TABLE IF EXISTS _potential_measures;
CREATE TEMP TABLE _potential_measures AS
SELECT
    pm.id
  , pm.patient_id
  , pm.measure_key
  , pm.measure_status_key
  , CASE
    WHEN pm.measure_key = 'trc_peid_patient_engagement_after_ip_discharge'   THEN must_close_by_date - 30
    WHEN pm.measure_key = 'trc_mrp_medication_reconciliation_post_discharge' THEN must_close_by_date - 30
    WHEN pm.measure_key = 'trc_rdi_receipt_of_discharge_information'         THEN must_close_by_date - 2
    END must_close_by_date
  , sp.patient_payer_id
FROM
    fdw_member_doc.qm_patient_measures pm
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pm.patient_id
    JOIN fdw_member_doc.qm_patient_config pc ON pc.patient_id = sp.patient_id AND pc.measure_key = pm.measure_key
WHERE
      pm.measure_key = 'trc_peid_patient_engagement_after_ip_discharge'
  AND pm.measure_source_key = 'mco'
  AND sp.patient_payer_id IN (2, 38, 40, 44, 48, 49, 50, 249)
UNION
SELECT
    pm.id
  , pm.patient_id
  , pm.measure_key
  , pm.measure_status_key
  , CASE
    WHEN pm.measure_key = 'trc_peid_patient_engagement_after_ip_discharge'   THEN must_close_by_date - 30
    WHEN pm.measure_key = 'trc_mrp_medication_reconciliation_post_discharge' THEN must_close_by_date - 30
    WHEN pm.measure_key = 'trc_rdi_receipt_of_discharge_information'         THEN must_close_by_date - 2
    END must_close_by_date
  , sp.patient_payer_id
FROM
    fdw_member_doc.qm_patient_measures pm
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pm.patient_id
    JOIN fdw_member_doc.qm_patient_config pc ON pc.patient_id = sp.patient_id AND pc.measure_key = pm.measure_key
WHERE
      pm.measure_key = 'trc_rdi_receipt_of_discharge_information'
  AND pm.measure_source_key = 'mco'
  AND sp.patient_payer_id = 38;



DROP TABLE IF EXISTS _compliance;
CREATE TEMP TABLE _compliance AS
SELECT rc.id chase_id, rc.due_date, rcfd.numerator_code, max(rcfd.inserted_at) last_sent
FROM
    reveleer_chases rc
join reveleer_compliance_file_details rcfd ON rc.id = rcfd.reveleer_chase_id
and rcfd.numerator_code in ('pat_engage', 'discharge_info')
where rc.measure_code = 'TRC'
GROUP BY rc.id, 2, 3
;

-- measures_not_sent_20250102
SELECT
    pm.id                pqm_id
  , pm.patient_id
  , pm.measure_key
  , pm.patient_payer_id
  , p.name
  , pm.measure_status_key
  , pm.must_close_by_date
  , rc.id                msh_chase_id
  , rc.external_chase_id rev_chase_id
  , c.numerator_code     compliance_numerator_code
  , c.last_sent          complaince_last_sent_at
FROM
    _potential_measures pm
    JOIN fdw_member_doc.payers p ON p.id = pm.patient_payer_id
    LEFT JOIN reveleer_chases rc ON pm.patient_id = rc.patient_id
        AND rc.measure_code = 'TRC'
        AND rc.yr = 2024
        AND rc.due_date = pm.must_close_by_date
    LEFT JOIN _compliance c ON rc.id = c.chase_id
        AND pm.must_close_by_date = c.due_date
        AND pm.measure_key = CASE WHEN c.numerator_code = 'pat_engage'
                                      THEN 'trc_peid_patient_engagement_after_ip_discharge'
                                  WHEN c.numerator_code = 'discharge_info'
                                      THEN 'trc_rdi_receipt_of_discharge_information' END
ORDER BY
    pm.patient_id, pm.id
;

SELECT *
FROM
    _;








SELECT *
FROM
    reveleer_chases order by id desc;
;
SELECT *
FROM
    reveleer_projects WHERE name ~* 'wellmed';