DROP TABLE IF EXISTS _closed_rx_fill;
CREATE TEMP TABLE _closed_rx_fill AS
SELECT
    s.analytics_id, s.patient_id, s.patient_medication_ids, s.measure_id
FROM
    fdw_member_doc_stage.patient_rx_fill_measures s;
CREATE INDEX on _closed_rx_fill(patient_medication_ids);
CREATE INDEX on _closed_rx_fill(patient_id);

DROP TABLE IF EXISTS _bad_closures;
CREATE TEMP TABLE _bad_closures AS
SELECT
    s.*
FROM
    _closed_rx_fill s
join prd.patient_medications pm on pm.id = ANY (s.patient_medication_ids)
WHERE
    NOT EXISTS( SELECT
                    1
                FROM
                    ref.hedis_med_list_to_codes mltc
                    JOIN ref.hedis_measure_to_med_list mtml
                         ON mtml.medication_list_name = mltc.medication_list_name AND mtml.yr = mltc.yr
                WHERE
                    mltc.code = pm.ndc
                  AND mtml.measure_id = s.measure_id
                  and mltc.code_system = 'NDC'
                  AND mtml.medication_list_name NOT IN ('Dementia Medications', 'Diabetes Medications', 'Estrogen Agonists Medications') )

;

SELECT * FROM _bad_closures
SELECT * FROM fdw_member_doc_stage.patient_rx_fill_measures;

-- create table junk.stage_patient_rx_fill_measures_20231130 as
-- SELECT * FROM stage.patient_rx_fill_measures;
-- delete out the bad closures
DELETE
    from fdw_member_doc_stage.patient_rx_fill_measures fm
WHERE fm.analytics_id in (select bc.analytics_id from _bad_closures bc);
;
DROP TABLE IF EXISTS _to_reopen;
CREATE TEMP TABLE _to_reopen AS
SELECT DISTINCT
    pt.patient_id patient_id
  , pt.id         patient_task_id
  , pqm.id        pqm_id
  , mpqm.id       mpqm_id
  , pfpt.id       rx_fill_pt_id
  , qm.code
  , qm.name
FROM
    fdw_member_doc.patient_tasks pt
    JOIN fdw_member_doc.patient_quality_measures_tasks pqmt ON pt.id = pqmt.patient_task_id
    JOIN fdw_member_doc.patient_quality_measures pqm ON pqm.id = pqmt.patient_measure_id
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc_stage.medication_adherence_measure_id_to_task_types m_to_tt
         ON m_to_tt.task_type = pt.task_type
    JOIN fdw_member_doc.prescription_fill_patient_task pfpt ON pt.id = pfpt.patient_task_id
    JOIN fdw_member_doc_stage.patient_rx_fill_measures fm ON fm.patient_id = pt.patient_id
        AND m_to_tt.measure_id = fm.measure_id
        AND fm.year = pqm.year
    LEFT JOIN fdw_member_doc.msh_patient_quality_measures mpqm ON pqm.id = mpqm.patient_quality_measure_id
    JOIN _bad_closures bc ON bc.patient_id = pt.patient_id AND bc.measure_id = qm.code
WHERE
      pfpt.is_system_verified_closed
  AND pqm.is_active;


-- create table junk._to_reopen as
-- SELECT * FROM _to_reopen;

call cb.x_util_create_fdw_member_doc();
-- created in member doc
CREATE TABLE stage._to_reopen_20231130 (
    patient_id      BIGINT,
    patient_task_id BIGINT,
    pqm_id          BIGINT,
    mpqm_id         BIGINT,
    rx_fill_pt_id   BIGINT,
    code            VARCHAR(255),
    name            TEXT
);

INSERT
INTO
    fdw_member_doc_stage._to_reopen_20231130 (patient_id, patient_task_id, pqm_id, mpqm_id, rx_fill_pt_id, code, name)
select patient_id, patient_task_id, pqm_id, mpqm_id, rx_fill_pt_id, code, name from _to_reopen;

SELECT *
FROM
fdw_member_doc_stage._to_reopen_20231130
;
drop table junk._to_reopen;
------------------------------------------------------------------------------------------------------------------------
/* switch to member doc db  */
------------------------------------------------------------------------------------------------------------------------
BEGIN;
rollback;

-- dedupe in case of multiple tasks
DROP TABLE IF EXISTS _measures_to_reopen;
CREATE TEMP TABLE _measures_to_reopen AS
SELECT
    ttc.patient_id
  , ttc.pqm_id
  , ttc.mpqm_id
  , ttc.code
  , ttc.name
  , max(ttc.rx_fill_pt_id) rx_fill_pt_id
  , max(ttc.patient_task_id) patient_task_id
FROM
    public.patient_quality_measures pqm
    JOIN stage._to_reopen_20231130 ttc ON ttc.pqm_id = pqm.id
WHERE
    pqm.status = 'closed_pending'
GROUP BY 1,2,3,4,5
;

-- create table junk.rx_fill_tasks_reopened_20231130 as
-- SELECT * FROM _measures_to_reopen;
-- WHERE
--     pqm.status IN ('closed_pending');
SELECT *
FROM
    junk.rx_fill_tasks_reopened_20231130;


-- update pqm statuses
            WITH
                upd  AS (
                    UPDATE public.patient_quality_measures pqm
                        SET status = 'in_progress', updated_at = NOW()
                        FROM _measures_to_reopen ttc
                        WHERE ttc.pqm_id = pqm.id
                            AND pqm.status IN ('closed_pending')
                        RETURNING pqm.id )
              , ins  AS (
                INSERT
                    INTO
                        public.patient_measure_status_history(patient_measure_id, status, changed_at, changed_by_id)
                        SELECT
                            id
                          , 'in_progress'
                          , NOW()
                          , 2
                        FROM
                            upd
                        RETURNING patient_measure_id )
                -- mpqm
              , upd2 AS (
                UPDATE public.msh_patient_quality_measures mpqm
                    SET substatus = 'pending_rx_fill', updated_at = NOW()
                    FROM ins ttc
                    WHERE ttc.patient_measure_id = mpqm.patient_quality_measure_id
--                         AND mpqm.substatus IS NOT NULL
                    RETURNING mpqm.id )
            INSERT
            INTO
                public.msh_patient_measure_substatus_history (msh_patient_quality_measure_id, substatus, changed_at, changed_by_id)
            SELECT
                id
              , 'pending_rx_fill'
              , NOW()
              , 2
            FROM
                upd2
            ;

-- Reopen tasks (needs to come after closing of tasks)
            WITH
                tasks_to_reopen AS ( SELECT
                                          patient_task_id
                                     FROM
                                         _measures_to_reopen

              )
              , pqmt            AS (
                UPDATE public.prescription_fill_patient_task pqmt
                    SET is_system_verified_closed = false, is_task_reopened = TRUE, updated_at = NOW()
                    FROM tasks_to_reopen ttr
                    WHERE ttr.patient_task_id = pqmt.patient_task_id
                    RETURNING pqmt.patient_task_id )
            UPDATE public.patient_tasks pt
            SET status = 'in_progress'
            FROM
                pqmt
            WHERE
                pqmt.patient_task_id = pt.id
            RETURNING *;

ROLLBACK ;
END;
commit;