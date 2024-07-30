            DROP TABLE IF EXISTS _rx_fill_tasks_to_close;
            CREATE TEMP TABLE _rx_fill_tasks_to_close AS
                SELECT
                    pt.id patient_task_id
                  , pqm.id pqm_id
                  , mpqm.id mpqm_id
                FROM
                    public.patient_tasks pt
                    JOIN public.patient_quality_measures_tasks pqmt ON pt.id = pqmt.patient_task_id
                    JOIN public.patient_quality_measures pqm ON pqm.id = pqmt.patient_measure_id
                    JOIN stage.medication_adherence_measure_id_to_task_types m_to_tt
                         ON m_to_tt.task_type = pt.task_type
                    JOIN public.prescription_fill_patient_task pfpt ON pt.id = pfpt.patient_task_id
                    JOIN stage.patient_rx_fill_measures fm ON fm.patient_id = pt.patient_id
                        AND m_to_tt.measure_id = fm.measure_id
                        AND fm.year = pqm.year
                    LEFT JOIN public.msh_patient_quality_measures mpqm on pqm.id = mpqm.patient_quality_measure_id
                WHERE
                  (pt.status = 'completed' AND pfpt.is_system_verified_closed)
                  AND pqm.status IN ('open', 'in_progress')
                ;

            -- pqm
            WITH
                upd  AS (
                    UPDATE public.patient_quality_measures pqm
                        SET status = 'closed_pending', updated_at = NOW()
                        FROM _rx_fill_tasks_to_close ttc
                        WHERE ttc.pqm_id = pqm.id
                            AND pqm.status IN ('open', 'in_progress')
                        RETURNING pqm.id )
              , ins  AS (
                INSERT
                    INTO
                        public.patient_measure_status_history(patient_measure_id, status, changed_at, changed_by_id)
                        SELECT
                            id
                          , 'closed_pending'
                          , NOW()
                          , 2
                        FROM
                            upd
                        RETURNING patient_measure_id )
                -- mpqm
              , upd2 AS (
                UPDATE public.msh_patient_quality_measures mpqm
                    SET substatus = NULL, updated_at = NOW()
                    FROM ins ttc
                    WHERE ttc.patient_measure_id = mpqm.patient_quality_measure_id
                        AND mpqm.substatus IS NOT NULL
                    RETURNING mpqm.id )
            INSERT
            INTO
                public.msh_patient_measure_substatus_history (msh_patient_quality_measure_id, substatus, changed_at, changed_by_id)
            SELECT
                id
              , NULL
              , NOW()
              , 2
            FROM
                upd2
            ;
