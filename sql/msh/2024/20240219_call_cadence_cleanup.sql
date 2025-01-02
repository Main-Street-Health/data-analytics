SELECT
    COUNT(*)
FROM
    patient_tasks pt
--     JOIN dates d ON d.date BETWEEN pt.inserted_at AND pt.updated_at
WHERE
      pt.inserted_at BETWEEN '2024-01-01' AND '2024-02-16'
  AND pt.task_type = 'patient_experience_follow_up'
  AND pt.updated_at > pt.inserted_at + '2 days'::INTERVAL
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      calls c
                  WHERE
                      c.patient_id = pt.patient_id
                  )
;

-- 7796
-- 3262
SELECT
    COUNT(*)
FROM
    patient_task_call_cadence_off_track ot
    JOIN patient_tasks pt ON ot.patient_task_id = pt.id
WHERE
      EXISTS( SELECT
                  1
              FROM
                  calls c
              WHERE
                    c.patient_id = pt.patient_id
                AND c.started_at BETWEEN ot.started_at AND ot.updated_at )
  AND pt.inserted_at BETWEEN '2024-01-01' AND '2024-02-16'
  AND ot.back_on_track_at ISNULL
;



