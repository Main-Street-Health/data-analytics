DROP TABLE IF EXISTS _unsuccessful_order_statuses;
CREATE TEMP TABLE _unsuccessful_order_statuses AS
SELECT *
FROM
    ( VALUES
          ('complete', 'attempts_exhausted_unable_to_reach', 0),
          ('complete', 'has_supply', 0),
          ('complete', 'lost_patient_refused', 0),
          ('complete', 'lost_provider_does_not_want_continued_outreach', 0),
          ('complete', 'med_prn', 0),
          ('complete', 'lost_patient_not_paying_through_insurance_will_fail_measure', 1),
          ('complete', 'med_discontinued', 1),

          ('closed', 'changed_pcp_or_no_longer_patient', 0),
          ('closed', 'fired_from_clinic', 0),
          ('closed', 'fired_from_everywhere', 0),
          ('closed', 'moved_out_of_area', 0) ) x(task_status, order_status, min_fill_count);





DROP TABLE IF EXISTS _most_recent_med_adh_tasks;
CREATE TEMP TABLE _most_recent_med_adh_tasks AS
SELECT distinct on (m.patient_measure_id)
    m.patient_measure_id
  , m.patient_id
  , m.measure_key
  , m.priority_status
  , m.adr
  , m.fill_count
  , sp.measure_status_key
  , sp.start_at < NOW() - '7 days'::INTERVAL status_older_than_7_days
FROM
    qm_pm_med_adh_metrics m
    JOIN qm_pm_status_periods sp ON sp.patient_measure_id = m.patient_measure_id AND sp.end_at ISNULL
    left JOIN qm_pm_med_adh_potential_fills pf ON pf.patient_measure_id = m.patient_measure_id
    left join patient_tasks pt on pf.patient_task_id = pt.id
WHERE
    m.measure_year = 2025
order by m.patient_measure_id, pf.updated_at
;



WITH
    _most_recent_tasks                         AS ( SELECT DISTINCT ON (m.patient_measure_id, pf.patient_task_id)
                                                        m.patient_measure_id
                                                      , m.patient_id
                                                      , pf.patient_task_id
                                                      , m.fill_count
                                                      , pt.status
                                                    FROM
                                                        qm_pm_med_adh_metrics m
                                                        JOIN qm_pm_med_adh_potential_fills pf
                                                             ON pf.patient_measure_id = m.patient_measure_id
                                                        JOIN patient_tasks pt ON pf.patient_task_id = pt.id
                                                    WHERE
                                                        m.measure_year = 2025
                                                    ORDER BY m.patient_measure_id, pf.patient_task_id, pf.updated_at DESC )
  , med_adh_unsuccessful_task_complete         AS ( SELECT DISTINCT
                                                        mrt.patient_task_id             AS task_id
                                                      , mrt.patient_measure_id          AS measure_id
                                                      , mrt.patient_id                  AS patient_id
                                                      , 'med_adh_unsuccessful'          AS review_reason
                                                      , 'task_complete_as_unsuccessful' AS review_description
                                                    FROM
                                                        _most_recent_tasks mrt
                                                        JOIN qm_pm_med_adh_potential_fills pf
                                                             ON pf.patient_task_id = mrt.patient_task_id
                                                    WHERE
                                                        (mrt.status = 'completed'
                                                            AND (pf.order_status IN (
                                                                                     'attempts_exhausted_unable_to_reach',
                                                                                     'has_supply',
                                                                                     'lost_patient_refused',
                                                                                     'lost_provider_does_not_want_continued_outreach'
                                                                ) OR pf.order_status IN
                                                                     ('lost_patient_not_paying_through_insurance_will_fail_measure',
                                                                      'med_discontinued') AND
                                                                     mrt.fill_count > 1)) )
  , med_adh_unsuccessful_task_closed           AS ( SELECT
                                                        mrt.patient_task_id                   AS task_id
                                                      , mrt.patient_measure_id                AS measure_id
                                                      , mrt.patient_id                        AS patient_id
                                                      , 'med_adh_unsuccessful'                AS review_reason
                                                      , 'task_closed_patient_status_override' AS review_description
                                                    FROM
                                                        _most_recent_tasks mrt
                                                        JOIN ( SELECT DISTINCT ON (patient_id)
                                                                   patient_id
                                                                 , start_at
                                                                 , end_at
                                                                 , substatus
                                                                 , status
                                                               FROM
                                                                   msh_patient_status_substatus_overrides
                                                               ORDER BY patient_id, start_at DESC ) psso
                                                             ON psso.patient_id = mrt.patient_id
                                                    WHERE
                                                          psso.substatus IN
                                                          ('changed_pcp_or_no_longer_patient', 'fired_from_clinic',
                                                           'fired_from_everywhere', 'moved_out_of_area')
                                                      AND mrt.status = 'closed' )
  ,
--      med_adh_priority_measure as (select null                           as task_id,
--                                                    pm.id                            as measure_id,
--                                                    pm.patient_id                   as patient_id,
--                                                    'med_adh_'          as review_reason,
--                                                    'task_complete_as_unsuccessful' as review_description
--                                   from qm_patient_measures pm
--                                            join qm_pm_med_adh_metrics mam on pm.id = mam.patient_measure_id
--                                   where mam.priority_status in ('high', 'medium')),
    med_adh_measure_stuck_7_days               AS ( SELECT
                                                        NULL::BIGINT                                          AS task_id
                                                      , pm.id                                                 AS measure_id
                                                      , pm.patient_id                                         AS patient_id
                                                      , 'med_adh_measure_stuck'                               AS review_reason
                                                      , 'priority_high_measure_in_pending_or_past_due_status' AS review_description
                                                    FROM
                                                        qm_patient_measures pm
                                                        JOIN qm_pm_med_adh_metrics mam ON pm.id = mam.patient_measure_id
                                                        JOIN public.qm_pm_status_periods sp
                                                             ON pm.id = sp.patient_measure_id AND sp.end_at IS NULL
                                                    WHERE
                                                          pm.measure_status_key IN
                                                          ('pending_discharge', 'past_due_pending_provider',
                                                           'past_due_pending_navigator')
                                                      AND mam.priority_status IN ('high', 'medium')
                                                      AND mam.measure_year = DATE_PART('year', NOW())
                                                      AND sp.start_at <= NOW()::DATE - 7 )
  , med_adh_measure_lost_adr_greater_than_zero AS ( SELECT
                                                        NULL::BIGINT                             AS task_id
                                                      , pm.id                                    AS measure_id
                                                      , pm.patient_id                            AS patient_id
                                                      , 'med_adh_measure_stuck'                  AS review_reason
                                                      , 'measure_lost_and_adr_greater_than_zero' AS review_description
                                                    FROM
                                                        qm_patient_measures pm
                                                        JOIN qm_pm_med_adh_metrics mam ON pm.id = mam.patient_measure_id
                                                    WHERE
                                                          pm.measure_status_key = 'lost_adr_gt_zero'
                                                      AND mam.measure_year = DATE_PART('year', NOW())
                                                      AND mam.priority_status IN ('high', 'medium') )
  , med_adh_measure_stuck_and_adr_less_than_20 AS ( SELECT
                                                        NULL::BIGINT                            AS task_id
                                                      , pm.id                                   AS measure_id
                                                      , pm.patient_id                           AS patient_id
                                                      , 'med_adh_measure_stuck'                 AS review_reason
                                                      , 'measure_lost_and_adr_less_than_twenty' AS review_description
                                                    FROM
                                                        qm_patient_measures pm
                                                        JOIN qm_pm_med_adh_metrics mam
                                                             ON pm.id = mam.patient_measure_id
                                                    -- BP: I commented out the join to pf. I think this was a typo in the doc, I'd ask for clarification
                                                    -- if its not a typo use the _most_recent_... table above
--         TODO: how do I filter on the most recent fill?? or do I want to do that??
--                                                         JOIN public.qm_pm_med_adh_potential_fills pf
--                                                              ON pm.id = pf.patient_measure_id
                                                    WHERE
                                                          mam.adr > 20
                                                      AND mam.measure_year = DATE_PART('year', NOW())
                                                      AND (pm.measure_status_key IN (
                                                                                     'meds_on_hand',
                                                                                     'pending_compliance_check',
                                                                                     'pharmacy_not_found',
                                                                                     'pharmacy_verified_pharmacy_found'
                                                        )
--                                                                OR pf.order_status = 'success_initial_task_pharmacy_verified_patient_picked_up_part_d_covered'
                                                              ) )
  , med_adh_measure_high_priority_unactioned   AS ( SELECT
                                                        NULL::BIGINT                       AS task_id
                                                      , pm.id                              AS measure_id
                                                      , pm.patient_id                      AS patient_id
                                                      , 'med_adh_high_priority_unactioned' AS review_reason
                                                      , 'measure_past_due_high_priority'   AS review_description
                                                    FROM
                                                        qm_patient_measures pm
                                                        JOIN qm_pm_med_adh_metrics mam
                                                             ON pm.id = mam.patient_measure_id
                                                        JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
                                                        JOIN qm_pm_status_periods sp
                                                             ON pm.id = sp.patient_measure_id AND sp.end_at IS NULL
                                                    WHERE
                                                          mam.priority_status IN ('high')
                                                      AND mam.measure_year = DATE_PART('year', NOW())
                                                      AND ((pm.measure_status_key = 'past_due_pending_navigator' OR
                                                            pm.measure_status_key = 'past_due_pending_provider')
                                                        AND sp.start_at <= NOW()::DATE - 1)
                                                      AND pm.is_active
--    TODO: some logic about "is_actioned" false?? is this the task actioned flag???
--  and
)
SELECT *
FROM
    med_adh_measure_high_priority_unactioned
UNION
SELECT *
FROM
    med_adh_measure_lost_adr_greater_than_zero
UNION
SELECT *
FROM
    med_adh_measure_stuck_and_adr_less_than_20
UNION
SELECT *
FROM
    med_adh_measure_stuck_7_days
UNION
SELECT *
FROM
    med_adh_unsuccessful_task_closed
UNION
SELECT *
FROM
    med_adh_unsuccessful_task_complete








;
