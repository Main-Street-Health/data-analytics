------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _latest;
CREATE TEMP TABLE _latest AS
SELECT distinct on (patient_id, measure_key)
    *
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures
ORDER BY patient_id, measure_key, next_fill_date desc
;

SELECT DISTINCT *
              , m.is_excluded excluded_per_ss_data
FROM
    _latest l
    JOIN supreme_pizza sp ON l.patient_id = sp.patient_id AND sp.is_medication_adherence
    LEFT JOIN qm_pm_med_adh_metrics m
              ON
                  m.patient_id = l.patient_id
                      AND m.measure_key = l.measure_key
WHERE
      (
          l.adr <= 20
--               OR (l.pdc < 0.8 and l.pdc > 0)
          )
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      qm_patient_measures pm
                      JOIN qm_pm_med_adh_potential_fills pf ON pf.patient_measure_id = pm.id
                      JOIN patient_tasks pt ON pf.patient_task_id = pt.id
                  WHERE
                        pm.patient_id = l.patient_id
                    AND pm.measure_key = l.measure_key );

SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id IN (399913, 970211, 92487)
  AND measure_key ~* 'hyp'
and last_filled_date >= '2024-01-01'::date
order by patient_id, last_filled_date
;







DROP TABLE IF EXISTS _failing;
CREATE TEMP TABLE _failing AS
SELECT *
FROM
    _latest l
WHERE
--       raw_inserted_at::DATE - next_fill_date > 30
    raw_inserted_at::DATE - next_fill_date > 6
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      qm_pm_med_adh_metrics m
                  WHERE
                        m.patient_id = l.patient_id
                    AND m.measure_key = l.measure_key
                    AND m.next_fill_date < NOW()::DATE - 5 )
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      patient_medication_fills f
                  WHERE
                        f.patient_id = l.patient_id
                    AND f.measure_key = l.measure_key
                    AND f.end_date >= l.next_fill_date
                    AND now()::date - f.end_date < 5
                  )
;
SELECT *
FROM
    _failing
-- WHERE patient_id = 814837
;


SELECT *
FROM
    _latest l
WHERE
      raw_inserted_at::DATE - next_fill_date > 6
--     raw_inserted_at::DATE - next_fill_date > 6
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      qm_pm_med_adh_metrics m
                  WHERE
                        m.patient_id = l.patient_id
                    AND m.measure_key = l.measure_key
                    AND m.next_fill_date < NOW()::DATE - 5 )
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      patient_medication_fills f
                  WHERE
                        f.patient_id = l.patient_id
                    AND f.measure_key = l.measure_key
                    AND f.end_date >= l.next_fill_date
                    AND NOW()::DATE - f.end_date < 5 )
  AND patient_id = 814837
;

SELECT *
FROM
    patient_medication_fills WHERE patient_id = 814837 and measure_key ~* 'diab' and last_filled_date >= '2024-01-01'::date;



SELECT *
FROM
    qm_pm_med_adh_metrics WHERE patient_id = 814837 and measure_key ~* 'diab';

SELECT
    COUNT(DISTINCT (f.patient_id, f.measure_key))                                    nd_failing
  , COUNT(DISTINCT (f.patient_id, f.measure_key)) FILTER ( WHERE pt.id IS NOT NULL ) nd_failing_w_task
  , COUNT(DISTINCT (f.patient_id, f.measure_key)) FILTER ( WHERE pt.id IS NULL ) nd_failing_no_task
  , COUNT(DISTINCT (f.patient_id, f.measure_key)) FILTER ( WHERE pt.id IS NOT NULL ) * 100.0 /
    COUNT(DISTINCT (f.patient_id, f.measure_key))                                    failing_pct_w_task
--   , COUNT(DISTINCT (f.patient_id, f.measure_key))
--     FILTER ( WHERE pt.id IS NULL AND pm.measure_status_key = 'meds_on_hand' ) * 100.0 /
--     COUNT(DISTINCT (f.patient_id, f.measure_key))                                    failing_pct_w_task
FROM
    _failing f
    JOIN qm_patient_measures pm ON pm.patient_id = f.patient_id AND pm.measure_key = f.measure_key
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pt.id = pf.patient_task_id and pt.status in ('new', 'in_progress')
where pm.is_active and pm.measure_status_key != 'excluded'
;
SELECT
    u.full_name
  , rp.name
  , COUNT(DISTINCT (f.patient_id, f.measure_key))
FROM
    _failing f
    JOIN qm_patient_measures pm ON pm.patient_id = f.patient_id AND pm.measure_key = f.measure_key
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pt.id = pf.patient_task_id and pt.status in ('new', 'in_progress')
    JOIN patients p ON p.id = f.patient_id
    JOIN care_teams ct ON ct.id = p.care_team_id
    JOIN care_team_members ctm ON ct.id = ctm.care_team_id AND ctm.role = 'health_navigator'
    JOIN users u ON u.id = ctm.user_id
    JOIN supreme_pizza sp ON sp.patient_id = p.id
    JOIN referring_partners rp ON rp.id = sp.primary_referring_partner_id
join payers pay on p.payer_id = pay.id
WHERE
      pm.is_active
  and pt.id ISNULL
  AND pm.measure_status_key != 'excluded'
GROUP BY
    u.full_name, 2
ORDER BY
    3 DESC

SELECT
--     u.full_name
--   , rp.name
    pay.name
  , COUNT(DISTINCT (f.patient_id, f.measure_key))
  , COUNT(DISTINCT (f.patient_id, f.measure_key)) filter (where pt.id isnull) no_task
  , COUNT(DISTINCT (f.patient_id, f.measure_key)) filter (where pt.id is not null) has_task
  , COUNT(DISTINCT (f.patient_id, f.measure_key)) filter (where pt.id is not null and pt.status in ('new', 'in_progress')) has_current_task

FROM
    _failing f
    JOIN qm_patient_measures pm ON pm.patient_id = f.patient_id AND pm.measure_key = f.measure_key
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pt.id = pf.patient_task_id
    JOIN patients p ON p.id = f.patient_id
    JOIN care_teams ct ON ct.id = p.care_team_id
    JOIN care_team_members ctm ON ct.id = ctm.care_team_id AND ctm.role = 'health_navigator'
    JOIN users u ON u.id = ctm.user_id
    JOIN supreme_pizza sp ON sp.patient_id = p.id
    JOIN referring_partners rp ON rp.id = sp.primary_referring_partner_id
join payers pay on p.payer_id = pay.id
WHERE
      pm.is_active
--   and pt.id ISNULL
  and sp.is_medication_adherence
  AND pm.measure_status_key != 'excluded'
GROUP BY
    1
--     u.full_name, 2
ORDER BY
    2 DESC


SELECT distinct
pay.name, f.*
FROM
    _failing f
    JOIN qm_patient_measures pm ON pm.patient_id = f.patient_id AND pm.measure_key = f.measure_key
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pt.id = pf.patient_task_id and pt.status in ('new', 'in_progress')
    JOIN patients p ON p.id = f.patient_id
    JOIN care_teams ct ON ct.id = p.care_team_id
    JOIN care_team_members ctm ON ct.id = ctm.care_team_id AND ctm.role = 'health_navigator'
    JOIN users u ON u.id = ctm.user_id
    JOIN supreme_pizza sp ON sp.patient_id = p.id
    JOIN referring_partners rp ON rp.id = sp.primary_referring_partner_id
join payers pay on p.payer_id = pay.id
WHERE
      pm.is_active
  and pt.id ISNULL
  and sp.is_medication_adherence
  AND pm.measure_status_key != 'excluded'
-- and sp.patient_id = 92487
--     patient_id = k
-- and pm.patient_id = 945588;
--     u.full_name, 2

;;
SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 399913
  and measure_key = 'med_adherence_hypertension'
  AND last_filled_date >= '2024-01-01'::DATE
order by start_date
;
SELECT p.name
FROM
    supreme_pizza sp
join payers p on p.id = sp.patient_payer_id
WHERE
    patient_id = 399913;


;
SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE
      patient_id = 399913
  and measure_key = 'med_adherence_hypertension'
--   AND last_filled_date >= '2024-01-01'::DATE
order by start_date
;


SELECT
    pm.measure_status_key
, count(*)
FROM
    _failing f
    LEFT JOIN qm_patient_measures pm ON pm.patient_id = f.patient_id AND pm.measure_key = f.measure_key
    LEFT JOIN qm_pm_med_adh_potential_fills pf ON pm.id = pf.patient_measure_id
    LEFT JOIN patient_tasks pt ON pt.id = pf.patient_task_id
where pt.id ISNULL
and pm.is_active
    and pm.measure_status_key != 'excluded'
GROUP BY pm.measure_status_key
;



WITH
    _patients_to_pull AS ( SELECT *
                           FROM
                               ( VALUES
                                     (399913, 'one_off_check_for_premium_deduping') ) x(patient_id, reason) )
INSERT
INTO
    public.sure_scripts_panel_patients (patient_id, sequence, last_name, first_name, middle_name, prefix,
                                        suffix, address_line_1, address_line_2, city, state, zip, dob, gender, npi,
                                        updated_at, inserted_at, reason_for_query)
SELECT DISTINCT
    p.id                                                  patient_id
  , ROW_NUMBER() OVER (ORDER BY p.id)                     sequence
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
  , COALESCE(mp.npi::TEXT, '1023087954')                  npi
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
      NOT EXISTS( SELECT
                      1
                  FROM
                      public.sure_scripts_panel_patients sspp
                  WHERE
                        sspp.sure_scripts_panel_id ISNULL
                    AND sspp.patient_id = p.id )
  AND LENGTH(p.first_name) >= 2 -- SS requires Two of a person's names (Last Name, First Name, Middle Name) must have 2 or more characters.
  AND LENGTH(p.last_name) >= 2
        ;

SELECT *
FROM
    patient_medication_fills
WHERE
    patient_id = 399913
and start_date >= '2024-01-01'::date
and measure_key is not null
;
