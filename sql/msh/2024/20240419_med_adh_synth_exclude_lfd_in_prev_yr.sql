DROP TABLE IF EXISTS _year_straddle;
CREATE TEMP TABLE _year_straddle AS
SELECT DISTINCT ON (m.patient_measure_id)
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , m.ipsd
  , f.last_filled_date
  , f.sold_date
  , f.start_date
FROM
         fdw_member_doc.qm_pm_med_adh_metrics m
    JOIN fdw_member_doc.patient_medication_fills f ON m.patient_id = f.patient_id AND m.measure_key = f.measure_key AND m.ipsd = f.start_date
    JOIN fdw_member_doc.qm_patient_measures qpm ON m.patient_measure_id = qpm.id AND qpm.is_active
WHERE
      m.measure_source_key = 'sure_scripts'
  AND f.last_filled_date < '2024-01-01'::DATE
ORDER BY m.patient_measure_id, f.last_filled_date desc
;

DROP TABLE IF EXISTS _to_re_run;
CREATE TEMP TABLE _to_re_run AS
SELECT DISTINCT ON (ys.patient_id)
    ys.patient_id
  , b.id
FROM
    _year_straddle ys
    JOIN sure_scripts_panel_patients pp ON pp.patient_id = ys.patient_id
join sure_scripts_med_histories h on h.sure_scripts_panel_id = pp.sure_scripts_panel_id
join prd.patient_med_adherence_synth_period_batches b on h.id = b.sure_scripts_med_history_id
ORDER BY
    1, 2 desc
;
SELECT distinct id
FROM
    _to_re_run;




WITH
    inputs AS ( SELECT DISTINCT
                    start_date
                  , end_date
                  , patient_ids
                  , sure_scripts_med_history_id
                FROM
                    prd.patient_med_adherence_synth_period_batches b
                    JOIN _to_re_run trr ON trr.id = b.id )
SELECT *
FROM
    inputs i
    CROSS JOIN prd.fn_build_med_adherence_synthetics(i.start_date, i.end_date, i.patient_ids,
                                                     i.sure_scripts_med_history_id) x
;

SELECT *
FROM
    analytics.prd.patient_med_adherence_synth_period_batches order by id desc;

begin;
        DELETE
        FROM
            fdw_member_doc.qm_pm_med_adh_synth_periods sp
        WHERE
              EXISTS( SELECT
                          1
                      FROM
                          _to_re_run  cpm
                      WHERE
                          cpm.patient_id = sp.patient_id )
          AND date_part('year', sp.start_date) = 2024;

DROP TABLE IF EXISTS _to_send_over;
CREATE TEMP TABLE _to_send_over AS
SELECT cpm.patient_id, max(sp.batch_id) latest_batch
FROM
                      _to_re_run cpm
join
                          prd.patient_med_adherence_synth_periods sp
on cpm.patient_id = sp.patient_id
GROUP BY cpm.patient_id
;

INSERT
INTO
    fdw_member_doc.qm_pm_med_adh_synth_periods (analytics_id, patient_id, measure_key,
                                                batch_id, fn_iteration, is_moved, join_key, days_supply, rn,
                                                start_date, end_date, overlap_id, overlap_start_date,
                                                overlap_end_date, value_set_item, og_start_date, og_end_date,
                                                prev_start_date, prev_days_supply, patient_medication_ids, ndcs,
                                                inserted_at, updated_at, yr)
SELECT
    id               analytics_id
  , sp.patient_id
  , coop_measure_key measure_key
  , batch_id
  , fn_iteration
  , is_moved
  , join_key
  , days_supply
  , rn
  , start_date
  , end_date
  , overlap_id
  , overlap_start_date
  , overlap_end_date
  , value_set_item
  , og_start_date
  , og_end_date
  , prev_start_date
  , prev_days_supply
  , patient_medication_ids
  , ndcs
  , sp.inserted_at
  , NOW()
  , DATE_PART('year', sp.start_date)
FROM
    prd.patient_med_adherence_synth_periods sp
    JOIN ref.med_adherence_measure_names mamm ON mamm.analytics_measure_id = sp.measure_id
JOIN _to_send_over tso on tso.patient_id = sp.patient_id and tso.latest_batch = sp.batch_id
WHERE
      EXISTS( SELECT
                  1
              FROM
                  _to_re_run cpm
              WHERE
                  sp.patient_id = cpm.patient_id )
  AND sp.batch_id BETWEEN 12904 AND 12957;

end;


------------------------------------------------------------------------------------------------------------------------
/* post cleanup */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _year_straddle;
CREATE TEMP TABLE _year_straddle AS
SELECT DISTINCT ON (m.patient_measure_id)
    m.patient_id
  , m.measure_key
  , m.patient_measure_id
  , m.ipsd
  , f.last_filled_date
  , f.sold_date
  , f.start_date
FROM
    fdw_member_doc.qm_pm_med_adh_metrics m
    JOIN fdw_member_doc.patient_medication_fills f
         ON m.patient_id = f.patient_id AND m.measure_key = f.measure_key AND m.ipsd = f.start_date
             AND f.last_filled_date < '2024-01-01'::DATE
    LEFT JOIN fdw_member_doc.patient_medication_fills f2
              ON m.patient_id = f2.patient_id AND m.measure_key = f2.measure_key AND m.ipsd = f2.start_date
                  AND f2.last_filled_date >= '2024-01-01'::DATE
    JOIN fdw_member_doc.qm_patient_measures qpm ON m.patient_measure_id = qpm.id AND qpm.is_active
join fdw_member_doc.supreme_pizza sp on sp.patient_id = m.patient_id and sp.is_medication_adherence
WHERE
      m.measure_source_key = 'sure_scripts'
  AND NOT m.is_excluded
  AND f2.analytics_id ISNULL
ORDER BY
    m.patient_measure_id, f.last_filled_date DESC
;


SELECT DISTINCT on  (    ys.patient_id , ys.measure_key )
    ys.patient_id
  , ys.measure_key
  , ys.patient_measure_id
  , ys.ipsd
  , ys.last_filled_date
  , ys.sold_date
  , ys.start_date
  , sb.*
FROM
    _year_straddle ys
-- join supreme_pizza sp ON ys.patient_id = sp.patient_id
--     JOIN fdw_member_doc.qm_pm_med_adh_synth_periods sp ON sp.patient_id = ys.patient_id AND sp.measure_key = ys.measure_key
    join ref.med_adherence_measure_names mn on mn.coop_measure_key = ys.measure_key
    join prd.patient_med_adherence_synth_periods sp on sp.patient_id = ys.patient_id AND sp.measure_id = mn.analytics_measure_id
    join prd.patient_med_adherence_synth_period_batches sb on sb.id = sp.batch_id
ORDER BY
    ys.patient_id, ys.measure_key
;
   ;

-- delete
select *
FROM
    fdw_member_doc.qm_pm_med_adh_synth_periods sp
where patient_id = 1220528 and measure_key = 'med_adherence_diabetes' order by start_date;

SELECT *
FROM
    fdw_member_doc.patient_medication_fills
WHERE
      patient_id = 1220528
  AND measure_key = 'med_adherence_diabetes'

ORDER BY
    last_filled_date;

-- patient_id = 102238 and measure_key = 'med_adherence_cholesterol';
;

;

SELECT *
FROM
    analytics.prd.patient_medications
WHERE
    patient_id IN ( 62207,311893,311893,311893,962287,1182346)

;

;
WITH
    _patients_to_pull AS ( SELECT *
                           FROM
                               ( VALUES

                                     (1220528, 'one off year straddle fix straggler')
--                                      (62207, 'one off year straddle fix straggler'),
--                                      (311893, 'one off year straddle fix straggler'),
--                                      (962287, 'one off year straddle fix straggler'),
--                                      (1182346, 'one off year straddle fix straggler')
                                 ) x(patient_id, reason) )
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
                    AND sspp.patient_id = ptp.patient_id )
  AND LENGTH(p.first_name) >= 2 -- SS requires Two of a person's names (Last Name, First Name, Middle Name) must have 2 or more characters.
  AND LENGTH(p.last_name) >= 2
        ;

SELECT DISTINCT ON (patient_id)
    patient_id
  , sure_scripts_panel_id
  , inserted_at
FROM
    sure_scripts_panel_patients
WHERE
    patient_id IN ( 62207,311893,311893,311893,962287,1182346)
ORDER BY
    patient_id, inserted_at DESC
;
SELECT *
FROM
    sure_scripts_panel_patients where sure_scripts_panel_id ISNULL ;
