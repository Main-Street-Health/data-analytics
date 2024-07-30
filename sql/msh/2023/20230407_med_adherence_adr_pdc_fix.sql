create temporary table _controls_ptcs_med_adherence_measures as
select ('2023-01-01')::date boy, ('2023-12-31')::date eoy, 2023, 331 synth_batch_id;

DROP TABLE IF EXISTS _fixed_pmam;
CREATE TEMP TABLE _fixed_pmam AS
select
            *,
            (absolute_fail_date - greatest(last_covered_date, calc_date))::int adr --  allowable days remaining   ----  --|--(-)[.....]|
        from
            (
                select
                    patient_id,
                    measure_id,
                    pmam_id,
                    calc_date,
                    nd_fills,
                    ipsd,
                    days_to_cover_to_date,
                    days_covered_to_date,
                    days_covered_to_period_end,
                    --
                    last_covered_date,
                    (days_covered_to_date       * 1.0 / days_to_cover_to_date  )::decimal(16,2) pdc_to_date,  -- proportion of days covered
                    (select ctx.eoy from _controls_ptcs_med_adherence_measures ctx)  - (days_needed_thru_eoy - days_covered_to_period_end)::int absolute_fail_date,
                    days_needed_thru_eoy,
                    (days_needed_thru_eoy - days_covered_to_period_end)  days_must_cover,
                    patient_medication_ids,
                    patient_med_adherence_synth_period_ids
                from (
                    select
                        mp.patient_id,
                        mp.measure_id,
                        pmam.id                                                                 pmam_id,
                        pmam.calc_date                                                          calc_date,
                        min(mp.start_date)                                                      ipsd,
                        max(end_date)                                                           last_covered_date,
                        count(distinct mp.start_date )                                          nd_fills,
                        array_agg(distinct mp.id)                                               patient_med_adherence_synth_period_ids,
                        --
                        pmam.calc_date - min(mp.start_date) + 1                                                              days_to_cover_to_date,
                        (((select ctx.eoy from _controls_ptcs_med_adherence_measures ctx) - min(mp.start_date)) * .8)::int days_needed_thru_eoy,
                        (((select ctx.eoy from _controls_ptcs_med_adherence_measures ctx) - min(mp.start_date)) * .2)::int days_not_needed_thru_eoy,
                        --
                        count(distinct d.day         ) filter ( where d.day between mp.start_date and least(pmam.calc_date, mp.end_date)) days_covered_to_date,
                        count(distinct d.day         ) filter ( where d.day between mp.start_date and mp.end_date                    )  days_covered_to_period_end,
                        --
                        ('{' || replace(replace(replace(array_agg(distinct mp.patient_medication_ids::text)::text,'}',''),'{',''),'"','') || '}')::bigint[] patient_medication_ids
                    from
                        (select id, patient_med_adherence_synth_period_ids, coalesce(calc_to_date, inserted_at::date) calc_date from prd.patient_med_adherence_measures) pmam
                        join prd.patient_med_adherence_synth_periods mp on mp.id =  any(pmam.patient_med_adherence_synth_period_ids)
                        join ref.dates d on d.day between (select ctx.boy from _controls_ptcs_med_adherence_measures ctx) and mp.end_date
--                         join ref.dates d on d.day between (select ctx.boy from _controls_ptcs_med_adherence_measures ctx) and current_date
                    where
                        mp.start_date between (select ctx.boy from _controls_ptcs_med_adherence_measures ctx) and pmam.calc_date
--                         and mp.batch_id = (select ctx.synth_batch_id from _controls_ptcs_med_adherence_measures ctx)
--                         and exists (select 1 from _ss_data sd where sd.patient_id = mp.patient_id)
                    group by 1,2,3,4
                ) x
        ) y
ORDER BY patient_id, measure_id, pmam_id
        ;
SELECT *
FROM
    _fixed_pmam
WHERE patient_id = 37907;

DROP TABLE IF EXISTS _updated_mams;
CREATE TEMP TABLE _updated_mams AS
with upd as
    (
        UPDATE prd.patient_med_adherence_measures pmam
SET
    absolute_fail_date         = fp.absolute_fail_date
  , days_covered_to_date       = fp.days_covered_to_date
  , days_covered_to_period_end = fp.days_covered_to_period_end
  , days_to_cover_to_date      = fp.days_to_cover_to_date
  , calc_to_date               = fp.calc_date
  , days_needed_thru_eoy       = fp.days_needed_thru_eoy
  , pdc_to_date                = fp.pdc_to_date
  , adr                        = fp.adr
  , updated_at                 = NOW()
FROM
    _fixed_pmam fp
WHERE
      fp.pmam_id = pmam.id
  AND (
              pmam.absolute_fail_date IS DISTINCT FROM fp.absolute_fail_date OR
              pmam.days_covered_to_date IS DISTINCT FROM fp.days_covered_to_date OR
              pmam.days_covered_to_period_end IS DISTINCT FROM fp.days_covered_to_period_end OR
              pmam.days_to_cover_to_date IS DISTINCT FROM fp.days_to_cover_to_date OR
              pmam.calc_to_date IS DISTINCT FROM fp.calc_date OR
              pmam.days_needed_thru_eoy IS DISTINCT FROM fp.days_needed_thru_eoy OR
              pmam.pdc_to_date IS DISTINCT FROM fp.pdc_to_date OR
              pmam.adr IS DISTINCT FROM fp.adr
          )
RETURNING pmam.id
        )
SELECT *
FROM
    upd;
;
SELECT count(*)
FROM
    _updated_mams; -- 59533

UPDATE fdw_member_doc_stage.patient_medication_adherences pmam
SET
    absolute_fail_date         = fp.absolute_fail_date
  , days_covered_to_date       = fp.days_covered_to_date
  , days_covered_to_period_end = fp.days_covered_to_period_end
  , days_to_cover_to_date      = fp.days_to_cover_to_date
  , calc_to_date               = fp.calc_date
  , days_needed_thru_eoy       = fp.days_needed_thru_eoy
  , pdc_to_date                = fp.pdc_to_date
  , adr                        = fp.adr
  , updated_at                 = NOW()
FROM
    _fixed_pmam fp
WHERE
      fp.pmam_id = pmam.analytics_id
  AND (
              pmam.absolute_fail_date IS DISTINCT FROM fp.absolute_fail_date OR
              pmam.days_covered_to_date IS DISTINCT FROM fp.days_covered_to_date OR
              pmam.days_covered_to_period_end IS DISTINCT FROM fp.days_covered_to_period_end OR
              pmam.days_to_cover_to_date IS DISTINCT FROM fp.days_to_cover_to_date OR
              pmam.calc_to_date IS DISTINCT FROM fp.calc_date OR
              pmam.days_needed_thru_eoy IS DISTINCT FROM fp.days_needed_thru_eoy OR
              pmam.pdc_to_date IS DISTINCT FROM fp.pdc_to_date OR
              pmam.adr IS DISTINCT FROM fp.adr
          );

UPDATE fdw_member_doc.patient_medication_adherences pmam
SET
    absolute_fail_date         = fp.absolute_fail_date
  , days_covered_to_date       = fp.days_covered_to_date
  , days_covered_to_period_end = fp.days_covered_to_period_end
  , days_to_cover_to_date      = fp.days_to_cover_to_date
  , calc_to_date               = fp.calc_date
  , days_needed_thru_eoy       = fp.days_needed_thru_eoy
  , pdc_to_date                = fp.pdc_to_date
  , adr                        = fp.adr
  , updated_at                 = NOW()
FROM
    _fixed_pmam fp
WHERE
      fp.pmam_id = pmam.analytics_id
  AND (
              pmam.absolute_fail_date IS DISTINCT FROM fp.absolute_fail_date OR
              pmam.days_covered_to_date IS DISTINCT FROM fp.days_covered_to_date OR
              pmam.days_covered_to_period_end IS DISTINCT FROM fp.days_covered_to_period_end OR
              pmam.days_to_cover_to_date IS DISTINCT FROM fp.days_to_cover_to_date OR
              pmam.calc_to_date IS DISTINCT FROM fp.calc_date OR
              pmam.days_needed_thru_eoy IS DISTINCT FROM fp.days_needed_thru_eoy OR
              pmam.pdc_to_date IS DISTINCT FROM fp.pdc_to_date OR
              pmam.adr IS DISTINCT FROM fp.adr
          );