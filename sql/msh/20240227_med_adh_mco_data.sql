-- create metrics that don't exist
-- update metrics that do exist
-- create patient_meds that don't exist
-- update patient_meds  that do exist
-- stage handoffs
SELECT *
FROM
    qm_pm_med_adh_handoffs where processed_at ISNULL ;

alter table qm_pm_med_adh_metrics add if not exists qm_pm_med_adh_mco_measure_prd_id bigint REFERENCES stage.qm_pm_med_adh_mco_measures(prd_id);
alter table qm_pm_med_adh_metrics add if not exists is_mco_next_fill_date_override boolean not null default false;





;
SELECT * FROM patient_medication_fills;

-- WHERE m.patient_id = 730167 and mam.measure_key = 'med_adherence_hypertension';;
;
1/17 for 90
SELECT *
FROM
    patient_medication_fills
WHERE
      patient_id = 730167
  AND measure_key = 'med_adherence_hypertension';

SELECT *
FROM
    qm_pm_med_adh_synth_periods
WHERE patient_id = 45674 and measure_key = 'med_adherence_hypertension';;

-- patient_id,measure_key
-- 10724,med_adherence_diabetes



-- crupdate metrics
INSERT
INTO
    qm_pm_med_adh_metrics (patient_id, measure_key, measure_year, measure_source_key,
                           fill_count, ipsd, next_fill_date, absolute_fail_date, calc_to_date, pdc_to_date, adr,
                           failed_last_year, inserted_at,
                           updated_at, is_on_90_day_supply)
SELECT patient_id, measure_key, measure_year, measure_source_key,
       fill_count, ipsd, next_fill_date, absolute_fail_date, calc_to_date, pdc_to_date, adr,
       failed_last_year, inserted_at,
       updated_at, is_on_90_day_supply
FROM
    ( SELECT
          patient_id
        , measure_key
        , measure_year
        , 'mco'                                                 measure_source_key
        , fill_count
        , ipsd
        , next_fill_date
        , absolute_fail_date
        , raw_inserted_at::DATE calc_to_date
        , pdc pdc_to_date
        , adr
        , is_prev_year_fail = 'Y'                               failed_last_year
        , NOW()                                                 inserted_at
        , NOW()                                                 updated_at
        , days_supply >= 90                                     is_on_90_day_supply
      FROM
          _latest_mco_data) x
ON CONFLICT (patient_id, measure_key, measure_year)
        DO UPDATE
        SET
          measure_source_key = excluded.measure_source_key,
          fill_count = excluded.fill_count,
          ipsd = excluded.ipsd,
          next_fill_date = excluded.next_fill_date,
          days_covered_to_period_end = excluded.days_covered_to_period_end,
          days_not_covered = excluded.days_not_covered,
          absolute_fail_date = excluded.absolute_fail_date,
          calc_to_date = excluded.calc_to_date,
          pdc_to_date = excluded.pdc_to_date,
          adr = excluded.adr,
          failed_last_year = excluded.failed_last_year,
          updated_at = excluded.updated_at,
          is_on_90_day_supply = excluded.is_on_90_day_supply;
;


--
-- -- thinking we don't create another table. Just use the stage table to get fill info when needed
-- DROP TABLE IF EXISTS public.qm_pm_med_adh_mco_fills;
-- CREATE TABLE public.qm_pm_med_adh_mco_fills (
--     analytics_prd_id  BIGINT                     NOT NULL PRIMARY KEY,
--     patient_id        BIGINT                     NOT NULL REFERENCES patients,
--     measure_key       TEXT REFERENCES qm_ref_measures,
--     ndc               TEXT,
--     drug_description  TEXT,
--     start_date        DATE,
--     days_supply       INTEGER,
--     end_date          DATE,
--     quantity          numeric,
--     refills_remaining INTEGER,
--     prescriber_name   TEXT,
--     prescriber_npi    TEXT,
--     prescriber_phone  TEXT,
--     dispenser_type    TEXT,
--     dispenser_name    TEXT,
--     dispenser_npi     TEXT,
--     dispenser_phone   TEXT,
--     sold_date         DATE,
--     last_filled_date  DATE,
--     written_date      DATE,
--     next_fill_date    DATE,
--     payer_id          bigint REFERENCES payers,
--     received_at       TIMESTAMP,
--     inserted_at       TIMESTAMP(0) DEFAULT NOW() NOT NULL,
--     updated_at        TIMESTAMP(0) DEFAULT NOW() NOT NULL,
--     is_high_cost      BOOLEAN      DEFAULT FALSE NOT NULL,
--     is_msh_provider   BOOLEAN      DEFAULT FALSE NOT NULL
-- );
--
-- -- crupdate fills. Won't work b/c analytics id is PK tied to prd.patient_medications
-- -- could create a new table but would have about the same format
-- INSERT
-- INTO
--     public.qm_pm_med_adh_mco_fills  (analytics_prd_id, patient_id, measure_key, ndc, drug_description, start_date, days_supply,
--                               end_date, quantity, prescriber_name, prescriber_npi,
--                               dispenser_type, dispenser_name, dispenser_phone,
--                               last_filled_date, payer_id, received_at, inserted_at, updated_at, next_fill_date)
-- SELECT
--     analytics_id, patient_id, measure_key, ndc, drug_description, start_date, days_supply,
--     end_date, quantity, prescriber_name, prescriber_npi,
--     dispenser_type, dispenser_name, dispenser_phone,
--     last_filled_date, payer_id, received_at, inserted_at, updated_at, next_fill_date
-- FROM
--     ( SELECT
--           prd_id               analytics_id
--         , patient_id
--         , measure_key
--         , ndc
--         , drug_name            drug_description
--         , last_fill_date       start_date
--         , days_supply
--         , next_fill_date - 1   end_date
--         , quantity
--         , prescribing_provider prescriber_name
--         , prescriber_npi
--         , 'pharmacy'           dispenser_type
--         , pharmacy_name        dispenser_name
--         , pharmacy_phone       dispenser_phone
--         , last_fill_date       last_filled_date
--         , payer_id
--         , raw_inserted_at      received_at
--         , NOW()                inserted_at
--         , NOW()                updated_at
--         , next_fill_date
--
--       FROM
--           _mco_data ) x
-- ON CONFLICT (analytics_id) DO UPDATE
-- SET
--     drug_description = excluded.drug_description,
--     start_date = excluded.start_date,
--     end_date = excluded.end_date,
--     quantity = excluded.quantity,
--     prescriber_name = excluded.prescriber_name,
--     prescriber_npi = excluded.prescriber_npi,
--     dispenser_type = excluded.dispenser_type,
--     dispenser_name = excluded.dispenser_name,
--     dispenser_phone = excluded.dispenser_phone,
--     last_filled_date = excluded.last_filled_date,
--     payer_id = excluded.payer_id,
--     received_at = excluded.received_at,
--     updated_at = excluded.updated_at,
--     next_fill_date = excluded.next_fill_date
-- ;
-- SELECT * FROM _v_md_portals_rosters;
-- SELECT * FROM _v_md_portals_rosters_all_no_galen;
-- SELECT * FROM _v_md_portals_msh_discharge_roster;

SELECT *
FROM
    patient_task_activities;
DROP TABLE IF EXISTS _pats;
CREATE TEMP TABLE _pats AS
SELECT distinct patient_id FROM (
  VALUES
  (49859)
, (49888)
, (49890)
, (49912)
, (49912)
, (49967)
, (49967)
, (49974)
, (50031)
, (50055)
, (50085)
, (50102)
, (50132)
, (50201)
, (50259)
, (50269)
, (50269)
, (50275)
, (50275)
, (63693)
, (63696)
, (63696)
, (63716)
, (63724)
, (63725)
, (63726)
, (63764)
, (63764)
, (63771)
, (63783)
, (63852)
, (95652)
, (95652)
, (95652)
, (95653)
, (400157)
, (400166)
, (400232)
, (400315)
, (400353)
, (419911)
, (419911)
, (419962)
, (419962)
, (419962)
, (419962)
, (419985)
, (420032)
, (420069)
, (420139)
, (420196)
, (420196)
, (480144)
, (480144)
, (480144)
, (480946)
, (480947)
, (482908)
, (482908)
, (482908)
, (483025)
, (493349)
, (493349)
, (493349)
, (493349)
, (493349)
, (501796)
, (535966)
, (536009)
, (536108)
, (536145)
, (536353)
, (536353)
, (536542)
, (536542)
, (536562)
, (537179)
, (537200)
, (537308)
, (537527)
, (540918)
, (540918)
, (540973)
, (541029)
, (541029)
, (541029)
, (541040)
, (541040)
, (541056)
, (541069)
, (541588)
, (541719)
, (541719)
, (541739)
, (541739)
, (541739)
, (541760)
, (542210)
, (542438)
, (542694)
, (542831)
, (542918)
, (543108)
, (543121)
, (543131)
, (543370)
, (543540)
, (543693)
, (543693)
, (593971)
, (619420)
, (619420)
, (619420)
, (653471)
, (687285)
, (687753)
, (687753)
, (772807)
, (850955)
, (851011)
, (869413)
  ) x(patient_id);


create table junk.viva_docs_20240229 as
SELECT d.id
FROM
    _pats p
    join documents d on d.patient_id = p.patient_id and d.type_id = 'partner_emr_encounter_note'
WHERE not exists(select 1 from junk.viva_docs_20240228 j where j.id = d.id);
with dids as (
    select id from junk.viva_docs_20240229
  union select id from junk.viva_docs_20240228
)
, docs as ( SELECT d.*
            FROM
                documents d
                JOIN dids _d ON _d.id = d.id )
SELECT distinct p.patient_id
FROM
    _pats p
left join docs d on d.patient_id = p.patient_id
where d.id ISNULL
;

SELECT
--     x.cca_identifier                               ws_id
    d.id
  , p.first_name
  , p.last_name
  , TO_CHAR(p.dob, 'YYYY_MM_DD') dob
  , d.s3_bucket
  , d.s3_key
FROM
--     sup.adhoc_cigna_sup_file_2023_created_2024_02_13 x
        junk.viva_docs_20240228 j
    JOIN documents d ON d.id = j.id AND d.deleted_at IS NULL
join patients p on d.patient_id = p.id
;

drop table if exists _controls_qm_pm_med_adh_process;
create temporary table _controls_qm_pm_med_adh_process as
select (2024 || '-01-01')::date boy, (2024 || '-12-31')::date eoy, 2024 yr;

drop table if exists _our_current_measures;
        create temporary table _our_current_measures as
        select
            patient_id
          , measure_key
          , fill_count
          , ipsd
          , days_covered_to_period_end
          , days_not_covered
          , absolute_fail_date
          , pdc_to_date
          , is_excluded
          , (select ctx.yr from _controls_qm_pm_med_adh_process ctx)              measure_year
          , 'sure_scripts'                                                        measure_source_key
          , last_covered_date + 1                                                 next_fill_date
          , now()::date                                                           calc_to_date
          , now()                                                                 inserted_at
          , now()                                                                 updated_at
          , (absolute_fail_date - greatest(last_covered_date, current_date))::int adr --  allowable days remaining   ----  --|--(-)[.....]|
          , coalesce(max_days_supply, 0) >= 90                                    is_on_90_day_supply
        from
            (
                select
                    x.patient_id,
                    x.measure_key,
                    array_length(patient_medication_ids, 1) fill_count,
                    ipsd,
                    days_to_cover_to_date,
                    days_covered_to_date,
                    days_to_cover_to_date - days_covered_to_date  days_not_covered,
                    days_covered_to_period_end,
                    --
                    last_covered_date,
                    (days_covered_to_date       * 1.0 / days_to_cover_to_date  )::decimal(16,2) pdc_to_date,  -- proportion of days covered
                    (select ctx.eoy from _controls_qm_pm_med_adh_process ctx)  - (days_needed_thru_eoy - days_covered_to_period_end)::int absolute_fail_date,
                    days_needed_thru_eoy,
                    max_days_supply,
                    ex.analytics_id IS NOT NULL is_excluded
--                     (days_needed_thru_eoy - days_covered_to_period_end)  days_must_cover,
--                     patient_medication_ids,
--                     patient_med_adherence_synth_period_ids
                from (
                    select
                        sp.patient_id,
                        sp.measure_key,
                        min(sp.start_date)                                                                                                                  ipsd,
                        max(end_date)                                                                                                                       last_covered_date,
--                         count(distinct sp.start_date )                                                                                                      fill_count,
--                         array_agg(distinct sp.patient_medication_ids)                                                                                       patient_med_adherence_synth_period_ids,
                        --
                        current_date - min(sp.start_date) + 1                                                                                               days_to_cover_to_date,
                        (((select ctx.eoy from _controls_qm_pm_med_adh_process ctx) - min(sp.start_date)) * .8)::int                                        days_needed_thru_eoy,
                        --
                        count(distinct d.date         ) filter ( where d.date between sp.start_date and least(current_date, sp.end_date))                   days_covered_to_date,
                        count(distinct d.date         ) filter ( where d.date between sp.start_date and sp.end_date                    )                    days_covered_to_period_end,
                        --
                        max(days_supply)                                                                                                                    max_days_supply,
                        ('{' || replace(replace(replace(array_agg(distinct sp.patient_medication_ids::text)::text,'}',''),'{',''),'"','') || '}')::bigint[] patient_medication_ids
                    from
                        public.qm_pm_med_adh_synth_periods sp
                        join public.dates d on d.date between (select ctx.boy from _controls_qm_pm_med_adh_process ctx) and sp.end_date
                        join public.supreme_pizza za on za.patient_id = sp.patient_id and za.is_medication_adherence
                    where
                        sp.start_date between (select ctx.boy from _controls_qm_pm_med_adh_process ctx) and (select ctx.eoy from _controls_qm_pm_med_adh_process ctx)
                    group by 1,2
                ) x
                left join public.qm_pm_med_adh_exclusions ex on ex.patient_id = x.patient_id
                                                            and ex.measure_key = x.measure_key
                                                            and ex.year = (select ctx.yr from _controls_qm_pm_med_adh_process ctx)
        ) y;
SELECT count(*) FROM _our_current_measures;


-- get latest unprocessed mco data for patients with no SS data
DROP TABLE IF EXISTS _latest_mco_data;
        CREATE TEMP TABLE _latest_mco_data AS
        SELECT DISTINCT ON (m.patient_id, m.measure_key, m.measure_year) m.*
        FROM
            stage.qm_pm_med_adh_mco_measures m
        join public.supreme_pizza sp on sp.patient_id = m.patient_id and sp.is_medication_adherence
        WHERE
              m.measure_year = DATE_PART('year', NOW())
          AND NOT EXISTS(SELECT 1
                         FROM _our_current_measures mam
                         WHERE mam.patient_id = m.patient_id
                           and mam.measure_key = m.measure_key
                           and mam.measure_year = m.measure_year
                           and mam.measure_source_key = 'sure_scripts'
                           -- only override ss if the ss next fill is less than the mco next fill
                           and mam.next_fill_date >= m.next_fill_date
                         )
          AND NOT m.is_processed
        ORDER BY
            m.patient_id, m.measure_key, m.measure_year, m.next_fill_date DESC
        ;
SELECT count(*) FROM _latest_mco_data;
-- 118
-- nav_mco_task_list
SELECT
    sp.patient_id
  , u.full_name                                nav
  , rp.name                                    rpl
  , CASE WHEN next_fill_date < NOW() - '5 days'::INTERVAL THEN measure_key
         ELSE measure_key || '_first_fill' END task_type

  , pay.name
  , m.next_fill_date
--   , m.last_fill_date
--   , m.days_supply
--   , m.adr
--   , m.pdc
--   , m.absolute_fail_date
-- , m.pharmacy_name
-- , m.pharmacy_phone
FROM
    _latest_mco_data m
    JOIN patients p ON p.id = m.patient_id
    JOIN supreme_pizza sp ON p.id = sp.patient_id AND sp.is_medication_adherence
    JOIN care_team_members ctm ON ctm.care_team_id = p.care_team_id AND ctm.role = 'health_navigator'
    JOIN users u ON ctm.user_id = u.id
    JOIN referring_partners rp ON rp.id = sp.primary_referring_partner_id
join payers pay on p.payer_id = pay.id
WHERE
    (
        next_fill_date < NOW() - '5 days'::INTERVAL
            OR
        (next_fill_date < NOW() + '5 days'::INTERVAL AND is_first_fill AND measure_key = 'med_adherence_cholesterol')
        )
and not exists(
    select 1 from qm_pm_med_adh_exclusions e
    where e.patient_id = m.patient_id
    and e.measure_key = m.measure_key
    and e.year = 2024
)
-- and u.first_name = 'Ebony'
ORDER BY
    u.full_name, rp.name;


SELECT * FROM qm_pm_med_adh_handoffs WHERE processed_at ISNULL ;

SELECT state, *
FROM
    oban_jobs
WHERE
      queue ~* 'med_adh'
  AND id = 2842374
ORDER BY
    id DESC
;

update oban_jobs j
    set max_attempts = max_attempts + 1, state = 'available', scheduled_at = now(), discarded_at = null
        where j.id = 2842374;
SELECT *
FROM
    qm_pm_med_adh_metrics WHERE patient_id = 730210

SELECT * FROM member_doc.stage.qm_pm_med_adh_mco_measures where adr ISNULL
SELECT * FROM member_doc.stage.qm_pm_med_adh_mco_measures where absolute_fail_date ISNULL
SELECT * FROM member_doc.stage.qm_pm_med_adh_mco_measures where pdc ISNULL

SELECT distinct wf.*, pf.*, pt.*
FROM
    qm_pm_med_adh_wfs wf
join qm_patient_measures pm on wf.patient_measure_id = pm.id
join qm_pm_med_adh_metrics m on pm.id = m.patient_measure_id
join qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
join patient_tasks pt on pf.patient_task_id = pt.id
join
where pm.is_active
and wf.is_active
and pm.id = 347953
;
select * from public.qm_pm_activities where patient_measure_id = 347953;

update  qm_pm_med_adh_wfs  w
set is_active = false , updated_at = now()
from (
SELECT *, row_number() OVER (PARTITION BY patient_measure_id order by id)  rn
FROM
    qm_pm_med_adh_wfs wf
WHERE
      is_active
  AND patient_measure_id IN ( SELECT
                                  patient_measure_id
                              FROM
                                  qm_pm_med_adh_wfs
                              WHERE
                                  is_active
                              GROUP BY 1
                              HAVING
                                  COUNT(*) > 1 )
ORDER BY patient_measure_id, wf.id
) x
where x.id = w.id
and x.rn = 1
;
SELECT *
FROM
    qm_pm_med_adh_metrics m
join qm_pm_med_adh_synth_periods qpmasp ON m.patient_id = qpmasp.patient_id and qpmasp.measure_key = m.measure_key
join qm_pm_med_adh_handoffs h on m.id = h.qm_pm_med_adh_metric_id
WHERE
    m.patient_measure_id = 347953
order by h.id
;

SELECT *
FROM
    supreme_pizza sp where sp.patient_id = 168171;


UPDATE qm_pm_med_adh_metrics m
SET
    is_excluded = TRUE, updated_at = NOW()
FROM
    public.qm_pm_med_adh_exclusions ex
WHERE
      ex.patient_id = m.patient_id
  AND ex.measure_key = m.measure_key
  AND ex.year = ( SELECT ctx.yr FROM _controls_qm_pm_med_adh_process ctx )
  AND NOT m.is_excluded;


SELECT *
FROM
    patient_task_activities WHERE patient_task_id = 1007258;
;
update  qm_pm_med_adh_wfs  wf
set is_active = false, updated_at = now()
    where wf.id = 6569;

SELECT p.name, max(m.raw_inserted_at)
FROM
    member_doc.stage.qm_pm_med_adh_mco_measures m
join payers p on m.payer_id = p.id
GROUP BY 1
ORDER BY 2
;