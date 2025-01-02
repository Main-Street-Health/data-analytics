------------------------------------------------------------------------------------------------------------------------
/*
 - Dedupe changes
    - fill dates within greatest(.5 x days supply, 30 days) and from different sources
 */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _med_adh_patients ;
CREATE TEMP TABLE _med_adh_patients  AS
SELECT
    patient_id
FROM
    fdw_member_doc.supreme_pizza sp
WHERE
    sp.is_medication_adherence;

create UNIQUE INDEX  on _med_adh_patients(patient_id);

DROP TABLE IF EXISTS _measure_meds;
CREATE TEMP TABLE _measure_meds AS
SELECT distinct
    pm.id
  , pm.patient_id
  , pm.ndc
  , pm.start_date
  , pm.last_filled_date
  , pm.sold_date
  , pm.days_supply
  , mhd.source_description
  , mhd.history_source_qualifier
  , mn.coop_measure_key
FROM
    _med_adh_patients ma
    join prd.patient_medications pm on ma.patient_id = pm.patient_id
    JOIN ref.med_adherence_value_sets vs
         ON vs.code = pm.ndc AND pm.start_date BETWEEN vs.from_date AND vs.thru_date -- only have ndc's
    JOIN ref.med_adherence_measures m
         ON m.value_set_id = vs.value_set_id AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
    JOIN sure_scripts_med_history_details mhd ON mhd.id = pm.last_src_id
    JOIN ref.med_adherence_measure_names mn ON mn.analytics_measure_id = m.measure_id
WHERE
      m.is_med = 'Y'
  AND m.is_exclusion <> 'Y'
  AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA')
  AND pm.last_filled_date >= '2024-01-01'::DATE
;
create index on _measure_meds(patient_id);



DROP TABLE IF EXISTS _overlap;
CREATE TEMP TABLE _overlap AS
SELECT
    mm.id
  , mm.patient_id
  , mm.ndc
  , mm.start_date
  , mm.last_filled_date
  , mm.sold_date
  , mm.days_supply
  , mm.source_description
  , mm.history_source_qualifier
  , mm.coop_measure_key
  , ARRAY_AGG(DISTINCT mm2.id)                       dupe_ids
  , ARRAY_AGG(DISTINCT mm2.source_description)       dupe_sources
  , ARRAY_AGG(DISTINCT mm2.history_source_qualifier) dupe_source_qual

  , COUNT(mm2.source_description)           n_dupe_sources
  , COUNT(mm2.history_source_qualifier)     n_dupe_source_qual
FROM
    _measure_meds mm
    JOIN _measure_meds mm2
         ON
             mm2.patient_id = mm.patient_id
                 AND mm2.ndc = mm.ndc
                 AND mm2.last_filled_date - mm.last_filled_date BETWEEN 1 AND LEAST(.5 * mm.days_supply, 30)
--                  AND mm2.source_description != mm.source_description
                 AND mm2.history_source_qualifier != mm.history_source_qualifier
WHERE
    mm.history_source_qualifier = 'P2'
GROUP BY
    mm.id, mm.patient_id, mm.ndc, mm.start_date, mm.last_filled_date, mm.sold_date, mm.days_supply
         , mm.source_description, mm.history_source_qualifier, mm.coop_measure_key
;
SELECT  mm.*, pm.written_date
FROM
    _overlap o
join _measure_meds mm on mm.id = any(o.dupe_ids || o.id)
join prd.patient_medications pm on pm.id = mm.id
where o.id in ( 13140078,14510814,14608333,15894024,16068246 ) order by patient_id, history_source_qualifier;

SELECT *
FROM
    _overlap order by
-- WHERE
--     ARRAY_LENGTH(dupe_ids, 1) > 1;


SELECT count(distinct (patient_id, coop_measure_key)) FROM _overlap; -- 9294

DROP TABLE IF EXISTS _to_dedupe;
CREATE TEMP TABLE _to_dedupe AS
SELECT  mm.*
FROM
    _overlap o
join _measure_meds mm on mm.id = any(o.dupe_ids || o.id)
order by o.id, mm.start_date
-- where id in (5897673, 11386019,13938536)
;

SELECT
    d.id
  , d.patient_id
  , d.ndc
  , d.start_date
  , pm.written_date
  , d.last_filled_date
  , d.sold_date
  , d.days_supply
  , d.source_description
  , d.history_source_qualifier
  , d.coop_measure_key
      FROM _to_dedupe d
join prd.patient_medications pm on pm.id = d.id
;
