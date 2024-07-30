-- UPDATE prd.mco_patient_measure_rx_fills SET is_first_fill = NULL WHERE TRUE;
-- UPDATE fdw_member_doc_stage.mco_patient_measure_rx_fills SET is_first_fill = NULL WHERE TRUE;

------------------------------------------------------------------------------------------------------------------------
/* UHC: 47 */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _uhc_attr;
CREATE TEMPORARY TABLE _uhc_attr (
    patient_id    BIGINT NOT NULL PRIMARY KEY,
    mco_member_id TEXT
);

INSERT
INTO
    _uhc_attr(patient_id, mco_member_id)
SELECT DISTINCT ON (mp.patient_id)
    mp.patient_id
  , mp.mco_member_id
FROM
    prd.mco_patients mp
WHERE
      mp.payer_id = 47
  AND mp.patient_id IS NOT NULL
ORDER BY
    mp.patient_id, mp.on_most_recent_file DESC /*true first*/, mp.id DESC; /* tie break */

;

SELECT
    f.patient_id, f.measure_id, f.last_fill_date, ur.date_of_last_refill, ur.is_1x_fill, ur.*
FROM
    prd.mco_patient_measure_rx_fills f
    JOIN raw.patient_rx_adherence_roster_uhc ur ON f.latest_raw_id = ur.id
--     JOIN prd.mco_patients mp ON mp.mco_member_id = h.humana_patient_id
WHERE
      f.payer_id = 47
order by f.patient_id, f.measure_id, f.last_fill_date
-- seems like no dupes

UPDATE prd.mco_patient_measure_rx_fills f
SET
    is_first_fill = CASE WHEN ur.is_1x_fill = 'Yes' THEN TRUE else FALSE END
  , updated_at    = NOW()
FROM
    raw.patient_rx_adherence_roster_uhc ur
WHERE
    f.latest_raw_id = ur.id
and f.payer_id = 47
;

-- folks with more than one first fill
SELECT patient_id, measure_id, payer_id, count(*)
FROM
    prd.mco_patient_measure_rx_fills
WHERE is_first_fill
GROUP BY 1,2, 3
having count(*) > 1
ORDER BY 4 desc
;



-- specific example
SELECT DISTINCT
    drug_name
  , date_of_last_refill
  , is_1x_fill
, ux.rx_category
FROM
    raw.patient_rx_adherence_roster_uhc ux
    JOIN _uhc_attr ua ON LOWER(TRIM(ua.mco_member_id)) = LOWER(TRIM(ux.patient_card_id))
WHERE
      ua.patient_id = 48615
  AND is_1x_fill = 'Yes'
;

-- uhc all dupes
WITH
    dupes AS ( SELECT
                   ua.patient_id
                 , ux.rx_category
                 , COUNT(DISTINCT date_of_last_refill)
--     , drug_name
--   , is_1x_fill
               FROM
                   raw.patient_rx_adherence_roster_uhc ux
                   JOIN _uhc_attr ua ON LOWER(TRIM(ua.mco_member_id)) = LOWER(TRIM(ux.patient_card_id))
               WHERE
                     is_1x_fill = 'Yes'

                 AND NULLIF(date_of_last_refill, '')::DATE >= '2023-01-01'::DATE
               GROUP BY
                   ua.patient_id
                 , ux.rx_category
               HAVING
                   COUNT(DISTINCT date_of_last_refill) > 1
               ORDER BY
                   3 DESC )
SELECT DISTINCT
    ua.patient_id
  , ux.rx_category
  , ux.drug_name
  , ux.date_of_last_refill
  , ux.is_1x_fill
FROM
    raw.patient_rx_adherence_roster_uhc ux
    JOIN _uhc_attr ua ON LOWER(TRIM(ua.mco_member_id)) = LOWER(TRIM(ux.patient_card_id))
WHERE
    EXISTS( SELECT 1 FROM dupes d WHERE d.patient_id = ua.patient_id AND d.rx_category = ux.rx_category )
ORDER BY
    patient_id, rx_category, date_of_last_refill
;


-- 6,490

------------------------------------------------------------------------------------------------------------------------
/* humana 44 */
------------------------------------------------------------------------------------------------------------------------
WITH
    dupes AS ( SELECT
                   h.humana_patient_id
                 , h.measure
                 , COUNT(DISTINCT last_fill)
--     , drug_name
--   , is_1x_fill
               FROM
                   raw.humana_rx_opportunity_detail_report h
               WHERE
                     first_fill = 'Y'
                 AND NULLIF(last_fill, '')::DATE >= '2023-01-01'::DATE
               GROUP BY
                   1, 2
               HAVING
                   COUNT(DISTINCT last_fill) > 1
               ORDER BY
                   3 DESC )
SELECT DISTINCT
    mp.patient_id
  , h.measure
  , h.medication
  , h.last_fill
  , h.first_fill
FROM
    raw.humana_rx_opportunity_detail_report h
    JOIN prd.mco_patients mp ON mp.mco_member_id = h.humana_patient_id
WHERE
    EXISTS( SELECT 1 FROM dupes d WHERE d.humana_patient_id = h.humana_patient_id AND d.measure = h.measure )
ORDER BY
    patient_id, measure, last_fill
;


SELECT
    h.*
FROM
    prd.mco_patient_measure_rx_fills f
    JOIN raw.humana_rx_opportunity_detail_report h ON f.latest_raw_id = h.id
    JOIN prd.mco_patients mp ON mp.mco_member_id = h.humana_patient_id
WHERE
      f.payer_id = 44
;
SELECT *
FROM
    prd.patient_med_adherence_year_measures;

------------------------------------------------------------------------------------------------------------------------
/* elevance 2 */
------------------------------------------------------------------------------------------------------------------------
SELECT
    mp.patient_id, count(distinct diabetes_last_fill_date)
FROM
    raw.elevance_pharmacy_report epr
    JOIN prd.mco_patients mp ON mp.mco_member_id = epr.sbscrbr_id AND mp.payer_id = 2
where diabetes_proportion_of_days_covered_pdc = 'FIRSTFILL'
    and NULLIF(TRIM(epr.diabetes_last_fill_date), '') IS NOT NULL
    and length(NULLIF(TRIM(epr.diabetes_last_fill_date), '')) > 5
    and NULLIF(TRIM(epr.diabetes_last_fill_date), '')::date >= '2023-01-01'::date
GROUP BY 1
having count(distinct diabetes_last_fill_date) > 1
-- 12
    ;
SELECT
    mp.patient_id, count(distinct cholesterol_last_fill_date)
FROM
    raw.elevance_pharmacy_report epr
    JOIN prd.mco_patients mp ON mp.mco_member_id = epr.sbscrbr_id AND mp.payer_id = 2
where cholesterol_proportion_of_days_covered_pdc = 'FIRSTFILL'
    and NULLIF(TRIM(epr.cholesterol_last_fill_date), '') IS NOT NULL
    and length(NULLIF(TRIM(epr.cholesterol_last_fill_date), '')) > 5
    and NULLIF(TRIM(epr.cholesterol_last_fill_date), '')::date >= '2023-01-01'::date
GROUP BY 1
having count(distinct cholesterol_last_fill_date) > 1
-- 50
    ;
SELECT
    mp.patient_id, count(distinct hypertension_last_fill_date)
FROM
    raw.elevance_pharmacy_report epr
    JOIN prd.mco_patients mp ON mp.mco_member_id = epr.sbscrbr_id AND mp.payer_id = 2
where hypertension_proportion_of_days_covered_pdc = 'FIRSTFILL'
    and NULLIF(TRIM(epr.hypertension_last_fill_date), '') IS NOT NULL
    and length(NULLIF(TRIM(epr.hypertension_last_fill_date), '')) > 5
    and NULLIF(TRIM(epr.hypertension_last_fill_date), '')::date >= '2023-01-01'::date
GROUP BY 1
having count(distinct hypertension_last_fill_date) > 1
-- 42
    ;

UPDATE prd.mco_patient_measure_rx_fills f
SET
    is_first_fill = case when hypertension_proportion_of_days_covered_pdc ='FIRSTFILL' then true else false end,
    updated_at = NOW()
FROM
    raw.elevance_pharmacy_report ur
WHERE
      f.latest_raw_id = ur.id
  AND f.drug_name = hypertension_drug_name_based_on_last_paid_claim
  AND f.payer_id = 2
;
UPDATE prd.mco_patient_measure_rx_fills f
SET
    is_first_fill = null
    , updated_at = NOW()
WHERE
      f.payer_id not in  (2, 44, 47);
;




UPDATE prd.mco_patient_measure_rx_fills f
SET
    is_first_fill = CASE WHEN ur.is_1x_fill = 'Yes' THEN TRUE else FALSE END
  , updated_at    = NOW()
FROM
    raw.patient_rx_adherence_roster_uhc ur
WHERE
    f.latest_raw_id = ur.id
and f.payer_id = 47
;


UPDATE prd.mco_patient_measure_rx_fills f
SET
    is_first_fill = CASE WHEN h.first_fill = 'Y' THEN TRUE WHEN h.first_fill = 'N' THEN FALSE END
  , updated_at    = NOW()
FROM
    raw.humana_rx_opportunity_detail_report h
WHERE
    f.latest_raw_id = h.id;

UPDATE fdw_member_doc_stage.mco_patient_measure_rx_fills f
SET
    is_first_fill = p.is_first_fill
, updated_at = now()
FROM
    prd.mco_patient_measure_rx_fills p
WHERE
      p.id = f.analytics_id
  AND p.is_first_fill IS DISTINCT FROM f.is_first_fill
  AND p.payer_id = 2
;

SELECT
    diabetes_drug_name_based_on_last_paid_claim
  , diabetes_last_fill_date
  , diabetes_proportion_of_days_covered_pdc
  , r.inserted_at
  , r.diabetes_last_fill_date
  , r.next_refill_date_diabetes
  , r.last_day_to_fail_diabetes
FROM
    raw.elevance_pharmacy_report r
    JOIN prd.mco_patients mp ON mp.mco_member_id = r.sbscrbr_id AND mp.payer_id = 2
WHERE
      NULLIF(TRIM(r.diabetes_drug_name_based_on_last_paid_claim), '') IS NOT NULL
  AND LENGTH(NULLIF(TRIM(r.diabetes_last_fill_date), '')) > 5
  AND LENGTH(NULLIF(TRIM(r.next_refill_date_diabetes), '')) > 5
                          AND length(NULLIF(TRIM(r.last_day_to_fail_diabetes), '')) > 5
  AND mp.patient_id = 336909
ORDER BY
    r.id DESC;




SELECT
    p.is_first_fill
  , r.first_fill
  , p.patient_id
  , r.*
FROM
    prd.mco_patient_measure_rx_fills p
    JOIN raw.humana_rx_opportunity_detail_report r ON r.id = p.latest_raw_id
WHERE
        patient_id IN (
                       144951, 149240, 70203, 147559, 68783, 194123, 64692, 48693, 146537
        )
ORDER BY
    patient_id, last_fill_date
;





-- measures_that_didnt_appear_in_most_recent_file
with most_recent as ( SELECT DISTINCT ON (patient_id, measure_id)
                          *
                      FROM
                          prd.mco_patient_measure_rx_fills p
                      WHERE
                          payer_id IN (47, 44, 2)
                      and p.measure_year = 2023
                      ORDER BY patient_id, measure_id, inserted_at DESC )
SELECT distinct *
FROM
    most_recent WHERE is_first_fill ISNULL ;
;



SELECT *
FROM
                            raw.patient_rx_adherence_roster_uhc ux
join _uhc_attr ua on lower(trim(ua.mco_member_id)) = lower(trim(ux.patient_card_id))
where
    ua.patient_id = 5759
and rx_category = 'MAD'
order by inbound_file_id, ux.next_refill_due
;

------------------------------------------------------------------------------------------------------------------------
/* edite3d sproc code to back fill all is first fill for uhc humana and elevance */
------------------------------------------------------------------------------------------------------------------------
CREATE PROCEDURE sp_patient_med_measures_mco_gaps(IN _payer_id bigint, IN _inbound_file_id bigint DEFAULT NULL::bigint)
    LANGUAGE plpgsql
AS
$$
    ------------------------------------------------------------------------------------------------------------------------
/*
 Load MCO med adherence measures from raw tables to prd.mco_patient_measure_rx_fills
 Revision History :
    --------------------------------------------------------------------------------------------
    Date            Author                  Comment
    --------------------------------------------------------------------------------------------
    2023-04-28      Brendon Pierson         Added Centene and Humana Processing
    2023-05-08      Brendon Pierson         Added Elevance Processing
    2023-06-15      Brendon + Austin        Update to new Centene File + (updated inbound_file_id logic on 06-30)
*/
------------------------------------------------------------------------------------------------------------------------
DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN

    BEGIN

        drop table if exists _controls_rtp_patient_mco_med_measures;
        create temporary table _controls_rtp_patient_mco_med_measures as
        select _payer_id payer_id, _inbound_file_id inbound_file_id;
--         select 49 payer_id, null::bigint inbound_file_id;

        drop table if exists _mco_patient_measure_fills;
        create temporary table _mco_patient_measure_fills
        (
            unique_key             text    not null primary key,
            payer_id               bigint  not null,
            latest_raw_id          bigint  not null,
            latest_inbound_file_id bigint  not null,
            measure_id             text    not null,
            measure_year           integer not null,
            is_new_to_measure      boolean,
            is_prev_year_fail      boolean,
            is_first_fill          boolean,
            pdc                    numeric,
            adr                    integer,
            days_missed            integer,
            absolute_fail_date     date,
            risk_strat             text,
            patient_id             bigint not null,
            mco_member_id          text,
            drug_name              text not null,
            ndc                    text,
            quantity               numeric,
            days_supply            integer,
            last_fill_date         date,
            next_fill_date         date not null,
            max_refill_due         date not null,
            pharmacy_name          text,
            pharmacy_phone         text,
            prescriber_npi         text,
            prescribing_provider   text
        );


        --------------------------------------------------------------------------------------------------------------------
        -- UHC
        if( exists(select 1 from _controls_rtp_patient_mco_med_measures ctx where ctx.payer_id = 47) ) then

            drop table if exists _uhc_attr;
            create temporary table _uhc_attr(patient_id bigint not null primary key, mco_member_id text);
            insert into _uhc_attr(patient_id, mco_member_id)
            select
                 distinct on (mp.patient_id)
                 mp.patient_id,
                 mp.mco_member_id
            from
                prd.mco_patients mp
            where
                mp.payer_id = 47
                and mp.patient_id is not null
            order by mp.patient_id, mp.on_most_recent_file desc /*true first*/, mp.id desc /* tie break */
            ;

            insert into _mco_patient_measure_fills(
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, is_first_fill
            )
            select
                distinct on (unique_key)
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, is_first_fill
            from (
                select
                    concat_ws('::', payer_id, measure_id, measure_year, patient_id, drug_name, last_fill_date) unique_key,
                    *
                from (
                    select
                        distinct
                        ux.payer_id,
                        ux.id              latest_raw_id,
                        ux.inbound_file_id latest_inbound_file_id,
                        -- measure ------------------------------------------------------------------------
                        case when trim(ux.rx_category) = 'MAD' then 'PDC-DR'
                             when trim(ux.rx_category) = 'MAH' then 'PDC-RASA'
                             when trim(ux.rx_category) = 'MAC' then 'PDC-STA'
                        end                                                                    measure_id,
                        coalesce(ux.measure_year::int, extract(year from ux.inserted_at)::int) measure_year,
                        case when new_to_measure ~* 'Y' then true else false end               is_new_to_measure,
                        case when ux.previous_year_failure ~* 'Y' then true else false end     is_prev_year_fail,
                        is_1x_fill ~ 'Yes'                                                     is_first_fill,
                        replace(nullif(ux.pdc_measure_level,''), '%', '')::decimal/100         pdc,
                        nullif(adr_measure_level,'')::int                                      adr,
                        nullif(days_missed_measure_level,'')::int                              days_missed,
                        nullif(absolute_fail_date,'')::date                                    absolute_fail_date,
                        case when risk = 'R' then 'high'
                             when risk = 'Y' then 'medium'
                             when risk = 'G' then 'low'
                        end risk_strat,
                        -- member  ------------------------------------------------------------------------
                        ua.patient_id,
                        ua.mco_member_id,
                        -- drug/rx  -----------------------------------------------------------------------
                        trim(ux.drug_name) drug_name,
                        null::text         ndc,
                        case when ux.quantity_ds = '' then null else nullif(split_part(ux.quantity_ds, '/', 1),'')::numeric end quantity,
                        case when ux.quantity_ds = '' then null else nullif(split_part(ux.quantity_ds, '/', 2),'')::int     end days_supply,
                        nullif(ux.date_of_last_refill,'')::date                                            last_fill_date,
                        nullif(ux.next_refill_due    ,'')::date                                            next_fill_date,
                        greatest(nullif(ux.next_refill_due,'')::date, nullif(ux.max_refill_due ,'')::date) max_refill_due, -- should always be greatest
                        -- pharmacy  -----------------------------------------------------------------------
                        split_part(nullif(ux.pharmacy_name_phone,'')                                            , '/', 1 ) pharmacy_name,
                        staging._cleanup_phone_number(split_part(nullif(ux.pharmacy_name_phone,''), '/', 2)) pharmacy_phone,
                        -- prescriber  ---------------------------------------------------------------------
                        trim(ux.prescriber_npi      ) prescriber_npi,
                        trim(ux.prescribing_provider) prescribing_provider
                    from
                        raw.patient_rx_adherence_roster_uhc ux
                        join _uhc_attr ua on lower(trim(ua.mco_member_id)) = lower(trim(ux.patient_card_id))
                        join fdw_member_doc.payers py on py.id = ux.payer_id
                    where
                        ux.rx_category in ('MAC', 'MAH', 'MAD') --, 'SUPD')
                        and quantity_ds ~* '/'
--                         and inbound_file_id = (select ctx.inbound_file_id from _controls_rtp_patient_mco_med_measures ctx)
                        and (
                            (
                                inbound_file_id = (select max(inbound_file_id) from raw.patient_rx_adherence_roster_uhc)
                                and not exists (select 1 from _controls_rtp_patient_mco_med_measures ctx where inbound_file_id is not null)
                            )
                            or
                            (inbound_file_id = (select ctx.inbound_file_id from _controls_rtp_patient_mco_med_measures ctx))
                        )
                ) x
            ) y
            order by unique_key, latest_inbound_file_id desc, latest_raw_id desc
            ;

        end if;
        -- END UHC
        -- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


        --------------------------------------------------------------------------------------------------------------------
        -- BCBSTN
        if( exists(select 1 from _controls_rtp_patient_mco_med_measures ctx where ctx.payer_id = 38) ) then

            insert into _mco_patient_measure_fills(
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider
            )
            select
                distinct on (unique_key)
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider
            from (
                select
                    concat_ws('::', payer_id, measure_id, measure_year, patient_id, drug_name, last_fill_date) unique_key,

     --  best case adr - approx days missed if started on jan 1
                    73 - (best_case_treatment_period - (pdc * best_case_treatment_period))::numeric            adr,
                    *
                from (
                    select
                        bx.payer_id,
                        bx.id              latest_raw_id,
                        bx.inbound_file_id latest_inbound_file_id,
                        -- measure ------------------------------------------------------------------------
                        mam.measure_id,
                        current_date - cb._boy(current_date)  best_case_treatment_period,
                        coalesce(bx.measure_year::int, extract(year from bx.inserted_at)) measure_year,
                        null::bool                                                        is_new_to_measure,
                        null::bool                                                        is_prev_year_fail,
                        replace(nullif(bx.pdc_percent,''), '%', '')::decimal/100 pdc,
                        null::int                                                days_missed,
                        null::date                                               absolute_fail_date,
                        null::text                                               risk_strat,
                        -- member  ------------------------------------------------------------------------
                        mp.patient_id,
                        coalesce(mp.mco_member_id, bx.member_id) mco_member_id,
                        -- drug/rx  -----------------------------------------------------------------------
                        trim(bx.medication_name)                                drug_name,
                        lpad(trim(drug_code), 11, '0')                          ndc,
                        null::numeric                                           quantity,
                        nullif(prescription_days_suplly_count,'')::numeric::int days_supply,
                        nullif(bx.last_date_filled,'')::date                    last_fill_date,
                        nullif(bx.pharmacy_refill_due_date,'')::date            next_fill_date,
                        nullif(bx.pharmacy_refill_due_date,'')::date            max_refill_due,
                        -- pharmacy  -----------------------------------------------------------------------
                        null::text  pharmacy_name,
                        null::text  pharmacy_phone, -- could use this (probably bad idea) staging._cleanup_phone_number(prescribing_provider_phone_number) pharmacy_phone,
                        -- prescriber  ---------------------------------------------------------------------
                        null::text                                     prescriber_npi,
                        trim(bx.prescribing_provider_name)             prescribing_provider
                    from
                        raw.bcbstn_medication_adherence bx
                        join ref.med_adherence_value_sets vs on vs.code = lpad(trim(drug_code), 11, '0')
                                                                and vs.code_type = 'NDC'-- fixed_ndc
                        join ref.med_adherence_measures mam on mam.value_set_id = vs.value_set_id and mam.is_exclusion != 'Y'
                        left join prd.mco_patients mp on mp.mco_member_id = bx.member_id and mp.payer_id = bx.payer_id
                    where
                        (
                            (
                                inbound_file_id = (select max(inbound_file_id) from raw.bcbstn_medication_adherence)
                                and
                                not exists (select 1 from _controls_rtp_patient_mco_med_measures ctx where inbound_file_id is not null)
                            )
                            or
                            (inbound_file_id = (select ctx.inbound_file_id from _controls_rtp_patient_mco_med_measures ctx))
                        )
                ) x
            ) y
            where
                y.measure_id in ('PDC-DR', 'PDC-RASA', 'PDC-STA')
            order by unique_key, latest_inbound_file_id desc, latest_raw_id desc
            ;

        end if;
        -- END BCBSTN
        -- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    ------------------------------------------------------------------------------------------------------------------------
    /* Elevance  2 */
        IF (EXISTS( SELECT 1 FROM _controls_rtp_patient_mco_med_measures ctx WHERE ctx.payer_id = 2 ))
        THEN

            INSERT
            INTO
                _mco_patient_measure_fills(unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id,
                                           measure_year,
                                           is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed,
                                           absolute_fail_date, risk_strat,
                                           patient_id, mco_member_id, drug_name, ndc, quantity, days_supply,
                                           last_fill_date, next_fill_date,
                                           max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi,
                                           prescribing_provider, is_first_fill)
            SELECT DISTINCT ON (unique_key)
                unique_key, payer_id, latest_raw_id, latest_inbound_file_id, measure_id,
                measure_year,
                is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed,
                absolute_fail_date, risk_strat,
                patient_id, mco_member_id, drug_name, ndc, quantity, days_supply,
                last_fill_date, next_fill_date,
                max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi,
                prescribing_provider, is_first_fill
            FROM
                ( SELECT
                      CONCAT_WS('::', payer_id, measure_id, measure_year, patient_id, drug_name,
                                last_fill_date) unique_key
                    , *
                  FROM
                      (
                      SELECT
                            2                                                                                              payer_id
                          , epr.id                                                                                         latest_raw_id
                          , epr.inbound_file_id                                                                            latest_inbound_file_id
                            -- measure ------------------------------------------------------------------------
                          , 'PDC-DR'                                                                                       measure_id
                          , EXTRACT(YEAR FROM epr.inserted_at)                                                             measure_year
                          , NULLIF(TRIM(epr.diabetes_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_new_to_measure
                          , NULLIF(TRIM(epr.diabetes_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_first_fill
                          , epr.diabetes_adherent_prior_year IS NOT DISTINCT FROM 'Y'                                      is_prev_year_fail
                          , REPLACE(
                                    NULLIF(NULLIF(NULLIF(TRIM(epr.diabetes_proportion_of_days_covered_pdc), ''),
                                                  'FIRSTFILL'), '.00%')
                                , '%', '')::DECIMAL / 100                                                                  pdc
                          , NULLIF(TRIM(epr.diabetes_allowable_days), '')::NUMERIC                                         adr
                          , NULL::INT                                                                                      days_missed
                          , NULLIF(TRIM(epr.last_day_to_fail_diabetes), '')::DATE                                          absolute_fail_date
                          , NULL::TEXT                                                                                     risk_strat
                            -- member  ------------------------------------------------------------------------
                          , mp.patient_id
                          , COALESCE(mp.mco_member_id, epr.sbscrbr_id)                                                     mco_member_id
                            -- drug/rx  -----------------------------------------------------------------------
                          , TRIM(epr.diabetes_drug_name_based_on_last_paid_claim)                                          drug_name
                          , NULL                                                                                           ndc
                          , NULL::NUMERIC                                                                                  quantity
                          , NULLIF(TRIM(epr.diabetes_drug_last_days_supply), '')::NUMERIC::INT                             days_supply
                          , NULLIF(TRIM(epr.diabetes_last_fill_date), '')::DATE                                            last_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_diabetes), '')::DATE                                          next_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_diabetes), '')::DATE                                          max_refill_due
                            -- pharmacy  -----------------------------------------------------------------------
                          , NULLIF(TRIM(epr.pharmacy_name), '')                                                            pharmacy_name
                          , NULLIF(TRIM(epr.pharmacy_phone_number), '')                                                    pharmacy_phone
                            -- prescriber  ---------------------------------------------------------------------
                          , NULLIF(TRIM(epr.prescriber_npi_for_diabetes), '')                                              prescriber_npi
                          , NULLIF(TRIM(epr.prescriber_for_diabetes), '')                                                  prescribing_provider
                        FROM
                            raw.elevance_pharmacy_report epr
                            JOIN prd.mco_patients mp ON mp.mco_member_id = epr.sbscrbr_id AND mp.payer_id = 2
                        WHERE
                              NULLIF(TRIM(epr.diabetes_drug_name_based_on_last_paid_claim), '') IS NOT NULL
                          AND length(NULLIF(TRIM(epr.diabetes_last_fill_date), '')) > 5
                          AND length(NULLIF(TRIM(epr.next_refill_date_diabetes), '')) > 5
                          AND (NULLIF(TRIM(epr.last_day_to_fail_diabetes), '') is null or length(NULLIF(TRIM(epr.last_day_to_fail_diabetes), '')) > 5)
--                           AND (
--                                   ( inbound_file_id = ( SELECT MAX(inbound_file_id) FROM raw.elevance_pharmacy_report )
--                                       AND
--                                       NOT EXISTS ( SELECT 1 FROM
--                                                        _controls_rtp_patient_mco_med_measures ctx
--                                                    WHERE
--                                                        inbound_file_id IS NOT NULL )
--                                   )
--                                   OR
--                                  (inbound_file_id = ( SELECT ctx.inbound_file_id FROM _controls_rtp_patient_mco_med_measures ctx ))
--                                   )
                        UNION
                       SELECT
                            2                                                                                              payer_id
                          , epr.id                                                                                         latest_raw_id
                          , epr.inbound_file_id                                                                            latest_inbound_file_id
                            -- measure ------------------------------------------------------------------------
                          , 'PDC-STA'                                                                                      measure_id
                          , EXTRACT(YEAR FROM epr.inserted_at)                                                             measure_year
                          , NULLIF(TRIM(epr.cholesterol_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_new_to_measure
                          , NULLIF(TRIM(epr.cholesterol_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_first_fill
                          , epr.cholesterol_adherent_prior_year IS NOT DISTINCT FROM 'Y'                                   is_prev_year_fail
                          , REPLACE(
                                    NULLIF(NULLIF(NULLIF(TRIM(epr.cholesterol_proportion_of_days_covered_pdc), ''),
                                                  'FIRSTFILL'), '.00%')
                                , '%', '')::DECIMAL / 100                                                                  pdc
                          , NULLIF(TRIM(epr.cholesterol_allowable_days), '')::NUMERIC                                      adr
                          , NULL::INT                                                                                      days_missed
                          , NULLIF(TRIM(epr.last_day_to_fail_cholesterol), '')::DATE                                       absolute_fail_date
                          , NULL::TEXT                                                                                     risk_strat
                            -- member  ------------------------------------------------------------------------
                          , mp.patient_id
                          , COALESCE(mp.mco_member_id, epr.sbscrbr_id)                                                     mco_member_id
                            -- drug/rx  -----------------------------------------------------------------------
                          , TRIM(epr.cholesterol_drug_name_based_on_last_paid_claim)                                       drug_name
                          , NULL                                                                                           ndc
                          , NULL::NUMERIC                                                                                  quantity
                          , NULLIF(TRIM(epr.cholesterol_drug_last_days_supply), '')::NUMERIC::INT                          days_supply
                          , NULLIF(TRIM(epr.cholesterol_last_fill_date), '')::DATE                                         last_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_cholesterol), '')::DATE                                       next_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_cholesterol), '')::DATE                                       max_refill_due
                            -- pharmacy  -----------------------------------------------------------------------
                          , NULLIF(TRIM(epr.pharmacy_name), '')                                                            pharmacy_name
                          , NULLIF(TRIM(epr.pharmacy_phone_number), '')                                                    pharmacy_phone
                            -- prescriber  ---------------------------------------------------------------------
                          , NULLIF(TRIM(epr.prescriber_npi_for_cholesterol), '')                                           prescriber_npi
                          , NULLIF(TRIM(epr.prescriber_for_cholesterol), '')                                               prescribing_provider
                        FROM
                            raw.elevance_pharmacy_report epr
                            JOIN prd.mco_patients mp ON mp.mco_member_id = epr.sbscrbr_id AND mp.payer_id = 2
                        WHERE
                              NULLIF(TRIM(epr.cholesterol_drug_name_based_on_last_paid_claim), '') IS NOT NULL
                          AND length(NULLIF(TRIM(epr.cholesterol_last_fill_date), '')) > 5
                          AND length(NULLIF(TRIM(epr.next_refill_date_cholesterol), '')) > 5
                          AND (NULLIF(TRIM(epr.last_day_to_fail_cholesterol), '') is null or length(NULLIF(TRIM(epr.last_day_to_fail_cholesterol), '')) > 5)
--                           AND (
--                                   ( inbound_file_id = ( SELECT MAX(inbound_file_id) FROM raw.elevance_pharmacy_report )
--                                       AND
--                                       NOT EXISTS ( SELECT 1 FROM
--                                                        _controls_rtp_patient_mco_med_measures ctx
--                                                    WHERE
--                                                        inbound_file_id IS NOT NULL )
--                                   )
--                                   OR
--                                  (inbound_file_id = ( SELECT ctx.inbound_file_id FROM _controls_rtp_patient_mco_med_measures ctx ))
--                                   )
                        UNION
                        SELECT
                            2                                                                                              payer_id
                          , epr.id                                                                                         latest_raw_id
                          , epr.inbound_file_id                                                                            latest_inbound_file_id
                            -- measure ------------------------------------------------------------------------
                          , 'PDC-RASA'                                                                                     measure_id
                          , EXTRACT(YEAR FROM epr.inserted_at)                                                             measure_year
                          , NULLIF(TRIM(epr.hypertension_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_new_to_measure
                          , NULLIF(TRIM(epr.hypertension_proportion_of_days_covered_pdc), '') IS NOT DISTINCT FROM 'FIRSTFILL' is_first_fill
                          , epr.hypertension_adherent_prior_year IS NOT DISTINCT FROM 'Y'                                      is_prev_year_fail
                          , REPLACE(
                                    NULLIF(NULLIF(NULLIF(TRIM(epr.hypertension_proportion_of_days_covered_pdc), ''),
                                                  'FIRSTFILL'), '.00%')
                                , '%', '')::DECIMAL / 100                                                                  pdc
                          , NULLIF(TRIM(epr.hypertension_allowable_days), '')::NUMERIC                                     adr
                          , NULL::INT                                                                                      days_missed
                          , NULLIF(TRIM(epr.last_day_to_fail_hypertension), '')::DATE                                      absolute_fail_date
                          , NULL::TEXT                                                                                     risk_strat
                            -- member  ------------------------------------------------------------------------
                          , mp.patient_id
                          , COALESCE(mp.mco_member_id, epr.sbscrbr_id)                                                     mco_member_id
                            -- drug/rx  -----------------------------------------------------------------------
                          , TRIM(epr.hypertension_drug_name_based_on_last_paid_claim)                                      drug_name
                          , NULL                                                                                           ndc
                          , NULL::NUMERIC                                                                                  quantity
                          , NULLIF(TRIM(epr.hypertension_drug_last_days_supply), '')::NUMERIC::INT                         days_supply
                          , NULLIF(TRIM(epr.hypertension_last_fill_date), '')::DATE                                        last_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_hypertension), '')::DATE                                      next_fill_date
                          , NULLIF(TRIM(epr.next_refill_date_hypertension), '')::DATE                                      max_refill_due
                            -- pharmacy  -----------------------------------------------------------------------
                          , NULLIF(TRIM(epr.pharmacy_name), '')                                                            pharmacy_name
                          , NULLIF(TRIM(epr.pharmacy_phone_number), '')                                                    pharmacy_phone
                            -- prescriber  ---------------------------------------------------------------------
                          , NULLIF(TRIM(epr.prescriber_npi_for_hypertension), '')                                          prescriber_npi
                          , NULLIF(TRIM(epr.prescriber_for_hypertension), '')                                              prescribing_provider
                        FROM
                            raw.elevance_pharmacy_report epr
                            JOIN prd.mco_patients mp ON mp.mco_member_id = epr.sbscrbr_id AND mp.payer_id = 2
                        WHERE
                              NULLIF(TRIM(epr.hypertension_drug_name_based_on_last_paid_claim), '') IS NOT NULL
                          AND length(NULLIF(TRIM(epr.hypertension_last_fill_date), '')) > 5
                          AND length(NULLIF(TRIM(epr.next_refill_date_hypertension), '')) > 5
                          AND (NULLIF(TRIM(epr.last_day_to_fail_hypertension), '') is null or length(NULLIF(TRIM(epr.last_day_to_fail_hypertension), '')) > 5)
--                           AND (
--                                   ( inbound_file_id = ( SELECT MAX(inbound_file_id) FROM raw.elevance_pharmacy_report )
--                                       AND
--                                       NOT EXISTS ( SELECT 1 FROM
--                                                        _controls_rtp_patient_mco_med_measures ctx
--                                                    WHERE
--                                                        inbound_file_id IS NOT NULL )
--                                   )
--                                   OR
--                                  (inbound_file_id = ( SELECT ctx.inbound_file_id FROM _controls_rtp_patient_mco_med_measures ctx ))
--                                   )

                        ) x
                  ) y
            where y.patient_id is not null
            ORDER BY unique_key, latest_inbound_file_id DESC, latest_raw_id DESC;

        END IF;
    -- END Elevance
    ------------------------------------------------------------------------------------------------------------------------

    ------------------------------------------------------------------------------------------------------------------------

    /* Centene */
        if( exists(select 1 from _controls_rtp_patient_mco_med_measures ctx where ctx.payer_id = 49) ) then

            INSERT
            INTO
                _mco_patient_measure_fills(unique_key, payer_id, latest_raw_id, latest_inbound_file_id,
                                           measure_id,
                                           measure_year,
                                           is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed,
                                           absolute_fail_date, risk_strat,
                                           patient_id, mco_member_id, drug_name, ndc, quantity, days_supply,
                                           last_fill_date, next_fill_date,
                                           max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi,
                                           prescribing_provider)
            SELECT DISTINCT ON (payer_id, measure_id, measure_year, patient_id, drug_name)
                -- NOTE: All other plans use the last fill date in unqiue key. We don't have it for Cenetene so we use next_fill_date
                CONCAT_WS('::', payer_id, measure_id, measure_year, patient_id, drug_name,
                          next_fill_date) unique_key
              , payer_id
              , latest_raw_id
              , latest_inbound_file_id
              , measure_id
              , measure_year
              , is_new_to_measure
              , is_prev_year_fail
              , pdc
              , adr
              , days_missed
              , calc_date + adr::INT      absolute_fail_date
              , risk_strat
              , patient_id
              , mco_member_id
              , drug_name
              , ndc
              , quantity
              , days_supply
              , last_fill_date
              , next_fill_date
              , max_refill_due
              , pharmacy_name
              , pharmacy_phone
              , prescriber_npi
              , prescribing_provider
            FROM
                ( SELECT
                      ctn.payer_id
                    , ctn.id                                                          latest_raw_id
                    , ctn.inbound_file_id                                             latest_inbound_file_id
                    , CASE WHEN ctn.measure_key = 'CHOL' THEN 'PDC-STA'
                           WHEN ctn.measure_key = 'DIAB' THEN 'PDC-DR'
                           WHEN ctn.measure_key = 'RASA' THEN 'PDC-RASA' END          measure_id
                    , DATE_PART('year', ctn.next_fill_due_date::DATE)                 measure_year
                    , NULL::BOOLEAN                                                   is_new_to_measure
                    , rl.id IS NOT NULL                                               is_prev_year_fail
                    , ctn.p_dayscovered::NUMERIC                                      pdc
                    , ctn.days_to_nonadh::NUMERIC                                     adr
                    , ctn.days_missed_ytd::INT                                        days_missed
--                  , null::DATE                                                                              absolute_fail_date
                    , (SUBSTRING((meta -> 'extra_args' ->> 'original_name') FROM
                                 '([0-9]{8})\.csv$'))::DATE                           calc_date
                    , NULL                                                            risk_strat
                    , mp.patient_id
                    , mp.mco_member_id
                    , NULLIF(TRIM(ctn.label_name), '')                                drug_name
                    , NULLIF(TRIM(REGEXP_REPLACE(ctn.last_fill_quantity, '\[|\]', '', 'g')),
                             '')::NUMERIC                                             quantity
                    , NULLIF(TRIM(REGEXP_REPLACE(ctn.last_fill_days_supply, '\[|\]', '', 'g')),
                             '')::INT                                                 days_supply
                    , NULLIF(TRIM(ctn.next_fill_due_date), '')::DATE                  next_fill_date
                    , NULLIF(TRIM(ctn.pharmacy_name), '')                             pharmacy_name
                    , NULLIF(TRIM(ctn.pharmacy_phone_number), '')                     pharmacy_phone
                    , NULLIF(TRIM(ctn.prescriber_npi), '')                            prescriber_npi
                    , NULLIF(TRIM(ctn.next_fill_due_date), '')::DATE                  max_refill_due
                    , ctn.last_fill_refill_date::DATE - ctn.last_fill_quantity::INT   last_fill_date
                    , ctn.last_fill_ndc                                               ndc
                    , CONCAT_WS(' ', NULLIF(TRIM(ctn.last_fill_prescriber_first_name), ''),
                                NULLIF(TRIM(ctn.last_fill_prescriber_last_name), '')) prescribing_provider
                  FROM
                      raw.centene_medadherence ctn
                      JOIN prd.mco_patients mp ON mp.mco_member_id = ctn.subscriber_id
                      LEFT JOIN prd.patient_med_adherence_red_list rl
                                ON rl.patient_id = mp.patient_id
                                    AND
                                   rl.measure_id = CASE WHEN ctn.measure_key = 'CHOL' THEN 'PDC-STA'
                                                        WHEN ctn.measure_key = 'DIAB' THEN 'PDC-DR'
                                                        WHEN ctn.measure_key = 'RASA'
                                                                                      THEN 'PDC-RASA' END
                                    AND rl.year = DATE_PART('year', ctn.next_fill_due_date::DATE) - 1
                  WHERE
                      NOT EXISTS( SELECT
                                      1
                                  FROM
                                      etl.processed_inbound_files pif
                                  WHERE
                                      pif.inbound_file_id = ctn.inbound_file_id ) ) x
            ORDER BY payer_id, measure_id, measure_year, patient_id, drug_name, next_fill_date DESC;


            INSERT
            INTO
                etl.processed_inbound_files (inbound_file_id, payer_id, processed_to)
            SELECT DISTINCT
                inbound_file_id
              , 49                                 payer_id
              , 'prd.mco_patient_measure_rx_fills' processed_to
            FROM
                raw.centene_medadherence ctn
            WHERE
                NOT EXISTS (SELECT 1
                            FROM etl.processed_inbound_files pif
                            WHERE pif.inbound_file_id = ctn.inbound_file_id);



        END IF;
    -- END Centene
    ------------------------------------------------------------------------------------------------------------------------

    ------------------------------------------------------------------------------------------------------------------------
    /* Humana */
        if( exists(select 1 from _controls_rtp_patient_mco_med_measures ctx where ctx.payer_id = 44) ) then
            INSERT
            INTO
                _mco_patient_measure_fills(unique_key,
                                           payer_id, latest_raw_id, latest_inbound_file_id, measure_id, measure_year,
                                           is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed,
                                           absolute_fail_date, risk_strat,
                                           patient_id, mco_member_id, drug_name, ndc, quantity, days_supply,
                                           last_fill_date, next_fill_date,
                                           max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi,
                                           prescribing_provider, is_first_fill)
            SELECT
                CONCAT_WS('::', payer_id, measure_id, measure_year, patient_id, drug_name, last_fill_date) unique_key
              , payer_id
              , latest_raw_id
              , latest_inbound_file_id
              , measure_id
              , measure_year
              , is_new_to_measure
              , is_prev_year_fail
              , pdc / 100.0 -- 2023-06-19 BP Added div by 100
              , adr
              , days_missed
              , absolute_fail_date
              , risk_strat
              , patient_id
              , mco_member_id
              , drug_name
              , ndc
              , quantity
              , days_supply
              , last_fill_date
              , next_fill_date
              , max_refill_due
              , pharmacy_name
              , pharmacy_phone
              , prescriber_npi
              , prescribing_provider
              , is_first_fill
            FROM
                ( SELECT distinct on (mp.patient_id, h.measure, h.last_fill)
                      h.payer_id
                    , h.id                                                                      latest_raw_id
                    , h.inbound_file_id                                                         latest_inbound_file_id
                    , CASE WHEN h.measure = 'ADH-DIABETES' THEN 'PDC-DR'
                           WHEN h.measure = 'ADH-STATINS'  THEN 'PDC-STA'
                           WHEN h.measure = 'ADH-ACE/ARB'  THEN 'PDC-RASA'
                          END                                                                   measure_id
                    , DATE_PART('year', NULLIF(NULLIF(TRIM(h.refill_due), ''), '-')::DATE) measure_year
                    , h.is_new = 'Y'                                                            is_new_to_measure
                    , h.prev_yr_fail = 'Y'                                                      is_prev_year_fail
                    , h.first_fill = 'Y'                                                        is_first_fill
                    , NULLIF(NULLIF(TRIM(h.current_yr_adh_perc), ''), '-')::NUMERIC             pdc
                    , NULL::INT                                                                 adr
                    , NULL::INT                                                                 days_missed
                    , NULLIF(NULLIF(TRIM(h.last_impactable), ''), '-')::DATE                    absolute_fail_date
                    , NULL                                                                      risk_strat
                    , mp.patient_id
                    , mp.mco_member_id
                    , NULLIF(TRIM(h.medication), '')                                            drug_name
                    , NULL                                                                      ndc
                    , NULL::NUMERIC                                                             quantity
                    , NULLIF(NULLIF(TRIM(h.day_supply), ''), '-')::NUMERIC::INT                 days_supply
                    , NULLIF(NULLIF(TRIM(h.last_fill), ''), '-')::DATE                          last_fill_date
                    , NULLIF(NULLIF(TRIM(h.refill_due), ''), '-')::DATE                         next_fill_date
                    , NULLIF(NULLIF(TRIM(h.refill_due), ''), '-')::DATE                         max_refill_due
                    , NULLIF(TRIM(h.rx_name), '')                                               pharmacy_name
                    , NULLIF(TRIM(h.rx_phone), '')                                              pharmacy_phone
                    , NULLIF(TRIM(h.prescriber_npi), '')                                        prescriber_npi
                    , NULLIF(TRIM(h.prescriber), '')                                            prescribing_provider
                  FROM
                      raw.humana_rx_opportunity_detail_report h
                      JOIN prd.mco_patients mp ON mp.mco_member_id = h.humana_patient_id
                  WHERE
                        h.measure IN ('ADH-DIABETES', 'ADH-STATINS', 'ADH-ACE/ARB')
--                     AND ((inbound_file_id =
--                           ( SELECT MAX(inbound_file_id) FROM raw.humana_rx_opportunity_detail_report )
--                       AND NOT EXISTS ( SELECT
--                                            1
--                                        FROM
--                                            _controls_rtp_patient_mco_med_measures ctx
--                                        WHERE
--                                            inbound_file_id IS NOT NULL ))
--                       OR
--                          (inbound_file_id =
--                           ( SELECT ctx.inbound_file_id FROM _controls_rtp_patient_mco_med_measures ctx ))
--                             )
                    order by mp.patient_id, h.measure, h.last_fill, h.inserted_at desc
                  ) x
            ; -- 44
        END IF;
    -- END Humana
    ------------------------------------------------------------------------------------------------------------------------
        UPDATE prd.mco_patient_measure_rx_fills p
        SET
            is_first_fill = f.is_first_fill
          , updated_at    = NOW()
        FROM
            _mco_patient_measure_fills f
        WHERE
            p.unique_key = f.unique_key

        ;

    ------------------------------------------------------------------------------------------------------------------------

        update prd.mco_patient_measure_rx_fills pxu
            set
                is_new_to_measure      = ux.is_new_to_measure ,
                is_prev_year_fail      = ux.is_prev_year_fail ,
                is_first_fill          = ux.is_first_fill,
                pdc                    = ux.pdc ,
                adr                    = ux.adr ,
                days_missed            = ux.days_missed ,
                absolute_fail_date     = ux.absolute_fail_date ,
                risk_strat             = ux.risk_strat ,
                mco_member_id          = ux.mco_member_id ,
                ndc                    = coalesce(ux.ndc        , pxu.ndc),
                quantity               = coalesce(ux.quantity   , pxu.quantity),
                days_supply            = coalesce(ux.days_supply, pxu.days_supply),
                last_fill_date         = ux.last_fill_date ,
                next_fill_date         = ux.next_fill_date ,
                max_refill_due         = ux.max_refill_due ,
                pharmacy_name          = coalesce(ux.pharmacy_name       , pxu.pharmacy_name       ),
                pharmacy_phone         = coalesce(ux.pharmacy_phone      , pxu.pharmacy_phone      ),
                prescriber_npi         = coalesce(ux.prescriber_npi      , pxu.prescriber_npi      ),
                prescribing_provider   = coalesce(ux.prescribing_provider, pxu.prescribing_provider),
                latest_inbound_file_id = ux.latest_inbound_file_id ,
                latest_raw_id          = ux.latest_raw_id ,
                inbound_file_ids       = pxu.inbound_file_ids || array[ux.latest_inbound_file_id],
                raw_ids                = pxu.inbound_file_ids || array[ux.latest_raw_id],
                updated_at             = now()
        from
            _mco_patient_measure_fills ux
        where
            pxu.unique_key = ux.unique_key
            and (
                       pxu.is_new_to_measure    is distinct from ux.is_new_to_measure
                    or pxu.is_prev_year_fail    is distinct from ux.is_prev_year_fail
                    or pxu.is_first_fill        is distinct from ux.is_first_fill
                    or pxu.pdc                  is distinct from ux.pdc
                    or pxu.adr                  is distinct from ux.adr
                    or pxu.days_missed          is distinct from ux.days_missed
                    or pxu.absolute_fail_date   is distinct from ux.absolute_fail_date
                    or pxu.risk_strat           is distinct from ux.risk_strat
                    or pxu.mco_member_id        is distinct from ux.mco_member_id
                    or pxu.ndc                  is distinct from ux.ndc
                    or pxu.quantity             is distinct from ux.quantity
                    or pxu.days_supply          is distinct from ux.days_supply
                    or pxu.last_fill_date       is distinct from ux.last_fill_date
                    or pxu.next_fill_date       is distinct from ux.next_fill_date
                    or pxu.max_refill_due       is distinct from ux.max_refill_due
                    or pxu.pharmacy_name        is distinct from ux.pharmacy_name
                    or pxu.pharmacy_phone       is distinct from ux.pharmacy_phone
                    or pxu.prescriber_npi       is distinct from ux.prescriber_npi
                    or pxu.prescribing_provider is distinct from ux.prescribing_provider
            )
        ;

        insert into prd.mco_patient_measure_rx_fills (
            unique_key, patient_id, payer_id, measure_id, measure_year,
            is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat, mco_member_id,
            drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date, max_refill_due,
            pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider,
            latest_inbound_file_id, latest_raw_id, is_sent_to_coop_med_adherence, is_sent_to_coop_med_adherence_at,
            inbound_file_ids, raw_ids, is_first_fill, inserted_at, updated_at
        )
        select
            unique_key, patient_id, payer_id, measure_id, measure_year,
            is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat, mco_member_id,
            drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date, max_refill_due,
            pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider,
            latest_inbound_file_id, latest_raw_id, false is_sent_to_coop_med_adherence, null::timestamp is_sent_to_coop_med_adherence_at,
            array[latest_inbound_file_id] inbound_file_ids, array[latest_raw_id] raw_ids, is_first_fill, now() inserted_at, now() updated_at
        from
            _mco_patient_measure_fills ux
        where
            not exists (
                select 1
                from prd.mco_patient_measure_rx_fills rx
                where
                    rx.unique_key = ux.unique_key
            )
        ;
        ----------------------------------------------------------------------------------------------------------------


        --------------------------------------------------------------------------------------------------------------------
        -- STEP 2 ::  LOAD TO COOP
        --------------------------------------------------------------------------------------------------------------------
        drop table if exists _to_process;
        create temporary table _to_process as
        select
            *
        from (
            select
                distinct on (payer_id, measure_id, patient_id)
                mcm.*
            from
                prd.mco_patient_measure_rx_fills mcm
            where
                mcm.measure_year = extract(year from current_date)
                and exists (select 1 from _mco_patient_measure_fills mcp where mcp.patient_id = mcm.patient_id and mcp.measure_id = mcm.measure_id)
            order by mcm.payer_id, mcm.measure_id, mcm.patient_id, greatest(mcm.max_refill_due, mcm.next_fill_date) desc
        ) tp
        where
--             -- ignore if sure_scripts has ever had a measure for that patient, year, measure_id
--             not exists (
--                 select 1
--                 from prd.patient_med_adherence_measures pmam
--                 where
--                     pmam.patient_id = tp.patient_id
--                     and pmam.measure_id = tp.measure_id
--                     and pmam.year = tp.measure_year
--             )
            exists (
                select 1
                from fdw_member_doc.supreme_pizza sp
                where sp.patient_id = tp.patient_id
                  and sp.is_medication_adherence
            )
        ;

        insert into fdw_member_doc_stage.patient_medication_adherence_compliances(patient_id, measure_id, is_processed, closed_patient_task_id, inserted_at, source, yr)
        select
            patient_id, measure_id, false is_processed, null::bigint closed_patient_task_id, now() inserted_at, 'mco' source, tp.measure_year
        from
            _to_process tp
        where
            greatest(tp.next_fill_date, tp.max_refill_due) >= current_date -- intentionally not using >= given the staleness of this data
        ;

        -- non-compliant part
        drop table if exists _to_create_potential_tasks;
        create temporary table _to_create_potential_tasks as
        with potential_tasks as (
            select
                *
            from
                _to_process tp
            where
                tp.is_sent_to_coop_med_adherence is false
                and greatest(tp.next_fill_date, tp.max_refill_due) + 5 < current_date
                and not exists(select 1 -- 2023-06-13 BP: don't create if SS exists for same patient measure
                               from prd.patient_med_adherence_year_measures pmay
                               where pmay.patient_id = tp.patient_id
                                 and pmay.measure_id = tp.measure_id
                                 and pmay.year = tp.measure_year
                               )
            -- BP removed 20230626, not sure why this was here
--                 and not exists (
--                 select 1
--                 from prd.patient_med_adherence_measures pmam
--                 where
--                     pmam.patient_id = tp.patient_id
--                     and pmam.measure_id = tp.measure_id
--                     and pmam.year = tp.measure_year
--             )
        ),
        prev_fills as (
            select
                pt.id,
                array_remove(array_agg(distinct mpr.last_fill_date),null) prev_fill_dates
            from
                potential_tasks pt
                left join prd.mco_patient_measure_rx_fills mpr on mpr.patient_id = pt.patient_id
                                                          and mpr.payer_id = pt.payer_id
                                                          and mpr.measure_year = pt.measure_year
                                                          and mpr.last_fill_date < pt.last_fill_date
                                                          and mpr.measure_id = pt.measure_id
                                                          and coalesce(mpr.ndc,mpr.drug_name) = coalesce(pt.ndc,mpr.drug_name)
                                                          and mpr.id <> pt.id
            group by 1
        )
        select
            pt.*,
            pf.prev_fill_dates
        from
            potential_tasks pt
            join prev_fills pf on pf.id = pt.id
        ;

        update prd.mco_patient_measure_rx_fills mx
            set is_sent_to_coop_med_adherence = true,
                is_sent_to_coop_med_adherence_at = now()
        from
            _to_create_potential_tasks tpt
        where
            tpt.id = mx.id
        ;

        insert into fdw_member_doc_stage.patient_medication_adherences(
            patient_id, measure_id, drug_name, ndc, days_supply, next_fill_date, last_fill_date,
            adjusted_next_fill_date, remaining_refills, prescriber_name, prescriber_npi, pharmacy_name,
            pharmacy_npi, pharmacy_phone, failed_last_year, analytics_id, inserted_at, updated_at,
            prescriber_phone, prev_fill_dates, yr, is_processed, medication_adherence_patient_task_id,
            patient_task_id, uuid, pdc_to_date, adr, absolute_fail_date, source, is_ignored
        )
        select
            tx.patient_id, tx.measure_id, tx.drug_name, tx.ndc, tx.days_supply, next_fill_date, last_fill_date,
            next_fill_date adjusted_next_fill_date, null::int remaining_refills, prescribing_provider prescriber_name,
            prescriber_npi, pharmacy_name, null::text pharmacy_npi, pharmacy_phone,
            rl.patient_id is not null failed_last_year, tx.id analytics_id, now() inserted_at, now() updated_at,
            null::text prescriber_phone, prev_fill_dates, measure_year yr, false is_processed, null::bigint medication_adherence_patient_task_id,
            null::bigint patient_task_id, gen_random_uuid() uuid, pdc pdc_to_date,
            tx.adr, tx.absolute_fail_date, 'mco' source, false is_ignored
        from
            _to_create_potential_tasks tx
            left join prd.patient_med_adherence_red_list rl on rl.patient_id = tx.patient_id
                                                               and rl.measure_id = tx.measure_id
                                                               and rl.year = tx.measure_year - 1
        ;

        -- copy over everything to stage for reporting BP added 2023-06-28
        INSERT INTO
            fdw_member_doc_stage.mco_patient_measure_rx_fills (analytics_id, unique_key, patient_id, payer_id, measure_id, measure_year, is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date, max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, latest_inbound_file_id, latest_raw_id, is_sent_to_coop_med_adherence, is_sent_to_coop_med_adherence_at, inbound_file_ids, raw_ids, inserted_at, updated_at, is_first_fill)
        SELECT
            id, unique_key, patient_id, payer_id, measure_id, measure_year, is_new_to_measure, is_prev_year_fail, pdc, adr, days_missed, absolute_fail_date, risk_strat, mco_member_id, drug_name, ndc, quantity, days_supply, last_fill_date, next_fill_date, max_refill_due, pharmacy_name, pharmacy_phone, prescriber_npi, prescribing_provider, latest_inbound_file_id, latest_raw_id, is_sent_to_coop_med_adherence, is_sent_to_coop_med_adherence_at, inbound_file_ids, raw_ids, inserted_at, updated_at, is_first_fill
        FROM prd.mco_patient_measure_rx_fills f
        where not exists(select 1 from fdw_member_doc_stage.mco_patient_measure_rx_fills s where s.analytics_id = f.id);

    ---------------------------
    ---------------------------
    EXCEPTION WHEN OTHERS THEN
        GET DIAGNOSTICS stack = PG_CONTEXT; GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT, exception_detail = PG_EXCEPTION_DETAIL, exception_hint = PG_EXCEPTION_HINT, exception_context = PG_EXCEPTION_CONTEXT;
        rollback;
        error_text = '(1) Message_Text( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
        INSERT INTO public.messages(body, message_transport_id, inserted_at, updated_at, recipient_phone_numbers)
        select body, 1 message_transport_id, now() inserted_at, now() updated_at, x.recipient_phone_numbers from
        (select  E'MSH Analytics : etl.sp_patient_med_measures_mco_gaps() threw an exception. :: \n ' || error_text body, array['+19084894555','+16154808909','+16159440573', '+16156134430', '+16159759275', '+12035200963'] recipient_phone_numbers ) x;
        commit;

        RAISE EXCEPTION 'etl.sp_patient_med_measures_mco_gaps :: %', error_text;

    END;
    COMMIT;
    -------

    --------------------------------------------------------------------------------------------------------------------
    -- STEP 3 ::  Call COOP.stage sproc to process the signal
    --------------------------------------------------------------------------------------------------------------------
    drop table if exists _trashy99;
    create temporary table _trashy99 as
    select 1 from dblink_exec('cb_member_doc', 'call stage.sp_stp_process_med_adherence_tasks()');


END;
$$;

ALTER PROCEDURE sp_patient_med_measures_mco_gaps(BIGINT, BIGINT) OWNER TO postgres;

