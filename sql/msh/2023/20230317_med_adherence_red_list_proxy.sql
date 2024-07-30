-- CREATE PROCEDURE med_ad_proxy()
--     LANGUAGE plpgsql
-- AS
-- $$
-- BEGIN
--

------------------------------------------------------------------------------------------------------------------------
/* Red List Proxy - trailing 12 months
   outputs:
       Patient
       DOB
       Measure
       Medication
       Current Quantity / Duration
       payer
       pdc
       rpl
       rpo
       n_fills just in 2023
   */
------------------------------------------------------------------------------------------------------------------------
-- limit to pilot rps
-- select count(*) from _patients; --56,330 -> drops to 12,564 when limited to mhd
DROP TABLE IF EXISTS _patients;
CREATE TEMP TABLE _patients AS
SELECT DISTINCT
    prp.patient_id
  , referring_partner_id
  , mhd.effective_date
  , mhd.expiration_date
FROM
    fdw_member_doc.patient_referring_partners prp
    JOIN sure_scripts_med_history_details mhd ON mhd.patient_id::BIGINT = prp.patient_id
WHERE
      prp."primary"
  AND prp.referring_partner_id IN
      (293, 297, 135, 143, 161, 285, 496, 310, 312, 134, 402, 403, 414, 115, 189, 329, 339, 330, 338, 356, 130, 248,
       300, 411, 464, 152, 239, 272, 281, 140, 133, 365, 193, 194, 276, 145, 230, 67, 262, 217, 224, 225, 213);

SELECT * FROM _patient_meds where patient_id = 118285;
SELECT * FROM sure_scripts_med_history_details where patient_id::bigint = 118285;

DROP TABLE IF EXISTS _patient_meds;
CREATE TEMP TABLE _patient_meds AS
SELECT DISTINCT
    p.patient_id
  , p.referring_partner_id
  , p.effective_date
  , p.expiration_date
  , mhd.patient_date_of_birth                                                       dob
  , mhd.drug_description
  , mhd.product_code                                                                ndc
  , mhd.days_supply
  , mhd.refills_value
  , mhd.refills_qualifier -- REM remaining number of refills
  , COALESCE(mhd.sold_date, last_filled_date, written_date)                         start_date
  , mhd.written_date
  , mhd.last_filled_date
  , mhd.sold_date
  , COALESCE(mhd.sold_date, last_filled_date, written_date) + days_supply::INT - 1  last_day_of_meds
  , COALESCE(mhd.sold_date, last_filled_date, written_date) + days_supply::INT      next_fill_date
  , COALESCE(mhd.sold_date, mhd.last_filled_date, mhd.written_date) >= '2023-01-01' is_2023_med
FROM
    _patients p
    JOIN sure_scripts_med_history_details mhd ON mhd.patient_id::BIGINT = p.patient_id
    JOIN ref.med_adherence_value_sets vs
         ON vs.code = mhd.product_code AND mhd.product_code_qualifier = 'ND' -- only have ndc's
    JOIN ref.med_adherence_measures m
         ON m.value_set_id = vs.value_set_id AND m.measure_id IN ('PDC-DR', 'PDC-RASA', 'PDC-STA');

-- SELECT * FROM _patient_meds;

create index on _patient_meds(ndc);
create index on _patient_meds(patient_id);
create index on _patient_meds(start_date);
create index on _patient_meds(expiration_date);
create index on _patient_meds(last_day_of_meds);
------------------------------------------------------------------------------------------------------------------------
/* PDC-DR Medication Adherence for Patients with Diabetes All Class (MAD). Part D Measure. */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _med_adherence;
CREATE TEMP TABLE _med_adherence AS
SELECT
    pm.patient_id
  , 'PDC-DR'                                                                               measure_id
  , ARRAY_AGG(DISTINCT pm.drug_description)                                                drugs
  , ARRAY_AGG(DISTINCT pm.ndc)                                                             ndcs
  , COUNT(DISTINCT pm.ndc)                                                                 nd_rx
  , COUNT(DISTINCT pm.start_date)                                                          nd_start_dates
  , COUNT(DISTINCT pm.start_date) FILTER ( WHERE pm.is_2023_med )                          nd_start_dates_2023
  , MIN(pm.start_date)                                                                     ipsd
  , MAX(pm.last_day_of_meds)                                                               run_out_date
  , COUNT(DISTINCT d_treatment.day) FILTER (WHERE d_treatment.day <= pm.last_day_of_meds ) n_days_covered
  , COUNT(DISTINCT d_treatment.day)                                                        n_days_treatment
FROM
    ref.med_adherence_measures m
    JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
    JOIN _patient_meds pm ON pm.ndc = vs.code
        AND vs.code_type = 'NDC'
        AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
    LEFT JOIN ref.dates d_treatment ON d_treatment.day BETWEEN pm.start_date AND pm.expiration_date - 1
WHERE
      m.measure_id = 'PDC-DR'
--   AND m.measure_version = '2023' -- NOTE: vsn numb was null on import, assumed 2022
  AND m.table_id IN ('BG', 'SFU', 'TZD', 'DPP4', 'GLP1', 'MEG', 'SGLT2')
      -- exclude anyone with any insulin rx
  AND NOT EXISTS(SELECT
                     1
                 FROM
                     ref.med_adherence_measures im
                     JOIN ref.med_adherence_value_sets ivs ON ivs.value_set_id = im.value_set_id
                     JOIN _patient_meds ipm ON ipm.ndc = ivs.code AND ivs.code_type = 'NDC' AND
                                               ipm.start_date BETWEEN ivs.from_date AND ivs.thru_date
                 WHERE
                       im.measure_id = 'PDC-DR'
                   AND im.table_id = 'INSULINS'
--                    AND im.measure_version = '2022' -- NOTE: vsn numb was null on import, assumed 2022
                   AND ipm.patient_id = pm.patient_id
    )
GROUP BY
    1
HAVING
    COUNT(DISTINCT pm.start_date) > 1
;


create unique index _med_adherence_patient_id_measure_id_idx on _med_adherence(patient_id, measure_id);
------------------------------------------------------------------------------------------------------------------------
/* PDC-RASA: Medication Adherence for Patients on Renin Angiotensin System (“RAS”) Antagonists (Hypertension/MAH). Part D Measure. */
------------------------------------------------------------------------------------------------------------------------

INSERT
INTO
    _med_adherence (patient_id, measure_id, drugs, ndcs, nd_rx, nd_start_dates, nd_start_dates_2023, ipsd, run_out_date,
                    n_days_covered, n_days_treatment)
SELECT
    pm.patient_id
  , 'PDC-RASA'
  , ARRAY_AGG(DISTINCT pm.drug_description)                                                drugs
  , ARRAY_AGG(DISTINCT pm.ndc)                                                             ndcs
  , COUNT(DISTINCT pm.ndc)                                                                 nd_rx
  , COUNT(DISTINCT pm.start_date)                                                           nd_start_dates
  , COUNT(DISTINCT pm.start_date) FILTER ( WHERE pm.is_2023_med )                          nd_start_dates_2023
  , MIN(pm.start_date)                                                                     ipsd
  , MAX(pm.last_day_of_meds)                                                               run_out_date
  , COUNT(DISTINCT d_treatment.day) FILTER (WHERE d_treatment.day <= pm.last_day_of_meds ) n_days_covered
  , COUNT(DISTINCT d_treatment.day)                                                        n_days_treatment
FROM
    ref.med_adherence_measures m
    JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
    JOIN _patient_meds pm ON pm.ndc = vs.code AND vs.code_type = 'NDC' AND
                             pm.start_date BETWEEN vs.from_date AND vs.thru_date
    LEFT JOIN ref.dates d_treatment ON d_treatment.day BETWEEN pm.start_date AND pm.expiration_date - 1
WHERE
--       m.measure_name ~* 'renin'
    m.measure_id = 'PDC-RASA'
  AND m.table_id = 'RASA'
--   AND m.measure_version = '2022'
  -- exclude any with  sacubitril/valsartan rx
  AND NOT EXISTS(
        SELECT
            1
        FROM
            ref.med_adherence_measures xm
            JOIN ref.med_adherence_value_sets xvs ON xvs.value_set_id = xm.value_set_id
            JOIN _patient_meds xpm ON xpm.ndc = xvs.code AND xvs.code_type = 'NDC' AND
                                      pm.start_date BETWEEN vs.from_date AND vs.thru_date
        WHERE
              xm.measure_id = 'PDC-RASA'
          AND xm.table_id = 'SAC-VAL'
--           AND xm.measure_version = '2022'
          AND xpm.patient_id = pm.patient_id
    )
GROUP BY
    1
HAVING
    COUNT(DISTINCT pm.start_date) > 1
;


------------------------------------------------------------------------------------------------------------------------
/* PDC-STA  */
------------------------------------------------------------------------------------------------------------------------

INSERT
INTO
    _med_adherence (patient_id, measure_id, drugs, ndcs, nd_rx, nd_start_dates, nd_start_dates_2023, ipsd, run_out_date,
                    n_days_covered, n_days_treatment)
SELECT
    pm.patient_id
  , 'PDC-STA'
  , ARRAY_AGG(DISTINCT pm.drug_description)                                                drugs
  , ARRAY_AGG(DISTINCT pm.ndc)                                                             ndcs
  , COUNT(DISTINCT pm.ndc)                                                                 nd_rx
  , COUNT(DISTINCT pm.start_date)                                                           nd_start_dates
  , COUNT(DISTINCT pm.start_date) FILTER ( WHERE pm.is_2023_med )                          nd_start_dates_2023
  , MIN(pm.start_date)                                                                     ipsd
  , MAX(pm.last_day_of_meds)                                                               run_out_date
  , COUNT(DISTINCT d_treatment.day) FILTER (WHERE d_treatment.day <= pm.last_day_of_meds ) n_days_covered
  , COUNT(DISTINCT d_treatment.day)                                                        n_days_treatment
FROM
    ref.med_adherence_measures m
    JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
    JOIN _patient_meds pm
         ON pm.ndc = vs.code AND vs.code_type = 'NDC' AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
    LEFT JOIN ref.dates d_treatment ON d_treatment.day BETWEEN pm.start_date AND pm.expiration_date - 1
WHERE
      m.measure_id = 'PDC-STA'
  AND table_id = 'STATINS'
GROUP BY
    1
HAVING
    COUNT(DISTINCT pm.start_date) > 1
;

SELECT
    pm.patient_id
  , 'PDC-STA'
  , ARRAY_AGG(DISTINCT pm.drug_description)                                                drugs
  , ARRAY_AGG(DISTINCT pm.ndc)                                                             ndcs
  , COUNT(DISTINCT pm.ndc)                                                                 nd_rx
  , COUNT(DISTINCT pm.start_date)                                                           nd_start_dates
  , COUNT(DISTINCT pm.start_date) FILTER ( WHERE pm.is_2023_med )                          nd_start_dates_2023
  , MIN(pm.start_date)                                                                     ipsd
  , MAX(pm.last_day_of_meds)                                                               run_out_date
  , COUNT(DISTINCT d_treatment.day) FILTER (WHERE d_treatment.day <= pm.last_day_of_meds ) n_days_covered
  , COUNT(DISTINCT d_treatment.day)                                                        n_days_treatment
FROM
    ref.med_adherence_measures m
    JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
    JOIN _patient_meds pm
         ON pm.ndc = vs.code AND vs.code_type = 'NDC' AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
    LEFT JOIN ref.dates d_treatment ON d_treatment.day BETWEEN pm.start_date AND pm.expiration_date - 1
WHERE
      m.measure_id = 'PDC-STA'
  AND table_id = 'STATINS'
GROUP BY
    1
HAVING
    COUNT(DISTINCT pm.start_date) > 1

;


------------------------------------------------------------------------------------------------------------------------
/* SUPD Statin Use in Persons with Diabetes
  Not a PDC measure. Need to treat differently, closed if they have any statin med in yr.
   */
------------------------------------------------------------------------------------------------------------------------
-- INSERT
-- INTO
--     _med_adherence (patient_id, measure_id, drugs, ndcs, nd_rx, nd_start_dates, nd_start_dates_2023, ipsd, run_out_date,
--                     n_days_covered, n_days_treatment)
-- WITH
-- --     ≥2 prescription claims on different dates of service for any diabetes medication (Medication Table DIABETES) during the measurement year.
-- diabetics AS ( SELECT
--                    pm.patient_id
--                FROM
--                    ref.med_adherence_measures m
--                    JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
--                    JOIN _patient_meds pm ON pm.ndc = vs.code AND vs.code_type = 'NDC' AND
--                                             pm.start_date BETWEEN vs.from_date AND COALESCE(vs.thru_date, NOW()::DATE)
--                WHERE
--                      m.measure_id = 'SUPD'
--                  AND m.table_id = 'DIABETES'
--                  AND m.measure_version = '2023'
--                      -- fertility exclusion
--                  AND NOT EXISTS(
--                        SELECT
--                            1
--                        FROM
--                            ref.med_adherence_measures xm
--                            JOIN ref.med_adherence_value_sets xvs ON xvs.value_set_id = xm.value_set_id
--                            JOIN _patient_meds xpm ON xpm.ndc = xvs.code AND xvs.code_type = 'NDC' AND
--                                                      xpm.start_date BETWEEN xvs.from_date AND xvs.thru_date
--                        WHERE
--                              xm.measure_id = 'SUPD'
--                          AND xm.is_med = 'Y'
--                          AND xm.is_exclusion = 'Y'
--                          AND xm.measure_version = '2023'
--                          AND xpm.patient_id = pm.patient_id
--                    )
--                GROUP BY 1
--                HAVING
--                    COUNT(DISTINCT pm.written_date) > 1 )
-- SELECT
--     pm.patient_id
--   , 'SUPD'
--   , ARRAY_AGG(DISTINCT pm.drug_description)                       drugs
--   , ARRAY_AGG(DISTINCT pm.ndc)                                    ndcs
--   , COUNT(DISTINCT pm.ndc)                                        nd_rx
--   , COUNT(DISTINCT pm.sold_date)                                  nd_start_dates
--   , COUNT(DISTINCT pm.start_date) FILTER ( WHERE pm.is_2023_med ) nd_start_dates_2023
--   , MIN(pm.start_date)                                            ipsd
--   , MAX(pm.last_day_of_meds)                                      run_out_date
-- , COUNT(DISTINCT d_treatment.day) FILTER (WHERE d_treatment.day <= pm.last_day_of_meds ) n_days_covered
--   , COUNT(DISTINCT d_treatment.day)                               n_days_treatment
-- FROM
--     ref.med_adherence_measures m
--     JOIN ref.med_adherence_value_sets vs ON vs.value_set_id = m.value_set_id
--     JOIN _patient_meds pm
--          ON pm.ndc = vs.code AND vs.code_type = 'NDC' AND pm.start_date BETWEEN vs.from_date AND vs.thru_date
--     JOIN diabetics db ON db.patient_id = pm.patient_id
--     LEFT JOIN ref.dates d_treatment ON d_treatment.day BETWEEN pm.start_date AND pm.expiration_date
-- WHERE
--       m.measure_id = 'SUPD'
--   AND m.table_id = 'STATINS'
--   AND m.measure_version = '2023'
-- GROUP BY
--     1
-- ;


------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
-- rough_med_adherence_measures_v2
SELECT DISTINCT
    ma.patient_id
  , p.first_name
  , p.last_name
  , p.dob
  , ma.measure_id
  , ma.drugs
  , ma.ndcs
  , ma.nd_rx
  , ma.nd_start_dates
  , ma.nd_start_dates_2023
  , ma.ipsd
  , ma.run_out_date
  , ma.n_days_covered
  , ma.n_days_treatment
  , (ma.n_days_covered * 100.0 / ma.n_days_treatment)::DECIMAL(16, 2) PDC
  , py.name                                                          payer_name
  , STRING_AGG(DISTINCT rp.name, ',')                                referring_partner
  , STRING_AGG(DISTINCT rpo.name, ',')                               referring_partner_organization
  , string_agg(distinct x.days_supply, ',') current_day_supplies
  , string_agg(distinct x.start_date::text, ',') current_start_dates
  , string_agg(distinct x.descr::text, '; ') FILTER ( WHERE descr is not null ) current_start_dates_and_info
--   , STRING_AGG(DISTINCT u.full_name, ',') health_navigators
FROM
    _med_adherence ma
    JOIN fdw_member_doc.patients p ON p.id = ma.patient_id::BIGINT
    LEFT JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = p.id
    LEFT JOIN fdw_member_doc.payers py ON py.id = sp.patient_payer_id
    LEFT JOIN fdw_member_doc.patient_referring_partners prp ON prp.patient_id = ma.patient_id::BIGINT and prp."primary"
    LEFT JOIN fdw_member_doc.referring_partners rp ON prp.referring_partner_id = rp.id
    LEFT JOIN fdw_member_doc.msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id

    join (
        select distinct on(patient_id, ndc) patient_id, ndc,  days_supply, start_date, drug_description || ' Last Fill: ' || start_date::text || ' Days Supply: ' || days_supply::text descr
        from _patient_meds pm
        ORDER BY pm.patient_id, pm.ndc, pm.start_date desc
        ) x on x.patient_id = ma.patient_id AND x.ndc = ANY(ma.ndcs)
--     LEFT JOIN fdw_member_doc.msh_care_team_referring_partners ctrp ON rp.id = ctrp.referring_partner_id
--     LEFT JOIN fdw_member_doc.care_teams c ON ctrp.care_team_id = c.id
--     LEFT JOIN fdw_member_doc.care_team_members ctm ON c.id = ctm.care_team_id AND role = 'health_navigator'
--     LEFT JOIN fdw_member_doc.users u ON u.id = ctm.user_id
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
order by 15
;

--     expiration_date <= '2023-01-01';
