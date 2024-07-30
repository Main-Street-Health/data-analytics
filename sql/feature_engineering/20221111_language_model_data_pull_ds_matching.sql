SELECT sfere_id, analytics_member_id, reporting_rec_hrs FROM junk.ds_rec_hrs_modelling_20221111;
ALTER table junk.ds_rec_hrs_modelling_20221111 ALTER COLUMN submitted_at type date using submitted_at::date;

-- DELETE FROM junk.ds_rec_hrs_modelling_20221111 WHERE sfere_id in ( 16704,7616,18074,14420,14723,10413,14496,11442,14532 ); -- dupes
-- CREATE INDEX ds_rec_hrs_modelling_20221111_payer_member_idx on junk.ds_rec_hrs_modelling_20221111(payer_id, analytics_member_id);
-- create INDEX ds_rec_hrs_modelling_20221111_submitted_at on junk.ds_rec_hrs_modelling_20221111(submitted_at);
-- SELECT count(*) , count(distinct sfere_id) FROM junk.ds_rec_hrs_modelling_20221111; -- should be 0


WITH
    encounter_level   AS ( SELECT DISTINCT
                               member_id
                             , mco_id
                             , date_from
                             , sf.sfere_id
                           FROM
                               cb.claims c
                               JOIN junk.ds_rec_hrs_modelling_20221111 sf ON sf.analytics_member_id = c.member_id
                                   AND sf.payer_id = c.mco_id
                                   AND c.paid_date BETWEEN sf.submitted_at - '12 months'::INTERVAL AND sf.submitted_at
                               LEFT JOIN ref.service_types st ON st.id = c.service_type_id
                           WHERE
                                 c.mco_id = 2
                             AND c.member_id = 1
                             AND c.service_type_id NOT IN (12, 13, 17, 18, 10, 15, 16)
                             AND NOT c.is_rx )
  , lagged_encounters AS ( SELECT
                               el.*
                             , LAG(date_from) OVER (PARTITION BY member_id ORDER BY date_from) prev_claim_date
                           FROM
                               encounter_level el )

SELECT
    le.member_id
  , c.date_from
  , le.sfere_id
  , sf.reporting_rec_hrs
  , le.date_from - prev_claim_date                                                   days_since_last_encounter
  , ARRAY_AGG(DISTINCT c.procedure_code ORDER BY c.procedure_code)
    FILTER ( WHERE c.procedure_code IS NOT NULL)                                     cpts_by_line
  , ARRAY_AGG(DISTINCT cd.diag ORDER BY cd.diag) FILTER ( WHERE cd.diag IS NOT NULL) icds_by_alpha
FROM
    lagged_encounters le
    JOIN junk.ds_rec_hrs_modelling_20221111 sf ON sf.analytics_member_id = le.member_id AND sf.payer_id = le.mco_id
    JOIN cb.claims c ON c.member_id = le.member_id
        AND c.mco_id = le.mco_id
        AND c.date_from = le.date_from
    LEFT JOIN cb.claims_diagnosis cd ON c.id = cd.claim_id
WHERE
      c.mco_id = 2
  AND cd.mco_id = 2
  AND c.member_id = 1
  AND NOT c.is_rx -- remove for ndc language model
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
    1, 2, 3, 4, 5;







