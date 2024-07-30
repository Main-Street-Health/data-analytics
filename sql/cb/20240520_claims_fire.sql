SELECT
    NOW() - query_start
  , *
FROM
    pg_stat_activity
WHERE
    state = 'active'
ORDER BY
    1 DESC ;



SELECT * FROM emr_origin_bill_item_dx;
SELECT * FROM emr_origin_bill_item;

SELECT
    icd10_code, i.code_formatted, count(*)
-- select *
FROM
    junk.elation_bill_item_dx_2024_05_15 bidx
left join icd10s i on i.code_formatted = bidx.icd10_code
where bidx.deletelog_id is not null and bill_item_deletion_time isnull
and i.id ISNULL
GROUP BY 1, 2
order by 3 desc
;
SELECT count(*)
FROM
    claims;

-- given table of claims and the icd 10 codes that need to be resubmitted
-- for each claim remove the icd10 codes from every line
  -- if the claim line has no icd10 codes pull the first remaining code from the first line

SELECT *
FROM
    claims c
    JOIN claim_responses cr ON c.id = cr.claim_id
WHERE
    cr.payer_claim_number IS NOT NULL
;

-- az banner is missing payer claim numbers, rest look good
-- az-banner	784
-- fl-sunshine	1
-- ma-uhc	4
-- tn-agp	1
-- tx-uhc	2
SELECT
    pay.id
  , COUNT(DISTINCT c.id)
FROM
    junk.deleted_claim_items_clean_claimed dc
    JOIN claims c ON c.id = dc.claims_id
    JOIN payers pay ON pay.id = c.payer_id
    LEFT JOIN claim_responses cr ON c.id = cr.claim_id AND cr.payer_claim_number IS NOT NULL
WHERE
      -- not re-added
      NOT EXISTS( SELECT
                      1
                  FROM
                      junk.elation_hell_encounters_all_codes_agg x
                  WHERE
                        dc.code_formatted_deleted = ANY (x.code_formatted_not_deleted)
                    AND dc.elation_encounter_bill_id = x.elation_encounter_bill_id )
      -- deleted code has to be on a claim
  AND EXISTS( SELECT
                  1
              FROM
                  junk.elation_hell_encounters_all_elation_encounter_codes_on_claims oc
              WHERE
                    oc.elation_encounter_bill_id = dc.elation_encounter_bill_id
                AND dc.code_formatted_deleted = ANY (oc.code_formatted_on_claims) )
  AND cr.id ISNULL -- missing_payer_claim_num
GROUP BY
    1
;
-- WTF happended to banner on 11/29
SELECT
    c.inserted_at::DATE
  , COUNT(*)
FROM
    claims c
    LEFT JOIN claim_responses cr ON cr.claim_id = c.id
        AND cr.payer_claim_number IS NOT NULL
WHERE
      c.payer_id = 14
  AND c.status = 'accepted'
  AND cr.id ISNULL
GROUP BY
    1
ORDER BY
    1;

-- order by c.id
;
SELECT
    c.id
  , cr.id
  , cr.inserted_at
  , cr.payer_claim_number
  , cr.claim_external_status_code
  , cr.claim_external_status_description
FROM
    claims c
    JOIN claim_responses cr ON cr.claim_id = c.id
--         AND cr.payer_claim_number IS NULL
WHERE
      c.payer_id = 14
  AND c.status = 'accepted'
ORDER BY
    c.id, cr.id;

--   AND cr.id ISNULL
;

SELECT
    c.inserted_at::DATE
  , COUNT(DISTINCT c.id)                                                     nd_claims
  , COUNT(DISTINCT c.id) FILTER ( WHERE cr.id IS NOT NULL )                  nd_claims_w_resp
  , COUNT(DISTINCT c.id) FILTER ( WHERE cr.payer_claim_number IS NOT NULL )  nd_claims_w_payer_claim_number
  , COUNT(DISTINCT cr.id)                                                    nd_responses
  , COUNT(DISTINCT cr.id) FILTER ( WHERE cr.payer_claim_number IS NOT NULL ) nd_responses_w_payer_claim_number

  , (COUNT(DISTINCT cr.id) * 1.0 / COUNT(DISTINCT c.id))::DECIMAL(16, 2)     resp_per_claim
  , (COUNT(DISTINCT cr.id) FILTER ( WHERE cr.payer_claim_number IS NOT NULL ) * 1.0 /
     COUNT(DISTINCT c.id))::DECIMAL(16, 2)                                   resp_per_claim_w_payer_claim_number
FROM
    claims c
    LEFT JOIN claim_responses cr ON cr.claim_id = c.id
WHERE
      c.payer_id = 14
  AND c.status = 'accepted'
GROUP BY
    1
ORDER BY
    1;
SELECT *
FROM
    payer_claim_configs WHERE payer_id = 14;




SELECT DISTINCT ON (c.id)
    c.id           claim_id
  , c.inserted_at claim_dt
  , cr.id          claim_resp_id
  , cr.inserted_at resp_dt
  , cr.payer_claim_number
  , cr.claim_external_status_code
  , cr.claim_external_status_description
FROM
    claims c
    LEFT JOIN claim_responses cr ON cr.claim_id = c.id AND cr.payer_claim_number IS NOT NULL
WHERE
      c.payer_id = 14
  AND c.status = 'accepted'
ORDER BY
    c.id, cr.id desc;


-- latest payer claim number for each claim
SELECT DISTINCT ON (c.id)
    c.id
  , cr.payer_claim_number
FROM
    junk.deleted_claim_items_clean_claimed dc
    JOIN claims c ON c.id = dc.claims_id
    JOIN payers pay ON pay.id = c.payer_id
    JOIN claim_responses cr ON c.id = cr.claim_id AND cr.payer_claim_number IS NOT NULL
WHERE
      -- not re-added
      NOT EXISTS( SELECT
                      1
                  FROM
                      junk.elation_hell_encounters_all_codes_agg x
                  WHERE
                        dc.code_formatted_deleted = ANY (x.code_formatted_not_deleted)
                    AND dc.elation_encounter_bill_id = x.elation_encounter_bill_id )
      -- deleted code has to be on a claim
  AND EXISTS( SELECT
                  1
              FROM
                  junk.elation_hell_encounters_all_elation_encounter_codes_on_claims oc
              WHERE
                    oc.elation_encounter_bill_id = dc.elation_encounter_bill_id
                AND dc.code_formatted_deleted = ANY (oc.code_formatted_on_claims) )
ORDER BY
    c.id, cr.inserted_at DESC
;
------------------------------------------------------------------------------------------------------------------------
/* 6/1 populating uhc payer claim numbers */
------------------------------------------------------------------------------------------------------------------------

-- begin;
-- end;
-- UPDATE rstage.elation_hell2_claims_todo t
-- SET
--     parsed_edi = NULL
-- FROM
--     payers pay
-- WHERE
--       pay.name ~* 'uhc'
--   AND pay.id = t.payer_id
--   AND t.parsed_edi ? 'bill_items';


-- ALTER TABLE rstage.elation_hell2_claims_todo
--     ADD latest_payer_claim_number TEXT;


WITH
    parsed AS ( SELECT DISTINCT ON (claim_id)
                    claim_id
                  , lines ->> 'payer_claim_number' payer_claim_number
                  , lines ->> 'date_of_service'    dos
                  , lines
                FROM
                    ( SELECT
                          claim_id
                        , JSONB_ARRAY_ELEMENTS(parsed_edi -> 'claim_status_response' -> 'parsed_response') lines
                      FROM
                          rstage.elation_hell2_claims_todo
                      WHERE
                          parsed_edi IS NOT NULL ) x
                WHERE
                    lines ->> 'payer_claim_number' IS NOT NULL
--   and claim_id = 11148
                ORDER BY
                    claim_id, (lines ->> 'status_effective_date') :: DATE DESC
                            , (lines ->> 'payer_claim_number') DESC
-- order by claim_id, (lines->>'date_of_service') :: date desc
    )
UPDATE rstage.elation_hell2_claims_todo t
SET
    latest_payer_claim_number = p.payer_claim_number
FROM
    parsed p
WHERE
    p.claim_id = t.claim_id
and t.latest_payer_claim_number ISNULL
;



WITH
    compare AS ( SELECT DISTINCT ON (t.claim_id)
                     t.claim_id
                   , t.latest_payer_claim_number
                   , cr.payer_claim_number
                   , t.latest_payer_claim_number IS DISTINCT FROM cr.payer_claim_number mismatched
                 FROM
                     rstage.elation_hell2_claims_todo t
                     JOIN payers pay ON pay.id = t.payer_id
                     LEFT JOIN claim_responses cr ON cr.claim_id = t.claim_id
                 WHERE
                       pay.name ~* 'uhc'
                   AND latest_payer_claim_number IS NOT NULL
                 ORDER BY
                     t.claim_id, cr.id DESC )
SELECT
    mismatched
  , COUNT(*)
FROM
    compare
GROUP BY
    1
;



------------------------------------------------------------------------------------------------------------------------
/* 6/3 tag dupes */
------------------------------------------------------------------------------------------------------------------------
-- alter table rstage.elation_hell2_claims_todo add was_split_claim boolean not null default false;
-- WITH
--     exploded AS ( SELECT
--                       t.claim_id
--                     , c2.emr_bill_id
--                     , JSONB_ARRAY_ELEMENTS(c2.data -> 'bill_items') bill_item
--                   FROM
--                       rstage.elation_hell2_claims_todo t
--                       JOIN claims c ON c.id = t.claim_id AND c.status = 'accepted'
--                       JOIN claims c2 ON c2.emr_bill_id = c.emr_bill_id AND c2.status = 'accepted' )
--   , splits   AS ( SELECT DISTINCT
--                       emr_bill_id
--                   FROM
--                       exploded e
--                   WHERE
--                       e.bill_item ->> 'procedure_code' = '99499' )
--
-- UPDATE rstage.elation_hell2_claims_todo t
-- SET
--     was_split_claim = TRUE
-- FROM
--     splits s
--     JOIN claims c
--          ON c.emr_bill_id = s.emr_bill_id AND c.status = 'accepted'
-- WHERE
--     t.claim_id = c.id
-- and not t.was_split_claim
-- ;

------------------------------------------------------------------------------------------------------------------------
/* 6/3 resubmit test for new uhc payer claim numbers */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS junk._uhc_new_payer_claim_numbers_test_20240603;
CREATE TABLE junk._uhc_new_payer_claim_numbers_test_20240603 AS
WITH
    post_payer_claim_number_test AS ( SELECT
                                          t.*
                                        , pay.name
                                        , ROW_NUMBER() OVER (PARTITION BY pay.name ORDER BY traunch, batch_no) rn
                                      FROM
                                          rstage.elation_hell2_claims_todo t
                                          JOIN payers pay ON pay.id = t.payer_id
                                      WHERE
                                            pay.name ~* 'uhc'
                                        AND NOT was_split_claim
                                        AND latest_payer_claim_number IS NOT NULL
                                        AND t.resubmitted_date ISNULL
                                        AND t.traunch = 1 )
SELECT *
FROM
    post_payer_claim_number_test  p
where p.rn <= 3
;
SELECT *
FROM
    junk._uhc_new_payer_claim_numbers_test_20240603;

begin;
UPDATE claims c
SET
    status     = 'flagged_for_resubmission'
  , data       = JSONB_SET(data, '{payer_claim_number}', ('"' || j.latest_payer_claim_number || '"')::jsonb)
  , updated_at = NOW()
FROM
    junk._uhc_new_payer_claim_numbers_test_20240603 j
WHERE
    j.claim_id = c.id;
end;


-- ran elixir resubmittion code,
-- fix, had splits showing up
-- begin;
-- update claims c
-- set status = 'accepted', updated_at = now(), resubmitted_at = null, is_resubmitted = false
-- FROM
--     junk._uhc_new_payer_claim_numbers_test_20240603 j
-- where c.id = j.claim_id;
-- DELETE
-- FROM
--     claims c
-- WHERE c.status = 'new';
-- end;


SELECT c.*
FROM
    junk._uhc_new_payer_claim_numbers_test_20240603 j
left join claims c on c.id = j.claim_id

SELECT *
FROM
    claims WHERE  emr_bill_id = 779246227619996 and status != 'new';

SELECT *
FROM
    rstage.elation_hell2_claims_todo
WHERE
    claim_id = 191928
    ;



SELECT *
FROM
    claims where status = 'new';


SELECT now(), * FROM oban_crons where name ~* 'claims';

-- SELECT * FROM oban_crons WHERE worker ~* 'ResubmissionWorker ';
-- SELECT * FROM claims where status = 'flagged_for_resubmission';


begin;

WITH
    resubmitted_records AS ( SELECT
                                 todo.claim_id                    old_claim_id
                               , MAX(new_claim.inserted_at::DATE) claim_resubmitted_date
                               , MAX(new_claim.id)                new_claim_id
                             FROM
                                 claims new_claim
                                 JOIN rstage.elation_hell2_claims_todo todo
                                      ON todo.claim_id = new_claim.original_claim_id
                             where new_claim.status = 'new'
                             GROUP BY 1 )

UPDATE rstage.elation_hell2_claims_todo
SET
    resubmitted_date = claim_resubmitted_date, resubmitted_claim_id = new_claim_id
FROM
    resubmitted_records rr
WHERE
    claim_id = rr.old_claim_id;
commit;

-- 6/3 these are the resubmit claims
SELECT
    c.id                                                 claim_id
  , c.status
  , c.payer_id
  , ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY cr.id) resp_number
  , cr.claim_external_status_description
  , cr.payer_claim_number
  , cr.inserted_at
FROM
    junk._uhc_new_payer_claim_numbers_test_20240603 j
    JOIN rstage.elation_hell2_claims_todo t ON j.claim_id = t.claim_id
    JOIN claims c ON c.original_claim_id = t.claim_id
    LEFT JOIN claim_responses cr ON c.id = cr.claim_id
-- claim_id IN (190729, 189701, 182347, 185309, 185397, 191051, 193548, 192970, 191241, 192741, 192730, 191473, 192200, 189746, 189755, 192502, 188384, 192067, 191625, 186884, 186960)
ORDER BY
    c.id, cr.id
;

-- 6/4  responses look good
SELECT c.status, count(DISTINCT cr.claim_id) nd_claim_w_resp_w_payer_claim_number
FROM
    junk._uhc_new_payer_claim_numbers_test_20240603 j
    JOIN rstage.elation_hell2_claims_todo t ON j.claim_id = t.claim_id
    JOIN claims c ON c.original_claim_id = t.claim_id
    JOIN claim_responses cr ON c.id = cr.claim_id AND payer_claim_number IS NOT NULL
GROUP BY c.status
;

-- claim_id IN (190729, 189701, 182347, 185309, 185397, 191051, 193548, 192970, 191241, 192741, 192730, 191473, 192200, 189746, 189755, 192502, 188384, 192067, 191625, 186884, 186960)
;

-- holding back uhc ks
SELECT * FROM payers where id = 6;

------------------------------------------------------------------------------------------------------------------------
/* 6/4 re do batching */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _plan;
CREATE TEMP TABLE _plan AS
SELECT *
FROM
    ( VALUES
          (40, 30),
          (1, 30),
          (10, 60),
          (28, 30),
          (7, 30),
          (29, 60),
          (14, 60),
          (8, 30),
          (27, 30),
          (31, 30),
          (21, 60),
          (30, 60),
          (4, 60),
-- (6, HOLD),
          (13, 60),
          (18, 30),
          (2, 60),
          (5, 60),
          (12, 60) ) x(payer_id, days_denom);

DROP TABLE IF EXISTS _traunch1_counts;
CREATE TEMP TABLE _traunch1_counts AS
SELECT
    payer_id
  , COUNT(*) n
FROM
    rstage.elation_hell2_claims_todo t
WHERE
    traunch = 1 and resubmitted_date ISNULL
GROUP BY
    payer_id
    ;

DROP TABLE IF EXISTS junk.claim_resubmition_payer_plans;
CREATE TABLE junk.claim_resubmition_payer_plans AS
SELECT
    tc.payer_id
  , CEILING(tc.n * 1.0 / p.days_denom) to_submit_daily
  , p.days_denom
  , NOW()::DATE                        start_date
  , NOW()::DATE + p.days_denom         end_date
FROM
    _traunch1_counts tc
    JOIN _plan p ON tc.payer_id = p.payer_id;

SELECT * FROM junk.claim_resubmition_payer_plans ;
------------------------------------------------------------------------------------------------------------------------
/* set payer claim numbers from our resp files */
------------------------------------------------------------------------------------------------------------------------
begin;
WITH
    most_recent_resp AS ( SELECT DISTINCT ON (cr.claim_id)
                              cr.claim_id
                            , cr.payer_claim_number
                          FROM
                              rstage.elation_hell2_claims_todo t
                              JOIN claim_responses cr ON cr.claim_id = t.claim_id
                          WHERE
                                t.latest_payer_claim_number ISNULL
                            AND cr.payer_claim_number IS NOT NULL
                          ORDER BY
                              cr.claim_id, cr.id DESC )
UPDATE rstage.elation_hell2_claims_todo t
SET
    latest_payer_claim_number = r.payer_claim_number
FROM
    most_recent_resp r
WHERE
    r.claim_id = t.claim_id
;
end;
------------------------------------------------------------------------------------------------------------------------
/* flag the todos with correct resubmit ordering
   Priority from Alan
  Traunch 1
    2023 Is Medicare
    2024 Is Medicare
    2023 not is medicare
    2024 not is medicare
   */
------------------------------------------------------------------------------------------------------------------------
-- alter table rstage.elation_hell2_claims_todo add COLUMN resubmit_order int;
SELECT *
FROM
    rstage.elation_hell2_claims_todo;
WITH
    prioritized AS (
    SELECT *
    , CASE
        -- non split
        WHEN t.encounter_year = 2023 AND t.is_medicare     AND NOT was_split_claim and latest_payer_claim_number is not null THEN 1
        WHEN t.encounter_year = 2024 AND t.is_medicare     AND NOT was_split_claim and latest_payer_claim_number is not null THEN 2
        WHEN t.encounter_year = 2023 AND NOT t.is_medicare AND NOT was_split_claim and latest_payer_claim_number is not null THEN 3
        WHEN t.encounter_year = 2024 AND NOT t.is_medicare AND NOT was_split_claim and latest_payer_claim_number is not null THEN 4
        -- splits
        WHEN t.encounter_year = 2023 AND t.is_medicare     and latest_payer_claim_number is not null                        THEN 5
        WHEN t.encounter_year = 2024 AND t.is_medicare     and latest_payer_claim_number is not null                        THEN 6
        WHEN t.encounter_year = 2023 AND NOT t.is_medicare and latest_payer_claim_number is not null                        THEN 7
        WHEN t.encounter_year = 2024 AND NOT t.is_medicare and latest_payer_claim_number is not null                        THEN 8
        ELSE 9
    END priority
                     FROM
                         rstage.elation_hell2_claims_todo t
                     WHERE
                           t.traunch = 1
                       AND t.resubmitted_date ISNULL )
  , ordered     AS ( SELECT
                         claim_id
                       , ROW_NUMBER() OVER (PARTITION BY payer_id ORDER BY p.priority, encounter_date) resubmit_order
                     FROM
                         prioritized p )
UPDATE rstage.elation_hell2_claims_todo t
SET
    resubmit_order = o.resubmit_order
FROM
    ordered o
WHERE
    o.claim_id = t.claim_id
;
-- make sure no 21/22 claims
UPDATE
    rstage.elation_hell2_claims_todo
SET
    resubmit_order = NULL
WHERE
      encounter_year < 2023
  AND resubmit_order IS NOT NULL;


------------------------------------------------------------------------------------------------------------------------
/* flag for resubmit */
------------------------------------------------------------------------------------------------------------------------
begin;
end;
WITH
    ordered    AS ( SELECT
                        t.claim_id
                      , t.payer_id
                      , t.resubmit_order
                      , t.latest_payer_claim_number
                      , ROW_NUMBER() OVER (PARTITION BY t.payer_id ORDER BY t.resubmit_order) position
                      , p.to_submit_daily
                    FROM
                        rstage.elation_hell2_claims_todo t
                        JOIN junk.claim_resubmition_payer_plans p ON p.payer_id = t.payer_id
                    WHERE
                          t.resubmitted_date ISNULL
                      AND t.latest_payer_claim_number IS NOT NULL
                      AND t.traunch = 1
                      AND NOT t.was_split_claim )
  , claims_upd AS (
    UPDATE claims c
        SET
            status = 'flagged_for_resubmission'
            , data = JSONB_SET(data, '{payer_claim_number}', ('"' || o.latest_payer_claim_number || '"')::jsonb)
            , updated_at = NOW()
        FROM
            ordered o
        WHERE
            o.claim_id = c.id
        AND o.position <= o.to_submit_daily
        RETURNING c.id )
UPDATE rstage.elation_hell2_claims_todo t
SET
    resubmitted_date = NOW()::DATE
FROM
    claims_upd cu
WHERE
    cu.id = t.claim_id;

-- elixir
-- MD.Claims.ResubmissionWorker.run_resubmission()
------------------------------------------------------------------------------------------------------------------------
/* update with new claim ingo */
-- need to run this after elixir code
------------------------------------------------------------------------------------------------------------------------
-- note this claim wasn't originally split but more icd's were added and it is now split
-- need to investigate how it turns out, right now it is going to use the payer claim number as if it were a resubmition
-- SELECT * FROM claims c WHERE c.id = 200489;

begin;
WITH
    resubmitted_records AS ( SELECT
                                 todo.claim_id                    old_claim_id
                               , MAX(new_claim.inserted_at::DATE) claim_resubmitted_date
                               , MAX(new_claim.id)                new_claim_id
                             FROM
                                 claims new_claim
                                 JOIN rstage.elation_hell2_claims_todo todo
                                      ON todo.claim_id = new_claim.original_claim_id
                             where new_claim.status = 'new'
                             GROUP BY 1 )

UPDATE rstage.elation_hell2_claims_todo
SET
    resubmitted_date = claim_resubmitted_date, resubmitted_claim_id = new_claim_id
FROM
    resubmitted_records rr
WHERE
    claim_id = rr.old_claim_id;
end;

------------------------------------------------------------------------------------------------------------------------
/* check status */
------------------------------------------------------------------------------------------------------------------------
SELECT
--     t.payer_id ,
    t.resubmitted_date
  , c.status
  , COUNT(distinct t.claim_id)
FROM
    rstage.elation_hell2_claims_todo t
    JOIN claims c ON t.claim_id = c.original_claim_id
where c.inserted_at >= '2024-06-04'
GROUP BY
    1,2
ORDER BY
    1, 2;
SELECT
--     t.payer_id ,
    t.resubmitted_date
  , c.status
  , c.id
  , cr.claim_external_status_description
     , p.name
, jsonb_array_elements( t.parsed_edi -> 'claim_status_response' -> 'parsed_response' ) -> 'payer_claim_number' line
FROM
    rstage.elation_hell2_claims_todo t
    JOIN claims c ON t.claim_id = c.original_claim_id
    JOIN claim_responses cr ON c.id = cr.claim_id
join payers p on p.id = t.payer_id

WHERE
      NOT cr.is_claim_accepted
  AND c.status != 'accepted'
  AND t.resubmitted_date >= '2024-06-04'::DATE
order by cr.id
SELECT *
FROM
    ( SELECT
          JSONB_ARRAY_ELEMENTS(parsed_edi -> 'claim_status_response' -> 'parsed_response') ->
          'payer_claim_number' line
      FROM
          rstage.elation_hell2_claims_todo ) x
WHERE
    line IS NOT NULL
;

A) codes on the main claim that were deleted
    - payer claim number 1
B) codes on the supp claim that were deleted (split cpt = 99499)
    - payer claim number 2

B) invalidate it (VOID)

resubmit a and b

Regenerate A (non 99499) -> split into A` B` use payer claim numbers from A and B
SELECT *
FROM
    payer_claim_configs;

SELECT id, name, should_run_claiming

FROM
    payers;
    ;



SELECT
    COUNT(*)
FROM
    rstage.elation_hell2_claims_todo
WHERE
    payer_id = 6
-- and traunch = 1
;

------------------------------------------------------------------------------------------------------------------------
/* traunch 2 */
------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS _plan;
-- CREATE TEMP TABLE _plan AS
-- SELECT *
-- FROM
--     ( VALUES
--           (40, 0),
--           (1, 30),
--           (10, 60),
--           (7, 30),
--           (28, 30),
--           (29, 60),
--           (14, 30),
--           (8, 30),
--           (27, 0),
--           (31, 0),
--           (21, 30),
--           (30, 30),
--           (4, 30),
-- -- (6,hold),
--           (13, 60),
--           (18, 0),
--           (2, 30),
--           (5, 60),
--           (12, 60) ) x(payer_id, days_denom);
-- SELECT *
-- FROM
--     _plan where days_denom = 0;
--
--
--
--
-- DROP TABLE IF EXISTS _traunch2_counts;
-- CREATE TEMP TABLE _traunch2_counts AS
-- SELECT
--     payer_id
--   , COUNT(*) n
-- FROM
--     rstage.elation_hell2_claims_todo t
-- WHERE
--     traunch = 2 and resubmitted_date ISNULL
-- GROUP BY
--     payer_id
--     ;
--
-- -- DROP TABLE IF EXISTS junk.claim_resubmition_payer_plans2;
-- -- CREATE TABLE junk.claim_resubmition_payer_plans2 AS
-- SELECT
--     tc.payer_id
--   , tc.n
-- --   , CEILING(tc.n * 1.0 / p.days_denom) to_submit_daily
--   , p.days_denom
--   , NOW()::DATE                start_date
--   , NOW()::DATE + p.days_denom end_date
-- FROM
--     _traunch2_counts tc
--     JOIN _plan p ON tc.payer_id = p.payer_id
--
-- ;
--
--
