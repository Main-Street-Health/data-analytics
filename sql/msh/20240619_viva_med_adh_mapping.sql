
--Raw Table:
SELECT *
FROM
    raw.viva_rx_adherence
WHERE
      adh_dm_last_fill_date = '2024-4-11'
  AND adh_dm_next_fill_date = '2024-7-10';

select '2024-11-7'::date - '2024-6-7'::date; -- 146
--Staging Table of Refined data:
SELECT
    s.id
  , member_id
  , excluded_reason_ids
  , is_excluded
  , measure_key
  , last_fill_date
  , days_supply
  , next_fill_date
  , absolute_fail_date
  , fill_count
  , pharmacy_name
  , r.adh_dm
  , r.adh_dm_1fill
  , r.adh_dm_last_fill_date
  , r.adh_dm_next_fill_date
  , r.cms_insulin
  , r.acumen_adh_exclusions
  , r.supd_diagnosis_exclusions
  , r.hospice
  , r.snf
FROM
    staging.rx_adherence s
    JOIN raw.viva_rx_adherence r ON s.raw_id = r.id
WHERE
      s.payer_id = 48
  AND last_fill_date = '2024-04-11'
  AND next_fill_date = '2024-07-10'
  AND s.id = 32574052
;

SELECT
    s.id
  , member_id
  , excluded_reason_ids
  , is_excluded
  , measure_key
  , last_fill_date
  , days_supply
  , next_fill_date
  , absolute_fail_date
  , fill_count
  , pharmacy_name
  , r.adh_dm
  , r.adh_dm_1fill
  , r.adh_dm_last_fill_date
  , r.adh_dm_next_fill_date
  , r.cms_insulin
  , r.acumen_adh_exclusions
  , r.supd_diagnosis_exclusions
  , r.hospice
  , r.snf
, r.*
FROM
    staging.rx_adherence s
    JOIN raw.viva_rx_adherence r ON s.raw_id = r.id
WHERE
      s.payer_id = 48
-- and r.adh_dm = '0'
--   AND last_fill_date = '2024-04-11'
--   AND next_fill_date = '2024-07-10'
--   AND s.id = 32574052
;

select * from raw.viva_rx_adherence r
-- where adh_dm = '0'
    where adh_dm_last_fill_date = ''
and adh_dm = '1'
;

--Sproc Containing logic:
call staging.rts_viva_gap_rx_adherence();
adh_dm = '1' -- has measure
adh_dm = '0' and adh_dm_last_fill_date is not null and adh_dm_1fill = 1 -- has measure and is first fill
adh_dm = '0' and adh_dm_last_fill_date is not null and adh_dm_1fill = 0 -- has measure but is excluded
adh_dm = '0' and adh_dm_last_fill_date isnull -- does not have measure

adh_dm = '0' and adh_dm_last_fill_date is not null and adh_dm_1fill = 0 and cms_insulin = 1 -- has measure but has insulin exclusion
adh_dm = '0' and adh_dm_last_fill_date is not null and adh_dm_1fill = 0 and cms_hospice = 1 -- has measure but has hospice exclusion
