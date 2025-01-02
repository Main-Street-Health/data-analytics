-- alter table prd.patient_medications drop column is_part_d;
alter table prd.patient_medications add column is_part_d BOOLEAN;
-- alter table prd.patient_medication_deletions drop column is_part_d;
alter table prd.patient_medication_deletions add column is_part_d BOOLEAN;

-- alter table patient_medication_fills add COLUMN is_part_d boolean not null default true;


UPDATE analytics.prd.patient_medications pm
SET
    is_part_d  = FALSE
  , updated_at = NOW()
FROM
    sure_scripts_med_history_details mhd
--     JOIN _mapping m ON m.payment_code = mhd.payment_code AND NOT m.is_part_d
WHERE
      pm.last_src_id = mhd.id
  AND mhd.payment_code NOT IN ('02', '03', '04')
;


update fdw_member_doc.patient_medication_fills c
set is_part_d = false, updated_at = now()
from prd.patient_medications pm
where c.analytics_id = pm.id
and not pm.is_part_d
;

SELECT *
FROM
    fdw_member_doc.patient_medication_fills f
join prd.patient_medications pm on pm.id = f.analytics_id
;
------------------------------------------------------------------------------------------------------------------------
/* REDO make nullable */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS _payment_code_part_d_mapping;
CREATE TEMP TABLE _payment_code_part_d_mapping AS
SELECT *
FROM
    ( VALUES
          ('01', 'cash', FALSE),
          ('02', 'caid', TRUE),
          ('03', 'care', TRUE),
          ('04', 'com', TRUE),
          ('05', 'va', FALSE),
          ('06', 'workers comp', FALSE),
          ('07', 'indian', FALSE),
          ('99', 'other', FALSE) ) x(payment_code, name, is_part_d);


SELECT *
FROM
    _payment_code_part_d_mapping;

UPDATE analytics.prd.patient_medications pm
SET
    is_part_d  = mhd.payment_code in ('02', '03', '04')
  , updated_at = NOW()
FROM
    sure_scripts_med_history_details mhd
--     JOIN _payment_code_part_d_mapping m ON m.payment_code = mhd.payment_code
WHERE
      pm.last_src_id = mhd.id
and mhd.payment_code is not null
;
--   AND m.is_part_d IS DISTINCT FROM pm.is_part_d
;


UPDATE fdw_member_doc.patient_medication_fills c
SET
    is_part_d = pm.is_part_d, updated_at = NOW()
FROM
    prd.patient_medications pm
WHERE
      c.analytics_id = pm.id
  AND pm.is_part_d IS DISTINCT FROM c.is_part_d
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    analytics.oban.oban_jobs
where args->>'sql' ~* 'med_adh'
ORDER BY
    id DESC;
update oban.oban_jobs set state = 'available', scheduled_at = now() where id = 185204979;
select * from  oban.oban_jobs  where id = 185204979;

SELECT inserted_at, reason_for_query
FROM
    sure_scripts_panel_patients WHERE patient_id = 368229;

SELECT * FROM (
  VALUES
    (1, 'foo'),
    (2, 'bar')
  ) x(id, name);


