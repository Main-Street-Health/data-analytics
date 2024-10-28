alter table prd.patient_medications add column is_part_d BOOLEAN not null default true;
alter table prd.patient_medication_deletions add column is_part_d BOOLEAN not null default true;



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


WITH
    mapping AS ( SELECT *
                 FROM
                     ( VALUES
                           (1, 'foo', TRUE),
                           (2, 'bar', TRUE),
                           (3, 'bear', FALSE) ) x(id, name, is_part_d) )
  , values  AS ( SELECT *
                 FROM
                     ( VALUES
                           (1, 'foo'),
                           (3, 'bear'),
                           (4, 'four') ) x(id, name) )
SELECT
    v.name
  , coalesce(BOOL_OR(m.is_part_d), true)
FROM
    values v
    LEFT JOIN mapping m ON v.id = m.id
GROUP BY
    1
;

;

