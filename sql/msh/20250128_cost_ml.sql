SELECT *
FROM
    prd.members;

SELECT *
FROM
    analytics.prd.member_months
LIMIT 10;

SELECT *
FROM
    prd.eligibility_months
LIMIT 10
    ;

SELECT *
FROM
    analytics.ref.line_of_businesses
ORDER BY
    id
;

SELECT em.bom, count(m.id)
FROM
    prd.members m
    JOIN prd.eligibility_months em ON em.payer_id = m.payer_id AND em.member_id = m.id
WHERE
      m.payer_id = 81
  AND em.bom BETWEEN '2020-01-01' AND '2024-12-01'
GROUP BY em.bom
order by 1
;


SELECT *
FROM
    fdw_member_doc.payers where id = 81;
-- time frames of good claims data by plan
-- limit members by lob?


------------------------------------------------------------------------------------------------------------------------
/* simple features
  age
  gender
  12 total tc bucket
  pmpm bucketed
  ip days
  er visits
  rx spend
  raf score
*/
------------------------------------------------------------------------------------------------------------------------