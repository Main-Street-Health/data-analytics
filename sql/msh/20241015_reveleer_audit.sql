------------------------------------------------------------------------------------------------------------------------
/* already closed error but sent compliance on most recent files?
   seems to be inactives
   */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    reveleer_chases rc
WHERE
    rc.id IN (
           1634701, 1635182, 1635607, 1635635, 1635752, 1635934, 1652108, 1659319, 1706800, 1749043,
           10543, 1624027, 1662546, 1696022, 1710221, 1717144, 1717543, 1781519, 1875015, 1875179, 1875225, 1875419,
           1875997, 1876553, 1876836, 1876842, 1876845, 1876846, 1876848, 1877036, 1878072, 1924777, 3083206, 35572,
           37563, 3781740, 3832279, 383228
        );

SELECT reveleer_chase_id, count(*)
FROM
    reveleer_compliance_file_details
WHERE
    reveleer_chase_id IN (
           1634701, 1635182, 1635607, 1635635, 1635752, 1635934, 1652108, 1659319, 1706800, 1749043,
           10543, 1624027, 1662546, 1696022, 1710221, 1717144, 1717543, 1781519, 1875015, 1875179, 1875225, 1875419,
           1875997, 1876553, 1876836, 1876842, 1876845, 1876846, 1876848, 1877036, 1878072, 1924777, 3083206, 35572,
           37563, 3781740, 3832279, 383228
        )
GROUP BY reveleer_chase_id
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM reveleer_chases WHERE id = 149012;
SELECT * FROM reveleer_chase_file_details where reveleer_chase_id = 149012 ;
SELECT * FROM reveleer_compliance_file_details where reveleer_chase_id = 149012 ;
SELECT * FROM reveleer_compliance_file_details where sample_id = '149012' ;
SELECT * FROM fdw_member_doc.qm_patient_measures WHERE id = 745196;
SELECT *
FROM
    fdw_member_doc.qm_ref_measures;
------------------------------------------------------------------------------------------------------------------------
/* inactive */
------------------------------------------------------------------------------------------------------------------------
SELECT * FROM junk.inactive_in_reveleer_202415;


DROP TABLE IF EXISTS _inactive_chases;
CREATE TEMP TABLE _inactive_chases AS
SELECT
    rc.id
  , rc.external_chase_id
  , rp.name
  , rp.reveleer_id         rev_proj_id
  , rc.patient_id
  , qpm.measure_key
  , qpm.is_active
  , i.client_chase_key IS NOT NULL on_prev_inactive_file
FROM
    reveleer_chases rc
    JOIN fdw_member_doc.qm_patient_measures qpm ON qpm.id = ANY (rc.qm_patient_measure_ids)
    JOIN reveleer_projects rp ON rp.id = rc.reveleer_project_id
    LEFT JOIN junk.inactive_in_reveleer_202415 i ON rc.id = i.client_chase_key
WHERE
      rc.yr = 2024
  AND qpm.operational_year = 2024
  AND i.client_chase_key ISNULL
  AND NOT qpm.is_active
  AND EXISTS( SELECT 1 FROM reveleer_chase_file_details cfd WHERE cfd.chase_id = rc.id AND cfd.yr = 2024 )
;


-- inactive_chases_not_already_inactived_20241015
SELECT
    id chase_id, external_chase_id, name, rev_proj_id, patient_id, measure_key, is_active, on_prev_inactive_file
FROM
    _inactive_chases ;

------------------------------------------------------------------------------------------------------------------------
/* reconciliation */
------------------------------------------------------------------------------------------------------------------------

drop table junk.reveleer_life_of_chase_20241015;
SELECT * FROM junk.reveleer_life_of_chase_20241015;
create INDEX  on junk.reveleer_life_of_chase_20241015(chaseid);
create INDEX  on junk.reveleer_life_of_chase_20241015(clientchasekey);

------------------------------------------------------------------------------------------------------------------------
/* external chase id not set */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _did_not_get_success_file;
CREATE TEMP TABLE _did_not_get_success_file AS
SELECT
    chaseid
  , clientchasekey
  , rp.reveleer_id reveleer_project_id
, rc.inserted_at
FROM
    reveleer_chases rc
    JOIN junk.reveleer_life_of_chase_20241015 loc ON loc.clientchasekey = rc.id AND rc.external_chase_id ISNULL
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id;


SELECT * FROM _did_not_get_success_file ;
-- update
UPDATE reveleer_chases rc
SET
    is_confirmed_in_reveleer_system = TRUE
  , external_chase_id               = sf.chaseid
  , confirmed_in_reveleer_system_at = now()
  , updated_at                      = NOW()
FROM
    _did_not_get_success_file sf
WHERE
      rc.external_chase_id ISNULL
  AND rc.yr = DATE_PART('year', NOW())
  AND rc.id = sf.clientchasekey;

------------------------------------------------------------------------------------------------------------------------
/* in reveleer but never sent? should be 0 */
------------------------------------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS _not_sent_in_reveleer;
    CREATE TEMP TABLE _not_sent_in_reveleer AS
    SELECT loc.*
    FROM
        analytics.junk.reveleer_life_of_chase_20241015 loc
    left join reveleer_chases rc on rc.id = loc.clientchasekey
    where rc.id ISNULL
    ;
SELECT * FROM _not_sent_in_reveleer ;

------------------------------------------------------------------------------------------------------------------------
/* sent but they don't have */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _sent_not_in_reveleer;
CREATE TEMP TABLE _sent_not_in_reveleer AS
SELECT *
FROM
    reveleer_chases rc
WHERE
      rc.yr = 2024
  AND EXISTS( SELECT
                  1
              FROM
                  reveleer_chase_file_details cfd
              WHERE
                    cfd.chase_id = rc.id
                AND cfd.yr = 2024
                AND cfd.reveleer_file_id IS NOT NULL )
and not exists(
    select 1 from junk.reveleer_life_of_chase_20241015 loc
             where loc.clientchasekey = rc.id
)
;
create table junk.reveleer_missing_chases_20241016 as
SELECT id chase_id, qm_patient_measure_ids, patient_id
FROM
    _sent_not_in_reveleer
;
SELECT
    rp.name
  , COUNT(*)
FROM
    _sent_not_in_reveleer ns
    JOIN reveleer_projects rp ON ns.reveleer_project_id = rp.id
GROUP BY
    1;

------------------------------------------------------------------------------------------------------------------------
/* staged to send to reveleer */
------------------------------------------------------------------------------------------------------------------------
-- delete FROM reveleer_chase_file_details WHERE reveleer_file_id ISNULL ;
-- delete FROM reveleer_compliance_file_details WHERE reveleer_file_id ISNULL ;
-- delete FROM reveleer_attribute_file_details WHERE reveleer_file_id ISNULL ;

SELECT
    rp.name, rp.reveleer_id, count(*)
FROM
    reveleer_chase_file_details rcfd
join reveleer_projects rp ON rcfd.reveleer_project_id = rp.id
WHERE
    rcfd.inserted_at::DATE = NOW()::DATE
GROUP BY 1,2
;

SELECT *
FROM
    analytics.oban.oban_jobs where queue = 'reveleer' order by id desc;

--
SELECT rc.qm_patient_measure_ids
    FROM
    reveleer_chase_file_details rcfd
    join reveleer_chases rc on rcfd.reveleer_chase_id = rc.id
WHERE
    rcfd.inserted_at::DATE = NOW()::DATE
and reveleer_chase_id = 1253822
;

SELECT
    health_plan
     , patient_id
     , measure_id
  , inserted_at
, reveleer_project_id
FROM
    reveleer_chase_file_details
WHERE
    reveleer_chase_id = 1253822 and yr = 2024;
SELECT *
FROM
    junk.reveleer_life_of_chase_20241015 WHERE clientchasekey = 1253822 ;

SELECT *
FROM
    _sent_not_in_reveleer where 65215 = any(qm_patient_measure_ids);



SELECT
    rp.name
  , rp.reveleer_id
  , rc.patient_id
  , rc.measure_code
  , rc.external_chase_id
  , rc.qm_patient_measure_ids
FROM
    reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
WHERE
    rc.id IN (1253822, 309631)

;

------------------------------------------------------------------------------------------------------------------------
/*  Duped across projects */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _tall_chases;
CREATE TEMP TABLE _tall_chases AS
SELECT
    rc.id
  , rc.reveleer_project_id
  , UNNEST(qm_patient_measure_ids) pqm_id
FROM
    reveleer_chases rc
WHERE
    yr = 2024;
 create index on _tall_chases(pqm_id);

DROP TABLE IF EXISTS _dupes;
CREATE TEMP TABLE _dupes AS 
SELECT
    t1.id                  t1_id
  , t1.reveleer_project_id t1_
  , t1.pqm_id              t1_pqm_id
  , t2.id                  t2_id
  , t2.reveleer_project_id t2_reveleer_project_id
  , t2.pqm_id              t2_pqm_id
FROM
    _tall_chases t1
    JOIN _tall_chases t2 ON t1.pqm_id = t2.pqm_id AND t1.id < t2.id
;

SELECT *
FROM
    _dupes;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    junk.life_of_a_chase;
CREATE unique index on junk.life_of_a_chase(msh_chase_id);

DROP TABLE IF EXISTS junk.coop_potential_reveleer_measures;
CREATE TABLE junk.coop_potential_reveleer_measures AS
SELECT DISTINCT
    sp.patient_id
  , NULL::BIGINT                             current_chase_id
  , NULL::BIGINT[]                           chase_ids
  , sp.patient_mbi
  , pay.name                                 payer_name
  , ptr.id                                   reveleer_project_id
  , CASE
        WHEN m.code = 'HBD'                          THEN 'A1C9'
        WHEN m.code IN ('MRP', 'NIA', 'PEID', 'RDI') THEN 'TRC'
        ELSE m.code
        END                                  measure_code
  , pqm.measure_key
  , pqm.id                                   patient_quality_measure_id
  , pqm.operational_year
  , pqm.measure_source_key
  , pqm.must_close_by_date
  , mpm.subscriber_id
  , pqm.measure_status_key                   coop_measure_status
  , pqm.measure_status_key = 'closed_system' is_closed_system
  , pqm.inserted_at                          measure_created_at
  , pqm.inserted_at >= '2024-10-17'::date    measure_since_last_load
  , FALSE                                    sent_to_reveleer
  , FALSE                                    received_back_from_reveleer
FROM
    fdw_member_doc.qm_patient_measures pqm
    JOIN fdw_member_doc.qm_mco_patient_measures mpm ON pqm.mco_patient_measure_id = mpm.id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--         JOIN public.reveleer_projects ptr ON mpm.payer_id = ptr.payer_id
    JOIN ( SELECT id, UNNEST(measures_to_send) measures_to_send, payer_id FROM public.reveleer_projects ) ptr
         ON mpm.payer_id = ptr.payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = mpm.payer_id
    JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
    JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
WHERE
      pqm.operational_year = 2024
  AND pqm.measure_source_key = 'mco'
  AND pqm.is_active
  AND sp.is_quality_measures
      -- need to include closed system for compliance file
  AND (st.send_to_reveleer OR pqm.measure_status_key = 'closed_system')
  AND pqm.measure_key = ptr.measures_to_send
    ;

CREATE unique INDEX ON junk.coop_potential_reveleer_measures(patient_quality_measure_id);
CREATE INDEX ON junk.coop_potential_reveleer_measures(patient_id);
CREATE UNIQUE INDEX ON junk.coop_potential_reveleer_measures(patient_id, measure_key, must_close_by_date, operational_year, reveleer_project_id);


WITH
    chase_ids AS ( SELECT
                       m.patient_quality_measure_id
                     , ARRAY_AGG(DISTINCT rc.id) chase_ids
                   FROM
                       reveleer_chases rc
                       JOIN junk.coop_potential_reveleer_measures m ON
                           m.patient_quality_measure_id = ANY (rc.qm_patient_measure_ids)
                   GROUP BY m.patient_quality_measure_id )
UPDATE junk.coop_potential_reveleer_measures m
SET
    chase_ids = c.chase_ids
FROM
    chase_ids c
WHERE
    m.patient_quality_measure_id = c.patient_quality_measure_id
;


UPDATE
    junk.coop_potential_reveleer_measures j
SET
    sent_to_reveleer = TRUE
WHERE
    EXISTS( SELECT 1
            FROM reveleer_chase_file_details cfd
            WHERE cfd.chase_id = any(j.chase_ids))
;

UPDATE
    junk.coop_potential_reveleer_measures j
SET
    received_back_from_reveleer = TRUE
WHERE
    EXISTS( SELECT 1
            FROM junk.reveleer_chases_20241016 r
            WHERE r.msh_chase_id = any(j.chase_ids) )
;




;
SELECT
    j.patient_id
  , j.current_chase_id
  , j.chase_ids
  , j.patient_mbi
  , j.payer_name
  , rp.name                                        project_name
  , rp.reveleer_id                                 reveleer_project_id
--   , j.reveleer_project_id
  , j.measure_code
  , j.measure_key
  , j.patient_quality_measure_id
  , j.operational_year
  , j.measure_source_key
  , j.must_close_by_date
  , j.subscriber_id
  , j.coop_measure_status
  , j.is_closed_system
  , j.measure_created_at
  , j.sent_to_reveleer
  , j.received_back_from_reveleer
  , j.coop_measure_status = 'pending_results' is_excluded_status
FROM
    junk.coop_potential_reveleer_measures j
    JOIN reveleer_projects rp ON j.reveleer_project_id = rp.id
WHERE
      NOT sent_to_reveleer
  AND NOT is_closed_system
  AND coop_measure_status != 'pending_results'
--   and not is_excluded_status
and not measure_since_last_load
;



SELECT * FROM reveleer_chases WHERE id = 413768;
SELECT * FROM reveleer_compliance_file_details WHERE reveleer_chase_id = 413768;
SELECT *
FROM
    reveleer_chase_file_details
WHERE
    reveleer_chase_id = 413768
and reveleer_file_id is not null;

SELECT *
FROM
    junk.inactive_in_reveleer_202415
WHERE
    client_chase_key = 413768;

select * from junk.life_of_a_chase  where msh_chase_id = 413768;

SELECT *
FROM
    fdw_member_doc.qm_patient_measures where id = 1059660 ;


;
– For each gap:
– How many total 2024 gaps
– how many sent to Reveleer
– How many were not sent to Reveleer
– Why weren’t they sent to Reveleer
– Breakdown by plan
– Breakdown by status in co-op
– Not including proxies and MSSP projects

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    ( SELECT
          UNNEST(qm_patient_measure_ids) pqm_id
      , *
      FROM
          reveleer_chases ) x
;
DROP TABLE IF EXISTS _coop_measures;
CREATE TEMP TABLE _coop_measures AS
SELECT id, is_active, measure_status_key, inserted_at
FROM
    fdw_member_doc.qm_patient_measures
WHERE
    id IN
    (330611, 791235, 334041, 663968, 1272760, 655987, 661205, 1272653, 663501, 656466, 659791, 654628, 657427, 657638,
     659792, 1250986, 334064, 1104404, 653702, 327872, 886303, 812347, 749667, 1272761, 660705, 1272694, 1272651,
     1272757, 317333, 854582, 854556, 854445, 1272645, 854587, 854566, 1272664, 854503, 1272635, 854671, 1272715,
     1272626, 854673, 854452, 1272640, 1272696, 1272697, 854516, 854567, 854513, 854601, 1272706, 854407, 854554,
     854641, 1272619, 854669, 1272657, 854668, 854604, 1272674, 854594, 1272693, 1272726, 1272766, 791495, 1005234,
     1272730, 335593, 332463, 1005232, 1272692, 319758, 1272622, 1005227, 322882, 319863, 1005223, 1005222, 1005221,
     1250973, 1005218, 1005217, 1272649, 1343181, 1272661, 1272743, 1005205, 1005202, 1178458, 1272703, 1005199,
     1005196, 322973, 906969, 854483, 1036924, 659933, 659688, 659689, 1348522, 834388, 848736, 655217, 333083, 791643,
     793241, 337547, 848414, 1272643, 910982, 1391044, 1390899, 1391048, 1391064, 574070, 1272652, 1355232, 1391032,
     43821, 1355233, 334697, 1035633, 850459, 1272680, 1365281, 894903, 1209318, 1391047, 1259969, 1391042, 336806,
     834345, 668326, 316107, 10610, 1343221, 1272634, 331087, 1023391, 1272723, 848442, 854636, 835433, 1272618,
     1343223, 1272713, 854514, 854480, 854547, 1343196, 854663, 1272725, 1272655, 854666, 1272751, 1343160, 1272770,
     659135, 1343159, 834406, 1272662, 335521, 1343182, 1343226, 1343144, 828665, 1343199, 1343208, 1343143, 1250969,
     1343198, 1272721, 1348514, 658032, 792029, 828724, 1272737, 1251016, 848564, 848773, 791277, 792927, 848431,
     849056, 849057, 848432, 848856, 321860, 317014, 848857, 848433, 848629, 848858, 331514, 848630, 848859, 848434,
     848632, 337872, 331515, 334700, 849059, 848633, 328259, 848435, 849060, 848635, 317015, 325022, 848636, 849061,
     848864, 792446, 1036552, 1272631, 667971, 1272675, 317573, 1036969, 1005239, 1005236, 793119, 792066, 848750,
     1005231, 1005197, 1250965, 329518, 834316, 1348520, 848631, 889353, 1343197, 1005198, 43061, 1251079, 1272644,
     1272633, 1272712, 1104120, 848550, 321865, 819814, 1251009, 328411, 1272722, 854499, 848909, 849108, 1245381,
     1272736, 1272642, 792755, 849132, 1005208, 1343192, 1272753, 848539, 963218, 849058, 10741, 11421, 1037027, 848430,
     848627, 325010, 317012, 848628, 320723, 848861, 337875, 848863, 325016, 792444, 791920, 973751, 1005224, 1005219,
     1005210, 1005200, 1272659, 1005193, 1395220, 1395219, 1395218, 1395217, 1395216, 1395215, 1391068, 1391037,
     1391063, 1391052, 330652, 1272705, 1251075, 1391057, 1272623, 1390892, 1343222, 1175535, 724463, 1036557, 1251017,
     1391065, 1391059, 731330, 573268, 793336, 1391043, 1023295, 792972, 1023366, 1391040, 849177, 1332672, 1272669,
     1005243, 792517, 1272746, 854478, 854471, 854610, 1272663, 1272714, 854577, 1272732, 1272636, 854626, 1272750,
     1295431, 1272654, 1365253, 1343191, 1295430, 1251047, 1272733, 1272724, 1272738, 1272739, 1272763, 1272671,
     1272716, 577471, 1251050, 1272672, 1344962, 854523, 1272685, 854640, 854506, 854481, 1272717, 1272673, 1272686,
     1344961, 1272620, 1272719, 1272700, 1295432, 1272765, 1272689, 1272741, 1343188, 1272720, 1272701, 1272647, 948923,
     854595, 1272650, 1272678, 854487, 1343227, 854558, 910570, 1272745, 1272744, 1395169, 575883, 335588, 1272691,
     1005230, 1005229, 332579, 329320, 849130, 1272677, 1272754, 1272629, 1005215, 1250971, 1272660, 1272641, 1005195,
     1366491, 319946, 1391049, 330258, 829434, 828729, 333510, 1180947, 791581, 854464, 1272767, 1070919, 1272648,
     1348510, 1348511, 1348503, 323497, 326678, 792200, 916216, 791644, 791645, 1272731, 1268053, 1272646, 1272638,
     1272690, 1007375, 332546, 1272727, 1365264, 822444, 334545, 316014, 11187, 318792, 325012, 325080, 854458, 848679,
     333406, 1365340, 329035, 323632, 333301, 333300, 1209298, 732195, 323836, 1391036, 316132, 849037, 1272670,
     1272711, 1125282, 828504, 724847, 724649, 1276428, 1390830, 337871, 848860, 331524, 1272632, 334893, 849094,
     965785, 854649, 854423, 1272762, 854461, 854644, 1251045, 1272681, 854526, 1272667, 1272684, 1272656, 854603,
     1334132, 1272688, 854408, 1259963, 854439, 948890, 653525, 1272676, 1105307, 793080, 1272630, 957135, 317664,
     1272734, 1005237, 835332, 1209243, 915697, 1391061, 1257380, 1343167, 1257385, 1250977, 1272756, 822463, 1343202,
     1272621, 849133, 1365226, 1272704, 1250966, 1272735, 1005194, 854573, 667622, 1348521, 1348523, 834137, 659707,
     732208, 1272695, 1272729, 1250968
        );
CREATE unique INDEX on _coop_measures(id);

DROP TABLE IF EXISTS _compare;
CREATE TEMP TABLE _compare AS
SELECT
    cm.id
  , cm.is_active
  , cm.measure_status_key
  , cm.inserted_at
  , rc.id                chase_id
  , rc.external_chase_id
  , MIN(cfd.inserted_at) first_sent_at
  , MAX(cfd.inserted_at) last_sent_at
FROM
    _coop_measures cm
    LEFT JOIN reveleer_chases rc ON cm.id = ANY (rc.qm_patient_measure_ids)
    LEFT JOIN reveleer_chase_file_details cfd ON cfd.reveleer_chase_id = rc.id AND cfd.reveleer_file_id IS NOT NULL
GROUP BY
    cm.id, cm.is_active, cm.measure_status_key, cm.inserted_at, rc.external_chase_id, rc.id
;


SELECT *
FROM
    _compare
-- where chase_id = 129862
where chase_id IS NULL
;

DROP TABLE IF EXISTS _patient_measures;
CREATE TEMP TABLE _patient_measures AS
    ;
SELECT DISTINCT
    sp.patient_id
  , NULL::BIGINT                             chase_id
  , sp.patient_mbi
  , pay.name                                 payer_name
  , ptr.id                                   reveleer_project_id
  , CASE
        WHEN m.code = 'HBD'                          THEN 'A1C9'
        WHEN m.code IN ('MRP', 'NIA', 'PEID', 'RDI') THEN 'TRC'
        ELSE m.code
        END                                  measure_code
  , pqm.measure_key
  , pqm.id                                   patient_quality_measure_id
  , pqm.operational_year
  , pqm.measure_source_key
  , pqm.must_close_by_date
  , mpm.subscriber_id
  , pqm.measure_status_key
  , pqm.measure_status_key = 'closed_system' is_closed_system
FROM
    fdw_member_doc.qm_patient_measures pqm
    JOIN fdw_member_doc.qm_mco_patient_measures mpm ON pqm.mco_patient_measure_id = mpm.id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--         JOIN public.reveleer_projects ptr ON mpm.payer_id = ptr.payer_id
    left JOIN ( SELECT id, UNNEST(measures_to_send) measures_to_send, payer_id FROM public.reveleer_projects ) ptr
         ON mpm.payer_id = ptr.payer_id
        and pqm.measure_key = ptr.measures_to_send
    left JOIN fdw_member_doc.payers pay ON pay.id = mpm.payer_id
    left JOIN fdw_member_doc.qm_ref_measures m ON m.key = pqm.measure_key
    JOIN fdw_member_doc.qm_ref_patient_measure_statuses st ON st.key = pqm.measure_status_key
WHERE
    pqm.id IN (654628, 854610, 1272640, 1251017, 854594, 1332672, 1272664, 1366491, 1343208, 1343197, 1272704, 1272673, 854526, 1005197, 1272630, 854666, 1272706, 1295432, 1272653, 854407, 1005205, 854556, 1343167, 1272753, 1343191, 854636, 1272647, 1272765, 1272691, 1348521, 1272725, 1343188, 1005199, 854566, 854461, 1005221, 1272648, 1250969, 1272690, 854439, 1272722, 1295431, 1272623, 1295430, 854487, 1272675, 854408, 1272701, 1343143, 1005195, 1005200, 1272714, 1272650, 1272644, 1259963, 854640, 854671, 1272763, 1272618, 854582, 854626, 1272761, 1343196, 1272681, 1251075, 1005196, 659135, 1005236, 1272719, 854554, 1272663, 1272626, 1005231, 657427, 1005224, 1250966, 1272760, 1272746, 749667, 854480, 1348522, 854452, 854595, 1250965, 854458, 1272655, 1272721, 1251009, 1272733, 854478, 1005217, 656466, 1343198, 822444, 1272641, 1272712, 1272659, 1272734, 854573, 333300, 1272621, 1272651, 854423, 1272692, 1250971, 1272638, 1348520, 854499, 333301, 1005223, 1272667, 1272715, 1272738, 655217, 854649, 1272713, 1272723, 1005232, 1365281, 1272750, 1251050, 1272770, 854604, 1005210, 854513, 1250986, 1272672, 657638, 1272705, 1272660, 854601, 1272730, 854516, 655987, 854481, 1272751, 854669, 854523, 1365226, 1343199, 1272661, 1272745, 661205, 1272642, 1272669, 1005227, 1272756, 659689, 1272767, 1272688, 1272724, 1272700, 1272693, 1272654, 1005239, 1348510, 1251045, 1178458, 854483, 1209298, 1005198, 334545, 1343222, 1272696, 854663, 854503, 1005202, 1272629, 1272726, 1343182, 1005234, 1343144, 1272737, 1272649, 659688, 1272686, 1005222, 1343202, 1272677, 1272697, 1272635, 659791, 1272766, 1251047, 1005230, 1272684, 854558, 663501, 1272720, 1272632, 1005193, 854644, 854471, 1272716, 1007375, 1250968, 854603, 1348523, 1272754, 1272619, 1250977, 1272743, 1272656, 337547, 1272622, 1365264, 1272735, 1272729, 1272645, 1005219, 1251016, 323632, 329035, 1272662, 1272689, 854587, 1272694, 318792, 1005229, 1005215, 1272676, 1344961, 1272711, 1343160, 1272762, 1272671, 1344962, 1343221, 1272620, 1365340, 1272643, 1272674, 1343226, 1272727, 1334132, 854567, 1005194, 1272685, 316014, 1272634, 1272633, 1272652, 854514, 1348514, 1272695, 1272739, 1343223, 1272680, 854673, 854641, 1005218, 1272670, 854445, 1272646, 1005243, 1272717, 1272657, 854464, 1365253, 1250973, 1251079, 1272741, 663968, 1272636, 854547, 1272736, 854668, 1272732, 854506, 1348503, 1005208, 659933, 11187, 1272731, 1272678, 659792, 1272631, 1343192, 1272703, 854577, 653525, 1343159, 333406, 1272757, 1272744, 1343181, 1348511, 1005237)
--   AND pqm.measure_source_key = 'mco'
--   AND pqm.is_active
--   AND sp.is_quality_measures

--   AND (st.send_to_reveleer OR pqm.measure_status_key = 'closed_system')
   ;
SELECT
    rp.id
  , UNNEST(measures_to_send) measures_to_send
  , p.name
FROM
    public.reveleer_projects rp
join fdw_member_doc.payers p on p.id = rp.payer_id
;
SELECT *
FROM
    reveleer_projects where name ~* 'hcsc';

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT
    pm.*
FROM
    ( SELECT
          UNNEST(rc.qm_patient_measure_ids) pqm_id
      FROM
          junk.in_reveleer_but_should_be_inactive_20241018 j
          JOIN reveleer_chases rc ON j.msh_chase_id = rc.id ) x
    JOIN fdw_member_doc.qm_patient_measures pm ON pm.id = x.pqm_id
WHERE
    pm.is_active
;
SELECT *
FROM
    reveleer_chases where 10551 = any(qm_patient_measure_ids);

SELECT *
FROM
--     fdw_member_doc.qm_pm_activities WHERE patient_measure_id = 316212 order by id desc;
fdw_member_doc.qm_pm_activities WHERE patient_measure_id = 316215 order by id desc;


SELECT *
FROM
    junk.inactive_in_reveleer_202415 where client_chase_key = 31893;

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







