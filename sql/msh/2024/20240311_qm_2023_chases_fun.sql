create table junk.chases_2023_reveleer_20240311 as
SELECT DISTINCT
    pqm.id pqm_id
FROM
    fdw_member_doc_stage.mco_patient_quality_measures sqm
    JOIN fdw_member_doc.patient_quality_measures pqm
         ON sqm.patient_id = pqm.patient_id AND sqm.measure_id = pqm.measure_id
WHERE
      sqm.inserted_at > '2024-03-10'
  AND payer_id = 44;

SELECT count(distinct pqm_id)
FROM junk.chases_2023_reveleer_20240311 j
where not exists(select 1 from reveleer_chase_file_details cfd where cfd.chase_id = j.pqm_id)
;

create table junk.chases_2023_SPD_reveleer_20240312 j
SELECT distinct chase_id
FROM
    reveleer_chase_file_details cfd
WHERE inserted_at ::date = now()::date
and measure_id = 'SPD'
;


SELECT distinct cfd.chase_id
FROM
    reveleer_chase_file_details cfd
-- join reveleer_files rf ON cfd.reveleer_file_id = rf.id
WHERE
    cfd.chase_id IN ( SELECT pqm_id FROM _pqms )
-- GROUP BY rf.id, type, file_name, s3_bucket, s3_key, push_job_id, rf.inserted_at, rf.updated_at, rf.state_payer_id
;

SELECT *
FROM
    reveleer_projects WHERE state_payer_id = 31;
call sp_reveleer_data_stager(true);

SELECT pdf.s3_key, pdf.document_id, cfd.*
FROM
    reveleer_cca_pdfs pdf
join reveleer_chase_file_details cfd on cfd.patient_id = pdf.patient_id and cfd.id
WHERE
    document_id IN
    (154392, 168948, 272592, 471710, 75524, 137864, 127282, 154736, 208858, 135305, 264379, 129067, 125353, 136846,
     177511, 136954, 76460, 262428, 74266, 235196, 182118, 160940, 81935, 158575, 147442, 229919, 138846, 177365,
     262815, 73418, 186995, 168944, 245767, 202753, 264461, 311700, 382078); --     s3_key ~* '117693_154392_2023-03-23.pdf' --     inserted_at::date = now()::date ORDER BY id DESC;



DROP TABLE IF EXISTS _matt_imports;
CREATE TEMP TABLE _matt_imports AS
select distinct patient_id from junk.qm_chase_2023_reveleer_gap_031224;;

SELECT
    distinct mi.patient_id
FROM
    _matt_imports mi
    LEFT JOIN fdw_member_doc.md_portal_roster_patients rp ON rp.patient_id = mi.patient_id
where rp.patient_id ISNULL
;
------------------------------------------------------------------------------------------------------------------------
/* next round humana */
------------------------------------------------------------------------------------------------------------------------
drop table junk.qm_chase_2023_reveleer_gaps_v2_20240312;
CREATE TABLE junk.qm_chase_2023_reveleer_gaps_v2_20240312 AS
SELECT DISTINCT
    pqm.id pqm_id
--     pqm.*, qm.*
--     qm.measure_code,
--     count(distinct pqm.id)
--     pqm.patient_id, pqm.measure_id, pqm.impact_date, count(distinct pqm.id)
--     payer_id,
--     raw_measure_name,
--      count(distinct pqm.id) as pqm_created_count
--     , count(distinct pqm.patient_id) as pqm_created_count

FROM
    fdw_member_doc_stage.mco_patient_quality_measures sqm
    JOIN fdw_member_doc.patient_quality_measures pqm
         ON sqm.patient_id = pqm.patient_id
                AND sqm.measure_id = pqm.measure_id
                AND sqm.measure_year = pqm.year
        and pqm.year = 2023
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN fdw_member_doc.quality_measures qm ON pqm.measure_id = qm.id
WHERE
      sqm.inserted_at >= '2024-03-12'
  AND qm.measure_code != 'SUPD';


SELECT
    pqm.mco_source_state_payer_id
     , rp.name
    , COUNT(*) pqms
FROM
    junk.qm_chase_2023_reveleer_gaps_v2_20240312 j
    join fdw_member_doc.patient_quality_measures pqm on j.pqm_id = pqm.id
join reveleer_projects rp ON pqm.mco_source_state_payer_id = rp.state_payer_id
WHERE
    NOT EXISTS( SELECT 1 FROM reveleer_chase_file_details cfd WHERE cfd.chase_id = j.pqm_id and cfd.inserted_at < now() - '2 hours'::interval )
GROUP BY 1, 2
;
SELECT *
FROM
    reveleer_projects
WHERE
    state_payer_id IN (
                       31,
        342
--                        48, 282
        );


SELECT *
from reveleer_chase_file_details
WHERE
--     inserted_at >= NOW() - '2 hours'::interval
patient_id in (
    181169, 290807,290807,179964,78129,201659,650595,182739,291212,182338,180172,650689,51347,650613,159775,180274,92828,650640,666257,267726,180753
    )
and left(enrollee_id, 1) != '0'
-- and inserted_at  > now() - '5 hours'::Interval
;




-- ["bcbst", "humana_ky", "wellcare_ny"]
-- ["cigna_tn", "humana_ky"]
call sp_reveleer_stage_new_cca_pdfs_for_upload(false);

-- send to mdp
DROP TABLE IF EXISTS _needs_to_go_on_roster;
CREATE TEMP TABLE _needs_to_go_on_roster AS
SELECT *
FROM
    reveleer_chase_file_details cfd
where inserted_at >= now() - '1 week'::interval
and not exists(
    select 1
    from fdw_member_doc.md_portal_roster_patients rp
    where rp.patient_id = cfd.patient_id
)
;
SELECT distinct patient_id
FROM
    _needs_to_go_on_roster ;
create table junk.qm_chase_2023_reveleer_gaps_the_purge_recreated_20240313  as
SELECT
    id pqm_id
FROM
    fdw_member_doc.patient_quality_measures;
WHERE
    id IN (
           671517, 671518, 671519, 671520, 671521, 671522, 671523, 671524, 671525, 671526, 671527, 671528, 671529,
           671530, 671531, 671532, 671533, 671534, 671535, 671536, 671537, 671538, 671539, 671540, 671541, 671542,
           671543, 671544, 671545, 671546, 671547, 671548, 671549, 671550, 671551, 671552, 671553, 671554, 671555,
           671556, 671557, 671558, 671559, 671560, 671561, 671562, 671563, 671564, 671565, 671566, 671567, 671568,
           671569, 671570, 671571, 671572, 671573, 671574, 671575, 671576, 671577, 671578, 671579, 671580, 671581,
           671582, 671583, 671584, 671585, 671586, 671587, 671588, 671589, 671590, 671591, 671592, 671593, 671594,
           671595, 671596, 671597, 671598, 671599, 671600, 671601, 671602, 671603, 671604, 671605, 671606, 671607,
           671608, 671609, 671610, 671611, 671612, 671613, 671614, 671615, 671616, 671617, 671618, 671619, 671620,
           671621, 671622, 671623, 671624, 671625, 671626, 671627, 671628, 671629, 671630, 671631, 671632, 671633,
           671634, 671635, 671636, 671637, 671638, 671639, 671640, 671641, 671642, 671643, 671644, 671645, 671646,
           671647, 671648, 671649, 671650, 671651, 671652, 671653, 671654, 671655, 671656, 671657, 671658, 671659,
           671660, 671661, 671662, 671663, 671664, 671665, 671666, 671667, 671668, 671669, 671670, 671671, 671672,
           671673, 671674, 671675, 671676, 671677, 671678, 671679, 671680, 671681, 671682, 671683, 671684, 671685,
           671686, 671687, 671688, 671689, 671690, 671691, 671692, 671693, 671694, 671695, 671696, 671697, 671698,
           671699, 671700, 671701, 671702, 671703, 671704, 671705, 671706, 671707, 671708, 671709, 671710, 671711,
           671712, 671713, 671714, 671715, 671716, 671717, 671718, 671719, 671720, 671721, 671722, 671723, 671724,
           671725, 671726, 671727, 671728, 671729, 671730, 671731, 671732, 671733, 671734, 671735, 671736, 671737,
           671738, 671739, 671740, 671741, 671742, 671743, 671744, 671745, 671746, 671747, 671748, 671749, 671750,
           671751, 671752, 671753, 671754, 671755, 671756, 671757, 671758, 671759, 671760, 671761, 671762, 671763,
           671764, 671765, 671766, 671767, 671768, 671769, 671770, 671771, 671772
        )
    ;




------------------------------------------------------------------------------------------------------------------------
/* 3/14 */
------------------------------------------------------------------------------------------------------------------------
drop table if exists _raw;
CREATE TEMPORARY TABLE _raw AS
    ( SELECT DISTINCT
          patient_id
        , mco_member_id
        , 'qm_chase_2023_reveleer_gap_031224' AS table_name
      FROM
          junk.qm_chase_2023_reveleer_gap_031224
      UNION ALL
      SELECT DISTINCT
          patient_id
        , mbr_id                              AS mco_member_id
        , 'qm_chase_2023_cigna_mrppcr_031224' AS table_name
      FROM
          junk.qm_chase_2023_cigna_mrppcr_031224
      UNION ALL
      -- select distinct patient_id, humana_patient_id as mco_member_id ,'qm_chase_humana_delta_031224' as table_name from junk.qm_chase_humana_delta_031224
-- union all
-- select distinct patient_id, humana_patient_id as mco_member_id ,'qm_chase_humana_part2_031124' as table_name from junk.qm_chase_humana_part2_031124
-- union all
      SELECT DISTINCT
          coop_patient_id                     AS patient_id
        , member_id                           AS mco_member_id
        , 'qm_chase_2023_humana_delta_031324' AS table_name
      FROM
          junk.qm_chase_2023_humana_delta_031324 );


select
    table_name,
    --qm.measure_key,
    count(distinct raw.patient_id) as n_matched_patients,
    count(distinct pqm.measure_id) as n_measure_types,
    count(distinct pqm.id)         as n_qms_impacted,
    count(distinct rev.chase_id)   as n_chases_created
from _raw raw
left join fdw_member_doc.patient_quality_measures pqm on raw.patient_id = pqm.patient_id
left join reveleer_chase_file_details rev on pqm.id = rev.chase_id
left join payer_ref.mco_quality_measure_mapping qm on pqm.measure_id = qm.measure_id
where (pqm.inserted_at > '2024-03-09' or pqm.updated_at > '2024-03-09')
  and pqm.measure_id != '18' --Filter out SUPD
group by 1
;
select
   qm.code, qm.measure_code, qm.name, pqm.*
from _raw raw
left join fdw_member_doc.patient_quality_measures pqm on raw.patient_id = pqm.patient_id
-- join fdw_member_doc.supreme_pizza sp on sp.patient_id = pqm.patient_id
where (pqm.inserted_at > '2024-03-09' or pqm.updated_at > '2024-03-09')
  and pqm.measure_id != '18' --Filter out SUPD
  and rev.chase_id is null
  ;
SELECT
    qm.code
  , qm.measure_code
  , qm.name
  , j.id
     , sp.patient_state_payer_id
     , sp.patient_payer_id
  , pqm.*
FROM
    _raw raw
    JOIN fdw_member_doc.patient_quality_measures pqm ON raw.patient_id = pqm.patient_id
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
--     left join junk.qm_chase_2023_reveleer_gaps_the_purge_recreated_20240313  j on j.pqm_id = pqm.id
    left join junk.qm_chase_2023_reveleer_gaps_20240313   j on j.id = pqm.id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
    LEFT JOIN reveleer_chase_file_details rev ON pqm.patient_id = rev.patient_id AND pqm.id = rev.chase_id
WHERE
      (pqm.inserted_at > '2024-03-09' OR pqm.updated_at > '2024-03-09')
  AND pqm.measure_id != '18' --Filter out SUPD
  and pqm.measure_id != 30
  AND rev.chase_id IS NULL;

SELECT *
FROM
    fdw_member_doc.quality_measures qm WHERE name ~* 'colo';

SELECT DISTINCT
    cfd.patient_id
  , cfd.member_fname
  , cfd.member_lname
  , cfd.member_dob
  , cfd.measure_id
  , rp.name
  , rp.reveleer_id project_id
FROM
    reveleer_chase_file_details cfd
    JOIN reveleer_projects rp ON rp.state_payer_id = cfd.state_payer_id;

-- join reveleer_files rf ON cfd.reveleer_file_id = rf.id;

+---------------------+------------------------------------+-------------------+---------------+---------------+----------------+
|nd_raw_mco_member_ids|count_n_mco_open_measures_matched_pt|nd_matched_coop_pts|n_measure_types|n_pqms_impacted|n_chases_created|
+---------------------+------------------------------------+-------------------+---------------+---------------+----------------+
|1781                 |1831                                |1751               |13             |2915           |2164            |
+---------------------+------------------------------------+-------------------+---------------+---------------+----------------+

SELECT
    ( SELECT COUNT(DISTINCT member_id) FROM junk.humana_ky_2023_gap_master )                               AS nd_raw_mco_member_ids
  , ( SELECT
          SUM(
                  (CASE WHEN has_open_bcs THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_cbp THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_coa_fsa THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_coa_mdr THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_coa_pns THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_col THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_col_45_50 THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_eed THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_fmc THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_hbd THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_omw_plus_1 THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_omw THEN 1 ELSE 0 END) +
                      --(CASE WHEN has_open_spc THEN 1 ELSE 0 END) +
                  (CASE WHEN has_open_trc_mrp THEN 1 ELSE 0 END)
          )
      FROM
          junk.humana_ky_2023_gap_master
      WHERE
          patient_id IS NOT NULL )                                                                            count_n_mco_open_measures_matched_pt
  , ( SELECT COUNT(DISTINCT patient_id)
      FROM junk.humana_ky_2023_gap_master
      WHERE
          patient_id IS NOT NULL )                                                                         AS nd_matched_coop_pts
  , COUNT(DISTINCT pqm.measure_id)                                                                         AS n_measure_types
  , COUNT(DISTINCT pqm.id)                                                                                 AS n_pqms_impacted
  , COUNT(DISTINCT rev.chase_id)                                                                           AS n_chases_created
FROM
    junk.humana_ky_2023_gap_master AS raw
    JOIN fdw_member_doc.patient_quality_measures AS pqm ON raw.patient_id = pqm.patient_id
    LEFT JOIN reveleer_chase_file_details AS rev ON pqm.patient_id = rev.patient_id AND pqm.id = rev.chase_id
    LEFT JOIN payer_ref.mco_quality_measure_mapping AS qm ON pqm.measure_id = qm.measure_id
WHERE
      (pqm.inserted_at > '2024-03-09' OR pqm.updated_at > '2024-03-09')
  AND pqm.measure_id != '18'; -- Filter out SUPD

SELECT distinct qm.code, qm.name, pqm.*
FROM
    junk.humana_ky_2023_gap_master AS raw
    JOIN fdw_member_doc.patient_quality_measures AS pqm ON raw.patient_id = pqm.patient_id
        join fdw_member_doc.quality_measures qm on qm.id = pqm.measure_id
    LEFT JOIN reveleer_chase_file_details AS rev ON  pqm.id = rev.chase_id

--     LEFT JOIN payer_ref.mco_quality_measure_mapping AS qm ON pqm.measure_id = qm.measure_id
WHERE
      (pqm.inserted_at > '2024-03-09' OR pqm.updated_at > '2024-03-09')
  AND pqm.measure_id != '18'
and rev.id ISNULL
and pqm.mco_source_state_payer_id ISNULL
; -- Filter out SUPD
create table junk.qm_chase_2023_reveleer_gaps_20240314 as
select
    distinct pqm.id pqm_id
FROM
    junk.humana_ky_2023_gap_master AS raw
JOIN fdw_member_doc.patient_quality_measures pqm ON raw.patient_id = pqm.patient_id
    left JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
    left JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    left JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    left JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
    LEFT JOIN reveleer_chase_file_details rev ON pqm.patient_id = rev.patient_id AND pqm.id = rev.chase_id
WHERE
      (pqm.inserted_at > '2024-03-09' OR pqm.updated_at > '2024-03-09')
  AND pqm.measure_id != '18' --Filter out SUPD
  AND rev.chase_id IS NULL

    ;

DROP TABLE IF EXISTS _needs_to_go_on_roster;
CREATE TEMP TABLE _needs_to_go_on_roster AS

SELECT distinct patient_id
FROM
    reveleer_chase_file_details cfd
where inserted_at >= now() - '1 week'::interval
and not exists(
    select 1
    from fdw_member_doc.md_portal_roster_patients rp
    where rp.patient_id = cfd.patient_id
)
;
SELECT *
FROM
    _needs_to_go_on_roster;

create table junk.qm_chase_2023_reveleer_gaps_v2_20240314 as
WITH
    missing AS ( SELECT *
                      , UNNEST(pqm_ids) pqm_id
                 FROM
                     junk.qm_chase_reveleer_check_20240314 x
                     LEFT JOIN reveleer_chase_file_details ch ON ch.chase_id = ANY (x.pqm_ids)
                 WHERE
                     ch.chase_id IS NULL )
SELECT distinct pqm.id pqm_id
FROM
    missing m
    JOIN fdw_member_doc.patient_quality_measures pqm ON pqm.id = m.pqm_id AND pqm.measure_id != '18' --Filter out SUPD
    ;

SELECT
    qm.code
  , pqm.*
FROM
    junk.qm_chase_2023_reveleer_gaps_v2_20240314 j
    LEFT JOIN reveleer_chase_file_details cfd ON cfd.chase_id = j.pqm_id
    JOIN fdw_member_doc.patient_quality_measures pqm ON pqm.id = j.pqm_id
    JOIN reveleer_projects rp ON rp.state_payer_id = pqm.mco_source_state_payer_id
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
WHERE
    cfd.id ISNULL
; --122

SELECT
--     count(*)
    rp.name, count(*)
--     qm.code
--   , pqm.*
-- distinct pqm.patient_id

FROM

        fdw_member_doc.patient_quality_measures pqm
        left JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
        join junk.qm_chase_2023_reveleer_gaps_v2_20240314  j on j.pqm_id = pqm.id
        left JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
            AND qmc.measure_id = pqm.measure_id
            AND qmc.measure_year = pqm.year
        JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
        left JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
        JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
        JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
        LEFT JOIN reveleer_chase_file_details cfd ON cfd.chase_id = j.pqm_id
join reveleer_projects rp on rp.state_payer_id = pqm.mco_source_state_payer_id
where cfd.id ISNULL
-- and sp.patient_id ISNULL
GROUP BY 1

-- DROP TABLE IF EXISTS _files_to_purge;
-- CREATE TEMP TABLE _files_to_purge AS
-- SELECT *
-- FROM
--     reveleer_files WHERE inserted_at > now() - '20 minutes'::interval;
-- delete from reveleer_chase_file_details cfd where reveleer_file_id in (select id from _files_to_purge);
-- delete from reveleer_attribute_file_details cfd where reveleer_file_id in (select id from _files_to_purge);
-- delete from reveleer_compliance_file_details cfd where reveleer_file_id in (select id from _files_to_purge);
-- delete from reveleer_files rf where rf.id in (select id from _files_to_purge);

drop table if exists junk.qm_chase_2023_reveleer_gaps_20240315;
create table junk.qm_chase_2023_reveleer_gaps_20240315 as
SELECT distinct pqm.id pqm_id
FROM
    junk.cigna_2023_gap_master AS raw
    JOIN fdw_member_doc.patient_quality_measures AS pqm
         ON raw.patient_id = pqm.patient_id AND raw.measure_id = pqm.measure_id AND pqm.year = 2023
join fdw_member_doc.supreme_pizza sp on sp.patient_id = pqm.patient_id
join fdw_member_doc.quality_measures qm on qm.id = pqm.measure_id
left join reveleer_chase_file_details as rev on pqm.patient_id = rev.patient_id and pqm.id = rev.chase_id
WHERE
      (pqm.inserted_at > '2024-03-14' OR pqm.updated_at > '2024-03-14')
  AND raw.raw_measure NOT IN ('KED', 'TRCPEI', 'SPC', 'PCR') --Filtering out KED, TRC PED, PCR, and SPC
  AND pqm.measure_id != '18'
and rev.chase_id ISNULL
;

insert into junk.qm_chase_2023_reveleer_gaps_20240315(pqm_id)
select distinct pqm_id from junk.qm_chase_2023_reveleer_gaps_v2_20240314 j
where not exists(select 1 from junk.qm_chase_2023_reveleer_gaps_20240315 new where new.pqm_id = j.pqm_id)
-- Filter out SUPD
    ;
SELECT
   ptr.name, count(DISTINCT pqm.id) nd_pqms
FROM
    fdw_member_doc.patient_quality_measures pqm
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
        join junk.qm_chase_2023_reveleer_gaps_20240315  j on j.pqm_id = pqm.id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
        -- don't need a cca for this pull - all left joins below
    LEFT JOIN fdw_member_doc.visits v ON v.patient_id = sp.patient_id
        AND v.date >= '2023-01-01'
        AND v.type_id = 'cca_recon'
    LEFT JOIN fdw_member_doc.msh_cca_worksheets w ON v.id = w.visit_id
        AND w.invalid_reason ISNULL
    LEFT JOIN fdw_member_doc.msh_cca_worksheet_patient_quality_measures mcwpqm ON w.id = mcwpqm.msh_cca_worksheet_id
        AND mcwpqm.deleted_at ISNULL
        AND mcwpqm.patient_quality_measure_id = pqm.id
    LEFT JOIN fdw_member_doc.patient_qm_assessment_answers paa
              ON w.id = paa.msh_cca_worksheet_id AND mcwpqm.patient_qm_assessment_id = paa.patient_qm_assessment_id
    LEFT JOIN fdw_member_doc.msh_quality_measure_assessment_answers qmaa
              ON qmaa.id = paa.qm_assessment_answer_id
    LEFT JOIN fdw_member_doc.msh_quality_measure_assessments qma ON qmaa.msh_quality_measure_assessment_id = qma.id
-- left join junk.qm_chase_2023_reveleer_gaps_20240315 j2 on j2.pqm_id = pqm.id
WHERE
      pqm.year = 2023
--   AND (pqm.inserted_at > '2024-03-14' OR pqm.updated_at > '2024-03-14')
  AND pqm.measure_id != '18'
GROUP BY 1
-- and j2.pqm_id isnull
;
SELECT * FROM fdw_member_doc.supreme_pizza sp WHERE sp.patient_id = 59104;
;
SELECT qm.code, pqm.*
FROM
    junk.qm_chase_2023_reveleer_gaps_20240315  j
        join fdw_member_doc.patient_quality_measures pqm on pqm.id = j.pqm_id
        join fdw_member_doc.quality_measures qm on qm.id = pqm.measure_id
    left join reveleer_chase_file_details cfd on j.pqm_id = cfd.chase_id
where cfd.id ISNULL
-- where cfd.inserted_at ::date = now()::date
-- GROUP BY 1
