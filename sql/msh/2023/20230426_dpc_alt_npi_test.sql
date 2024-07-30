DROP TABLE IF EXISTS _failed_mbis;
CREATE TEMP TABLE _failed_mbis AS
SELECT
    p.source_id patient_id
  , p.mbi
  , ARRAY_AGG(DISTINCT dp.npi) npis
FROM
    dpoc_bulk_export_job_errors je
    JOIN dpoc_bulk_export_jobs j ON je.dpoc_bulk_export_job_id = j.id
    JOIN dpoc_practitioners dp ON j.dpoc_practitioner_id = dp.id
    JOIN dpoc_patients p ON je.mbi = p.mbi OR je.mbi = ANY (p.mbis)
WHERE
    NOT EXISTS( SELECT 1 FROM dpoc_coverage c WHERE c.patient = p.bene_id )
GROUP BY
    1, 2
    ;
SELECT * FROM _failed_mbis

;
DROP TABLE IF EXISTS _alt_providers;
CREATE TEMP TABLE _alt_providers AS
SELECT DISTINCT ON (fm.patient_id)
    fm.patient_id
  , npi
  , msh_physician_id
FROM
    _failed_mbis fm
    left join fdw_member_doc_stage.patient_physician_location_hierarchy pplh on pplh.patient_id = fm.patient_id
    left JOIN fdw_member_doc.msh_physicians mp ON mp.id = pplh.msh_physician_id and mp.npi::text != any(fm.npis)
;

SELECT count(*), count(*) FILTER ( WHERE npi is not null ) FROM _alt_providers;
SELECT
    COUNT(DISTINCT ap.npi)
  , COUNT(DISTINCT ap.patient_id) FILTER ( WHERE dp.is_registered )
  , COUNT(DISTINCT ap.patient_id) FILTER ( WHERE dp.is_registered and sp.is_dpc)
  , COUNT(DISTINCT dp.npi) FILTER ( WHERE dp.is_registered )
FROM
    _alt_providers ap
    LEFT JOIN dpoc_practitioners dp ON dp.npi = ap.npi::TEXT
left join fdw_member_doc.supreme_pizza sp on sp.patient_id = ap.patient_id
;
-- 22,923 total failed mbi's
-- 5,685 new matches found in seans phys_hier
-- 546 out of 705 of the new matched providers are registered
-- 4,784 members to retry
-- 4,264 still valid to try


DROP TABLE IF EXISTS junk.dpc_new_npis_attempt_20230427;
CREATE TABLE junk.dpc_new_npis_attempt_20230427 AS
SELECT DISTINCT
    ap.patient_id
  , pats.mbi
  , ap.npi
FROM
    _alt_providers ap
    JOIN dpoc_practitioners dp ON dp.npi = ap.npi::TEXT AND dp.is_registered
    JOIN dpoc_patients pats ON pats.source_id = ap.patient_id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = ap.patient_id AND sp.is_dpc
;


/* practitioner to patient */
-- archive old
WITH
    archived AS (
        INSERT
            INTO
                dpoc_practitioner_group_patients_history(id, npi, mbi, is_registered, last_refreshed_at,
                                                         dpoc_provider_org_id, archived_at, inserted_at, updated_at)
                SELECT
                    id
                  , npi
                  , mbi
                  , is_registered
                  , last_refreshed_at
                  , dpoc_provider_org_id
                  , NOW() archived_at
                  , inserted_at
                  , updated_at
                FROM
                    dpoc_practitioner_group_patients gp
                WHERE
                    EXISTS(SELECT
                                   1
                               FROM
                                   junk.dpc_new_npis_attempt_20230427 r
                               WHERE
                                     r.npi::TEXT != gp.npi AND r.mbi = gp.mbi)
                RETURNING * )
DELETE
FROM
    dpoc_practitioner_group_patients pgp
WHERE
    EXISTS(SELECT 1 FROM archived a WHERE a.id = pgp.id)
;

-- create new attribution groups (npi <-> mbi)
INSERT
INTO
    dpoc_practitioner_group_patients (npi, mbi, inserted_at, updated_at, dpoc_provider_org_id)
SELECT
    npi
  , mbi
  , NOW() inserted_at
  , NOW() updated_at
  , 1     dpoc_org_id -- msh org = 1
FROM
    junk.dpc_new_npis_attempt_20230427 r
ON CONFLICT (npi, mbi) DO NOTHING ;
DROP TABLE IF EXISTS junk.dpc_new_distinct_npis_attempt_20230427;
create table junk.dpc_new_distinct_npis_attempt_20230427 as
SELECT distinct npi::text npi
FROM
    junk.dpc_new_npis_attempt_20230427;

SELECT *
FROM
    junk.dpc_new_distinct_npis_attempt_20230427;

SELECT
    state
  , COUNT(*)
FROM
    oban.oban_jobs
WHERE
      queue = 'dpoc_bulk_export_worker'
  AND state != 'completed'
GROUP BY
    1;



SELECT
    COUNT(DISTINCT pat.source_id)                                        nd
  , COUNT(DISTINCT pat.source_id) FILTER ( WHERE c.patient IS NOT NULL ) nd_worked
FROM
    junk.dpc_new_npis_attempt_20230427 j
    JOIN dpoc_patients pat ON pat.source_id = j.patient_id
    LEFT JOIN dpoc_coverage c ON c.patient = pat.bene_id
;



