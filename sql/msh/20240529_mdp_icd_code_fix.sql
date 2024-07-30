
DROP TABLE IF EXISTS junk.md_portal_suspect_icd_code_mismatches;
CREATE TABLE junk.md_portal_suspect_icd_code_mismatches AS
SELECT
    h.id
  , h.golgi_patient_id patient_id
  , h.icd_10_code
  , i_wrong.code_formatted wrong_icd
  , i_wrong.id             wrong_icd_id
  , i_right.code_formatted right_icd
  , i_right.id             right_icd_id
  , h.source_date
FROM
    stage.msh_md_portal_suspects_history h
    JOIN icd10s i_wrong ON i_wrong.id = h.icd10_id
        AND h.icd_10_code != i_wrong.code_formatted
    JOIN icd10s i_right ON i_right.code_formatted = h.icd_10_code;

CREATE INDEX ON junk.md_portal_suspect_icd_code_mismatches(patient_id);


SELECT
    COUNT(*)
  , COUNT(DISTINCT (patient_id, icd_10_code))
FROM
    junk.md_portal_suspect_icd_code_mismatches;

-- 96, not in pizza, 1196 not listed as md_portal. can maybe ignore these, might be an issue if med adh is turned back on
SELECT
    COUNT(DISTINCT j.patient_id)
FROM
    junk.md_portal_suspect_icd_code_mismatches j
    LEFT JOIN supreme_pizza sp ON sp.patient_id = j.patient_id AND sp.is_md_portal_full
WHERE
    sp.patient_id ISNULL
;

SELECT *
FROM
    junk.md_portal_suspect_icd_code_mismatches j
    LEFT JOIN msh_external_emr_diagnoses xdx ON xdx.patient_id = j.patient_id
        AND xdx.icd10_id = j.wrong_icd_id
WHERE
    xdx.id ISNULL
;



------------------------------------------------------------------------------------------------------------------------
/* Plan:
   0. fix icd mappings
   1. determine which ones can be soft deleted
   2. soft delete those, keep record of what was deleted
   3. move all affected patients from history to stage table
   4. manually reprocess stage table

*/
------------------------------------------------------------------------------------------------------------------------
begin;
update stage.msh_md_portal_suspects_history h
set icd10_id = j.right_icd_id
FROM
   junk.md_portal_suspect_icd_code_mismatches j
where j.id = h.id
;
end;

drop table if exists _to_delete;
CREATE TEMPORARY TABLE _to_delete AS ( SELECT DISTINCT
                                           xdxd.id
                                       FROM
                                           public.msh_external_emr_diagnoses xdxd
                                           JOIN junk.md_portal_suspect_icd_code_mismatches j
                                                ON xdxd.patient_id = j.patient_id AND xdxd.icd10_id = j.wrong_icd_id
                                       WHERE
                                             NOT EXISTS ( SELECT
                                                              1
                                                          FROM
                                                              public.msh_cca_worksheet_dxs cca_dx
                                                          WHERE
                                                                xdxd.id = cca_dx.external_emr_diagnosis_id
                                                            AND cca_dx.code_status ~* 'yes' )
                                         AND xdxd.source = 'md_portal'
                                         AND xdxd.diagnosis_type = 'suspect' -- if a recapture comes in for what was a suspect, we'll flip to recapture and retain the source
                                         AND xdxd.cms_contract_year = DATE_PART('year', NOW())
                                         AND xdxd.is_deleted IS FALSE -- do not remove deleted ADH 2022-11-19
);

CREATE INDEX idx_to_delete ON _to_delete(id);

SELECT count(*) FROM _to_delete;

CREATE TABLE junk.md_portal_suspect_icd_code_mismatches_deleted_xdxd AS
SELECT
    xdxd.*
FROM
    _to_delete td
    JOIN public.msh_external_emr_diagnoses xdxd
         ON td.id = xdxd.id
;


UPDATE public.msh_external_emr_diagnoses xdxd
SET
    is_deleted = TRUE
WHERE
    EXISTS ( SELECT 1 FROM _to_delete td WHERE td.id = xdxd.id )
        ;

INSERT
INTO
    member_doc.stage.msh_md_portal_suspects (id, golgi_patient_id, icd_10_code, reason, source_fact_name,
                                             source_supportive_evidence, source_excerpt, source_date, source_author,
                                             source_description, source_location, compendium_url, hcc_category,
                                             icd10_id, old_icd10_id, source_evidence_type, source_document_type,
                                             source_document_type_loinc, source_document_title, source_encounter_type,
                                             source_encounter_organization, source_organization_name,
                                             source_author_software, md_portals_batch_id, source_text)
SELECT
    id
  , golgi_patient_id
  , icd_10_code
  , reason
  , source_fact_name
  , source_supportive_evidence
  , source_excerpt
  , source_date
  , source_author
  , source_description
  , source_location
  , compendium_url
  , hcc_category
  , icd10_id
  , old_icd10_id
  , source_evidence_type
  , source_document_type
  , source_document_type_loinc
  , source_document_title
  , source_encounter_type
  , source_encounter_organization
  , source_organization_name
  , source_author_software
  , md_portals_batch_id
  , source_text
FROM
    stage.msh_md_portal_suspects_history h
WHERE
    EXISTS( SELECT 1 FROM junk.md_portal_suspect_icd_code_mismatches j WHERE h.golgi_patient_id = j.patient_id );

SELECT count(*)
FROM
    member_doc.stage.msh_md_portal_suspects;

-- call stage._process_md_portals_proc();
SELECT i.code_formatted, *
FROM
    stage.msh_md_portal_suspects_history h
join icd10s i on i.id = h.icd10_id
WHERE
      golgi_patient_id = 358830
--   AND icd_10_code = 'C81.41';
-- icd_10_code = 'C81.41';
;

SELECT *
FROM
    msh_external_emr_diagnoses xdx
    JOIN icd10s i ON xdx.icd10_id = i.id
WHERE
      patient_id = 358830
  AND i.code_formatted = 'C61';
-- AND i.code_formatted = 'C81.41';