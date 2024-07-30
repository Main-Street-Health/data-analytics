CREATE OR REPLACE PROCEDURE sp_reveleer_stage_new_cca_pdfs_for_upload()
    LANGUAGE plpgsql
AS
$$
BEGIN
    DROP TABLE IF EXISTS _controls;
    CREATE TEMP TABLE _controls AS
    SELECT
        DATE_PART('year', NOW()) yr
      , _boy(NOW()::DATE)        boy;


    DROP TABLE IF EXISTS _measure_patients;
    CREATE TEMP TABLE _measure_patients AS
    SELECT
        rc.patient_id
      , rc.id reveleer_chase_id
      , rc.qm_patient_measure_ids
      , rc.yr
    FROM
        reveleer_chases rc
    join reveleer_projects rp ON rc.reveleer_project_id = rp.id and rp.is_active
    WHERE
        rc.yr = ( SELECT yr FROM _controls )
-- once reveleer does return files add logic like so
-- AND is_confirmed_in_reveleer_system
;


-- measure doc direct link
    DROP TABLE IF EXISTS _measure_docs;
    CREATE TEMP TABLE _measure_docs AS
    WITH
        measure_docs AS ( SELECT
                              document_id
                            , patient_id
                            , wf.patient_measure_id
                          FROM
                              fdw_member_doc.patient_colorectal_screening_results r
                              JOIN fdw_member_doc.qm_pm_colorectal_screening_wfs wf
                                   ON wf.patient_colorectal_screening_result_id = r.id
                          WHERE
                              document_id IS NOT NULL
                          UNION
                          SELECT
                              document_id
                            , patient_id
                            , wf.patient_measure_id
                          FROM
                              fdw_member_doc.patient_hba1cs r
                              JOIN fdw_member_doc.qm_pm_hbd_wfs wf ON wf.patient_hba1c_id = r.id
                          WHERE
                              document_id IS NOT NULL
                          UNION
                          SELECT
                              document_id
                            , patient_id
                            , wf.patient_measure_id
                          FROM
                              fdw_member_doc.patient_eye_exams r
                              JOIN fdw_member_doc.qm_pm_eye_exam_wfs wf ON wf.patient_eye_exam_id = r.id
                          WHERE
                              document_id IS NOT NULL
                          UNION
                          SELECT
                              document_id
                            , patient_id
                            , wf.patient_measure_id
                          FROM
                              fdw_member_doc.patient_osteoporosis_management_results r
                              JOIN fdw_member_doc.qm_pm_osteoporosis_management_wfs wf ON wf.patient_omw_id = r.id
                          WHERE
                              document_id IS NOT NULL
                          UNION
                          SELECT
                              document_id
                            , patient_id
                            , wf.patient_measure_id
                          FROM
                              fdw_member_doc.patient_breast_cancer_screenings r
                              JOIN fdw_member_doc.qm_pm_breast_cancer_screening_wfs wf
                                   ON wf.patient_breast_cancer_screening_id = r.id
                          WHERE
                              document_id IS NOT NULL )
    SELECT distinct on (md.document_id)
        md.patient_id
      , md.document_id
      , mp.reveleer_chase_id
      , NULL::DATE   visit_date
      , NULL::BIGINT visit_id
    FROM
        measure_docs md
        JOIN _measure_patients mp ON mp.patient_id = md.patient_id
            AND md.patient_measure_id = ANY (mp.qm_patient_measure_ids)
    order by md.document_id, mp.reveleer_chase_id
    ;

    CREATE UNIQUE INDEX ON _measure_docs(document_id);

-- cca doc link
    INSERT
    INTO
        _measure_docs (patient_id, document_id, visit_date, visit_id)
    SELECT DISTINCT
        d.patient_id
      , mcw.document_id
      , v.date
      , v.id
    FROM
        fdw_member_doc.documents d
        JOIN fdw_member_doc.msh_cca_worksheets mcw ON d.id = mcw.document_id
        JOIN fdw_member_doc.visits v ON mcw.visit_id = v.id
            AND DATE_PART('year', v.date) = ( SELECT yr FROM _controls )
    WHERE
          d.type_id IN ('cca_worksheet', 'gaps_worksheet')
      AND d.deleted_at ISNULL
      AND DATE_PART('year', COALESCE(v.date, d.inserted_at::DATE)) = ( SELECT yr FROM _controls )
      AND EXISTS( SELECT 1 FROM _measure_patients mp WHERE mp.patient_id = d.patient_id )
    ON CONFLICT DO NOTHING;

-- docs that may have indirect link to measure
    INSERT
    INTO
        _measure_docs (patient_id, document_id, reveleer_chase_id)
    WITH
        coop_docs AS ( SELECT
                           id      document_id
                         , patient_id
                         , d.type_id
                         , CASE
                               WHEN type_id IN ('col_fobt_results', 'col_fit_dna_results', 'col_colonoscopy_results')
                                                                  THEN 'COL'
                               WHEN type_id = 'hba1c_results'     THEN 'A1C9'
                               WHEN type_id = 'eye_exam_results'  THEN 'EED'
                               WHEN type_id = 'omw_results'       THEN 'OMW'
                               WHEN type_id = 'mammogram_results' THEN 'BCS'
                               END measure_code
                       FROM
                           fdw_member_doc.documents d
                       WHERE
                             DATE_PART('year', d.inserted_at) = ( SELECT yr FROM _controls )
                         AND d.deleted_at ISNULL
                         AND type_id IN (
                                         'col_fobt_results', 'col_fit_dna_results', 'col_colonoscopy_results',
                                         'hba1c_results', 'eye_exam_results', 'omw_results', 'mammogram_results',
                           -- need to include the non measure docs here in case they aren't linked to wksht
                                         'cca_worksheet', 'gaps_worksheet', 'ehr_clinical_data'
                         )
                         AND EXISTS( SELECT 1 FROM _measure_patients mp WHERE mp.patient_id = d.patient_id )
                       )
    SELECT
        cd.patient_id
      , cd.document_id
      , rc.id
    FROM
        coop_docs cd
        LEFT JOIN reveleer_chases rc ON rc.patient_id = cd.patient_id
            AND rc.measure_code = cd.measure_code AND
                                        rc.yr = ( SELECT yr FROM _controls )
    ON CONFLICT DO NOTHING
    ;


--     SELECT * FROM _measure_docs md
--     join fdw_member_doc.documents d on d.id = md.document_id
--
--     ;

    INSERT
    INTO
        reveleer_cca_pdfs (patient_id, reveleer_chase_id, visit_id, document_id, visit_date, yr, s3_bucket, s3_key)
    SELECT DISTINCT
        md.patient_id
      , md.reveleer_chase_id
      , md.visit_id
      , md.document_id
      , md.visit_date
      , ( SELECT yr FROM _controls ) yr
      , d.s3_bucket
      , d.s3_key
    FROM
        _measure_docs md
        JOIN fdw_member_doc.documents d ON md.document_id = d.id
            AND d.deleted_at ISNULL
            AND d.is_uploaded
    ON CONFLICT DO NOTHING
    ;


END;
$$;
SELECT document_id, count(*)
FROM
    _measure_docs group by 1 having count(*) > 1;

SELECT
    *
-- delete
FROM reveleer_cca_pdfs p
-- join fdw_member_doc.documents d on d.id = p.document_id
where p.inserted_at::date = now()::date
-- and d.inserted_at < '2024-01-01'::date
    ;


ALTER PROCEDURE sp_reveleer_stage_new_cca_pdfs_for_upload() OWNER TO postgres;


------------------------------------------------------------------------------------------------------------------------
/* old */
------------------------------------------------------------------------------------------------------------------------
-- CREATE PROCEDURE sp_reveleer_stage_new_cca_pdfs_for_upload()
--     LANGUAGE plpgsql
-- AS
-- $$
-- BEGIN
--
--     DROP TABLE IF EXISTS _measure_patients;
--     CREATE TEMP TABLE _measure_patients AS
--     SELECT DISTINCT
--         patient_id
--     FROM
--         reveleer_chase_file_details
--     WHERE
--         yr = 2023;
-- --     yr = DATE_PART('year', NOW());
--
--     -- switched to go off doc type regardless of assoc to wksht/visit 2023-12-04 discussed with Benn Huffman/MThack and reporting
--     INSERT
--     INTO
--         reveleer_cca_pdfs (patient_id, visit_id, document_id, visit_date, yr, s3_bucket, s3_key)
--     SELECT DISTINCT
--         d.patient_id
--       , v.id
--       , d.id
--       , v.date
--       , 2023 --DATE_PART('year', NOW())
--       , s3_bucket
--       , s3_key
--     FROM
--         fdw_member_doc.documents d
--         LEFT JOIN fdw_member_doc.msh_cca_worksheets mcw ON d.id = mcw.document_id
--         LEFT JOIN fdw_member_doc.visits v ON mcw.visit_id = v.id
--             AND DATE_PART('year', v.date) = 2023
-- --             AND DATE_PART('year', v.date) = DATE_PART('year', NOW())
--     WHERE
--           d.type_id IN ('cca_worksheet', 'gaps_worksheet', 'ehr_clinical_data')
--       AND d.deleted_at ISNULL
-- --       AND d.content_type = 'application/pdf'
--       AND d.content_type != 'application/pdf'
--       AND DATE_PART('year', COALESCE(v.date, d.inserted_at::DATE)) = 2023
-- --       AND DATE_PART('year', COALESCE(v.date, d.inserted_at::DATE)) = DATE_PART('year', NOW())
--       AND EXISTS( SELECT 1 FROM _measure_patients mp WHERE mp.patient_id = d.patient_id )
--     ON CONFLICT DO NOTHING;
--
--
-- END;
-- $$;
--
-- ALTER PROCEDURE sp_reveleer_stage_new_cca_pdfs_for_upload() OWNER TO postgres;
--
