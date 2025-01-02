CREATE PROCEDURE synchronize_hcc_module_from_xdx(IN _yr integer DEFAULT date_part('year'::text, now()))
    LANGUAGE plpgsql
AS
$$
BEGIN
    DROP TABLE IF EXISTS _ctrls;
    CREATE TEMP TABLE _ctrls AS select _yr as yr;
    -- CREATE TEMP TABLE _ctrls AS select 2024 as yr;

    -- build patient HCC-ICD level from xdx
    DROP TABLE IF EXISTS _xdx_patient_hcc_icds;
    CREATE TEMP TABLE _xdx_patient_hcc_icds AS
    SELECT DISTINCT
        xdx.patient_id
      , xdx.hcc_id
      , xdx.icd10_id
      , xdx.cms_contract_year
      , xdx.hcc_number
      , coalesce(xdx.suspect_reason, xdx.suspect_reason_machine)                          suspect_reason
      , i.code_formatted                                                                  icd_code_formatted
      , CASE WHEN xdx.diagnosis_type = 'rural' THEN 'suspect' ELSE xdx.diagnosis_type END capture_type
      , COALESCE(xdx.plan_capture_date, xdx.practice_capture_date)                        captured_date
      , COALESCE(xdx.plan_capture_date, xdx.practice_capture_date) IS NOT NULL            is_captured
      , case when xdx.practice_capture_date is not null then 'practice'
             when xdx.plan_capture_date is not null then 'plan' end                       captured_source
      , xdx.source
      , hi.id                                                                             hcc_icd10_id
      , ph.id                                                                             patient_hcc_id
      , ph.is_captured                                                                    existing_patient_hcc_is_captured
      , ph.capture_type                                                                   existing_patient_hcc_capture_type
      , bool_or(coalesce(wdx.code_status = 'checked_yes_on_worksheet', false))            we_captured
    FROM
        public.msh_external_emr_diagnoses xdx
        JOIN public.icd10s i on i.id = xdx.icd10_id
        JOIN public.hcc_icd10s hi ON xdx.hcc_id = hi.hcc_id
                                     AND xdx.icd10_id = hi.icd10_id
                                     AND xdx.cms_contract_year = hi.yr
        LEFT JOIN public.patient_hccs ph ON ph.patient_id = xdx.patient_id
                                            AND ph.hcc_id = xdx.hcc_id
                                            AND ph.yr = xdx.cms_contract_year
        LEFT JOIN public.msh_cca_worksheet_dxs wdx ON xdx.id = wdx.external_emr_diagnosis_id and not wdx.is_deleted
    WHERE
          -- could maybe add more filters on patient status here
        NOT xdx.is_deleted
        AND xdx.cms_contract_year = ( SELECT yr FROM _ctrls )
        AND xdx.hcc_id IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
    ;

    create index _tmp_idx_xdx_patient_hcc_icds_pat_hcc_icd on _xdx_patient_hcc_icds (patient_hcc_id, icd_code_formatted);

    -- Group xdx level up to patient hcc's
    DROP TABLE IF EXISTS _xdx_patient_hccs;
    CREATE TEMP TABLE _xdx_patient_hccs AS
    SELECT
        xdx.patient_id
      , xdx.hcc_id
      , xdx.hcc_number
      , xdx.cms_contract_year
      , patient_hcc_id
      , existing_patient_hcc_is_captured
      , existing_patient_hcc_capture_type
      , CASE WHEN patient_hcc_id ISNULL THEN 'ins' ELSE 'upd' END                             op
      , MIN(xdx.captured_date)                                                                captured_date
      , BOOL_OR(xdx.is_captured)                                                              is_captured
        -- recapture trumps suspect
      , CASE WHEN BOOL_OR(xdx.capture_type = 'recapture') THEN 'recapture' ELSE 'suspect' END capture_type
      , ARRAY_AGG(DISTINCT xdx.source)                                                        sources
      , false                                                                                 is_dominated
    FROM
        _xdx_patient_hcc_icds xdx
    WHERE
        xdx.patient_hcc_id ISNULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7;


    -- create new patient hccs from grouped up table
    WITH
        ins AS (
            INSERT INTO patient_hccs(patient_id, hcc_id, yr, capture_type, inserted_at, updated_at, is_captured,
                                     suspect_sources)
                SELECT DISTINCT
                    xph.patient_id
                  , xph.hcc_id
                  , xph.cms_contract_year
                  , xph.capture_type
                  , NOW() inserted_at
                  , NOW() updated_at
                  , xph.is_captured
                  , xph.sources
                FROM
                    _xdx_patient_hccs xph
                WHERE
                    xph.patient_hcc_id ISNULL
                RETURNING id, patient_id, hcc_id )
    -- update with new patient_hccs ids
    UPDATE _xdx_patient_hcc_icds xdx
    SET patient_hcc_id = i.id
    FROM
        ins i
    WHERE
          i.patient_id = xdx.patient_id
      AND i.hcc_id = xdx.hcc_id
    ;

    -- update patient hccs if captured or capture type becomes recapture
    UPDATE patient_hccs ph
    SET
        is_captured  = xph.is_captured
      , capture_type = xph.capture_type
      , updated_at   = NOW()
    FROM
        _xdx_patient_hccs xph
    WHERE
          xph.patient_hcc_id = ph.id
      and xph.op = 'upd'
      AND (ph.is_captured != xph.is_captured OR ph.capture_type != xph.capture_type);

    -- set is dominated and update patient_hccs table
    UPDATE _xdx_patient_hccs xph
    SET is_dominated = TRUE  -- defaulted to false above so only need to set the trues here
    WHERE
        EXISTS(SELECT
                   1
               FROM
                   public.patient_hccs ph
                   JOIN public.hccs h ON h.id = ph.hcc_id AND h.yr = ph.yr
               WHERE
                     ph.patient_id = xph.patient_id
                 AND ph.yr = xph.cms_contract_year
                 AND ph.is_captured
                 AND xph.hcc_number = ANY (h.dominant_over_hccs)
            );

    UPDATE patient_hccs ph
    SET
        is_dominated = xph.is_dominated
      , updated_at   = NOW()
    FROM
        _xdx_patient_hccs xph
    WHERE
          xph.patient_hcc_id = ph.id
      AND ph.is_dominated IS DISTINCT FROM xph.is_dominated;

-- insert captured records
    INSERT
    INTO
        patient_hcc_captured_icd10s (patient_hcc_id, hcc_icd10_id, our_status, source, capture_date, we_captured,
                                     inserted_at, updated_at)
    SELECT
        xdx.patient_hcc_id
      , xdx.hcc_icd10_id
      , NULL
      , xdx.captured_source
      , xdx.captured_date
      , xdx.we_captured
      , NOW()
      , NOW()
    FROM
        _xdx_patient_hcc_icds xdx
    WHERE
          xdx.is_captured
      AND NOT EXISTS(
            SELECT
                1
            FROM
                patient_hcc_captured_icd10s phc
            WHERE
                  phc.patient_hcc_id = xdx.patient_hcc_id
              AND phc.hcc_icd10_id = xdx.hcc_icd10_id
        )
    ;

    -- delete suspects that no longer exist
    DELETE
    FROM
        public.hcc_suspect_reasons sr
        USING public.patient_hccs ph
    WHERE
          sr.patient_hcc_id = ph.id
      AND ph.yr = ( SELECT yr FROM _ctrls )
      AND NOT EXISTS(
            SELECT
                1
            FROM
                _xdx_patient_hcc_icds xdx
            WHERE
                  sr.patient_hcc_id = xdx.patient_hcc_id
              AND sr.icd10 = xdx.icd_code_formatted
              AND xdx.capture_type = 'suspect'
        );


    -- delete recaptures that no longer exist
    DELETE
    FROM
        public.patient_hcc_recapture_icd10s r
        USING public.patient_hccs ph
    WHERE
          r.patient_hcc_id = ph.id
      AND ph.yr = ( SELECT yr FROM _ctrls )
      AND NOT EXISTS(
            SELECT
                1
            FROM
                _xdx_patient_hcc_icds xdx
            WHERE
                  r.patient_hcc_id = xdx.patient_hcc_id
              AND r.hcc_icd10_id = xdx.hcc_icd10_id
              AND xdx.capture_type = 'recapture'
        );


-- insert recaptures
    INSERT
    INTO
        patient_hcc_recapture_icd10s (patient_hcc_id, hcc_icd10_id, source, capture_date, inserted_at, updated_at, ddos,
                                      is_cms_valid)
    SELECT
        xdx.patient_hcc_id
      , xdx.hcc_icd10_id
      , xdx.source
      , xdx.captured_date
      , NOW()
      , NOW()
      , 1
      , TRUE -- could maybe get this from? SELECT * FROM stage.msh_external_dx_charges;
    FROM
        _xdx_patient_hcc_icds xdx
    WHERE
          xdx.capture_type = 'recapture'
      AND NOT EXISTS(
            SELECT
                1
            FROM
                patient_hcc_recapture_icd10s ri
            WHERE
                ri.patient_hcc_id = xdx.patient_hcc_id
          and ri.hcc_icd10_id = xdx.hcc_icd10_id
        )
    ;

--     create index on public.hcc_suspect_reasons (patient_hcc_id, icd10);
-- insert suspects
    WITH
        new AS ( SELECT
                     xdx.patient_hcc_id
                   , xdx.source
                   , xdx.suspect_reason
                   , xdx.icd_code_formatted
                 FROM
                     _xdx_patient_hcc_icds xdx
                     LEFT JOIN public.hcc_suspect_reasons sr ON xdx.patient_hcc_id = sr.patient_hcc_id
                                                            AND xdx.icd_code_formatted = sr.icd10
                 WHERE
                       sr.id ISNULL
                   AND xdx.suspect_reason IS NOT NULL )
    INSERT
    INTO
        public.hcc_suspect_reasons (patient_hcc_id, source, suspect_reason, icd10, inserted_at, updated_at)
    SELECT
        patient_hcc_id
      , source
      , suspect_reason
      , icd_code_formatted
      , NOW()
      , NOW()
    FROM
        new xdx
    ;


-- update existing suspects
    UPDATE hcc_suspect_reasons sr
    SET
        suspect_reason = xdx.suspect_reason
      , updated_at = now()
    from
        _xdx_patient_hcc_icds xdx
    WHERE xdx.patient_hcc_id = sr.patient_hcc_id
      and sr.icd10 = xdx.icd_code_formatted
      and sr.suspect_reason != xdx.suspect_reason;

    -- don't think we need to update existing recaptures

    -- delete out dangling patient hccs that have no longer recaps or suspects
    with cte as (
        select
            ph.id as patient_hcc_id
        from
            public.patient_hccs ph
            left join public.hcc_suspect_reasons sr on ph.id = sr.patient_hcc_id
            left join public.patient_hcc_recapture_icd10s r on ph.id = r.patient_hcc_id
        where
            ph.yr = (select yr from _ctrls)
        and sr.id ISNULL
        and r.id ISNULL
        and not ph.is_captured
        and not exists(select 1 from public.patient_hcc_captured_icd10s phci where phci.patient_hcc_id = ph.id)
    ),
    capture_del as (
        delete from public.patient_hcc_captured_icd10s phr
        where exists(select 1 from cte where cte.patient_hcc_id = phr.patient_hcc_id) and phr.we_captured is false
    ),
    status_del as (
        delete from public.hcc_capture_statuses hcs
        where exists(select 1 from cte where cte.patient_hcc_id = hcs.patient_hcc_id)
    ),
    notes_del as (
        delete from public.hcc_notes hn
        where exists(select 1 from cte where cte.patient_hcc_id = hn.patient_hcc_id)
    )
    delete from public.patient_hccs phh
    where exists(select 1 from cte where cte.patient_hcc_id = phh.id);

    -- Cleanup
    DROP TABLE IF EXISTS _xdx_patient_hcc_icds;
    DROP TABLE IF EXISTS _xdx_patient_hccs;
    DROP TABLE IF EXISTS _ctrls;
END
$$;

ALTER PROCEDURE synchronize_hcc_module_from_xdx(INTEGER) OWNER TO postgres;

