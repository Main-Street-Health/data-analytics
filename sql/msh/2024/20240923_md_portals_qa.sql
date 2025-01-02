------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
-- select pg_terminate_backend(5850);
SELECT
    NOW() - query_start run_time
--      , pg_terminate_backend(pid)
  , query
  , application_name
  , usename
  , state
  , wait_event_type
  , wait_event
  , pid
FROM
    pg_stat_activity
-- WHERE
--     state != 'idle'
ORDER BY
    1 DESC
;
SELECT count(*), count(distinct golgi_patient_id) FROM stage.msh_md_portal_suspects;
SELECT golgi_patient_id, count(*) FROM stage.msh_md_portal_suspects GROUP BY 1 order by 2 desc;
-- golgi_patient_id,count
-- 407769,173482
-- 410358,151928
-- 428866,147459
-- 370675,143375
-- 104188,135777
-- 100439,119770
-- 409636,116248
-- 350347,112082
-- 774114,110123
-- 100245,100661
-- 1355605,94719
-- 482727,93664
-- 1189238,92341
-- 411285,91496
-- 103775,82601
-- 1391086,82296

-- 44,596,474.

;select  44596474.0 / 70346

SELECT *, ended_at - md_portal_runs.started_at
FROM
    member_doc.stage.md_portal_runs order by id desc;

-- 16702
SELECT count(*) FROM member_doc.stage.msh_md_portal_suspects_history where golgi_patient_id = 407769;
SELECT count(*) FROM stage.msh_md_portal_suspects where golgi_patient_id = 407769;
SELECT * FROM stage.msh_md_portal_suspects where golgi_patient_id = 407769 order by icd_10_code, source_date, source_fact_name;

SELECT
   golgi_patient_id
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

  , inserted_at
FROM
    member_doc.stage.msh_md_portal_suspects;

------------------------------------------------------------------------------------------------------------------------
/* order patients */
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE junk.md_portal_suspects_processing_order_20240923 AS
WITH
    pats AS ( SELECT
                  golgi_patient_id
                , MIN(inserted_at) min_ins_at
              FROM
                  member_doc.stage.msh_md_portal_suspects
              GROUP BY 1 )
SELECT
    golgi_patient_id
  , ROW_NUMBER() OVER (ORDER BY min_ins_at, golgi_patient_id) rn
FROM
    pats;
SELECT count(*)
FROM
    junk.md_portal_suspects_processing_order_20240923
where rn <= 1000
;
create index on     junk.md_portal_suspects_processing_order_20240923 (golgi_patient_id, rn);

------------------------------------------------------------------------------------------------------------------------
/* copy of sproc */
------------------------------------------------------------------------------------------------------------------------
-- CREATE PROCEDURE _process_md_portals_proc()
--     LANGUAGE plpgsql
-- AS
-- $$
-- DECLARE
--        has_rows BOOLEAN; message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text; sms_message_text text;
-- begin
--
--     begin
--
--         SELECT EXISTS (SELECT 1 FROM stage.msh_md_portal_suspects) INTO has_rows;
--
--         -- Exit the procedure early if the table has no rows
--         IF NOT has_rows THEN
--             RETURN;
--         END IF;

        -- START --
        drop table if exists _batch_run_id;
        create temporary table _batch_run_id as
        with x as (
            insert into stage.md_portal_runs(started_at)
            select clock_timestamp()
            returning id
        )
        select id from x;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 1 , clock_timestamp() from _batch_run_id;

        -- update icd10_id's
-- CREATE INDEX ON stage.msh_md_portal_suspects(icd_10_code);
        UPDATE stage.msh_md_portal_suspects s
        SET
            icd10_id = i.id
        FROM
            public.icd10s i
        WHERE
              s.icd10_id ISNULL
              AND i.code_formatted = s.icd_10_code
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 2 , clock_timestamp() from _batch_run_id;

        -- Map obsolete icd10s
        UPDATE stage.msh_md_portal_suspects s
        SET
            old_icd10_id = om.old_icd10_id
          , icd10_id     = om.new_icd10_id
          , icd_10_code   = i.code_formatted
        FROM
            public.icd10_obsolete_map om
        join public.icd10s i on i.id = om.new_icd10_id
        WHERE
          s.icd10_id = om.old_icd10_id
          AND (om.to_date ISNULL OR NOW()::DATE BETWEEN om.from_date AND om.to_date)
        ;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 3 , clock_timestamp() from _batch_run_id;

        DROP TABLE IF EXISTS _author_mappings;
        CREATE TEMP TABLE _author_mappings AS
        select * from stage.md_portal_author_mappings x where is_deleted is false
        ;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 4 , clock_timestamp() from _batch_run_id;
        DROP TABLE IF EXISTS _good_icds;
        CREATE TEMP TABLE _good_icds AS
        select * from stage.fn_cca_valid_icd10s()
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 5 , clock_timestamp() from _batch_run_id;

        DROP TABLE IF EXISTS _all_codes;
        CREATE TEMP TABLE _all_codes AS
        SELECT
            s.golgi_patient_id patient_id
          , icd_10_code icd10_code
          , s.icd10_id
          , hi.hcc_id
          , chs.months_look_back
          , chs.hcc_number
          , xdx.suspect_reason_machine
          , null::text[] source_evidence_types
          -- ADH (2022-08-24) | no longer filtering & instead | include/exclude dependent on hcc months look_back
          , case when bool_or(coalesce(s.source_date, now()) > NOW() - interval '1 month' * chs.months_look_back) then 'include' else 'exclude' end include_in_worksheet_machine
          , case when bool_or(coalesce(s.source_date, now()) > NOW() - interval '1 month' * chs.months_look_back) then  '{}'::text[] else '{source_date too old}'::text[] end exclude_from_worksheet_reasons -- default to empty array for easy appending
          -- BP 2023-02-02: Want suspects with both expired source_date and null source_date records to make it through date exclusions
          -- , max(coalesce(source_date, current_date)) most_recent_evidence_date -- wrong old way excluded if only populated dates were expired
          , max(coalesce(s.source_date, current_date)) coalesce_most_recent_evidence_date -- if any nulls, current_date is most recent, used for date restrictions
          , max(s.source_date)                         real_most_recent_evidence_date     -- actual most recent known evidence, used to populate xdx table
        FROM
            stage.msh_md_portal_suspects s
/* new */
/* Chunk */ join junk.md_portal_suspects_processing_order_20240923 j on j.golgi_patient_id = s.golgi_patient_id and j.rn <= 1000
/* new */
            join public.hcc_icd10s hi on hi.icd10_id = s.icd10_id and hi.is_dominant and hi.yr = date_part('year', current_date)
            join public.hccs hc on hc.id = hi.hcc_id
            join stage.cca_hcc_suspect_months_look_back chs on chs.hcc_number = hc.number
                                                           and chs.cms_model = hc.cms_model
            left join public.msh_external_emr_diagnoses xdx on xdx.patient_id = s.golgi_patient_id
                                                           and xdx.cms_contract_year = hi.yr
                                                           and xdx.icd10_id = s.icd10_id
                                                           and not xdx.is_deleted
        GROUP BY 1,2,3,4,5,6,7
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 6 , clock_timestamp() from _batch_run_id;
        create index idx__all_codesxxxxxx on _all_codes(patient_id, icd10_id);
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 7 , clock_timestamp() from _batch_run_id;

        /*
         - evidence_type
         -
         */

        drop table if exists _distinct_group_facts;
        create temporary table _distinct_group_facts as
        SELECT
            distinct
            golgi_patient_id,
            coalesce(source_fact_name, 'Yes') source_fact_name,
            source_supportive_evidence,
            source_excerpt,
            source_date,
            source_author,
            source_text,
            hcc.id hcc_id,
            hcc.number hcc_number,
           s.source_evidence_type
        FROM
            stage.msh_md_portal_suspects s
/* new */
/* Chunk */ join junk.md_portal_suspects_processing_order_20240923 j on j.golgi_patient_id = s.golgi_patient_id and j.rn <= 1000
/* new */
            join public.hcc_icd10s hi on s.icd10_id = hi.icd10_id and hi.yr = date_part('year', now()) and hi.is_dominant --_yr
            join public.hccs hcc on hi.hcc_id = hcc.id
                -- BP added 10/3/22
            join stage.cca_hcc_suspect_months_look_back chs on chs.hcc_number = hcc.number
                                                           and chs.cms_model = hcc.cms_model
                                                           and coalesce(s.source_date, now()) > NOW() - interval '1 month' * chs.months_look_back
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 8 , clock_timestamp() from _batch_run_id;

        DROP TABLE IF EXISTS _distinct_icd_facts;
        CREATE TEMP TABLE _distinct_icd_facts AS
        SELECT
            golgi_patient_id
          , source_fact_name
          , source_supportive_evidence
          , source_excerpt
          , source_date
          , coalesce(source_author, source_organization_name, source_encounter_organization) source_author
          , source_text
          , hcc_id
          , hcc_number
          , source_evidence_type
          , s.icd10_id
        FROM
            stage.msh_md_portal_suspects s
/* new */
/* Chunk */ join junk.md_portal_suspects_processing_order_20240923 j on j.golgi_patient_id = s.golgi_patient_id and j.rn <= 1000
/* new */
            JOIN public.hcc_icd10s hi
                 ON s.icd10_id = hi.icd10_id AND hi.yr = DATE_PART('year', NOW()) AND hi.is_dominant --_yr
            JOIN public.hccs hcc ON hi.hcc_id = hcc.id
            JOIN stage.cca_hcc_suspect_months_look_back chs ON chs.hcc_number = hcc.number
                                                           AND chs.cms_model = hcc.cms_model
                                                           AND COALESCE(s.source_date, NOW()) > NOW() - INTERVAL '1 month' * chs.months_look_back
        ;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 9 , clock_timestamp() from _batch_run_id;

        -- Group by HCC to produce grouped suspect reason
        DROP TABLE IF EXISTS _formatted_agg_facts;
        CREATE TEMP TABLE _formatted_agg_facts AS
        WITH
            base AS ( SELECT
                          golgi_patient_id
                        , source_fact_name
                        , source_supportive_evidence
                        , source_excerpt
                        , source_date
                        , source_author
                        , source_text
                        , hcc_id
                        , hcc_number
                        , source_evidence_type
                        , null::bigint icd10_id -- allows us to later group by hcc
                      FROM
                          _distinct_group_facts
                      UNION
                      SELECT
                          golgi_patient_id
                        , source_fact_name
                        , source_supportive_evidence
                        , source_excerpt
                        , source_date
                        , source_author
                        , source_text
                        , hcc_id
                        , hcc_number
                        , source_evidence_type
                        , icd10_id
                      FROM
                          _distinct_icd_facts
            ),
            numbered_facts AS (
            SELECT b.*
                 , row_number() OVER (PARTITION BY golgi_patient_id, hcc_number, icd10_id, lower(source_fact_name) ORDER BY source_date DESC ) rn -- deduplicate case insensitive : 2023-06-6 : ADH & BP
            FROM
                base b
        ), most_recent_facts AS (
            -- Take most recent of each fact, take two most recent labs
            SELECT
                DISTINCT *
            FROM
                 numbered_facts
            WHERE
                (
                  rn = 1
                  OR
                  -- TODO: Lab flag here
                  ( rn IN (1, 2) AND source_supportive_evidence NOT IN ('Yes', 'No') )
              )

        ), bp_readings as (
            -- Need to combine BP into one row
            -- Had to split out to format both rows together
            SELECT
                'BP ' || (regexp_split_to_array(sys.source_supportive_evidence, ' '))[1] || '/' || (regexp_split_to_array(dia.source_supportive_evidence, ' '))[1] suspect_reason
                 , sys.*
            FROM
                 most_recent_facts sys
                 join most_recent_facts dia on sys.golgi_patient_id = dia.golgi_patient_id
                                               and sys.rn = dia.rn
            WHERE
                sys.source_fact_name = 'Systolic BP'
                and dia.source_fact_name = 'Diastolic BP'
                and sys.rn <= 2
                and dia.rn <= 2

        ), egfr_readings as (
            -- Need to dedupe egfrs types
            -- BP added 10/4/2022
            SELECT
                golgi_patient_id,
                -- concat(source_fact_name, ': ',  source_supportive_evidence) suspect_reason, -- 2023-06-5 : ADH &Adam Pritts - no need to keep African American determination or non...
                concat('eGFR: ',  source_supportive_evidence) suspect_reason,
                source_fact_name,
                source_supportive_evidence,
                source_excerpt,
                source_date,
                source_author,
                hcc_id,
                hcc_number,
                icd10_id,
                rn,
                row_number() OVER (
                    PARTITION BY golgi_patient_id, hcc_id, rn
                    ORDER BY
                        case
                            when egfr.source_fact_name = 'eGFR'                        then 1
                            when egfr.source_fact_name = 'eGFR (non-African American)' then 2
                            when egfr.source_fact_name = 'eGFR (African American)'     then 3
                        END
                    ) egfr_rn
            FROM
                 most_recent_facts egfr
            WHERE
                egfr.source_fact_name in ( 'eGFR', 'eGFR (African American)', 'eGFR (non-African American)' )

        ), formatted_facts AS (
            -- exclude no's, include yes's w/o yes, if drug include source excerpt up to mg, otherwise combine fact name and evidence
            -- union with the bp's
            SELECT
                CASE
                    -- 2023-12-18 BP added source text if medication
                    WHEN source_evidence_type = 'Medications' and source_text is not null                      THEN source_text
                    WHEN source_supportive_evidence = '' OR source_fact_name LIKE '%Drug Class'                THEN (REGEXP_SPLIT_TO_ARRAY(source_excerpt, 'mg|MG'))[1] || 'MG'
                    WHEN coalesce(source_supportive_evidence, 'Yes') = 'Yes' AND source_fact_name = 'Yes'      THEN source_excerpt -- BP: Added 12/13/22
                    WHEN coalesce(source_supportive_evidence, 'Yes') = 'Yes' AND source_fact_name is not null  THEN
                                                                                                                 CASE -- BP Added 2024-02-20
                                                                                                                   WHEN source_evidence_type not in ('medications', 'medications administered', 'results', 'vital signs')
                                                                                                                   THEN 'mentioned in chart ' || source_fact_name ELSE source_fact_name END
                    WHEN coalesce(source_supportive_evidence, 'Yes') = 'Yes' AND source_fact_name is null      THEN source_excerpt
                                                                                                               ELSE concat(source_fact_name, case when source_fact_name is not null and source_supportive_evidence is not null then ': ' end,  source_supportive_evidence)
                END suspect_reason
                , golgi_patient_id, source_fact_name, source_supportive_evidence, source_excerpt, source_date, source_author, hcc_id, icd10_id, hcc_number, rn
            FROM
                most_recent_facts mrf
            WHERE
                coalesce(source_supportive_evidence,'Yes') != 'No' --Added 2022-09-28, to include nulls covered by case statement coalesce
                AND source_fact_name NOT IN ('Race', 'Gender', 'Systolic BP', 'Diastolic BP',  'eGFR', 'eGFR (African American)', 'eGFR (non-African American)' )
            --
            UNION
            --
            select
                suspect_reason, golgi_patient_id, source_fact_name, source_supportive_evidence, source_excerpt, source_date, source_author, hcc_id, icd10_id, hcc_number, rn
            from
                bp_readings
            UNION
            -- BP added 10/4/2022
            select
                suspect_reason, golgi_patient_id, source_fact_name, source_supportive_evidence, source_excerpt, source_date, source_author, hcc_id, icd10_id, hcc_number, rn
            from
                egfr_readings  e
            where e.egfr_rn = 1
        )
        -- group the suspect reasons
        SELECT
            distinct
              golgi_patient_id
            , hcc_id
            , h.number hcc_number
            , icd10_id
            , source_date
            , coalesce(am.author_abbr, af.source_author) author
            , STRING_AGG(distinct suspect_reason, ', ') reasons
        FROM
            formatted_facts af
            join public.hccs h on h.id = af.hcc_id
            left join _author_mappings am on am.source_author = af.source_author
        GROUP BY
            1, 2, 3, 4, 5, 6
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 10, clock_timestamp() from _batch_run_id;

        -- Note: _md_portal_code_groups is at the hcc level if limited to where icd10_id isnull, otherwise it is icd level
        DROP TABLE IF EXISTS _md_portal_code_groups;
        CREATE TEMP TABLE _md_portal_code_groups AS
        with abbr_facts as (
            -- abbreviate certain conditions
            SELECT
                stage.suspect_reason_abbreviations(reasons) abbr_reason
                , *
            FROM
                _formatted_agg_facts ff

        )
        -- final suspect reason formatting
        SELECT
            af.golgi_patient_id patient_id,
            af.hcc_id,
            af.hcc_number,
            af.icd10_id,
            null::text[] source_evidence_types,
            string_agg(
                    trim(
                        concat(
                            to_char(source_date, 'FMMM/FMDD/YY'),
                            case when source_date is not null and trim(coalesce(author,'')) <> '' then ' ' end,
                            author,
                            case when (source_date is not null or trim(coalesce(author,'')) <> '') and coalesce(trim(abbr_reason),'') <> '' then ': ' end,
                            abbr_reason
                        )
                    ),
            '\n'  order by source_date DESC ) suspect_reason_machine
        FROM
            abbr_facts af
        GROUP BY 1,2,3,4;

        -- 2024-02-20 add "mentioned in chart " only once https://github.com/Main-Street-Health/member-doc/issues/8023
        -- this clears out any "mentioned in chart" after the first occurrence, it's fkd but IDK another way
        update _md_portal_code_groups af
        set suspect_reason_machine  =
                -- string up to end of first 'mentioned in chart '
                SUBSTRING(suspect_reason_machine  FROM 1 FOR POSITION('mentioned in chart ' IN suspect_reason_machine ) + LENGTH('mentioned in chart ') - 1)
                -- rest of string with 'mentioned in chart ' removed
                || REPLACE(SUBSTRING(suspect_reason_machine  FROM POSITION('mentioned in chart ' IN suspect_reason_machine ) + LENGTH('mentioned in chart ')), 'mentioned in chart ', '')
        where af.suspect_reason_machine  like '%mentioned in chart %'
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 11, clock_timestamp() from _batch_run_id;

        -- put the icd level suspect reasons into all codes suspect_reason_machine
        update _all_codes ac
        SET
            suspect_reason_machine = mpcg.suspect_reason_machine
        FROM
            _md_portal_code_groups mpcg
        WHERE
              mpcg.icd10_id IS NOT NULL
          AND ac.patient_id = mpcg.patient_id
          AND ac.icd10_id = mpcg.icd10_id
          AND ac.suspect_reason_machine IS DISTINCT FROM mpcg.suspect_reason_machine;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 12, clock_timestamp() from _batch_run_id;

        -- update all codes with distinct source_evidence_types
        with source_evidence_types as (
            select golgi_patient_id, icd10_id, hcc_id
                 , array_agg(distinct source_evidence_type order by source_evidence_type) source_evidence_types
            from _distinct_icd_facts di
            group by 1, 2, 3
        )
        update _all_codes ac
            set source_evidence_types = st.source_evidence_types
        from
            source_evidence_types st
        where
            st.golgi_patient_id = ac.patient_id
            and st.hcc_id = ac.hcc_id
            and st.icd10_id = ac.icd10_id
        ;
        -- no longer need _distinct_icd_facts after evidence types is populated
        DROP TABLE IF EXISTS _distinct_icd_facts;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 13, clock_timestamp() from _batch_run_id;
        -- Done with the icd level suspect reasons, nuking them to avoid confusion
        delete from _md_portal_code_groups where icd10_id is not null;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 14, clock_timestamp() from _batch_run_id;

        -- update with source_evidence_types
        with source_evidence_types as (
            select golgi_patient_id, hcc_id, hcc_number, array_agg(distinct source_evidence_type) source_evidence_types
            from _distinct_group_facts dg
            group by 1, 2, 3
        )
        update _md_portal_code_groups mx
            set source_evidence_types = st.source_evidence_types
        from
            source_evidence_types st
        where
            st.golgi_patient_id = mx.patient_id
            and st.hcc_id = mx.hcc_id
        ;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 15, clock_timestamp() from _batch_run_id;
        UPDATE _all_codes mpc
        SET exclude_from_worksheet_reasons = '{evidence_gt_' || mpc.months_look_back::text || '_months_look_back}' || exclude_from_worksheet_reasons,
            include_in_worksheet_machine = 'exclude'
        WHERE mpc.coalesce_most_recent_evidence_date < now() - interval '1 months' * mpc.months_look_back
        ;

        UPDATE _all_codes mpc
        SET exclude_from_worksheet_reasons = '{icd_exclusion}' || exclude_from_worksheet_reasons,
            include_in_worksheet_machine = 'exclude'
        WHERE NOT exists(select 1 from _good_icds gi where gi.icd10_id = mpc.icd10_id)
        ;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 16, clock_timestamp() from _batch_run_id;
        drop table if exists _inbound_patients;
        create temporary table _inbound_patients(patient_id bigint primary key);
        insert into _inbound_patients(patient_id)
        select
            distinct j.golgi_patient_id patient_id
        from
            stage.msh_md_portal_suspects s
/* new */
/* Chunk */ join junk.md_portal_suspects_processing_order_20240923 j on j.golgi_patient_id = s.golgi_patient_id and j.rn <= 1000
/* new */
        ;
        create index on _inbound_patients(patient_id);
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 17, clock_timestamp() from _batch_run_id;
        -----------------------------------------------------------------------------------------------------------------------------------------------------
        -- MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE MUTATE
        -----------------------------------------------------------------------------------------------------------------------------------------------------
        -- MOVING TO INCREMENTAL UPDATE
        -- delete all md_portal suspects that have not been qa'ed or put on a worksheet
        -- this is pretty slow, may want to find alternative

        -- DELETE XDX records

        drop table if exists _to_delete;
        create temporary table _to_delete as (
            select
                distinct id
            from public.msh_external_emr_diagnoses xdxd
            where
                not exists (select 1 from public.msh_cca_worksheet_dxs cca_dx where xdxd.id = cca_dx.external_emr_diagnosis_id)
                and exists (select 1 from _inbound_patients ib where ib.patient_id = xdxd.patient_id) -- only delete if the patient is in this md_portals feed
                and (
                        not exists (select 1 from _all_codes ac where ac.patient_id = xdxd.patient_id and ac.icd10_id = xdxd.icd10_id) -- delete if code isn't in this latest batch
                    or
                        exists(select 1 from stage.msh_md_portal_suspects s where s.golgi_patient_id = xdxd.patient_id and s.icd_10_code ISNULL) -- null icd from mdp indicates the patient has no codes in mdp, delete all
                )
                and xdxd.source = 'md_portal'
                and xdxd.diagnosis_type = 'suspect' -- if a recapture comes in for what was a suspect, we'll flip to recapture and retain the source
                and xdxd.cms_contract_year = date_part('year', current_date)
                and xdxd.is_deleted is false -- do not remove deleted ADH 2022-11-19
        );
        create index idx_to_delete on _to_delete(id);

        -- TODO // WTF
        -- hard deletes are too slow
        update public.msh_external_emr_diagnoses xdxd
        set is_deleted = true
        where exists (select 1 from _to_delete td where td.id = xdxd.id)
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 18, clock_timestamp() from _batch_run_id;

        -- SOFT DELETE XDX records if on worksheet :: ADH 2022-11-15
        update public.msh_external_emr_diagnoses xdxd
            set is_deleted = true
        where
                exists(select 1 from public.msh_cca_worksheet_dxs cca_dx where xdxd.id = cca_dx.external_emr_diagnosis_id and cca_dx.code_status !~* 'yes') -- unless checked yes on worksheet :: ADH 2022-11-15
            and     exists (select 1 from _inbound_patients ib where ib.patient_id = xdxd.patient_id) -- only delete if the patient is in this md_portals feed
            and (
                        not exists (select 1 from _all_codes ac where ac.patient_id = xdxd.patient_id and ac.icd10_id = xdxd.icd10_id) -- delete if code isn't in this latest batch
                    or
                        exists(select 1 from stage.msh_md_portal_suspects s where s.golgi_patient_id = xdxd.patient_id and s.icd_10_code ISNULL) -- null icd from mdp indicates the patient has no codes in mdp, delete all
            )
            and xdxd.source = 'md_portal'
            and xdxd.diagnosis_type = 'suspect' -- if a recapture comes in for what was a suspect, we'll flip to recapture and retain the source
            and xdxd.cms_contract_year = date_part('year', current_date)
            and xdxd.is_deleted is false
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 19, clock_timestamp() from _batch_run_id;

        -- INSERT new codes
        drop table if exists _patients_with_inserted_codes;
        create temporary table _patients_with_inserted_codes as
        with insrt as (

            INSERT into public.msh_external_emr_diagnoses (
                patient_id, include_in_worksheet, include_in_worksheet_machine, exclude_in_worksheet_rule_reasons, icd10_id, diagnosis_type, procedure_dos,
                inserted_at, updated_at, source, cms_contract_year, suspect_reason_machine,
                 most_recent_suspect_evidence_date, needs_review,
                hcc_number, hcc_id, source_evidence_types
            )
            SELECT
                  distinct
                   mpc.patient_id
                 , 'needs_decision' include_in_worksheet
                 , mpc.include_in_worksheet_machine
                 , mpc.exclude_from_worksheet_reasons
                 , mpc.icd10_id
                 , 'suspect'   diagnosis_type
                 , null::date  procedure_dos
                 , now()       inserted_at
                 , now()       updated_at
                 , 'md_portal' source
                 , date_part('year', now()) cms_contract_year
                 , mpc.suspect_reason_machine
                 , coalesce(real_most_recent_evidence_date, current_date)
                 , true
                 , mpc.hcc_number
                 , mpc.hcc_id
                 , mpc.source_evidence_types
            FROM
                _all_codes mpc
            where
                not exists(
                            select 1
                            from
                                public.msh_external_emr_diagnoses ext_dx
                            where
                                ext_dx.patient_id = mpc.patient_id
                                and ext_dx.icd10_id = mpc.icd10_id
                                and ext_dx.cms_contract_year = date_part('year', now())::int
                                and ext_dx.is_deleted is false
                )
            returning patient_id
        )
        select
            distinct i.patient_id
        from
            insrt i
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 19, clock_timestamp() from _batch_run_id;

        -- ADH & BP 2023-01-23 (so the CCA can pick up all suspected dxs)
        -- update the diagnosis type if not (recapture, suspect) -- should overwrite the rural, frailty, ...
        update public.msh_external_emr_diagnoses ext_dx
            set diagnosis_type = 'suspect',
                suspect_reason_machine = mpc.suspect_reason_machine,
                source_evidence_types = mpc.source_evidence_types,
                updated_at = now()
        from _all_codes mpc
        where
            ext_dx.patient_id = mpc.patient_id
            and ext_dx.icd10_id = mpc.icd10_id
            and ext_dx.cms_contract_year = date_part('year', now())::int
            and ext_dx.is_deleted is false
            and ext_dx.diagnosis_type in ('rural', 'frailty')
        ;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 20, clock_timestamp() from _batch_run_id;
        -- BP 2023-01-23 update xdx suspect reason_machine
        update public.msh_external_emr_diagnoses ext_dx
            set suspect_reason_machine = mpc.suspect_reason_machine,
                source_evidence_types = mpc.source_evidence_types,
                updated_at = now()
        from _all_codes mpc
        where
            ext_dx.patient_id = mpc.patient_id
            and ext_dx.icd10_id = mpc.icd10_id
            and ext_dx.cms_contract_year = date_part('year', now())::int
            and ext_dx.is_deleted is false
            and (
                   mpc.suspect_reason_machine IS DISTINCT FROM ext_dx.suspect_reason_machine
                OR mpc.source_evidence_types IS DISTINCT FROM ext_dx.source_evidence_types
            )
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 21, clock_timestamp() from _batch_run_id;


        -- add compendium urls to patients BP added 2023-09-25
        UPDATE public.patients p
        SET
            md_portal_suspect_source_link = mds.compendium_url
          , updated_at                    = NOW()
        FROM
            ( SELECT DISTINCT ON (golgi_patient_id)
                  golgi_patient_id
                , compendium_url
              FROM
                  stage.msh_md_portal_suspects
              ORDER BY golgi_patient_id, id DESC ) mds
        WHERE
              mds.golgi_patient_id = p.id
          AND p.md_portal_suspect_source_link ISNULL;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 22, clock_timestamp() from _batch_run_id;

        -- NEW CODES -> Indicate so on worksheet
        update public.msh_cca_worksheets wsu
            set  new_diagnoses_available = true
        from
            _patients_with_inserted_codes x
            join public.msh_cca_worksheets ws on ws.patient_id = x.patient_id
                 and ws.status in ('draft', 'qa_completed')
        where
            wsu.id = ws.id
        ;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 23, clock_timestamp() from _batch_run_id;

--         UPDATE public.msh_external_diagnosis_groups dg
--         SET suspect_reason_machine = mpcd.suspect_reason_machine,
--             suspect_reason_machine_updated_at = now(),
--             updated_at = now(),
--             source_evidence_types = coalesce(mpcd.source_evidence_types, dg.source_evidence_types) -- ADDED by ADH 2022-10-24
--         from _md_portal_code_groups mpcd
--         WHERE
--             mpcd.patient_id = dg.patient_id
--             and mpcd.hcc_id = dg.hcc_id
--             and dg.yr = date_part('year', current_date)
--             and (
--                 trim(coalesce(mpcd.suspect_reason_machine,'')) <> coalesce(dg.suspect_reason_machine,'')
--                 and trim(coalesce(mpcd.suspect_reason_machine,'')) <> ''
--                 --
--                 or
--                 -- compare the evidence types
--                 not coalesce((dg.source_evidence_types @> mpcd.source_evidence_types and dg.source_evidence_types <@ mpcd.source_evidence_types), false)
--             )
--         ;


        -- insert new hcc groups
--         INSERT INTO public.msh_external_diagnosis_groups (patient_id, hcc_id, hcc_number, yr, suspect_reason_machine, inserted_at, updated_at, source_evidence_types)
--         SELECT
--             distinct
--             patient_id,
--             hcc_id,
--             h.number hcc_number,
--             date_part('year', now()) yr, --_yr,
--             suspect_reason_machine,
--             now() inserted_at,
--             now() updated_at,
--             cg.source_evidence_types
--         FROM
--             _md_portal_code_groups cg
--             join public.hccs h on h.id = cg.hcc_id
--         WHERE
--             NOT exists(
--                         select 1
--                         from public.msh_external_diagnosis_groups dg2
--                         where
--                             dg2.patient_id = cg.patient_id
--                             and dg2.hcc_id = cg.hcc_id
--                             and dg2.yr = date_part('year', now()) -- _yr
--         );


        -- update all of the xdx records: ADH 2022-11-06 (moving this to the update edges)
        drop table if exists _prep_patients;
        create temporary table _prep_patients as
                                    /* BP swap use of stage.suspects for _inbound_patients */
        with prep_pats as (select array_agg(distinct patient_id) pis from _inbound_patients)
        select 1 from
            prep_pats pp
            cross join stage._process_cca_groupings_for_cca_draft_multi(pp.pis)
        ;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 24, clock_timestamp() from _batch_run_id;
        -------------------------------
        -- for the love of debugging --

            -- cleanup the old patient records
            with inbound_golgi_ids as (
                select distinct patient_id golgi_patient_id from _inbound_patients
            )
            delete from stage.msh_md_portal_suspects_history xx where exists (select 1 from inbound_golgi_ids eek where xx.golgi_patient_id = eek.golgi_patient_id);

            insert into stage.msh_md_portal_suspects_history(
                id, golgi_patient_id, icd_10_code, reason, source_fact_name, source_supportive_evidence, source_excerpt, source_date, source_author, source_description, source_location, compendium_url, hcc_category, icd10_id, old_icd10_id,
                source_evidence_type, source_document_type, source_document_type_loinc, source_document_title, source_encounter_type, source_encounter_organization, source_organization_name, source_author_software, md_portals_batch_id, source_text
            )
            select
                id, golgi_patient_id, icd_10_code, reason, source_fact_name, source_supportive_evidence, source_excerpt, source_date, source_author, source_description, source_location, compendium_url, hcc_category, icd10_id, old_icd10_id,
                source_evidence_type, source_document_type, source_document_type_loinc, source_document_title, source_encounter_type, source_encounter_organization, source_organization_name, source_author_software, md_portals_batch_id, source_text
            from stage.msh_md_portal_suspects
            ;

        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 25, clock_timestamp() from _batch_run_id;
        -- end of for the love of debug --

        -- END ---

        -- DO NOT REMOVE (intentionally removing all old records) :: Brendon Pierson :) 2022-10-24
        truncate table stage.msh_md_portal_suspects; -- ADH Moved from Analytics to here 2022-11-10
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 26, clock_timestamp() from _batch_run_id;

        update stage.md_portal_runs set ended_at = clock_timestamp() where id = (select id from _batch_run_id);
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 27, clock_timestamp() from _batch_run_id;


        -- clean up temp tables
        DROP TABLE IF EXISTS _author_mappings;
        DROP TABLE IF EXISTS _good_icds;
        DROP TABLE IF EXISTS _md_portal_urls;
        DROP TABLE IF EXISTS _all_codes;
        drop table if exists _distinct_group_facts;
        DROP TABLE IF EXISTS _formatted_agg_facts;
        DROP TABLE IF EXISTS _md_portal_code_groups;
        drop table if exists _inbound_patients;
        drop table if exists _to_delete;
        drop table if exists _patients_with_inserted_codes;
        drop table if exists _prep_patients;
        insert into stage.md_portals_timing (run_id, step, inserted_at) select id, 28, clock_timestamp() from _batch_run_id;
        drop table if exists _batch_run_id;

