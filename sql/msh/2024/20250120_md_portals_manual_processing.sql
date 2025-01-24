SELECT min(inserted_at)
FROM
    member_doc.stage.msh_md_portal_suspects;
SELECT
    ef.modified_at::DATE           dropped_at
  , COUNT(*)                       n_files
  , SUM(file_size)                 sum_file_size_bytes
  , PG_SIZE_PRETTY(SUM(file_size)) sum_file_size_pretty
FROM
    file_router.external_files ef
WHERE
      ef.ftp_server_id = 91
  AND ef.s3_bucket IS NOT NULL
  AND ef.modified_at > NOW() - '100 days'::INTERVAL
GROUP BY
    1
ORDER BY
    1
;


SELECT *
FROM
    oban.oban_crons WHERE name ~* 'md_portals';
SELECT
--     completed_at - oban_jobs.attempted_at
--   , inserted_at
--   , *
queue, worker, args, state, scheduled_at, max_attempts
FROM
    member_doc.oban.oban_jobs
WHERE
    args ->> 'sql' ~* 'md_portal'
ORDER BY
    id
;
INSERT
INTO
    oban.oban_jobs (queue, worker, args, state, scheduled_at, max_attempts)
VALUES
    ('deus_sql_runner_md_portals', 'Deus.SQLRunner', '{
      "sql": "call stage._process_md_portals_proc();",
      "params": [],
      "timeout": 86400000,
      "database_connection_id": 34
    }', 'available', NOW(), 1)
returning *
;
SELECT
    COALESCE(completed_at, NOW()) - oban_jobs.attempted_at
  , state
  , *
FROM
    member_doc.oban.oban_jobs
WHERE
    id = 205730539;
-- deus_sql_runner_async
SELECT
    COALESCE(completed_at, NOW()) - oban_jobs.attempted_at
  , state
  , *
FROM
    member_doc.oban.oban_jobs
WHERE
      queue = 'deus_sql_runner_md_portals'
  AND state IN ('available', 'executing')--, 'completed')
;
------------------------------------------------------------------------------------------------------------------------
/* performance */
------------------------------------------------------------------------------------------------------------------------
SELECT *, ended_at - started_at
FROM
    stage.md_portal_runs r
ORDER BY  id desc
;
SELECT *
     , inserted_at - LAG(inserted_at) OVER (ORDER BY id) step_processing_time
FROM
    stage.md_portals_timing
WHERE
    run_id = 30921;


------------------------------------------------------------------------------------------------------------------------
/* remove indexes */
------------------------------------------------------------------------------------------------------------------------

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx145;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx146;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx147;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx148;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx149;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx150;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx151;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx152;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx153;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx154;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx155;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx156;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx157;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx158;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx159;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx160;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx161;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx162;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx163;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx164;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx165;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx166;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx167;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx168;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx169;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx170;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx171;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx172;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx173;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx174;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx175;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx176;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx177;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx178;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx179;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx180;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx181;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx182;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx183;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx184;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx185;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx186;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx187;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx188;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx189;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx190;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx191;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx192;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx193;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx194;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx195;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx196;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx197;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx198;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx199;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx200;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx201;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx202;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx203;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx204;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx205;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx206;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx207;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx208;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx209;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx210;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx211;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx212;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx213;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx214;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx215;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx216;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx217;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx218;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx219;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx220;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx221;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx222;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx223;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx224;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx225;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx226;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx227;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx228;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx229;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx230;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx231;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx232;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx233;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx234;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx235;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx236;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx237;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx238;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx239;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx240;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx241;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx242;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx243;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx244;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx245;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx246;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx247;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx248;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx249;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx250;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx251;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx252;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx253;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx254;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx255;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx256;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx257;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx258;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx259;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx260;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx261;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx262;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx263;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx264;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx265;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx266;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx267;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx268;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx269;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx270;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx271;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx272;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx273;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx274;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx275;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx276;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx277;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx278;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx279;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx280;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx281;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx282;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx283;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx284;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx285;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx286;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx287;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx288;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx289;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx290;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx291;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx292;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx293;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx294;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx295;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx296;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx297;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx298;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx299;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx300;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx301;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx302;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx303;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx304;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx305;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx306;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx307;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx2;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx3;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx4;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx5;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx6;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx7;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx8;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx9;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx10;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx11;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx12;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx13;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx14;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx15;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx16;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx17;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx18;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx19;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx20;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx21;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx22;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx23;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx24;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx25;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx26;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx27;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx28;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx29;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx30;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx31;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx32;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx33;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx34;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx35;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx36;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx37;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx38;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx39;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx40;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx41;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx42;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx43;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx44;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx45;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx46;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx47;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx48;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx49;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx50;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx51;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx52;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx53;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx54;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx55;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx56;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx57;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx58;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx59;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx60;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx61;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx62;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx63;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx64;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx65;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx66;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx67;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx68;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx69;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx70;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx71;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx72;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx73;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx74;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx75;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx76;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx77;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx78;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx79;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx80;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx81;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx82;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx83;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx84;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx85;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx86;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx87;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx88;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx89;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx90;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx91;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx92;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx93;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx94;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx95;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx96;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx97;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx98;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx99;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx100;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx101;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx102;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx103;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx104;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx105;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx106;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx107;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx108;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx109;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx110;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx111;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx112;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx113;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx114;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx115;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx116;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx117;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx118;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx119;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx120;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx121;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx122;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx123;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx124;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx125;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx126;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx127;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx128;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx129;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx130;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx131;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx132;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx133;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx134;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx135;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx136;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx137;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx138;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx139;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx140;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx141;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx142;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx143;

DROP INDEX IF EXISTS stage.msh_md_portal_suspects_golgi_patient_id_icd10_id_idx144;



------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    stage.msh_md_portal_suspects_history
WHERE
    golgi_patient_id = 1546902;

SELECT
    i10s.code_formatted
    , i10s.id
--   , h.cms_model
--   , h.number
--   , h.description
--   , h.dominated_by_hccs
  , xdx.*
FROM
    msh_external_emr_diagnoses xdx
    JOIN icd10s i10s ON xdx.icd10_id = i10s.id
    JOIN hcc_icd10s hi10s ON i10s.id = hi10s.icd10_id -- and hi10s.yr = 2025
    JOIN hccs h ON hi10s.hcc_id = h.id --and h.yr = 2025
WHERE
      xdx.patient_id = 1546902
--   AND xdx.cms_contract_year = 2025
and not xdx.is_deleted;
SELECT *
FROM
    hccs WHERE yr = 2025;
-- call msh_daily_sproc()

SELECT *
FROM
    icd10s i
    JOIN hcc_icd10s hi10s ON i.id = hi10s.icd10_id AND hi10s.yr = 2025
WHERE
    i.code_unformatted = 'I471'


-- duplicative indexes
SELECT indrelid::regclass::text AS table
     , idx_columns
     , indexrelid::regclass::text AS index
FROM   pg_index i
     , LATERAL (
   SELECT string_agg(attname, ', ') AS idx_columns
   FROM   pg_attribute
   WHERE  attrelid = i.indrelid
   AND    attnum  = ANY(i.indkey)  -- 0 excluded by: indexprs IS NULL
   ) a
WHERE  EXISTS (
   SELECT FROM pg_index
   WHERE  indrelid = i.indrelid
   AND    indkey = i.indke  y
   AND    indexrelid <> i.indexrelid  -- exclude self
   )
AND    indexprs IS NULL -- exclude expression indexes
ORDER  BY 1, 2, 3;;
------------------------------------------------------------------------------------------------------------------------
/*
 ICDs surfacing with no support, some without compendiums and/or no surfaced ICDs listed in MDPs
Patient IDs: 1208088, 713081

 surfaced ICDs not found in MDPs list of newly identified possibilities
Patient IDs: 289166, 207457

In this example the D47.3 is listed under the known conditions but surfaced under suspected conditions.  Also noted blank suspect language
Patient ID: 240005

*/
------------------------------------------------------------------------------------------------------------------------
SELECT i.code_formatted, xdx.*
FROM
    msh_external_emr_diagnoses xdx
join icd10s i on xdx.icd10_id = i.id
WHERE xdx.patient_id = 1208088  --, 713081
  and xdx.cms_contract_year = 2025
--   and xdx.source = 'md_portal'
  and xdx.source = 'carrier'
;
SELECT i.code_formatted, i.id, xdx.*
FROM
    msh_external_emr_diagnoses xdx
join icd10s i on xdx.icd10_id = i.id
WHERE xdx.patient_id = 713081
  and xdx.cms_contract_year = 2025
  and xdx.source = 'md_portal'
and i.code_formatted = 'G70.00'
order by cms_contract_year, inserted_at
-- and not xdx.is_deleted
;

SELECT *
FROM
    member_doc.stage.msh_md_portal_suspects_history
WHERE
      golgi_patient_id = 713081
--   AND icd10_id = 6542;

;
SELECT i.code_formatted, i.id, xdx.*
FROM
    msh_external_emr_diagnoses xdx
join icd10s i on xdx.icd10_id = i.id
WHERE xdx.patient_id = 289166
  and xdx.cms_contract_year in (2024, 2025)
  and xdx.source = 'md_portal'
and i.code_formatted = 'C90.00'
order by cms_contract_year, inserted_at
-- and i.code_formatted = 'C95.90'
-- and not xdx.is_deleted
;

SELECT *
FROM
    member_doc.stage.msh_md_portal_suspects_history
WHERE
      golgi_patient_id = 289166
-- and icd_10_code = 'C90.00'
--   AND icd10_id = 6542;

SELECT *
FROM
    payers where id = 298 ;

SELECT * FROM oban_crons where name ~* 'md_portal';