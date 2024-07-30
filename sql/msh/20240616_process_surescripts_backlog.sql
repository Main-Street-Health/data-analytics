SELECT * FROM pg_stat_activity where state = 'active';

SELECT * FROM analytics.oban.oban_crons where name ~* 'sure';
SELECT * FROM analytics.oban.oban_jobs WHERE worker = 'Deus.SureScripts.SureScriptsWorker' order by id desc;
SELECT * FROM sure_scripts_panels order by id desc; 12904                                                              10     11     12      13     14
SELECT sure_scripts_panel_id, * FROM sure_scripts_responses where sure_scripts_panel_id >= 12904 order by id desc;     12904, 12937, 12970,  13003, 13036, 13069
SELECT sure_scripts_panel_id, * FROM sure_scripts_med_histories where sure_scripts_panel_id >= 12904 order by id desc; 12904, 12937, 12970, 13003, 13036, 13069
SELECT sure_scripts_panel_id, * FROM sure_scripts_med_histories where id >= 13564 order by id desc; 12904, 12937, 12970, 13003, 13036
SELECT sure_scripts_med_history_id FROM prd.patient_med_adherence_synth_period_batches order by id desc;              (13597, 13663, 13630, 13729, 13696)

SELECT
    mh.id
  , mh.sure_scripts_panel_id
  , COUNT(*)
FROM
    sure_scripts_med_histories mh
    JOIN sure_scripts_med_history_details mhd ON mh.id = mhd.sure_scripts_med_history_id
WHERE
    mh.sure_scripts_panel_id >= 12904
GROUP BY
    1, 2
ORDER BY
    2
    ;

SELECT
    sure_scripts_med_history_id, *
FROM
    prd.patient_med_adherence_synth_period_batches
ORDER BY
    sure_scripts_med_history_id DESC


SELECT *
FROM
    fdw_file_router.ftp_servers
WHERE
    name = 'sure_scripts_med_history_inbound';
SELECT *
FROM
    fdw_file_router.ftp_servers
WHERE
    name ~* 'sure';
SELECT *
FROM
    sure_scripts_panels where id = 13004;


SELECT
    p.id
  , p.inserted_at
, count(*)
FROM
    sure_scripts_panels p
    left JOIN sure_scripts_panel_patients pp ON p.id = pp.sure_scripts_panel_id
WHERE p.inserted_at > now() - '3 month'::interval
GROUP BY p.id, p.inserted_at
ORDER BY 1
;

-- call etl.sp_med_adherence_load_surescripts_to_coop(13597);
-- call etl.sp_med_adherence_load_surescripts_to_coop(13663);
-- call etl.sp_med_adherence_load_surescripts_to_coop(13630);
-- call etl.sp_med_adherence_load_surescripts_to_coop(13729);
-- call etl.sp_med_adherence_load_surescripts_to_coop(13696);
SELECT *
FROM
    oban.oban_jobs
WHERE
    args ->> 'sql' ~* 'sp_med_adherence_load_surescripts_to_coop'
and state != 'completed'
ORDER BY
    id DESC;

SELECT * FROM analytics.oban.oban_jobs where id = 147155682;

SELECT * FROM analytics.oban.oban_jobs where worker ~* 'medhist';
-- update oban.oban_jobs set state = 'completed' where id IN (146052403, 146041633, 145197226, 144899152)

SELECT *
FROM
    analytics.prd.patient_medications order by id desc;
SELECT *
FROM
    sure_scripts_med_history_details where id = 94369893;