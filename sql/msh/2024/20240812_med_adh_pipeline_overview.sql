------------------------------------------------------------------------------------------------------------------------
/* Med adh flow */
------------------------------------------------------------------------------------------------------------------------
-- oban cron that kicks off the whole process
SELECT * FROM oban.oban_crons where name = 'sure_scripts_panel_generation';

-- that cron job runs an elixir function: Deus.SureScripts.SureScriptsPanelGenerator.generate_panel_file
-- that function stages who should be sent with this stored procedure:
call public.sp_populate_sure_scripts_panel_patients()

-- once the data is staged in the below deus pulls it into a file and drops on the surescripts sftp using file router
SELECT *
FROM
    public.sure_scripts_panel_patients pp
    JOIN public.sure_scripts_panels p ON pp.sure_scripts_panel_id = p.id
    ;

-- an initial response file is usually returned quickly, picked up by this file router ftp server (also used for the outbound panel drop)
SELECT * FROM fdw_file_router.ftp_servers WHERE name = 'sure_scripts_panel';

-- the response data is stored in:
SELECT *
FROM
    sure_scripts_responses r
join sure_scripts_response_details rd on r.id = rd.sure_scripts_response_id
-- join sure_scripts_panels p on p.id = rd.sure_scripts_panel_id
;

-- eventually a med history file is dropped and picked up by this ftp_server
SELECT * FROM fdw_file_router.ftp_servers WHERE name = 'sure_scripts_med_history_inbound';

-- once the med history is pulled it is processed with Deus.SureScripts.SureScriptsFileProcessor.process_med_history_file
-- the raw data is loaded into
SELECT *
FROM
    sure_scripts_med_histories h
join sure_scripts_med_history_details mhd on h.id = mhd.sure_scripts_med_history_id
-- join sure_scripts_panels p on p.id = h.sure_scripts_panel_id
;

-- once the raw data is loaded this stored procedure is called to etl the data into a deduped standardized table format
-- this has the logic that ties NDCs to measures, dedupes patient_medications, and builds synth periods
-- this procedure also populates the coop tables: public.patient_medication_fills and public.qm_pm_med_adh_synth_periods
-- The last step calls the coop stored procedure: call public.qm_pm_med_adh_process();
call etl.sp_med_adherence_load_surescripts_to_coop(_sure_scripts_med_history_id := -99999);

---- switch to member doc db
-- Note In parallel to the above pipeline mco data is loaded through Matt Z's pipeline to stage.qm_pm_med_adh_mco_measures

-- this sproc uses the synth periods or mco data to calculate the latest med adh metrics
-- The sproc chooses mco or SS data based on which is more recent, a single metric will only be based off of one source
-- then using the metrics handoffs are create to signal a potential change in the state of the measure ie nfd should gen a task or close a task or the measure is excluded/inactive etc
-- the final step is to trigger an oban job that kicks off the med adh worker in Coop MD.QualityMeasures2.Workflows.MedAdhWorker
call public.qm_pm_med_adh_process();

-- Note: it's very important that the panel is processed the day it is generated
-- when the last step call public.qm_pm_med_adh_process(); is run it will generate tasks based on who has a next fill date trigger or reopen date (compliance_check_date on active wf) regardless of if they've been sent to sure scripts that day

