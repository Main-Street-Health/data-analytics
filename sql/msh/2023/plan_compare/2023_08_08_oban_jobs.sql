begin;
rollback;
INSERT INTO oban.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at,
                            attempted_at, completed_at, attempted_by, discarded_at, priority, tags, meta, cancelled_at,
                            state)
VALUES ('inbound_files_processing', 'Deus.Inbound.InboundFileProcessor', '{
  "id": 5,
  "name": "milliman_plan_compare_detailed_drug_cost",
  "s3_key": "inbound/milliman_plan_compare/RSV_DPC_Output_DetailedDrugCost.csv",
  "filename": "RSV_DPC_Output_DetailedDrugCost.csv",
  "s3_bucket": "msh-analytics-us-east-1-prd",
  "content_type": "text/csv"
}', '{}', 0, 20, now(), now(), null,
        null, null, null, 0,
        '{}', '{}', null, 'available');

commit;


begin;
rollback;
INSERT INTO oban.oban_jobs (queue, worker, args, errors, attempt, max_attempts, inserted_at, scheduled_at,
                            attempted_at, completed_at, attempted_by, discarded_at, priority, tags, meta, cancelled_at,
                            state)
VALUES ('inbound_files_processing', 'Deus.Inbound.InboundFileProcessor', '{
  "id": 1,
  "name": "milliman_plan_compare_providers",
  "s3_key": "inbound/milliman_plan_compare/RSV_DPC_Output_Providers.csv",
  "filename": "RSV_DPC_Output_Providers.csv",
  "s3_bucket": "msh-analytics-us-east-1-prd",
  "content_type": "text/csv"
}', '{}', 0, 20, now(), now(), null,
        null, null, null, 0,
        '{}', '{}', null, 'available');
commit;

57526973