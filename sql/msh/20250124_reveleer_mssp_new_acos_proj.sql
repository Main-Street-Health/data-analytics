
-- Current (OLD) table
SELECT * FROM raw.reveleer_mssp_cqm_aco_membrs_20240624;

-- new table from sean, existing aco is A5404
select * from raw.reveleer_mssp_cqm_aco_members_2024_q3_alr;

CREATE INDEX ON raw.reveleer_mssp_cqm_aco_members_2024_q3_alr(patient_id);

ALTER TABLE raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
    ADD reveleer_project_id bigint;

SELECT *
FROM
    reveleer_projects where id = 298 ;

INSERT
INTO
    public.reveleer_projects (name, payer_id, state_payer_id, reveleer_id, inserted_at, updated_at,
                              measures_to_send, yr, is_active)
VALUES
    ('mssp_cqm_aco_the_rest', 81, NULL, '2886', NOW(), NOW(),
     '{cbp_controlling_high_blood_pressure,hbd_hemoglobin_a1c_control_for_patients_with_diabetes,dsf_depression}', 2024,
     TRUE)
returning *
;

update raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
    set reveleer_project_id = case when contract_id = 'A5404' then 298 else 397 end;



SELECT
    reveleer_project_id
  , COUNT(*)
FROM
    raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
GROUP BY
    1
;
SELECT
    COUNT(*) n
  , COUNT(DISTINCT (patient_id, reveleer_project_id)) nd_pat_contract
  , COUNT(DISTINCT patient_id) nd_pat
FROM
    raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
WHERE
    patient_id IS NOT NULL;


SELECT *
FROM
    ( SELECT
          patient_id
        , COUNT(DISTINCT contract_id)     n
        , ARRAY_AGG(DISTINCT contract_id) contracts
      FROM
          raw.reveleer_mssp_cqm_aco_members_2024_q3_alr
      WHERE
          patient_id IS NOT NULL
      GROUP BY
          1
      HAVING
          COUNT(DISTINCT contract_id) > 1 ) x
WHERE
    'A5404' = ANY (x.contracts)

;


call sp_reveleer_data_stager();
    ;
SELECT reveleer_project_id, count(*)
FROM
    reveleer_chase_file_details cfd
where inserted_at > now() - '2 weeks'::interval and reveleer_project_id in (298, 397)
GROUP BY reveleer_project_id
;
SELECT *
FROM
    fdw_file_router.ftp_servers ;
SELECT *
FROM
    reveleer_projects order by id desc;
SELECT external_chase_id is null missing_reveleer_chase_id,  count(*)
FROM
    reveleer_chases rc
WHERE
    reveleer_project_id = 397
GROUP BY external_chase_id
;

