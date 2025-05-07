SELECT *
FROM
    reveleer_projects
where yr = 2024
;
INSERT
INTO
    reveleer_projects (name, payer_id, state_payer_id, reveleer_id, yr, is_active)
VALUES
    ('bcbs_tn', 38, NULL, 3036, 2025, TRUE);
