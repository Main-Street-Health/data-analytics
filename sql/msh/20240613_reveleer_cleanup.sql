SELECT *
FROM
    reveleer_files
WHERE
    id > 4489
ORDER BY
    id DESC;


-- delete FROM reveleer_compliance_file_details where reveleer_file_id > 4489;
-- delete FROM reveleer_attribute_file_details where reveleer_file_id > 4489;
-- delete FROM reveleer_chase_file_details where reveleer_file_id > 4489;
-- delete FROM reveleer_files where id > 4489;
--
-- update reveleer_compliance_file_details set reveleer_file_id = null where reveleer_file_id > 4489;
-- update reveleer_attribute_file_details set reveleer_file_id = null where reveleer_file_id > 4489;
-- update reveleer_chase_file_details set reveleer_file_id = null where reveleer_file_id > 4489;

SELECT *
FROM
    reveleer_chase_file_details WHERE reveleer_project_id = 265 and reveleer_file_id ISNULL ;
SELECT *
FROM
    reveleer_projects where id = 265;

SELECT *
FROM
    reveleer_projects
WHERE
    yr = 2024;

SELECT *
FROM
    reveleer_chase_file_details
WHERE
    reveleer_project_id = 240;

SELECT distinct reveleer_file_id
FROM
    reveleer_chase_file_details
WHERE
    reveleer_project_id = 237;