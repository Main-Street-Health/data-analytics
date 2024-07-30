DROP TABLE IF EXISTS _files_to_delete;
CREATE TEMP TABLE _files_to_delete AS
SELECT *
FROM
    reveleer_files
WHERE
    inserted_at::DATE = '2023-09-18';

DELETE from reveleer_chase_file_details cfd
where cfd.reveleer_file_id in (select id from _files_to_delete)
DELETE from reveleer_attribute_file_details cfd
where cfd.reveleer_file_id in (select id from _files_to_delete)

DELETE from reveleer_files cfd
where cfd.id in (select id from _files_to_delete)

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT distinct cfd.patient_id, cfd.chase_id, qmc.is_contracted, qm.is_reveleer, sp.is_quality_measures
FROM
    reveleer_chase_file_details cfd
    join fdw_member_doc.patient_quality_measures pqm on pqm.id = cfd.chase_id
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
--         AND qmc.is_contracted -- this SHOULD be reflected in should_display already, but this is safer
    JOIN fdw_member_doc.quality_measures qm ON qm.id = qmc.measure_id
--                                                    AND qm.is_reveleer
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--                                                 AND sp.is_quality_measures;
where not qmc.is_contracted or not qm.is_reveleer or not sp.is_quality_measures
