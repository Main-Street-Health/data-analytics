DROP TABLE IF EXISTS _chases;
CREATE TEMP TABLE _chases AS
SELECT *
FROM
    public.reveleer_chases rc where rc.external_chase_id in (
                                                                       '23568568', '25455763', '25457400', '23567785',
                                                                       '23566725', '23566385', '25455710', '23567192',
                                                                       '25456381', '23567542', '23567224', '23566314',
                                                                       '23567757', '23567306', '23568063', '25457040',
                                                                       '25315101', '23566743', '23566860', '25315135',
                                                                       '23566757', '23568620', '23567483', '25456491',
                                                                       '25456639', '23566859', '23566333', '23566315',
                                                                       '23567155', '25458071', '23567027', '25455544',
                                                                       '25315175', '23568803', '25456046', '23568836',
                                                                       '23566913', '23566363', '23568285', '23568904',
                                                                       '23566988', '23566989', '23566494', '23567002',
                                                                       '23567718', '23568390', '23566439', '25315167',
                                                                       '23566403', '23568863', '25457278', '23568188',
                                                                       '25456415', '23568460', '23567962', '23568734',
                                                                       '23568316', '23568522', '23567555', '25456283',
                                                                       '23567812', '23568103', '25315134', '23567156',
                                                                       '25457144', '23568271', '23566445', '23568788',
                                                                       '23568092', '23567176', '25457323', '23566288',
                                                                       '23567331', '25456635', '23568597', '23567133',
                                                                       '23567021', '23567901', '23568523', '23567989',
                                                                       '25457085', '23566603', '25458214', '23568835',
                                                                       '23566583', '25457427', '23566332', '23566423',
                                                                       '23566604', '23567717', '25456720', '25455815',
                                                                       '25457455', '23566444', '23566741', '23568567',
                                                                       '23567456', '23567175', '25456661', '23566616',
                                                                       '25455958', '23568583', '25315084', '23567107',
                                                                       '23567251', '23567164', '23568172', '23567421',
                                                                       '23568794', '23568518', '23567132', '23567020',
                                                                       '23568886', '23567982', '23567191', '23567756',
                                                                       '25455489', '23566574', '23566421', '23567313',
                                                                       '23567312', '25315075', '23568868', '23566396',
                                                                       '23568741', '23566395', '23566446', '23566447',
                                                                       '23566300', '23568414', '23568302', '23568651',
                                                                       '23566716', '23568627', '25458155', '23568746',
                                                                       '23568590', '23568589', '23567420'
    )

------------------------------------------------------------------------------------------------------------------------
/*
 Hey, I need to update the actives and inactives for the MSSP projects.
 I have about 140 chases in the first MSSP project that we had marked as inactive.
 If I provide you those, could tell me
 1) if any of them should be reactivated, and
 2) If any chases we sent for either of the MSSP projects need to be inactivated?
 I think we uploaded the Q4 denominator files from CMS since we sent these, so they've likely changed a little.
This is just for projects 2250 and 2886
 */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS _pqms;
CREATE TEMP TABLE _pqms AS
SELECT c.external_chase_id, unnest(qm_patient_measure_ids) pqm_id
FROM
    _chases c
;
-- now_active
SELECT p.*
FROM
    _pqms p
join fdw_member_doc.qm_patient_measures pm on pm.id = p.pqm_id and pm.is_active
;
DROP TABLE IF EXISTS _pqms_sent;
CREATE TEMP TABLE _pqms_sent AS
SELECT distinct rp.reveleer_id project_id, rc.external_chase_id, unnest(qm_patient_measure_ids) pqm_id
FROM
    reveleer_chases rc
join reveleer_projects rp ON rc.reveleer_project_id = rp.id
join reveleer_chase_file_details rcfd on rc.id = rcfd.reveleer_chase_id and rcfd.reveleer_file_id is not null
where rp.reveleer_id in ('2250', '2886') and rc.yr = 2024;

-- sent_now_inactive
SELECT ps.*
FROM
    _pqms_sent ps
    join fdw_member_doc.qm_patient_measures pm on pm.id = ps.pqm_id and not pm.is_active
;