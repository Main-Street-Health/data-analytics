alter table public.reveleer_projects add measures_to_send text[] default '{}';


SELECT *
FROM
    public.reveleer_projects;


select * from fdw_member_doc.quality_measures qm
where   qm.name IN (
                        'Breast Cancer Screening',
                        'Colorectal Screening',
                        'Controlling High Blood Pressure < 140/90',
                        'Eye Exam for Patients with Diabetes',
                        'Hemoglobin A1c Control for Patients With Diabetes',
                        'Kidney Health Evaluation for Patients with Diabetes',
                        'Osteoporosis Management', -- Note: none found for time range
                        'Care for Older Adult: Pain Assessment',
                        'Care for Older Adult: Medication Review',
                        'Care for Older Adult: Functional Status Assessment'
        )
;


Wellcare/Centene
BCS
CBP
COA - Pain Assessment
COA - Medication Review
EED
HBD
COL
OMW

WITH
    qms AS ( SELECT
                 ARRAY_AGG(name) names
             FROM
                 ( VALUES
                       ('Breast Cancer Screening'),
                       ('Colorectal Screening'),
                       ('Controlling High Blood Pressure < 140/90'),
                       ('Eye Exam for Patients with Diabetes'),
                       ('Hemoglobin A1c Control for Patients With Diabetes') ,
--                        ('Kidney Health Evaluation for Patients with Diabetes'),
                       ('Osteoporosis Management'),
                       ('Care for Older Adult: Pain Assessment'),
                       ('Care for Older Adult: Medication Review')
--                        ('Care for Older Adult: Functional Status Assessment')
                 ) x(name) )
UPDATE reveleer_projects rp
SET
    measures_to_send = names
FROM
    qms
WHERE
    rp.payer_id = 49
;



   SELECT *
   FROM
       fdw_member_doc.payers where name ~* 'wellca';



    SELECT DISTINCT
        p.name
      , msp.state
      , qm.code
    , ptr.*
    FROM
        fdw_member_doc.msh_state_payers msp
        JOIN fdw_member_doc.payers p ON p.id = msp.payer_id
        JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
            AND qmc.is_contracted -- this SHOULD be reflected in should_display already, but this is safer
        JOIN fdw_member_doc.quality_measures qm ON qm.id = qmc.measure_id
        JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    WHERE
            ptr.name IN
            ('anthem_ky', 'anthem_tn', 'bcbst', 'centene_tn', 'humana_al', 'humana_tn', 'uhc_al', 'uhc_ar', 'uhc_tn',
             'uhc_wv')
      AND   measure_year = 2023
    ORDER BY
        1, 2, 3;
