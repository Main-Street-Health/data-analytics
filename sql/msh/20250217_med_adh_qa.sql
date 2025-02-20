SELECT * FROM patient_tasks WHERE id = 2891319;
SELECT * FROM patient_sure_scripts_panels WHERE patient_id = 595607;
SELECT *
FROM
    qm_pm_med_adh_metrics
WHERE
      patient_id = 595607
  AND measure_key = 'med_adherence_cholesterol'
  AND measure_year = 2025;


 -- last queried on 2/6
 -- ipsd = 1/11
 -- nfd = 2/10

 -- 2/10 pdc = 100
 -- 2/11 pdc = 30/31 = 97 -- priority low
 -- 2/12 pdc = 30/32 = 94 -- priority low
 -- 2/13 pdc = 30/33 = 91 -- priority low
 -- 2/14 pdc = 30/34 = 88 -- priority low

 -- sat 2/15 pdc = 30/35 -- priority med (but not calced)
 -- sun 2/16 pdc = 30/36 -- priority low (but not calced)

 -- mon 2/17 pdc = 30/37 = 81 -- priority high

-- when we submit the panel the patient looks low priority because priority is calced every time we process new data and isn't submitted
-- when the data comes back we process and the patient is now high priority and a task is generated
 
 ------------------------------------------------------------------------------------------------------------------------
 /* https://github.com/Main-Street-Health/member-doc/issues/16253 */
 ------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    public.msh_external_emr_diagnoses xdx
    JOIN public.icd10s i ON xdx.icd10_id = i.id
WHERE
      patient_id = 1359961
  AND i.code_formatted = 'N18.31';

SELECT *
FROM
    member_doc.stage.msh_md_portal_suspects_history
WHERE
      golgi_patient_id = 1359961
  AND icd_10_code = 'N18.31';

SELECT *
FROM
    member_doc.stage.msh_md_portal_suspects_history
WHERE
      golgi_patient_id = 820029
  AND icd_10_code = 'N18.32';


SELECT *
FROM
    member_doc.stage.msh_md_portal_suspects_history
WHERE
      golgi_patient_id = 964508 and icd_10_code =  'N18.31'

 -- out of date range
SELECT s.source_date, chs.*
FROM
    member_doc.stage.msh_md_portal_suspects_history s
join public.hcc_icd10s hi on hi.icd10_id = s.icd10_id and hi.is_dominant and hi.yr = date_part('year', current_date)
            join public.hccs hc on hc.id = hi.hcc_id
            join stage.cca_hcc_suspect_months_look_back chs on chs.hcc_number = hc.number
                                                           and chs.cms_model = hc.cms_model
WHERE
      golgi_patient_id = 564680 and icd_10_code =  'N18.31'
    ;

-- not seeing any of these codes.
SELECT *
FROM
    member_doc.stage.msh_md_portal_suspects_history
WHERE
      golgi_patient_id = 217043 and icd_10_code in  ('F17.200', 'I25.2', 'R07.9')

-- all the codes are source emr
SELECT *
FROM
    msh_external_emr_diagnoses xdx
join icd10s i on xdx.icd10_id = i.id
WHERE
      patient_id = 217043 and i.code_formatted in  ('F17.200', 'I25.2', 'R07.9')
-- call stage._process_md_portals_proc()


-- has source date and in range
SELECT
--     s.source_date, chs.*
s.*

FROM
    member_doc.stage.msh_md_portal_suspects_history s
    JOIN public.hcc_icd10s hi ON s.icd10_id = hi.icd10_id AND hi.yr = DATE_PART('year', NOW()) AND hi.is_dominant --_yr
    JOIN public.hccs hcc ON hi.hcc_id = hcc.id
    JOIN stage.cca_hcc_suspect_months_look_back chs ON chs.hcc_number = hcc.number
        AND chs.cms_model = hcc.cms_model
        AND COALESCE(s.source_date, NOW()) > NOW() - INTERVAL '1 month' * chs.months_look_back
WHERE
      golgi_patient_id = 562302
--   AND icd_10_code = 'N18.31'
and source_fact_name != 'Race'
;

SELECT i.code_formatted, xdx.*
FROM
    public.msh_external_emr_diagnoses xdx
    JOIN public.icd10s i ON xdx.icd10_id = i.id
WHERE
      patient_id = 562302
  AND cms_contract_year = 2025
  AND diagnosis_type = 'suspect'
and source = 'md_portal'
;

--   AND i.code_formatted = 'N18.31';


SELECT *
FROM
    patients
WHERE
--     last_name LIKE 'Yancey' AND first_name LIKE 'Carol'
    id = 562302;


