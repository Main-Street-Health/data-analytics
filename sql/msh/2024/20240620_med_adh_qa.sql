------------------------------------------------------------------------------------------------------------------------
/* This one is mco data based. no ss data, haven't received newer data from the plan*/
------------------------------------------------------------------------------------------------------------------------
1429710
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
    JOIN      fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
--     pm.patient_id =
    pt.id = 1429710
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM sure_scripts_panel_patients WHERE patient_id =810970 order by id desc;
SELECT * FROM fdw_member_doc_stage.qm_pm_med_adh_mco_measures WHERE patient_id =810970;
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 810970 and measure_key ~* 'diabetes';

------------------------------------------------------------------------------------------------------------------------
/* */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , pm.measure_source_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
    JOIN      fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
--     pm.patient_id =
    pt.id = 1428033
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM sure_scripts_panel_patients WHERE patient_id =1010658 order by id desc;
SELECT * FROM fdw_member_doc_stage.qm_pm_med_adh_mco_measures WHERE patient_id =953913;

------------------------------------------------------------------------------------------------------------------------
/* last queried ss on 6/10 and still received the feb fill */
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , pm.measure_source_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
    JOIN      fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
--     pm.patient_id =
    pt.id = 1428033
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM sure_scripts_panel_patients WHERE patient_id =1010658 order by id desc;
SELECT * FROM fdw_member_doc_stage.qm_pm_med_adh_mco_measures WHERE patient_id =1010658;
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 1010658 and measure_key ~* 'med_adherence_cholesterol';


------------------------------------------------------------------------------------------------------------------------
/* feb fill still returned by ss on 6/16*/
------------------------------------------------------------------------------------------------------------------------
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , pm.measure_source_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
    JOIN      fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
--     pm.patient_id =
    pt.id = 1434688
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM sure_scripts_panel_patients WHERE patient_id =197863 order by id desc;
SELECT * FROM fdw_member_doc_stage.qm_pm_med_adh_mco_measures WHERE patient_id =197863;
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 197863 and measure_key ~* 'med_adherence_cholesterol';
------------------------------------------------------------------------------------------------------------------------
/* 6/21 */
------------------------------------------------------------------------------------------------------------------------
3280
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , pm.measure_source_key
  , m.next_fill_date
  , m.adr
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
    JOIN      fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 3280
--     pt.id = 1434688
ORDER BY pm.measure_key, pf.id
;
SELECT * FROM sure_scripts_panel_patients WHERE patient_id =197863 order by id desc;
SELECT * FROM fdw_member_doc_stage.qm_pm_med_adh_mco_measures WHERE patient_id =197863;
SELECT * FROM fdw_member_doc.patient_medication_fills WHERE patient_id = 197863 and measure_key ~* 'med_adherence_cholesterol';

SELECT *
FROM fdw_member_doc.qm_patient_measures WHERE id = 396440 ;
SELECT * FROM fdw_member_doc.qm_pm_med_adh_metrics WHERE patient_measure_id = 396440 ;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
3280
SELECT
    m.patient_id
  , m.patient_measure_id
  , m.measure_key
  , pm.measure_status_key
  , pm.measure_source_key
  , m.next_fill_date
  , m.adr
     , m.ipsd
  , m.pdc_to_date
  , m.measure_source_key
  , wf.id wf_id
  , wf.is_active
  , wf.is_closed
  , wf.is_reopened
  , wf.compliance_check_date
  , pf.id pf_id
  , pf.drug_description
  , pf.order_status
  , pf.medication_status
  , pf.pharmacy_verified_fill_date
  , pf.pharmacy_verified_days_supply
  , pf.system_verified_closed_at
  , pf.inserted_at
  , pf.updated_at
  , pt.status
  , pt.id
FROM
              fdw_member_doc.qm_patient_measures pm
    JOIN      fdw_member_doc.qm_pm_med_adh_metrics m ON pm.id = m.patient_measure_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_wfs wf ON m.id = wf.qm_pm_med_adh_metric_id
    LEFT JOIN fdw_member_doc.qm_pm_med_adh_potential_fills pf ON pf.qm_pm_med_adh_wf_id = wf.id
    LEFT JOIN fdw_member_doc.patient_tasks pt ON pf.patient_task_id = pt.id
    --     LEFT JOIN patient_medication_fills pmf ON pmf.patient_id = pm.patient_id
--         AND pmf.measure_key = pm.measure_key
--         AND DATE_PART('year', pmf.start_date) = pm.operational_year
--    left join qm_pm_med_adh_synth_periods sp on pm.id = sp.patient_measure_id
WHERE
    pm.patient_id = 10435
--     pt.id = 1434688
ORDER BY pm.measure_key, pf.id
;
SELECT *
FROM
    fdw_member_doc_stage.qm_pm_med_adh_mco_measures
WHERE
    patient_id = 10435
and measure_key ~* 'diabetes'
;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS _bad_status;
CREATE TEMP TABLE _bad_status AS
SELECT
    m.measure_key
  , m.patient_id
  , m.id
  , m.measure_year
  , m.measure_source_key
  , 'compliant but status is: ' || pm.measure_status_key reason
  , is_active
  , pm.measure_status_key
  , m.patient_measure_id
  , m.id                                                 metric_id
  , m.next_fill_date
  , m.calc_to_date
  , m.updated_at
  , m.adr
FROM
    fdw_member_doc.qm_pm_med_adh_metrics m
    JOIN fdw_member_doc.qm_patient_measures pm ON m.patient_measure_id = pm.id
        AND pm.is_active
WHERE
      NOT m.is_excluded
  AND m.next_fill_date >= NOW()::DATE + 5
  AND pm.measure_status_key = 'pending_compliance_check'
ORDER BY
    m.patient_measure_id;
create index on _bad_status(patient_measure_id);

SELECT measure_source_key, count(*)
FROM
    _bad_status bs
GROUP BY 1
;

-- all but one have synth to back up
SELECT *
FROM
    _bad_status bs
WHERE
      measure_source_key = 'sure_scripts'
  AND NOT EXISTS( SELECT
                      1
                  FROM
                      fdw_member_doc.qm_pm_med_adh_synth_periods sp
                  WHERE
--                         sp.patient_measure_id = bs.patient_measure_id
                      sp.patient_id = bs.patient_id
                    AND sp.measure_key = bs.measure_key
                    AND sp.end_date + 1 = bs.next_fill_date )
    ;

SELECT *
FROM
    _bad_status bs
        join fdw_member_doc.qm_pm_status_periods sp on sp.patient_measure_id = bs.patient_measure_id
-- join fdw_member_doc.qm_pm_med_adh_wfs wf on wf.patient_measure_id = bs.patient_measure_id
DROP TABLE IF EXISTS _bad_status;


------------------------------------------------------------------------------------------------------------------------
/*  */
-----------------------------------------------------------------------------------------------------------------------
SELECT pm.id, pm.patient_id, pm.measure_key, pm.measure_source_key, pm.measure_status_key, m.ipsd, m.fill_count, m.next_fill_date
FROM
    fdw_member_doc.qm_patient_measures pm
join fdw_member_doc.qm_pm_med_adh_metrics m on m.patient_measure_id = pm.id
where pm.patient_id in ( 472877,1404,2944,6117,6183,7388,8244,9468,9671,10435,11593,13464,13921,19155,20059,21540,26892,28626,31058,32249,34304,35631,36791,37193,37310,37670,37771,37901,38046,38076,38669,38831,43879,46339,46980,47109,47183 )
and pm.measure_key = 'med_adherence_cholesterol'
;
SELECT name, ftp_inbound_path, creds_hash, * FROM fdw_file_router.ftp_servers WHERE entity_key ~* 'bcbsar';
SELECT
    patient_id
  , is_med_adh_mah_convertable_to_90_days
  , is_med_adh_mad_convertable_to_90_days
  , is_med_adh_mac_convertable_to_90_days
FROM
    fdw_member_doc.cache_register
WHERE
     is_med_adh_mah_convertable_to_90_days
  OR is_med_adh_mad_convertable_to_90_days
  OR is_med_adh_mac_convertable_to_90_days;

