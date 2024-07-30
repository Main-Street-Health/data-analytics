------------- patient base list
SELECT *
FROM
    _pats;
DROP TABLE IF EXISTS _pats;
CREATE TEMP TABLE _pats AS
WITH
    _patient_base AS ( SELECT DISTINCT ON (mbi) ---- dedup patients with same MBI, maybe add dob?
                                                patient_id
                         ,                      mbi
                         ,                      patient_name
                         ,                      patient_market
                         ,                      date_of_birth
                         ,                      primary_referring_partner_name
                         ,                      organization_name
                         ,                      patient_attribution_status
                                                ---attribution_substatus,
                         ,                      md_primary_physician_name
                         ,                      payer_name
                         ,                      coverage_source
                       FROM
                           fdw_member_doc_ent.patients
                       WHERE
                             (patient_attribution_status ~* 'ma_at_risk' OR
                              patient_attribution_status = 'om_at_risk_yes')
                         AND substatus != 'deceased'
                       UNION
                       SELECT
                           patient_id
                         , mbi
                         , patient_name
                         , patient_market
                         , date_of_birth
                         , primary_referring_partner_name
                         , organization_name
                         , patient_attribution_status
                           ---attribution_substatus,
                         , md_primary_physician_name
                         , payer_name
                         , coverage_source
                       FROM
                           fdw_member_doc_ent.patients
                       WHERE
                             (patient_attribution_status ~* 'ma_at_risk' OR
                              patient_attribution_status = 'om_at_risk_yes')
                         AND substatus != 'deceased'
                         AND mbi IS NULL )
  , _patient      AS ( SELECT
                           pb.patient_id
                         , MAX( rfrnc_yr) AS coverage_year
                         , 'dpc'             AS SOURCE
                       FROM
                           _patient_base pb
                           LEFT JOIN dpoc_coverage sc ON pb.patient_id = sc.patient::BIGINT
                       GROUP BY
                           1 )
  ,
----------Get patient ever sent DPC
    DPC_history   AS ( SELECT
                           u.mbi
                         , dpc_history
                         , MAX( id) FILTER ( WHERE ROW = 1 ) AS dpoc_id
                         , MAX( inserted_at) FILTER ( WHERE ROW = 1 ) AS recent_inserted_at
                       FROM
                           ( SELECT
                                 p.mbi
                               , TRUE                                                       AS DPC_history
                               , dp.npi
                               , dp.first_name
                               , dp.last_name
                               , j.inserted_at:: DATE
                               , j.id
                               , ROW_NUMBER( ) OVER ( PARTITION BY p.mbi ORDER BY p.mbi, j.inserted_at DESC) AS ROW
                             FROM
                                 PUBLIC.dpoc_bulk_export_jobs j
                                 JOIN dpoc_practitioners dp ON j.dpoc_practitioner_id = dp.id
                                 JOIN dpoc_practitioner_group_patients gp ON gp.npi = dp.npi
                                 JOIN fdw_member_doc_ent.patients p ON p.mbi = gp.mbi
                                 JOIN fdw_member_doc.msh_physicians doc ON doc.npi::TEXT = dp.npi ) u
                       GROUP BY
                           1, 2 )
  ,
------------Get recent DPC from patient DPC history
    recent_base   AS ( SELECT
                           j.mbi
                         , MAX( je.id) IS NOT NULL       has_dpc_error
                         , STRING_AGG( DISTINCT je.error, ', ') errors
                       FROM
                           DPC_history j
                           LEFT JOIN dpoc_bulk_export_job_errors je
                                     ON je.dpoc_bulk_export_job_id = j.dpoc_id
                                         AND j.mbi = je.mbi
                       GROUP BY
                           1 )
  , FINAL         AS ( SELECT
                           pb.*
                         , p.coverage_year
                         , p.source
                         , CASE WHEN p.source IS NULL THEN pb.coverage_source ELSE p.source END AS patient_cov
                         , dh.DPC_history
                         ,
                           --- dh.npi,
                           ---concat(dh.first_name,' ',dh.last_name) as practitioner_name,
                           rb.has_dpc_error
                         , rb.errors
                       FROM
                           _patient_base pb
                           LEFT JOIN _patient p ON pb.patient_id = p.patient_id
                           LEFT JOIN DPC_history Dh ON pb.mbi = Dh.mbi
                           LEFT JOIN recent_base rb ON Dh.mbi = rb.mbi )
SELECT *
FROM
    final
WHERE
      mbi IS NOT NULL
  AND DPC_history IS NULL;
