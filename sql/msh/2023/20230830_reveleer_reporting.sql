WITH
    pat_measures AS ( SELECT
                          pqm.patient_id
                        , pqm.id         patient_quality_measure_id
                        , state_pay.name measure_source_payer
                        , msp.state      payer_state
                        , msp.id         state_payer_id
                        , qm.measure_code
                        , qm.name        measure_name
                        , pqm.status     measure_status
                      FROM
                          fdw_member_doc.patient_quality_measures pqm
                          JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
                          JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
                              AND qmc.measure_id = pqm.measure_id
                              AND qmc.measure_year = pqm.year
                              AND
                                                                            qmc.is_contracted -- this SHOULD be reflected in should_display already, but this is safer
                          JOIN fdw_member_doc.quality_measures qm ON qm.id = qmc.measure_id AND qm.is_reveleer
                          JOIN fdw_member_doc.supreme_pizza sp
                               ON sp.patient_id = pqm.patient_id AND sp.is_quality_measures
                          JOIN fdw_member_doc.layer_cake_patients lcp
                               ON lcp.patient_id = sp.patient_id AND lcp.is_quality_measures
                          JOIN fdw_member_doc.payers state_pay ON state_pay.id = msp.payer_id
                      WHERE
                            pqm.year = DATE_PART('year', NOW())
                        AND pqm.status IN ('closed_pending', 'in_progress', 'open', 'ordered', 'recommended', 'refused')
                        AND pqm.source IN ('mco', 'mco_fall_off')
                        AND pqm.should_display
                        AND qm.is_reveleer
                        AND sp.is_quality_measures )
  , sent_to_rev  AS ( SELECT DISTINCT chase_id FROM reveleer_chase_file_details cfd )
SELECT
    pm.patient_id
  , pm.patient_quality_measure_id
  , pm.measure_source_payer
  , pm.payer_state
  , pm.measure_code
  , pm.measure_name
  , pm.measure_status
  , str.chase_id IS NOT NULL sent_to_reveleer
FROM
    pat_measures pm
    left JOIN public.reveleer_projects ptr
         ON pm.state_payer_id = ptr.state_payer_id AND pm.measure_name = ANY (ptr.measures_to_send)
    LEFT JOIN sent_to_rev str ON str.chase_id = pm.patient_quality_measure_id
;

WITH
    pat_measures AS ( SELECT
                          pqm.patient_id
                        , pqm.id         patient_quality_measure_id
                        , state_pay.name measure_source_payer
                        , msp.state      payer_state
                        , ptr.name       reveleer_project
                        , qm.measure_code
                        , qm.name        measure_name
                        , pqm.status     measure_status
                        , mpqm.substatus measure_substatus
                      FROM
                          fdw_member_doc.patient_quality_measures pqm
                          JOIN fdw_member_doc.msh_patient_quality_measures mpqm on pqm.id = mpqm.patient_quality_measure_id
                          JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
                          JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
                              AND qmc.measure_id = pqm.measure_id
                              AND qmc.measure_year = pqm.year
                              AND qmc.is_contracted -- this SHOULD be reflected in should_display already, but this is safer
                          JOIN fdw_member_doc.quality_measures qm ON qm.id = qmc.measure_id AND qm.is_reveleer
                          JOIN fdw_member_doc.supreme_pizza sp
                               ON sp.patient_id = pqm.patient_id AND sp.is_quality_measures
                          JOIN fdw_member_doc.layer_cake_patients lcp
                               ON lcp.patient_id = sp.patient_id AND lcp.is_quality_measures
                          JOIN public.reveleer_projects ptr
                               ON msp.id = ptr.state_payer_id AND qm.name = ANY (ptr.measures_to_send)
                          JOIN fdw_member_doc.payers state_pay ON state_pay.id = msp.payer_id
                      WHERE
                            pqm.year = DATE_PART('year', NOW())
                        AND pqm.status IN ('closed_pending', 'in_progress', 'open', 'ordered', 'recommended', 'refused')
                        AND pqm.source IN ('mco', 'mco_fall_off')
                        AND pqm.should_display
                        AND qm.is_reveleer
                        AND sp.is_quality_measures )
  , sent_to_rev  AS ( SELECT DISTINCT on (chase_id) chase_id, inserted_at::date sent_date FROM reveleer_chase_file_details cfd order by cfd.chase_id, cfd.inserted_at)
SELECT
    pm.patient_id
  , pm.patient_quality_measure_id
  , pm.measure_source_payer
  , pm.payer_state
  , pm.reveleer_project
  , pm.measure_code
  , pm.measure_name
  , pm.measure_status
  , pm.measure_substatus
  , str.chase_id IS NOT NULL sent_to_reveleer
  , str.sent_date
FROM
    pat_measures pm
    LEFT JOIN sent_to_rev str ON str.chase_id = pm.patient_quality_measure_id;


         ON pm.state_payer_id = ptr.state_payer_id AND pm.measure_name = ANY (ptr.measures_to_send)

