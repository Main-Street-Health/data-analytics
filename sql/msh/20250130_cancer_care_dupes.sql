SELECT *
FROM
    cc_patients cc
WHERE
    cc.id in (1586, 1587)
--     first_name ~* 'Rosea';
;
1586 -- keep
1587 -- del

SELECT * FROM cc_patient_referrals where cc_patient_id in (1586, 1587);
SELECT * FROM cc_patient_tasks where cc_patient_id in (1586, 1587);
SELECT * FROM cc_patient_task_activities where cc_patient_task_id IN (3897, 5380);

SELECT *
FROM
    cc_coop_documents ;

------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
begin;
end;
ROLLBACK ;
WITH
    pat                AS ( SELECT 1587 AS id )
  , refferal           AS ( SELECT
                                id
                            FROM
                                cc_patient_referrals
                            WHERE
                                cc_patient_id IN ( SELECT id FROM pat ) )
  , calls              AS (
    DELETE
        FROM
            cc_calls c
            WHERE c.cc_patient_referral_id IN ( SELECT id FROM refferal ) )

  , tasks              AS ( SELECT id FROM cc_patient_tasks pt WHERE pt.cc_patient_id IN ( SELECT id FROM pat ) )

  , task_act           AS (
    DELETE FROM cc_patient_task_activities a
        WHERE a.cc_patient_task_id IN ( SELECT id FROM tasks ) )
  , task_del           AS (
    DELETE FROM cc_patient_tasks pt WHERE id IN ( SELECT id FROM tasks ) )

  , ref_act            AS (
    DELETE FROM cc_referral_activities a
        WHERE a.cc_patient_referral_id IN ( SELECT id FROM refferal ) )
  , nav_blockas        AS (
    DELETE FROM cc_referral_nav_blocks nb WHERE nb.cc_patient_referral_id IN ( SELECT id FROM refferal ) )
  , ref_status_periods AS (
    DELETE FROM cc_referral_status_periods sp WHERE sp.cc_patient_referral_id IN ( SELECT id FROM refferal ) )
  , wf                 AS (
    DELETE FROM cc_referral_wfs wf WHERE wf.cc_patient_referral_id IN ( SELECT id FROM refferal ) )
  , ref_del            AS (
    DELETE
        FROM
            cc_patient_referrals
            WHERE
                id = ( SELECT id FROM refferal ) )
DELETE
FROM
    cc_patients p
WHERE
    p.id IN ( SELECT id FROM pat );




