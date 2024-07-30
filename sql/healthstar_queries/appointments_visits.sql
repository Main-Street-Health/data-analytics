SELECT
    +right(P.NUMBER_PRIMARY,4) as `region`,
    PG.code as 'member_program',
    `P`.`NUMBER_PRIMARY` AS `member_id`,
    `P`.`FIRSTNAME` AS `first_name`,
    `P`.`LASTNAME` AS `last_name`,
    `P`.`ssn` AS `ssn`,
    `P`.`choices_group` AS `choices_group`,
    `P`.`enrollment_date` AS `enrollment_date`,
    SC.title AS `service_description`,
    A.ID authorization_id,
    A.REF_NO auth_ref_no,
    A.HCPCS auth_procedure_code,
    A.MODIFIERS auth_modifiers,
    A.PATID healthstar_patient_id,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(APP.START_DATE), 'GMT', APP.locale), "%m-%d-%Y") AS `service_date`,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(APP.STARTTIME), 'GMT', APP.locale), "%m-%d-%Y %H:%i:%s") AS  `start_time`,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(APP.ENDTIME), 'GMT', APP.locale), "%m-%d-%Y %H:%i:%s") AS  `end_time`,
    CASE
        WHEN (APP.GPS_STARTTIME = 0 OR (APP.GPS_STARTTIME >= APP.MISSEDTIME)) THEN 'Missed'
        WHEN (APP.GPS_STARTTIME >= APP.LATETIME) THEN 'Late'
        WHEN (APP.GPS_STARTTIME < APP.LATETIME AND APP.GPS_STARTTIME > 0) THEN 'On-time'
    END AS `visit_status`,
    IF(APP.GPS_STARTTIME = 0, NULL, CONVERT_TZ(FROM_UNIXTIME(APP.GPS_STARTTIME), 'GMT', APP.locale)) AS `check_in_time`,
    IF(APP.GPS_ENDTIME = 0, NULL, CONVERT_TZ(FROM_UNIXTIME(APP.GPS_ENDTIME), 'GMT', APP.locale)) AS `check_out_time`,
    APP.APPID AS `appointment_id`,
    CASE
        WHEN SRC.SUBTYPE IS NULL AND SRC3.SUBTYPE IS NOT NULL THEN SRC3.SUBTYPE
        WHEN SRC.SUBTYPE IS NULL AND SRC2.SUBTYPE IS NOT NULL THEN SRC2.SUBTYPE
        WHEN SRC.SUBTYPE IS NULL THEN 'Provider Initiated'
        WHEN SRC.SUBTYPE IS NOT NULL THEN SRC.SUBTYPE
    END as `reason_code_type`,
    CASE
        WHEN SRC.TITLE IS NULL AND SRC3.TITLE IS NOT NULL THEN SRC3.TITLE
        WHEN SRC.TITLE IS NULL AND SRC2.TITLE IS NOT NULL THEN SRC2.TITLE
        WHEN SRC.TITLE IS NULL THEN 'Staff Scheduling Issue'
        WHEN SRC.TITLE IS NOT NULL THEN SRC.TITLE
    END as `reason_code_title`,
    APP.deviated_from,
    CASE
    WHEN RC.TITLE IS NULL AND RC2.TITLE IS NOT NULL THEN RC2.TITLE
    WHEN RC.title IS NOT NULL THEN RC.title
    END as "system_reason_code_title",
    APP.RESOLUTION_STATUS AS `resolution_status`,
    lmv.reason as `late_missed_details`,
    lmv.follow_up_actions as `follow_up_details`,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(APP.MADE_TIME), 'GMT', APP.locale), "%Y-%m-%d") AS `created_date`,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(A.created_at), 'GMT', APP.locale), "%Y-%m-%d") as `auth_added_date`,
    CASE
      WHEN APP.deviated_to is not null and APP.deviated_from and APP.prev_gps_endtime != 0 THEN CONCAT('Deviated To: ', APP.deviated_to, ', Deviated From: ', APP.deviated_from, ', Manually Confirmed')
      WHEN APP.deviated_to is not null and APP.deviated_from THEN CONCAT('Deviated To: ', APP.deviated_to, ', Deviated From: ', APP.deviated_from)
      WHEN APP.deviated_to is not null THEN CONCAT('Deviated To: ', APP.deviated_to)
      WHEN APP.deviated_from is not null THEN CONCAT('Deviated From: ', APP.deviated_from)
      WHEN APP.prev_gps_endtime != 0 THEN 'Manually Confirmed'
    END as 'note'
FROM
       APPOINTMENTS APP
  JOIN PATIENTS AS P ON P.PATID = APP.PATID
  JOIN AUTHORIZATIONS AS A ON A.REF_NO = APP.AUTH_REF_NO
  LEFT JOIN REASON_CODES AS RC ON APP.EMP_REASON_CODE = RC.RCID
  LEFT JOIN STATE_REASON_CODES AS SRC ON RC.state_reason_code = SRC.RCID
  LEFT JOIN DEVIATION_REQUESTS DR on DR.app_id = APP.APPID and DR.status = 'Approved'
  LEFT JOIN REASON_CODES RC2 on RC2.RCID = DR.reason_id
  LEFT JOIN STATE_REASON_CODES AS SRC2 ON RC2.state_reason_code = SRC2.RCID
  LEFT JOIN t_hcpcs AS SC ON SC.nnumber = A.HCPCS
  LEFT JOIN providers AS S ON S.SITEID = APP.provider_id
  LEFT JOIN P_USERS AS E ON E.USERID = APP.USERID
  LEFT JOIN programs AS PG ON PG.id = A.program_id
  LEFT JOIN lmv_resolution lmv ON APP.APPID = lmv.appointment_id
  LEFT JOIN MAN_CONF_REQS AS MCR ON MCR.appointment_id = APP.APPID
  LEFT JOIN REASON_CODES rc3 ON rc3.RCID = MCR.reason_code_id
  LEFT JOIN STATE_REASON_CODES AS SRC3 ON rc3.mc_state_reason_code_id = SRC3.RCID
WHERE
  CAST(CONVERT_TZ(FROM_UNIXTIME(APP.STARTTIME), 'GMT', APP.locale) AS DATETIME) BETWEEN CAST("2017-07-01" AS DATETIME) AND CAST("2020-06-30" AS DATETIME)
  AND APP.DDELETE = 0
  AND APP.DEVIATED_TO IS NULL
  -- AND APP.deviated_from is not null
  AND IFNULL((
    SELECT
        PS.`status`
    FROM
        PATIENT_STATUS PS
        LEFT JOIN patient_status_hcpcs psh ON psh.patient_status_id = PS.STATUSID
        LEFT JOIN t_hcpcs h ON h.HCID = psh.hcpcs_id
    WHERE
        PS.PATID = A.`PATID`
        AND PS.`ignore` = 0
        AND (
                (APP.STARTTIME > (UNIX_TIMESTAMP(PS.`BEGIN_DATE`) + (60 * 60 * 24)) AND APP.STARTTIME < (UNIX_TIMESTAMP(PS.`END_DATE`) - (60 * 60 * 24)))
                OR ((APP.STARTTIME > (UNIX_TIMESTAMP(PS.`BEGIN_DATE`) + (60 * 60 * 24))) AND (PS.`END_DATE` IS NULL) AND (PS.`BEGIN_DATE` < NOW()))
                OR (PS.`BEGIN_DATE` IS NULL AND APP.STARTTIME < (UNIX_TIMESTAMP(PS.`END_DATE`) - (60 * 60 * 24)) AND (PS.`END_DATE` < NOW()))
                OR (PS.`BEGIN_DATE` IS NULL AND PS.`END_DATE` IS NULL)
            )
        AND (A.HCPCS = h.nnumber OR PS.hold_all_hcpcs = 1)
        ORDER BY PS.BEGIN_DATE DESC
        LIMIT 1
    ), "Active") = 'Active';


/* -- OLD QUERY
SELECT
    +right(P.NUMBER_PRIMARY,4) as `region`,
    PG.code as 'member_program',
    `P`.`NUMBER_PRIMARY` AS `member_id`,
    `P`.`FIRSTNAME` AS `first_name`,
    `P`.`LASTNAME` AS `last_name`,
    `P`.`ssn` AS `ssn`,
    `P`.`choices_group` AS `choices_group`,
    `P`.`enrollment_date` AS `enrollment_date`,
    SC.title AS `service_description`,
    A.ID authorization_id,
    A.REF_NO auth_ref_no,
    A.HCPCS auth_procedure_code,
    A.MODIFIERS auth_modifiers,
    A.PATID healthstar_patient_id,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(APP.START_DATE), 'GMT', APP.locale), "%m-%d-%Y") AS `service_date`,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(APP.STARTTIME), 'GMT', APP.locale), "%m-%d-%Y %H:%i:%s") AS  `start_time`,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(APP.ENDTIME), 'GMT', APP.locale), "%m-%d-%Y %H:%i:%s") AS  `end_time`,
    CASE
        WHEN (APP.GPS_STARTTIME = 0 OR (APP.GPS_STARTTIME >= APP.MISSEDTIME)) THEN 'Missed'
        WHEN (APP.GPS_STARTTIME >= APP.LATETIME) THEN 'Late'
        WHEN (APP.GPS_STARTTIME < APP.LATETIME AND APP.GPS_STARTTIME > 0) THEN 'On-time'
    END AS `visit_status`,
    IF(APP.GPS_STARTTIME = 0, NULL, CONVERT_TZ(FROM_UNIXTIME(APP.GPS_STARTTIME), 'GMT', APP.locale)) AS `check_in_time`,
    IF(APP.GPS_ENDTIME = 0, NULL, CONVERT_TZ(FROM_UNIXTIME(APP.GPS_ENDTIME), 'GMT', APP.locale)) AS `check_out_time`,
    APP.APPID AS `appointment_id`,
    CASE
        WHEN SRC.SUBTYPE IS NULL AND SRC3.SUBTYPE IS NOT NULL THEN SRC3.SUBTYPE
        WHEN SRC.SUBTYPE IS NULL AND SRC2.SUBTYPE IS NOT NULL THEN SRC2.SUBTYPE
        WHEN SRC.SUBTYPE IS NULL THEN 'Provider Initiated'
        WHEN SRC.SUBTYPE IS NOT NULL THEN SRC.SUBTYPE
    END as `reason_code_type`,
    CASE
        WHEN SRC.TITLE IS NULL AND SRC3.TITLE IS NOT NULL THEN SRC3.TITLE
        WHEN SRC.TITLE IS NULL AND SRC2.TITLE IS NOT NULL THEN SRC2.TITLE
        WHEN SRC.TITLE IS NULL THEN 'Staff Scheduling Issue'
        WHEN SRC.TITLE IS NOT NULL THEN SRC.TITLE
    END as `reason_code_title`,
    APP.deviated_from,
    CASE
    WHEN RC.TITLE IS NULL AND RC2.TITLE IS NOT NULL THEN RC2.TITLE
    WHEN RC.title IS NOT NULL THEN RC.title
    END as "system_reason_code_title",
    APP.RESOLUTION_STATUS AS `resolution_status`,
    lmv.reason as `late_missed_details`,
    lmv.follow_up_actions as `follow_up_details`,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(APP.MADE_TIME), 'GMT', APP.locale), "%Y-%m-%d") AS `created_date`,
    DATE_FORMAT(CONVERT_TZ(FROM_UNIXTIME(A.created_at), 'GMT', APP.locale), "%Y-%m-%d") as `auth_added_date`,
    CASE
      WHEN APP.deviated_to is not null and APP.deviated_from and APP.prev_gps_endtime != 0 THEN CONCAT('Deviated To: ', APP.deviated_to, ', Deviated From: ', APP.deviated_from, ', Manually Confirmed')
      WHEN APP.deviated_to is not null and APP.deviated_from THEN CONCAT('Deviated To: ', APP.deviated_to, ', Deviated From: ', APP.deviated_from)
      WHEN APP.deviated_to is not null THEN CONCAT('Deviated To: ', APP.deviated_to)
      WHEN APP.deviated_from is not null THEN CONCAT('Deviated From: ', APP.deviated_from)
      WHEN APP.prev_gps_endtime != 0 THEN 'Manually Confirmed'
    END as 'note'
FROM
       APPOINTMENTS APP
  JOIN PATIENTS AS P ON P.PATID = APP.PATID
  JOIN AUTHORIZATIONS AS A ON A.REF_NO = APP.AUTH_REF_NO
  LEFT JOIN DEVIATION_REQUESTS DR on DR.app_id = APP.APPID and DR.status = 'Approved'

  LEFT JOIN t_hcpcs AS SC ON SC.nnumber = A.HCPCS
  LEFT JOIN providers AS S ON S.SITEID = APP.provider_id
  LEFT JOIN P_USERS AS E ON E.USERID = APP.USERID
  LEFT JOIN programs AS PG ON PG.id = A.program_id
  LEFT JOIN lmv_resolution lmv ON APP.APPID = lmv.appointment_id
  LEFT JOIN MAN_CONF_REQS AS MCR ON MCR.appointment_id = APP.APPID

  LEFT JOIN REASON_CODES AS RC ON RC.RCID = APP.EMP_REASON_CODE
  LEFT JOIN STATE_REASON_CODES AS SRC ON RC.state_reason_code = SRC.RCID

  LEFT JOIN REASON_CODES RC2 on RC2.RCID = DR.reason_id
  LEFT JOIN STATE_REASON_CODES AS SRC2 ON RC2.state_reason_code = SRC2.RCID

  LEFT JOIN REASON_CODES rc3 ON rc3.RCID = MCR.reason_code_id
  LEFT JOIN STATE_REASON_CODES AS SRC3 ON rc3.state_reason_code_id = SRC3.RCID
WHERE
  CAST(CONVERT_TZ(FROM_UNIXTIME(APP.STARTTIME), 'GMT', APP.locale) AS DATETIME) BETWEEN CAST("2017-07-01" AS DATETIME) AND CAST("2020-06-30" AS DATETIME)
  AND APP.DDELETE = 0
  AND APP.DEVIATED_TO IS NULL
  -- AND APP.deviated_from is not null
  AND IFNULL((
    SELECT
        PS.`status`
    FROM
        PATIENT_STATUS PS
        LEFT JOIN patient_status_hcpcs psh ON psh.patient_status_id = PS.STATUSID
        LEFT JOIN t_hcpcs h ON h.HCID = psh.hcpcs_id
    WHERE
        PS.PATID = A.`PATID`
        AND PS.`ignore` = 0
        AND (
                (APP.STARTTIME > (UNIX_TIMESTAMP(PS.`BEGIN_DATE`) + (60 * 60 * 24)) AND APP.STARTTIME < (UNIX_TIMESTAMP(PS.`END_DATE`) - (60 * 60 * 24)))
                OR ((APP.STARTTIME > (UNIX_TIMESTAMP(PS.`BEGIN_DATE`) + (60 * 60 * 24))) AND (PS.`END_DATE` IS NULL) AND (PS.`BEGIN_DATE` < NOW()))
                OR (PS.`BEGIN_DATE` IS NULL AND APP.STARTTIME < (UNIX_TIMESTAMP(PS.`END_DATE`) - (60 * 60 * 24)) AND (PS.`END_DATE` < NOW()))
                OR (PS.`BEGIN_DATE` IS NULL AND PS.`END_DATE` IS NULL)
            )
        AND (A.HCPCS = h.nnumber OR PS.hold_all_hcpcs = 1)
        ORDER BY PS.BEGIN_DATE DESC
        LIMIT 1
    ), "Active") = 'Active';
/*