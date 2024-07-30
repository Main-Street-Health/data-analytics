DROP TABLE IF EXISTS _med_claim_matches;
CREATE TEMP TABLE _med_claim_matches AS
SELECT distinct
    mc."MemberId" sent_member
  , p.memberid received_member
FROM
    bk_up.dpc_milliman_med_claims mc
    LEFT JOIN raw.milliman_plan_compare_providers p ON p.memberid::BIGINT = mc."MemberId" AND p.inbound_file_id = 20
WHERE
      mc.pulled_at::DATE = '2023-08-16'
;
SELECT count(*) FROM _med_claim_matches;

SELECT
    rp.id                              rp_id
  , rp.name                            rp_name
  , COUNT(DISTINCT mc.sent_member)     sent
  , COUNT(DISTINCT mc.received_member) received
FROM
    _med_claim_matches mc
    JOIN fdw_member_doc.patient_referring_partners prp ON prp.patient_id = mc.sent_member AND prp."primary"
    JOIN fdw_member_doc.referring_partners rp ON rp.id = prp.referring_partner_id and rp.id in (188, 225, 227, 275, 274, 273, 276)
GROUP BY
    1, 2
;


DROP TABLE IF EXISTS _rx_claims;
CREATE TEMP TABLE _rx_claims AS
SELECT distinct
    mc."MemberId" sent_member
  , p.memberid received_member
FROM
    bk_up.dpc_milliman_rx_claims mc
    LEFT JOIN raw.milliman_plan_compare_detailed_drug_cost p ON p.memberid::BIGINT = mc."MemberId" AND p.inbound_file_id = 20
    JOIN fdw_member_doc.patient_referring_partners prp ON prp.patient_id = mc."MemberId" AND prp."primary"
    JOIN fdw_member_doc.referring_partners rp ON rp.id = prp.referring_partner_id
WHERE
      mc.pulled_at::DATE = '2023-08-16'
;

SELECT
    rp.id                              rp_id
  , rp.name                            rp_name
  , COUNT(DISTINCT mc.sent_member)     sent
  , COUNT(DISTINCT mc.received_member) received
FROM
    _rx_claims mc
    JOIN fdw_member_doc.patient_referring_partners prp ON prp.patient_id = mc.sent_member AND prp."primary"
    JOIN fdw_member_doc.referring_partners rp ON rp.id = prp.referring_partner_id  and rp.id in (188, 225, 227, 275, 274, 273, 276)
GROUP BY
    1, 2
;


DROP TABLE IF EXISTS _dpc;
CREATE TEMP TABLE _dpc AS
SELECT DISTINCT
    con."MemberId" contact
  , med."MemberId" med
  , rx."MemberId"  rx

FROM
    ( SELECT DISTINCT "MemberId" FROM bk_up.dpc_milliman_contacts WHERE pulled_at::DATE = '2023-08-16' ) con
    LEFT JOIN ( SELECT DISTINCT "MemberId" FROM bk_up.dpc_milliman_med_claims WHERE pulled_at::DATE = '2023-08-16' ) med
              ON med."MemberId" = con."MemberId"
    LEFT JOIN ( SELECT DISTINCT "MemberId" FROM bk_up.dpc_milliman_rx_claims WHERE pulled_at::DATE = '2023-08-16' ) rx
              ON rx."MemberId" = con."MemberId"


;
CREATE TABLE junk.sure_scripts_milliman_roster_20230907 AS
SELECT
    contact patient_id
FROM
    _dpc
WHERE
    rx ISNULL;
INSERT
        INTO
            public.sure_scripts_panel_patients (patient_id, sequence, last_name, first_name, middle_name, prefix,
                                                suffix, address_line_1, address_line_2, city, state, zip, dob, gender, npi,
                                                updated_at, inserted_at, reason_for_query)
        SELECT DISTINCT
            ptp.patient_id
          , ROW_NUMBER() OVER (ORDER BY ptp.patient_id)           sequence
          , REGEXP_REPLACE(p.last_name, E'[\\n\\r]+', '', 'g')    last_name
          , REGEXP_REPLACE(p.first_name, E'[\\n\\r]+', '', 'g')   first_name
          , NULL                                                  middle_name
          , NULL                                                  prefix
          , NULL                                                  suffix
          , REGEXP_REPLACE(pa.line1, E'[\\n\\r]+', '', 'g')       address_line_1
          , REGEXP_REPLACE(pa.line2, E'[\\n\\r]+', '', 'g')       address_line_2
          , REGEXP_REPLACE(pa.city, E'[\\n\\r]+', '', 'g')        city
          , REGEXP_REPLACE(pa.state, E'[\\n\\r]+', '', 'g')       state
          , REGEXP_REPLACE(pa.postal_code, E'[\\n\\r]+', '', 'g') zip
          , p.dob
          , LEFT(p.gender, 1)                                     gender
          , coalesce(mp.npi::TEXT, '1023087954')                  npi
          , NOW()                                                 updated_at
          , NOW()                                                 inserted_at
          , 'One off query for milliman when no dpc rx'
        FROM
            junk.sure_scripts_milliman_roster_20230907 ptp
            JOIN fdw_member_doc.patients p ON ptp.patient_id = p.id
            JOIN fdw_member_doc.patient_addresses pa ON p.id = pa.patient_id
            LEFT JOIN fdw_member_doc.patient_contacts pc
                      ON p.id = pc.patient_id AND pc.relationship = 'physician' AND pc.is_primary
            LEFT JOIN fdw_member_doc.msh_physicians mp ON mp.contact_id = pc.contact_id AND mp.npi IS NOT NULL
        WHERE
              -- don't add if patient already exists
              NOT EXISTS(SELECT
                             1
                         FROM
                             public.sure_scripts_panel_patients sspp
                         WHERE
                               sspp.sure_scripts_panel_id ISNULL
                           AND sspp.patient_id = ptp.patient_id)
              and length(p.first_name) >= 2 -- SS requires Two of a person's names (Last Name, First Name, Middle Name) must have 2 or more characters.
              and length(p.last_name) >= 2
        ;

SELECT *
FROM
    sure_scripts_panel_patients where sure_scripts_panel_id = 5281;

SELECT * FROM sure_scripts_med_history_details where sure_scripts_panel_id = 5281;

SELECT
    rp.id                     rp_id
  , rp.name                   rp_name
  , COUNT(DISTINCT d.contact) n_contact
  , COUNT(DISTINCT d.med)     n_med
  , COUNT(DISTINCT d.rx)      n_rx
FROM
    _dpc d
    JOIN fdw_member_doc.patient_referring_partners prp ON prp.patient_id = d.contact AND prp."primary"
    JOIN fdw_member_doc.referring_partners rp
         ON rp.id = prp.referring_partner_id AND rp.id IN (188, 225, 227, 275, 274, 273, 276)
GROUP BY
    1, 2
;

DROP TABLE IF EXISTS _dpc;
CREATE TEMP TABLE _dpc AS
SELECT DISTINCT
    con."MemberId" contact
  , med."MemberId" med
  , rx."MemberId"  rx

FROM
    ( SELECT DISTINCT "MemberId" FROM bk_up.dpc_milliman_contacts WHERE pulled_at::DATE = '2023-08-16' ) con
    LEFT JOIN ( SELECT DISTINCT "MemberId" FROM bk_up.dpc_milliman_med_claims WHERE pulled_at::DATE = '2023-08-16' ) med
              ON med."MemberId" = con."MemberId"
    LEFT JOIN ( SELECT DISTINCT "MemberId" FROM bk_up.dpc_milliman_rx_claims WHERE pulled_at::DATE = '2023-08-16' ) rx
              ON rx."MemberId" = con."MemberId"


DROP TABLE IF EXISTS _ss_med_hist;
CREATE TEMP TABLE _ss_med_hist AS
WITH
    ssmd AS ( SELECT DISTINCT ON (ssmhd.patient_id)
                  ssmhd.patient_id
                , ssmhd.sure_scripts_med_history_id
              FROM
                  _dpc d
                  JOIN sure_scripts_med_history_details ssmhd ON ssmhd.patient_id = d.contact::TEXT
              WHERE
                  rx ISNULL
              ORDER BY ssmhd.patient_id, sure_scripts_med_history_id )
SELECT hd.*
FROM
    ssmd
    JOIN sure_scripts_med_history_details hd
         ON hd.sure_scripts_med_history_id = ssmd.sure_scripts_med_history_id AND ssmd.patient_id = hd.patient_id;

;
SELECT *
FROM
    _ss_med_hist;


DROP TABLE IF EXISTS _dpc_milliman_rx_claims;
CREATE TEMP TABLE _dpc_milliman_rx_claims AS
SELECT
    h.id                                                      "ClaimID"
  , h.patient_id                                              "MemberId"
  , COALESCE(h.sold_date, h.last_filled_date, h.written_date) "FromDate"
  , h.product_code                                            "NDC"
  , h.prescriber_npi                                          "PrescriberID"
  , h.pharmacy_npi                                            "ProviderID"
  , NULL                                                      "Billed"
  , NULL                                                      "Allowed"
  , NULL                                                      "Paid"
  , NULL                                                      "Deductible"
  , NULL                                                      "Copay"
  , NULL                                                      "Coinsurance"
  , h.days_supply                                             "DaysSupply"
  , h.quantity_prescribed                                     "QuantityDispensed"
  , 'P'                                                       "ClaimLineStatus"
FROM
    _ss_med_hist h;

SELECT *
FROM
    _dpc_milliman_rx_claims;
DROP TABLE IF EXISTS _dpc_milliman_rx_claims;
CREATE TEMP TABLE _dpc_milliman_rx_claims AS
SELECT
    h.id                                                      "ClaimID"
  , h.patient_id                                              "MemberId"
  , COALESCE(h.sold_date, h.last_filled_date, h.written_date) "FromDate"
  , h.product_code                                            "NDC"
  , h.prescriber_npi                                          "PrescriberID"
  , h.pharmacy_npi                                            "ProviderID"
  , NULL                                                      "Billed"
  , NULL                                                      "Allowed"
  , NULL                                                      "Paid"
  , NULL                                                      "Deductible"
  , NULL                                                      "Copay"
  , NULL                                                      "Coinsurance"
  , h.days_supply                                             "DaysSupply"
  , h.quantity_prescribed                                     "QuantityDispensed"
  , 'P'                                                       "ClaimLineStatus"
FROM
    sure_scripts_med_history_details h where h.sure_scripts_panel_id = 5281;

SELECT count(distinct "MemberId")
FROM
    _dpc_milliman_rx_claims;
INSERT
INTO
    bk_up.dpc_milliman_rx_claims ("ClaimID", "MemberId", "FromDate", "NDC", "PrescriberID", "ProviderID", "Billed",
                                  "Allowed", "Paid", "Deductible", "Copay", "Coinsurance", "DaysSupply",
                                  "QuantityDispensed", "ClaimLineStatus")
SELECT
    "ClaimID"
  , "MemberId" ::bigint
  , "FromDate"
  , "NDC"
  , "PrescriberID"
  , "ProviderID"
  , "Billed"::numeric
  , "Allowed"::numeric
  , "Paid"::numeric
  , "Deductible"::numeric
  , "Copay"::numeric
  , "Coinsurance"::numeric
  , "DaysSupply"::int
  , "QuantityDispensed"::numeric
  , "ClaimLineStatus"
FROM
    _dpc_milliman_rx_claims;

