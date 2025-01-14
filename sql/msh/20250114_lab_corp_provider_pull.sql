--

-- lab_corp_physician_counts_20250114
SELECT
    mp.full_name           "Participant Provider Name"
  , mp.npi                 "NPI"
  , rp.name                "Practice name"
  , rp.address1            "Practice address"
  , rp.city                "City"
  , rp.state               "State"
  , rp.zip                 "Zip code"
  , rp.admin_contact_phone "Phone"
  , COUNT(DISTINCT p.id)   "# of attributed Medicare beneficiaries"
FROM
    msh_physicians mp
    JOIN msh_physicians_care_teams mpct ON mp.id = mpct.msh_physician_id
    JOIN care_teams ct ON mpct.care_team_id = ct.id
    JOIN patients p ON p.care_team_id = ct.id
    JOIN msh_care_team_referring_partners ctrp ON ct.id = ctrp.care_team_id
    JOIN referring_partners rp ON ctrp.referring_partner_id = rp.id
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8
;

SELECT *
FROM
    member_doc.oban.oban_crons where name ~* 'panel';
