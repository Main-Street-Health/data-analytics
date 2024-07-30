------------------------------------------------------------------------------------------------------------------------
/* FULL Roster */
------------------------------------------------------------------------------------------------------------------------
CREATE or REPLACE VIEW _v_md_portals_full_rosters_no_galen
            ( patient_org_source_id, patient_first_name, patient_middle_name, patient_last_name, patient_dob
            , patient_sex, patient_phone, patient_address, patient_city, patient_state, patient_zip
            , provider_direct_address, provider_name, provider_group_name, icd_codes, next_apt_date, last_visit_date)
AS
SELECT DISTINCT
    p.patient_id                                                                        AS patient_org_source_id
  , p.patient_first_name
  , null                                                                                AS patient_middle_name
  , p.patient_last_name
  , p.date_of_birth                                                                     AS patient_dob
  , left(p.gender, 1)                                                                   AS patient_sex
  , p.primary_phone_number                                                              AS patient_phone
  , p.address_line1                                                                     AS patient_address
  , p.address_city                                                                      AS patient_city
  , p.address_state                                                                     AS patient_state
  , p.address_postal_code                                                               AS patient_zip
  , NULL::TEXT                                                                          AS provider_direct_address
  , NULL::TEXT                                                                          AS provider_name
  , STRING_AGG(DISTINCT COALESCE(rpo.name, rp.name), '; '::TEXT)                        AS provider_group_name
  , dx.icds                                                                             AS icd_codes
  , appt.next_visit_date                                                                AS next_apt_date
  , visit.last_visit_date                                                               AS last_visit_date
FROM
    ent.patients p
    JOIN supreme_pizza sp on sp.patient_id = p.patient_id and sp.is_cca
    JOIN referring_partners rp ON sp.primary_referring_partner_id = rp.id
    JOIN msh_referring_partner_organizations rpo on rpo.id = rp.organization_id
    LEFT JOIN ( SELECT DISTINCT ON (v.patient_id)
                    v.patient_id
                  , v.visit_date_ct AS last_visit_date
                FROM
                    ent.visits v
                WHERE
                  v.visit_type_id = 'cca_recon'
                  AND v.visit_outcome = 'completed'
                ORDER BY
                    v.patient_id, v.visit_date_ct DESC
    ) visit ON visit.patient_id = p.patient_id
    left  join (
        select distinct on (a.patient_id) a.patient_id,
                 a.start_ct::date as next_visit_date
               from ent.appointments a
               where a.visit_type_name ~* 'cca_recon'
                  and a.status != 'cancelled'
                  and a.scheduled_date_ct > current_date
               order by a.patient_id, a.start_ct
    ) appt on appt.patient_id = p.patient_id
    left join (
        SELECT
            meed.patient_id
          , STRING_AGG(DISTINCT i10s.code_formatted, ',') FILTER ( WHERE i10s.id IS NOT NULL )        icds
        FROM
            public.msh_external_emr_diagnoses meed
            JOIN public.icd10s i10s ON meed.icd10_id = i10s.id
            LEFT JOIN cpt.cca_quality cq ON cq.patient_id = meed.patient_id AND cq.icd_id = meed.icd10_id
        WHERE
          meed.cms_contract_year = DATE_PART('year', NOW())
          AND NOT meed.is_deleted
           AND (
                       meed.diagnosis_type = 'recapture'
                   OR
                       cq.code_status = 'checked_yes_on_worksheet'
               )
        GROUP BY
            1
   ) dx on dx.patient_id = p.patient_id
WHERE
    p.gender <> 'Unknown'
    and rpo.id not in (7 /* basset */, 67 /* hill country */, 10 /* Mercy Community */) -- @HARD EXCLUSION
    and NOT (EXISTS(SELECT
                      1
                  FROM
                      patient_referring_partners prp2
                      JOIN referring_partners rp2 ON prp2.referring_partner_id = rp2.id
                      JOIN msh_referring_partner_organizations mrpo2 ON rp2.organization_id = mrpo2.id
                  WHERE
                        p.patient_id = prp2.patient_id
                    AND mrpo2.id = 248))
GROUP BY
    p.patient_id, p.patient_first_name, p.patient_last_name, p.date_of_birth, p.gender
                , p.primary_phone_number, p.address_line1, p.address_city, p.address_state, p.address_postal_code
                , dx.icds, appt.next_visit_date, visit.last_visit_date;

SELECT *
FROM
    _v_md_portals_full_rosters_no_galen;

------------------------------------------------------------------------------------------------------------------------
/* Daily MSH roster */
------------------------------------------------------------------------------------------------------------------------

CREATE or replace VIEW _v_md_portals_rosters
            ( patient_org_source_id, patient_first_name, patient_middle_name, patient_last_name, patient_dob
            , patient_sex, patient_phone, patient_address, patient_city, patient_state, patient_zip
            , provider_direct_address, provider_name, provider_group_name, icd_codes, next_apt_date)
AS
WITH
    lst AS ( SELECT
                 MAX(mdr.inserted_at) - '00:01:00'::INTERVAL AS last_mdp_at
             FROM
                 md_portal_rosters mdr )
SELECT DISTINCT
    p.patient_id                                                       AS patient_org_source_id
  , p.patient_first_name
  , NULL::TEXT                                                         AS patient_middle_name
  , p.patient_last_name
  , p.date_of_birth                                                    AS patient_dob
  , left(p.gender, 1)                                                  AS patient_sex
  , p.primary_phone_number                                             AS patient_phone
  , p.address_line1                                                    AS patient_address
  , p.address_city                                                     AS patient_city
  , p.address_state                                                    AS patient_state
  , p.address_postal_code                                              AS patient_zip
  , NULL::TEXT                                                         AS provider_direct_address
  , NULL::TEXT                                                         AS provider_name
  , STRING_AGG(DISTINCT COALESCE(rpo.name, rp.name::TEXT), '; '::TEXT) AS provider_group_name
  , dx.icds                                                            AS icd_codes
  , appt.next_visit_date                                               AS next_apt_date
FROM
    ent.patients p
    JOIN msh_patient_integration_configs ic ON ic.patient_id = p.patient_id
    JOIN patient_referring_partners prp ON prp.patient_id = p.patient_id
    JOIN referring_partners rp ON prp.referring_partner_id = rp.id
    JOIN msh_referring_partner_organizations rpo ON rpo.id = rp.organization_id
    LEFT JOIN ( SELECT DISTINCT ON (a.patient_id)
                    a.patient_id
                  , a.start_ct::DATE AS next_visit_date
                FROM
                    ent.appointments a
                WHERE
                      a.visit_type_name ~* 'cca_recon'::TEXT
                  AND a.status <> 'cancelled'::TEXT
                  AND a.scheduled_date_ct > CURRENT_DATE
                ORDER BY a.patient_id, a.start_ct ) appt ON appt.patient_id = p.patient_id
    LEFT JOIN ( SELECT
                    meed.patient_id
                  , STRING_AGG(DISTINCT i10s.code_formatted, ','::TEXT) FILTER (WHERE i10s.id IS NOT NULL) AS icds
                FROM
                    msh_external_emr_diagnoses meed
                    JOIN icd10s i10s ON meed.icd10_id = i10s.id
                    LEFT JOIN cpt.cca_quality cq ON cq.patient_id = meed.patient_id AND cq.icd_id = meed.icd10_id
                WHERE
                        meed.cms_contract_year::DOUBLE PRECISION = DATE_PART('year'::TEXT, NOW())
                  AND   NOT meed.is_deleted
                  AND   (meed.diagnosis_type = 'recapture'::TEXT OR cq.code_status = 'checked_yes_on_worksheet'::TEXT)
                GROUP BY meed.patient_id ) dx ON dx.patient_id = p.patient_id
WHERE
      p.gender <> 'Unknown'::TEXT
  AND NOT (EXISTS(SELECT
                      1
                  FROM
                      patient_referring_partners prp2
                      JOIN referring_partners rp2 ON prp2.referring_partner_id = rp2.id
                      JOIN msh_referring_partner_organizations mrpo2 ON rp2.organization_id = mrpo2.id
                  WHERE
                        p.patient_id = prp2.patient_id
                    AND mrpo2.id = 248))
  AND (ic.md_portal OR (EXISTS(SELECT
                                   1
                               FROM
                                   hospitalizations h
                                   JOIN lst ON h.discharge_date > lst.last_mdp_at
                               WHERE
                                   h.patient_id = p.patient_id)))
GROUP BY
    p.patient_id, p.patient_first_name, p.patient_last_name, p.date_of_birth, p.gender, p.primary_phone_number
                , p.address_line1, p.address_city, p.address_state, p.address_postal_code, dx.icds
                , appt.next_visit_date;


------------------------------------------------------------------------------------------------------------------------
/* GALEN */
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW _v_md_portals_galen_rosters
            ( patient_org_source_id, patient_first_name, patient_middle_name, patient_last_name, patient_dob
            , patient_sex, patient_phone, patient_address, patient_city, patient_state, patient_zip
            , provider_direct_address, provider_name, provider_group_name, icd_codes, next_apt_date)
AS
WITH
    next_appts AS ( SELECT DISTINCT ON (emr_appts_historical_files.patient_id)
                        emr_appts_historical_files.patient_id
                      , emr_appts_historical_files.appt_date
                    FROM
                        stage.emr_appts_historical_files
                    WHERE
                          emr_appts_historical_files.is_provider_appt
                      AND emr_appts_historical_files.msh_physician_id IS NOT NULL
                      AND emr_appts_historical_files.appt_cancelled IS FALSE
                      AND emr_appts_historical_files.referring_partner_id = 279
                      AND emr_appts_historical_files.appt_date >= NOW()::DATE
                      AND emr_appts_historical_files.appt_date <= (NOW() + '15 days'::INTERVAL)
                    ORDER BY emr_appts_historical_files.patient_id, emr_appts_historical_files.appt_date )
SELECT DISTINCT
    p.patient_id                                                        AS patient_org_source_id
  , p.patient_first_name
  , NULL                                                                AS patient_middle_name
  , p.patient_last_name
  , p.date_of_birth                                                     AS patient_dob
  , left(p.gender, 1)                                                   AS patient_sex
  , p.primary_phone_number                                              AS patient_phone
  , p.address_line1                                                     AS patient_address
  , p.address_city                                                      AS patient_city
  , p.address_state                                                     AS patient_state
  , p.address_postal_code                                               AS patient_zip
  , NULL::TEXT                                                          AS provider_direct_address
  , NULL::TEXT                                                          AS provider_name
  , STRING_AGG(DISTINCT COALESCE(mrpo.name, rp.name::TEXT), '; '::TEXT) AS provider_group_name
  , dx.icds                                                             AS icd_codes
  , na.appt_date                                                        AS next_apt_date
FROM
    ent.patients p
    JOIN msh_patient_integration_configs ic ON ic.patient_id = p.patient_id
    JOIN patient_referring_partners prp ON prp.patient_id = p.patient_id
    JOIN referring_partners rp ON prp.referring_partner_id = rp.id
    JOIN msh_referring_partner_organizations mrpo ON mrpo.id = rp.organization_id
    JOIN next_appts na ON na.patient_id = p.patient_id
    LEFT JOIN ( SELECT
                    meed.patient_id
                  , STRING_AGG(DISTINCT i10s.code_formatted, ','::TEXT) FILTER (WHERE i10s.id IS NOT NULL) AS icds
                FROM
                    msh_external_emr_diagnoses meed
                    JOIN icd10s i10s ON meed.icd10_id = i10s.id
                    LEFT JOIN cpt.cca_quality cq ON cq.patient_id = meed.patient_id AND cq.icd_id = meed.icd10_id
                WHERE
                        meed.cms_contract_year::DOUBLE PRECISION = DATE_PART('year'::TEXT, NOW())
                  AND   NOT meed.is_deleted
                  AND   (meed.diagnosis_type = 'recapture'::TEXT OR cq.code_status = 'checked_yes_on_worksheet'::TEXT)
                GROUP BY meed.patient_id ) dx ON dx.patient_id = p.patient_id
WHERE
      mrpo.id = 248
  AND p.gender <> 'Unknown'::TEXT
  AND ic.md_portal
GROUP BY
    p.patient_id, p.patient_first_name, p.patient_last_name, p.date_of_birth, p.gender
                , p.primary_phone_number, p.address_line1, p.address_city, p.address_state, p.address_postal_code
                , dx.icds, na.appt_date;

