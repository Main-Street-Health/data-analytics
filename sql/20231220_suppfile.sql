
drop TABLE perm.supp_file_uhc_20231220;
CREATE TABLE perm.supp_file_uhc_20231220 AS
WITH
    good_mbi                 AS
        ( SELECT
              mco_id
            , analytics_member_id
            , medicare_no
            , CASE WHEN medicare_no !~* 's|l|o|i|b|z' -- invalid characters in an MBI
                AND LENGTH(REPLACE(LOWER(TRIM(medicare_no)), '-', '')) = 11
                       THEN UPPER((REGEXP_MATCH(REGEXP_REPLACE(LOWER(TRIM(medicare_no)), '[s|l|o|i|b|z|-]', '', 'g'),
                                                '[1-9][a-z][0-9a-z][0-9][a-z][0-9a-z][0-9][a-z][a-z][0-9][0-9]'))[1])
                  END mbi
          FROM
              fdw_member_doc.analytics_patients ap
          WHERE
              medicare_no IS NOT NULL
          )
  , encounter_icds           AS ( SELECT
                                      b.id                                                                      elation_bill_id
                                    , ARRAY_AGG(i.code ORDER BY bidx.seqno) FILTER ( WHERE i.code IS NOT NULL ) icds
                                  FROM
                                      fdw_member_doc.emr_origin_bill b
                                      JOIN fdw_member_doc.emr_origin_bill_item bi
                                           ON bi.bill_id = b.id AND bi.deletion_time ISNULL
                                      JOIN fdw_member_doc.emr_origin_visit_note vn
                                           ON vn.id = b.visit_note_id AND vn.deletion_time ISNULL
                                      JOIN fdw_member_doc.emr_origin_bill_item_dx bidx ON bi.id = bidx.bill_item_id
                                      JOIN fdw_member_doc.emr_origin_icd10 i ON i.id = bidx.icd10_id
                                      JOIN fdw_member_doc.patients p ON p.emr_origin_patient_id = vn.patient_id
                                      JOIN fdw_member_doc.payers pay
                                           ON p.payer_id = pay.id AND pay.name ~* 'uhc' AND pay.should_run_claiming -- ALL UHC payers that are claiming

                                  WHERE
                                        vn.signed_by_user_id IS NOT NULL
                                    AND vn.signed_time IS NOT NULL
                                    AND vn.signed_time >= '2023-06-16' --'{from_date}'
--                                     AND i.code not in ( 'M545', 'I714', 'D7582' ) -- not valid for billing per resp file
                                  GROUP BY
                                      1 )
  , encounter_ra_cpt         AS ( SELECT
                                      b.id                                                    elation_bill_id
                                    , UPPER(bi.cpt)                                           procedure_code
                                    , ROW_NUMBER() OVER (PARTITION BY b.id ORDER BY bi.seqno) rn
                                  FROM
                                      fdw_member_doc.emr_origin_bill b
                                      JOIN encounter_icds ei ON ei.elation_bill_id = b.id
                                      JOIN fdw_member_doc.emr_origin_bill_item bi
                                           ON bi.bill_id = b.id AND bi.deletion_time ISNULL
                                      JOIN fdw_member_doc.procedure_codes pc
                                           ON pc.code = bi.cpt AND pc.is_valid_for_hccs
                                  WHERE
                                    bi.cpt not in ('98966', '99211', '99422', '99421') -- failed as non ra in resp file
                                    AND (
                                        b.place_of_service IN (2, 11, 12)
                                        OR (
                                                b.place_of_service = 10 AND
                                                bi.modifier_1 = '95' AND
                                                bi.cpt IN
                                                ('99202', '99203', '99204', '99205', '99206', '99207', '99208', '99209',
                                                 '99210', '99211', '99212', '99213', '99214', '99215')
                                        )
                                      )
                                  )
  , _subscriber_ids           AS ( SELECT DISTINCT ON (analytics_member_id)
                                      analytics_member_id
                                    , insurance_member_id
                                  FROM
                                      fdw_member_doc.analytics_patient_insurances api
                                  where insurance_member_id is not null
                                  ORDER BY
                                      analytics_member_id, CASE
                                      WHEN api.rank = 'primary'   THEN 1
                                      WHEN api.rank = 'secondary' THEN 2
                                      WHEN api.rank = 'tertiary'  THEN 3 END )
  , pre_medicare_filter_list AS ( SELECT
                                      b.id                                elation_bill_id
                                    , p.first_name                        patient_first_name
                                    , p.last_name                         patient_last_name
                                    , p.dob                               patient_dob
                                    , gm.mbi                              mbi -- BP 6/23 Previously did COALESCE(gm.mbi, TRIM(gm.medicare_no)) I think we don't want to do that coalesce since mbi trumps subscriber id
                                    , si.insurance_member_id              member_id
                                    , LEFT(p.gender, 1)                   gender
                                    , vn.document_date ::DATE             date_of_service
                                    , emr_u.first_name                    provider_first_name
                                    , emr_u.last_name                     provider_last_name
                                    , emr_u.email                         provider_email
                                    , emr_u.npi                           provider_npi
                                    , ntc.medicare_specialty_code_int::text medicare_specialty_code_int
                                    , '84-2590508'                        tax_id
                                    , case when b.place_of_service = 2 then '02' else b.place_of_service::text end place_of_service -- no longer hard code pos 2
                                    , 'A'                                 ra_code
                                    , ecpt.procedure_code                 procedure_code
                                    , ei.icds                             icds
                                    , p.analytics_member_id               analytics_member_id
                                    , p.id                                golgi_id
                                  FROM
                                      fdw_member_doc.emr_origin_bill b
                                      JOIN encounter_icds ei ON ei.elation_bill_id = b.id
                                      JOIN encounter_ra_cpt ecpt
                                           ON ei.elation_bill_id = ecpt.elation_bill_id AND ecpt.rn = 1
                                      JOIN fdw_member_doc.emr_origin_visit_note vn
                                           ON vn.id = b.visit_note_id AND vn.deletion_time ISNULL
                                      JOIN fdw_member_doc.emr_origin_user emr_u ON emr_u.id = vn.physician_user_id
                                      LEFT JOIN fdw_member_doc.provider_taxonomy_codes ptc ON ptc.npi = emr_u.npi
                                      LEFT JOIN ref.npi_taxonomy_crosswalk ntc
                                                ON ntc.provider_taxonomy_code = ptc.taxonomy_code
                                      JOIN fdw_member_doc.patients p ON p.emr_origin_patient_id = vn.patient_id
                                      LEFT JOIN _subscriber_ids si on si.analytics_member_id = p.analytics_member_id
                                      LEFT JOIN good_mbi gm
                                                ON gm.analytics_member_id = p.analytics_member_id AND mbi IS NOT NULL
                                      -- left JOIN fdw_member_doc.analytics_patients ap ON p.analytics_member_id = ap.analytics_member_id
                                      -- JOIN fdw_member_doc.patient_addresses pa ON p.id = pa.patient_id -- 1-1 for now, careful for future!
                                  WHERE
                                        vn.signed_by_user_id IS NOT NULL
                                    AND vn.signed_time IS NOT NULL
                                                                            -- AND ap.line_of_business_name != 'medicaid'
                                  ORDER BY
                                      vn.document_date::DATE )
  , ever_medicare            AS ( SELECT DISTINCT
                                      pl.analytics_member_id
                                    , pl.golgi_id
                                  FROM
                                      ( SELECT DISTINCT analytics_member_id, golgi_id
                                        FROM pre_medicare_filter_list x ) pl
                                      JOIN cb.members m ON m.id = pl.analytics_member_id
                                      JOIN cb.eligibility_days ed
                                           ON ed.member_id = pl.analytics_member_id AND ed.mco_id = m.mco_id AND
                                              ed.line_of_business_id IN (2, 3) )
  , one_address              AS ( SELECT *
                                  FROM
                                      ( SELECT
                                            pa.*
                                          , ROW_NUMBER()
                                            OVER (PARTITION BY em.golgi_id ORDER BY CASE WHEN pa.type = 'home' THEN '1' ELSE pa.type END ASC) rn
                                        FROM
                                            ever_medicare em
                                            JOIN fdw_member_doc.patient_addresses pa ON em.golgi_id = pa.patient_id ) addr
                                  WHERE
                                      addr.rn = 1 )
SELECT
    pml.elation_bill_id
  , pml.patient_first_name
  , pml.patient_last_name
  , pml.patient_dob
  , pml.mbi
  , pml.member_id
  , pml.gender
  , pml.date_of_service
  , pml.provider_first_name
  , pml.provider_last_name
  , pml.provider_email
  , pml.provider_npi
  , pml.medicare_specialty_code_int
  , pml.tax_id
  , pml.place_of_service
  , pml.ra_code
  , oa.line1
  , oa.line2
  , oa.city
  , oa.state
  , oa.postal_code
  , pml.procedure_code
  , pml.icds
FROM
    pre_medicare_filter_list pml
    JOIN ever_medicare em ON em.analytics_member_id = pml.analytics_member_id
    JOIN one_address oa ON oa.patient_id = pml.golgi_id
;
select COUNT(*) FROM perm.supp_file_uhc_20231220;
with dupes as ( SELECT
                    elation_bill_id
                  , COUNT(*)
                FROM
                    perm.supp_file_uhc_20231220
                GROUP BY
                    1
                HAVING
                    COUNT(*) > 1 )
SELECT distinct sf.*
FROM
    dupes d
join perm.supp_file_uhc_20231220 sf on sf.elation_bill_id = d.elation_bill_id
;


SELECT count(*) FROM perm.supp_file_uhc_20230616;


SELECT
    npi
  , COUNT(*)
FROM
    fdw_member_doc.provider_taxonomy_codes
GROUP BY 1
ORDER BY
    2 DESC;

SELECT *
FROM
    fdw_member_doc.emr_origin_user
WHERE
    npi = '1588736938';

select *
FROM
    fdw_member_doc.provider_taxonomy_codes
WHERE
    npi = '1588736938';


