DROP TABLE IF EXISTS _missing_humana;
CREATE TEMP TABLE _missing_humana AS
SELECT *
FROM
    ( VALUES
          (50052, 'CBP'),
          (50370, 'CBP'),
          (60310, 'CBP'),
          (60310, 'EED'),
          (61788, 'CBP'),
          (63729, 'A1C9'),
          (64417, 'OMW'),
          (66516, 'OMW'),
          (70122, 'OMW'),
          (82921, 'OMW'),
          (94967, 'A1C9'),
          (118494, 'OMW'),
          (118590, 'OMW'),
          (143907, 'OMW'),
          (149405, 'OMW'),
          (158975, 'CBP'),
          (193880, 'OMW'),
          (195948, 'OMW'),
          (206570, 'A1C9'),
          (206570, 'EED'),
          (206669, 'CBP'),
          (206669, 'COA'),
          (206669, 'COL'),
          (206742, 'CBP'),
          (206742, 'COL'),
          (206742, 'EED'),
          (207025, 'CBP'),
          (207118, 'CBP'),
          (207133, 'A1C9'),
          (207133, 'CBP'),
          (207322, 'CBP'),
          (207407, 'BCS'),
          (207407, 'CBP'),
          (207891, 'CBP'),
          (208020, 'CBP'),
          (208189, 'A1C9'),
          (208207, 'A1C9'),
          (208207, 'EED'),
          (208256, 'CBP'),
          (208778, 'A1C9'),
          (209489, 'OMW'),
          (209525, 'CBP'),
          (210390, 'OMW'),
          (210409, 'COL'),
          (210679, 'BCS'),
          (211014, 'A1C9'),
          (211014, 'CBP'),
          (231860, 'A1C9'),
          (231860, 'COA'),
          (231860, 'EED'),
          (233521, 'OMW'),
          (233904, 'OMW'),
          (233966, 'OMW'),
          (237401, 'OMW'),
          (238633, 'OMW'),
          (239925, 'OMW'),
          (240128, 'OMW'),
          (240651, 'OMW'),
          (240736, 'OMW'),
          (241013, 'OMW'),
          (241737, 'OMW'),
          (244370, 'OMW'),
          (245209, 'OMW'),
          (245854, 'OMW'),
          (249368, 'OMW'),
          (250895, 'OMW'),
          (270134, 'OMW'),
          (310613, 'A1C9'),
          (311210, 'A1C9'),
          (311210, 'EED'),
          (317578, 'CBP'),
          (320389, 'BCS'),
          (320389, 'COL'),
          (343173, 'OMW'),
          (345218, 'OMW'),
          (345338, 'OMW'),
          (350628, 'OMW'),
          (354205, 'OMW'),
          (358276, 'OMW'),
          (379871, 'CBP'),
          (387236, 'OMW'),
          (429175, 'CBP'),
          (429175, 'COA'),
          (436918, 'OMW'),
          (483002, 'COL'),
          (483014, 'A1C9'),
          (483014, 'BCS'),
          (483014, 'COL'),
          (500201, 'OMW'),
          (612300, 'OMW'),
          (613782, 'CBP'),
          (613842, 'COL'),
          (650715, 'A1C9'),
          (650715, 'CBP'),
          (650715, 'COL'),
          (650741, 'COL'),
          (651360, 'A1C9'),
          (651360, 'EED'),
          (679232, 'A1C9'),
          (679232, 'CBP'),
          (679232, 'COA'),
          (679232, 'EED'),
          (729515, 'CBP'),
          (743936, 'CBP'),
          (752635, 'OMW'),
          (795534, 'OMW'),
          (837501, 'BCS'),
          (837501, 'CBP'),
          (837501, 'COL'),
          (858485, 'COL'),
          (870002, 'COL'),
          (884863, 'COL') ) x(patient_id, measure_id);

DROP TABLE IF EXISTS _to_purge;
CREATE TEMP TABLE _to_purge AS
SELECT *
FROM
    ( SELECT DISTINCT
          cfd.id                                                                                   chase_file_detail_id
        , cfd.chase_id
        , cfd.reveleer_file_id                                                                     chase_file_id
        , cfd.state_payer_id
        , cfd.inserted_at                                                                          chase_ins_at
        , afd.id                                                                                   attr_file_detail_id
        , afd.reveleer_file_id                                                                     attr_file_id
        , afd.inserted_at                                                                          attr_ins_at
        , rf.file_name ~* 'admin'                                                                  is_admin_file
        , ROW_NUMBER() OVER (PARTITION BY cfd.chase_id ORDER BY cfd.inserted_at - afd.inserted_at) rank
      FROM
          _missing_humana mh
          JOIN reveleer_chase_file_details cfd ON cfd.patient_id = mh.patient_id
              AND cfd.measure_id = mh.measure_id
          JOIN reveleer_files rf ON cfd.reveleer_file_id = rf.id
          JOIN reveleer_projects rp ON rp.state_payer_id = cfd.state_payer_id
          left JOIN reveleer_attribute_file_details afd ON afd.state_payer_id = cfd.state_payer_id
              AND afd.sample_id::BIGINT = cfd.chase_id ) x
;
SELECT * FROM _to_purge;

-- create table junk.reveleer_chase_file_details_deleted as
SELECT *, now()  deleted_at from reveleer_chase_file_details rcfd
where id in (select chase_file_detail_id from _to_purge);

DELETE
FROM
    reveleer_chase_file_details
WHERE
    id IN
    ( SELECT id FROM junk.reveleer_chase_file_details_deleted );



-- create table junk.reveleer_attribute_file_details_deleted as
SELECT *, now()  deleted_at from reveleer_attribute_file_details af
where id in (select attr_file_detail_id from _to_purge);

DELETE
FROM
    reveleer_attribute_file_details af
WHERE
    id IN ( SELECT id FROM junk.reveleer_attribute_file_details_deleted );


CREATE TABLE junk.reveleer_missing_humana_to_resend_20240318
AS
SELECT DISTINCT
    chase_id pqm_id
FROM
    _to_purge;

SELECT distinct rp.name
FROM
    _to_purge
join reveleer_projects rp ON _to_purge.state_payer_id = rp.state_payer_id
;
-- ["humana_tn", "humana_ky", "humana_al"]

DELETE
FROM
    junk.reveleer_missing_humana_to_resend_20240318 j
WHERE
    j.pqm_id IN
    ( SELECT
          pqm.id
      FROM
          junk.reveleer_missing_humana_to_resend_20240318 j
          JOIN fdw_member_doc.patient_quality_measures pqm ON j.pqm_id = pqm.id
          JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
      WHERE
            qm.code = 'OMW'
        AND pqm.impact_date - 180 NOT BETWEEN '2022-07-01' AND '2023-06-30' )
;


select count(distinct pqm_id) from junk.reveleer_missing_humana_to_resend_20240318  j
SELECT pqm.id pqm_id, pqm.impact_date - 180
FROM
    junk.reveleer_missing_humana_to_resend_20240318 j
    JOIN fdw_member_doc.patient_quality_measures pqm ON j.pqm_id = pqm.id
        JOIN fdw_member_doc.quality_measures qm on qm.id = pqm.measure_id
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
-- where qm.code = 'OMW'
-- and pqm.impact_date - 180 not between '2022-07-01' and '2023-06-30'
    ;

-- these were deleted pqm's
SELECT chase_id, patient_id, measure_id, inserted_at
FROM
    junk.reveleer_chase_file_details_deleted j
WHERE
    j.chase_id IN (
                   669873,
                   669901,
                   669968,
                   669893
        );

------------------------------------------------------------------------------------------------------------------------
/* round 2 */
------------------------------------------------------------------------------------------------------------------------
-- purge
DROP TABLE IF EXISTS _chases_to_purge;
CREATE TEMP TABLE _chases_to_purge AS
SELECT DISTINCT
    cfd.id chase_file_detail_id
-- cfd.chase_id
FROM
    junk.missing_cigna_wellcare_uhc_chases_20240318 j
    JOIN reveleer_chase_file_details cfd ON cfd.patient_id = j.patient_id
        AND cfd.measure_id = j.measure_id
;
SELECT count(*), count(distinct chase_file_detail_id)  FROM _chases_to_purge;

DROP TABLE IF EXISTS _attr_to_purge;
CREATE TEMP TABLE _attr_to_purge AS
SELECT DISTINCT
--     cfd.id chase_file_detail_id
afd.id attr_file_detail_id
FROM
    junk.missing_cigna_wellcare_uhc_chases_20240318 j
    JOIN reveleer_chase_file_details cfd ON cfd.patient_id = j.patient_id
        AND cfd.measure_id = j.measure_id
    JOIN public.reveleer_attribute_file_details afd ON afd.sample_id::BIGINT = cfd.chase_id
;


-- create table junk.reveleer_chase_file_details_deleted as
INSERT
INTO
    junk.reveleer_chase_file_details_deleted (id, reveleer_file_id, patient_id, row_id, health_plan, contract,
                                              member_id, line_of_business, product, sample_id, sequence, measure_id,
                                              chase_id, enrollee_id, member_fname, member_lname, member_mi,
                                              member_gender, member_dob, member_address1, member_address2, member_city,
                                              member_state, member_zip, member_phone, member_cell_phone, member_fax,
                                              member_email, member_last4, user_defined_values, active, retrieval_source,
                                              chart_action, third_party_vendor, provider_id, provider_firstname,
                                              provider_lastname, provider_npi, tin, provider_specialty,
                                              provider_taxonomy, chart_address_phone, chart_address_extension,
                                              chart_address_fax, chart_address_email, chart_address_secondaryphone,
                                              chart_address_grouping, chart_site_id, chart_address_type, chart_address1,
                                              chart_address2, chart_city, chart_state, chart_zip_code, comment,
                                              alternate_address_phone, alternate_address_extension,
                                              alternate_address_fax, alternate_address_email,
                                              alternate_address_secondary_phone, alternate_address_grouping,
                                              alternate_site_id, alternate_address_type, alternate_address1,
                                              alternate_address2, alternate_city, alternate_state, alternate_zipcode,
                                              group_name, contact_name, dos_from, dos_through, chart_address_tag,
                                              chase_tag, chart_filename, inserted_at, updated_at, state_payer_id, yr,
                                              deleted_at)
SELECT
    id, reveleer_file_id, patient_id, row_id, health_plan, contract,
                                              member_id, line_of_business, product, sample_id, sequence, measure_id,
                                              chase_id, enrollee_id, member_fname, member_lname, member_mi,
                                              member_gender, member_dob, member_address1, member_address2, member_city,
                                              member_state, member_zip, member_phone, member_cell_phone, member_fax,
                                              member_email, member_last4, user_defined_values, active, retrieval_source,
                                              chart_action, third_party_vendor, provider_id, provider_firstname,
                                              provider_lastname, provider_npi, tin, provider_specialty,
                                              provider_taxonomy, chart_address_phone, chart_address_extension,
                                              chart_address_fax, chart_address_email, chart_address_secondaryphone,
                                              chart_address_grouping, chart_site_id, chart_address_type, chart_address1,
                                              chart_address2, chart_city, chart_state, chart_zip_code, comment,
                                              alternate_address_phone, alternate_address_extension,
                                              alternate_address_fax, alternate_address_email,
                                              alternate_address_secondary_phone, alternate_address_grouping,
                                              alternate_site_id, alternate_address_type, alternate_address1,
                                              alternate_address2, alternate_city, alternate_state, alternate_zipcode,
                                              group_name, contact_name, dos_from, dos_through, chart_address_tag,
                                              chase_tag, chart_filename, inserted_at, updated_at, state_payer_id, yr,
                                              deleted_at
from
    ( SELECT *
           , NOW() deleted_at
      FROM
          reveleer_chase_file_details rcfd
      WHERE
          id IN ( SELECT chase_file_detail_id FROM _chases_to_purge ) ) x
;

DELETE
FROM
    reveleer_chase_file_details
WHERE
    id IN
    ( SELECT id FROM junk.reveleer_chase_file_details_deleted );



-- create table junk.reveleer_attribute_file_details_deleted as
INSERT
INTO
    junk.reveleer_attribute_file_details_deleted (id, reveleer_file_id, patient_id, row_id, member_id, measure_id,
                                                  attribute_group_name, attribute_code, attribute_value,
                                                  numerator_event_id, data_type_flag, inserted_at, updated_at,
                                                  sample_id, state_payer_id, yr, deleted_at)
select
    id, reveleer_file_id, patient_id, row_id, member_id, measure_id,
                                                  attribute_group_name, attribute_code, attribute_value,
                                                  numerator_event_id, data_type_flag, inserted_at, updated_at,
                                                  sample_id, state_payer_id, yr, deleted_at
from
    ( SELECT *
           , NOW() deleted_at
      FROM
          reveleer_attribute_file_details af
      WHERE
          id IN ( SELECT attr_file_detail_id FROM _attr_to_purge ) ) x;

DELETE
FROM
    reveleer_attribute_file_details af
WHERE
    id IN ( SELECT id FROM junk.reveleer_attribute_file_details_deleted );

-- end purge
-- stage
create table junk.reveleer_missing_wc_cig_uhc_to_resend_20240318 as
SELECT distinct d.chase_id pqm_id
FROM
    junk.missing_cigna_wellcare_uhc_chases_20240318 j
join junk.reveleer_chase_file_details_deleted d on d.patient_id = j.patient_id and d.measure_id = j.measure_id
;
SELECT
    ptr.name,
    count(distinct pqm.id) pqms
--     ptr.name
--     COUNT(DISTINCT pqm.id)
--   , COUNT(*)
--   , ARRAY_AGG(DISTINCT pqm.patient_id) FILTER ( WHERE sp.patient_id ISNULL )         missing_in_pizza
--   , ARRAY_AGG(DISTINCT pqm.id) FILTER ( WHERE pqm.mco_source_state_payer_id ISNULL ) missing_state_payer
FROM
--     junk.reveleer_missing_wc_cig_uhc_to_resend_20240318 j
--     JOIN fdw_member_doc.patient_quality_measures pqm ON j.pqm_id = pqm.id
reveleer_chase_file_details cfd
    JOIN fdw_member_doc.patient_quality_measures pqm on cfd.chase_id = pqm.id
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
where cfd.inserted_at > now() - '1 day'::interval
-- where qm.measure_code in ('TRC', 'FMC', 'MRP')
-- and pqm.impact_date ISNULL
GROUP BY 1
;
SELECT impact_date
FROM
    fdw_member_doc.patient_quality_measures WHERE id = 618177;
SELECT *
FROM
    fdw_member_doc.patients WHERE id = 366616;

name
["centene_ar", "uhc_ar", "uhc_al", "uhc_tn", "wellcare_ky", "uhc_wv", "cigna_tn", "wellcare_tn", "wellcare_ny", "uhc_ky", "wellcare_ar", "anthem_tn"]

SELECT *
FROM
    reveleer_chase_file_details cfd
join reveleer_files rf on rf.id = cfd.reveleer_file_id
;
-- redo, after fixed trc mrp dates
DROP TABLE IF EXISTS _files_to_purge;
CREATE TEMP TABLE _files_to_purge AS
SELECT id
FROM
    reveleer_files
where inserted_at > '2024-03-18 17:46:39.427256'
ORDER BY
    id DESC;
delete from reveleer_chase_file_details cfd
where cfd.reveleer_file_id in (select id from _files_to_purge);
delete from reveleer_attribute_file_details cfd
where cfd.reveleer_file_id in (select id from _files_to_purge);
delete from reveleer_compliance_file_details cfd
where cfd.reveleer_file_id in (select id from _files_to_purge);

delete from reveleer_files cfd
where cfd.id in (select id from _files_to_purge);


------------------------------------------------------------------------------------------------------------------------
/* from amy */
------------------------------------------------------------------------------------------------------------------------
SELECT
    p.id
  , p.status
  , qm.code
  , pqm.inserted_at
  , pqm.status
, pqm.mco_source_state_payer_id
, pqm.source
FROM
    fdw_member_doc.patients p
    JOIN fdw_member_doc.patient_quality_measures pqm ON p.id = pqm.patient_id AND pqm.year = 2023
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
-- JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
--     JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
--         AND qmc.measure_id = pqm.measure_id
--         AND qmc.measure_year = pqm.year
--     JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
--     JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
--     JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
WHERE
    pqm.id = 123568;



reveleer_chase_file_details cfd
    JOIN fdw_member_doc.patient_quality_measures pqm on cfd.chase_id = pqm.id
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
where cfd.inserted_at > now() - '1 day'::interval
-
CREATE table junk.reveleer_missing_hum_20240318 as
select id pqm_id from fdw_member_doc.patient_quality_measures where patient_id = 146794 and measure_id = 44 union -- Deborah Pennington
select id from fdw_member_doc.patient_quality_measures where patient_id = 309576 and measure_id = 45 union -- Susie Woods
select id from fdw_member_doc.patient_quality_measures where patient_id = 504408 and measure_id = 3; -- Della Shepherd
FMC

SELECT pqm.id, pqm.patient_id, pqm.measure_id, pqm.impact_date, qm.code
FROM
    junk.reveleer_missing_hum_20240318 j
        join fdw_member_doc.patient_quality_measures pqm on pqm.id = j.pqm_id
        join fdw_member_doc.quality_measures qm on qm.id = pqm.measure_id
left join reveleer_chase_file_details cfd on cfd.chase_id = j.pqm_id
where cfd.id ISNULL


;
-- 80
-- | name | count |
-- | :--- | :--- |
-- | anthem\_ky | 22 |
-- | anthem\_tn | 18 |
-- | humana\_ky | 36 |
-- | humana\_tn | 1 |
-- | uhc\_tn | 3 |
create table junk.reveleer_missing_hum_20240319 as
SELECT distinct
pqm.id pqm_id
FROM
    fdw_member_doc.patient_quality_measures pqm
        JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
left join reveleer_chase_file_details cfd on cfd.chase_id = pqm.id
where cfd.id ISNULL
    and ptr.name in ('humana_ky', 'humana_tn')
    and pqm.id in (
 486694
, 426492
, 446341
, 446819
, 446900
, 446901
, 453242
, 456367
, 462325
, 462326
, 462397
, 462398
, 462534
, 462613
, 462630
, 463251
, 463256
, 463257
, 463258
, 463525
, 463527
, 464089
, 464532
, 464533
, 464736
, 464741
, 464790
, 464794
, 464796
, 465356
, 465634
, 465636
, 465638
, 466038
, 466040
, 483540
, 484104
, 484106
, 485533
, 485542
, 485587
, 485590
, 485626
, 485631
, 485652
, 486878
, 487213
, 487299
, 487300
, 487340
, 487446
, 487448
, 487459
, 487463
, 487536
, 487550
, 487565
, 487567
, 488490
, 489288
, 489185
, 489209
, 489210
, 489232
, 489328
, 489330
, 489331
, 489348
, 489375
, 489382
, 497345
, 499718
, 499719
, 499801
, 500038
, 502764
, 517009
, 517056
, 517188
, 517466
, 519942
, 520014
, 520063
, 520064
, 520116
, 520117
, 520300
, 520372
, 520457
, 520484
, 520735
, 520736
, 520820
, 520859
, 520863
, 520990
, 521158
, 521186
, 522642
, 522956
, 522957
, 522958
, 522959
, 522960
, 426454
, 446178
, 446340
, 446818
, 447110
, 456366
, 456368
, 456370
, 460891
, 460893
, 462323
, 462612
, 462633
, 462634
, 462636
, 462753
, 462816
, 463252
, 463371
, 463372
, 463938
, 463939
, 464028
, 464073
, 464101
, 464738
, 464742
, 464744
, 464745
, 464762
, 464791
, 464792
, 464795
, 464854
, 464925
, 465119
, 465120
, 465466
, 465637
, 465973
, 465975
, 465977
, 483501
, 483502
, 483504
, 483505
, 484117
, 485555
, 485557
, 485579
, 485581
, 485583
, 485586
, 485630
, 485653
, 486876
, 486888
, 486891
, 487271
, 487429
, 487465
, 488488
, 489186
, 489329
, 489347
, 489349
, 489378
, 489379
, 489385
, 489388
, 489406
, 489408
, 490881
, 491685
, 491686
, 499754
, 500574
, 502749
, 516893
, 517181
, 517191
, 517195
, 517273
, 517284
, 517463
, 517547
, 520131
, 520283
, 520284
, 520285
, 520299
, 520455
, 520485
, 520600
, 520615
, 520683
, 520734
, 520741
, 520826
, 520827
, 520864
, 520873
, 521159
, 522640
, 522641
, 523605
, 665717
, 665719
, 665723
, 665725
, 665728
, 665729
, 665731
, 665732
, 665733
, 665740
, 665749
, 665752
, 665757
, 665769
, 665771
, 665774
, 665775
, 665776
, 665777
, 665778
, 665779
, 665918
, 665919
, 665921
, 665926
, 665929
, 665934
, 665942
, 665946
, 665948
, 665950
, 665954
, 665960
, 665961
, 665962
, 665963
, 665964
, 665965
, 665966
, 665967
, 665968
, 665969
, 665970
, 665980
, 665993
, 666124
, 666125
, 666126
, 666127
, 666128
, 666129
, 666132
, 666134
, 666138
, 666139
, 666143
, 666153
, 666154
, 666156
, 666161
, 666165
, 666170
, 666171
, 666173
, 666177
, 666178
, 666179
, 666185
, 666318
, 666319
, 666320
, 666321
, 666327
, 666328
, 666340
, 666347
, 666354
, 666360
, 666364
, 666369
, 666371
, 666373
, 666374
, 666382
, 666385
, 666196
, 485582
, 485589
, 487298
, 486890
, 487524
, 489407
, 419231
, 419234
, 426712
, 426713
, 426714
, 465117
, 465246
, 516890
, 517053
, 521234
, 523626
, 523628
, 523631
, 419232
, 454083
, 487034
, 489173
, 500039
, 502619
, 522354
, 522604
, 523632
, 419233
, 426455
, 426490
, 426680
, 426719
, 446720
, 446775
, 447112
, 449327
, 455132
, 456369
, 456371
, 460892
, 462064
, 462324
, 462614
, 462628
, 462631
, 462632
, 462635
, 462754
, 462861
, 462863
, 463166
, 463255
, 463370
, 464088
, 464246
, 464247
, 464289
, 464290
, 464531
, 464534
, 464737
, 464739
, 464764
, 464765
, 464922
, 465121
, 465355
, 465467
, 465632
, 465635
, 465976
, 466036
, 483503
, 483529
, 483766
, 485534
, 485541
, 485543
, 485558
, 485632
, 485633
, 485634
, 485635
, 486877
, 486887
, 486889
, 487272
, 487290
, 487291
, 487292
, 487295
, 487297
, 487339
, 487341
, 487422
, 487430
, 487460
, 487464
, 487540
, 487549
, 487564
, 487566
, 487780
, 487781
, 488165
, 489189
, 489190
, 489233
, 489333
, 489356
, 489377
, 489384
, 489387
, 489404
, 489405
, 499596
, 499797
, 500037
, 500040
, 500042
, 502781
, 516880
, 516881
, 517174
, 517175
, 517176
, 517177
, 517180
, 517182
, 517183
, 517184
, 517197
, 517272
, 517274
, 517275
, 517465
, 519411
, 520015
, 520118
, 520156
, 520286
, 520310
, 520311
, 520312
, 520459
, 520509
, 520602
, 520684
, 520685
, 520715
, 520716
, 520717
, 520737
, 520747
, 520817
, 520819
, 520871
, 520991
, 522639
, 426453
, 426456
, 426491
, 446342
, 446343
, 446344
, 446438
, 446648
, 446776
, 446902
, 447111
, 453243
, 459500
, 462396
, 462399
, 462627
, 462629
, 462752
, 462755
, 462862
, 462949
, 462950
, 462951
, 463028
, 463253
, 463254
, 463523
, 463524
, 463526
, 464102
, 464291
, 464740
, 464743
, 464763
, 464766
, 464793
, 464855
, 464856
, 464857
, 464921
, 464923
, 464924
, 465118
, 465353
, 465354
, 465633
, 465974
, 466037
, 466039
, 483528
, 484105
, 487447
, 485489
, 485490
, 485554
, 485556
, 485580
, 485588
, 485591
, 485629
, 485654
, 486886
, 487035
, 487296
, 487521
, 487537
, 487538
, 487539
, 487541
, 488487
, 489234
, 489332
, 489334
, 489376
, 489383
, 491263
, 497346
, 499595
, 499753
, 499755
, 499800
, 500036
, 500575
, 502631
, 502757
, 516882
, 517052
, 517057
, 517071
, 517127
, 517192
, 517194
, 517196
, 517271
, 517283
, 517464
, 519412
, 520373
, 520456
, 520458
, 520460
, 520601
, 520818
, 520828
, 520862
, 520872
, 520971
, 522638
, 523627
, 523629
, 523630
, 523633
    )
-- group by 1
-- order by 2
;
------------------------------------------------------------------------------------------------------------------------
/* from sean 3/21 */
------------------------------------------------------------------------------------------------------------------------
CREATE table junk.reveleer_missing_cigna_20240321 as
SELECT
    distinct pqm.id pqm_id
--     ptr.name
-- , count(*) n
FROM
    fdw_member_doc.patient_quality_measures pqm
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
WHERE
    pqm.inserted_at = '2024-03-21 13:51:30'
    ;

SELECT
    pqm.impact_date
  , CASE WHEN cfd.measure_id = 'FMC' THEN pqm.impact_date - 7
         WHEN cfd.measure_id = 'MRP' THEN pqm.impact_date - 30 END AS discharge_date
  , cfd.*
FROM
    junk.reveleer_missing_cigna_20240321 j
    JOIN reveleer_chase_file_details cfd ON cfd.chase_id = j.pqm_id
    JOIN fdw_member_doc.patient_quality_measures pqm ON pqm.id = cfd.chase_id;

-- where cfd.id ISNULL
;

DROP TABLE IF EXISTS _missing_roster_patients;
CREATE TEMP TABLE _missing_roster_patients AS 
SELECT distinct cfd.patient_id
FROM
    reveleer_chase_file_details cfd
    left join fdw_member_doc.md_portal_roster_patients rp
on cfd.patient_id = rp.patient_id
where cfd.inserted_at > now() - '1 week'::interval
and rp.id ISNULL
;
SELECT *
FROM
    _missing_roster_patients ;

------------------------------------------------------------------------------------------------------------------------
/* cleanup */
------------------------------------------------------------------------------------------------------------------------
delete
FROM reveleer_chase_file_details
where chase_id in ( 681076,681077,681079,681102,681103,681106,681197,681230,681246,681251 );

delete
FROM reveleer_attribute_file_details
where sample_id::bigint in ( 681076,681077,681079,681102,681103,681106,681197,681230,681246,681251 );

--
DROP TABLE IF EXISTS _to_delete;
CREATE TEMP TABLE _to_delete AS
select j.pqm_id
FROM
    junk.reveleer_missing_cigna_20240321 j
    join fdw_member_doc.patient_quality_measures pqm on j.pqm_id = pqm.id
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
    left JOIN reveleer_chase_file_details cfd ON cfd.chase_id = pqm.id
where cfd.id isnull
and ptr.name != 'cigna_tn'

DELETE
FROM
    junk.reveleer_missing_cigna_20240321 j
WHERE j.pqm_id in ( SELECT pqm_id FROM _to_delete);

------------------------------------------------------------------------------------------------------------------------
/* from ben */
------------------------------------------------------------------------------------------------------------------------

SELECT ptr.name, qm.code, pqm.*, cfd.*
FROM
    fdw_member_doc.patient_quality_measures pqm
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id
    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
    left JOIN reveleer_chase_file_details cfd ON cfd.chase_id = pqm.id
-- and cfd.id ISNULL
;

DROP TABLE IF EXISTS _to_purge;
CREATE TEMP TABLE _to_purge AS
select id from reveleer_chase_file_details where chase_id IN (598548, 78022);

-- create table junk.reveleer_chase_file_details_deleted as
INSERT
INTO
    junk.reveleer_chase_file_details_deleted (id, reveleer_file_id, patient_id, row_id, health_plan, contract,
                                              member_id, line_of_business, product, sample_id, sequence, measure_id,
                                              chase_id, enrollee_id, member_fname, member_lname, member_mi,
                                              member_gender, member_dob, member_address1, member_address2, member_city,
                                              member_state, member_zip, member_phone, member_cell_phone, member_fax,
                                              member_email, member_last4, user_defined_values, active, retrieval_source,
                                              chart_action, third_party_vendor, provider_id, provider_firstname,
                                              provider_lastname, provider_npi, tin, provider_specialty,
                                              provider_taxonomy, chart_address_phone, chart_address_extension,
                                              chart_address_fax, chart_address_email, chart_address_secondaryphone,
                                              chart_address_grouping, chart_site_id, chart_address_type, chart_address1,
                                              chart_address2, chart_city, chart_state, chart_zip_code, comment,
                                              alternate_address_phone, alternate_address_extension,
                                              alternate_address_fax, alternate_address_email,
                                              alternate_address_secondary_phone, alternate_address_grouping,
                                              alternate_site_id, alternate_address_type, alternate_address1,
                                              alternate_address2, alternate_city, alternate_state, alternate_zipcode,
                                              group_name, contact_name, dos_from, dos_through, chart_address_tag,
                                              chase_tag, chart_filename, inserted_at, updated_at, state_payer_id, yr,
                                              deleted_at)
SELECT id, reveleer_file_id, patient_id, row_id, health_plan, contract,
                                              member_id, line_of_business, product, sample_id, sequence, measure_id,
                                              chase_id, enrollee_id, member_fname, member_lname, member_mi,
                                              member_gender, member_dob, member_address1, member_address2, member_city,
                                              member_state, member_zip, member_phone, member_cell_phone, member_fax,
                                              member_email, member_last4, user_defined_values, active, retrieval_source,
                                              chart_action, third_party_vendor, provider_id, provider_firstname,
                                              provider_lastname, provider_npi, tin, provider_specialty,
                                              provider_taxonomy, chart_address_phone, chart_address_extension,
                                              chart_address_fax, chart_address_email, chart_address_secondaryphone,
                                              chart_address_grouping, chart_site_id, chart_address_type, chart_address1,
                                              chart_address2, chart_city, chart_state, chart_zip_code, comment,
                                              alternate_address_phone, alternate_address_extension,
                                              alternate_address_fax, alternate_address_email,
                                              alternate_address_secondary_phone, alternate_address_grouping,
                                              alternate_site_id, alternate_address_type, alternate_address1,
                                              alternate_address2, alternate_city, alternate_state, alternate_zipcode,
                                              group_name, contact_name, dos_from, dos_through, chart_address_tag,
                                              chase_tag, chart_filename, inserted_at, updated_at, state_payer_id, yr,
                                               now()  deleted_at from reveleer_chase_file_details rcfd
where id in (select id from _to_purge);

DELETE
FROM
    reveleer_chase_file_details
WHERE
    id IN
    ( SELECT id FROM junk.reveleer_chase_file_details_deleted );



DROP TABLE IF EXISTS _to_purge;
CREATE TEMP TABLE _to_purge AS
select id from reveleer_attribute_file_details  where sample_id::bigint IN (598548, 78022);



CREATE TABLE junk.reveleer_missing_cigna_to_resend_20240322
AS
SELECT DISTINCT
    id pqm_id
FROM
    fdw_member_doc.patient_quality_measures pqm
WHERE
    pqm.id IN (598548, 78022);
UPDATE fdw_member_doc.patient_quality_measures
SET
    mco_source_state_payer_id = 342
WHERE
    id = 598548;


SELECT DISTINCT
    pqm.id pqm_id, mco_source_state_payer_id, rp.name
FROM
    fdw_member_doc.patient_quality_measures pqm
join reveleer_projects rp on rp.state_payer_id = pqm.mco_source_state_payer_id
join junk.reveleer_missing_cigna_to_resend_20240322 j on j.pqm_id = pqm.id

SELECT ptr.name, pqm.impact_date, qm.code, pqm.patient_id, qm.id
FROM
    fdw_member_doc.patient_quality_measures pqm
    JOIN fdw_member_doc.msh_state_payers msp ON msp.id = pqm.mco_source_state_payer_id

    JOIN fdw_member_doc.quality_measure_config qmc ON qmc.payer_id = msp.payer_id
        AND qmc.measure_id = pqm.measure_id
        AND qmc.measure_year = pqm.year
    JOIN fdw_member_doc.quality_measures qm ON qm.id = pqm.measure_id
    JOIN fdw_member_doc.supreme_pizza sp ON sp.patient_id = pqm.patient_id
    JOIN public.reveleer_projects ptr ON msp.id = ptr.state_payer_id
    JOIN fdw_member_doc.payers pay ON pay.id = msp.payer_id
        -- don't need a cca for this pull - all left joins below
    LEFT JOIN fdw_member_doc.visits v ON v.patient_id = sp.patient_id
        AND v.date >= '2023-01-01'
        AND v.type_id = 'cca_recon'
    LEFT JOIN fdw_member_doc.msh_cca_worksheets w ON v.id = w.visit_id
        AND w.invalid_reason ISNULL
    LEFT JOIN fdw_member_doc.msh_cca_worksheet_patient_quality_measures mcwpqm ON w.id = mcwpqm.msh_cca_worksheet_id
        AND mcwpqm.deleted_at ISNULL
        AND mcwpqm.patient_quality_measure_id = pqm.id
    LEFT JOIN fdw_member_doc.patient_qm_assessment_answers paa
              ON w.id = paa.msh_cca_worksheet_id AND mcwpqm.patient_qm_assessment_id = paa.patient_qm_assessment_id
    LEFT JOIN fdw_member_doc.msh_quality_measure_assessment_answers qmaa
              ON qmaa.id = paa.qm_assessment_answer_id
    LEFT JOIN fdw_member_doc.msh_quality_measure_assessments qma ON qmaa.msh_quality_measure_assessment_id = qma.id
-- left join reve
WHERE
      pqm.year = 2023
  AND pqm.id = 681270;

SELECT *
FROM
    reveleer_chase_file_details
WHERE
    chase_id = 681270;

;





SELECT DISTINCT
        *
    FROM
        prd.mco_patient_quality_measures mpqm
    WHERE
    mpqm.patient_id = 37930
                 AND mpqm.measure_year = 2023
--                  AND mpqm.measure_id = 12 -- 'OMW'
      AND mpqm.measure_due_date IS NOT NULL
    ;