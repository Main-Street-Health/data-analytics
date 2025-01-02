SELECT DISTINCT ON (rc.id)

    rc.patient_id
  , measure_code
  , rc.id                msh_chase_id
  , rc.external_chase_id rev_chase_id
  , rp.name
  , cfd.inserted_at
  , cfd.user_defined_values
FROM
    analytics.public.reveleer_chases rc
    JOIN reveleer_projects rp ON rc.reveleer_project_id = rp.id
    JOIN reveleer_chase_file_details cfd ON rc.id = cfd.reveleer_chase_id
WHERE
    rc.external_chase_id IN (
                             '24332082', '24332082', '24331707', '24331707', '24331707', '22969998', '22971051',
                             '24043591', '24043591', '24043591', '24332214', '24332214', '24332214', '24052586',
                             '24052586', '23385014', '23385014', '24332538', '24044503', '24044503', '24044503',
                             '22969851', '22978258', '22978258', '22978258', '23652722', '22978575', '22978575',
                             '22978575', '24052697', '24052697', '24052697', '24042310', '24042310', '24042310',
                             '22977383', '24042461', '24042461', '22968570', '24052593', '22970129', '24052636',
                             '24052636', '24052636', '23384987', '23384987', '24042136', '24042136', '24042136',
                             '22969768', '22969809', '24041851', '22970012', '24042229', '24042229', '24042229',
                             '24042229', '24043657', '22976151', '22975607', '24041030', '24041030', '24041030',
                             '22970023', '22970023', '22970023', '24333385', '24333385', '24333385', '22978127',
                             '22968774', '24041424', '24041424', '24041424', '24043554', '22978113', '22976802',
                             '24052432', '24052432', '24052432', '23384594', '23384594', '23384594', '22970045',
                             '22969874', '22969825', '24044643', '24044643', '24044643', '22970053', '24052712',
                             '24052712', '24052712', '24042178', '24052619', '24043922'
        )
ORDER BY  rc.id, cfd.id desc
;

select distinct a from (
select unnest(array['24332082', '24332082', '24331707', '24331707', '24331707', '22969998', '22971051',
                             '24043591', '24043591', '24043591', '24332214', '24332214', '24332214', '24052586',
                             '24052586', '23385014', '23385014', '24332538', '24044503', '24044503', '24044503',
                             '22969851', '22978258', '22978258', '22978258', '23652722', '22978575', '22978575',
                             '22978575', '24052697', '24052697', '24052697', '24042310', '24042310', '24042310',
                             '22977383', '24042461', '24042461', '22968570', '24052593', '22970129', '24052636',
                             '24052636', '24052636', '23384987', '23384987', '24042136', '24042136', '24042136',
                             '22969768', '22969809', '24041851', '22970012', '24042229', '24042229', '24042229',
                             '24042229', '24043657', '22976151', '22975607', '24041030', '24041030', '24041030',
                             '22970023', '22970023', '22970023', '24333385', '24333385', '24333385', '22978127',
                             '22968774', '24041424', '24041424', '24041424', '24043554', '22978113', '22976802',
                             '24052432', '24052432', '24052432', '23384594', '23384594', '23384594', '22970045',
                             '22969874', '22969825', '24044643', '24044643', '24044643', '22970053', '24052712',
                             '24052712', '24052712', '24042178', '24052619', '24043922']) a ) x;
------------------------------------------------------------------------------------------------------------------------
/*  */
------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM
    junk.reveleer_all_chases_20241113;
create UNIQUE INDEX on junk.reveleer_all_chases_20241113(client_chase_key, chase_id)



-- don't exist on our side
SELECT count(*)
FROM
    junk.reveleer_all_chases_20241113 j
left join reveleer_chases rc on j.client_chase_key = rc.id
where
    rc.yr = 2024
    and rc.id ISNULL
;

-- 3546
SELECT count(*)
FROM
    reveleer_chases rc
join junk.reveleer_all_chases_20241113 j on j.client_chase_key = rc.id
where
    rc.yr = 2024
and rc.external_chase_id ISNULL
;
UPDATE reveleer_chases rc
SET
    external_chase_id = j.chase_id, is_confirmed_in_reveleer_system = TRUE, confirmed_in_reveleer_system_at = NOW()
FROM
    junk.reveleer_all_chases_20241113 j
WHERE
      rc.yr = 2024
  AND j.client_chase_key = rc.id
  AND rc.external_chase_id ISNULL
;

SELECT j.*, sp.patient_mbi
FROM
    reveleer_chases rc
join junk.reveleer_all_chases_20241113 j on j.client_chase_key = rc.id
join fdw_member_doc.supreme_pizza sp on sp.patient_id = rc.patient_id
where
    rc.yr = 2024;

-- 6641
SELECT count(*) FROM reveleer_chase_file_details WHERE inserted_at >= now() - '2 days'::interval and measure_id = 'BCS' and is_new;
-- 7440
SELECT count(*) FROM reveleer_cca_pdfs WHERE inserted_at >= now() - '2 days'::interval;
