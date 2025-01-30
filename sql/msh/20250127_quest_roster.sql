CREATE PROCEDURE sp_stage_quest_roster_patients()
LANGUAGE plpgsql
AS
$$
BEGIN
    drop table if exists _pats;
    create temp table _pats as
    select distinct on (qpm.patient_id)
        qpm.patient_id,
        mco.payer_id,
        left(upper(case
            when rm.subscriber_id ~* '[a-zA-Z]{3}' then -- yeah this is stupid
                substring(rm.subscriber_id from '.{3}(\d+)00')
            else rm.subscriber_id end),15)                                member_id,
        upper(rm.subscriber_id)                                           raw_sub_id,
        left(upper(gm.mbi), 15)                                           medicare_id,
        left(gm.ssn, 9)                                                   ssn,
        initcap(left(gm.first_name,25))                                   member_fn,
        initcap(left(gm.last_name,35))                                    member_ln,
        initcap(left(gm.first_name,25))                                   subscriber_fn,
        initcap(left(gm.last_name,35))                                    subscriber_ln,
        initcap(left(gm.middle_name, 1))                                  member_middle_initial,
        replace(gm.date_of_birth::text,'-','')                            dob,
        left(coalesce(gm.gender,'U'), 1)                                  gender,
        coalesce(case when cip.cms_plan_type ~* 'hmo' then 'MC'
            when cip.cms_plan_type ~* 'ppo' then 'PP'
            end,'MC')                                                     program_type, -- if unknown... do HMO (they say it is required)
        coalesce(rpad(replace(cip.start_date::text,'-',''),8),'20240101') effective_date, -- this is... a swag at best
        -- use RPL address if address is missing
        replace(left(nullif(upper(case when pa.line1 is not null and pa.line1 not in ('.', 'Unknown') then pa.line1 else rp.address1 end),''), 30),',','')  address_line_1,
        replace(left(nullif(upper(case when pa.line1 is not null and pa.line1 not in ('.', 'Unknown') then pa.line2 else rp.address2 end),''), 30),',','')  address_line_2,
        replace(left(nullif(upper(case when pa.line1 is not null and pa.line1 not in ('.', 'Unknown') then pa.city else rp.city end),''), 25),',','')       address_city,
        replace(left(nullif(upper(case when pa.line1 is not null and pa.line1 not in ('.', 'Unknown') then pa.state else rp.state end),''), 2),',','')      address_state,
        replace(left(nullif(upper(case when pa.line1 is not null and pa.line1 not in ('.', 'Unknown') then pa.postal_code else rp.zip end),''), 12),',','') address_postal_code,
        NOW()      inserted_at,
        NOW()      updated_at,
        '01'       relationship_to_subscriber,
        '99991231' expiration_date,
        null::text relationship_code,
        null::text provider_id,
        null::text employer_code,
        null::text division_number,
        null::text plan_group_1,
        null::text plan_group_2,
        null::text plan_group_3,
        null::text group_number,
        null::text region_code,
        null::text product_code,
        null::text medicaid_id,
        null::text subscriber_area_code,
        null::text subscriber_exchange,
        null::text subscriber_tele,
        null::text subscriber_extension
    from
        fdw_member_doc.qm_patient_measures qpm
        join fdw_member_doc.supreme_pizza sp on sp.patient_id = qpm.patient_id
        join fdw_member_doc.qm_mco_patient_measures mco on mco.id = qpm.mco_patient_measure_id
        join fdw_member_doc_analytics.risk_members rm on rm.patient_id = qpm.patient_id
            and rm.payer_id = mco.payer_id
            and rm.subscriber_id is not null
        join gmm.global_members gm on gm.patient_id = qpm.patient_id
        left join fdw_member_doc.cvg_patient_insurances cpi on cpi.patient_id = qpm.patient_id and cpi.is_primary
        left join fdw_member_doc.cvg_insurance_plans cip on cip.id = cpi.insurance_plan_id and cip.payer_id = mco.payer_id
        left join fdw_member_doc.patient_addresses pa on pa.patient_id = qpm.patient_id
        left join fdw_member_doc.referring_partners rp on rp.id = sp.primary_referring_partner_id
    where
        sp.is_md_portal_full
    ;

    INSERT
    INTO
        quest_roster_patients (patient_id, member_id, relationship_code, ssn, dob, member_ln, member_fn,
                               member_middle_initial, gender, relationship_to_subscriber, program_type, effective_date, expiration_date,
                               provider_id, employer_code, division_number, plan_group_1, plan_group_2, plan_group_3,
                               group_number, region_code, product_code, medicare_id, medicaid_id, address_line_1,
                               address_line_2, address_city, address_state, address_postal_code, subscriber_fn,
                               subscriber_ln, subscriber_area_code, subscriber_exchange, subscriber_tele,
                               subscriber_extension, inserted_at, updated_at)
    SELECT
        patient_id, member_id, relationship_code, ssn, dob, member_ln, member_fn,
        member_middle_initial, gender, relationship_to_subscriber, program_type, effective_date, expiration_date,
        provider_id, employer_code, division_number, plan_group_1, plan_group_2, plan_group_3,
        group_number, region_code, product_code, medicare_id, medicaid_id, address_line_1,
        address_line_2, address_city, address_state, address_postal_code, subscriber_fn,
        subscriber_ln, subscriber_area_code, subscriber_exchange, subscriber_tele,
        subscriber_extension, inserted_at, updated_at
    FROM
        _pats
    ON CONFLICT DO NOTHING
    ;
    DROP TABLE IF EXISTS _pats;
END;
$$;


SELECT * FROM quest_roster_patients;
-- update quest_roster_patients SET quest_roster_id = null;


-- drop table quest_roster_patients;
-- drop table quest_rosters;
-- delete from schema_migrations where version = '20250128191358';
select * from schema_migrations where version = '20250128191358';
-- 20250128191358_quest_rosters.exs

------------------------------------------------------------------------------------------------------------------------
/* OLD below */
------------------------------------------------------------------------------------------------------------------------

    drop table if exists _header;
    create temp table _header as
    select concat('H',
        'MAINST',
        replace(current_date::text,'-',''),
        rpad('',7), -- long record indicator,
        rpad('',28), -- filler,
        rpad('',2), -- quest internal no of months,
        rpad('',4), -- filler
        rpad('',1), -- quest internal expire flag
        rpad('',2), -- filler,
        rpad('',60), -- quest internal file name
        rpad('',307)) field; -- fillter

    drop table if exists _eligiblity;
    create temp table _eligiblity as
    select
        concat(
        'E',                                                                    -- eligibility
        rpad(subscriber_id,15),                                                 -- member id
        rpad('',3),                                                             -- relationship code
        rpad(coalesce(ssn,''),9),                                               -- ssn
        rpad(replace(date_of_birth::text,'-',''),8),                            -- dob
        rpad(initcap(last_name),35),                                            -- member ln
        rpad(initcap(first_name),25),                                           -- member fn
        rpad(coalesce(initcap(left(middle_name,1)),''),1),                      -- member middle initial
        upper(coalesce(gender,'U')),                                            -- gender
        '01',                                                                   -- relationship to subscriber (01 = self)
        rpad(coalesce(program_type,''),3),                                      -- not sure all of the options here, but mapping HMO and PPO
        coalesce(rpad(replace(effective_date::text,'-',''),8),'20240101'),      -- coverage start date
        '99991231',                                                             -- expiration date
        rpad('',15),                                                            -- provider ID
        rpad('',12),                                                            -- employer code
        rpad('',10),                                                            -- division number
        rpad('',15),                                                            -- plan group 1
        rpad('',10),                                                            -- plan group 2
        rpad('',10),                                                            -- plan group 3
        rpad('',10),                                                            -- group number
        rpad('',10),                                                            -- region code
        rpad('',10),                                                            -- product code
        rpad(coalesce(mbi,''),15),                                              -- optional, but including
        rpad('',15),                                                            -- medicaid id,
        rpad(coalesce(line1,''),30),                                            -- address line1
        rpad(coalesce(line2,''),30),                                            -- address line2
        rpad(coalesce(city,''),25),                                             -- address city
        rpad(coalesce(state,''),2),                                             -- address state
        rpad(coalesce(postal_code,''),12),                                      -- address postal_code
        rpad(initcap(first_name),25),                                           -- subscriber fn
        rpad(initcap(last_name),35),                                            -- subscriber ln
        rpad('',3),                                                             -- sub area code
        rpad('',3),                                                             -- sub exchange
        rpad('',4),                                                             -- sub tele
        rpad('',6)                                                              -- sub extension
        ) field
    from _pats;

    drop table if exists _trailer;
    create temp table _trailer as
    select
        concat('T',
        rpad(((select count(*) from _eligiblity) + 2)::text, 10), -- number of records in eligibility + 1 header and 1 footer
        rpad('',415)) field
    ;

    -- qa length (all should be 426)
    select 'header',      length(field), count(*) records from _header     group by 1,2 union all
    select 'eligibility', length(field), count(*) records from _eligiblity group by 1,2 union all
    select 'trailer',     length(field), count(*) records from _trailer    group by 1,2;

    drop table if exists _output;
    create temp table _output as
    select 1 sortkey, * from _header
    union all
    select 2, * from _eligiblity
    union all
    select 3, * from _trailer;

    -- remove any line feeds... important otherwise you'll get a few bad records
    update _output
        set field = regexp_replace(field, '[\n\r]+', '', 'g' )
    where
        field ~* '[\n\r]+'
    ;

    select field from _output order by sortkey;