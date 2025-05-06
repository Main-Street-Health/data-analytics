------------------------------------------------------------------------------------------------------------------------
/* tables */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS prd.sure_scripts_patient_phones ;

CREATE TABLE prd.sure_scripts_patient_phones (
    id                          BIGSERIAL PRIMARY KEY,
    patient_id                  BIGINT    NOT NULL,
    phone_number                TEXT      NOT NULL,
-- could add this to speed up batch processing but its only 5s right now
--     sure_scripts_med_history_id BIGINT REFERENCES sure_scripts_med_histories(id),
    inserted_at                 TIMESTAMP NOT NULL DEFAULT NOW(),
    last_received_at            TIMESTAMP NOT NULL DEFAULT NOW()
);



CREATE UNIQUE INDEX ON prd.sure_scripts_patient_phones(patient_id, phone_number);

------------------------------------------------------------------------------------------------------------------------
/* populate */
------------------------------------------------------------------------------------------------------------------------
-- SELECT DISTINCT ON (patient_id , patient_primary_phone_number)
--     patient_id
--   , patient_primary_phone_number
--   , inserted_at
-- FROM
--     sure_scripts_med_history_details
-- WHERE
--     patient_primary_phone_number IS NOT NULL
-- ORDER BY
--     patient_id, patient_primary_phone_number, inserted_at DESC
-- ;

INSERT
INTO
    prd.sure_scripts_patient_phones (patient_id, phone_number, last_received_at)
SELECT
    patient_id::BIGINT
  , patient_primary_phone_number
  , MAX(inserted_at)
FROM
    sure_scripts_med_history_details
WHERE
      patient_primary_phone_number IS NOT NULL
  AND LENGTH(patient_primary_phone_number) = 10
GROUP BY
    1, 2
;


SELECT * FROM prd.sure_scripts_patient_phones;
SELECT length(phone_number), count(*) FROM prd.sure_scripts_patient_phones GROUP BY 1 ;

SELECT *
FROM
    fdw_member_doc.contact_phones;
;
SELECT *
FROM
    contact_phones;
------------------------------------------------------------------------------------------------------------------------
/* member doc */
------------------------------------------------------------------------------------------------------------------------
CREATE TABLE stage.sure_scripts_patient_phones (
    id               BIGINT PRIMARY KEY,
    patient_id       BIGINT    NOT NULL,
    phone_number     TEXT      NOT NULL,
    contact_phone_id BIGINT,
    inserted_at      TIMESTAMP NOT NULL,
    last_received_at TIMESTAMP NOT NULL
);
alter TABLE stage.sure_scripts_patient_phones add COLUMN contact_phone_id BIGINT;

CREATE UNIQUE INDEX ON stage.sure_scripts_patient_phones(patient_id, phone_number);

------------------------------------------------------------------------------------------------------------------------
/* analytics */
------------------------------------------------------------------------------------------------------------------------
call cb.x_util_rebuild_fdw_stage();
SELECT count(*)
FROM
    fdw_member_doc_stage.sure_scripts_patient_phones;
INSERT
INTO
    fdw_member_doc_stage.sure_scripts_patient_phones (id, patient_id, phone_number, inserted_at, last_received_at)
select id, patient_id, phone_number, inserted_at, last_received_at
from prd.sure_scripts_patient_phones;

------------------------------------------------------------------------------------------------------------------------
/* member doc  */
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
/* sync patient phones */
------------------------------------------------------------------------------------------------------------------------

    -- insert new
    drop table if exists _new_contact_phones;
    create temp table _new_contact_phones as
    with ins as (
        insert into public.contact_phones (contact_id, phone_number, type, status, inserted_at, updated_at, created_by_id, updated_by_id, nickname, is_src_surescripts)
            select distinct on (sspp.patient_id) -- if a patient has multiple "self" contacts (some do...) we need this
                pc.contact_id,
                '1' || sspp.phone_number phone_number,
                'home'                   type,
                'active'                 status,
                now()                    inserted_at,
                now()                    updated_at,
                2                        created_by_id,
                2                        updated_by_id,
                sspp.patient_id::text    nickname,
                true                     is_src_surescripts
            from
                stage.sure_scripts_patient_phones sspp
                join public.patient_contacts pc on pc.patient_id = sspp.patient_id
                    and pc.relationship = 'self'
            where
                sspp.contact_phone_id isnull
                and not exists (select 1 from public.contact_phones cp where cp.contact_id = pc.contact_id
                                                                    and cp.phone_number = '1' || sspp.phone_number )
            order by sspp.patient_id, pc.contact_id desc
        returning id contact_phone_id, contact_id, type, nickname
    )
    select
        ins.contact_phone_id,
        ins.contact_id,
        ins.type,
        ins.nickname::bigint patient_id
    from
        ins
    ;

    update public.contact_phones cp
        set nickname = null
    from
        _new_contact_phones ncp
    where
        ncp.contact_phone_id = cp.id
    ;

    -- create patient_contact_phones record(s). Set to primary if one doesn't exist
    INSERT
    INTO
        public.patient_contact_phones (patient_id, contact_phone_id, inserted_at, updated_at, created_by_id, updated_by_id,
                                       "primary", primary_sms)
    SELECT
        patient_id
      , contact_phone_id
      , NOW()                                                                 inserted_at
      , NOW()                                                                 updated_at
      , 2                                                                     created_by_id
      , 2                                                                     updated_by_id
      , NOT EXISTS ( SELECT 1
                     FROM public.patient_contact_phones pcp
                     WHERE pcp.patient_id = dp.patient_id AND pcp."primary" ) "primary"
      , FALSE                                                                 primary_sms
    FROM
        _new_contact_phones dp
        ;

        -- update existing, add src ss
        WITH
            to_upd
                   AS ( SELECT DISTINCT ON (sspp.patient_id) -- if a patient has multiple "self" contacts (some do...) we need this
                            cp.id   contact_phone_id
                          , sspp.id sspp_id
                        FROM
                            stage.sure_scripts_patient_phones sspp
                            JOIN public.patient_contacts pc ON pc.patient_id = sspp.patient_id
                                AND pc.relationship = 'self'
                            JOIN public.contact_phones cp ON cp.contact_id = pc.contact_id
                                AND cp.phone_number = '1' || sspp.phone_number
                        ORDER BY sspp.patient_id, pc.contact_id DESC )
          , cp_upd AS (
            UPDATE public.contact_phones cp
                SET is_src_surescripts = TRUE, updated_at = NOW()
                FROM to_upd tu WHERE tu.contact_phone_id = cp.id
                    AND
                                     NOT cp.is_src_surescripts )
        UPDATE stage.sure_scripts_patient_phones sspp
        SET contact_phone_id = tu.contact_phone_id
        FROM
            to_upd tu
        WHERE
              tu.sspp_id = sspp.id
          AND sspp.contact_phone_id ISNULL
        ;



rd.sure_scripts_patient_phones where phone_number = '0000000000'