{alicia flores, john yager, andrea leoanard, carly jacobs, katie fuller,
lee logan, lee cottrell, madison cowan-banker, madison pennington, nicole dickinson, self referral, tyler phillips}


drop table junk.rn_providers_to_delete_20240920;
create table junk.rn_providers_to_delete_20240920 as
with dupes as ( SELECT
                    npi
                  , COUNT(*)
                FROM
                    rn_providers p
                GROUP BY npi
                HAVING
                    COUNT(*) > 1 )
, ordered as ( SELECT
                  rp.id
                 , rp.first_name
                 , rp.last_name
                 , rp.npi
                 , rp.inserted_at
                 , rp.updated_at
                 , u.full_name created_by_user_name
                 , ROW_NUMBER() OVER (PARTITION BY d.npi ORDER BY LENGTH(rp.first_name), rp.id) ranked
               FROM
                   dupes d
                   JOIN rn_providers rp ON rp.npi = d.npi
                   LEFT JOIN users u ON u.id = rp.created_by_id
               ORDER BY d.npi, ranked )
SELECT *
FROM
    ordered ;
;


DELETE
FROM
    rn_provider_insurances
WHERE rn_provider_insurances.rn_provider_id in (SELECT id FROM junk.rn_providers_to_delete_20240920 WHERE ranked > 1);
DELETE
FROM
    rn_facility_providers
WHERE rn_provider_id in (SELECT id FROM junk.rn_providers_to_delete_20240920 WHERE ranked > 1);

WITH
    grps AS ( SELECT
                  j_to_del.id  id_to_del
                , j_to_keep.id id_to_keep
              FROM
                  junk.rn_providers_to_delete_20240920 j_to_del
                  JOIN junk.rn_providers_to_delete_20240920 j_to_keep ON j_to_del.npi = j_to_keep.npi
              WHERE
                    j_to_del.ranked > 1
                AND j_to_keep.ranked = 1 )
UPDATE rn_patient_referrals rpr
SET
    referring_provider_id = g.id_to_keep
FROM
    grps g
WHERE
    rpr.referring_provider_id = g.id_to_del
    ;


DELETE
FROM
    rn_providers
WHERE id in (SELECT id FROM junk.rn_providers_to_delete_20240920  WHERE ranked > 1);

