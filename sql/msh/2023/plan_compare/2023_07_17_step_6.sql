-- Step 6 of Plan Compare


-- Suppressions 1,2,3,4
-- Show only Medicare Advantage plans (i.e., do not show PDPs or MedSupp plans)
-- Suppress carriers that are under active CMS sanction (i.e., plans not permitted to enroll new members)
-- Suppress carriers/plans where MSH is not at-Risk
-- Suppress plans where patient’s PCP (or Patient's RPL primary PCP) is out-of-network


--- CMS RX coverage table

DROP TABLE IF EXISTS _cms_bid_rx_coverage;

CREATE TEMP TABLE _cms_bid_rx_coverage AS
select replace(bid_id, '_', '-')                       as bid_id,
       case when rx_coverage = 1 then 'Y' else 'N' end as rx_coverage
from ref.cms_bid_rx_coverage;



DROP TABLE IF EXISTS _step_6_suppressions;

CREATE TEMP TABLE _step_6_suppressions AS
-- RP PCP

with rp_pcp as (select pir.api_plan_id,
                       pir.patient_id,
                       pirr.full_name,
                       pirr.referring_partner,
                       pir.referring_partner_id,
                       pir.plan_type,
                       pir.plan_name,
                       pir.carrier_name,
                       pir.plan_identifier,
                       pirr.partner_physician,
                       pirr.partner_physician_npi,
                       pirp.network_status
                from fdw_member_doc_stage.plan_compare_patient_ideon_response pir
                         join fdw_member_doc_stage.plan_compare_patient_ideon_response_providers pirp
                              on pirp.patient_id = pir.patient_id and pirp.api_plan_id = pir.api_plan_id
                         join fdw_member_doc_stage.plan_compare_patient_ideon_request pirr
                              on pirr.patient_id = pir.patient_id
                         join _cms_bid_rx_coverage bid_rx on bid_rx.bid_id = substring(pir.plan_identifier, 1, 11)
                where (plan_type not in ('Medicare Prescription Drug Plan') and plan_identifier !~* 'H0251')
                  and pirr.partner_physician_npi::bigint = pirp.provider_npi
                  and rx_coverage = 'Y'),
     pt_pcp as (

-- pt pcp
         select pir.api_plan_id,
                pir.patient_id,
                pirr.full_name,
                pir.plan_type,
                pir.referring_partner_id,
                pirr.referring_partner,
                pir.carrier_name,
                pir.plan_name,
                pir.plan_identifier,
                pirr.partner_physician,
                pirr.partner_physician_npi,
                pirr.primary_pcp,
                pirr.primary_pcp_npi,
                pirp.network_status
         from fdw_member_doc_stage.plan_compare_patient_ideon_response pir
                  join fdw_member_doc_stage.plan_compare_patient_ideon_response_providers pirp
                       on pirp.patient_id = pir.patient_id and pirp.api_plan_id = pir.api_plan_id
                  join fdw_member_doc_stage.plan_compare_patient_ideon_request pirr on pirr.patient_id = pir.patient_id
                  join _cms_bid_rx_coverage bid_rx on bid_rx.bid_id = substring(pir.plan_identifier, 1, 11)
         where (plan_type not in ('Medicare Prescription Drug Plan') and plan_identifier !~* 'H0251')
           and pirr.primary_pcp_npi::bigint = pirp.provider_npi
           and rx_coverage = 'Y')

select rp_pcp.patient_id,
       rp_pcp.full_name      patient_name,
       rp_pcp.referring_partner_id,
       rp_pcp.referring_partner,
       rp_pcp.api_plan_id,
       rp_pcp.plan_identifier,
       rp_pcp.plan_name,
       rp_pcp.plan_type,
       rp_pcp.partner_physician,
       rp_pcp.partner_physician_npi,
       rp_pcp.carrier_name,
       rp_pcp.network_status partner_physician_network_status,
       pt_pcp.primary_pcp,
       pt_pcp.primary_pcp_npi,
       pt_pcp.network_status patient_pcp_network_status


from rp_pcp
         join pt_pcp on rp_pcp.api_plan_id = pt_pcp.api_plan_id and
                        rp_pcp.referring_partner_id = pt_pcp.referring_partner_id and
                        pt_pcp.patient_id = rp_pcp.patient_id;

CREATE INDEX ON _step_6_suppressions (patient_id);
CREATE INDEX ON _step_6_suppressions (patient_id, referring_partner_id);
CREATE INDEX ON _step_6_suppressions (api_plan_id);
CREATE INDEX ON _step_6_suppressions (plan_identifier);


--- note Leslie will give a file for this so load into ref
--
-- Exclusions:	Details
-- 1	If patient is a regular non-Medicaid beneficiary, do not show DSNP or ISNP plans
-- 2	If patient is a regular non-Medicaid beneficiary, do not show CSNP plans
-- 3	If patient is a Dual, only show DSNP plans
-- 4	If patient is a long-term care/institutionalized, show both MAPD plans and ISNP plans
-- 5	If patient is a long-term care/institutionalized AND a Dual, show both DSNP plans and ISNP plans
--
DROP TABLE IF EXISTS _step_6_dual_patients;


CREATE TEMP TABLE _step_6_dual_patients as
select *
from (values (7007),
             (7419),
             (7458),
             (7758),
             (7862),
             (7994),
             (8533),
             (8852),
             (8862),
             (8897),
             (16719),
             (16721),
             (29605),
             (36841),
             (43901),
             (46297),
             (46308),
             (46309),
             (47945),
             (49826),
             (49833),
             (49847),
             (49855),
             (49893),
             (49895),
             (49897),
             (49918),
             (49930),
             (49934),
             (49952),
             (49980),
             (50007),
             (50040),
             (50053),
             (50087),
             (50108),
             (50109),
             (50112),
             (50113),
             (50120),
             (50121),
             (50131),
             (50143),
             (50149),
             (50151),
             (50163),
             (50165),
             (50172),
             (50173),
             (50180),
             (50185),
             (50193),
             (50194),
             (50202),
             (50208),
             (50214),
             (50218),
             (50219),
             (50226),
             (50243),
             (50250),
             (50258),
             (50274),
             (50279),
             (50288),
             (50308),
             (50313),
             (50320),
             (50334),
             (50336),
             (50348),
             (50350),
             (50354),
             (50366),
             (50369),
             (50379),
             (52007),
             (52234),
             (52268),
             (52348),
             (52740),
             (52796),
             (53168),
             (54396),
             (54417),
             (54430),
             (54435),
             (54465),
             (54485),
             (54496),
             (54501),
             (54514),
             (54518),
             (54541),
             (54549),
             (54583),
             (54617),
             (54625),
             (54631),
             (54697),
             (54736),
             (54761),
             (54800),
             (54809),
             (54810),
             (54824),
             (54845),
             (54851),
             (54855),
             (62525),
             (63869),
             (71163),
             (80395),
             (80487),
             (82414),
             (85185),
             (91157),
             (95682),
             (104664),
             (104995),
             (105035),
             (105228),
             (105422),
             (106038),
             (106705),
             (108537),
             (108615),
             (123052),
             (123349),
             (123356),
             (123658),
             (123730),
             (123731),
             (123978),
             (124150),
             (124189),
             (124307),
             (124316),
             (124322),
             (124373),
             (124376),
             (124528),
             (124611),
             (124613),
             (124632),
             (124836),
             (158429),
             (158437),
             (158445),
             (158459),
             (158461),
             (158478),
             (158502),
             (158506),
             (158523),
             (158538),
             (158547),
             (158565),
             (158573),
             (158574),
             (158575),
             (158576),
             (158582),
             (158584),
             (158589),
             (158607),
             (158612),
             (158613),
             (158616),
             (158617),
             (158618),
             (158629),
             (158645),
             (158650),
             (158666),
             (158668),
             (158671),
             (158672),
             (158677),
             (158697),
             (158698),
             (158700),
             (158722),
             (158728),
             (158750),
             (158755),
             (158762),
             (158792),
             (158843),
             (158848),
             (158853),
             (158888),
             (158889),
             (158894),
             (158902),
             (158903),
             (158915),
             (158927),
             (158928),
             (158952),
             (158955),
             (158959),
             (158999),
             (159007),
             (159034),
             (159056),
             (159078),
             (159087),
             (159089),
             (159094),
             (159112),
             (159113),
             (159114),
             (159116),
             (159118),
             (159127),
             (159130),
             (159138),
             (159157),
             (159175),
             (159180),
             (167379),
             (167386),
             (167393),
             (167399),
             (167401),
             (167405),
             (167411),
             (167412),
             (167414),
             (167425),
             (167427),
             (167431),
             (167433),
             (167437),
             (167449),
             (167463),
             (167476),
             (167482),
             (167501),
             (167508),
             (167532),
             (167552),
             (167554),
             (167564),
             (167568),
             (167569),
             (167575),
             (167577),
             (167588),
             (167597),
             (167600),
             (167605),
             (167606),
             (167611),
             (167614),
             (167618),
             (167620),
             (167635),
             (167637),
             (167643),
             (167660),
             (167664),
             (167666),
             (167678),
             (167684),
             (167689),
             (167691),
             (167699),
             (167701),
             (167704),
             (167708),
             (167712),
             (167713),
             (167714),
             (167721),
             (167725),
             (167748),
             (167750),
             (167762),
             (167764),
             (167765),
             (167779),
             (167794),
             (167800),
             (167808),
             (167810),
             (167817),
             (167828),
             (167832),
             (167850),
             (167874),
             (167876),
             (167878),
             (167886),
             (167905),
             (167926),
             (167931),
             (167933),
             (167939),
             (167950),
             (167953),
             (167960),
             (167968),
             (167972),
             (167976),
             (167992),
             (167993),
             (167994),
             (167996),
             (168008),
             (168010),
             (168015),
             (168016),
             (168021),
             (168028),
             (168034),
             (168044),
             (168053),
             (168056),
             (168063),
             (168068),
             (168081),
             (168084),
             (168094),
             (168098),
             (168099),
             (168106),
             (168123),
             (168126),
             (168127),
             (168133),
             (168158),
             (168160),
             (168166),
             (168167),
             (168175),
             (168177),
             (168183),
             (168184),
             (168194),
             (168201),
             (168205),
             (168208),
             (168215),
             (168226),
             (168231),
             (168248),
             (168250),
             (168266),
             (168268),
             (168269),
             (168287),
             (168289),
             (168300),
             (168302),
             (168307),
             (168313),
             (168316),
             (168324),
             (168331),
             (168347),
             (168348),
             (168353),
             (168354),
             (168369),
             (168370),
             (168371),
             (168375),
             (168382),
             (168384),
             (168391),
             (168392),
             (168396),
             (168403),
             (168405),
             (168409),
             (168410),
             (168418),
             (168434),
             (168435),
             (168441),
             (168447),
             (168455),
             (168462),
             (168480),
             (168485),
             (168492),
             (168493),
             (168502),
             (168506),
             (168508),
             (168511),
             (168514),
             (168525),
             (168535),
             (168538),
             (168541),
             (168544),
             (168545),
             (168552),
             (168565),
             (168569),
             (168573),
             (168585),
             (168593),
             (168597),
             (168598),
             (168606),
             (168608),
             (168609),
             (168616),
             (168627),
             (168628),
             (168636),
             (168638),
             (168649),
             (168653),
             (168672),
             (168676),
             (168677),
             (168679),
             (168685),
             (168686),
             (168688),
             (168699),
             (168716),
             (168717),
             (168718),
             (168719),
             (168720),
             (168727),
             (168731),
             (168737),
             (168741),
             (168747),
             (168748),
             (168750),
             (168751),
             (168758),
             (168770),
             (168774),
             (168784),
             (168794),
             (168796),
             (168799),
             (168801),
             (168809),
             (193895),
             (194079),
             (194082),
             (194141),
             (194218),
             (194704),
             (194709),
             (194966),
             (194973),
             (195047),
             (195213),
             (195229),
             (195477),
             (195573),
             (195710),
             (195725),
             (195752),
             (195816),
             (195818),
             (196020),
             (196083),
             (196160),
             (196539),
             (196953),
             (197028),
             (197170),
             (197527),
             (197560),
             (197636),
             (197840),
             (197943),
             (198032),
             (249682),
             (268026),
             (285264),
             (289220),
             (289601),
             (289954),
             (298031),
             (311250),
             (311920),
             (317654),
             (317858),
             (318136),
             (333492),
             (333499),
             (334739),
             (338366),
             (338748),
             (339291),
             (339653),
             (339711),
             (339823),
             (340003),
             (340436),
             (342551),
             (343545),
             (345433),
             (346618),
             (347068),
             (354481),
             (354734),
             (354880),
             (380870),
             (392120),
             (432307),
             (432336)) x(patient_id);

DROP TABLE IF EXISTS _step_6_exclusions_suppressions_non_duals;

CREATE TEMP TABLE _step_6_exclusions_suppressions_non_duals AS

select *
from _step_6_suppressions sup
where (sup.patient_pcp_network_status
    or sup.partner_physician_network_status)
  and sup.plan_name !~* 'SNP'
  and not exists(select patient_id from _step_6_dual_patients s6dp where sup.patient_id = s6dp.patient_id);



DROP TABLE IF EXISTS _step_6_exclusions_suppressions_duals;

CREATE TEMP TABLE _step_6_exclusions_suppressions_duals AS

select *
from _step_6_suppressions sup
where (sup.patient_pcp_network_status
    or sup.partner_physician_network_status)
  and sup.plan_name ~* 'DSNP|D-SNP'
  and exists(select patient_id from _step_6_dual_patients s6dp where sup.patient_id = s6dp.patient_id);


-- excluded NPIs (Patient PCP, RP PCP, Hospital prefs)
drop table if exists _step_6_patient_excluded_npis;

CREATE TEMP TABLE _step_6_patient_excluded_npis AS


select distinct patient_id,
                hospital_1_npi || hospital_2_npi || hospital_3_npi || primary_pcp_npi ||
                partner_physician_npi excluded_npis
from fdw_member_doc_stage.plan_compare_patient_ideon_request;


-- final speciality non duals
DROP TABLE IF EXISTS _step_6_specialty_non_duals;

CREATE TEMP TABLE _step_6_specialty_non_duals AS

select nd.*, pirp.provider_npi, pirp.provider_addresses, pirp.id, network_status
from _step_6_exclusions_suppressions_non_duals nd
         join fdw_member_doc_stage.plan_compare_patient_ideon_response_providers pirp
              on pirp.patient_id = nd.patient_id and pirp.api_plan_id = nd.api_plan_id;


--  speciality non duals summary
DROP TABLE IF EXISTS _step_6_specialty_non_duals_summary;

CREATE TEMP TABLE _step_6_specialty_non_duals_summary AS


select patient_id,
       api_plan_id,
       count(1)                                   total_plans,
       count(1) filter (where network_status)     plans_specialty_in,
       count(1) filter (where not network_status) plans_specialty_out
from _step_6_specialty_non_duals
group by 1, 2;


-- send to Leslie - non Duals
select nd_summary.patient_id,
       s6.patient_name,
       nd_summary.api_plan_id,
       s6.referring_partner,
       s6.plan_name,
       s6.plan_identifier,
       s6.plan_type,
       'non_dual'                                                           patient_type,
       round(((plans_specialty_in / total_plans::float) * 100)::decimal, 2) specialty_percentage
from _step_6_specialty_non_duals_summary nd_summary
         join _step_6_exclusions_suppressions_non_duals s6
              on nd_summary.patient_id = s6.patient_id and nd_summary.api_plan_id = s6.api_plan_id
order by patient_id, specialty_percentage desc, plan_name;


-- final speciality  duals
DROP TABLE IF EXISTS _step_6_specialty_duals;

CREATE TEMP TABLE _step_6_specialty_duals AS

select nd.*, pirp.provider_npi, pirp.provider_addresses, pirp.id, network_status
from _step_6_exclusions_suppressions_duals nd
         join fdw_member_doc_stage.plan_compare_patient_ideon_response_providers pirp
              on pirp.patient_id = nd.patient_id and pirp.api_plan_id = nd.api_plan_id;


--  speciality  duals summary
DROP TABLE IF EXISTS _step_6_specialty_duals_summary;

CREATE TEMP TABLE _step_6_specialty_duals_summary AS


select patient_id,
       api_plan_id,
       count(1)                                   total_plans,
       count(1) filter (where network_status)     plans_specialty_in,
       count(1) filter (where not network_status) plans_specialty_out
from _step_6_specialty_duals
group by 1, 2;


-- send to Leslie -  Duals
select nd_summary.patient_id,
       s6.patient_name,
       nd_summary.api_plan_id,
       s6.referring_partner,
       s6.plan_name,
       s6.plan_identifier,
       s6.plan_type,
       'dual'                                                               patient_type,
       round(((plans_specialty_in / total_plans::float) * 100)::decimal, 2) specialty_percentage
from _step_6_specialty_duals_summary nd_summary
         join _step_6_exclusions_suppressions_duals s6
              on nd_summary.patient_id = s6.patient_id and nd_summary.api_plan_id = s6.api_plan_id
order by patient_id, specialty_percentage desc, plan_name;


--- Duals Detailed Specialty
select d.patient_id,
       d.patient_name,
       d.referring_partner_id,
       d.referring_partner,
       d.api_plan_id,
       d.plan_identifier                                                       bid_id,
       d.plan_name,
       d.plan_type,
       d.primary_pcp,
       d.primary_pcp_npi,
       case when d.patient_pcp_network_status then 'PCP IN' else 'PCP OON' end pcp_network_status,
       mpcp.npi                                                                specialty_npi,
       initcap(mpcp.providerfirstname || ' ' || coalesce(mpcp.providermiddlename, ' ') || ' ' ||
       mpcp.providerlastname)                                                   milliman_provider_name,
       initcap(mpcp.providerorganizationname) milliman_org_name,
       case when d.network_status then 'Specialty IN' else 'Specialty OON' end specialty_network_status,
       mpcp.specialtycode,
       mpcp.specialtytype,
       mpcp.medicareassignment
from _step_6_specialty_duals d
         join raw.milliman_plan_compare_providers mpcp
              on mpcp.npi::bigint = d.provider_npi and mpcp.memberid::int = d.patient_id
where not exists (select * from _step_6_patient_excluded_npis excluded where excluded.patient_id = d.patient_id and mpcp.npi = ANY(excluded_npis));



-- Non Duals


select d.patient_id,
       d.patient_name,
       d.referring_partner_id,
       d.referring_partner,
       d.api_plan_id,
       d.plan_identifier                                                       bid_id,
       d.plan_name,
       d.plan_type,
       d.primary_pcp,
       d.primary_pcp_npi,
       case when d.patient_pcp_network_status then 'PCP IN' else 'PCP OON' end pcp_network_status,
       mpcp.npi                                                                specialty_npi,
       initcap(mpcp.providerfirstname || ' ' || coalesce(mpcp.providermiddlename, ' ') || ' ' ||
       mpcp.providerlastname)                                                   milliman_provider_name,
       initcap(mpcp.providerorganizationname) milliman_org_name,
       case when d.network_status then 'Specialty IN' else 'Specialty OON' end specialty_network_status,
       mpcp.specialtycode,
       mpcp.specialtytype,
       mpcp.medicareassignment
from _step_6_specialty_non_duals d
         join raw.milliman_plan_compare_providers mpcp
              on mpcp.npi::bigint = d.provider_npi and mpcp.memberid::int = d.patient_id
where not exists (select * from _step_6_patient_excluded_npis excluded where excluded.patient_id = d.patient_id and mpcp.npi = ANY(excluded_npis));




