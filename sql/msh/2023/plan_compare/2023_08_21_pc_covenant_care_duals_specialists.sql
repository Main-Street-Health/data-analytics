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
                         join raw.plan_compare_covenant_care_duals rpcc on pirr.patient_id = rpcc.patient_id
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
                  join raw.plan_compare_covenant_care_duals rpcc on pirr.patient_id = rpcc.patient_id
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






DROP TABLE IF EXISTS _step_6_exclusions_suppressions_duals;

CREATE TEMP TABLE _step_6_exclusions_suppressions_duals AS

select *
from _step_6_suppressions sup
where (sup.patient_pcp_network_status
    or sup.partner_physician_network_status)
  and sup.plan_name ~* 'DSNP|D-SNP'
  and exists(
  select patient_id from raw.plan_compare_covenant_care_duals rpcc where sup.patient_id = rpcc.patient_id);


-- excluded NPIs (Patient PCP, RP PCP, Hospital prefs)
drop table if exists _step_6_patient_excluded_npis;

CREATE TEMP TABLE _step_6_patient_excluded_npis AS


select distinct r.patient_id,
                hospital_1_npi || hospital_2_npi || hospital_3_npi || primary_pcp_npi ||
                partner_physician_npi excluded_npis
from fdw_member_doc_stage.plan_compare_patient_ideon_request r join raw.plan_compare_covenant_care_duals rpcc on r.patient_id = rpcc.patient_id;



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
               mpcp.providerlastname)                                          milliman_provider_name,
       initcap(mpcp.providerorganizationname)                                  milliman_org_name,
       case when d.network_status then 'Specialty IN' else 'Specialty OON' end specialty_network_status,
       mpcp.specialtycode,
       mpcp.specialtytype,
       mpcp.medicareassignment
from _step_6_specialty_duals d
         join raw.milliman_plan_compare_providers mpcp
              on mpcp.npi::bigint = d.provider_npi and mpcp.memberid::int = d.patient_id
where not exists (select *
                  from _step_6_patient_excluded_npis excluded
                  where excluded.patient_id = d.patient_id
                    and mpcp.npi = ANY (excluded_npis));


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
               mpcp.providerlastname)                                          milliman_provider_name,
       initcap(mpcp.providerorganizationname)                                  milliman_org_name,
       case when d.network_status then 'Specialty IN' else 'Specialty OON' end specialty_network_status,
       mpcp.specialtycode,
       mpcp.specialtytype,
       mpcp.medicareassignment
from _step_6_specialty_non_duals d
         join raw.milliman_plan_compare_providers mpcp
              on mpcp.npi::bigint = d.provider_npi and mpcp.memberid::int = d.patient_id
where not exists (select *
                  from _step_6_patient_excluded_npis excluded
                  where excluded.patient_id = d.patient_id
                    and mpcp.npi = ANY (excluded_npis));




