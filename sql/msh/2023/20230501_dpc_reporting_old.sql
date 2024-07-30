------------- patient base list
with _patient_base as(
     select distinct on (mbi) ---- dedup patients with same MBI, maybe add dob?
            patient_id,
            mbi,
            patient_name,
            patient_market,
            date_of_birth,
            primary_referring_partner_name,
            organization_name,
            patient_attribution_status,
            ---attribution_substatus,
            md_primary_physician_name,
            payer_name,
            coverage_source,
            ins_primary_subscriber_id,
            'MBI' as cat
     from fdw_member_doc_ent.patients
     where (patient_attribution_status~*'ma_at_risk' or patient_attribution_status='om_at_risk_yes' or (patient_attribution_status in ('om_at_risk_no','om_at_risk_pending') and patient_market~*'Tennessee'))
            and substatus !='deceased'
            and status!='inactive'---- remove inactive patients
            and organization_name not in ('Bassett Healthcare Network' ,'Galen Medical Group')
--             and {{patient_market}}
--             and {{organization_name}}
--             and {{primary_referring_partner_name}}
     union
     select
            patient_id,
            mbi,
            patient_name,
            patient_market,
            date_of_birth,
            primary_referring_partner_name,
            organization_name,
            patient_attribution_status,
            ---attribution_substatus,
            md_primary_physician_name,
            payer_name,
            coverage_source,
            ins_primary_subscriber_id,
            'No MBI' as Cat
     from fdw_member_doc_ent.patients
     where (patient_attribution_status~*'ma_at_risk' or patient_attribution_status='om_at_risk_yes' or (patient_attribution_status in ('om_at_risk_no','om_at_risk_pending') and patient_market~*'Tennessee'))
            and substatus !='deceased'
            and mbi is null
            and status!='inactive'
            and organization_name not in ('Bassett Healthcare Network' ,'Galen Medical Group')  ----- per Jake, remove them for entire report
--             and {{patient_market}}
--             and {{organization_name}}
--             and {{primary_referring_partner_name}}),

_patient as (
   select pb.patient_id,
          pb.ins_primary_subscriber_id,
          pb.cat,
          max(rfrnc_yr) as coverage_year,
          'dpc' as source
   from _patient_base pb
   left join dpoc_coverage sc on pb.patient_id = sc.patient::bigint
   group by 1,2,3),


----------Get patient ever sent DPC
DPC_history as (
    SELECT
               u.mbi,
               dpc_history,
               max(id) filter ( where row=1 ) as dpoc_id,
               max(inserted_at) filter ( where row=1 ) as recent_inserted_at
    from (
    select p.mbi,
           true as DPC_history,
           dp.npi,
           dp.first_name,
           dp.last_name,
           j.inserted_at::date,
           j.id,
           row_number() over (partition by p.mbi order by p.mbi,j.inserted_at desc) as row
    FROM
         public.dpoc_bulk_export_jobs j
         JOIN dpoc_practitioners dp ON j.dpoc_practitioner_id = dp.id
         JOIN dpoc_practitioner_group_patients gp ON gp.npi = dp.npi
         JOIN fdw_member_doc_ent.patients p ON p.mbi = gp.mbi
         JOIN fdw_member_doc.msh_physicians doc ON doc.npi::TEXT = dp.npi
        ) u
    group by 1,2
),

------------Get recent DPC from patient DPC history
recent_base as(
    select
           j.mbi,
           MAX(je.id) IS NOT NULL             has_dpc_error,
           STRING_AGG(DISTINCT je.error, ', ') errors
    FROM DPC_history j
    LEFT JOIN dpoc_bulk_export_job_errors je ON je.dpoc_bulk_export_job_id = j.dpoc_id
        AND j.mbi = je.mbi
    group by 1)
,

final as(
select pb.*,
       p.coverage_year,
       p.source,
       case when p.source is null then pb.coverage_source else p.source end as patient_cov,
       dh.DPC_history,
      --- dh.npi,
       ---concat(dh.first_name,' ',dh.last_name) as practitioner_name,
       rb.has_dpc_error,
       rb.errors
from _patient_base pb
left join _patient p on pb.patient_id=p.patient_id
left join DPC_history Dh on pb.mbi = Dh.mbi
left join recent_base rb on Dh.mbi = rb.mbi
)

select 'All Patients Classified as MA or as OM & MSSP or OM in TN' as bucket, count(*) as num, 1 as perc --,null  as perc_previous
from final
union all
select '--Patients without an MBI ' as bucket, count(*) filter ( where mbi is null ) as num, count(*) filter ( where mbi is null )*1./count(*) as perc --,count(*) filter ( where mbi is null )*1./count(*) as perc_previous
from final
union all
select '-->Patients without an MBI or subscriber ID ' as bucket, count(*) filter ( where mbi is null and ins_primary_subscriber_id is  null ) as num, count(*) filter ( where mbi is null and ins_primary_subscriber_id is  null )*1./count(*) as perc --,count(*) filter ( where mbi is null and ins_primary_subscriber_id is  null )*1./count(*) as perc_previous
from final
union all
select '-->Patients without an MBI but with subscriber ID ' as bucket, count(*) filter ( where mbi is null and ins_primary_subscriber_id is not null ) as num, count(*) filter ( where mbi is null and ins_primary_subscriber_id is not null )*1./count(*) as perc-- ,count(*) filter ( where mbi is null and ins_primary_subscriber_id is not null )*1./count(*) as perc_previous
from final
union all
select '--Patients with an MBI'as bucket,count(*) filter ( where mbi is not null ) as num,count(*) filter ( where mbi is not null )*1./count(*) as perc-- ,count(*) filter ( where mbi is not null )*1./count(*) as perc_previous
from final
union All
select '****Patients not yet sent to DPC ' as bucket,count(*) filter ( where mbi is not null and DPC_history is null ) as num, count(*) filter ( where mbi is not null and DPC_history is null )*1./count(*)  as perc --,count(*) filter ( where mbi is not null and DPC_history is null )*1./count(*) filter ( where mbi is not null ) as perc_previous
from final
union All
select '****Patients sent to DPC (ever, not just on most recent pull) ' as bucket,count(*) filter ( where mbi is not null and DPC_history is true) as num, count(*) filter ( where mbi is not null and DPC_history is true)*1./count(*)  as perc--,count(*) filter ( where mbi is not null and DPC_history is true)*1./count(*) filter ( where mbi is not null ) as perc_previous
from final
union All
select '>>>>>>>>Patients returned from DPC with errors on most recent submission' as bucket,count(*) filter ( where has_dpc_error is true ) as num, count(*) filter ( where has_dpc_error is true )*1./count(*) as perc--,count(*) filter ( where has_dpc_error is true )*1./count(*) filter ( where DPC_history is true and has_dpc_error is not null) as perc_previous
from final
union All
select '>>>>>>>>Patients returned from DPC without errors on most recent submission' as bucket, count(*) filter ( where has_dpc_error is false )  as num, count(*) filter ( where has_dpc_error is false )*1./count(*)  as perc--,count(*) filter ( where has_dpc_error is false )*1./count(*) filter ( where DPC_history is true and has_dpc_error is not null) as perc_previous
from final
--union All
--select '~~~~~~~~~~Patients with current coverage source = DPC ' as bucket, count(*) filter ( where has_dpc_error is false and patient_cov='dpc' ) as num, count(*) filter ( where has_dpc_error is false and patient_cov='dpc')*1./count(*)  as perc,count(*) filter ( where has_dpc_error is false and patient_cov='dpc')*1./count(*) filter ( where has_dpc_error is false ) as perc_previous from final
--union All
--select '~~~~~~~~~~Patients with current coverage source = Plan file ' as bucket, count(*) filter ( where has_dpc_error is false and patient_cov in ('uhc','mco','bcbstn','mssp_roster') ) as num, count(*) filter ( where has_dpc_error is false and patient_cov in ('uhc','mco','bcbstn','mssp_roster') )*1./count(*)  as perc ,count(*) filter ( where has_dpc_error is false and patient_cov in ('uhc','mco','bcbstn','mssp_roster') )*1./count(*) filter ( where has_dpc_error is false ) as perc_previous from final
--union All
--select '~~~~~~~~~~Patients with current coverage source = Other (not DPC or Plan file) ' as bucket, count(*) filter ( where has_dpc_error is false and patient_cov not  in ('dpc','uhc','mco','bcbstn','mssp_roster') ) as num, count(*) filter ( where has_dpc_error is false and patient_cov not  in ('dpc','uhc','mco','bcbstn','mssp_roster') )*1./count(*)  as perc,count(*) filter ( where has_dpc_error is false and patient_cov not  in ('dpc','uhc','mco','bcbstn','mssp_roster') )*1./count(*) filter ( where has_dpc_error is false ) as perc_previous from final
