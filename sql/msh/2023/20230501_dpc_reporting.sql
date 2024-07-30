------------- patient base list
DROP TABLE IF EXISTS _patient_base ;
CREATE TEMP TABLE _patient_base  AS
         select distinct on (mbi) ---- dedup patients with same MBI, maybe add dob?
            p.patient_id,
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
     from fdw_member_doc_ent.patients p
     join fdw_member_doc.supreme_pizza sp on sp.patient_id = p.patient_id
--      where (patient_attribution_status~*'ma_at_risk' or patient_attribution_status='om_at_risk_yes' or (patient_attribution_status in ('om_at_risk_no','om_at_risk_pending') and patient_market~*'Tennessee'))
     where sp.is_dpc
            and mbi is not null
            and substatus !='deceased'
            and status!='inactive'---- remove inactive patients
            and organization_name not in ('Bassett Healthcare Network' ,'Galen Medical Group')
--             and {{patient_market}}
--             and {{organization_name}}
--             and {{primary_referring_partner_name}}
     union
     select
            p.patient_id,
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
     from fdw_member_doc_ent.patients p
         join fdw_member_doc.supreme_pizza sp on sp.patient_id = p.patient_id
             where sp.is_dpc
            and substatus !='deceased'
            and mbi is null
            and status!='inactive'
--             and organization_name not in ('Bassett Healthcare Network' ,'Galen Medical Group')  ----- per Jake, remove them for entire report
--             and {{patient_market}}
--             and {{organization_name}}
--             and {{primary_referring_partner_name}}
;
create index on _patient_base(patient_id);


DROP TABLE IF EXISTS _rpt_patients;
CREATE TEMP TABLE _rpt_patients AS
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

SELECT count(distinct rp.patient_id)
, count(distinct pb.patient_id)
FROM
    _rpt_patients rp
left join _patient_base pb on pb.patient_id = rp.patient_id
;

SELECT
    COUNT(DISTINCT rp.patient_id) reporting_count
  , COUNT(DISTINCT pb.patient_id) is_dpc_count
FROM
    _patient_base pb
    LEFT JOIN _rpt_patients rp ON pb.patient_id = rp.patient_id
;


DROP TABLE IF EXISTS _patient;
CREATE TEMP TABLE  _patient as
   select pb.patient_id,
          pb.ins_primary_subscriber_id,
          pb.cat,
          max(rfrnc_yr) as coverage_year,
          'dpc' as source
   from _patient_base pb
   left join dpoc_patients dp on pb.patient_id = dp.source_id
   left join dpoc_coverage sc on dp.bene_id = sc.patient
   group by 1,2,3;


----------Get patient ever sent DPC
DROP TABLE IF EXISTS _dpc_history;
CREATE TEMP TABLE _dpc_history AS
SELECT DISTINCT ON (mbi)
    x.mbi
  , TRUE is_dpc_history
  , x.dpoc_id
  , x.recent_inserted_at
FROM
    ( SELECT
          UNNEST(j.mbis)      mbi
        , j.inserted_at::DATE recent_inserted_at
        , j.id                dpoc_id
      FROM
          public.dpoc_bulk_export_jobs j
    ) x
ORDER BY
    x.mbi, x.recent_inserted_at DESC;
create index on _dpc_history(mbi);

-- CREATE INDEX ON dpoc_patients USING gin(mbis);
-- CREATE INDEX ON dpoc_patients(source_id);

-- need one more level to account for multiple mbis per member
DROP TABLE IF EXISTS _most_recent_dpc_history;
CREATE TEMP TABLE _most_recent_dpc_history AS
SELECT distinct on (dp.source_id)
    dh.mbi, dh.is_dpc_history, dh.dpoc_id, dh.recent_inserted_at
FROM
    _dpc_history dh
join dpoc_patients dp ON dh.mbi = any(dp.mbis)
order by dp.source_id, dh.recent_inserted_at desc
;


------------Get recent DPC from patient DPC history
DROP TABLE IF EXISTS _recent_base;
CREATE TEMP TABLE  _recent_base AS
    select
           j.mbi,
           MAX(je.id) IS NOT NULL             has_dpc_error,
           STRING_AGG(DISTINCT je.error, ', ') errors
    FROM _dpc_history j
    LEFT JOIN dpoc_bulk_export_job_errors je ON je.dpoc_bulk_export_job_id = j.dpoc_id
        AND j.mbi = je.mbi
    group by 1
    ;

;
with
final as(
select pb.*,
       p.coverage_year,
       p.source,
       case when p.source is null then pb.coverage_source else p.source end as patient_cov,
       dh.is_dpc_history,
      --- dh.npi,
       ---concat(dh.first_name,' ',dh.last_name) as practitioner_name,
       rb.has_dpc_error,
       rb.errors
from _patient_base pb
left join _patient p on pb.patient_id=p.patient_id
left join _most_recent_dpc_history Dh on pb.mbi = Dh.mbi
left join _recent_base rb on Dh.mbi = rb.mbi
)

select 'All Patients Classified as MA or as OM & MSSP or OM in TN' as bucket, count(*) as num, 1 as perc ,null  as perc_previous from final
union all
select '--Patients without an MBI ' as bucket, count(*) filter ( where mbi is null ) as num, count(*) filter ( where mbi is null )*1./count(*) as perc ,count(*) filter ( where mbi is null )*1./count(*) as perc_previous from final
union all
select '-->Patients without an MBI or subscriber ID ' as bucket, count(*) filter ( where mbi is null and ins_primary_subscriber_id is  null ) as num, count(*) filter ( where mbi is null and ins_primary_subscriber_id is  null )*1./count(*) as perc ,count(*) filter ( where mbi is null and ins_primary_subscriber_id is  null )*1./count(*) as perc_previous from final
union all
select '-->Patients without an MBI but with subscriber ID ' as bucket, count(*) filter ( where mbi is null and ins_primary_subscriber_id is not null ) as num, count(*) filter ( where mbi is null and ins_primary_subscriber_id is not null )*1./count(*) as perc ,count(*) filter ( where mbi is null and ins_primary_subscriber_id is not null )*1./count(*) as perc_previous from final
union all
select '--Patients with an MBI'as bucket,count(*) filter ( where mbi is not null ) as num,count(*) filter ( where mbi is not null )*1./count(*) as perc ,count(*) filter ( where mbi is not null )*1./count(*) as perc_previous from final
union All
select '****Patients not yet sent to DPC ' as bucket,count(*) filter ( where mbi is not null and is_dpc_history is null ) as num, count(*) filter ( where mbi is not null and is_dpc_history is null )*1./count(*)  as perc ,count(*) filter ( where mbi is not null and is_dpc_history is null )*1./count(*) filter ( where mbi is not null ) as perc_previous from final
union All
select '****Patients sent to DPC (ever, not just on most recent pull) ' as bucket,count(*) filter ( where mbi is not null and is_dpc_history is true) as num, count(*) filter ( where mbi is not null and is_dpc_history is true)*1./count(*)  as perc,count(*) filter ( where mbi is not null and is_dpc_history is true)*1./count(*) filter ( where mbi is not null ) as perc_previous from final
union All
select '>>>>>>>>Patients returned from DPC with errors on most recent submission' as bucket,count(*) filter ( where has_dpc_error is true ) as num, count(*) filter ( where has_dpc_error is true )*1./count(*) as perc,count(*) filter ( where has_dpc_error is true )*1./count(*) filter ( where is_dpc_history is true and has_dpc_error is not null) as perc_previous from final
union All
select '>>>>>>>>Patients returned from DPC without errors on most recent submission' as bucket, count(*) filter ( where has_dpc_error is false )  as num, count(*) filter ( where has_dpc_error is false )*1./count(*)  as perc,count(*) filter ( where has_dpc_error is false )*1./count(*) filter ( where is_dpc_history is true and has_dpc_error is not null) as perc_previous from final
union All
select '~~~~~~~~~~Patients with current coverage source = DPC ' as bucket, count(*) filter ( where has_dpc_error is false and patient_cov='dpc' ) as num, count(*) filter ( where has_dpc_error is false and patient_cov='dpc')*1./count(*)  as perc,count(*) filter ( where has_dpc_error is false and patient_cov='dpc')*1./count(*) filter ( where has_dpc_error is false ) as perc_previous from final
union All
select '~~~~~~~~~~Patients with current coverage source = Plan file ' as bucket, count(*) filter ( where has_dpc_error is false and patient_cov in ('uhc','mco','bcbstn','mssp_roster') ) as num, count(*) filter ( where has_dpc_error is false and patient_cov in ('uhc','mco','bcbstn','mssp_roster') )*1./count(*)  as perc ,count(*) filter ( where has_dpc_error is false and patient_cov in ('uhc','mco','bcbstn','mssp_roster') )*1./count(*) filter ( where has_dpc_error is false ) as perc_previous from final
union All
select '~~~~~~~~~~Patients with current coverage source = Other (not DPC or Plan file) ' as bucket, count(*) filter ( where has_dpc_error is false and patient_cov not  in ('dpc','uhc','mco','bcbstn','mssp_roster') ) as num, count(*) filter ( where has_dpc_error is false and patient_cov not  in ('dpc','uhc','mco','bcbstn','mssp_roster') )*1./count(*)  as perc,count(*) filter ( where has_dpc_error is false and patient_cov not  in ('dpc','uhc','mco','bcbstn','mssp_roster') )*1./count(*) filter ( where has_dpc_error is false ) as perc_previous from final





