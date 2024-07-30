select
    count(distinct a.healthstar_member_id)
from v_planofcare_responces a
where
    from_unixtime(a.ttimestamp) between '01-01-2019' and '12-31-2019';


select
  pa.number                 as assessment_id,
  pr.import_id              as import_id,
  pa.version                as version,
  question_number           as raw_question_number,
  pr.patient_id             as healthstar_member_id,
  pc.number                 as concept_id,
  ROUND((select question_number from planofcare_concepts where number = pc.number and display != '' limit 1),1) as question_number,
  pc.name                   as 'concept_question',
  (select display from planofcare_concepts where number = pc.number and display != '' limit 1) as question,
  case when pca.display is null then pr.response
       when pca.display is not null then pca.display
  end                       as response,
  pca.display               as raw_concept_answer,
  pr.response               as raw_response,
  greatest(
          coalesce(pr.`when`,0)
        , coalesce(pr.sys_created_at, 0)
        , coalesce(pr.sys_updated_at, 0)
        , coalesce(pc.sys_updated_at, 0)
        , coalesce(pc.sys_created_at, 0)
        , coalesce(pca.sys_updated_at, 0)
        , coalesce(pca.sys_created_at, 0)
        , coalesce(pa.sys_updated_at, 0)
        , coalesce(pa.sys_created_at, 0)
  ) as ttimestamp
from
    planofcare_responses as pr
    inner join planofcare_concepts as pc on pc.id=pr.poc_concept_id
    inner join planofcare_concept_answers as pca on pca.poc_concept_number=pc.number and pr.response=pca.number
    inner join planofcare_assessments pa on pa.id = pr.poc_assessment_id
where pr.deleted='0'
and pc.deleted = '0'
and pca.deleted = '0';
