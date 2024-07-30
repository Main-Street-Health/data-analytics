set schema 'cb';

drop table churn;
create temporary table churn as
with dp as (
    select
        distinct
        d.year,
        d.bom,
        d.eom
    from ref.dates d
    where d.year in (2019)
)
select
    ed.year,
    dp.bom,
    dp.eom,
    ((ed.line_of_business_id::text || coalesce(ed.ggroup,'9')::text)::int * case when ed.is_unaligned then -1 else 1 end) ttype,
    ed.member_id,
    count(1) n_days
from
    cb.eligibility_days ed
    join dp on ed.date between dp.bom and dp.eom
where
    ed.mco_id = 2
group by 1,2,3,4,5;
create index adxc on churn(member_id, bom);

select
    case when bom = '2017-01-01' then ttype end ttype17_01,
    case when bom = '2017-02-01' then ttype end ttype17_02,
    case when bom = '2017-03-01' then ttype end ttype17_03,
    case when bom = '2017-04-01' then ttype end ttype17_04,
    case when bom = '2017-05-01' then ttype end ttype17_05,
    case when bom = '2017-06-01' then ttype end ttype17_06,
    case when bom = '2017-07-01' then ttype end ttype17_07,
    case when bom = '2017-08-01' then ttype end ttype17_08,
    case when bom = '2017-09-01' then ttype end ttype17_09,
    case when bom = '2017-10-01' then ttype end ttype17_10,
    case when bom = '2017-11-01' then ttype end ttype17_11,
    case when bom = '2017-12-01' then ttype end ttype17_12,
    case when bom = '2018-01-01' then ttype end ttype18_01,
    case when bom = '2018-02-01' then ttype end ttype18_02,
    case when bom = '2018-03-01' then ttype end ttype18_03,
    case when bom = '2018-04-01' then ttype end ttype18_04,
    case when bom = '2018-05-01' then ttype end ttype18_05,
    case when bom = '2018-06-01' then ttype end ttype18_06,
    case when bom = '2018-07-01' then ttype end ttype18_07,
    case when bom = '2018-08-01' then ttype end ttype18_08,
    case when bom = '2018-09-01' then ttype end ttype18_09,
    case when bom = '2018-10-01' then ttype end ttype18_10,
    case when bom = '2018-11-01' then ttype end ttype18_11,
    case when bom = '2018-12-01' then ttype end ttype18_12,
    case when bom = '2019-01-01' then ttype end ttype19_01,
    case when bom = '2019-02-01' then ttype end ttype19_02,
    case when bom = '2019-03-01' then ttype end ttype19_03,
    case when bom = '2019-04-01' then ttype end ttype19_04,
    case when bom = '2019-05-01' then ttype end ttype19_05,
    case when bom = '2019-06-01' then ttype end ttype19_06,
    case when bom = '2019-07-01' then ttype end ttype19_07,
    case when bom = '2019-08-01' then ttype end ttype19_08,
    case when bom = '2019-09-01' then ttype end ttype19_09,
    case when bom = '2019-10-01' then ttype end ttype19_10,
    case when bom = '2019-11-01' then ttype end ttype19_11,
    case when bom = '2019-12-01' then ttype end ttype19_12,
    count(distinct c.member_id) nd
from
    churn c
group by
 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,
12,13,14,15,16,17,18,19,20,21,22,23,24,
25,26,27,28,29,30,31,32,33,34,35,36


select
    churn_19_01.ttype ttype19_01,
    churn_19_02.ttype ttype19_02,
    churn_19_03.ttype ttype19_03,
    churn_19_04.ttype ttype19_04,
    churn_19_05.ttype ttype19_05,
    churn_19_06.ttype ttype19_06,
    churn_19_07.ttype ttype19_07,
    churn_19_08.ttype ttype19_08,
    churn_19_09.ttype ttype19_09,
    churn_19_10.ttype ttype19_10,
    churn_19_11.ttype ttype19_11,
    churn_19_12.ttype ttype19_12,


    count(distinct x.member_id) nd
from
    churn x
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-01-01'::date) churn_19_01 on x.member_id = churn_19_01.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-02-01'::date) churn_19_02 on x.member_id = churn_19_02.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-03-01'::date) churn_19_03 on x.member_id = churn_19_03.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-04-01'::date) churn_19_04 on x.member_id = churn_19_04.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-05-01'::date) churn_19_05 on x.member_id = churn_19_05.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-06-01'::date) churn_19_06 on x.member_id = churn_19_06.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-07-01'::date) churn_19_07 on x.member_id = churn_19_07.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-08-01'::date) churn_19_08 on x.member_id = churn_19_08.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-09-01'::date) churn_19_09 on x.member_id = churn_19_09.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-10-01'::date) churn_19_10 on x.member_id = churn_19_10.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-11-01'::date) churn_19_11 on x.member_id = churn_19_11.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-12-01'::date) churn_19_12 on x.member_id = churn_19_12.member_id

    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-01-01'::date) churn18_01 on x.member_id = churn18_01.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-02-01'::date) churn18_02 on x.member_id = churn18_02.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-03-01'::date) churn18_03 on x.member_id = churn18_03.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-04-01'::date) churn18_04 on x.member_id = churn18_04.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-05-01'::date) churn18_05 on x.member_id = churn18_05.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-06-01'::date) churn18_06 on x.member_id = churn18_06.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-07-01'::date) churn18_07 on x.member_id = churn18_07.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-08-01'::date) churn18_08 on x.member_id = churn18_08.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-09-01'::date) churn18_09 on x.member_id = churn18_09.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-10-01'::date) churn18_10 on x.member_id = churn18_10.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-11-01'::date) churn18_11 on x.member_id = churn18_11.member_id
    join (select cx.member_id, cx.ttype from  churn cx where bom = '2018-12-01'::date) churn18_12 on x.member_id = churn18_12.member_id

group by
    1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12


select
    churn_19_01.ttype ttype19_01,
    count(1) m,
    count(distinct x.member_id) nd
from
    (select distinct member_id from churn) x
    left join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-01-01'::date) churn_19_01 on x.member_id = churn_19_01.member_id
    --left join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-02-01'::date) churn_19_02 on x.member_id = churn_19_02.member_id
    --left join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-03-01'::date) churn_19_03 on x.member_id = churn_19_03.member_id
    --left join (select cx.member_id, cx.ttype from  churn cx where bom = '2019-04-01'::date) churn_19_04 on x.member_id = churn_19_04.member_id
group by 1








