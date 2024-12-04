-- https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Managing.Performance.html

-- # cluster
-- https://www.postgresql.org/docs/current/sql-cluster.html

-- repack
-- https://reorg.github.io/pg_repack/
-- https://reorg.github.io/pg_repack/#usage


SELECT
    inbound_file_config_id
  , ifc.ftp_server_name
  , SUM(row_count) rows
FROM
    inbound_file_logs l
    JOIN inbound_file_config ifc ON l.inbound_file_config_id = ifc.id
WHERE
    l.inserted_at BETWEEN '2024-11-21'::date and '2024-11-22'::date
GROUP BY
    1, 2
ORDER BY
    3 DESC
LIMIT 10;


SELECT
    schemaname
  , relname
  , n_dead_tup
  , PG_SIZE_PRETTY(PG_TABLE_SIZE(relid)) AS table_size
FROM
    pg_stat_user_tables
ORDER BY
    n_dead_tup DESC;



DROP TABLE IF EXISTS _current;
CREATE TEMP TABLE _current AS
SELECT
    schemaname,
    t.relname,
    pg_table_size(relid) AS table_size,
    pg_size_pretty(pg_table_size(relid)) AS table_size_h,
    pg_size_pretty(pg_total_relation_size(relid) - pg_table_size(relid)) AS bloat_size_h,
    pg_total_relation_size(relid) - pg_table_size(relid) AS bloat_size
FROM pg_stat_user_tables t
-- ORDER BY bloat_size DESC;
ORDER BY table_size DESC;

SELECT
    i.schemaname,
    i.relname AS index_name,
    pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size,
    pg_size_pretty(pg_relation_size(i.indexrelid) - pg_table_size(i2.relid)) AS bloat_size_h,
    pg_relation_size(i.indexrelid) - pg_table_size(i2.relid) AS bloat_size
FROM pg_stat_user_indexes i
JOIN pg_statio_user_indexes i2 USING (indexrelid)
ORDER BY bloat_size DESC;



-- 1.1 TB table bloat
SELECT
--     schemaname,
--     t.relname,
    PG_SIZE_PRETTY(SUM(PG_TOTAL_RELATION_SIZE(relid)))                                                 AS total_size
  , PG_SIZE_PRETTY(SUM(PG_TABLE_SIZE(relid)))                                                          AS table_size
  , PG_SIZE_PRETTY(SUM(PG_INDEXES_SIZE(relid)))                                                        AS index_size
  , PG_SIZE_PRETTY(SUM(PG_TOTAL_RELATION_SIZE(relid) - PG_INDEXES_SIZE(relid) - PG_TABLE_SIZE(relid))) AS bloat_size_h
--     pg_total_relation_size(relid) - pg_table_size(relid) AS bloat_size
FROM
    pg_stat_user_tables t;


-- table_size,bloat_size_h
-- 9613 GB,1124 GB


-- ORDER BY bloat_size DESC;

SELECT relname AS TableName, n_live_tup AS LiveTuples, n_dead_tup AS DeadTuples, n_tup_del, n_tup_upd, last_autovacuum AS Autovacuum, last_vacuum AS ManualVacuum, now() FROM pg_stat_user_tables;



SELECT
    f.name
  , SUM(ef.file_size)
  , pg_size_pretty(SUM(ef.file_size)) total_size
FROM
    fdw_file_router.external_files ef
    JOIN fdw_file_router.ftp_servers f ON ef.ftp_server_id = f.id
WHERE
      ef.s3_bucket IS NOT NULL
  AND ef.inserted_at::DATE = '2024-11-22'
GROUP BY
    1
order by 2 desc
;
------------------------------------------------------------------------------------------------------------------------
/* compare backup */
------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS _current;
CREATE TEMP TABLE _current AS
SELECT
    schemaname,
    t.relname,
    pg_table_size(relid) AS table_size,
    pg_indexes_size(relid) AS index_size,
    pg_size_pretty(pg_indexes_size(relid)) AS index_size_h,
    pg_size_pretty(pg_table_size(relid)) AS table_size_h,
    pg_size_pretty(pg_total_relation_size(relid) - pg_table_size(relid)) AS bloat_size_h,
    pg_total_relation_size(relid) - pg_table_size(relid) AS bloat_size
FROM pg_stat_user_tables t
-- ORDER BY bloat_size DESC;
ORDER BY table_size DESC;

DROP TABLE IF EXISTS _compare;
CREATE TEMP TABLE _compare AS
SELECT
    c.schemaname
  , c.relname
  , c.table_size
  , c.table_size_h
  , c.table_size - j.table_size                 size_growth
  , PG_SIZE_PRETTY(c.table_size - j.table_size) size_growth_h

  , c.index_size - j.index_size                 index_size_growth
  , PG_SIZE_PRETTY(c.index_size - j.index_size) index_size_growth_h
  , c.bloat_size - j.bloat_size::bigint                 bloat_growth
  , PG_SIZE_PRETTY(c.bloat_size - j.bloat_size::bigint) bloat_growth_h
FROM
    _current c
    LEFT JOIN junk.backup_table_sizes_w_index j ON c.schemaname = j.schemaname AND c.relname = j.relname
ORDER BY
    5 DESC NULLS LAST
;


SELECT
--     schemaname
--   , relname
--   , table_size
--   , table_size_h
    PG_SIZE_PRETTY(SUM(size_growth)) total_growth
  , PG_SIZE_PRETTY(SUM(bloat_growth)) total_bloat_growth
  , PG_SIZE_PRETTY(SUM(index_size_growth)) total_index_growth
--   , size_growth_h
FROM
    _compare
WHERE
    schemaname !~* 'pg_temp'
;

SELECT
    schemaname
  , relname
  , table_size
  , table_size_h
--   , size_growth
  , size_growth_h
  , index_size_growth_h
  , bloat_growth_h
FROM
    _compare
WHERE
    schemaname !~* 'pg_temp'
LIMIT 10;


-- where
--     size_growth ISNULL
;


SELECT PG_SIZE_PRETTY( sum(table_size))
FROM _compare
where size_growth ISNULL
and schemaname !~* 'pg_temp'


------------------------------------------------------------------------------------------------------------------------
/* maybe indexes? */
------------------------------------------------------------------------------------------------------------------------

SELECT
    i.schemaname
    , i.relname
  , indexrelname
  , PG_SIZE_PRETTY(PG_TOTAL_RELATION_SIZE(relid)) AS "Total Size"
  , PG_SIZE_PRETTY(PG_INDEXES_SIZE(relid))        AS "Total Size of all Indexes"
  , PG_SIZE_PRETTY(PG_RELATION_SIZE(relid))       AS "Table Size"
  , PG_SIZE_PRETTY(PG_RELATION_SIZE(indexrelid))     "Index Size"
  , reltuples::BIGINT                                "Estimated table row count"
FROM
    pg_stat_all_indexes i
    JOIN pg_class c ON i.relid = c.oid;

--  WHERE i.relname='uploads'

------------------------------------------------------------------------------------------------------------------------
/* MD porrtals cleanup */
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS junk.mdp_max_id_to_del;
CREATE TABLE junk.mdp_max_id_to_del AS
SELECT id
FROM
    analytics.bk_up.md_portal_suspects
where inserted_at::date <= '2023-12-31'
order by id desc
limit 1
    ;

SELECT *
FROM
    junk.mdp_max_id_to_del;