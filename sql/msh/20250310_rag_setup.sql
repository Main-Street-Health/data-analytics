CREATE EXTENSION vector;
create schema newman;

drop TABLE newman.docs;
CREATE TABLE newman.docs (
    id          BIGSERIAL PRIMARY KEY,
    content     TEXT,
    source      TEXT,
    embedding   vector(768),
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

DROP TABLE IF EXISTS ;
-- CREATE TABLE junk.newman_docs_20250312 AS
select * from newman.docs;
-- TRUNCATE newman.docs RESTART IDENTITY ;
DELETE
FROM
    newman.docs where id = 1;
WHERE;

-- pg_dump --schema=etl --schema-only -h msh-prd-analytics.cluster-ro-culqpk9pcgmf.us-east-1.rds.amazonaws.com -p 5432 -U postgres -d analytics > etl_schema.sql