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


-- drop TABLE newman.mattermost_chats;
CREATE TABLE newman.mattermost_chats (
    id              BIGSERIAL PRIMARY KEY,
    username        TEXT,
    channel         TEXT,
    question        TEXT,
    answer          TEXT,
    messages        jsonb,
    requests        INT,
    request_tokens  BIGINT,
    response_tokens BIGINT,
    response_time_s decimal(16,3),
    inserted_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- TRUNCATE newman.mattermost_chats RESTART IDENTITY ;
SELECT * FROM newman.mattermost_chats;

SELECT *
FROM
    now();