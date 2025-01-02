SELECT *
FROM
    member_doc.junk.mdp_abbreviations_table_20240219
;

update stage.md_portal_author_mappings m
set author_abbr = j.author_abbr, updated_at = now()
FROM
    member_doc.junk.mdp_abbreviations_table_20240219 j
where m.id = j.id
and m.author_abbr != j.author_abbr;

DROP TABLE IF EXISTS _new;
CREATE TEMP TABLE _new AS
SELECT distinct j.*
FROM
    member_doc.junk.mdp_abbreviations_table_20240219 j
left join stage.md_portal_author_mappings m on m.source_author = j.source_author
where j.author_abbr is not null
and m.source_author ISNULL
;
SELECT *
FROM
    _new;

INSERT
INTO
    member_doc.stage.md_portal_author_mappings (source_author, author_abbr, is_deleted, deleted_at, inserted_at, updated_at)
select 
    n.source_author, n.author_abbr, false is_deleted, null deleted_at, now() inserted_at, now()  updated_at
from _new n;