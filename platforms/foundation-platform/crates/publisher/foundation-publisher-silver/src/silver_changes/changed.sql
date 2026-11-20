-- The staged rows of a table compared by key and content (root ADR-0311: append only the changed
-- keys): those whose key is new or whose content differs from a current row of that key. It was the
-- Spark loads' `changed_only` (`data_go_kr_onbid`, then `silver_release_compare`), which hashed each
-- column's null-ness apart from its value: `IS NOT DISTINCT FROM` on every key and content column is
-- the same comparison without the hash.
-- Parameters: {current} (the table's current rows at the pinned head, or a query of none),
-- {staged} (the staged rows), {out} (the changed rows), {matches} (`c.x IS NOT DISTINCT FROM s.x`
-- per key and content column).
CREATE OR REPLACE TEMP TABLE {out}_current AS {current};

CREATE OR REPLACE TEMP TABLE {out} AS
SELECT s.* FROM {staged} s
WHERE NOT EXISTS (SELECT 1 FROM {out}_current c WHERE {matches});
