-- The rows of a compared table (item detail, bid info, announcements) whose key is new or whose
-- content differs from a current row of that key (root ADR-0311: append only the changed keys). It
-- was `data_go_kr_onbid.changed_only`, which hashed each column's null-ness apart from its value:
-- `IS NOT DISTINCT FROM` on every key and content column is the same comparison without the hash.
-- Parameters: {current} (the table's current rows at the pinned head, or a query of none),
-- {matches} (`c.x IS NOT DISTINCT FROM s.x` per key and content column).
CREATE OR REPLACE TEMP TABLE onbid_current AS {current};

CREATE OR REPLACE TEMP TABLE onbid_changed AS
SELECT s.* FROM onbid_rows s
WHERE NOT EXISTS (SELECT 1 FROM onbid_current c WHERE {matches});
