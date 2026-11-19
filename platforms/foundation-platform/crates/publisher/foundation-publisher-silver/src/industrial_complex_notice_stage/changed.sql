-- The attachment rows whose (notice, hash) is new or whose content differs from its current row
-- (root ADR-0311: append only the changed keys). It was `data_go_kr_onbid.changed_only`, which
-- hashed each column's null-ness apart from its value: `IS NOT DISTINCT FROM` on every content
-- column is the same comparison without the hash. Parameters: {current} (the current-rows query
-- of the attachment table, rendered from its contract, or a query of no rows), {matches} (one
-- `c.x IS NOT DISTINCT FROM a.x` per key and content column, joined by AND).
CREATE OR REPLACE TEMP TABLE notice_attachments_changed AS
SELECT a.* FROM notice_attachments a
WHERE NOT EXISTS (SELECT 1 FROM ({current}) c WHERE {matches})
ORDER BY a.notice_sequence, a.file_sequence;
