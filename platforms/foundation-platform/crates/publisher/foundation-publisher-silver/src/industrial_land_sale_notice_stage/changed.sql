-- The attachment rows whose (source, notice, hash) is new or whose content differs from its current
-- row (root ADR-0311: append only the changed keys), as the ILIS notice stage compares its own.
-- Parameters: {current} (the current-rows query of the attachment table, rendered from its
-- contract, or a query of no rows), {matches} (one `c.x IS NOT DISTINCT FROM a.x` per key and
-- content column, joined by AND).
CREATE OR REPLACE TEMP TABLE sale_attachments_changed AS
SELECT a.* FROM sale_attachments a
WHERE NOT EXISTS (SELECT 1 FROM ({current}) c WHERE {matches})
ORDER BY a.notice_source, a.notice_id, a.file_sequence;
