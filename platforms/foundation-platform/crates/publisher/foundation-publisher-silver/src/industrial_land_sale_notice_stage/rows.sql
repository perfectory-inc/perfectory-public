-- One Bronze object of industrial land sale notice reads as Silver rows (root ADR-0603): a read row
-- per line, and an attachment row per file the media store holds. Every source writes the same line
-- envelope (`industrial_land_sale_notice_collect::line`): the provider's answer stays as served in
-- `body`, and what the collector did with each attachment is in the top-level `attachments` array,
-- so this reads no provider field. `python_values.sql` (the shared value readers) runs first.
--
-- Input: sale_lines(line_number BIGINT, line VARCHAR): the object's non-blank lines, numbered from 1.
-- Parameters: {object_key}, {snapshot_id}, {ingested_at_epoch_seconds}.
-- Output: sale_reads, sale_attachments (the two tables' columns, in contract order).

-- A whole number written with digits; NULL otherwise.
CREATE OR REPLACE MACRO sale_whole(v) AS
  CASE WHEN v IS NULL OR json_type(v) = 'NULL' THEN NULL
       WHEN regexp_full_match(replace(trim(py_str(v), py_ws()), ',', ''), '-?[0-9]+')
       THEN CAST(replace(trim(py_str(v), py_ws()), ',', '') AS BIGINT) END;

CREATE OR REPLACE TEMP TABLE sale_parsed AS
SELECT line_number, j,
       py_text(json_extract(j, '$.notice_source')) AS notice_source,
       py_text(json_extract(j, '$.notice_id')) AS notice_id,
       py_text(json_extract(j, '$.list_fingerprint')) AS list_fingerprint,
       notice_utc(json_extract(j, '$.requested_at_utc')) AS requested_at_utc
FROM (SELECT line_number, CAST(line AS JSON) AS j FROM sale_lines);

-- A line the collector wrote names its source, the notice, the list fingerprint and when it asked;
-- any other line is refused, never skipped: the collector writes no other.
CREATE OR REPLACE TEMP TABLE sale_valid AS
SELECT line_number, j, notice_source, notice_id, list_fingerprint, requested_at_utc,
       CASE WHEN json_type(json_extract(j, '$.attachments')) = 'ARRAY' THEN json_extract(j, '$.attachments')
            WHEN NOT py_truthy(json_extract(j, '$.attachments')) THEN '[]'::JSON
            ELSE error('line ' || line_number || ': attachments is not a list') END AS files
FROM sale_parsed
WHERE CASE WHEN notice_source IS NULL OR notice_id IS NULL OR list_fingerprint IS NULL OR requested_at_utc IS NULL
           THEN error('line ' || line_number || ': not a sale notice read (source, id, fingerprint, time)')
           ELSE true END;

-- One row per attachment, in its line's order; an attachment that is not an object is refused.
CREATE OR REPLACE TEMP TABLE sale_file_rows AS
SELECT v.line_number, v.notice_source, v.notice_id, v.requested_at_utc, e.ordinal,
       CASE WHEN json_type(e.file) = 'OBJECT' THEN e.file
            ELSE error('line ' || v.line_number || ': an attachment is not an object') END AS file
FROM sale_valid v,
     LATERAL (SELECT unnest(range(json_array_length(v.files)::BIGINT)) AS ordinal) i,
     LATERAL (SELECT i.ordinal, json_extract(v.files, '$[' || i.ordinal || ']') AS file) e;

-- The files the media store holds: stored by this read or already there (deduplicated).
CREATE OR REPLACE TEMP TABLE sale_stored AS
SELECT * FROM sale_file_rows
WHERE json_type(json_extract(file, '$.store_outcome')) = 'VARCHAR'
  AND json_extract_string(file, '$.store_outcome') IN ('stored', 'deduplicated')
  AND py_text(json_extract(file, '$.sha256')) IS NOT NULL
  AND py_text(json_extract(file, '$.media_format')) IS NOT NULL;

CREATE OR REPLACE TEMP TABLE sale_reads AS
SELECT
  v.notice_source,
  v.notice_id,
  v.list_fingerprint,
  v.requested_at_utc,
  CAST(sale_whole(json_extract(v.j, '$.http_status')) AS INTEGER) AS http_status,
  coalesce(py_text(json_extract(v.j, '$.read_outcome')), 'failed') AS read_outcome,
  CASE WHEN coalesce(py_text(json_extract(v.j, '$.read_outcome')), 'failed') <> 'failed'
       THEN CAST(json_array_length(v.files) AS INTEGER) END AS file_count,
  CASE WHEN coalesce(py_text(json_extract(v.j, '$.read_outcome')), 'failed') <> 'failed'
       THEN CAST((SELECT count(*) FROM sale_stored s WHERE s.line_number = v.line_number) AS INTEGER) END AS files_stored,
  '{object_key}' AS source_record_id,
  CAST(v.line_number AS INTEGER) AS source_line_number,
  '{snapshot_id}' AS source_snapshot_id,
  to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM sale_valid v
ORDER BY v.line_number;

-- One row per (source, notice, file hash): the last file of the object that names it decides it.
CREATE OR REPLACE TEMP TABLE sale_attachments AS
SELECT
  notice_source,
  notice_id,
  sha256,
  CAST(coalesce(sale_whole(json_extract(file, '$.file_sequence')), ordinal + 1) AS INTEGER) AS file_sequence,
  py_text(json_extract(file, '$.file_title')) AS file_title,
  py_text(json_extract(file, '$.provider_file_id')) AS provider_file_id,
  sale_whole(json_extract(file, '$.provider_file_size')) AS provider_file_size,
  coalesce(py_text(json_extract(file, '$.source_url')), '') AS source_url,
  py_text(json_extract(file, '$.media_format')) AS media_format,
  coalesce(sale_whole(json_extract(file, '$.size_bytes')), 0) AS size_bytes,
  coalesce(notice_utc(json_extract(file, '$.fetched_at_utc')), requested_at_utc) AS fetched_at_utc,
  '{object_key}' AS source_record_id,
  CAST(line_number AS INTEGER) AS source_line_number,
  '{snapshot_id}' AS source_snapshot_id,
  to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM (
  SELECT *, py_text(json_extract(file, '$.sha256')) AS sha256,
         row_number() OVER (PARTITION BY notice_source, notice_id, py_text(json_extract(file, '$.sha256'))
                            ORDER BY line_number DESC, ordinal DESC) AS last
  FROM sale_stored
)
WHERE last = 1
ORDER BY notice_source, notice_id, file_sequence;
