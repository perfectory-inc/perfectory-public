-- One Bronze object of ILIS notice detail reads as Silver rows (root ADR-0484): a read row per line,
-- and an attachment row per file the media store holds. It was `industryland_or_kr.rows_of`; the
-- values follow what that Python did with the JSON it read (`text_or_none`, `whole`, `ymd`,
-- `platform_contracts.rfc3339_utc`), so the rows a Spark load wrote are the rows this writes.
--
-- Input: notice_lines(line_number BIGINT, line VARCHAR): the object's non-blank lines, numbered
-- from 1. Parameters: {result_field}, {files_field}, {path_field}, {name_field} (from
-- industryland-or-kr.contract.json), {object_key}, {snapshot_id}, {ingested_at_epoch_seconds}.
-- Output: notice_reads, notice_attachments (the two tables' columns, in contract order).

-- Python's str.isspace() characters, the ones str.strip() removes.
CREATE OR REPLACE MACRO notice_ws() AS
  chr(9) || chr(10) || chr(11) || chr(12) || chr(13) || chr(28) || chr(29) || chr(30) || chr(31) || chr(32) || chr(133) || chr(160)
  || chr(5760) || chr(8192) || chr(8193) || chr(8194) || chr(8195) || chr(8196) || chr(8197) || chr(8198) || chr(8199)
  || chr(8200) || chr(8201) || chr(8202) || chr(8232) || chr(8233) || chr(8239) || chr(8287) || chr(12288);

-- Python's str() of a JSON value as `json.loads` gives it: a string as is, True/False, numbers.
CREATE OR REPLACE MACRO notice_str(v) AS
  CASE json_type(v)
    WHEN 'VARCHAR' THEN json_extract_string(v, '$')
    WHEN 'BOOLEAN' THEN CASE WHEN CAST(v AS BOOLEAN) THEN 'True' ELSE 'False' END
    WHEN 'DOUBLE' THEN CAST(CAST(v AS DOUBLE) AS VARCHAR)
    ELSE CAST(v AS VARCHAR)
  END;

-- Python truthiness of a JSON value: null, "", 0, false, [] and {} are false.
CREATE OR REPLACE MACRO notice_truthy(v) AS
  v IS NOT NULL AND CASE json_type(v)
    WHEN 'NULL' THEN false
    WHEN 'VARCHAR' THEN json_extract_string(v, '$') <> ''
    WHEN 'BOOLEAN' THEN CAST(v AS BOOLEAN)
    WHEN 'ARRAY' THEN json_array_length(v) > 0
    WHEN 'OBJECT' THEN len(json_keys(v)) > 0
    ELSE CAST(v AS DOUBLE) <> 0
  END;

-- text_or_none: the value as text, stripped; None when it is None or blank.
CREATE OR REPLACE MACRO notice_text(v) AS
  CASE WHEN v IS NULL OR json_type(v) = 'NULL' THEN NULL ELSE nullif(trim(notice_str(v), notice_ws()), '') END;

-- whole: an integer written with digits (commas allowed); None otherwise.
CREATE OR REPLACE MACRO notice_whole(v) AS
  CASE WHEN v IS NULL OR json_type(v) = 'NULL' THEN NULL
       WHEN regexp_full_match(replace(trim(notice_str(v), notice_ws()), ',', ''), '-?[0-9]+')
       THEN CAST(replace(trim(notice_str(v), notice_ws()), ',', '') AS BIGINT) END;

-- ymd: a YYYY-MM-DD date; None otherwise.
CREATE OR REPLACE MACRO notice_ymd(v) AS
  CASE WHEN notice_truthy(v) THEN CAST(try_strptime(trim(notice_str(v), notice_ws()), '%Y-%m-%d') AS DATE) END;

-- rfc3339_utc: an RFC 3339 time (fraction cut or padded to microseconds, no zone read as UTC).
CREATE OR REPLACE MACRO notice_utc(v) AS
  CASE WHEN notice_truthy(v) THEN (
    SELECT CASE WHEN m.seconds <> '' THEN TRY_CAST(
             replace(m.seconds, ' ', 'T') || '.' || rpad(left(m.fraction, 6), 6, '0')
             || CASE WHEN m.zone IN ('', 'Z', 'z') THEN '+00:00' ELSE m.zone END AS TIMESTAMPTZ) END
    FROM (SELECT unnest(regexp_extract(trim(notice_str(v), notice_ws()),
            '^([0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]+))?(Z|z|[+-][0-9]{2}:[0-9]{2})?$',
            ['seconds', 'fraction', 'zone'])) ) m) END;

-- Each line that names a notice and when it was asked; the others are not reads (rows_of skips them).
CREATE OR REPLACE TEMP TABLE notice_parsed AS
SELECT line_number, j,
       notice_whole(json_extract(j, '$.notice_sequence')) AS notice_sequence,
       notice_utc(json_extract(j, '$.requested_at_utc')) AS requested_at_utc
FROM (SELECT line_number, CAST(line AS JSON) AS j FROM notice_lines);

CREATE OR REPLACE TEMP TABLE notice_valid AS
SELECT line_number, j, notice_sequence, requested_at_utc,
       -- body = line["body"] or {}: anything else truthy that is not an object stopped the Python load.
       CASE WHEN NOT notice_truthy(json_extract(j, '$.body')) THEN '{}'::JSON
            WHEN json_type(json_extract(j, '$.body')) = 'OBJECT' THEN json_extract(j, '$.body')
            ELSE error('line ' || line_number || ': body is neither an object nor empty') END AS body
FROM notice_parsed
WHERE notice_sequence IS NOT NULL AND requested_at_utc IS NOT NULL;

CREATE OR REPLACE TEMP TABLE notice_results AS
SELECT line_number, j, notice_sequence, requested_at_utc,
       -- result = body[result] or {}, and {} when it is not an object.
       CASE WHEN notice_truthy(json_extract(body, '$."{result_field}"'))
                 AND json_type(json_extract(body, '$."{result_field}"')) = 'OBJECT'
            THEN json_extract(body, '$."{result_field}"') ELSE '{}'::JSON END AS result
FROM notice_valid;

CREATE OR REPLACE TEMP TABLE notice_files AS
SELECT r.line_number, r.notice_sequence, r.requested_at_utc, r.result,
       CASE WHEN notice_truthy(json_extract(r.result, '$."{files_field}"'))
                 AND json_type(json_extract(r.result, '$."{files_field}"')) = 'ARRAY'
            THEN json_extract(r.result, '$."{files_field}"') ELSE '[]'::JSON END AS files
FROM notice_results r;

-- One row per file, in its line's order; a file that is not an object stopped the Python load.
CREATE OR REPLACE TEMP TABLE notice_file_rows AS
SELECT f.line_number, f.notice_sequence, f.requested_at_utc, notice_text(json_extract(f.result, '$.danji_cd')) AS code,
       e.ordinal,
       CASE WHEN json_type(e.file) = 'OBJECT' THEN e.file
            ELSE error('line ' || f.line_number || ': an attachment is not an object') END AS file
FROM notice_files f,
     LATERAL (SELECT unnest(range(json_array_length(f.files)::BIGINT)) AS ordinal) i,
     LATERAL (SELECT i.ordinal, json_extract(f.files, '$[' || i.ordinal || ']') AS file) e;

CREATE OR REPLACE TEMP TABLE notice_stored AS
SELECT * FROM notice_file_rows
WHERE json_type(json_extract(file, '$.store_outcome')) = 'VARCHAR'
  AND json_extract_string(file, '$.store_outcome') IN ('stored', 'deduplicated')
  AND notice_text(json_extract(file, '$.sha256')) IS NOT NULL
  AND notice_text(json_extract(file, '$.media_format')) IS NOT NULL;

CREATE OR REPLACE TEMP TABLE notice_reads AS
SELECT
  r.notice_sequence,
  r.requested_at_utc,
  CAST(notice_whole(json_extract(r.j, '$.http_status')) AS INTEGER) AS http_status,
  CASE WHEN notice_truthy(json_extract(r.j, '$.read_outcome')) THEN notice_str(json_extract(r.j, '$.read_outcome'))
       ELSE 'failed' END AS read_outcome,
  notice_text(json_extract(r.result, '$.danji_cd')) AS official_complex_code,
  notice_text(json_extract(r.result, '$.noti_title')) AS notice_title,
  notice_text(json_extract(r.result, '$.noti_num')) AS notice_number,
  notice_ymd(json_extract(r.result, '$.noti_dtm')) AS notice_date,
  notice_text(json_extract(r.result, '$.noti_type')) AS notice_type_code,
  notice_text(json_extract(r.result, '$.noti_type_nm')) AS notice_type_raw,
  notice_text(json_extract(r.result, '$.noti_dantype')) AS complex_kind_code,
  notice_text(json_extract(r.result, '$.noti_dantype_nm')) AS complex_kind_raw,
  CASE WHEN notice_truthy(r.result) THEN CAST(json_array_length(f.files) AS INTEGER) END AS file_count,
  CASE WHEN notice_truthy(r.result)
       THEN CAST((SELECT count(*) FROM notice_stored s WHERE s.line_number = r.line_number) AS INTEGER) END AS files_stored,
  '{object_key}' AS source_record_id,
  CAST(r.line_number AS INTEGER) AS source_line_number,
  '{snapshot_id}' AS source_snapshot_id,
  to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM notice_results r JOIN notice_files f USING (line_number)
ORDER BY r.line_number;

-- One row per (notice, file hash): the last file of the object that names it decides it.
CREATE OR REPLACE TEMP TABLE notice_attachments AS
SELECT
  notice_sequence,
  sha256,
  CAST(coalesce(notice_whole(json_extract(file, '$.file_sequence')), 0) AS INTEGER) AS file_sequence,
  code AS official_complex_code,
  notice_text(json_extract(file, '$.file_name')) AS file_title,
  notice_text(json_extract(file, '$.file_extension')) AS file_extension_raw,
  notice_whole(json_extract(file, '$.file_size')) AS provider_file_size,
  notice_text(json_extract(file, '$."{path_field}"')) AS provider_file_path,
  notice_text(json_extract(file, '$."{name_field}"')) AS provider_file_id,
  coalesce(notice_text(json_extract(file, '$.source_url')), '') AS source_url,
  notice_text(json_extract(file, '$.media_format')) AS media_format,
  coalesce(notice_whole(json_extract(file, '$.size_bytes')), 0) AS size_bytes,
  coalesce(notice_utc(json_extract(file, '$.fetched_at_utc')), requested_at_utc) AS fetched_at_utc,
  '{object_key}' AS source_record_id,
  CAST(line_number AS INTEGER) AS source_line_number,
  '{snapshot_id}' AS source_snapshot_id,
  to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM (
  SELECT *, notice_text(json_extract(file, '$.sha256')) AS sha256,
         row_number() OVER (PARTITION BY notice_sequence, notice_text(json_extract(file, '$.sha256'))
                            ORDER BY line_number DESC, ordinal DESC) AS last
  FROM notice_stored
)
WHERE last = 1
ORDER BY notice_sequence, file_sequence;
