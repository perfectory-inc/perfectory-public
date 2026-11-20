-- A 감정평가서 object's rows (courtauction_go_kr.report_rows_of): one read row per line, and one
-- report row per line that stored a file (outcome `stored`, a hash and a media format), the last
-- line naming a round key winning in each. A line without its key refuses the object (the caller
-- checks `ca_report_lines`). Parameters: {object_key}, {snapshot_id}, {ingested_at_epoch_seconds}.
CREATE OR REPLACE TEMP TABLE ca_report_lines AS
SELECT line_number,
       py_text(json_extract(asked, '$.court_office_code')) AS court_office_code,
       py_text(json_extract(asked, '$.case_no')) AS case_no,
       ca_whole(json_extract(asked, '$.round_seq')) AS round_seq,
       py_text(json_extract(asked, '$.appraisal_no')) AS appraisal_no,
       onbid_ymd(json_extract(asked, '$.written_date')) AS written_date,
       coalesce(py_text(json_extract(j, '$.outcome')), 'failed') AS outcome,
       ca_whole(json_extract(facts, '$.http_status')) AS http_status,
       ca_whole(json_extract(facts, '$.file_http_status')) AS file_http_status,
       py_text(json_extract(j, '$.sha256')) AS sha256,
       py_text(json_extract(j, '$.media_format')) AS media_format_text,
       json_extract(j, '$.media_format') AS media_format_raw,
       ca_whole(json_extract(j, '$.size_bytes')) AS size_bytes,
       coalesce(notice_utc(json_extract(j, '$.requested_at_utc')), to_timestamp({ingested_at_epoch_seconds})) AS requested_at_utc
FROM (SELECT line_number, j,
             CASE WHEN py_truthy(json_extract(j, '$.request')) THEN json_extract(j, '$.request') ELSE '{}'::JSON END AS asked,
             CASE WHEN py_truthy(json_extract(j, '$.facts')) THEN json_extract(j, '$.facts') ELSE '{}'::JSON END AS facts
      FROM ca_parsed);

CREATE OR REPLACE TEMP TABLE ca_court_auction_appraisal_report_read AS
SELECT court_office_code, case_no, CAST(round_seq AS INTEGER) AS round_seq, appraisal_no, written_date, outcome,
       CAST(http_status AS INTEGER) AS http_status, CAST(file_http_status AS INTEGER) AS file_http_status, sha256,
       requested_at_utc,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM ca_report_lines
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, round_seq, appraisal_no, written_date
                           ORDER BY line_number DESC) = 1;

-- `line["media_format"]` as received: the Python kept the value itself, a text as is.
CREATE OR REPLACE TEMP TABLE ca_court_auction_appraisal_report AS
SELECT court_office_code, case_no, CAST(round_seq AS INTEGER) AS round_seq, appraisal_no, written_date, sha256,
       py_str(media_format_raw) AS media_format, size_bytes, requested_at_utc,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM ca_report_lines
WHERE outcome = 'stored' AND sha256 IS NOT NULL AND media_format_text IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, round_seq, appraisal_no, written_date
                           ORDER BY line_number DESC) = 1;
