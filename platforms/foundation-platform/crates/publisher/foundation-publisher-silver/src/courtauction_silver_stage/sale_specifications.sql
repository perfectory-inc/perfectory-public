-- A 매각물건명세서 object's rows (courtauction_go_kr.spec_read_rows_of): one per version asked, the
-- last line naming a version winning. A line without its key refuses the object (the caller checks
-- `ca_spec_lines`). Parameters: {object_key}, {snapshot_id}, {ingested_at_epoch_seconds}.
CREATE OR REPLACE TEMP TABLE ca_spec_lines AS
SELECT line_number,
       py_text(json_extract(asked, '$.court_office_code')) AS court_office_code,
       py_text(json_extract(asked, '$.case_no')) AS case_no,
       py_text(json_extract(asked, '$.item_seq')) AS item_seq,
       py_text(json_extract(asked, '$.document_id')) AS document_id,
       onbid_ymd(json_extract(asked, '$.written_date')) AS spec_written_date,
       py_text(json_extract(facts, '$.document_name')) AS document_name,
       onbid_ymd(json_extract(facts, '$.document_written_ymd')) AS document_written_date,
       coalesce(py_text(json_extract(j, '$.outcome')), 'failed') AS outcome,
       ca_whole(json_extract(facts, '$.log_http_status')) AS log_http_status,
       ca_whole(json_extract(facts, '$.info_http_status')) AS info_http_status,
       ca_whole(json_extract(facts, '$.file_http_status')) AS file_http_status,
       py_text(json_extract(j, '$.sha256')) AS sha256,
       py_text(json_extract(j, '$.media_format')) AS media_format,
       ca_whole(json_extract(j, '$.size_bytes')) AS size_bytes,
       coalesce(notice_utc(json_extract(j, '$.requested_at_utc')), to_timestamp({ingested_at_epoch_seconds})) AS requested_at_utc
FROM (SELECT line_number, j,
             CASE WHEN py_truthy(json_extract(j, '$.request')) THEN json_extract(j, '$.request') ELSE '{}'::JSON END AS asked,
             CASE WHEN py_truthy(json_extract(j, '$.facts')) THEN json_extract(j, '$.facts') ELSE '{}'::JSON END AS facts
      FROM ca_parsed);

CREATE OR REPLACE TEMP TABLE ca_court_auction_sale_specification_read AS
SELECT court_office_code, case_no, item_seq, document_id, spec_written_date, document_name, document_written_date,
       outcome, CAST(log_http_status AS INTEGER) AS log_http_status, CAST(info_http_status AS INTEGER) AS info_http_status,
       CAST(file_http_status AS INTEGER) AS file_http_status, sha256, media_format, size_bytes, requested_at_utc,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM ca_spec_lines
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, item_seq, document_id
                           ORDER BY line_number DESC) = 1;
