-- A 사건 문서 object's lines (courtauction_go_kr.case_document_rows, root ADR-0540 §1, §4): each line's
-- case key read back from its request (`case_documents.key_fields`; a line without one refuses the
-- object, the caller checks `ca_case_lines`), its asked and answered rounds, its answer's `data`
-- (an object, `{}` for a failed read or any other body) and whether it has one to read rows from.
-- Every line is one read row; the last line naming a read key wins.
-- Parameters: {document}, {request_object}, {round_field} (`NULL` when the document has none),
-- {answered_round_path} (`NULL` when it has none), {object_key}, {snapshot_id},
-- {ingested_at_epoch_seconds}.
CREATE OR REPLACE TEMP TABLE ca_case_lines AS
SELECT line_number, j,
       py_text(json_extract(asked, '$.cortOfcCd')) AS court_office_code,
       py_text(json_extract(asked, '$.csNo')) AS case_no,
       CASE WHEN {round_field} IS NULL THEN 0
            ELSE coalesce(ca_whole(json_extract(asked, '$.' || {round_field})), 0) END AS asked_round,
       CASE WHEN {answered_round_path} IS NOT NULL THEN ca_whole(json_extract(data, {answered_round_path})) END
         AS answered_round,
       data,
       py_truthy(data) AS answered
FROM (SELECT line_number, j,
             coalesce(CASE WHEN py_truthy(json_extract(j, '$.request'))
                           THEN json_extract(j, '$.request."{request_object}"') END, '{}'::JSON) AS asked,
             CASE WHEN read THEN coalesce(ca_data(body), '{}'::JSON) ELSE '{}'::JSON END AS data
      FROM ca_parsed);

CREATE OR REPLACE TEMP TABLE ca_court_auction_case_document_read AS
SELECT court_office_code, case_no, '{document}' AS document, CAST(asked_round AS INTEGER) AS asked_round,
       CAST(answered_round AS INTEGER) AS answered_round,
       coalesce(notice_utc(json_extract(j, '$.requested_at_utc')), to_timestamp({ingested_at_epoch_seconds}))
         AS requested_at_utc,
       CAST(ca_whole(json_extract(j, '$.http_status')) AS INTEGER) AS http_status,
       CASE WHEN py_truthy(json_extract(j, '$.read_outcome')) THEN py_str(json_extract(j, '$.read_outcome'))
            ELSE 'read' END AS read_outcome,
       py_text(json_extract(j, '$.list_fingerprint')) AS list_fingerprint,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM ca_case_lines
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, asked_round ORDER BY line_number DESC) = 1;

