-- A 문건·송달 object's document rows (courtauction_go_kr.case_answer_rows, role `case_filings`, root
-- ADR-0575 §5), after case_lines.sql: per case, the 문건 and 송달 lists of the last line that answered
-- (an empty list included), each text kept as its role, document kind and action, never the name.
CREATE OR REPLACE TEMP VIEW ca_filing_answer AS
SELECT * FROM ca_case_lines
WHERE answered
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no ORDER BY line_number DESC) = 1;

CREATE OR REPLACE TEMP TABLE ca_court_auction_case_filing AS
WITH listed AS (
  SELECT court_office_code, case_no, line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(data, '$.dlt_ofdocDtsLst')) AS l FROM ca_filing_answer)
)
SELECT court_office_code, case_no, CAST(n AS INTEGER) AS filing_seq,
       onbid_ymd(json_extract(item, '$.ofdocRcptYmd')) AS received_date,
       parts.role AS filer_role, parts.kind AS document_kind, parts.action AS action,
       py_text(json_extract(item, '$.rcptRslt')) AS result,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM (SELECT *, ca_filing_parts(json_extract(item, '$.rcptDts')) AS parts FROM listed);

CREATE OR REPLACE TEMP TABLE ca_court_auction_case_delivery AS
WITH listed AS (
  SELECT court_office_code, case_no, line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(data, '$.dlt_dlvrDtsLst')) AS l FROM ca_filing_answer)
)
SELECT court_office_code, case_no, CAST(n AS INTEGER) AS delivery_seq,
       onbid_ymd(json_extract(item, '$.dlvrbkRegYmd')) AS registered_date,
       reached.reached AS reached_date, reached.result AS delivery_result,
       parts.role AS recipient_role, parts.kind AS document_kind, parts.action AS action,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM (SELECT *, ca_filing_parts(json_extract(item, '$.dlvrDts')) AS parts,
             ca_reached_parts(json_extract(item, '$.lastDlvrblRchYmd')) AS reached
      FROM listed);
