-- A 배당요구종기공고 object's rows (courtauction_go_kr.dividend_rows_of): one notice row per listed
-- case (the notice lists) and one row per property of each case's 물건 list, the last line naming a
-- key winning. Only read lines whose body is an object with an object `data`. The list's creditor,
-- debtor and owner fields name people and are never read.
-- Parameters: {list_operation}, {objects_operation}, {object_key}, {snapshot_id},
-- {ingested_at_epoch_seconds}.
CREATE OR REPLACE TEMP TABLE ca_dividend_answers AS
SELECT line_number, json_extract_string(j, '$.operation') AS operation, ca_data(j) AS data
FROM ca_parsed
WHERE read AND ca_data(j) IS NOT NULL;

CREATE OR REPLACE TEMP TABLE ca_court_auction_dividend_notice AS
WITH lists AS (
  SELECT line_number, json_extract(data, '$.dlt_lstprdPbancCorOfc') AS list
  FROM ca_dividend_answers
  WHERE operation = '{list_operation}' AND json_type(json_extract(data, '$.dlt_lstprdPbancCorOfc')) = 'ARRAY'
), listed AS (
  SELECT l.line_number, i.ordinal, i.item
  FROM lists l,
  LATERAL (SELECT unnest(range(CAST(json_array_length(l.list) AS BIGINT))) AS ordinal,
                 unnest(json_extract(l.list, '$[*]')) AS item) i
  WHERE json_type(i.item) = 'OBJECT'
)
SELECT py_text(json_extract(item, '$.cortOfcCd')) AS court_office_code,
       py_text(json_extract(item, '$.csNo')) AS case_no,
       py_text(json_extract(item, '$.userCsNo')) AS user_case_no,
       py_text(json_extract(item, '$.jdbnCd')) AS department_code,
       py_text(json_extract(item, '$.cortAuctnJdbnNm')) AS department_name,
       py_text(json_extract(item, '$.orddcsDvsCd')) AS order_kind_code,
       onbid_ymd(json_extract(item, '$.csCmdcYmd')) AS commencement_date,
       onbid_ymd(json_extract(item, '$.dstrtDemnLstprdPbancYmd')) AS notice_date,
       onbid_ymd(json_extract(item, '$.dstrtDemnLstprdYmd')) AS deadline_date,
       CAST(ca_whole(json_extract(item, '$.dspslObjctCnt')) AS INTEGER) AS object_count,
       py_text(json_extract(item, '$.auctnLstDvsCd')) AS list_kind_code,
       py_text(json_extract(item, '$.rprsLtnoAddr')) AS lot_raw,
       py_text(json_extract(item, '$.bldNm')) AS building_name,
       py_text(json_extract(item, '$.realSt')) AS print_address_raw,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed
WHERE py_text(json_extract(item, '$.cortOfcCd')) IS NOT NULL AND py_text(json_extract(item, '$.csNo')) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no ORDER BY line_number DESC, ordinal DESC) = 1;

CREATE OR REPLACE TEMP TABLE ca_court_auction_dividend_object AS
WITH lists AS (
  SELECT line_number, json_extract(data, '$.dlt_dstrtDemnLstDts') AS list
  FROM ca_dividend_answers
  WHERE operation = '{objects_operation}' AND json_type(json_extract(data, '$.dlt_dstrtDemnLstDts')) = 'ARRAY'
), listed AS (
  SELECT l.line_number, i.ordinal, i.item
  FROM lists l,
  LATERAL (SELECT unnest(range(CAST(json_array_length(l.list) AS BIGINT))) AS ordinal,
                 unnest(json_extract(l.list, '$[*]')) AS item) i
  WHERE json_type(i.item) = 'OBJECT'
)
SELECT py_text(json_extract(item, '$.cortOfcCd')) AS court_office_code,
       py_text(json_extract(item, '$.csNo')) AS case_no,
       CAST(ca_whole(json_extract(item, '$.dspslObjctSeq')) AS INTEGER) AS object_seq,
       py_text(json_extract(item, '$.objctRletCarUnqNo')) AS registry_unique_no,
       py_text(json_extract(item, '$.auctnLstDvsCd')) AS list_kind_code,
       py_text(json_extract(item, '$.usgNm')) AS usage_name,
       py_text(json_extract(item, '$.rgstRcrd')) AS register_record,
       py_text(json_extract(item, '$.rprsLtnoAddr')) AS lot_raw,
       py_text(json_extract(item, '$.bldNm')) AS building_name,
       py_text(json_extract(item, '$.bldDtlDts')) AS building_unit_raw,
       py_text(json_extract(item, '$.realSt')) AS print_address_raw,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed
WHERE py_text(json_extract(item, '$.cortOfcCd')) IS NOT NULL AND py_text(json_extract(item, '$.csNo')) IS NOT NULL
  AND ca_whole(json_extract(item, '$.dspslObjctSeq')) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, object_seq ORDER BY line_number DESC, ordinal DESC) = 1;
