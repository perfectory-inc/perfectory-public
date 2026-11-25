-- A 매각공고 object's rows (courtauction_go_kr.notice_rows_of, root ADR-0575 §6): one notice row per
-- listed notice (its list row, the last list line naming it winning, with the case count of the
-- last detail line after that one), and one edition per notice of its lists (`sale`, `correction`,
-- `cancellation`), the last detail line naming the notice winning (an empty list included). Only
-- read lines whose body is an object with an object `data`. A detail line without its key refuses
-- the object (the caller checks `ca_notice_detail_lines`).
-- Parameters: {object_path}, {list_operation}, {detail_operation}, {detail_request_object}, {object_key},
-- {snapshot_id}, {ingested_at_epoch_seconds}.
-- The file is read here, not through lines.sql (root ADR-0616 §3-1): a notice answers up to 130 KiB a
-- line, and its body is parsed once by json_transform into only what the rows below read (the notice
-- list, the case count and the three lists, each a list of JSON values), so no statement holds a whole
-- body. Kept: read lines whose body is an object with an object `data` (`ca_data`); a body is an
-- object when it is JSON whose first character past the blanks is `{`.
CREATE OR REPLACE TEMP TABLE ca_notice_answers AS
SELECT line_number, json_extract_string(operation, '$') AS operation, request,
       json_transform(text, '{"data": {"dlt_rletDspslPbancLst": ["JSON"],
                                       "result": {"dspslPbanc": {"csCnt": "JSON", "pbancInfo": {"lst": ["JSON"]}},
                                                  "crrctPbancLst": ["JSON"], "rtrcnPbancLst": ["JSON"]}}}').data AS data
FROM (SELECT ordinality AS line_number, operation, request, read_outcome, py_truthy(body) AS truthy,
             json_extract_string(body, '$') AS text
      FROM read_json('{object_path}', format = 'newline_delimited', records = true, maximum_object_size = 33554432,
                     columns = {operation: 'JSON', request: 'JSON', read_outcome: 'JSON', body: 'JSON'})
           WITH ORDINALITY)
WHERE coalesce(json_extract_string(read_outcome, '$') = 'read', false)
  AND truthy AND json_valid(text) AND left(ltrim(text, ' ' || chr(9) || chr(10) || chr(13)), 1) = '{'
  -- json_type with the path asks the type in the parse: json_extract would write `data` out again.
  AND json_type(text, '$.data') = 'OBJECT';

-- Each detail line's key, read back from its request, and what the notice rows take from its data:
-- the case count and the three lists, each of its objects only (`_list`).
CREATE OR REPLACE TEMP TABLE ca_notice_detail_lines AS
SELECT line_number,
       ca_whole(data.result.dspslPbanc.csCnt) AS case_count,
       list_filter(coalesce(data.result.dspslPbanc.pbancInfo.lst, []), lambda x: json_type(x) = 'OBJECT') AS sale_list,
       list_filter(coalesce(data.result.crrctPbancLst, []), lambda x: json_type(x) = 'OBJECT') AS correction_list,
       list_filter(coalesce(data.result.rtrcnPbancLst, []), lambda x: json_type(x) = 'OBJECT') AS cancellation_list,
       py_text(json_extract(asked, '$.cortOfcCd')) AS court_office_code,
       py_text(json_extract(asked, '$.jdbnCd')) AS department_code,
       onbid_ymd(json_extract(asked, '$.dspslDxdyYmd')) AS sale_date,
       py_text(json_extract(asked, '$.bidDvsCd')) AS bid_kind_code
FROM (SELECT line_number, data,
             coalesce(CASE WHEN py_truthy(request)
                           THEN json_extract(request, '$."{detail_request_object}"') END, '{}'::JSON) AS asked
      FROM ca_notice_answers
      WHERE operation = '{detail_operation}');

CREATE OR REPLACE TEMP TABLE ca_notice_listed AS
WITH listed AS (
  SELECT line_number, unnest(l) AS item, generate_subscripts(l, 1) AS ordinal
  FROM (SELECT line_number, list_filter(data.dlt_rletDspslPbancLst, lambda x: json_type(x) = 'OBJECT') AS l
        FROM ca_notice_answers
        WHERE operation = '{list_operation}' AND data.dlt_rletDspslPbancLst IS NOT NULL)
)
SELECT line_number, ordinal, item,
       py_text(json_extract(item, '$.cortOfcCd')) AS court_office_code,
       py_text(json_extract(item, '$.jdbnCd')) AS department_code,
       onbid_ymd(json_extract(item, '$.dspslDxdyYmd')) AS sale_date,
       py_text(json_extract(item, '$.bidDvsCd')) AS bid_kind_code
FROM listed;
DROP TABLE ca_notice_answers;

CREATE OR REPLACE TEMP TABLE ca_court_auction_sale_notice AS
WITH notices AS (
  SELECT *
  FROM ca_notice_listed
  WHERE court_office_code IS NOT NULL AND department_code IS NOT NULL AND sale_date IS NOT NULL
    AND bid_kind_code IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY court_office_code, department_code, sale_date, bid_kind_code
                             ORDER BY line_number DESC, ordinal DESC) = 1
), counts AS (
  -- The last detail line after the notice's list line sets its count (None when it gives none).
  SELECT n.court_office_code, n.department_code, n.sale_date, n.bid_kind_code,
         d.case_count
  FROM notices n
  JOIN ca_notice_detail_lines d
    ON d.court_office_code = n.court_office_code AND d.department_code = n.department_code
   AND d.sale_date = n.sale_date AND d.bid_kind_code = n.bid_kind_code AND d.line_number > n.line_number
  QUALIFY row_number() OVER (PARTITION BY n.court_office_code, n.department_code, n.sale_date, n.bid_kind_code
                             ORDER BY d.line_number DESC) = 1
)
SELECT n.court_office_code, n.department_code, n.sale_date, n.bid_kind_code,
       py_text(json_extract(item, '$.cortAuctnJdbnNm')) AS department_name,
       py_text(json_extract(item, '$.jdbnTelno')) AS department_phone,
       py_text(json_extract(item, '$.dspslPlcNm')) AS sale_place,
       py_text(json_extract(item, '$.fstDspslHm')) AS first_time_raw,
       py_text(json_extract(item, '$.scndDspslHm')) AS second_time_raw,
       py_text(json_extract(item, '$.thrdDspslHm')) AS third_time_raw,
       py_text(json_extract(item, '$.fothDspslHm')) AS fourth_time_raw,
       onbid_ymd(json_extract(item, '$.bidBgngYmd')) AS bid_start_date,
       onbid_ymd(json_extract(item, '$.bidEndYmd')) AS bid_end_date,
       CAST(ca_whole(json_extract(item, '$.corCnt')) AS INTEGER) AS correction_count,
       CAST(ca_whole(json_extract(item, '$.canCnt')) AS INTEGER) AS cancellation_count,
       CAST(c.case_count AS INTEGER) AS case_count,
       '{object_key}' AS source_record_id, CAST(n.line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM notices n
LEFT JOIN counts c
  ON c.court_office_code = n.court_office_code AND c.department_code = n.department_code
 AND c.sale_date = n.sale_date AND c.bid_kind_code = n.bid_kind_code;

-- The detail line each notice's lists come from: the last one naming it.
CREATE OR REPLACE TEMP TABLE ca_court_auction_sale_notice_item AS
WITH chosen AS (
  SELECT *
  FROM ca_notice_detail_lines
  WHERE court_office_code IS NOT NULL AND department_code IS NOT NULL AND sale_date IS NOT NULL
    AND bid_kind_code IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY court_office_code, department_code, sale_date, bid_kind_code
                             ORDER BY line_number DESC) = 1
), lists AS (
  SELECT *, 'sale' AS list_name,
         sale_list AS l
  FROM chosen
  UNION ALL
  SELECT *, 'correction' AS list_name, correction_list AS l
  FROM chosen
  UNION ALL
  SELECT *, 'cancellation' AS list_name, cancellation_list AS l
  FROM chosen
), items AS (
  SELECT court_office_code, department_code, sale_date, bid_kind_code, line_number, list_name,
         unnest(l) AS item, generate_subscripts(l, 1) AS ordinal
  FROM lists
)
SELECT court_office_code, department_code, sale_date, bid_kind_code,
       list_name AS list,
       CAST(row_number() OVER (PARTITION BY court_office_code, department_code, sale_date, bid_kind_code, list_name
                               ORDER BY ordinal) AS INTEGER) AS list_seq,
       nullif(trim(split_part(coalesce(ca_plain(json_extract(item, '$.csNo')), ''), chr(10), 1), py_ws()), '')
         AS user_case_no,
       py_text(json_extract(item, '$.dspslGdsCsNo')) AS sale_case_no,
       CAST(ca_whole(json_extract(item, '$.dspslSeq')) AS INTEGER) AS property_seq,
       py_text(CASE WHEN py_truthy(json_extract(item, '$.usgNm')) THEN json_extract(item, '$.usgNm')
                    ELSE json_extract(item, '$.dspslUsgNm') END) AS usage_name,
       ca_amount(json_extract(item, '$.aeeEvlAmt')) AS appraisal_amount_krw,
       ca_amount(json_extract(item, '$.lwsDspslPrc')) AS minimum_bid_price_krw,
       ca_plain(json_extract(item, '$.dspslRmk')) AS remarks,
       ca_plain(json_extract(item, '$.st')) AS property_text,
       ca_plain(json_extract(item, '$.crrctCtt')) AS change_text,
       onbid_ymd(json_extract(item, '$.crrctPbancYmd')) AS change_date,
       py_text(json_extract(item, '$.rletPbancDvsCd')) AS notice_kind_code,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM items;
