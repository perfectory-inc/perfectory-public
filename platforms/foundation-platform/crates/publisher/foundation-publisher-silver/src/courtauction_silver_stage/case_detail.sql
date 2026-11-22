-- A 사건상세 object's document rows (courtauction_go_kr.case_answer_rows, role `case_detail`), after
-- case_lines.sql: the case row (the last line stating it winning), and per case the lists of the last
-- line that answered (an empty list included): related and merged cases, the 매각물건 objects (one per
-- object number, the first naming it) and the parties. The fields that name a person (당사자
-- `intrpsNm`, 지분 서술 `dspslStkCtt`) are never read.
-- Parameters: {object_code} (`code_of(item, OBJECT_CODE)` of `item`), and case_lines.sql's.
-- The base and the deadline list are taken out of the answer into a table first: the macro reads its
-- arguments once per field, and in one statement the optimizer reads them out of the whole answer
-- each time (a 3,980-line 물건상세 object above 2 GiB, 2026-11-22).
CREATE OR REPLACE TEMP TABLE ca_case_bases AS
SELECT court_office_code, case_no, line_number, json_extract(data, '$.dma_csBasInf') AS base, json_extract(data, '$.dlt_dstrtDemnLstprdDts') AS deadlines
FROM ca_case_lines
WHERE answered AND ca_case_base(json_extract(data, '$.dma_csBasInf'));

CREATE OR REPLACE TEMP TABLE ca_court_auction_case AS
SELECT court_office_code, case_no, unnest(ca_case_row(base, deadlines)),
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM ca_case_bases
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no ORDER BY line_number DESC) = 1;

CREATE OR REPLACE TEMP VIEW ca_case_answer AS
SELECT * FROM ca_case_lines
WHERE answered
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no ORDER BY line_number DESC) = 1;

CREATE OR REPLACE TEMP TABLE ca_court_auction_case_related AS
WITH listed AS (
  SELECT court_office_code, case_no, line_number, 1 AS part, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(data, '$.dlt_rletReltCsLst')) AS l FROM ca_case_answer)
  UNION ALL
  SELECT court_office_code, case_no, line_number, 2 AS part, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(data, '$.dlt_dpcnMrgTrnscsCsRlet')) AS l FROM ca_case_answer)
)
SELECT court_office_code, case_no,
       CAST(row_number() OVER (PARTITION BY court_office_code, case_no ORDER BY part, n) AS INTEGER) AS related_seq,
       CASE part WHEN 1 THEN 'related' ELSE 'merged' END AS list,
       py_text(json_extract(item, CASE part WHEN 1 THEN '$.reltCsCortOfcCd' ELSE '$.reltCortOfcCd' END))
         AS related_court_office_code,
       py_text(json_extract(item, '$.reltCsNo')) AS related_case_no,
       py_text(json_extract(item, '$.userReltCsNo')) AS related_user_case_no,
       py_text(json_extract(item, '$.reltCsDvsCd')) AS relation_kind_code,
       CASE part WHEN 1 THEN py_text(json_extract(item, '$.reltCsDvsNm')) END AS relation_kind_name,
       py_text(json_extract(item, '$.cortOfcNm')) AS related_court_name,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed;

CREATE OR REPLACE TEMP TABLE ca_court_auction_case_object AS
WITH listed AS (
  SELECT court_office_code, case_no, line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(data, '$.dlt_rletCsDspslObjctLst')) AS l FROM ca_case_answer)
)
SELECT court_office_code, case_no,
       CAST(ca_whole(json_extract(item, '$.dspslObjctSeq')) AS INTEGER) AS object_seq,
       py_text(json_extract(item, '$.auctnLstDvsCd')) AS list_kind_code,
       py_text(json_extract(item, '$.auctnLstNm')) AS list_kind_name,
       py_text(json_extract(item, '$.ultmtDvsCd')) AS closure_kind_code,
       py_text(json_extract(item, '$.ultmtNm')) AS closure_kind_name,
       {object_code} AS legal_dong_code,
       py_text(json_extract(item, '$.rprsLtnoAddr')) AS lot_raw,
       py_text(json_extract(item, '$.bldNm')) AS building_name,
       py_text(json_extract(item, '$.bldDtlDts')) AS building_unit_raw,
       py_text(json_extract(item, '$.userSt')) AS print_address_raw,
       py_text(json_extract(item, '$.rgstRcrdYn')) AS register_record_yn,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed
WHERE ca_whole(json_extract(item, '$.dspslObjctSeq')) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, object_seq ORDER BY n) = 1;

CREATE OR REPLACE TEMP TABLE ca_court_auction_case_party AS
WITH listed AS (
  SELECT court_office_code, case_no, line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(data, '$.dlt_rletCsIntrpsLst')) AS l FROM ca_case_answer)
)
SELECT court_office_code, case_no, CAST(n AS INTEGER) AS party_seq,
       py_text(json_extract(item, '$.auctnIntrpsDvsCd')) AS party_kind_code,
       py_text(json_extract(item, '$.auctnIntrpsDvsNm')) AS party_kind_name,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed;
