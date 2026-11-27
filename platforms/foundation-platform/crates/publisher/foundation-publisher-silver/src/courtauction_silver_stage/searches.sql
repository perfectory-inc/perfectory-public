-- A 물건 검색 or 매각결과 검색 object's rows (courtauction_go_kr.rows_of and item_row): one row per 물건
-- (court office, case, property and item sequence), the last line that holds it winning, the later
-- answer when the list shifted between pages. Only lines answered with HTTP 200 whose body is JSON
-- with a `data.dlt_srchResult` list. A row without its key refuses the object (the caller checks
-- `ca_search_lines`). The PNU is not placed here: `place_rows` hands each row's source code PNU and
-- its two addresses to the one place-identity resolver (place_identity_stage).
-- Parameters: {object_path}, {maximum_line_bytes}, {code_fields} (`code_of(item, FROM_CODE)` of `item`),
-- {lot_field}, {object_key}, {snapshot_id}, {ingested_at_epoch_seconds}.
-- The file is read here once, not through lines.sql (root ADR-0616 §3-1): a search answers up to 165 KiB
-- a line, and each body is parsed once by json_transform into what this file and the pages read
-- (`ca_search_answers`: the 물건 list, each 물건 in its declared shape of the fields this file and
-- {code_fields} read, each a JSON value so the value rules read what they read before, and the
-- answer's totalCnt), with whether the body is an object; the pages
-- (`courtauction_silver_stage::search_pages_sql`) are read from it, not from the file again. No
-- statement holds a whole body or a whole 물건, and no 물건 is parsed again per field (2026-11-25:
-- 38 parses a 물건, and the file read twice, needed over 2 GiB for one day's search). A field present
-- as null reads as a missing one does, as `item.get` reads both. Line numbers as lines.sql numbers
-- them.
CREATE OR REPLACE TEMP TABLE ca_search_answers AS
SELECT ordinality AS line_number, request,
       CASE WHEN json_valid(json_extract_string(body, '$'))
            THEN json_transform(json_extract_string(body, '$'), '{"data": {"dlt_srchResult": [{
                     "boCd": "JSON", "saNo": "JSON", "maemulSer": "JSON", "mokmulSer": "JSON", "srnSaNo": "JSON",
                     "jiwonNm": "JSON", "jpDeptNm": "JSON", "gamevalAmt": "JSON", "minmaePrice": "JSON",
                     "maeGiil": "JSON", "maeHh1": "JSON", "maegyuljGiil": "JSON", "maePlace": "JSON",
                     "yuchalCnt": "JSON", "maeAmt": "JSON", "jinstatCd": "JSON", "mulStatcd": "JSON",
                     "mulJinYn": "JSON", "lclsUtilCd": "JSON", "mclsUtilCd": "JSON", "sclsUtilCd": "JSON",
                     "hjguSido": "JSON", "hjguSigu": "JSON", "hjguDong": "JSON", "hjguRd": "JSON",
                     "daepyoSidoCd": "JSON", "daepyoSiguCd": "JSON", "daepyoDongCd": "JSON", "daepyoRdCd": "JSON",
                     "daepyoLotno": "JSON", "rdNm": "JSON", "buldNo": "JSON", "buldNm": "JSON", "printSt": "JSON",
                     "wgs84Xcordi": "JSON", "wgs84Ycordi": "JSON"}], "dma_pageInfo": {"totalCnt": "JSON"}}}') END AS b,
       coalesce(left(ltrim(json_extract_string(body, '$'), ' ' || chr(9) || chr(10) || chr(13)), 1) = '{', false) AS object_body
FROM read_json('{object_path}', format = 'newline_delimited', records = true, maximum_object_size = {maximum_line_bytes},
               columns = {request: 'JSON', http_status: 'JSON', body: 'JSON'})
     WITH ORDINALITY
WHERE json_type(http_status) IN ('BIGINT', 'UBIGINT', 'DOUBLE')
  AND TRY_CAST(http_status AS DOUBLE) = 200;

CREATE OR REPLACE TEMP TABLE ca_search_lines AS
WITH listed AS (
  SELECT line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT line_number, coalesce(b.data.dlt_srchResult, []) AS l FROM ca_search_answers)
)
SELECT line_number, n, item,
       trim(coalesce(py_str(item.boCd), 'None'), py_ws()) AS court_office_code,
       trim(coalesce(py_str(item.saNo), 'None'), py_ws()) AS case_no,
       ca_whole(item.maemulSer) AS property_seq,
       ca_whole(item.mokmulSer) AS item_seq
FROM listed;

CREATE OR REPLACE TEMP TABLE ca_search_rows AS
SELECT court_office_code, case_no, CAST(property_seq AS INTEGER) AS property_seq, CAST(item_seq AS INTEGER) AS item_seq,
       py_text(item.srnSaNo) AS user_case_no,
       py_text(item.jiwonNm) AS court_name,
       py_text(item.jpDeptNm) AS department_name,
       ca_whole(item.gamevalAmt) AS appraisal_amount_krw,
       ca_whole(item.minmaePrice) AS minimum_bid_price_krw,
       onbid_ymd(item.maeGiil) AS sale_date,
       py_text(item.maeHh1) AS sale_time_raw,
       onbid_ymd(item.maegyuljGiil) AS sale_decision_date,
       py_text(item.maePlace) AS sale_place,
       CAST(ca_whole(item.yuchalCnt) AS INTEGER) AS failed_bid_count,
       nullif(ca_whole(item.maeAmt), 0) AS sale_amount_krw,
       py_text(item.jinstatCd) AS case_status_code,
       py_text(item.mulStatcd) AS item_status_code,
       py_text(item.mulJinYn) AS item_in_progress_yn,
       py_text(item.lclsUtilCd) AS usage_lcls_code,
       py_text(item.mclsUtilCd) AS usage_mcls_code,
       py_text(item.sclsUtilCd) AS usage_scls_code,
       py_text(item.hjguSido) AS address_sido_name,
       py_text(item.hjguSigu) AS address_sigungu_name,
       py_text(item.hjguDong) AS address_dong_name,
       py_text(item.hjguRd) AS address_ri_name,
       py_text(item.daepyoLotno) AS representative_lot_raw,
       py_text(item.rdNm) AS road_name,
       py_text(item.buldNo) AS building_no_raw,
       py_text(item.buldNm) AS building_name,
       py_text(item.printSt) AS print_address_raw,
       ca_decimal(item.wgs84Xcordi) AS wgs84_x,
       ca_decimal(item.wgs84Ycordi) AS wgs84_y,
       -- code_lot_pnu: the 10-digit code and the lot (산 -> ledger 2) as a 19-digit PNU, or none; a lot
       -- over 9999 names none.
       (SELECT CASE WHEN {code_fields} IS NOT NULL AND x.main <> ''
                     AND place_int(x.main) <= 9999 AND coalesce(place_int(nullif(x.sub, '')), 0) <= 9999
                    THEN {code_fields} || CASE WHEN x.mountain = '산' THEN '2' ELSE '1' END
                         || lpad(CAST(place_int(x.main) AS VARCHAR), 4, '0')
                         || lpad(CAST(coalesce(place_int(nullif(x.sub, '')), 0) AS VARCHAR), 4, '0') END
        FROM (SELECT unnest(regexp_extract(
                regexp_replace(coalesce(py_text(item.{lot_field}), ''), '[' || py_ws() || ']+', '', 'g'),
                place_lot_re(), ['mountain', 'main', 'sub']))) x) AS source_code_pnu_raw,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM ca_search_lines
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, property_seq, item_seq
                           ORDER BY line_number DESC, n DESC) = 1;
