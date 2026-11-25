-- A 물건 검색 or 매각결과 검색 object's rows (courtauction_go_kr.rows_of and item_row): one row per 물건
-- (court office, case, property and item sequence), the last line that holds it winning, the later
-- answer when the list shifted between pages. Only lines answered with HTTP 200 whose body is JSON
-- with a `data.dlt_srchResult` list. A row without its key refuses the object (the caller checks
-- `ca_search_lines`). The PNU is not placed here: `place_rows` hands each row's source code PNU and
-- its two addresses to the one place-identity resolver (place_identity_stage).
-- Parameters: {object_path}, {code_fields} (`code_of(item, FROM_CODE)` of `item`), {lot_field}, {object_key},
-- {snapshot_id}, {ingested_at_epoch_seconds}.
-- The file is read here, not through lines.sql (root ADR-0616 §3-1): a search answers up to 165 KiB a line,
-- and its body is parsed once by json_transform straight into the list of 물건 (each a JSON value), so no
-- statement holds a whole line or a whole body. Line numbers as lines.sql numbers them.
CREATE OR REPLACE TEMP TABLE ca_search_lines AS
WITH answers AS (
  SELECT ordinality AS line_number,
         CASE WHEN json_valid(json_extract_string(body, '$'))
              THEN json_transform(json_extract_string(body, '$'), '{"data": {"dlt_srchResult": ["JSON"]}}').data.dlt_srchResult
         END AS l
  FROM read_json('{object_path}', format = 'newline_delimited', records = true, maximum_object_size = 33554432,
                 columns = {http_status: 'JSON', body: 'JSON'})
       WITH ORDINALITY
  -- `line.get("http_status") != 200`: the number 200 (a text "200" is no answer).
  WHERE json_type(http_status) IN ('BIGINT', 'UBIGINT', 'DOUBLE')
    AND TRY_CAST(http_status AS DOUBLE) = 200
), listed AS (
  SELECT line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT line_number, coalesce(l, []) AS l FROM answers)
)
SELECT line_number, n, item,
       trim(coalesce(py_str(json_extract(item, '$.boCd')), 'None'), py_ws()) AS court_office_code,
       trim(coalesce(py_str(json_extract(item, '$.saNo')), 'None'), py_ws()) AS case_no,
       ca_whole(json_extract(item, '$.maemulSer')) AS property_seq,
       ca_whole(json_extract(item, '$.mokmulSer')) AS item_seq
FROM listed;

CREATE OR REPLACE TEMP TABLE ca_search_rows AS
SELECT court_office_code, case_no, CAST(property_seq AS INTEGER) AS property_seq, CAST(item_seq AS INTEGER) AS item_seq,
       py_text(json_extract(item, '$.srnSaNo')) AS user_case_no,
       py_text(json_extract(item, '$.jiwonNm')) AS court_name,
       py_text(json_extract(item, '$.jpDeptNm')) AS department_name,
       ca_whole(json_extract(item, '$.gamevalAmt')) AS appraisal_amount_krw,
       ca_whole(json_extract(item, '$.minmaePrice')) AS minimum_bid_price_krw,
       onbid_ymd(json_extract(item, '$.maeGiil')) AS sale_date,
       py_text(json_extract(item, '$.maeHh1')) AS sale_time_raw,
       onbid_ymd(json_extract(item, '$.maegyuljGiil')) AS sale_decision_date,
       py_text(json_extract(item, '$.maePlace')) AS sale_place,
       CAST(ca_whole(json_extract(item, '$.yuchalCnt')) AS INTEGER) AS failed_bid_count,
       nullif(ca_whole(json_extract(item, '$.maeAmt')), 0) AS sale_amount_krw,
       py_text(json_extract(item, '$.jinstatCd')) AS case_status_code,
       py_text(json_extract(item, '$.mulStatcd')) AS item_status_code,
       py_text(json_extract(item, '$.mulJinYn')) AS item_in_progress_yn,
       py_text(json_extract(item, '$.lclsUtilCd')) AS usage_lcls_code,
       py_text(json_extract(item, '$.mclsUtilCd')) AS usage_mcls_code,
       py_text(json_extract(item, '$.sclsUtilCd')) AS usage_scls_code,
       py_text(json_extract(item, '$.hjguSido')) AS address_sido_name,
       py_text(json_extract(item, '$.hjguSigu')) AS address_sigungu_name,
       py_text(json_extract(item, '$.hjguDong')) AS address_dong_name,
       py_text(json_extract(item, '$.hjguRd')) AS address_ri_name,
       py_text(json_extract(item, '$.daepyoLotno')) AS representative_lot_raw,
       py_text(json_extract(item, '$.rdNm')) AS road_name,
       py_text(json_extract(item, '$.buldNo')) AS building_no_raw,
       py_text(json_extract(item, '$.buldNm')) AS building_name,
       py_text(json_extract(item, '$.printSt')) AS print_address_raw,
       ca_decimal(json_extract(item, '$.wgs84Xcordi')) AS wgs84_x,
       ca_decimal(json_extract(item, '$.wgs84Ycordi')) AS wgs84_y,
       -- code_lot_pnu: the 10-digit code and the lot (산 -> ledger 2) as a 19-digit PNU, or none; a lot
       -- over 9999 names none.
       (SELECT CASE WHEN {code_fields} IS NOT NULL AND x.main <> ''
                     AND place_int(x.main) <= 9999 AND coalesce(place_int(nullif(x.sub, '')), 0) <= 9999
                    THEN {code_fields} || CASE WHEN x.mountain = '산' THEN '2' ELSE '1' END
                         || lpad(CAST(place_int(x.main) AS VARCHAR), 4, '0')
                         || lpad(CAST(coalesce(place_int(nullif(x.sub, '')), 0) AS VARCHAR), 4, '0') END
        FROM (SELECT unnest(regexp_extract(
                regexp_replace(coalesce(py_text(json_extract(item, '$.{lot_field}')), ''), '[' || py_ws() || ']+', '', 'g'),
                place_lot_re(), ['mountain', 'main', 'sub']))) x) AS source_code_pnu_raw,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM ca_search_lines
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, property_seq, item_seq
                           ORDER BY line_number DESC, n DESC) = 1;
