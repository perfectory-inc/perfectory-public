-- A 물건상세 object's rows (courtauction_go_kr.detail_rows_of and document_rows, root ADR-0384,
-- ADR-0540 §4): one detail row per line (its key read back from the request; a line without one
-- refuses the object, the caller checks `ca_detail_lines`); from each read line whose answer is an
-- object, the case and spec rows (the last line stating a key winning), the lists of the property
-- (schedule, objects, annexes) and of the case (감정평가 요점) of the last such line (an empty list
-- included), and one photo row per stored photo of a case, the last line naming it winning. The
-- fields that name a person (`dspslStkCtt`, `jdgeAojAsstnNm`) are never read; free text is masked
-- afterwards (`Role::masked`).
-- Parameters: {request_object}, {result_path}, {photo_list}, {photo_kind}, {photo_sequence},
-- {photo_page}, {photo_title}, {object_code}, {object_key}, {snapshot_id},
-- {ingested_at_epoch_seconds}.
CREATE OR REPLACE TEMP TABLE ca_detail_lines AS
SELECT line_number, j,
       CASE WHEN py_truthy(json_extract(asked, '$.cortOfcCd')) THEN py_str(json_extract(asked, '$.cortOfcCd')) END
         AS court_office_code,
       CASE WHEN py_truthy(json_extract(asked, '$.csNo')) THEN py_str(json_extract(asked, '$.csNo')) END AS case_no,
       ca_whole(json_extract(asked, '$.dspslGdsSeq')) AS property_seq,
       CASE WHEN read AND json_type(json_extract(ca_body(j), '{result_path}')) = 'OBJECT'
            THEN json_extract(ca_body(j), '{result_path}') END AS result
FROM (SELECT line_number, j, read,
             coalesce(CASE WHEN py_truthy(json_extract(j, '$.request'))
                           THEN json_extract(j, '$.request."{request_object}"') END, '{}'::JSON) AS asked
      FROM ca_parsed);

CREATE OR REPLACE TEMP TABLE ca_court_auction_item_detail AS
SELECT court_office_code, case_no, CAST(property_seq AS INTEGER) AS property_seq,
       coalesce(notice_utc(json_extract(j, '$.requested_at_utc')), to_timestamp({ingested_at_epoch_seconds}))
         AS requested_at_utc,
       CAST(ca_whole(json_extract(j, '$.http_status')) AS INTEGER) AS http_status,
       CASE WHEN py_truthy(json_extract(j, '$.read_outcome')) THEN py_str(json_extract(j, '$.read_outcome'))
            ELSE 'read' END AS read_outcome,
       py_text(json_extract(j, '$.list_fingerprint')) AS list_fingerprint,
       CAST(ca_whole(json_extract(j, '$.photo_count')) AS INTEGER) AS photo_count,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM ca_detail_lines;

CREATE OR REPLACE TEMP TABLE ca_detail_answers AS
SELECT * FROM ca_detail_lines WHERE result IS NOT NULL;

CREATE OR REPLACE TEMP TABLE ca_court_auction_item_photo AS
WITH listed AS (
  SELECT court_office_code, case_no, line_number, unnest(l) AS photo, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(result, '$."{photo_list}"')) AS l FROM ca_detail_answers)
)
SELECT coalesce(py_text(json_extract(photo, '$.cortOfcCd')), court_office_code) AS court_office_code,
       coalesce(py_text(json_extract(photo, '$.csNo')), case_no) AS case_no,
       py_text(json_extract(photo, '$.sha256')) AS sha256,
       py_text(json_extract(photo, '$."{photo_kind}"')) AS photo_kind_code,
       CAST(ca_whole(json_extract(photo, '$."{photo_sequence}"')) AS INTEGER) AS sequence,
       CAST(ca_whole(json_extract(photo, '$."{photo_page}"')) AS INTEGER) AS page_sequence,
       py_text(json_extract(photo, '$."{photo_title}"')) AS title,
       py_str(json_extract(photo, '$.media_format')) AS media_format,
       ca_whole(json_extract(photo, '$.size_bytes')) AS size_bytes,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed
WHERE py_text(json_extract(photo, '$.sha256')) IS NOT NULL AND py_text(json_extract(photo, '$.media_format')) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY coalesce(py_text(json_extract(photo, '$.cortOfcCd')), court_office_code),
                                        coalesce(py_text(json_extract(photo, '$.csNo')), case_no),
                                        py_text(json_extract(photo, '$.sha256'))
                           ORDER BY line_number DESC, n DESC) = 1;

CREATE OR REPLACE TEMP TABLE ca_court_auction_case AS
SELECT court_office_code, case_no, unnest(c),
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM (SELECT court_office_code, case_no, line_number,
             ca_case_row(json_extract(result, '$.csBaseInfo'), json_extract(result, '$.dstrtDemnInfo')) AS c
      FROM ca_detail_answers)
WHERE c IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no ORDER BY line_number DESC) = 1;

CREATE OR REPLACE TEMP TABLE ca_court_auction_item_spec AS
SELECT court_office_code, case_no, CAST(property_seq AS INTEGER) AS property_seq,
       py_text(json_extract(spec, '$.auctnGdsUsgCd')) AS item_usage_code,
       py_text(json_extract(spec, '$.auctnGdsStatCd')) AS item_status_code,
       py_text(json_extract(spec, '$.auctnDxdyGdsStatCd')) AS schedule_item_status_code,
       py_text(json_extract(spec, '$.realMulKind')) AS real_item_kind,
       ca_whole(json_extract(spec, '$.aeeEvlAmt')) AS appraisal_amount_krw,
       ca_whole(json_extract(spec, '$.fstPbancLwsDspslPrc')) AS minimum_bid_price_krw,
       CAST(ca_whole(json_extract(spec, '$.flbdNcnt')) AS INTEGER) AS failed_bid_count,
       py_text(json_extract(spec, '$.bidDvsCd')) AS bid_kind_code,
       onbid_ymd(json_extract(spec, '$.dspslDxdyYmd')) AS sale_date,
       py_text(json_extract(spec, '$.fstDspslHm')) AS sale_time_raw,
       py_text(json_extract(spec, '$.dspslPlcNm')) AS sale_place,
       onbid_ymd(json_extract(spec, '$.bidBgngYmd')) AS bid_start_date,
       onbid_ymd(json_extract(spec, '$.bidEndYmd')) AS bid_end_date,
       onbid_ymd(json_extract(spec, '$.dspslDcsnDxdyYmd')) AS sale_decision_date,
       py_text(json_extract(spec, '$.dspslDcsnHm')) AS sale_decision_time_raw,
       py_text(json_extract(spec, '$.dspslDcsnPlcNm')) AS sale_decision_place,
       CAST(ca_whole(json_extract(spec, '$.prchDposRate')) AS INTEGER) AS deposit_rate_percent,
       onbid_ymd(json_extract(spec, '$.gdsSpcfcWrtYmd')) AS spec_written_date,
       py_text(json_extract(spec, '$.ndstrcRghCtt')) AS assumed_rights,
       py_text(json_extract(spec, '$.sprfcExstcDts')) AS superficies,
       py_text(json_extract(spec, '$.gdsSpcfcRmk')) AS spec_remarks,
       py_text(json_extract(spec, '$.tprtyRnkHypthcStngDts')) AS first_rank_setting,
       py_text(json_extract(spec, '$.dspslGdsRmk')) AS item_remarks,
       py_text(json_extract(spec, '$.dspslGdsSpcfcEcdocId')) AS spec_document_id,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM (SELECT *, json_extract(result, '$.dspslGdsDxdyInfo') AS spec FROM ca_detail_answers)
WHERE json_type(spec) = 'OBJECT' AND py_truthy(spec)
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, property_seq ORDER BY line_number DESC) = 1;

-- The line each property's lists come from, and the line each case's 요점 come from.
CREATE OR REPLACE TEMP TABLE ca_detail_property AS
SELECT * FROM ca_detail_answers
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, property_seq ORDER BY line_number DESC) = 1;
CREATE OR REPLACE TEMP TABLE ca_detail_case AS
SELECT * FROM ca_detail_answers
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no ORDER BY line_number DESC) = 1;

CREATE OR REPLACE TEMP TABLE ca_court_auction_item_schedule AS
WITH listed AS (
  SELECT court_office_code, case_no, property_seq, line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(result, '$.gdsDspslDxdyLst')) AS l FROM ca_detail_property)
)
SELECT court_office_code, case_no, CAST(property_seq AS INTEGER) AS property_seq, CAST(n AS INTEGER) AS schedule_seq,
       onbid_ymd(json_extract(item, '$.dxdyYmd')) AS schedule_date,
       py_text(json_extract(item, '$.dxdyHm')) AS schedule_time_raw,
       py_text(json_extract(item, '$.auctnDxdyKndCd')) AS schedule_kind_code,
       py_text(json_extract(item, '$.dxdyPlcNm')) AS schedule_place,
       py_text(json_extract(item, '$.auctnDxdyRsltCd')) AS result_code,
       py_text(json_extract(item, '$.auctnDxdyGdsStatCd')) AS schedule_item_status_code,
       nullif(ca_whole(json_extract(item, '$.tsLwsDspslPrc')), 0) AS minimum_bid_price_krw,
       nullif(ca_whole(json_extract(item, '$.dspslAmt')), 0) AS sale_amount_krw,
       onbid_ymd(json_extract(item, '$.bidBgngYmd')) AS bid_start_date,
       onbid_ymd(json_extract(item, '$.bidEndYmd')) AS bid_end_date,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed;

CREATE OR REPLACE TEMP TABLE ca_court_auction_item_object AS
WITH listed AS (
  SELECT court_office_code, case_no, property_seq, line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(result, '$.gdsDspslObjctLst')) AS l FROM ca_detail_property)
)
SELECT court_office_code, case_no, CAST(property_seq AS INTEGER) AS property_seq,
       CAST(ca_whole(json_extract(item, '$.dspslObjctSeq')) AS INTEGER) AS object_seq,
       py_text(json_extract(item, '$.auctnLstDvsCd')) AS list_kind_code,
       py_text(json_extract(item, '$.rletDvsDts')) AS realty_kind,
       py_text(json_extract(item, '$.pjbBuldList')) AS building_structure,
       py_text(json_extract(item, '$.objctArDts')) AS area_raw,
       py_text(json_extract(item, '$.ldcgDts')) AS land_category,
       ca_whole(json_extract(item, '$.aeeEvlAmt')) AS appraisal_amount_krw,
       py_text(json_extract(item, '$.lclDspslGdsLstUsgCd')) AS usage_lcls_code,
       py_text(json_extract(item, '$.mclDspslGdsLstUsgCd')) AS usage_mcls_code,
       py_text(json_extract(item, '$.sclDspslGdsLstUsgCd')) AS usage_scls_code,
       {object_code} AS legal_dong_code,
       py_text(json_extract(item, '$.rprsLtnoAddr')) AS lot_raw,
       py_text(json_extract(item, '$.pnuNoCtt')) AS source_pnu_raw,
       py_text(json_extract(item, '$.bldNm')) AS building_name,
       py_text(json_extract(item, '$.bldDtlDts')) AS building_unit_raw,
       py_text(json_extract(item, '$.userPrintSt')) AS print_address_raw,
       ca_decimal(json_extract(item, '$.stXcrd')) AS x_raw,
       ca_decimal(json_extract(item, '$.stYcrd')) AS y_raw,
       nullif(ca_decimal(json_extract(item, '$.dspslStkNmrtVal')), 0) AS share_numerator,
       nullif(ca_decimal(json_extract(item, '$.dspslStkDnmnVal')), 0) AS share_denominator,
       py_text(json_extract(item, '$.rgstRdngYn')) AS register_read_yn,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed
WHERE ca_whole(json_extract(item, '$.dspslObjctSeq')) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, property_seq, object_seq ORDER BY n) = 1;

CREATE OR REPLACE TEMP TABLE ca_court_auction_item_annex AS
WITH groups AS (
  SELECT court_office_code, case_no, property_seq, line_number, unnest(g) AS grp, generate_subscripts(g, 1) AS gn
  FROM (SELECT *, CASE WHEN json_type(json_extract(result, '$.gdsNotSugtBldLsstAll')) = 'ARRAY'
                       THEN json_extract(json_extract(result, '$.gdsNotSugtBldLsstAll'), '$[*]') ELSE [] END AS g
        FROM ca_detail_property)
), listed AS (
  SELECT court_office_code, case_no, property_seq, line_number, gn, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(grp) AS l FROM groups)
)
SELECT court_office_code, case_no, CAST(property_seq AS INTEGER) AS property_seq,
       CAST(ca_whole(json_extract(item, '$.dspslObjctSeq')) AS INTEGER) AS object_seq,
       CAST(row_number() OVER (PARTITION BY court_office_code, case_no, property_seq,
                                            ca_whole(json_extract(item, '$.dspslObjctSeq'))
                               ORDER BY gn, n) AS INTEGER) AS annex_seq,
       py_text(json_extract(item, '$.etcUsgCtt')) AS usage_raw,
       py_text(json_extract(item, '$.bldStrcDts')) AS structure_raw,
       py_text(json_extract(item, '$.bldArDts')) AS area_raw,
       ca_whole(json_extract(item, '$.evlAmt')) AS appraisal_amount_krw,
       py_text(json_extract(item, '$.sugtBsdsBldRmk')) AS remarks,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed
WHERE ca_whole(json_extract(item, '$.dspslObjctSeq')) IS NOT NULL;

CREATE OR REPLACE TEMP TABLE ca_court_auction_appraisal_point AS
WITH listed AS (
  SELECT court_office_code, case_no, line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(result, '$.aeeWevlMnpntLst')) AS l FROM ca_detail_case)
)
SELECT court_office_code, case_no, CAST(n AS INTEGER) AS point_seq,
       py_text(json_extract(item, '$.aeeWevlMnpntTbltDvsCd')) AS table_code,
       py_text(json_extract(item, '$.aeeWevlMnpntItmCd')) AS item_code,
       CAST(ca_whole(json_extract(item, '$.aeeWevlMnpntDtlSeq')) AS INTEGER) AS detail_seq,
       py_text(json_extract(item, '$.aeeWevlMnpntCtt')) AS content,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed;
