-- A 현황조사 object's document rows (courtauction_go_kr.case_answer_rows, role `case_survey`), after
-- case_lines.sql: per case and answered round, the survey row and the list of 부동산 objects (one per
-- object number, the first naming it) of the last line whose answer holds the survey. A line with no
-- answered round or no survey adds nothing. Free text is masked afterwards (`Role::masked`).
CREATE OR REPLACE TEMP VIEW ca_survey_answer AS
SELECT *, json_extract(data, '$.dma_ordTsCurstExmnMngInf') AS info
FROM ca_case_lines
WHERE answered AND answered_round IS NOT NULL
  AND json_type(json_extract(data, '$.dma_ordTsCurstExmnMngInf')) = 'OBJECT'
  AND py_truthy(json_extract(data, '$.dma_ordTsCurstExmnMngInf'))
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, answered_round ORDER BY line_number DESC) = 1;

CREATE OR REPLACE TEMP TABLE ca_court_auction_survey AS
SELECT court_office_code, case_no, CAST(answered_round AS INTEGER) AS round_seq,
       py_text(json_extract(info, '$.auctnCurstExmnTrsmStatCd')) AS transmit_status_code,
       py_text(json_extract(info, '$.execIntgCsNmCd')) AS case_name_code,
       py_text(json_extract(info, '$.exmnDtDts')) AS surveyed_raw,
       onbid_ymd(json_extract(info, '$.exmndcSndngYmd')) AS sent_date,
       onbid_ymd(json_extract(info, '$.exmndcRcptnYmd')) AS received_date,
       onbid_ymd(json_extract(info, '$.exmndcRtrcnYmd')) AS cancelled_date,
       py_text(json_extract(info, '$.lstPossRltnDts')) AS possession_summary,
       py_text(json_extract(info, '$.fstmLstPossRltnDts')) AS first_possession,
       py_text(json_extract(info, '$.scntmLstPossRltnDts')) AS second_possession,
       py_text(json_extract(info, '$.lesDts')) AS lease_text,
       py_text(json_extract(info, '$.lwstDvsCd')) AS lawsuit_kind_code,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM ca_survey_answer;

CREATE OR REPLACE TEMP TABLE ca_court_auction_survey_object AS
WITH listed AS (
  SELECT court_office_code, case_no, answered_round, line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(data, '$.dlt_ordTsCurstExmnRletLst')) AS l FROM ca_survey_answer)
)
SELECT court_office_code, case_no, CAST(answered_round AS INTEGER) AS round_seq,
       CAST(ca_whole(json_extract(item, '$.objctSeq')) AS INTEGER) AS object_seq,
       CAST(ca_whole(json_extract(item, '$.dspslObjctSeq')) AS INTEGER) AS sale_object_seq,
       py_text(json_extract(item, '$.auctnLstDvsCd')) AS list_kind_code,
       py_text(json_extract(item, '$.auctnPossRltnCd')) AS possession_code,
       py_text(json_extract(item, '$.gdsPossCtt')) AS possession_text,
       py_text(json_extract(item, '$.rletLstRmk')) AS remarks,
       CAST(ca_whole(json_extract(item, '$.lesCnt')) AS INTEGER) AS lease_count,
       py_text(json_extract(item, '$.ldcgDts')) AS land_category,
       py_text(json_extract(item, '$.objctArDts')) AS area_raw,
       py_text(json_extract(item, '$.rprsLtnoAddr')) AS lot_raw,
       py_text(json_extract(item, '$.bldNm')) AS building_name,
       py_text(json_extract(item, '$.bldDtlDts')) AS building_unit_raw,
       py_text(json_extract(item, '$.printSt')) AS print_address_raw,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed
WHERE ca_whole(json_extract(item, '$.objctSeq')) IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, answered_round, object_seq ORDER BY n) = 1;
