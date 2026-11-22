-- A 현황조사서 object's document rows (courtauction_go_kr.case_answer_rows, role `case_survey_report`),
-- after case_lines.sql: per case and answered round, the 임대차 list of the last line that answered
-- with a round (an empty list included). The lessee's name (`intrpsNm`) and resident number field
-- (`enrrno`) are never read; free text is masked afterwards (`Role::masked`).
CREATE OR REPLACE TEMP TABLE ca_court_auction_survey_tenant AS
WITH chosen AS (
  SELECT * FROM ca_case_lines
  WHERE answered AND answered_round IS NOT NULL
  QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, answered_round ORDER BY line_number DESC) = 1
), listed AS (
  SELECT court_office_code, case_no, answered_round, line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT *, ca_objects(json_extract(data, '$.dlt_ordTsLserLtn')) AS l FROM chosen)
)
SELECT court_office_code, case_no, CAST(answered_round AS INTEGER) AS round_seq, CAST(n AS INTEGER) AS tenant_seq,
       CAST(ca_whole(json_extract(item, '$.objctSeq')) AS INTEGER) AS object_seq,
       py_text(json_extract(item, '$.auctnIntrpsDvsCd')) AS party_kind_code,
       py_text(json_extract(item, '$.btprtPrsnlDvsCd')) AS person_kind_code,
       py_text(json_extract(item, '$.auctnLesUsgCd')) AS usage_code,
       py_text(json_extract(item, '$.lesUsgDts')) AS usage_raw,
       py_text(json_extract(item, '$.lesDposDts')) AS deposit_raw,
       py_text(json_extract(item, '$.mmrntAmtDts')) AS monthly_rent_raw,
       py_text(json_extract(item, '$.mvinDtlCtt')) AS move_in_raw,
       py_text(json_extract(item, '$.rgstryCrtcpCfmtnCtt')) AS fixed_date_raw,
       py_text(json_extract(item, '$.lesPartCtt')) AS leased_part_raw,
       py_text(json_extract(item, '$.gdsPossCtt')) AS possession_text,
       py_text(json_extract(item, '$.lesDtsRmk')) AS remarks,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM listed;
