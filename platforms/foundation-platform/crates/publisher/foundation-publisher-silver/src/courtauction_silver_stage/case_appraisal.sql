-- A 감정평가 object's document rows (courtauction_go_kr.case_answer_rows, role `case_appraisal`), after
-- case_lines.sql: per case and answered round, the appraisal row of the last line whose answer holds
-- it. The 감정평가사 (`aeeEvlExamrNm`, `aeeEvlJdgrNm`) are never read; the opinion is masked
-- afterwards (`Role::masked`).
CREATE OR REPLACE TEMP TABLE ca_court_auction_appraisal AS
SELECT court_office_code, case_no, CAST(answered_round AS INTEGER) AS round_seq,
       py_text(json_extract(info, '$.aeeWevlNo')) AS appraisal_no,
       onbid_ymd(json_extract(info, '$.dspslPrcCrtrYmd')) AS price_basis_date,
       onbid_ymd(json_extract(info, '$.exmnYmd')) AS surveyed_date,
       onbid_ymd(json_extract(info, '$.wrtYmd')) AS written_date,
       py_text(json_extract(info, '$.auctnLstDvsCd')) AS list_kind_code,
       py_text(json_extract(info, '$.fstmEvlDcsnOponCtt')) AS opinion,
       py_text(json_extract(info, '$.cortOfcNm')) AS court_name,
       py_text(json_extract(info, '$.userCsNo')) AS user_case_no,
       py_text(json_extract(info, '$.lwstDvsCd')) AS lawsuit_kind_code,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM (SELECT *, json_extract(data, '$.dma_ordTsIndvdAeeWevlInf') AS info FROM ca_case_lines)
WHERE answered AND answered_round IS NOT NULL AND json_type(info) = 'OBJECT' AND py_truthy(info)
QUALIFY row_number() OVER (PARTITION BY court_office_code, case_no, answered_round ORDER BY line_number DESC) = 1;
