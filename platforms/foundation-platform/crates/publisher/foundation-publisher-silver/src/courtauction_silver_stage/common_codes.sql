-- A 공통코드 object's rows (courtauction_go_kr.code_rows_of): one per (system, owner, group, code)
-- the answers of its read lines list at {result_pointer}, the last line (then the last item) naming
-- a code winning. A failed read, a body that is not JSON and an item without its system, group or
-- code give none.
-- Parameters: {result_pointer} (`$.data.sccdLst`), {object_key}, {snapshot_id},
-- {ingested_at_epoch_seconds}.
CREATE OR REPLACE TEMP TABLE ca_court_auction_code AS
WITH lists AS (
  SELECT line_number, json_extract(ca_body(j), '{result_pointer}') AS list
  FROM ca_parsed
  WHERE read AND json_type(ca_body(j)) = 'OBJECT' AND json_type(json_extract(ca_body(j), '{result_pointer}')) = 'ARRAY'
), listed AS (
  SELECT l.line_number, i.ordinal, i.item
  FROM lists l,
  LATERAL (SELECT unnest(range(CAST(json_array_length(l.list) AS BIGINT))) AS ordinal,
                 unnest(json_extract(l.list, '$[*]')) AS item) i
  WHERE json_type(i.item) = 'OBJECT'
), codes AS (
  SELECT line_number, ordinal,
         py_text(json_extract(item, '$.systmCd')) AS system_code,
         coalesce(py_text(json_extract(item, '$.intgCdOnrIdntId')), py_text(json_extract(item, '$.systmCd'))) AS owner_code,
         py_text(json_extract(item, '$.intgGrpCd')) AS group_code,
         py_text(json_extract(item, '$.intgCd')) AS code,
         py_text(json_extract(item, '$.intgCdNm')) AS name,
         py_text(json_extract(item, '$.intgCdOnrDvsCd')) AS owner_kind_code,
         py_text(json_extract(item, '$.intgInscrtMtjrCdLstCtt')) AS listed_codes_raw,
         py_text(json_extract(item, '$.fstmIntgCdAddCtt')) AS first_extra_raw,
         py_text(json_extract(item, '$.scntmIntgCdAddCtt')) AS second_extra_raw
  FROM listed
)
SELECT system_code, owner_code, group_code, code, name, owner_kind_code, listed_codes_raw,
       first_extra_raw, second_extra_raw,
       '{object_key}' AS source_record_id, CAST(line_number AS INTEGER) AS source_line_number,
       '{snapshot_id}' AS source_snapshot_id, to_timestamp({ingested_at_epoch_seconds}) AS ingested_at_utc
FROM codes
WHERE system_code IS NOT NULL AND group_code IS NOT NULL AND code IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY system_code, owner_code, group_code, code
                           ORDER BY line_number DESC, ordinal DESC) = 1;
