-- The first statement of searches.sql before the typed read (2026-11-25): the search lines from
-- ca_parsed (lines.sql of the same date). Kept only as the oracle of the typed read's equality test.
CREATE OR REPLACE TEMP TABLE ca_search_lines AS
WITH answers AS (
  SELECT line_number, json_extract(TRY_CAST(json_extract_string(j, '$.body') AS JSON), '$.data.dlt_srchResult') AS list
  FROM ca_parsed
  -- `line.get("http_status") != 200`: the number 200 (a text "200" is no answer).
  WHERE json_type(json_extract(j, '$.http_status')) IN ('BIGINT', 'UBIGINT', 'DOUBLE')
    AND TRY_CAST(json_extract(j, '$.http_status') AS DOUBLE) = 200
), listed AS (
  SELECT line_number, unnest(l) AS item, generate_subscripts(l, 1) AS n
  FROM (SELECT line_number, CASE WHEN json_type(list) = 'ARRAY' THEN json_extract(list, '$[*]') ELSE [] END AS l
        FROM answers)
)
SELECT line_number, n, item,
       trim(coalesce(py_str(json_extract(item, '$.boCd')), 'None'), py_ws()) AS court_office_code,
       trim(coalesce(py_str(json_extract(item, '$.saNo')), 'None'), py_ws()) AS case_no,
       ca_whole(json_extract(item, '$.maemulSer')) AS property_seq,
       ca_whole(json_extract(item, '$.mokmulSer')) AS item_seq
FROM listed;
