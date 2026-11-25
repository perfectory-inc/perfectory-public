-- The requests of one 차세대 온비드 Bronze object and the items of their answers (root ADR-0280). It
-- was `data_go_kr_onbid.answer_items` and `item_pass_complete`: what that Python read from each line.
--
-- Input: onbid_lines(line_number BIGINT, line VARCHAR): the object's non-blank lines, numbered from 1.
-- Three parts, split by `-- step:` lines: `init` once, `line` once per part of the object's lines
-- (duckdb_parquet_stage::read_line_parts hands it a part at a time in onbid_lines), `pass` once at
-- the end. A line holds a page of up to a thousand items (1.5 MB) and DuckDB keeps every parse of a
-- chunk until the chunk ends: a whole object in one chunk outgrew 2 GB; a part holds at most the
-- stage's memory limit divided by the contract's silver_load.memory_per_line_byte.
-- Parameters: {ok_code}, {no_data_code} (the source contract's result codes), {combos} (a VALUES list
-- of every (prptDivCd, pvctTrgtYn) an item list pass asks).
-- A 200 whose body is not JSON is not an answer (2026-11-17: the provider sent its "503 - Service
-- Unavailable" HTML page with a 200, and the object stopped every hourly load): it is read as a
-- request that was not answered, like any other status, and counted (`unreadable`).
-- Output: onbid_answers (one row per line), onbid_items (one row per item of a 00 answer, in order),
-- onbid_pass (one row: whether an item list object read every combination to its end).

-- step: init
CREATE OR REPLACE TEMP TABLE onbid_answers (line_number BIGINT, request JSON, http_ok BOOLEAN, code JSON,
                                            items BIGINT, total_count JSON, unreadable BOOLEAN);
CREATE OR REPLACE TEMP TABLE onbid_items (line_number BIGINT, ordinal BIGINT, item JSON);

-- step: line
-- The line's request, whether it is a 200 with a JSON body, and (for one) the body's parts the load reads.
CREATE OR REPLACE TEMP TABLE onbid_line_parts AS
SELECT line_number, request, is_200 AND is_json AS http_ok, is_200 AND NOT is_json AS unreadable, body
FROM (
  SELECT line_number,
         CASE WHEN py_truthy(parts[1]) THEN parts[1] ELSE '{}'::JSON END AS request,
         -- `line.get("http_status") != 200`: a number equal to 200, whatever its JSON form.
         coalesce(json_type(parts[2]) IN ('UBIGINT', 'BIGINT', 'DOUBLE') AND TRY_CAST(parts[2] AS DOUBLE) = 200, false) AS is_200,
         coalesce(json_valid(json_extract_string(parts[3], '$')), false) AS is_json,
         parts[3] AS body
  FROM (SELECT line_number, json_extract(line, ['$.request', '$.http_status', '$.body']) AS parts
        FROM onbid_lines)
);

CREATE OR REPLACE TEMP TABLE onbid_body_parts AS
SELECT line_number, request, http_ok, unreadable,
       CASE WHEN http_ok THEN json_extract(json_extract_string(body, '$'), ['$.header', '$.result', '$.body.items.item', '$.body.totalCount']) END AS parts
FROM onbid_line_parts;

-- An error body carries its code under `result`, as the collector reads it. answer_items: the items
-- of a 00 answer; [] for an empty (03) or quota answer.
CREATE OR REPLACE TEMP TABLE onbid_line_items AS
SELECT line_number, request, http_ok, unreadable, code, total_count,
       CASE WHEN http_ok AND json_type(code) = 'VARCHAR' AND json_extract_string(code, '$') = '{ok_code}' THEN
         CASE json_type(item) WHEN 'OBJECT' THEN [item] WHEN 'ARRAY' THEN json_extract(item, '$[*]') END
       END AS items
FROM (
  SELECT line_number, request, http_ok, unreadable,
         CASE WHEN json_type(parts[1]) = 'OBJECT' THEN json_extract(parts[1], '$.resultCode')
              WHEN py_truthy(parts[2]) THEN json_extract(parts[2], '$.resultCode') END AS code,
         -- body.items.item, read only when body.items is an object (json_extract reaches into nothing else).
         parts[3] AS item,
         parts[4] AS total_count
  FROM onbid_body_parts
);

INSERT INTO onbid_answers
SELECT line_number, request, http_ok, code, coalesce(len(items), 0), total_count, unreadable FROM onbid_line_items;

INSERT INTO onbid_items
SELECT l.line_number, i.ordinal, i.item
FROM onbid_line_items l,
LATERAL (SELECT unnest(range(CAST(len(l.items) AS BIGINT))) AS ordinal, unnest(l.items) AS item) i
WHERE l.items IS NOT NULL;

-- step: pass
-- item_pass_complete: a combination ends at a 03 answer, at an empty page, or once its pages hold
-- totalCount items; a combination the run never reached has no such line.
CREATE OR REPLACE TEMP TABLE onbid_pass AS
WITH asked AS (
  SELECT a.line_number,
         CASE WHEN coalesce(json_type(json_extract(request, '$.prptDivCd')), 'NULL') = 'NULL' THEN 'None' ELSE py_str(json_extract(request, '$.prptDivCd')) END AS division,
         CASE WHEN coalesce(json_type(json_extract(request, '$.pvctTrgtYn')), 'NULL') = 'NULL' THEN 'None' ELSE py_str(json_extract(request, '$.pvctTrgtYn')) END AS private_flag,
         CASE WHEN json_type(code) = 'VARCHAR' THEN json_extract_string(code, '$') END AS code,
         items,
         onbid_whole(total_count) AS total
  FROM onbid_answers a
  WHERE http_ok
), counted AS (
  SELECT *, CASE WHEN code = '{ok_code}'
                 THEN sum(items) OVER (PARTITION BY division, private_flag, code = '{ok_code}' ORDER BY line_number
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) END AS so_far
  FROM asked
), ended AS (
  SELECT DISTINCT division, private_flag FROM counted
  WHERE code = '{no_data_code}'
     OR (code = '{ok_code}' AND (items = 0 OR (total IS NOT NULL AND so_far >= total)))
)
SELECT NOT EXISTS (
  SELECT 1 FROM (VALUES {combos}) AS c(division, private_flag)
  WHERE NOT EXISTS (SELECT 1 FROM ended e WHERE e.division = c.division AND e.private_flag = c.private_flag)
) AS complete;
