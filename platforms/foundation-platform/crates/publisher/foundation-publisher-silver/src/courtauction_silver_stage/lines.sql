-- A line read as the job read it: its number, the JSON, whether it is a read (`read_outcome`), and
-- its body as `json.loads(line.get("body") or "")` read it (None when it is not JSON), parsed once
-- here: a body parsed again in every expression that reads it ran a 20,219-line object out of the
-- stage's memory (2026-11-21).
CREATE OR REPLACE TEMP TABLE ca_parsed AS
SELECT line_number, j,
       coalesce(json_extract_string(j, '$.read_outcome') = 'read', false) AS read,
       TRY_CAST(CASE WHEN py_truthy(json_extract(j, '$.body')) THEN json_extract_string(j, '$.body') END AS JSON) AS body
FROM (SELECT line_number, CAST(line AS JSON) AS j FROM ca_lines);
