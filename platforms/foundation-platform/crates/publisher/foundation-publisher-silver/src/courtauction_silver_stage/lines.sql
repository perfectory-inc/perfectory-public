-- A line read as the job read it: its number, the JSON, whether it is a read (`read_outcome`), and
-- its body as `json.loads(line.get("body") or "")` read it (None when it is not JSON), parsed once
-- here: a body parsed again in every expression that reads it ran a 20,219-line object out of the
-- stage's memory (2026-11-21).
-- The line, then its body's text, each into a table before the body is parsed: in one statement the
-- CASE over the whole line held a 2,055-line 물건상세 object's parse above 1 GiB (2026-11-22).
CREATE OR REPLACE TEMP TABLE ca_lines_json AS
SELECT line_number, CAST(line AS JSON) AS j FROM ca_lines;
CREATE OR REPLACE TEMP TABLE ca_bodies AS
SELECT line_number, py_truthy(json_extract(j, '$.body')) AS truthy, json_extract_string(j, '$.body') AS body_text
FROM ca_lines_json;
CREATE OR REPLACE TEMP TABLE ca_parsed AS
SELECT line_number, j,
       coalesce(json_extract_string(j, '$.read_outcome') = 'read', false) AS read,
       TRY_CAST(CASE WHEN truthy THEN body_text END AS JSON) AS body
FROM ca_lines_json JOIN ca_bodies USING (line_number);
DROP TABLE ca_bodies;
DROP TABLE ca_lines_json;
