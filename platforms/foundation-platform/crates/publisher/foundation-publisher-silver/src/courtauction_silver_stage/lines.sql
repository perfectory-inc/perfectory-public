-- A line read as the job read it: its number, the JSON, and whether it is a read (`read_outcome`).
CREATE OR REPLACE TEMP TABLE ca_parsed AS
SELECT line_number, CAST(line AS JSON) AS j,
       coalesce(json_extract_string(CAST(line AS JSON), '$.read_outcome') = 'read', false) AS read
FROM ca_lines;
