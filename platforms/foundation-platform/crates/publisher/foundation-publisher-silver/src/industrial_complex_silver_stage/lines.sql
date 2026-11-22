-- The lines of one export of `bronze.industrial_complexes_raw_jsonl` as JSON objects, and the two
-- readers `rows.sql` uses (root ADR-0559). Input: complex_lines(line_number BIGINT, line VARCHAR), the
-- export's non-blank lines. Output: complex_lines_json(line_number, j). A line that is not a JSON
-- object refuses the export, as Spark's reader did not read it.
--
-- `complex_ic_text_or_null` is Spark's `trim_to_null`.

CREATE OR REPLACE MACRO complex_ic_field(j, name) AS json_extract_string(j, '$."' || name || '"');

CREATE OR REPLACE MACRO complex_ic_text_or_null(v) AS nullif(trim(v), '');

CREATE OR REPLACE TEMP TABLE complex_lines_json AS
SELECT line_number,
       CASE WHEN json_type(TRY_CAST(line AS JSON)) = 'OBJECT' THEN TRY_CAST(line AS JSON)
            ELSE error('line ' || line_number || ' of the export is not a JSON object') END AS j
FROM complex_lines;
