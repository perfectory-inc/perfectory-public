-- What courtauction_go_kr.py did with a value json.loads gave it, as DuckDB macros over JSON (root
-- ADR-0559), for the values the shared readers (python_values.sql, which runs first) do not cover.

-- whole: `str(value or "").strip().replace(",", "")` as an int when it is digits (an optional
-- minus); None otherwise. A falsy value (0, "", false) is None, and a float's text ("5.0") is no
-- whole number.
CREATE OR REPLACE MACRO ca_whole(v) AS
  CASE WHEN regexp_full_match(onbid_ascii_digits(replace(onbid_or_blank(v), ',', '')), '-?[0-9]+')
       THEN TRY_CAST(onbid_ascii_digits(replace(onbid_or_blank(v), ',', '')) AS BIGINT) END;

-- `(json.loads(line.get("body") or "") or {}).get("data")`, kept only when it is an object: the
-- answer's data of a read line, None for any other body.
CREATE OR REPLACE MACRO ca_body(line) AS
  TRY_CAST(CASE WHEN py_truthy(json_extract(line, '$.body')) THEN json_extract_string(line, '$.body') END AS JSON);
CREATE OR REPLACE MACRO ca_data(line) AS
  CASE WHEN json_type(ca_body(line)) = 'OBJECT' AND json_type(json_extract(ca_body(line), '$.data')) = 'OBJECT'
       THEN json_extract(ca_body(line), '$.data') END;
