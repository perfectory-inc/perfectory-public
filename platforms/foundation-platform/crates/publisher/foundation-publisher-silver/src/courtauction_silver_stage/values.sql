-- What courtauction_go_kr.py did with a value json.loads gave it, as DuckDB macros over JSON (root
-- ADR-0559), for the values the shared readers (python_values.sql, which runs first) do not cover.

-- whole: `str(value or "").strip().replace(",", "")` as an int when it is digits (an optional
-- minus); None otherwise. A falsy value (0, "", false) is None, and a float's text ("5.0") is no
-- whole number.
CREATE OR REPLACE MACRO ca_whole(v) AS
  CASE WHEN regexp_full_match(onbid_ascii_digits(replace(onbid_or_blank(v), ',', '')), '-?[0-9]+')
       THEN TRY_CAST(onbid_ascii_digits(replace(onbid_or_blank(v), ',', '')) AS BIGINT) END;

-- `(json.loads(line.get("body") or "") or {}).get("data")` of a line's parsed body (`ca_parsed.body`),
-- kept only when it is an object: the answer's data of a read line, None for any other body.
CREATE OR REPLACE MACRO ca_data(body) AS
  CASE WHEN json_type(body) = 'OBJECT' AND json_type(json_extract(body, '$.data')) = 'OBJECT'
       THEN json_extract(body, '$.data') END;

-- `_list(value)`'s objects as a DuckDB list of JSON, in order; empty for any other value.
CREATE OR REPLACE MACRO ca_objects(v) AS
  CASE WHEN json_type(v) = 'ARRAY'
       THEN list_filter(json_extract(v, '$[*]'), lambda x: json_type(x) = 'OBJECT') ELSE [] END;

-- `v.get(path) if isinstance(v.get(path), Mapping) else {}`: the object at `path`, `{}` for any other value.
CREATE OR REPLACE MACRO ca_object(v, path) AS
  CASE WHEN json_type(json_extract(v, path)) = 'OBJECT' THEN json_extract(v, path) ELSE '{}'::JSON END;

-- _plain: a notice's HTML text as plain text: each `<br>` a line, other tags dropped, each line's
-- spaces collapsed, blank lines left out; None when nothing is left. `\s` is Python's (`py_ws`).
CREATE OR REPLACE MACRO ca_plain(v) AS
  CASE WHEN py_text(v) IS NOT NULL THEN nullif(trim(array_to_string(list_filter(list_transform(
    regexp_split_to_array(py_text(v), '(?i)</?br[' || py_ws() || ']*/?>|\r?\n'),
    lambda p: trim(regexp_replace(regexp_replace(p, '<[^>]+>', ' ', 'g'), '[' || py_ws() || ']+', ' ', 'g'), py_ws())),
    lambda p: p <> ''), chr(10)), py_ws()), '') END;

-- _amount: the first amount a notice's price text gives ("<img ..> 11,000,000<br>입찰시간(10:00)").
-- shortcut: a digit of another script than ASCII or full width (Python's int() reads any `\d`) is
-- None here; none was seen in the site's amounts, upgrade if a verdict finds one.
CREATE OR REPLACE MACRO ca_amount(v) AS
  TRY_CAST(onbid_ascii_digits(replace(nullif(regexp_extract(
    regexp_replace(coalesce(py_text(v), ''), '<[^>]+>', ' ', 'g'), '(\p{Nd}[\p{Nd},]*)', 1), ''), ',', '')) AS BIGINT);
