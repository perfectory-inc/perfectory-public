-- What the Python Silver loads did with a value `json.loads` gave them, as DuckDB macros over JSON
-- (root ADR-0559): the loads that leave Spark run these, so the rows a Spark load wrote are the rows
-- the publisher writes. Shared by every stage that reads a Bronze JSON object
-- (`industrial_complex_notice_stage`, `onbid_silver_stage`).

-- Python's str.isspace() characters, the ones str.strip() and str.split() use.
CREATE OR REPLACE MACRO py_ws() AS
  chr(9) || chr(10) || chr(11) || chr(12) || chr(13) || chr(28) || chr(29) || chr(30) || chr(31) || chr(32) || chr(133) || chr(160)
  || chr(5760) || chr(8192) || chr(8193) || chr(8194) || chr(8195) || chr(8196) || chr(8197) || chr(8198) || chr(8199)
  || chr(8200) || chr(8201) || chr(8202) || chr(8232) || chr(8233) || chr(8239) || chr(8287) || chr(12288);

-- Python's str() of a JSON value as `json.loads` gives it: a string as is, True/False, numbers.
CREATE OR REPLACE MACRO py_str(v) AS
  CASE json_type(v)
    WHEN 'VARCHAR' THEN json_extract_string(v, '$')
    WHEN 'BOOLEAN' THEN CASE WHEN TRY_CAST(v AS BOOLEAN) THEN 'True' ELSE 'False' END
    WHEN 'DOUBLE' THEN CAST(TRY_CAST(v AS DOUBLE) AS VARCHAR)
    ELSE CAST(v AS VARCHAR)
  END;

-- Python truthiness of a JSON value: null, "", 0, false, [] and {} are false.
CREATE OR REPLACE MACRO py_truthy(v) AS
  v IS NOT NULL AND CASE json_type(v)
    WHEN 'NULL' THEN false
    WHEN 'VARCHAR' THEN json_extract_string(v, '$') <> ''
    WHEN 'BOOLEAN' THEN TRY_CAST(v AS BOOLEAN)
    WHEN 'ARRAY' THEN json_array_length(v) > 0
    WHEN 'OBJECT' THEN len(json_keys(v)) > 0
    ELSE TRY_CAST(v AS DOUBLE) <> 0
  END;

-- text_or_none: the value as text, stripped; None when it is None or blank.
CREATE OR REPLACE MACRO py_text(v) AS
  CASE WHEN v IS NULL OR json_type(v) = 'NULL' THEN NULL ELSE nullif(trim(py_str(v), py_ws()), '') END;

-- A JSON value of json.loads, written again as `json.dumps(…, ensure_ascii=False, sort_keys=True,
-- separators=(",", ":"))` writes it: an object's keys in code point order (DuckDB's string order is
-- the UTF-8 byte order, the same), the keys in `drop` left out of every object, numbers and strings
-- as json.dumps writes them (to_json escapes as it does: \" \\ \n \r \t \b \f and \u00xx). Three
-- levels deep, as deep as the provider's lists go; a deeper value refuses the object rather than be
-- written another way.
CREATE OR REPLACE MACRO py_json_leaf(v) AS
  CASE coalesce(json_type(v), 'NULL')
    WHEN 'NULL' THEN 'null'
    WHEN 'BOOLEAN' THEN CASE WHEN TRY_CAST(v AS BOOLEAN) THEN 'true' ELSE 'false' END
    WHEN 'VARCHAR' THEN to_json(json_extract_string(v, '$'))::VARCHAR
    WHEN 'DOUBLE' THEN CAST(TRY_CAST(v AS DOUBLE) AS VARCHAR)
    WHEN 'ARRAY' THEN error('a JSON value nests deeper than a Silver list column is written')
    WHEN 'OBJECT' THEN error('a JSON value nests deeper than a Silver list column is written')
    ELSE CAST(v AS VARCHAR)
  END;

CREATE OR REPLACE MACRO py_json_level2(v, drop) AS
  CASE json_type(v)
    WHEN 'ARRAY' THEN '[' || array_to_string(list_transform(json_extract(v, '$[*]'), lambda e: py_json_leaf(e)), ',') || ']'
    WHEN 'OBJECT' THEN '{' || array_to_string(list_transform(
        list_filter(list_sort(map_entries(json_transform(v, '"MAP(VARCHAR, JSON)"'))), lambda e: NOT list_contains(drop, e.key)),
        lambda e: to_json(e.key)::VARCHAR || ':' || py_json_leaf(e.value)), ',') || '}'
    ELSE py_json_leaf(v)
  END;

CREATE OR REPLACE MACRO py_json_level1(v, drop) AS
  CASE json_type(v)
    WHEN 'ARRAY' THEN '[' || array_to_string(list_transform(json_extract(v, '$[*]'), lambda e: py_json_level2(e, drop)), ',') || ']'
    WHEN 'OBJECT' THEN '{' || array_to_string(list_transform(
        list_filter(list_sort(map_entries(json_transform(v, '"MAP(VARCHAR, JSON)"'))), lambda e: NOT list_contains(drop, e.key)),
        lambda e: to_json(e.key)::VARCHAR || ':' || py_json_level2(e.value, drop)), ',') || '}'
    ELSE py_json_leaf(v)
  END;

CREATE OR REPLACE MACRO py_json_dumps(v, drop) AS
  CASE json_type(v)
    WHEN 'ARRAY' THEN '[' || array_to_string(list_transform(json_extract(v, '$[*]'), lambda e: py_json_level1(e, drop)), ',') || ']'
    WHEN 'OBJECT' THEN '{' || array_to_string(list_transform(
        list_filter(list_sort(map_entries(json_transform(v, '"MAP(VARCHAR, JSON)"'))), lambda e: NOT list_contains(drop, e.key)),
        lambda e: to_json(e.key)::VARCHAR || ':' || py_json_level1(e.value, drop)), ',') || '}'
    ELSE py_json_leaf(v)
  END;

-- The readers below are shared by more than one stage; each keeps the name of the load that
-- needed it first (the onbid and ILIS notice row SQL calls them by these names).

-- Full-width digits as ASCII ones, as Python's int() and float() read them.
CREATE OR REPLACE MACRO onbid_ascii_digits(t) AS translate(t, '０１２３４５６７８９', '0123456789');

-- str(value or "").strip(): Python's text of a truthy value, "" for a falsy one.
CREATE OR REPLACE MACRO onbid_or_blank(v) AS
  CASE WHEN py_truthy(v) THEN trim(py_str(v), py_ws()) ELSE '' END;

-- `datetime.strptime(text, "%Y%m%d")` as CPython's _strptime reads it: the format's regex matched from
-- the start with its alternatives tried in order (RE2's leftmost-first matching is the same), and the
-- text refused when anything is left after the match or the fields are no date.
CREATE OR REPLACE MACRO onbid_date_re() AS '^([0-9]{4})(1[0-2]|0[1-9]|[1-9])(3[01]|[12][0-9]|0[1-9]|[1-9]| [1-9])';

-- The date of a match's first three groups (year, month, day), or None when they name no date.
CREATE OR REPLACE MACRO onbid_match_date(t, re) AS
  TRY_CAST(regexp_extract(t, re, 1) || '-' || lpad(trim(regexp_extract(t, re, 2)), 2, '0') || '-'
           || lpad(trim(regexp_extract(t, re, 3)), 2, '0') AS DATE);

-- ymd: `yyyyMMdd` as a date; None when it is not one.
CREATE OR REPLACE MACRO onbid_ymd(v) AS
  CASE WHEN onbid_or_blank(v) <> '' AND regexp_extract(onbid_or_blank(v), onbid_date_re(), 0) = onbid_or_blank(v)
       THEN onbid_match_date(onbid_or_blank(v), onbid_date_re()) END;

-- rfc3339_utc: an RFC 3339 time (fraction cut or padded to microseconds, no zone read as UTC).
CREATE OR REPLACE MACRO notice_utc(v) AS
  CASE WHEN py_truthy(v) THEN (
    SELECT CASE WHEN m.seconds <> '' THEN TRY_CAST(
             replace(m.seconds, ' ', 'T') || '.' || rpad(left(m.fraction, 6), 6, '0')
             || CASE WHEN m.zone IN ('', 'Z', 'z') THEN '+00:00' ELSE m.zone END AS TIMESTAMPTZ) END
    FROM (SELECT unnest(regexp_extract(trim(py_str(v), py_ws()),
            '^([0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]+))?(Z|z|[+-][0-9]{2}:[0-9]{2})?$',
            ['seconds', 'fraction', 'zone'])) ) m) END;
