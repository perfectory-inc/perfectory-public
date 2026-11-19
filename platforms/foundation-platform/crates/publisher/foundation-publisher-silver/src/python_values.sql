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
