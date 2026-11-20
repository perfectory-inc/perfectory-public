-- The readers `data_go_kr_onbid.READERS` and `valid_pnu` applied to each provider field, as DuckDB
-- macros over the JSON value (root ADR-0559). They follow what that Python did, quirks included, so
-- the rows a Spark load wrote are the rows this writes (python_values.sql runs first).
-- Parameter: {pnu_dialect} (`WHEN '<provider digit>' THEN '<standard digit>'`, one per entry of the
-- source contract's pnu.provider_ledger_kind_digits), {ledger_kind_digits} (a list literal).

-- whole: a whole number from a number or a string of digits (commas allowed); None for anything else.
CREATE OR REPLACE MACRO onbid_whole(v) AS
  CASE
    WHEN v IS NULL THEN NULL
    WHEN json_type(v) IN ('NULL', 'BOOLEAN', 'ARRAY', 'OBJECT') THEN NULL
    WHEN json_type(v) IN ('UBIGINT', 'BIGINT') THEN TRY_CAST(CAST(v AS VARCHAR) AS BIGINT)
    WHEN json_type(v) = 'DOUBLE' THEN
      CASE WHEN TRY_CAST(v AS DOUBLE) = floor(TRY_CAST(v AS DOUBLE)) THEN TRY_CAST(TRY_CAST(v AS DOUBLE) AS BIGINT) END
    -- Python's \d is any decimal digit; the full-width ones are the ones a Korean text holds.
    WHEN regexp_full_match(onbid_ascii_digits(replace(trim(py_str(v), py_ws()), ',', '')), '-?[0-9]+')
      THEN TRY_CAST(onbid_ascii_digits(replace(trim(py_str(v), py_ws()), ',', '')) AS BIGINT)
  END;

-- decimal: float(text without commas) or float(number); None for anything else.
CREATE OR REPLACE MACRO onbid_decimal(v) AS
  CASE
    WHEN v IS NULL THEN NULL
    WHEN json_type(v) IN ('NULL', 'BOOLEAN', 'ARRAY', 'OBJECT') THEN NULL
    WHEN json_type(v) = 'VARCHAR' THEN TRY_CAST(onbid_ascii_digits(trim(replace(json_extract_string(v, '$'), ',', ''), py_ws())) AS DOUBLE)
    ELSE TRY_CAST(v AS DOUBLE)
  END;

-- kst_minute: the provider's `yyyyMMddHHmm` (12 digits) or `yyyyMMddHHmmss` (14) in Korean time, the
-- instant it names; None when it is neither or names no time. The instant, not a wall clock, so the
-- stage's time zone does not enter it. The Spark load read it with `strptime` and the seconds format
-- first, whose regex took a 12-digit minute's second digit as the second (202611190930 -> 09:03:00):
-- 2026-11-19 the current Silver held 6,116 item rows ending at 17:05:09 for 17:59 and 3,270 bid
-- results opened at 11:03 for 11:30. Fixed width, each field two digits.
CREATE OR REPLACE MACRO onbid_kst_re() AS '^([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})?$';
CREATE OR REPLACE MACRO onbid_kst_text(t) AS
  CASE WHEN regexp_full_match(t, '[0-9]{12}|[0-9]{14}') THEN
    regexp_extract(t, onbid_kst_re(), 1) || '-' || regexp_extract(t, onbid_kst_re(), 2) || '-'
    || regexp_extract(t, onbid_kst_re(), 3) || ' ' || regexp_extract(t, onbid_kst_re(), 4) || ':'
    || regexp_extract(t, onbid_kst_re(), 5) || ':' || coalesce(nullif(regexp_extract(t, onbid_kst_re(), 6), ''), '00')
  END;
CREATE OR REPLACE MACRO onbid_kst(v) AS
  to_timestamp(epoch(TRY_CAST(onbid_kst_text(onbid_or_blank(v)) AS TIMESTAMP)) - 32400);

-- valid_pnu: the provider's ltnoPnu with its 11th digit read through the hub dialect, kept only when
-- it is what Pnu::parse accepts (19 ASCII digits, a standard 대장구분 digit); None otherwise.
CREATE OR REPLACE MACRO onbid_pnu_digit(t) AS CASE substr(t, 11, 1) {pnu_dialect} END;
CREATE OR REPLACE MACRO onbid_pnu(v) AS
  CASE WHEN regexp_full_match(onbid_or_blank(v), '[0-9]{19}')
            AND list_contains({ledger_kind_digits}, onbid_pnu_digit(onbid_or_blank(v)))
       THEN left(onbid_or_blank(v), 10) || onbid_pnu_digit(onbid_or_blank(v)) || substr(onbid_or_blank(v), 12) END;

-- json_text: a list (or object) as received, less the private keys, as compact sorted JSON; None
-- when it is None, "", [] or {}.
CREATE OR REPLACE MACRO onbid_json_text(v, drop) AS
  CASE
    WHEN v IS NULL OR json_type(v) = 'NULL' THEN NULL
    WHEN json_type(v) = 'VARCHAR' AND json_extract_string(v, '$') = '' THEN NULL
    WHEN json_type(v) = 'ARRAY' AND json_array_length(v) = 0 THEN NULL
    WHEN json_type(v) = 'OBJECT' AND len(json_keys(v)) = 0 THEN NULL
    ELSE py_json_dumps(v, drop)
  END;
