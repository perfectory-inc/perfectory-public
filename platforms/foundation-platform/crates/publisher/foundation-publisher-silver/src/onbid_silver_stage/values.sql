-- The readers `data_go_kr_onbid.READERS` and `valid_pnu` applied to each provider field, as DuckDB
-- macros over the JSON value (root ADR-0559). They follow what that Python did, quirks included, so
-- the rows a Spark load wrote are the rows this writes (python_values.sql runs first).
-- Parameter: {pnu_dialect} (`WHEN '<provider digit>' THEN '<standard digit>'`, one per entry of the
-- source contract's pnu.provider_ledger_kind_digits), {ledger_kind_digits} (a list literal).

-- Full-width digits as ASCII ones, as Python's int() and float() read them.
CREATE OR REPLACE MACRO onbid_ascii_digits(t) AS translate(t, '０１２３４５６７８９', '0123456789');

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

-- str(value or "").strip(): Python's text of a truthy value, "" for a falsy one.
CREATE OR REPLACE MACRO onbid_or_blank(v) AS
  CASE WHEN py_truthy(v) THEN trim(py_str(v), py_ws()) ELSE '' END;

-- `datetime.strptime(text, fmt)` as CPython's _strptime reads it: the format's regex matched from the
-- start with its alternatives tried in order (RE2's leftmost-first matching is the same), and the
-- text refused when anything is left after the match or the fields are no date. So a 12-digit
-- `yyyyMMddHHmm` read with the seconds format first takes its minute's second digit as the second:
-- 202611190930 is 09:03:00. That is what every Spark load wrote; the stage writes the same.
CREATE OR REPLACE MACRO onbid_seconds_re() AS '^([0-9]{4})(1[0-2]|0[1-9]|[1-9])(3[01]|[12][0-9]|0[1-9]|[1-9]| [1-9])(2[0-3]|[0-1][0-9]|[0-9])([0-5][0-9]|[0-9])(6[0-1]|[0-5][0-9]|[0-9])';
CREATE OR REPLACE MACRO onbid_minutes_re() AS '^([0-9]{4})(1[0-2]|0[1-9]|[1-9])(3[01]|[12][0-9]|0[1-9]|[1-9]| [1-9])(2[0-3]|[0-1][0-9]|[0-9])([0-5][0-9]|[0-9])';
CREATE OR REPLACE MACRO onbid_date_re() AS '^([0-9]{4})(1[0-2]|0[1-9]|[1-9])(3[01]|[12][0-9]|0[1-9]|[1-9]| [1-9])';

-- The date of a match's first three groups (year, month, day), or None when they name no date.
CREATE OR REPLACE MACRO onbid_match_date(t, re) AS
  TRY_CAST(regexp_extract(t, re, 1) || '-' || lpad(trim(regexp_extract(t, re, 2)), 2, '0') || '-'
           || lpad(trim(regexp_extract(t, re, 3)), 2, '0') AS DATE);

CREATE OR REPLACE MACRO onbid_strptime_seconds(t) AS
  CASE WHEN t <> '' AND regexp_extract(t, onbid_seconds_re(), 0) = t
            AND TRY_CAST(regexp_extract(t, onbid_seconds_re(), 6) AS INTEGER) <= 59 THEN
    onbid_match_date(t, onbid_seconds_re())
      + to_hours(TRY_CAST(regexp_extract(t, onbid_seconds_re(), 4) AS BIGINT))
      + to_minutes(TRY_CAST(regexp_extract(t, onbid_seconds_re(), 5) AS BIGINT))
      + to_seconds(TRY_CAST(regexp_extract(t, onbid_seconds_re(), 6) AS BIGINT))
  END;

CREATE OR REPLACE MACRO onbid_strptime_minutes(t) AS
  CASE WHEN t <> '' AND regexp_extract(t, onbid_minutes_re(), 0) = t THEN
    onbid_match_date(t, onbid_minutes_re())
      + to_hours(TRY_CAST(regexp_extract(t, onbid_minutes_re(), 4) AS BIGINT))
      + to_minutes(TRY_CAST(regexp_extract(t, onbid_minutes_re(), 5) AS BIGINT))
  END;

-- kst_minute: `yyyyMMddHHmm` (or with seconds) in Korean time, the instant it names; None when it is
-- not one. The instant, not a wall clock, so the stage's time zone does not enter it.
CREATE OR REPLACE MACRO onbid_kst(v) AS
  to_timestamp(epoch(coalesce(onbid_strptime_seconds(onbid_or_blank(v)), onbid_strptime_minutes(onbid_or_blank(v)))) - 32400);

-- ymd: `yyyyMMdd` as a date; None when it is not one.
CREATE OR REPLACE MACRO onbid_ymd(v) AS
  CASE WHEN onbid_or_blank(v) <> '' AND regexp_extract(onbid_or_blank(v), onbid_date_re(), 0) = onbid_or_blank(v)
       THEN onbid_match_date(onbid_or_blank(v), onbid_date_re()) END;

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
