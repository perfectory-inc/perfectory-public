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

-- case_rows: the case row the 사건 기본 states (물건상세 `csBaseInfo`, 사건상세 `dma_csBasInf`), with
-- the latest 배당요구종기 of its list; None for a base that is no object or empty. The 사법보좌관
-- (`jdgeAojAsstnNm`) is never read.
CREATE OR REPLACE MACRO ca_case_row(base, deadline_list) AS
  CASE WHEN json_type(base) = 'OBJECT' AND py_truthy(base) THEN struct_pack(
    court_name := py_text(json_extract(base, '$.cortOfcNm')),
    court_branch_name := py_text(json_extract(base, '$.cortSptNm')),
    user_case_no := py_text(json_extract(base, '$.userCsNo')),
    case_name := py_text(json_extract(base, '$.csNm')),
    case_received_date := onbid_ymd(json_extract(base, '$.csRcptYmd')),
    commencement_date := onbid_ymd(json_extract(base, '$.csCmdcYmd')),
    claim_amount_krw := ca_whole(json_extract(base, '$.clmAmt')),
    department_code := py_text(json_extract(base, '$.jdbnCd')),
    department_name := py_text(json_extract(base, '$.cortAuctnJdbnNm')),
    department_phone := py_text(json_extract(base, '$.jdbnTelno')),
    execution_officer_phone := py_text(json_extract(base, '$.execrCsTelno')),
    case_progress_status_code := py_text(json_extract(base, '$.csProgStatCd')),
    suspension_status_code := py_text(json_extract(base, '$.auctnSuspStatCd')),
    closure_kind_code := py_text(json_extract(base, '$.ultmtDvsCd')),
    closed_date := onbid_ymd(json_extract(base, '$.csUltmtYmd')),
    duplicate_merge_kind_code := py_text(json_extract(base, '$.auctnDpcnMrgDvsCd')),
    appeal_yn := py_text(json_extract(base, '$.rletApalYn')),
    lawsuit_kind_code := py_text(json_extract(base, '$.lwstDvsCd')),
    dividend_demand_deadline := list_max(list_transform(ca_objects(deadline_list),
                                                        lambda x: onbid_ymd(json_extract(x, '$.dstrtDemnLstprdYmd'))))
  ) END;

-- `(text_or_none(value) or "").split()`: the words of a value, split on Python's whitespace.
CREATE OR REPLACE MACRO ca_words(v) AS
  list_filter(regexp_split_to_array(coalesce(py_text(v), ''), '[' || py_ws() || ']+'), lambda w: w <> '');

-- filing_parts' words before its action: the last word is the action when it is one of what happens
-- to a 문건·송달 document.
CREATE OR REPLACE MACRO ca_filing_action(words) AS
  CASE WHEN len(words) > 0 AND words[-1] IN ('제출', '발송', '접수', '송달', '교부') THEN words[-1] END;
CREATE OR REPLACE MACRO ca_filing_words(words) AS
  CASE WHEN ca_filing_action(words) IS NOT NULL THEN words[1:len(words) - 1] ELSE words END;

-- filing_parts: a 문건·송달 text's role (the words before the first masked one, the Latin O the site
-- masks a name with), document kind (the words after the last masked one, less the action) and
-- action. The masked words are the name and are never kept; with none masked only the action is.
CREATE OR REPLACE MACRO ca_filing_parts(v) AS (
  SELECT struct_pack(
    role := CASE WHEN first > 0 THEN nullif(array_to_string(w[1:first - 1], ' '), '') END,
    kind := CASE WHEN first > 0 THEN nullif(array_to_string(w[last + 1:len(w)], ' '), '') END,
    action := action)
  FROM (SELECT w, action, list_position(flags, true) AS first,
               len(flags) - list_position(list_reverse(flags), true) + 1 AS last
        FROM (SELECT w, action, list_transform(w, lambda x: contains(x, 'O')) AS flags
              FROM (SELECT ca_filing_words(ca_words(v)) AS w, ca_filing_action(ca_words(v)) AS action))));

-- reached_parts: `lastDlvrblRchYmd` as the site gives it ("2026.01.30 도달"): the date and the result;
-- no date and the whole text when it does not start with one. `\s` is Python's (`py_ws`).
CREATE OR REPLACE MACRO ca_reached_parts(v) AS (
  SELECT CASE WHEN regexp_matches(t, re) THEN struct_pack(
           reached := TRY_CAST(onbid_ascii_digits(regexp_extract(t, re, 1) || '-' || regexp_extract(t, re, 2) || '-'
                                                  || regexp_extract(t, re, 3)) AS DATE),
           result := nullif(trim(regexp_extract(t, re, 4), py_ws()), ''))
         ELSE struct_pack(reached := NULL::DATE, result := py_text(v)) END
  FROM (SELECT coalesce(py_text(v), '') AS t,
               '^[' || py_ws() || ']*(\p{Nd}{4})[.\-](\p{Nd}{2})[.\-](\p{Nd}{2})[' || py_ws() || ']*(.*)$' AS re));

-- decimal: `float(str(value).strip())`, None when Python refuses it (None and True/False are "None"
-- and "True"): digits with `_` between them, a fraction and an exponent, or inf/infinity/nan with a
-- sign. Full-width digits are read as Python reads them.
CREATE OR REPLACE MACRO ca_decimal(v) AS (
  SELECT CASE
    WHEN t IS NULL THEN NULL
    WHEN regexp_full_match(t, '[+-]?(([0-9](_?[0-9])*)(\.([0-9](_?[0-9])*)?)?|\.[0-9](_?[0-9])*)([eE][+-]?[0-9](_?[0-9])*)?')
      THEN TRY_CAST(replace(t, '_', '') AS DOUBLE)
    WHEN regexp_full_match(lower(t), '[+-]?(inf|infinity)')
      THEN CASE WHEN starts_with(t, '-') THEN '-inf'::DOUBLE ELSE 'inf'::DOUBLE END
    WHEN regexp_full_match(lower(t), '[+-]?nan') THEN 'nan'::DOUBLE
  END
  FROM (SELECT CASE WHEN json_type(v) IN ('VARCHAR', 'BIGINT', 'UBIGINT', 'DOUBLE', 'HUGEINT')
                    THEN onbid_ascii_digits(trim(py_str(v), py_ws())) END AS t));
