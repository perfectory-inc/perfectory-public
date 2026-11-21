-- The 실거래 deals as the placement reads them (root ADR-0351 §3): every deal's codes come from its
-- full address, never from the codes the Silver load wrote. The rules of `with_codes` and `lot_pnu` in
-- `real_transaction_silver_to_gold.py`.
--
-- `deals_raw`: the Silver's current rows, the job's DEAL_COLUMNS. `address_codes`: `_address` (a
-- 법정동's full name, whitespace made one space), `source_legal_dong` (its code in the newest code
-- snapshot), `legal_dong` (that code carried to the current one, or NULL for a split).
--
-- `pnu` is the SOURCE-code PNU of an unmasked lot (the 법정동 the address names, the 산 flag and the
-- 본번·부번 from the provider's columns, else from its 지번 text); `resolve.sql` carries it to the
-- current parcel. A masked lot (`3**`) or a lot out of range has none.
CREATE OR REPLACE TABLE deals AS
WITH coded AS (
  SELECT d.*, c.source_legal_dong, c.legal_dong,
         concat_ws('|', d.dataset_slug, d.deal_ym, d.row_key) AS row_id,
         trim(coalesce(d.jibun_raw, '')) AS _raw,
         trim(coalesce(d.bonbun, '')) AS _bon,
         trim(coalesce(d.bubun, '')) AS _bu
  FROM deals_raw d
  LEFT JOIN address_codes c
    ON c._address = trim(regexp_replace(coalesce(d.address_raw, ''), '\s+', ' ', 'g'))
),
lots AS (
  SELECT *,
         regexp_matches(_bon, '^\d+$') AND coalesce(TRY_CAST(_bon AS INTEGER), 0) > 0 AS _from_columns
  FROM coded
),
parts AS (
  SELECT *,
         CASE WHEN _from_columns THEN TRY_CAST(_bon AS INTEGER)
              ELSE TRY_CAST(regexp_extract(_raw, '^(산\s*)?(\d{1,4})(?:-(\d{1,4}))?$', 2) AS INTEGER) END AS _main,
         CASE WHEN _from_columns
              THEN CASE WHEN regexp_matches(_bu, '^\d+$') THEN TRY_CAST(_bu AS INTEGER) ELSE 0 END
              ELSE coalesce(TRY_CAST(nullif(regexp_extract(_raw, '^(산\s*)?(\d{1,4})(?:-(\d{1,4}))?$', 3), '')
                                     AS INTEGER), 0) END AS _sub
  FROM lots
)
SELECT * EXCLUDE (_raw, _bon, _bu, _from_columns, _main, _sub),
       CASE WHEN NOT contains(_raw, '*')
                 AND (_from_columns OR regexp_matches(_raw, '^(산\s*)?(\d{1,4})(?:-(\d{1,4}))?$'))
                 AND _main BETWEEN 1 AND 9999 AND _sub <= 9999 AND source_legal_dong IS NOT NULL
            THEN concat(source_legal_dong, CASE WHEN starts_with(_raw, '산') THEN '2' ELSE '1' END,
                        lpad(CAST(_main AS VARCHAR), 4, '0'), lpad(CAST(_sub AS VARCHAR), 4, '0'))
       END AS pnu
FROM parts;
