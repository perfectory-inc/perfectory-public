-- One export's rows with the 법정동 their address names and the source PNU of their lot (root ADR-0349
-- §5, ADR-0469): the rules of `legal_dong_code`, `lot_of` and `candidate_pnu` in
-- `rt_molit_real_transaction.py`.
--
-- {rows}: the export's rows (the Silver columns the export fills). `address_codes` (`_address`, a
-- 법정동's full name with its whitespace made one space; `source_legal_dong`, its code in the newest
-- code snapshot; `legal_dong`, that code carried to the current one, NULL for a split).
--
-- {out} adds `pnu` (the SOURCE-code PNU of an unmasked lot, which `resolve.sql` carries to the
-- current parcel), `rt_current_code` (the row's `legal_dong_code`) and `rt_reason`: why the row names
-- no lot (`no_address`, `legal_dong_not_found`, then the lot's own: `jibun_masked`, `block_lot` and
-- `provisional_lot` for a development zone's block and a readjustment's provisional number, which
-- are no 지번, `no_jibun`, `unparsed`).
CREATE OR REPLACE TEMP TABLE {out} AS
WITH coded AS (
  SELECT r.*, c.source_legal_dong AS _source_code, c.legal_dong AS rt_current_code,
         rt_address_key(r.address_raw) AS _address,
         trim(coalesce(r.jibun_raw, '')) AS _raw,
         trim(coalesce(r.bonbun, '')) AS _bon,
         trim(coalesce(r.bubun, '')) AS _bu
  FROM {rows} r
  LEFT JOIN address_codes c ON c._address = rt_address_key(r.address_raw)
),
read AS (
  SELECT *,
         regexp_matches(_bon, '^\d+$') AND coalesce(TRY_CAST(_bon AS BIGINT), 0) > 0 AS _from_columns,
         regexp_full_match(_raw, '(산\s*)?(\d{1,4})(?:\s*-\s*(\d{1,4}))?') AS _lot_text
  FROM coded
),
lots AS (
  SELECT *,
         CASE WHEN contains(_raw, '*') THEN 'jibun_masked'
              WHEN regexp_matches(_raw, 'BL|블록') THEN 'block_lot'
              WHEN regexp_matches(_raw, '^가\s*-') THEN 'provisional_lot'
              WHEN _from_columns THEN 'ok'
              WHEN _raw = '' THEN 'no_jibun'
              WHEN _lot_text THEN 'ok'
              ELSE 'unparsed' END AS _lot,
         CASE WHEN _from_columns THEN starts_with(_raw, '산')
              ELSE regexp_extract(_raw, '^(산\s*)?(\d{1,4})(?:\s*-\s*(\d{1,4}))?$', 1) <> '' END AS _mountain,
         CASE WHEN _from_columns THEN TRY_CAST(_bon AS BIGINT)
              ELSE TRY_CAST(regexp_extract(_raw, '^(산\s*)?(\d{1,4})(?:\s*-\s*(\d{1,4}))?$', 2) AS BIGINT) END AS _main,
         CASE WHEN _from_columns
              THEN CASE WHEN regexp_matches(_bu, '^\d+$') THEN TRY_CAST(_bu AS BIGINT) ELSE 0 END
              ELSE coalesce(TRY_CAST(nullif(regexp_extract(_raw, '^(산\s*)?(\d{1,4})(?:\s*-\s*(\d{1,4}))?$', 3), '')
                                     AS BIGINT), 0) END AS _sub
  FROM read
)
SELECT * EXCLUDE (_source_code, _address, _raw, _bon, _bu, _from_columns, _lot_text, _lot, _mountain, _main, _sub),
       CASE WHEN _lot = 'ok' AND _source_code IS NOT NULL AND _main BETWEEN 1 AND 9999 AND _sub <= 9999
            THEN concat(_source_code, CASE WHEN _mountain THEN '2' ELSE '1' END,
                        lpad(CAST(_main AS VARCHAR), 4, '0'), lpad(CAST(_sub AS VARCHAR), 4, '0'))
       END AS pnu,
       CASE WHEN _address = '' THEN 'no_address'
            WHEN _source_code IS NULL THEN 'legal_dong_not_found'
            WHEN _lot <> 'ok' THEN _lot END AS rt_reason
FROM lots;
