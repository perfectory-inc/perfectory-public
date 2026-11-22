-- One vintage's unit prices (root ADR-0095, ADR-0182, ADR-0317 §6). It was the Spark job
-- `unit_official_price.py` (DICTIONARY_SQL, PRICES_SQL, JOIN_SQL); each step reads its rows the way
-- that job did:
--
-- - `trim` is Spark's `trim`: spaces only, and a null stays null.
-- - `regexp_matches` with `^…$` is Spark's `REGEXP` with the same anchors.
-- - A cast the validity test has not cleared is `TRY_CAST`: Spark's non-ANSI cast is null where
--   `DuckDB`'s would fail, and `DuckDB` does not promise to test the conjuncts in order.
-- - A (management key, reference date)'s correction notices resolve to the newest notice, then the
--   higher price (Spark's `ROW_NUMBER` over `notice_date DESC, price_won DESC`: nulls last).
--
-- Input (`unit_official_price_stage::load`): price_source(mgmt_key, base_date, notice_date,
-- price_won), the vintage's rows of the price table at the batch's price pin; exclusive_source
-- (mgmt_key, pnu, dong_name, ho_name), the exclusive units as of the vintage at the batch's unit pin.
-- Parameters: @batch@ (the batch's source_snapshot_id), @vintage@, and @scope@, a predicate on the
-- dictionary's `pnu` (`TRUE` for the nation, one 시도's range for a verdict). A key names one unit
-- (the caller refuses the dictionary otherwise), so the 시도's rows are the nation's rows of that 시도.
-- Output: unit_dictionary, unit_reference_prices, and unit_prices with the columns of
-- silver.unit_official_price in its order.

CREATE OR REPLACE TEMP TABLE unit_dictionary AS
SELECT DISTINCT mgmt_key, pnu, coalesce(dong_name, '') AS dong_name, coalesce(ho_name, '') AS ho_name
FROM exclusive_source
WHERE mgmt_key IS NOT NULL AND trim(mgmt_key) <> '';

CREATE OR REPLACE TEMP TABLE unit_reference_prices AS
SELECT mgmt_key, base_date, price_won
FROM (
  SELECT mgmt_key, base_date, price_won,
         row_number() OVER (PARTITION BY mgmt_key, base_date
                            ORDER BY notice_date DESC NULLS LAST, price_won DESC NULLS LAST) AS notice_rank
  FROM (
    SELECT mgmt_key, trim(base_date) AS base_date, trim(notice_date) AS notice_date,
           TRY_CAST(trim(price_won) AS BIGINT) AS price_won
    FROM price_source
    WHERE mgmt_key IS NOT NULL AND trim(mgmt_key) <> ''
      AND regexp_matches(trim(base_date), '^[0-9]{8}$')
      AND TRY_CAST(substr(trim(base_date), 1, 4) AS INTEGER) BETWEEN 1000 AND 9999
      AND regexp_matches(trim(price_won), '^[0-9]{1,18}$')
      -- Only the keys the scope's units hold: the rest join to nothing in it.
      AND mgmt_key IN (SELECT mgmt_key FROM unit_dictionary WHERE @scope@)
  ) normalized
) ranked
WHERE notice_rank = 1;

CREATE OR REPLACE TEMP TABLE unit_prices AS
SELECT d.mgmt_key AS mgm_bldrgst_pk, d.pnu, d.dong_name, d.ho_name, p.base_date, p.price_won,
       substr(d.pnu, 1, 2) AS sido,
       '@batch@' AS source_snapshot_id,
       '@vintage@' AS vintage,
       -- One load identity per 시도, as the per-province runs wrote them.
       'unit-official-price/base-date-v2/' || substr(d.pnu, 1, 2) || '/@batch@' AS source_record_id
FROM unit_reference_prices p
JOIN unit_dictionary d ON p.mgmt_key = d.mgmt_key
WHERE regexp_matches(d.pnu, '^[0-9]{19}$') AND @scope@;
