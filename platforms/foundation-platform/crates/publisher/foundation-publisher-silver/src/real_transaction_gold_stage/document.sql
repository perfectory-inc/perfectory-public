-- The 실거래 by-PNU kind's Gold rows (root ADR-0350 §2, ADR-0405, ADR-0479, ADR-0483): the rules of
-- `gold_rows` and `with_place_ids` in `real_transaction_silver_to_gold.py` and
-- `place_id_join.with_place_ids`, written so `real_transaction_json` and `row_digest` are byte for byte
-- what that job writes.
--
-- Inputs: `placed` (real_transaction_placement_stage: every deal, its PNU or NULL), `parcels(pnu)`,
-- `land(pnu, land_category, …)`, `region` (region_stats.sql's rows), `place_ids(pnu, place_id)`.
-- The stage fills: the build month's first day, COMPARABLE_MONTHS and REGION_MONTHS before it, the
-- month after it, COMPARABLE_COUNT, the document bound less its envelope, `history_coverage` as JSON,
-- and a deal's and a comparable's JSON object over `placed`'s columns.
--
-- The JSON is Spark's `to_json(…, ignoreNullFields=false)` as text, field by field: every key, a missing
-- value null, a double as Java writes it (`spark_double_text`). `DuckDB`'s own `to_json` writes a
-- string as Jackson does and is used for strings and string lists only. Spark's `length` is a count of
-- characters, as `length` is.
CREATE OR REPLACE TABLE gold_rows AS
WITH own AS (
  SELECT pnu, row_id, deal_date, dataset_slug, main_lot_group_key, {transaction} AS _entry
  FROM placed WHERE pnu IS NOT NULL
),
-- Comparables: one row per deal (the lowest PNU it is placed on, if any), newest first.
by_deal AS (
  SELECT * FROM placed QUALIFY row_number() OVER (PARTITION BY row_id ORDER BY pnu ASC NULLS LAST) = 1
),
ranked AS (
  SELECT *, row_number() OVER (PARTITION BY legal_dong ORDER BY deal_date DESC, row_id) AS _n
  FROM by_deal
  WHERE dataset_slug = 'real_transaction_industrial_trade' AND legal_dong IS NOT NULL
    AND NOT coalesce(cancelled, false)
    AND deal_date >= DATE '{comparables_from}' AND deal_date < DATE '{next_month}'
),
comparables AS (
  -- A 본번 group deal is one comparable: its group's key and size, no one PNU (root ADR-0405).
  SELECT legal_dong, '[' || string_agg({comparable}, ',' ORDER BY _n) || ']' AS comparables
  FROM ranked WHERE _n <= {comparable_count} GROUP BY legal_dong
),
-- Region: the 법정동's 공장창고 and 토지 rows of the trailing 12 months, m² prices in 만원.
region_doc AS (
  SELECT region_code AS legal_dong,
         '[' || string_agg('{"dataset_slug":' || to_json(dataset_slug)::VARCHAR
           || ',"deal_count":' || deal_count || ',"cancelled_count":' || cancelled_count
           || ',"land_price_per_m2_10k_krw":' || CASE WHEN land_price_per_m2_median IS NOT NULL
                THEN '{"p25":' || spark_double_text(land_price_per_m2_p25)
                  || ',"median":' || spark_double_text(land_price_per_m2_median)
                  || ',"p75":' || spark_double_text(land_price_per_m2_p75) || '}' ELSE 'null' END
           || ',"building_price_per_m2_10k_krw":' || CASE WHEN dataset_slug = 'real_transaction_industrial_trade'
                  AND building_price_per_m2_median IS NOT NULL
                THEN '{"p25":' || spark_double_text(building_price_per_m2_p25)
                  || ',"median":' || spark_double_text(building_price_per_m2_median)
                  || ',"p75":' || spark_double_text(building_price_per_m2_p75) || '}' ELSE 'null' END
           || '}', ',' ORDER BY dataset_slug) || ']' AS datasets
  FROM region
  WHERE period_kind = 'trailing_12_months' AND region_level = 'legal_dong'
    AND dataset_slug IN ('real_transaction_industrial_trade', 'real_transaction_land_trade')
  GROUP BY region_code
),
-- Which PNUs carry comparables: 지목 공장용지·창고용지, or a 공장창고 deal of their own.
eligible AS (
  SELECT DISTINCT e.pnu FROM (
    SELECT pnu FROM land WHERE land_category IN ('공장용지', '창고용지')
    UNION ALL
    SELECT pnu FROM own WHERE dataset_slug = 'real_transaction_industrial_trade'
  ) e SEMI JOIN parcels p ON p.pnu = e.pnu
),
-- Past the document bound the oldest deals are left out: a deal is kept while the characters of
-- the newest ones up to it fit.
cumulative AS (
  SELECT *, sum(length(_entry) + 1) OVER (PARTITION BY pnu ORDER BY deal_date DESC NULLS LAST, row_id
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _cum
  FROM own
),
listed AS (
  SELECT pnu, count(*)::BIGINT AS transaction_total,
         list(struct_pack(_cum := _cum, _e := _entry) ORDER BY _cum) AS _all
  FROM cumulative GROUP BY pnu
),
-- The 본번 group a PNU's group deals name: its own group's current parcels, written once.
keys AS (SELECT DISTINCT pnu, main_lot_group_key FROM own WHERE main_lot_group_key IS NOT NULL),
groups AS (
  SELECT substr(pnu, 1, 15) AS main_lot_group_key, list_sort(list(DISTINCT pnu)) AS _group
  FROM parcels
  WHERE substr(pnu, 1, 15) IN (SELECT main_lot_group_key FROM keys)
  GROUP BY 1
),
group_of AS (SELECT k.pnu, g._group FROM keys k JOIN groups g USING (main_lot_group_key)),
pnus AS (SELECT pnu FROM listed UNION SELECT pnu FROM eligible),
joined AS (
  SELECT p.pnu, l.transaction_total, l._all, coalesce(g._group, []::VARCHAR[]) AS _group,
         CASE WHEN e.pnu IS NOT NULL THEN c.comparables END AS comparables, r.datasets
  FROM pnus p
  LEFT JOIN listed l USING (pnu)
  LEFT JOIN group_of g USING (pnu)
  LEFT JOIN comparables c ON c.legal_dong = substr(p.pnu, 1, 10)
  LEFT JOIN region_doc r ON r.legal_dong = substr(p.pnu, 1, 10)
  LEFT JOIN eligible e USING (pnu)
  WHERE l._all IS NOT NULL OR (e.pnu IS NOT NULL AND c.comparables IS NOT NULL)
),
kept AS (
  SELECT *, list_transform(list_filter(coalesce(_all, []), lambda x: x._cum <= {budget}
              - length('{"comparables":' || coalesce(comparables, 'null') || ',"datasets":'
                       || coalesce(datasets, 'null') || ',"g":' || to_json(_group)::VARCHAR || '}')),
            lambda x: x._e) AS _kept
  FROM joined
),
documents AS (
  SELECT pnu, _group,
         '{"as_of_month":"{build_month}","transactions":[' || array_to_string(_kept, ',')
           || '],"transactions_omitted":' || (len(coalesce(_all, [])) - len(_kept))
           || ',"main_lot_group_pnus":' || to_json(_group)::VARCHAR
           || ',"comparables":' || coalesce(comparables, '[]')
           || ',"comparables_from":"{comparables_from}","region":{"legal_dong_code":"' || substr(pnu, 1, 10)
           || '","from":"{region_from}","datasets":' || coalesce(datasets, '[]')
           || '},"coverage":{coverage_json}}' AS real_transaction_json,
         coalesce(transaction_total, 0)::BIGINT AS transaction_total
  FROM kept
),
members AS (
  SELECT d.pnu, to_json(list_sort(list(DISTINCT i.place_id)))::VARCHAR AS main_lot_group_place_ids
  FROM (SELECT pnu, unnest(_group) AS _member FROM documents) d
  JOIN place_ids i ON i.pnu = d._member
  WHERE i.place_id IS NOT NULL
  GROUP BY d.pnu
)
SELECT d.pnu, d.real_transaction_json, d.transaction_total, CAST(NULL AS VARCHAR) AS attached_via,
       i.place_id, m.main_lot_group_place_ids,
       -- Spark's default to_json of the content columns: a null field left out.
       sha256('{"pnu":' || to_json(d.pnu)::VARCHAR
              || ',"real_transaction_json":' || to_json(d.real_transaction_json)::VARCHAR
              || ',"transaction_total":' || d.transaction_total
              || coalesce(',"place_id":' || to_json(i.place_id)::VARCHAR, '')
              || coalesce(',"main_lot_group_place_ids":' || to_json(m.main_lot_group_place_ids)::VARCHAR, '')
              || '}') AS row_digest
FROM documents d
LEFT JOIN place_ids i USING (pnu)
LEFT JOIN members m USING (pnu);
