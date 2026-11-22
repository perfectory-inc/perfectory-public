-- The 실거래 by-PNU kind's Gold rows (root ADR-0350 §2, ADR-0405, ADR-0479, ADR-0483): the rules of
-- `gold_rows` and `with_place_ids` in `real_transaction_silver_to_gold.py` and
-- `place_id_join.with_place_ids`, written so `real_transaction_json` and `row_digest` are byte for byte
-- what that job writes.
--
-- One 시군구's rows (`{chunk}`, the PNU's first five digits): every part of a document is keyed by
-- the PNU's 법정동, so a 시군구 is built alone and memory stays one 시군구's. `document_setup.sql`
-- builds the comparables and region documents (`rt_comparables`, `rt_region_doc`) and `gold_rows`.
--
-- Inputs: `placed` (real_transaction_placement_stage: every deal, its PNU or NULL), `parcels(pnu)`,
-- `land(pnu, land_category, …)`, `region` (region_stats.sql's rows), `place_ids(pnu, place_id)`.
-- The stage fills: the build month's first day, COMPARABLE_MONTHS and REGION_MONTHS before it, the
-- month after it, COMPARABLE_COUNT, the document bound less its envelope, `history_coverage` as JSON,
-- and a deal's and a comparable's JSON object over `placed`'s columns.
--
-- The JSON is text built field by field: every key, a missing value null, a double as `json_double`
-- writes it. `DuckDB`'s own `to_json` is used for strings and string lists only. The bound counts
-- characters, as `length` does: the Spark job's bound, kept, since it decides which deals are left out.
INSERT INTO gold_rows
WITH own AS (
  SELECT pnu, row_id, deal_date, dataset_slug, main_lot_group_key, {transaction} AS _entry
  FROM placed WHERE pnu IS NOT NULL AND substr(pnu, 1, 5) = '{chunk}'
),
-- Which PNUs carry comparables: 지목 공장용지·창고용지, or a 공장창고 deal of their own.
eligible AS (
  SELECT DISTINCT e.pnu FROM (
    SELECT pnu FROM land WHERE land_category IN ('공장용지', '창고용지') AND substr(pnu, 1, 5) = '{chunk}'
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
  WHERE substr(pnu, 1, 5) = '{chunk}' AND substr(pnu, 1, 15) IN (SELECT main_lot_group_key FROM keys)
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
  LEFT JOIN rt_comparables c ON c.legal_dong = substr(p.pnu, 1, 10)
  LEFT JOIN rt_region_doc r ON r.legal_dong = substr(p.pnu, 1, 10)
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
       -- The content columns as JSON, a null field left out.
       sha256('{"pnu":' || to_json(d.pnu)::VARCHAR
              || ',"real_transaction_json":' || to_json(d.real_transaction_json)::VARCHAR
              || ',"transaction_total":' || d.transaction_total
              || coalesce(',"place_id":' || to_json(i.place_id)::VARCHAR, '')
              || coalesce(',"main_lot_group_place_ids":' || to_json(m.main_lot_group_place_ids)::VARCHAR, '')
              || '}') AS row_digest
FROM documents d
LEFT JOIN place_ids i USING (pnu)
LEFT JOIN members m USING (pnu);
