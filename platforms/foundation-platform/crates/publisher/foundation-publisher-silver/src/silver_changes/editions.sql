-- The staged rows of an editions table compared by whole list (root ADR-0540 §3): a group (`per`)
-- whose staged list is new or differs from the rows of its newest edition is appended whole, an
-- unchanged one appends nothing, so a group's newest edition is always its last read's list. It was
-- the Spark load's `changed_editions`, which compared the sorted hashes of each row's null-ness and
-- values: the two `EXCEPT ALL` (which take nulls as equal) are the same comparison without the hash.
-- The newest edition is `platform_contracts.newest_edition`'s: per group, the edition with the
-- greatest maxima of `newest_first`, compared in turn; a tie refuses the load (checked by the caller).
-- Parameters: {source} (the table at the pinned head, or a query of none), {staged}, {out}, {per},
-- {edition}, {maxima} (`max(x) AS m0, …`), {order} (`m0 DESC NULLS LAST, …`), {content},
-- {per_match} (`r.x IS NOT DISTINCT FROM s.x` per group column), {columns}.
-- {source} is read inside a subquery: a table pinned with `AT (VERSION => n)` takes no alias after it
-- (2026-11-23: the first load onto a table that already held editions failed on `... r`).
CREATE OR REPLACE TEMP TABLE {out}_editions AS
WITH editions AS (
  SELECT {per}, {edition}, {maxima}
  FROM (SELECT * FROM {source}) r
  WHERE EXISTS (SELECT 1 FROM {staged} s WHERE {per_match})
  GROUP BY ALL
)
SELECT *, rank() OVER (PARTITION BY {per} ORDER BY {order}) AS foundation_edition_rank
FROM editions
QUALIFY foundation_edition_rank = 1;

CREATE OR REPLACE TEMP TABLE {out}_current AS
SELECT {columns} FROM (SELECT * FROM {source}) r
WHERE EXISTS (SELECT 1 FROM {out}_editions s WHERE {per_match}
              AND r.{edition} IS NOT DISTINCT FROM s.{edition});

CREATE OR REPLACE TEMP TABLE {out}_moved AS
SELECT DISTINCT {per} FROM (
  (SELECT {per}, {content} FROM {staged} EXCEPT ALL SELECT {per}, {content} FROM {out}_current)
  UNION ALL
  (SELECT {per}, {content} FROM {out}_current EXCEPT ALL SELECT {per}, {content} FROM {staged})
);

CREATE OR REPLACE TEMP TABLE {out} AS
SELECT s.* FROM {staged} s
WHERE EXISTS (SELECT 1 FROM {out}_moved r WHERE {per_match});
