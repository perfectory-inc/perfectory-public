-- What one bucket of a release appends to `main` (root ADR-0275, ADR-0318, ADR-0599 §3): every row
-- of the release whose key is new or whose rows changed, and one removal marker per key with
-- current rows and none in the release. It was `silver_release_compare.compared_changes`.
--
-- A key's release is a set of rows (a source repeats a key, root ADR-0090), so a key is compared by
-- the multiset of its rows' content: how many rows, and the sum and the xor of their fingerprints.
-- Keys match null-safe. Null-ness is hashed apart from the value, so a null and an empty string
-- differ.
--
-- Parameters, rendered by `release_compare_stage::render` (and by nothing else):
--   {release}        the release's local copy restricted to the bucket, the table's columns
--   {current_narrow} the contract's current-rows query over main's bucket, projecting the key and
--                    `rc_fp` only: the rank window carries no content column (measured 2026-11-20:
--                    7 GB of spill for 87M versions, against 22.6 GB with every column)
--   {current_full}   the contract's current-rows query over main's bucket, the keys of
--                    `rc_removed` only, every column: what a marker is made of
--   {key}, {s_key}, {c_key}, {m_key}: the key columns, bare and qualified
--   {same_s_c}, {same_k_r}: the key matched null-safe between two aliases
--   {fingerprint}    the content fingerprint of a release row
--   {stamped}        a release row's columns with the release's edition and this run's instant
--   {marker}         a current row's columns as a removal marker of this release
--   {marker_order}   the content columns, then every other column: one order, no tie
-- Output: rc_changed and rc_removed (the keys), and rc_changes (the table's columns), a view: the
-- rows are read once, by the copy that writes them (materialised, bucket 41 of silver.land_use_plan
-- spilled 11.6 GB on 2026-11-21).

CREATE OR REPLACE TEMP TABLE rc_current AS
SELECT {key}, count(*) AS rc_n, sum(CAST(rc_fp AS HUGEINT)) AS rc_s, bit_xor(rc_fp) AS rc_x
FROM ({current_narrow}) GROUP BY {key};

CREATE OR REPLACE TEMP TABLE rc_staged AS
SELECT {key}, count(*) AS rc_n, sum(CAST({fingerprint} AS HUGEINT)) AS rc_s, bit_xor({fingerprint}) AS rc_x
FROM {release} GROUP BY {key};

CREATE OR REPLACE TEMP TABLE rc_changed AS
SELECT {s_key} FROM rc_staged s LEFT JOIN rc_current c ON {same_s_c}
WHERE c.rc_n IS NULL OR c.rc_n <> s.rc_n OR c.rc_s <> s.rc_s OR c.rc_x <> s.rc_x;

CREATE OR REPLACE TEMP TABLE rc_removed AS
SELECT {c_key} FROM rc_current c
WHERE NOT EXISTS (SELECT 1 FROM rc_staged s WHERE {same_s_c});

CREATE OR REPLACE TEMP VIEW rc_changes AS
SELECT {stamped} FROM {release} r
WHERE EXISTS (SELECT 1 FROM rc_changed k WHERE {same_k_r})
UNION ALL
SELECT {marker} FROM (
  SELECT m.*, row_number() OVER (PARTITION BY {m_key} ORDER BY {marker_order}) AS rc_first
  FROM ({current_full}) m
) m
WHERE rc_first = 1;
