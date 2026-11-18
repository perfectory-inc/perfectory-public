-- The silver.parcel_lineage rows of a checked fold handoff (root ADR-0115 §9), in the table's sort
-- order. The run id is the digest of the sorted decision ids, so the same decisions always make the
-- same run and the append's re-run guard recognises them. {derived_at_epoch_seconds} is the one
-- value the caller supplies.
SELECT
    nullif(predecessor_pnu, '') AS predecessor_pnu,
    successor_pnu,
    relation,
    CAST(NULL AS VARCHAR) AS cardinality,
    effective_date,
    grade,
    'steward' AS evidence_kind,
    evidence_ref,
    from_snapshot_id,
    to_snapshot_id,
    'steward-v1' AS rules_version,
    run.derivation_run_id,
    to_timestamp({derived_at_epoch_seconds}) AS derived_at_utc
FROM fold_handoff
CROSS JOIN (
    SELECT 'steward-fold-' || left(sha256(string_agg(decision_id, chr(10) ORDER BY decision_id)), 24)
        AS derivation_run_id
    FROM fold_handoff
) AS run
ORDER BY to_snapshot_id, successor_pnu
