-- The rows of a fold handoff the append refuses, with the reason (root ADR-0115 §9): anything the
-- exporter (stewardship_domain::fold::lineage_row) would not write. One row per refused row.
SELECT successor_pnu, reason
FROM (
    SELECT ordinal, successor_pnu,
        CASE
            WHEN evidence_kind <> 'steward' THEN 'not a steward row'
            WHEN NOT regexp_full_match(successor_pnu, '[0-9]{19}') THEN 'not a PNU'
            WHEN grade = 'official' AND NOT regexp_full_match(coalesce(predecessor_pnu, ''), '[0-9]{19}')
                THEN 'a link names a predecessor PNU'
            WHEN grade = 'pending' AND coalesce(predecessor_pnu, '') <> ''
                THEN '''not a link'' names no predecessor'
            WHEN grade NOT IN ('official', 'pending') THEN 'steward grade ' || grade
            WHEN count(*) OVER (PARTITION BY decision_id) > 1 THEN 'a decision appears twice'
        END AS reason
    FROM fold_handoff
) AS judged
WHERE reason IS NOT NULL
ORDER BY ordinal
