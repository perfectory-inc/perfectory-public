WITH RECURSIVE all_applications AS (
         SELECT application.id AS application_id,
                application.proposal_id,
                CASE
                    WHEN application.before_snapshot ? 'lineage_predecessor_proposal_id'
                    THEN application.before_snapshot->>'lineage_predecessor_proposal_id'
                    ELSE application.before_snapshot->'active_override'->>'proposal_id'
                END AS predecessor_proposal_id,
                application.after_snapshot,
                proposal.target_identity,
                COALESCE(
                    jsonb_typeof(application.before_snapshot) = 'object'
                    AND application.before_snapshot ? 'active_override'
                    AND (
                        application.before_snapshot->'active_override' = 'null'::jsonb
                        OR (
                            jsonb_typeof(application.before_snapshot->'active_override') = 'object'
                            AND jsonb_typeof(
                                application.before_snapshot->'active_override'->'proposal_id'
                            ) = 'string'
                            AND application.before_snapshot->'active_override'->>'proposal_id'
                                ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        )
                    )
                    AND (
                        NOT application.before_snapshot ? 'lineage_predecessor_proposal_id'
                        OR application.before_snapshot->'lineage_predecessor_proposal_id'
                            = 'null'::jsonb
                        OR (
                            jsonb_typeof(
                                application.before_snapshot->'lineage_predecessor_proposal_id'
                            ) = 'string'
                            AND application.before_snapshot->>'lineage_predecessor_proposal_id'
                                ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                        )
                    )
                    AND jsonb_typeof(application.after_snapshot) = 'object'
                    AND jsonb_typeof(application.after_snapshot->'proposal_id') = 'string'
                    AND application.after_snapshot->>'proposal_id' = application.proposal_id::text
                    AND jsonb_typeof(application.after_snapshot->'target_identity') = 'object'
                    AND application.after_snapshot->'target_identity' = proposal.target_identity
                    AND jsonb_typeof(application.after_snapshot->'proposed_record') = 'object'
                    AND COALESCE(
                        (
                            SELECT bool_and(COALESCE(
                                rollback.command_type = $4
                                AND rollback.target_kind = application.target_kind
                                AND rollback.proposal_id = application.proposal_id
                                AND rollback.target_id IS NOT DISTINCT FROM application.target_id
                                AND rollback.expected_version > 0
                                AND (
                                    (
                                        jsonb_typeof(rollback.before_snapshot) = 'object'
                                        AND rollback.before_snapshot ? 'active_override'
                                        AND (
                                            rollback.before_snapshot->'active_override'
                                                = 'null'::jsonb
                                            OR (
                                                jsonb_typeof(
                                                    rollback.before_snapshot->'active_override'
                                                ) = 'object'
                                                AND jsonb_typeof(
                                                    rollback.before_snapshot
                                                        ->'active_override'->'proposal_id'
                                                ) = 'string'
                                                AND rollback.before_snapshot
                                                        ->'active_override'->>'proposal_id'
                                                    ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                                                AND jsonb_typeof(
                                                    rollback.before_snapshot
                                                        ->'active_override'->'target_identity'
                                                ) = 'object'
                                                AND rollback.before_snapshot
                                                        ->'active_override'->'target_identity'
                                                    = proposal.target_identity
                                                AND jsonb_typeof(
                                                    rollback.before_snapshot
                                                        ->'active_override'->'proposed_record'
                                                ) = 'object'
                                            )
                                        )
                                        AND jsonb_typeof(rollback.after_snapshot) = 'object'
                                        AND rollback.after_snapshot ? 'active_override'
                                        AND (
                                            rollback.after_snapshot->'active_override'
                                                = 'null'::jsonb
                                            OR (
                                                jsonb_typeof(
                                                    rollback.after_snapshot->'active_override'
                                                ) = 'object'
                                                AND jsonb_typeof(
                                                    rollback.after_snapshot
                                                        ->'active_override'->'proposal_id'
                                                ) = 'string'
                                                AND rollback.after_snapshot
                                                        ->'active_override'->>'proposal_id'
                                                    ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                                                AND jsonb_typeof(
                                                    rollback.after_snapshot
                                                        ->'active_override'->'target_identity'
                                                ) = 'object'
                                                AND rollback.after_snapshot
                                                        ->'active_override'->'target_identity'
                                                    = proposal.target_identity
                                                AND jsonb_typeof(
                                                    rollback.after_snapshot
                                                        ->'active_override'->'proposed_record'
                                                ) = 'object'
                                            )
                                        )
                                    )
                                    OR (
                                        rollback.before_snapshot = application.after_snapshot
                                        AND rollback.after_snapshot = application.before_snapshot
                                    )
                                ),
                                false
                            ))
                            FROM catalog.normalization_application rollback
                            WHERE rollback.rollback_of = application.id
                        ),
                        true
                    ),
                    false
                ) AS snapshots_valid,
                NOT EXISTS (
                    SELECT 1
                    FROM catalog.normalization_application rollback
                    WHERE rollback.rollback_of = application.id
                ) AS is_active
         FROM catalog.normalization_application application
         JOIN catalog.normalization_proposal proposal
           ON proposal.id = application.proposal_id
         WHERE application.target_kind = 'building_register_unit'
           AND application.command_type = $1
           AND application.rollback_of IS NULL
           AND ($2::jsonb IS NULL OR proposal.target_identity = $2::jsonb)
     ),
     chain_walk AS (
         SELECT application.application_id,
                application.proposal_id,
                application.predecessor_proposal_id,
                application.after_snapshot,
                application.target_identity,
                application.is_active,
                1::bigint AS depth,
                ARRAY[application.application_id] AS path
         FROM all_applications application
         WHERE application.predecessor_proposal_id IS NULL

         UNION ALL

         SELECT successor.application_id,
                successor.proposal_id,
                successor.predecessor_proposal_id,
                successor.after_snapshot,
                successor.target_identity,
                successor.is_active,
                predecessor.depth + 1,
                predecessor.path || successor.application_id
         FROM chain_walk predecessor
         JOIN all_applications successor
           ON successor.target_identity IS NOT DISTINCT FROM predecessor.target_identity
          AND successor.predecessor_proposal_id = predecessor.proposal_id::text
         WHERE NOT successor.application_id = ANY(predecessor.path)
     ),
     target_stats AS (
         SELECT application.target_identity,
                count(*) AS total_count,
                count(*) FILTER (
                    WHERE application.predecessor_proposal_id IS NULL
                ) AS root_count,
                bool_and(
                    application.target_identity IS NOT NULL
                    AND jsonb_typeof(application.target_identity) = 'object'
                    AND application.snapshots_valid
                ) AS snapshots_valid
         FROM all_applications application
         GROUP BY application.target_identity
     ),
     walk_stats AS (
         SELECT chain.target_identity,
                count(DISTINCT chain.application_id) AS reachable_count
         FROM chain_walk chain
         GROUP BY chain.target_identity
     ),
     successor_counts AS (
         SELECT predecessor.target_identity,
                predecessor.application_id,
                count(successor.application_id) AS successor_count
         FROM all_applications predecessor
         LEFT JOIN all_applications successor
           ON successor.target_identity IS NOT DISTINCT FROM predecessor.target_identity
          AND successor.predecessor_proposal_id = predecessor.proposal_id::text
         GROUP BY predecessor.target_identity, predecessor.application_id
     ),
     graph_stats AS (
         SELECT successor.target_identity,
                sum(successor.successor_count) AS edge_count,
                max(successor.successor_count) AS max_successor_count
         FROM successor_counts successor
         GROUP BY successor.target_identity
     ),
     ranked_active AS (
         SELECT chain.application_id,
                chain.after_snapshot,
                chain.target_identity,
                row_number() OVER (
                    PARTITION BY chain.target_identity
                    ORDER BY chain.depth DESC
                ) AS active_rank
         FROM chain_walk chain
         WHERE chain.is_active
           AND ($3::uuid IS NULL OR chain.application_id <> $3)
     ),
     ranked_tail AS (
         SELECT chain.proposal_id,
                chain.target_identity,
                row_number() OVER (
                    PARTITION BY chain.target_identity
                    ORDER BY chain.depth DESC
                ) AS tail_rank
         FROM chain_walk chain
     )
     SELECT active.application_id,
            active.after_snapshot,
            tail.proposal_id AS lineage_tail_proposal_id,
            stats.target_identity,
            stats.snapshots_valid
                AND stats.root_count = 1
                AND COALESCE(walk.reachable_count, 0) = stats.total_count
                AND graph.edge_count = stats.total_count - 1
                AND graph.max_successor_count <= 1 AS chain_valid
     FROM target_stats stats
     LEFT JOIN walk_stats walk
       ON walk.target_identity IS NOT DISTINCT FROM stats.target_identity
     JOIN graph_stats graph
       ON graph.target_identity IS NOT DISTINCT FROM stats.target_identity
     LEFT JOIN ranked_active active
       ON active.target_identity IS NOT DISTINCT FROM stats.target_identity
      AND active.active_rank = 1
     LEFT JOIN ranked_tail tail
       ON tail.target_identity IS NOT DISTINCT FROM stats.target_identity
      AND tail.tail_rank = 1
     ORDER BY stats.target_identity
