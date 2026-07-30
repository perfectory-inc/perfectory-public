-- One ledger for keyed Catalog publication mutations, so a retry returns the first outcome.
--
-- Today the five v2 publication commands all carry an `idempotency_key` that reaches no column.
-- What stops a double publication is incidental: the compare-and-swap refuses a retry because its
-- `expected_active_release_id` is stale. That is safe and it is not idempotency — the caller gets an
-- error for a request that already succeeded, and cannot tell that from a request that failed.
--
-- Recognising a retry was already spelled three ways in this schema:
--
--   * catalog.collection_job — idempotency_key + request_fingerprint_sha256 +
--     request_fingerprint_schema_version + success jsonb (and no UNIQUE on the key; it dedupes by
--     job_id, which contradicts gongzzang ADR-0047 §218);
--   * catalog.vector_tile_build_job — UNIQUE (publication_unit_id, idempotency_key), written by
--     nothing;
--   * catalog.vector_tile_refresh_observation — idempotency_key UNIQUE.
--
-- This table is the one definition for the keyed publication commands. It deliberately copies
-- collection_job's column vocabulary rather than inventing a fourth: same names, same CHECK shapes,
-- and the same `*_schema_version` idea.
--
-- What that version column does, precisely, because the first version of this comment overstated it:
-- it is read back on every claim and compared before the digest is. Two digests produced by
-- different encodings say nothing about each other, so a mismatch is reported as its own condition
-- rather than as a key reuse — the caller did nothing wrong, the deployment's encoding changed. It
-- does NOT make an encoding change transparent: keys recorded under an older version still have to
-- be re-minted. Making them replay would require storing the old encoding, not just its name.
--
-- Scope, stated so the "one ledger" claim is not read wider than it is:
--
--   * covered: mark_tile_layer_dynamic, start_vector_tile_build, promote_tile_layer_static,
--     rollback_tile_layer_source — the four commands that carry a key;
--   * exempt: record_vector_tile_build_result, which carries no key because its request identity is
--     already (build_job_id, outcome); a key would be a second spelling of that fact;
--   * NOT covered: catalog.promote_vector_tile_runtime_manifest called directly, and the raw-SQL
--     operator command in foundation-outbox-publisher. Both move the pointer without a unit of work.
--     Recorded as debt in ADR-0015.
--
-- What is deliberately absent:
--
--   * No status/INPROGRESS column and no lease. Every covered command that moves the pointer takes
--     SHARE ROW EXCLUSIVE on catalog.vector_tile_runtime_manifest_pointer first, and the two build
--     commands serialize on this table's primary key, so a duplicate blocks on a real lock and then
--     reads a committed-or-absent row. An "in progress" state would be a second, weaker copy of a
--     lock Postgres already holds, and Postgres aborting a dead backend's transaction is the
--     equivalent of the expiry those designs need.
--   * No failure record. A failed mutation rolls its ledger row back with itself, so a retry
--     re-executes and a key can never be poisoned. Recording a failure would need a second
--     connection and a second commit, which is precisely the atomicity this table exists to have.
--   * No retention sweep. Nothing in this platform prunes any catalog.* table today, including the
--     outbox. `created_at` is here so a future sweep has an indexable predicate without a data
--     migration; ADR-0015 records the revisit trigger and, per command, what a deleted key would
--     allow.
--
-- The outcome is a reference, never a snapshot. Every row a runtime manifest is rebuilt from is
-- immutable, and `load_vector_tile_runtime_manifest_by_id` already reproduces it, so storing a
-- response body would be a second statement of the wire shape that can disagree with the reader.

CREATE TABLE catalog.catalog_mutation_idempotency (
    idempotency_key text NOT NULL,
    command_kind text NOT NULL,
    request_fingerprint_sha256 character(64) NOT NULL,
    request_fingerprint_schema_version text NOT NULL,
    outcome_manifest_id uuid,
    outbox_event_id uuid,
    operator_staff_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT catalog_mutation_idempotency_pkey PRIMARY KEY (idempotency_key),
    -- Bounded, not merely non-empty. `collection_job` only asserts non-empty, which leaves an
    -- unbounded index key and a log-injection surface; `outbox_quarantine_consumer_key_check` is the
    -- better in-repo precedent. The 255 ceiling is what the vendors converge on.
    CONSTRAINT catalog_mutation_idempotency_key_check CHECK (idempotency_key ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{1,254}$'),
    -- Mirrors catalog_domain::CatalogMutationKind exactly, the same way
    -- vector_tile_build_job_status_check mirrors VectorTileBuildStatus.
    CONSTRAINT catalog_mutation_idempotency_command_kind_check CHECK (command_kind IN ('mark_tile_layer_dynamic', 'start_vector_tile_build', 'promote_tile_layer_static', 'rollback_tile_layer_source')),
    CONSTRAINT catalog_mutation_idempotency_fingerprint_check CHECK (request_fingerprint_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT catalog_mutation_idempotency_fingerprint_version_check CHECK (btrim(request_fingerprint_schema_version) <> ''),
    -- The three pointer-moving commands answer with a manifest; the build start answers with a build
    -- job id and must not claim one. No outcome column is pre-added for the build id: three of the
    -- four commands have no implementation yet, and pre-shaping their storage is how
    -- vector_tile_build_job became a table nothing writes.
    CONSTRAINT catalog_mutation_idempotency_manifest_outcome_check CHECK (
        (command_kind IN ('mark_tile_layer_dynamic', 'promote_tile_layer_static', 'rollback_tile_layer_source') AND outcome_manifest_id IS NOT NULL)
        OR
        (command_kind = 'start_vector_tile_build' AND outcome_manifest_id IS NULL)
    )
);

-- DEFERRABLE has no precedent in this repository's migrations, and it is the point of this
-- constraint rather than a convenience.
--
-- The claim has to be the transaction's first write. The pointer lock that follows it is
-- table-level, so it serializes activations of every publication unit; a ledger read placed after it
-- would make a replay queue behind an unrelated in-flight activation just to discover it has nothing
-- to do — and every pooled connection carries statement_timeout = 2500ms, so that queue ends in a
-- redacted 500 rather than a retryable answer.
--
-- The manifest id, meanwhile, is only resolvable later: `manifest_generation` is derived as
-- max + 1 and the promotion gate refuses a non-increasing generation, so the manifest row cannot be
-- written before the pointer is locked. A deferred foreign key is exactly the tool for "these two
-- rows must agree by commit time, in either write order". The invariant it buys is real: a committed
-- ledger row always points at a manifest that exists, so a manifest can never be removed while a
-- ledger row would replay it.
--
-- `outbox_event_id` gets no foreign key on purpose. `catalog.outbox_event` has a primary key on
-- `event_id`, so one is possible, but published rows are a candidate for pruning and an FK would
-- silently make this table the reason a pruner cannot run.
ALTER TABLE catalog.catalog_mutation_idempotency
    ADD CONSTRAINT catalog_mutation_idempotency_outcome_manifest_fkey
    FOREIGN KEY (outcome_manifest_id) REFERENCES catalog.vector_tile_runtime_manifest(id)
    DEFERRABLE INITIALLY DEFERRED;

-- Answers "what has this operator published" without scanning, and gives a future retention sweep an
-- indexed predicate. Not unique: an operator legitimately holds many keys.
CREATE INDEX catalog_mutation_idempotency_operator_idx
    ON catalog.catalog_mutation_idempotency (operator_staff_id, created_at DESC, idempotency_key);
