//! Recurrence guard for the executable Martin/PostGIS/PMTiles proof harness.
//!
//! The parcel-only proof (`scripts/tiles/tiles-slice-proof.sh`, its compose stack and HTTP evidence
//! helper) retired with the PostGIS parcel copy (root ADR-0571); `boundary-slice-proof.sh` is the
//! harness left.

#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(4)
        .expect("foundation-api must be nested under the repository root")
        .to_path_buf()
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    let message = format!("read {}", path.display());
    fs::read_to_string(&path).expect(&message)
}

fn require_all(haystack: &str, needles: &[&str], context: &str) {
    for needle in needles {
        assert!(
            haystack.contains(needle),
            "{context} must contain {needle:?}"
        );
    }
}

/// One embedded migrator for the whole package. The library defines it and both readers use it:
/// the runner applies it, and `/readyz` compares the running database against it. A second
/// embedding in the runner would let the two disagree about what "complete" means — the probe
/// would pass a database the runner considers unfinished, or refuse one it built.
fn assert_one_embedded_migrator(migrator: &str) {
    let library = read("platforms/foundation-platform/services/foundation-api/src/lib.rs");
    require_all(
        &library,
        &["pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!(\"../../migrations\")"],
        "canonical Foundation migrator definition",
    );
    require_all(
        migrator,
        &["use foundation_api::MIGRATOR", "MIGRATOR.run(&pool).await?"],
        "canonical Foundation migration runner",
    );
    assert!(
        !migrator.contains("sqlx::migrate!"),
        "the migration runner must use the library's migrator, not embed a second one"
    );
}

#[test]
fn production_static_martin_config_uses_private_r2_prefix_discovery() {
    let config = read("scripts/tiles/martin-static-production.yaml");
    require_all(
        &config,
        &[
            "pmtiles:\n  aws_access_key_id: ${FOUNDATION_PLATFORM_TILE_DERIVATIVES_READER_ACCESS_KEY_ID}",
            "  aws_secret_access_key: ${FOUNDATION_PLATFORM_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY}",
            concat!(
                "  aws_region: ${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_REGION:",
                "-auto}"
            ),
            "  aws_endpoint_url: ${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_ENDPOINT}",
            "  aws_virtual_hosted_style_request: false",
            "  paths:",
            "reload_interval:",
            "GONGZZANG_PUBLIC_ORIGIN",
        ],
        "production static Martin config",
    );
    assert!(!config.contains("${FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID}"));
    assert!(!config.contains("${FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY}"));
    assert!(!config.contains("${R2_S3_ENDPOINT}"));
    assert!(!config.contains("allow_http"));
    assert!(!config.contains("pmtiles:\n  sources:"));
}

#[test]
fn root_tile_contract_inputs_trigger_their_authoritative_ci_lanes() {
    let foundation_ci = read(".github/workflows/foundation-ci.yml");
    // Area workflows take their inputs from the change-scope contract (root ADR-0202).
    let change_scope: serde_json::Value =
        serde_json::from_str(&read("tools/ci/change-scope.json")).expect("parse change scope");

    // Foundation CI is a scoped workflow too (root ADR-0313): its inputs are in the contract, and a
    // change to the contract itself reruns every scoped workflow.
    let foundation_inputs = change_scope["workflows"]["foundation-ci.yml"]["paths"]
        .as_array()
        .expect("foundation-ci.yml is a scoped workflow");
    for (input, why) in [
        ("scripts/tiles/**", "tile proof/config inputs"),
        (
            "scripts/verify/integration.sh",
            "its disposable-DB integration provisioner",
        ),
    ] {
        assert!(
            foundation_inputs.iter().any(|glob| glob == input),
            "Foundation CI must take {why} ({input}) as an input"
        );
    }
    require_all(
        &foundation_ci,
        &[
            "static_release_toolchain_contract.py install",
            "--platform linux-x86_64",
            "--platform windows-x86_64",
            "verify-static-release-toolchain",
        ],
        "Foundation executable-toolchain preflight",
    );
}

#[test]
fn canonical_foundation_migrator_is_the_only_schema_executor() {
    let proof = read("scripts/tiles/boundary-slice-proof.sh");
    let integration = read("scripts/verify/integration.sh");
    let foundation_ci = read(".github/workflows/foundation-ci.yml");
    let migrator =
        read("platforms/foundation-platform/services/foundation-api/src/bin/foundation-migrate.rs");
    let build_script = read("platforms/foundation-platform/services/foundation-api/build.rs");
    let canonical = "cargo run --locked --quiet -p foundation-api --bin foundation-migrate";

    assert_one_embedded_migrator(&migrator);
    assert_eq!(
        build_script
            .matches("cargo:rerun-if-changed=../../migrations")
            .count(),
        1,
        "Cargo must invalidate the embedded migrator when a migration file is added or removed"
    );

    for (label, contents) in [
        ("tile proof", proof.as_str()),
        ("disposable integration harness", integration.as_str()),
        ("Foundation CI", foundation_ci.as_str()),
    ] {
        assert!(
            contents.contains(canonical),
            "{label} must invoke the canonical embedded SQLx migrator"
        );
        for duplicated_ledger_knowledge in ["migration_files=", "migration_ledger="] {
            assert!(
                !contents.contains(duplicated_ledger_knowledge),
                "{label} must leave embedded migration/ledger validation to SQLx in foundation-migrate: {duplicated_ledger_knowledge}"
            );
        }
    }
    for (label, contents) in [
        ("tile proof", proof.as_str()),
        ("disposable integration harness", integration.as_str()),
    ] {
        assert!(
            !contents.contains("_sqlx_migrations"),
            "{label} must not inspect SQLx's private ledger table outside foundation-migrate"
        );
    }

    assert!(!integration.contains("PREPARE_SQL=("));
    assert!(!proof.contains("migrations=("));
    assert!(!foundation_ci.contains("cargo install sqlx-cli"));
    assert!(!foundation_ci.contains("sqlx migrate run"));

    let migration_dir = repo_root().join("platforms/foundation-platform/migrations");
    let migration_names = fs::read_dir(&migration_dir)
        .expect("read Foundation migration directory")
        .map(|entry| entry.expect("read Foundation migration entry"))
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "sql")
        })
        .map(|entry| {
            entry
                .file_name()
                .into_string()
                .expect("migration filenames must be UTF-8")
        })
        .collect::<Vec<_>>();
    assert!(!migration_names.is_empty());

    for migration in migration_names {
        assert!(
            !build_script.contains(&migration),
            "the Cargo invalidation guard must watch the migration directory, not pin {migration}"
        );
        for (label, contents) in [
            ("tile proof", proof.as_str()),
            ("disposable integration harness", integration.as_str()),
            ("Foundation CI", foundation_ci.as_str()),
        ] {
            assert!(
                !contents.contains(&migration),
                "{label} must discover migrations through foundation-migrate, not pin {migration}"
            );
        }
    }
}

#[test]
fn outbox_manifest_roundtrip_cannot_skip_a_missing_seed_or_duplicate_its_version() {
    let roundtrip =
        read("platforms/foundation-platform/crates/foundation-outbox/tests/publish_roundtrip.rs");

    require_all(
        &roundtrip,
        &[
            "const LOCAL_VECTOR_TILE_MANIFEST_ID: Uuid",
            "SELECT id, current_version",
            "WHERE id = $1",
            ".fetch_one(pool)",
            "Some(active_manifest.current_version.as_str())",
        ],
        "Foundation outbox vector-tile manifest roundtrip",
    );
    for forbidden in [
        "current_version = 'dev-local'",
        "TestResult<Option<Uuid>>",
        "let Some(active_manifest_id) = activate_local_vector_tile_manifest_seed",
    ] {
        assert!(
            !roundtrip.contains(forbidden),
            "the integration test must fail rather than skip when its stable seed is missing: {forbidden}"
        );
    }
}

#[test]
fn production_runbook_locks_private_derivative_bucket_and_pointer_safety() {
    let runbook =
        read("platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md");

    // Anchors are technical identifiers and the English terms the Korean-first policy keeps
    // verbatim. Fourteen of these used to be English prose; the Korean-first migration (558c5beb)
    // translated those sentences and this test broke — masked at first, because `cargo test`
    // fail-fasts at the first failing target and `administrative_boundary_contract` fell over
    // before this binary ran. Same repair as there: rewording a sentence must not fail this test,
    // deleting a mechanism must.
    require_all(
        &runbook,
        &[
            // Canonical data and serving derivatives stay in separate private buckets.
            "serving-derivative",
            "Lakehouse, Bronze, recovery, backup",
            "gold/vector-tiles/releases",
            // PostGIS remains the complete warm projection; static only sheds render load.
            "warm serving projection",
            // Every release pins the Catalog-selected snapshot and logical revision.
            "Iceberg",
            "`data_revision`",
            // Split credentials: Martin reads, the publisher writes; the prefix is not IAM.
            "읽기 전용 자격증명",
            "쓰기 자격증명",
            "not an IAM boundary",
            "create-only precondition",
            "`FOUNDATION_PLATFORM_R2_LAKEHOUSE_*`",
            "static-release-toolchain.contract.json",
            "`pmtiles.paths`",
            "named sources are startup snapshots",
            "The R2 bucket itself needs no public domain",
            "`gold/manifest.json` bytes unchanged",
            "`gold/vector-tiles/runtime-manifest.json`",
            "`CF-Cache-Status`/`Age` evidence",
            "Compare and swap",
            "emit the outbox projection event",
            "`SUPERSEDED`",
            "never edits a historical manifest",
            // The frozen v1 fixture keeps the proof-only alias beside the canonical property.
            "`PNU`",
            "`pnu`",
            "production `foundation-migrate` SQLx runner",
            "user curl configuration",
            "full public readback SHA-256",
            "PutObject response body is",
            "discarded instead of being written to disk",
        ],
        "tile proof production runbook",
    );
    for forbidden in [
        "dedicated public static-tile serving bucket",
        "must retarget `FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET`",
        "outbox publisher writes the public R2 manifest pointer",
    ] {
        assert!(
            !runbook.contains(forbidden),
            "private derivative-bucket runbook must not restore obsolete public-default wording: {forbidden}"
        );
    }
}

/// Every tile-derivative variable the runbook names is one the connection contract defines.
///
/// The runbook is the sheet an operator copies an `export` line from, and it named four variables
/// that do not exist: `..._PUBLISHER_ACCESS_KEY_ID`, `..._PUBLISHER_SECRET_ACCESS_KEY`,
/// `..._MARTIN_READ_ACCESS_KEY_ID`, `..._MARTIN_READ_SECRET_ACCESS_KEY`, where
/// `config/r2-connections.contract.json` and `validate-tile-derivative-r2` both say `..._WRITER_*`
/// and `..._READER_*`. Nothing failed: the same document also spelled the real names correctly
/// twelve lines later, so a reader could not tell which half was the contract. The exported-but-
/// wrong name is silently empty and the preflight reports a missing variable the operator believes
/// they just set.
///
/// The contract file is the source both sides are compared against, so this cannot be satisfied by
/// editing prose — only by naming a variable the connection contract actually declares.
#[test]
fn the_runbook_only_names_tile_derivative_variables_the_contract_declares() {
    const PREFIX: &str = "FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_";
    // The keys are named after their Cloudflare tokens, without `R2_` (root ADR-0247).
    const CREDENTIAL_PREFIX: &str = "FOUNDATION_PLATFORM_TILE_DERIVATIVES_";

    let contract = read("platforms/foundation-platform/config/r2-connections.contract.json");
    let runbook =
        read("platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md");

    // The runbook abbreviates the prefix as `...` after naming it in full, so both spellings are
    // resolved to the same full variable name before the comparison.
    let mut named = BTreeSet::new();
    for (marker, prefix, cut) in [
        (PREFIX, PREFIX, 0),
        (CREDENTIAL_PREFIX, CREDENTIAL_PREFIX, 0),
        ("`..._", PREFIX, 1),
    ] {
        let mut rest = runbook.as_str();
        while let Some(start) = rest.find(marker) {
            let tail = &rest[start + marker.len() - cut..];
            let suffix: String = tail
                .chars()
                .take_while(|character| character.is_ascii_uppercase() || *character == '_')
                .collect();
            let suffix = suffix.trim_start_matches('_').trim_end_matches('_');
            if !suffix.is_empty() && suffix != "*" {
                named.insert(format!("{prefix}{suffix}"));
            }
            rest = &rest[start + marker.len()..];
        }
    }
    assert!(
        named.len() >= 6,
        "the runbook should name the tile-derivative connection variables; found {named:?}"
    );

    for variable in &named {
        assert!(
            contract.contains(variable.as_str()),
            "the tile runbook names {variable}, which config/r2-connections.contract.json does not \
             declare; an operator exporting it sets nothing"
        );
    }
}
