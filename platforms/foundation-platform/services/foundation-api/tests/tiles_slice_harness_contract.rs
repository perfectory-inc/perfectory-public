//! Recurrence guard for the executable Martin/PostGIS/PMTiles proof harness.

#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

const POSTGIS_IMAGE: &str = "postgis/postgis:17-3.5-alpine@sha256:fe9821935d163abca5611e3e0a6a7c73c8c547f3412ed2036ec0ed8f789390da";
const RUST_IMAGE: &str =
    "rust:1.96.0-bookworm@sha256:5e2214abe154fe26e39f64488952e5c991eeed1d6d6da7cc8381ae83927f0cfc";

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(4)
        .expect("foundation-api must be nested under the repository root")
        .to_path_buf()
}

fn bash_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn bash_command() -> Command {
    #[cfg(windows)]
    {
        let git_exec_path = Command::new("git")
            .arg("--exec-path")
            .output()
            .expect("locate the Git installation that provides Bash");
        assert!(
            git_exec_path.status.success(),
            "{}",
            output_text(&git_exec_path)
        );
        let git_exec_path = String::from_utf8(git_exec_path.stdout)
            .expect("Git exec path must be UTF-8")
            .trim()
            .to_owned();
        let git_root = Path::new(&git_exec_path)
            .ancestors()
            .nth(3)
            .expect("Git exec path must be nested under the Git installation");
        let bash = git_root.join("bin/bash.exe");
        assert!(bash.is_file(), "Git Bash must exist at {}", bash.display());
        Command::new(bash)
    }
    #[cfg(not(windows))]
    {
        Command::new("bash")
    }
}

fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    let message = format!("read {}", path.display());
    fs::read_to_string(&path).expect(&message)
}

fn r2_validation_output(script: &Path, bucket: &str) -> Output {
    let mut command = bash_command();
    command
        .arg("-x")
        .arg(bash_path(script))
        .arg("--validate-r2-config-only");
    for name in [
        "FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCOUNT_ID",
        "FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCESS_KEY_ID",
        "FOUNDATION_PLATFORM_R2_TILE_PROOF_SECRET_ACCESS_KEY",
        "FOUNDATION_PLATFORM_R2_TILE_PROOF_BUCKET",
        "FOUNDATION_PLATFORM_R2_TILE_PROOF_ENDPOINT",
        "FOUNDATION_PLATFORM_R2_TILE_PROOF_READ_BASE_URL",
        "FOUNDATION_PLATFORM_R2_TILE_PROOF_READ_URL",
        "FOUNDATION_PLATFORM_R2_TILE_PROOF_OBJECT_KEY",
    ] {
        command.env_remove(name);
    }
    command
        .env(
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCOUNT_ID",
            "00000000000000000000000000000000",
        )
        .env(
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCESS_KEY_ID",
            "FAKEACCESS",
        )
        .env(
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_SECRET_ACCESS_KEY",
            "FAKESECRET",
        )
        .env("FOUNDATION_PLATFORM_R2_TILE_PROOF_BUCKET", bucket)
        .env(
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_READ_BASE_URL",
            "https://tiles-slice-proof.invalid",
        )
        .output()
        .expect("run the Bash R2 configuration preflight")
}

fn output_text(output: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
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
fn compose_is_disposable_digest_pinned_and_loopback_only() {
    let compose = read("scripts/tiles/compose.yaml");

    assert_eq!(compose.matches(POSTGIS_IMAGE).count(), 1);
    assert_eq!(compose.matches("${PERFECTORY_MARTIN_IMAGE:?").count(), 2);
    require_all(
        &compose,
        &[
            "postgis:",
            "dynamic-martin:",
            "static-martin:",
            "tmpfs:",
            "pg_isready -h 127.0.0.1",
            "127.0.0.1:3110:3000",
            "127.0.0.1:3101:3000",
            "TILES_SLICE_ARTIFACT_DIR:?",
            "TILES_SLICE_PMTILES_URL:",
            "file:///artifacts/tiles-slice-proof/local/foundation-static.pmtiles",
            "TILES_SLICE_MARTIN_CONFIG:-/etc/martin/config.yaml",
            "TILES_SLICE_STATIC_ENV_FILE:?set TILES_SLICE_STATIC_ENV_FILE",
            "martin-static-production.yaml",
            "TILES_R2_PMTILES_PREFIX",
            "RUST_LOG: warn",
            "profiles:",
            "- static",
        ],
        "tile proof compose",
    );
    assert!(
        !compose.contains("/var/lib/postgresql/data:"),
        "proof PostGIS must not use a persistent named or host volume"
    );
    assert!(
        !compose.contains("latest"),
        "proof images must never use a mutable latest tag"
    );
}

#[test]
fn production_static_martin_config_uses_private_r2_prefix_discovery() {
    let config = read("scripts/tiles/martin-static-production.yaml");
    require_all(
        &config,
        &[
            "pmtiles:\n  aws_access_key_id: ${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_ACCESS_KEY_ID}",
            "  aws_secret_access_key: ${FOUNDATION_PLATFORM_R2_TILE_DERIVATIVES_READER_SECRET_ACCESS_KEY}",
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
    assert!(!config.contains("${FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID}"));
    assert!(!config.contains("${FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY}"));
    assert!(!config.contains("${R2_S3_ENDPOINT}"));
    assert!(!config.contains("allow_http"));
    assert!(!config.contains("pmtiles:\n  sources:"));
}

#[test]
#[allow(clippy::too_many_lines)]
fn proof_script_locks_toolchain_feature_checks_and_r2_write_safety() {
    let proof = read("scripts/tiles/tiles-slice-proof.sh");

    assert!(proof.starts_with("#!/usr/bin/env bash\n"));
    assert!(
        !proof.contains("export MSYS_NO_PATHCONV"),
        "MSYS path conversion must be disabled only for Docker; exporting it breaks Git Bash curl /dev/null"
    );
    require_all(
        &proof,
        &[
            "set -euo pipefail",
            "set +x",
            "elif [[ \"${DOCKER_EXECUTABLE:-docker}\" == \"docker.exe\"",
            "BASH_REMATCH[2]",
            "DOCKER_EXECUTABLE=docker",
            "docker.exe info",
            "docker() { command \"$DOCKER_EXECUTABLE\" \"$@\"; }",
            "COMPOSE_ENV_FILE_PATH=",
            "write_compose_env()",
            "static_release_toolchain_contract.py\" image-env --shell",
            "write_static_martin_env()",
            "--env-file \"$DOCKER_COMPOSE_ENV_FILE\"",
            "rm -f -- \"$COMPOSE_ENV_FILE_PATH\"",
            "< \"$REPO_ROOT/platforms/foundation-platform/infra/db/seeds/local_vector_tile_runtime_manifest_v2.sql\"",
            "MSYS_NO_PATHCONV=1 docker compose",
            "MSYS_NO_PATHCONV=1 docker run",
            RUST_IMAGE,
            "RUSTUP_TOOLCHAIN=1.96.0-x86_64-unknown-linux-gnu",
            "PERFECTORY_PMTILES_IMAGE",
            "--entrypoint martin-cp",
            "--entrypoint mbtiles",
            "--source parcel_anchor_aggregate",
            "--source parcel_anchor",
            "--source parcels,parcel_anchor",
            "--encoding identity",
            "--min-zoom 0 --max-zoom 11",
            "--min-zoom 12 --max-zoom 13",
            "--min-zoom 14 --max-zoom 16",
            "dynamic-composite.tilejson.json",
            r#""http://127.0.0.1:3110/parcels,parcel_anchor_aggregate,parcel_anchor")"#,
            "dynamic Martin composite TileJSON request failed",
            "dynamic_tilejson_compact=",
            "dynamic Martin TileJSON is missing parseable vector_layers metadata",
            "DYNAMIC_CATALOG_PATH=",
            "dynamic Martin catalog request failed",
            "dynamic Martin catalog is missing configured source",
            "local_vector_tile_runtime_manifest_v2.sql",
            "active parcel view is not bound to the single runtime-manifest pointer",
            "validate",
            "unpack",
            "for zoom in $(seq 0 16)",
            "archive has no tile at manifest zoom",
            "0|1|2|3|4|5|6|7|8|9|10|11) expected_layers=\"parcel_anchor_aggregate\"",
            "12|13) expected_layers=\"parcel_anchor\"",
            "14|15|16) expected_layers=\"parcel_anchor,parcels\"",
            "tr -d '\\r\\n\\t'",
            "MBTILES_VECTOR_LAYERS=",
            "meta-set \"$MBTILES_CONTAINER\" json \"$MBTILES_VECTOR_LAYERS\"",
            "meta-set \"$MBTILES_CONTAINER\" bounds \"$BBOX\"",
            "meta-set \"$MBTILES_CONTAINER\" center \"$TILESET_CENTER\"",
            "convert",
            "verify",
            "--content-encoding identity",
            "REQUEST_ORIGIN=\"http://127.0.0.1:3000\"",
            "--header \"Origin: $REQUEST_ORIGIN\"",
            "cors_origin=",
            "\"$cors_origin\" == \"$REQUEST_ORIGIN\" || \"$cors_origin\" == \"*\"",
            "--expect-identity",
            "--expect-property \"pnu=$pnu\"",
            "parcel_anchor_aggregate=1",
            "parcels=3",
            "parcel_anchor=3",
            "http://127.0.0.1:3110/health",
            "http://127.0.0.1:3101/health",
            "wait_for_static_source",
            "static Martin did not discover a PMTiles source",
            "STATIC_SOURCE_ID=\"foundation_static\"",
            "http://127.0.0.1:3110/parcels,parcel_anchor_aggregate,parcel_anchor/11/1747/803",
            "http://127.0.0.1:3110/parcels,parcel_anchor_aggregate,parcel_anchor/14/13977/6426",
            "foundation_static",
            "vector_layers",
            "static TileJSON vector_layers fields differ",
            "static TileJSON bounds differ from the frozen build bbox",
            "static TileJSON center differs from the frozen build center",
            "flat_tile_count",
            "flat_tile_total_bytes",
            "summary_matches=",
            "sed -n '1s/.*:[[:space:]]*//p'",
            "DYNAMIC tile OK",
            "STATIC tile OK",
            "MATCHING features",
            "z11 static MVT bytes differ from dynamic",
            "z14 static MVT bytes differ from dynamic",
            "LOCAL PMTiles fallback",
            "REAL R2",
            "TILES_SLICE_USE_CANONICAL_TILE_R2",
            "REAL R2 via Martin S3 origin",
            "validate_r2_direct_bucket",
        ],
        "tile proof script",
    );

    require_all(
        &proof,
        &[
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCOUNT_ID",
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_ACCESS_KEY_ID",
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_SECRET_ACCESS_KEY",
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_BUCKET",
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_ENDPOINT",
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_READ_BASE_URL",
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_READ_URL",
            "FOUNDATION_PLATFORM_R2_TILE_PROOF_OBJECT_KEY",
            "declare -p \"$name\"",
            "validate_r2_test_bucket",
            "must be query-free for the contracted Martin",
            "--validate-r2-config-only",
            "protected_names=\"$(repository_protected_bucket_names)\"",
            "repository protected bucket SSOT is empty",
            "\"$bucket\" != *--*",
            "lakehouse_registry.rs",
            "FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_BUCKET",
            "must contain tiles-slice-proof",
            "tiles-slice-proof/",
            "source \"$HTTP_EVIDENCE_HELPER\"",
            "command curl --disable \"$@\"",
            "RAW_RESPONSE_HEADERS=()",
            "UNVERIFIED_RESPONSE_BODIES=()",
            "tiles_remove_http_artifacts \"${RAW_RESPONSE_HEADERS[@]}\" \"${UNVERIFIED_RESPONSE_BODIES[@]}\"",
            "--config -",
            "--aws-sigv4",
            "aws:amz:auto:s3",
            "If-None-Match: *",
            "x-amz-meta-sha256",
            "r2-put-headers.redacted.txt",
            "r2-head-headers.redacted.txt",
            "r2-range-headers.redacted.txt",
            "r2-public-readback.pmtiles",
            "r2-evidence.txt",
            "sha256sum",
            "readback_sha256",
            "[[ \"$readback_sha256\" == \"$archive_sha256\" ]]",
            "Content-Length",
            "Content-Range",
            "ETag",
            "Range: bytes=0-511",
            "head -c 512 \"$readback_path\" | cmp --silent - \"$ARTIFACT_DIR/r2-range-proof.bin\"",
            "206",
        ],
        "real-R2 safety branch",
    );
    assert!(
        proof.contains(
            "put_status=\"$(r2_signed_curl --silent --show-error --output /dev/null \\\n"
        ),
        "the proof must discard the unverified R2 PutObject response body"
    );
    assert!(
        !proof.contains("r2-put-response.txt"),
        "the proof must never retain an unverified R2 PutObject response body"
    );

    assert_eq!(
        proof.matches("command curl ").count(),
        1,
        "the proof must contain exactly one executable command-curl boundary: the clean_curl wrapper"
    );

    for line in proof.lines() {
        let code = line.trim_start();
        let bypasses_clean_curl = code.starts_with("curl ")
            || code.starts_with("if curl ")
            || code.contains("$(curl ")
            || code.contains("| curl ");
        assert!(
            !bypasses_clean_curl,
            "all proof HTTP calls must use the curl wrapper that disables user config: {code}"
        );
    }

    let full_readback = proof
        .find("r2-public-readback.pmtiles")
        .expect("the public R2 URL must be downloaded in full");
    let digest_check = proof
        .find("[[ \"$readback_sha256\" == \"$archive_sha256\" ]]")
        .expect("the public readback digest must equal the uploaded archive digest");
    let readback_retained = proof
        .find("unset 'UNVERIFIED_RESPONSE_BODIES[0]'")
        .expect("the full readback must become retainable only after verification");
    let martin_export = proof
        .find("export TILES_SLICE_PMTILES_URL=\"$R2_READ_OBJECT_URL\"")
        .expect("Martin must consume the verified public read URL");
    assert!(
        full_readback < digest_check
            && digest_check < readback_retained
            && readback_retained < martin_export,
        "full public readback verification must finish before Martin consumes the R2 URL"
    );

    let range_prefix_check = proof
        .find(
            "head -c 512 \"$readback_path\" | cmp --silent - \"$ARTIFACT_DIR/r2-range-proof.bin\"",
        )
        .expect("the Range response must match the verified archive prefix");
    let range_retained = proof
        .find("unset 'UNVERIFIED_RESPONSE_BODIES[1]'")
        .expect("the Range body must become retainable only after byte verification");
    assert!(
        range_prefix_check < range_retained,
        "the Range response must remain cleanup-eligible until its bytes are verified"
    );

    for forbidden in [
        "aws s3 rm",
        "rclone delete",
        "DeleteObject",
        "--request DELETE",
        "-X DELETE",
    ] {
        assert!(
            !proof.contains(forbidden),
            "proof must never contain an R2 deletion path: {forbidden}"
        );
    }
    assert!(
        !proof.contains("set -x"),
        "proof must not trace secret-bearing commands"
    );
    let xtrace_position = proof
        .find("set +x")
        .expect("proof must explicitly disable inherited xtrace");
    let r2_position = proof
        .find("configure_r2_mode")
        .expect("proof must configure the R2/local mode");
    assert!(
        xtrace_position < r2_position,
        "inherited xtrace must be disabled before any R2 values are inspected"
    );
    assert!(
        !proof.contains("--user \"$FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID:$FOUNDATION_PLATFORM_R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY\""),
        "R2 credentials must not be exposed in curl argv"
    );
    assert!(
        !proof.contains("$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET"),
        "the proof must never consume the generic production-capable bucket variable"
    );
    assert!(
        !proof.contains("done < <(repository_protected_bucket_names)"),
        "process-substitution status loss must not make protected-bucket loading fail open"
    );
    assert!(
        !proof.contains("tr -d '\\r\\n\\t '"),
        "TileJSON compaction must preserve JSON string spaces"
    );
}

#[test]
fn http_evidence_redacts_url_secrets_and_failure_cleanup_removes_unverified_artifacts() {
    let helper = repo_root().join("scripts/tiles/http-evidence.sh");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must follow the Unix epoch")
        .as_nanos();
    let isolated_root = std::env::temp_dir().join(format!(
        "tiles-http-evidence-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&isolated_root).expect("create isolated HTTP evidence directory");

    let raw_redirect = isolated_root.join("redirect.raw");
    let redacted = isolated_root.join("redirect.redacted.txt");
    fs::write(
        &raw_redirect,
        concat!(
            "HTTP/1.1 302 Found\r\n",
            "Location: https://tiles.invalid/a.pmtiles?X-Amz-Signature=FAKE_SIGNATURE\r\n",
            "Content-Location: https://tiles.invalid/a.pmtiles?token=FAKE_SIGNATURE\r\n",
            "Link: <https://tiles.invalid/a.pmtiles?sig=FAKE_SIGNATURE>; rel=alternate\r\n",
            "Refresh: 0; url=https://tiles.invalid/a.pmtiles?sig=FAKE_SIGNATURE\r\n",
            "Authorization: Bearer FAKE_SIGNATURE\r\n",
            "Set-Cookie: proof=FAKE_SIGNATURE\r\n",
            "X-Amz-Security-Token: FAKE_SIGNATURE\r\n",
            "ETag: \"safe-etag\"\r\n",
            "Content-Length: 512\r\n",
            "\r\n"
        ),
    )
    .expect("write redirect headers containing fake secrets");

    let redaction = bash_command()
        .args([
            "-ceu",
            "source \"$1\"; tiles_redact_response_headers \"$2\" \"$3\"",
            "_",
            &bash_path(&helper),
            &bash_path(&raw_redirect),
            &bash_path(&redacted),
        ])
        .output()
        .expect("run the production response-header redactor");
    assert!(redaction.status.success(), "{}", output_text(&redaction));
    assert!(
        !raw_redirect.exists(),
        "redaction must remove the raw response headers"
    );
    let retained = fs::read_to_string(&redacted).expect("read redacted response headers");
    let retained_lower = retained.to_ascii_lowercase();
    assert!(retained.contains("ETag: \"safe-etag\""));
    assert!(retained.contains("Content-Length: 512"));
    for forbidden in [
        "fake_signature",
        "location:",
        "content-location:",
        "link:",
        "refresh:",
        "authorization:",
        "set-cookie:",
        "x-amz-security-token:",
    ] {
        assert!(
            !retained_lower.contains(forbidden),
            "retained evidence leaked forbidden response header content: {forbidden}"
        );
    }

    let failed_raw = isolated_root.join("failed-curl.raw");
    let failed_body = isolated_root.join("failed-curl.body");
    fs::write(
        &failed_raw,
        "Location: https://tiles.invalid/a.pmtiles?X-Amz-Signature=FAKE_SIGNATURE\r\n",
    )
    .expect("write raw headers from a simulated failed curl");
    fs::write(
        &failed_body,
        "origin reflected X-Amz-Signature=FAKE_SIGNATURE",
    )
    .expect("write a simulated unverified curl error body");
    let failed = bash_command()
        .args([
            "-ceu",
            "source \"$1\"; trap 'tiles_remove_http_artifacts \"$2\" \"$3\"' EXIT; exit 7",
            "_",
            &bash_path(&helper),
            &bash_path(&failed_raw),
            &bash_path(&failed_body),
        ])
        .output()
        .expect("simulate a nonzero curl exit after raw headers were written");
    assert_eq!(failed.status.code(), Some(7), "{}", output_text(&failed));
    assert!(
        !failed_raw.exists(),
        "EXIT cleanup must remove raw headers after a nonzero curl result"
    );
    assert!(
        !failed_body.exists(),
        "EXIT cleanup must remove an unverified response body after a nonzero curl result"
    );

    fs::remove_dir_all(&isolated_root).expect("remove isolated HTTP evidence directory");
}

#[test]
fn root_tile_contract_inputs_trigger_their_authoritative_ci_lanes() {
    let foundation_ci = read(".github/workflows/foundation-ci.yml");
    let gongzzang_frontend_ci = read(".github/workflows/gongzzang-frontend.yml");

    assert_eq!(
        foundation_ci.matches("- \"scripts/tiles/**\"").count(),
        1,
        "Foundation CI must watch tile proof/config inputs on main pushes; pull requests are unfiltered"
    );
    assert_eq!(
        foundation_ci
            .matches("- \"scripts/verify/integration.sh\"")
            .count(),
        1,
        "Foundation CI must watch its disposable-DB integration provisioner"
    );
    assert_eq!(
        foundation_ci
            .matches("- \".github/workflows/gongzzang-frontend.yml\"")
            .count(),
        1,
        "Foundation CI must rerun the cross-workflow tile-contract guard when Gongzzang CI routing changes"
    );
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
    assert_eq!(
        gongzzang_frontend_ci
            .matches("- \"scripts/tiles/vector-tile-manifest.local.json\"")
            .count(),
        1,
        "Gongzzang frontend CI must watch the root manifest consumed by its Vitest contract"
    );
}

#[test]
fn canonical_foundation_migrator_is_the_only_schema_executor() {
    let proof = read("scripts/tiles/tiles-slice-proof.sh");
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
    require_all(
        &proof,
        &[
            "--volume \"$REPO_HOST_PATH:/work:ro\"",
            "--volume perfectory-target-platforms-foundation-platform:/work/platforms/foundation-platform/target",
            "--workdir /work/platforms/foundation-platform",
        ],
        "tile proof canonical Cargo cache mount",
    );
    assert!(
        !proof.contains(
            "perfectory-target-platforms-foundation-platform:/workspace/platforms/foundation-platform/target"
        ),
        "one named Cargo target must never be reused under two container source roots"
    );
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

    let contract = read("platforms/foundation-platform/config/r2-connections.contract.json");
    let runbook =
        read("platforms/foundation-platform/docs/runbooks/tiles-object-storage-first-slice.md");

    // The runbook abbreviates the prefix as `...` after naming it in full, so both spellings are
    // resolved to the same full variable name before the comparison.
    let mut named = BTreeSet::new();
    for (marker, cut) in [(PREFIX, 0), ("`..._", 1)] {
        let mut rest = runbook.as_str();
        while let Some(start) = rest.find(marker) {
            let tail = &rest[start + marker.len() - cut..];
            let suffix: String = tail
                .chars()
                .take_while(|character| character.is_ascii_uppercase() || *character == '_')
                .collect();
            let suffix = suffix.trim_start_matches('_').trim_end_matches('_');
            if !suffix.is_empty() && suffix != "*" {
                named.insert(format!("{PREFIX}{suffix}"));
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

#[test]
fn r2_preflight_fails_closed_without_bucket_ssot_and_for_protected_bucket() {
    let script = repo_root().join("scripts/tiles/tiles-slice-proof.sh");

    let valid = r2_validation_output(&script, "perfectory-tiles-slice-proof-test");
    assert!(valid.status.success(), "{}", output_text(&valid));
    assert!(
        String::from_utf8_lossy(&valid.stdout).contains("R2 configuration validation OK"),
        "{}",
        output_text(&valid)
    );
    assert!(!output_text(&valid).contains("FAKESECRET"));

    let protected = r2_validation_output(&script, "foundation-platform-lakehouse-prod");
    assert!(!protected.status.success(), "{}", output_text(&protected));
    assert!(
        String::from_utf8_lossy(&protected.stderr).contains("repository-declared protected bucket"),
        "{}",
        output_text(&protected)
    );
    assert!(!output_text(&protected).contains("FAKESECRET"));

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must follow the Unix epoch")
        .as_nanos();
    let isolated_root = std::env::temp_dir().join(format!(
        "tiles-slice-proof-missing-ssot-{}-{nonce}",
        std::process::id()
    ));
    let isolated_script = isolated_root.join("scripts/tiles/tiles-slice-proof.sh");
    let isolated_helper = isolated_root.join("scripts/tiles/http-evidence.sh");
    fs::create_dir_all(
        isolated_script
            .parent()
            .expect("isolated script must have a parent"),
    )
    .expect("create isolated proof-script directory");
    fs::copy(&script, &isolated_script).expect("copy proof script without bucket SSOT files");
    fs::copy(
        repo_root().join("scripts/tiles/http-evidence.sh"),
        &isolated_helper,
    )
    .expect("copy the proof script's HTTP evidence helper");

    let missing = r2_validation_output(&isolated_script, "perfectory-tiles-slice-proof-test");
    let missing_text = output_text(&missing);
    fs::remove_dir_all(&isolated_root).expect("remove isolated proof-script directory");

    assert!(!missing.status.success(), "{missing_text}");
    assert!(
        String::from_utf8_lossy(&missing.stderr)
            .contains("repository bucket SSOT files are missing"),
        "{missing_text}"
    );
    assert!(!missing_text.contains("FAKESECRET"));
}
