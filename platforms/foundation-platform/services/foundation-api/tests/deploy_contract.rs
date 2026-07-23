//! Independent Foundation build and deploy contract.

use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use sha2::{Digest, Sha256};

type TestResult = Result<(), Box<dyn Error>>;

/// The Foundation CI workflow moved from the workspace-local
/// `.github/workflows/ci.yml` to the monorepo root as
/// `.github/workflows/foundation-ci.yml`, so it is reached by climbing out of
/// the workspace root that [`read_repo_file`] resolves against. (Secret
/// scanning moved to the root `secret-scan.yml` workflow; the ported
/// Foundation workflow carries no gitleaks steps.)
const FOUNDATION_CI_WORKFLOW: &str = "../../.github/workflows/foundation-ci.yml";

#[test]
fn compose_builds_migrates_and_runs_the_foundation_api_with_separate_roles() -> TestResult {
    let compose = read_repo_file("docker-compose.yml")?;

    for required in [
        "foundation-migrate:\n",
        "foundation-runtime-grants:\n",
        "foundation-api:\n",
        "foundation_migrator:${FOUNDATION_MIGRATOR_PASSWORD:?set FOUNDATION_MIGRATOR_PASSWORD}",
        "foundation_api:${FOUNDATION_API_PASSWORD:?set FOUNDATION_API_PASSWORD}",
        "dockerfile: services/foundation-api/Dockerfile",
        "127.0.0.1:${FOUNDATION_REDIS_PORT:-16379}:6379",
        "healthcheck:\n",
    ] {
        assert!(compose.contains(required), "Compose is missing {required}");
    }
    assert_eq!(
        compose
            .matches("dockerfile: services/foundation-api/Dockerfile")
            .count(),
        1,
        "the shared runtime image must have exactly one Compose builder"
    );
    assert!(!compose.contains("foundation_platform_dev_2026"));
    Ok(())
}

#[test]
fn long_running_runtime_services_restart_after_docker_daemon_recovery() -> TestResult {
    let compose = read_repo_file("docker-compose.yml")?;
    let lakehouse = read_repo_file("compose.lakehouse.yml")?;

    for (service, document) in [
        ("foundation-api", compose.as_str()),
        ("spark", lakehouse.as_str()),
    ] {
        let header = format!("  {service}:");
        let service_contract = document
            .lines()
            .skip_while(|line| *line != header)
            .skip(1)
            .take_while(|line| !line.starts_with("  ") || line.starts_with("    "))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            service_contract.contains("restart: unless-stopped"),
            "{service} must recover after a Docker daemon restart"
        );
    }
    Ok(())
}

#[test]
fn mutable_lakehouse_state_is_release_independent_and_prepared_for_runtime_uid() -> TestResult {
    let lakehouse = read_repo_file("compose.lakehouse.yml")?;
    let runtime = read_repo_file("scripts/deploy/foundation-runtime.sh")?;
    let release = read_repo_file("scripts/deploy/foundation-release.sh")?;
    let runbook =
        read_repo_file("docs/runbooks/foundation-platform-low-cost-production-hardening.md")?;

    assert!(lakehouse.contains(
        "${FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT:-./target/lakehouse}:/workspace/target/lakehouse"
    ));
    assert!(lakehouse.contains(
        "${FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_STATE_ROOT:-./target/remote-lakehouse}:/workspace/target/remote-lakehouse"
    ));
    for required in [
        "FOUNDATION_PLATFORM_STATE_ROOT",
        "FOUNDATION_PLATFORM_LAKEHOUSE_STATE_ROOT",
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_STATE_ROOT",
    ] {
        assert!(
            runtime.contains(required),
            "runtime wrapper is missing {required}"
        );
    }
    for required in [
        "prepare_mutable_state",
        "${state_root}/lakehouse",
        "${state_root}/remote-lakehouse",
        "FOUNDATION_PLATFORM_LAKEHOUSE_UID",
        "FOUNDATION_PLATFORM_LAKEHOUSE_GID",
    ] {
        assert!(
            release.contains(required),
            "release script is missing {required}"
        );
    }
    for required in [
        "/var/lib/foundation-platform/lakehouse",
        "/var/lib/foundation-platform/remote-lakehouse",
        "FOUNDATION_PLATFORM_COMPOSE_PROJECT=foundation-platform-compute",
    ] {
        assert!(
            runbook.contains(required),
            "production runbook is missing {required}"
        );
    }
    Ok(())
}

#[test]
fn api_image_is_locked_non_root_and_health_checked() -> TestResult {
    let dockerfile = read_repo_file("services/foundation-api/Dockerfile")?;

    assert!(dockerfile.contains("cargo build --locked --release"));
    assert!(dockerfile.contains("USER 10001:10001"));
    assert!(dockerfile.contains("HEALTHCHECK"));
    assert!(dockerfile.contains("foundation-migrate"));
    assert!(dockerfile
        .lines()
        .filter(|line| line.starts_with("FROM "))
        .all(|line| line.contains("@sha256:")));
    assert!(!dockerfile.contains("apt-get install"));
    assert!(!dockerfile.contains("COPY . ."));
    assert!(!dockerfile.contains("latest"));
    assert!(!dockerfile.contains("FROM busybox:"));
    assert!(!dockerfile.contains("/usr/local/bin/busybox"));
    assert!(dockerfile.contains("CMD [\"/usr/local/bin/foundation-api\", \"--healthcheck\"]"));
    Ok(())
}

#[test]
fn all_docker_and_compose_images_are_immutable_and_helpers_are_non_root() -> TestResult {
    for dockerfile in [
        "services/foundation-api/Dockerfile",
        "services/foundation-outbox-publisher/Dockerfile.lakehouse-control",
        "services/foundation-provider-acquisition-worker/Dockerfile.raon-agent-proof",
        "services/foundation-provider-acquisition-worker/Dockerfile.raon-batch",
    ] {
        let contents = read_repo_file(dockerfile)?;
        assert!(
            contents
                .lines()
                .filter(|line| line.starts_with("FROM "))
                .all(|line| line.contains("@sha256:")),
            "{dockerfile} has a mutable FROM"
        );
        assert_versioned_apt_installs(dockerfile, &contents);
    }

    let compose = read_repo_file("docker-compose.yml")?;
    let lakehouse_compose = read_repo_file("compose.lakehouse.yml")?;
    let observability_compose = read_repo_file("compose.observability.yml")?;
    for image in [
        compose.as_str(),
        lakehouse_compose.as_str(),
        observability_compose.as_str(),
    ]
    .into_iter()
    .flat_map(str::lines)
    .map(str::trim)
    .filter_map(|line| line.strip_prefix("image: "))
    {
        assert!(
            image.ends_with(":local") || image.contains("@sha256:"),
            "mutable Compose image: {image}"
        );
    }
    assert_eq!(compose.matches("user: \"70:70\"").count(), 3);

    let provider_lock =
        read_repo_file("services/foundation-provider-acquisition-worker/requirements.lock")?;
    assert!(provider_lock.contains("--hash=sha256:"));
    for dockerfile in [
        "services/foundation-provider-acquisition-worker/Dockerfile.raon-agent-proof",
        "services/foundation-provider-acquisition-worker/Dockerfile.raon-batch",
    ] {
        let contents = read_repo_file(dockerfile)?;
        assert!(contents.contains("requirements.lock"));
        assert!(contents.contains("--require-hashes"));
        assert!(contents.contains("--no-deps"));
        assert!(!contents.contains("pip install --no-cache-dir --upgrade pip"));
        assert!(!contents.contains("if python -m patchright install chromium"));
    }

    let lakehouse_control =
        read_repo_file("services/foundation-outbox-publisher/Dockerfile.lakehouse-control")?;
    assert!(lakehouse_control.contains("USER 10001:10001"));
    assert!(!compose.contains("chown -R"));
    assert!(!lakehouse_compose.contains("chown -R"));
    let lakehouse_user_contract = [
        "user: \"$",
        "{FOUNDATION_PLATFORM_LAKEHOUSE_UID:-185}:$",
        "{FOUNDATION_PLATFORM_LAKEHOUSE_GID:-185}\"",
    ]
    .concat();
    assert!(lakehouse_compose.contains(&lakehouse_user_contract));

    let smoke = read_repo_file("scripts/compose-smoke.sh")?;
    for service in [
        "foundation-bootstrap",
        "foundation-migrate",
        "foundation-runtime-grants",
        "foundation-finalize",
    ] {
        assert!(
            smoke.contains(format!("run_uid_probe {service}").as_str()),
            "Compose smoke does not prove non-root execution for {service}"
        );
    }
    Ok(())
}

#[test]
fn observability_compose_scrapes_routes_and_persists_foundation_alerts() -> TestResult {
    let root = read_repo_file("docker-compose.yml")?;
    assert!(root.contains("- path: compose.observability.yml"));

    let compose = read_repo_file("compose.observability.yml")?;
    for required in [
        "prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996",
        "prom/alertmanager:v0.28.1@sha256:27c475db5fb156cab31d5c18a4251ac7ed567746a2483ff264516437a39b15ba",
        "127.0.0.1:${FOUNDATION_PROMETHEUS_PORT:-19090}:9090",
        "127.0.0.1:${FOUNDATION_ALERTMANAGER_PORT:-19093}:9093",
        "read_only: true",
        "no-new-privileges:true",
        "prometheus_data:/prometheus",
        "alertmanager_data:/alertmanager",
    ] {
        assert!(compose.contains(required), "observability Compose is missing {required}");
    }

    let prometheus = read_repo_file("infra/observability/prometheus/prometheus.yml")?;
    assert!(prometheus.contains("foundation-api:8080"));
    assert!(prometheus.contains("alertmanager:9093"));
    assert!(prometheus.contains("foundation-api.rules.yml"));

    let alertmanager = read_repo_file("infra/observability/alertmanager/alertmanager.yml")?;
    assert!(alertmanager.contains("receiver: prelaunch-audit"));

    let rules = read_repo_file("infra/observability/prometheus/foundation-api.rules.yml")?;
    assert!(rules.contains("foundation_api_up != 1 or absent(foundation_api_up)"));
    Ok(())
}

#[test]
fn production_runtime_entrypoint_cannot_drop_the_recovery_overlay() -> TestResult {
    let runtime = read_repo_file("scripts/deploy/foundation-runtime.sh")?;
    for required in [
        "--project-directory \"${root_dir}\"",
        "-f \"${root_dir}/docker-compose.yml\"",
        "-f \"${root_dir}/compose.recovery.yml\"",
        "--project-name \"${FOUNDATION_PLATFORM_COMPOSE_PROJECT}\"",
        "--env-file \"${FOUNDATION_PLATFORM_ENV_FILE}\"",
    ] {
        assert!(
            runtime.contains(required),
            "production runtime entrypoint is missing {required}"
        );
    }

    let runbook =
        read_repo_file("docs/runbooks/foundation-platform-low-cost-production-hardening.md")?;
    assert!(runbook.contains("scripts/deploy/foundation-runtime.sh up"));
    assert!(!runbook.contains("docker compose --project-name foundation-platform-runtime"));
    Ok(())
}

#[test]
fn lakehouse_compose_is_an_independent_compute_boundary() -> TestResult {
    let compose = read_repo_file("docker-compose.yml")?;
    let lakehouse_compose = read_repo_file("compose.lakehouse.yml")?;

    assert!(compose.contains("include:\n  - path: compose.lakehouse.yml"));
    for service in [
        "trino",
        "lakehouse-target-init",
        "spark",
        "lakehouse-control",
    ] {
        let service_key = format!("  {service}:\n");
        assert!(
            !compose.contains(&service_key),
            "{service} is duplicated in the default Compose file"
        );
        assert!(
            lakehouse_compose.contains(&service_key),
            "{service} is missing from the lakehouse Compose boundary"
        );
    }
    for unrelated_secret in [
        "FOUNDATION_ADMIN_PASSWORD",
        "FOUNDATION_MIGRATOR_PASSWORD",
        "FOUNDATION_API_PASSWORD",
    ] {
        assert!(
            !lakehouse_compose.contains(unrelated_secret),
            "lakehouse Compose requires unrelated runtime secret {unrelated_secret}"
        );
    }

    Ok(())
}

#[test]
fn lakehouse_runtime_user_contract_has_one_ssot() -> TestResult {
    let contract_files = [
        "compose.lakehouse.yml",
        ".env.example",
        ".env.local.example",
        "docs/runbooks/lakehouse-compute-engines.md",
        "services/foundation-outbox-publisher/src/remote_lakehouse_job.rs",
    ];

    for path in contract_files {
        let contents = read_repo_file(path)?;
        assert!(
            contents.contains("FOUNDATION_PLATFORM_LAKEHOUSE_UID"),
            "{path} does not use the lakehouse UID contract"
        );
        assert!(
            contents.contains("FOUNDATION_PLATFORM_LAKEHOUSE_GID"),
            "{path} does not use the lakehouse GID contract"
        );
        for legacy in [
            "FOUNDATION_PLATFORM_HOST_UID",
            "FOUNDATION_PLATFORM_HOST_GID",
            "FOUNDATION_PLATFORM_SPARK_UID",
            "FOUNDATION_PLATFORM_SPARK_GID",
        ] {
            assert!(
                !contents.contains(legacy),
                "{path} retains split lakehouse ownership variable {legacy}"
            );
        }
    }

    Ok(())
}

#[test]
fn compose_smoke_requires_terminal_zero_exit_before_postconditions() -> TestResult {
    let smoke = read_repo_file("scripts/compose-smoke.sh")?;
    for required in [
        "ONE_SHOT_TIMEOUT_SECONDS",
        "timeout --foreground",
        "run --rm -T --no-deps",
        "--interactive=false",
        "state=exited exit=0 postcondition=PASS",
        "reason=helper_timeout_or_exit",
        "postcondition",
        "id -u",
    ] {
        assert!(
            smoke.contains(required),
            "Compose smoke is missing {required}"
        );
    }
    assert!(!smoke.contains("state=retained postcondition=PASS"));
    assert!(!smoke.contains("run -d --no-deps"));

    let ci = read_repo_file(FOUNDATION_CI_WORKFLOW)?;
    assert!(ci.contains("scripts/compose-smoke.sh -- start-api"));
    assert!(ci.contains("scripts/compose-smoke.sh -- rerun-api"));
    Ok(())
}

#[test]
fn api_health_probes_use_the_native_command_and_bind_aware_listener() -> TestResult {
    let compose = read_repo_file("docker-compose.yml")?;
    assert!(compose.contains("[\"CMD\", \"/usr/local/bin/foundation-api\", \"--healthcheck\"]"));
    assert!(!compose.contains("/usr/local/bin/busybox"));

    let runtime = read_repo_file("services/foundation-api/src/lib.rs")?;
    assert!(runtime.contains("healthcheck_address(bind_addr_from_env()?)"));
    assert!(runtime.contains("address.is_unspecified()"));
    for path in ["scripts/compose-smoke.sh", FOUNDATION_CI_WORKFLOW] {
        let contents = read_repo_file(path)?;
        assert!(
            contents.contains("State.Health.Status"),
            "{path} does not consume the native Foundation API health result"
        );
        assert!(!contents.contains("/usr/local/bin/busybox"));
    }
    Ok(())
}

#[test]
fn compose_smoke_provisions_a_bounded_oidc_verifier_fixture() -> TestResult {
    let compose = read_repo_file("docker-compose.yml")?;
    for required in [
        "foundation-oidc-smoke:\n",
        "foundation-api-smoke:\n",
        "profiles:\n      - compose-smoke",
        "busybox:1.36@sha256:",
        "user: \"65534:65534\"",
        "./infra/compose/oidc-smoke:/www:ro",
        "network_mode: \"service:foundation-oidc-smoke\"",
        "IDENTITY_API_BASE_URL: http://127.0.0.1:18082",
    ] {
        assert!(compose.contains(required), "Compose is missing {required}");
    }

    let discovery = read_repo_file("infra/compose/oidc-smoke/.well-known/openid-configuration")?;
    let discovery: serde_json::Value = serde_json::from_str(&discovery)?;
    assert_eq!(discovery["issuer"], "http://127.0.0.1:18081");
    assert_eq!(discovery["jwks_uri"], "http://127.0.0.1:18081/jwks.json");
    let jwks = read_repo_file("infra/compose/oidc-smoke/jwks.json")?;
    let jwks: serde_json::Value = serde_json::from_str(&jwks)?;
    assert_eq!(jwks["keys"][0]["alg"], "RS256");

    let smoke = read_repo_file("scripts/compose-smoke.sh")?;
    assert!(smoke.contains("wait_oidc_fixture"));
    assert!(smoke.contains("oidc_fixture_issuer=\"http://127.0.0.1:18081\""));
    let ci = read_repo_file(FOUNDATION_CI_WORKFLOW)?;
    assert!(ci.contains("ZITADEL_ISSUER_URL: http://127.0.0.1:18081"));
    Ok(())
}

#[test]
fn migration_database_create_is_open_only_for_the_migration_window() -> TestResult {
    let bootstrap = read_repo_file("infra/compose/bootstrap-foundation.sql")?;
    let finalize = read_repo_file("infra/compose/finalize-foundation.sql")?;

    assert!(bootstrap.contains("GRANT CREATE ON DATABASE foundation TO foundation_migrator"));
    assert!(finalize.contains("REVOKE CREATE ON DATABASE foundation FROM foundation_migrator"));
    Ok(())
}

#[test]
fn examples_and_ci_cover_independent_foundation_deployability() -> TestResult {
    let example = read_repo_file(".env.example")?;
    for required in [
        "FOUNDATION_ADMIN_PASSWORD=REPLACE_WITH_",
        "FOUNDATION_MIGRATOR_PASSWORD=REPLACE_WITH_",
        "FOUNDATION_API_PASSWORD=REPLACE_WITH_",
        "DATABASE_URL=postgres://foundation_api:",
        "IDENTITY_API_BASE_URL=",
        "FOUNDATION_PLATFORM_ZITADEL_AUDIENCE=foundation-api",
    ] {
        assert!(
            example.contains(required),
            ".env.example is missing {required}"
        );
    }
    let local_example = read_repo_file(".env.local.example")?;
    for required in [
        "DATABASE_URL=postgres://foundation_api:",
        "IDENTITY_API_BASE_URL=",
        "ZITADEL_ISSUER_URL=",
        "FOUNDATION_PLATFORM_ZITADEL_AUDIENCE=",
    ] {
        assert!(
            local_example.contains(required),
            ".env.local.example is missing {required}"
        );
    }

    let ci = read_repo_file(FOUNDATION_CI_WORKFLOW)?;
    for required in [
        // ADR-0004: full-workspace fmt/clippy/test verification is owned by the
        // single SSOT command (xtask clippy --all-targets + test --workspace
        // --all-features compiles+checks the whole workspace); the standalone
        // `cargo build --locked --workspace` step was retired as redundant.
        "cargo xtask verify foundation",
        "scripts/compose-smoke.sh -- start-api",
        "docker compose down -v --remove-orphans",
    ] {
        assert!(ci.contains(required), "CI is missing {required}");
    }
    Ok(())
}

#[test]
fn postgres_recovery_contract_is_encrypted_bounded_and_rehearsable() -> TestResult {
    assert_postgres_recovery_compose_contract()?;
    assert_pgbackrest_repository_contract()?;
    assert_postgres_restore_drill_contract()?;
    assert_postgres_backup_scheduler_contract()?;
    assert_postgres_recovery_lock_boundary()?;
    Ok(())
}

fn assert_postgres_recovery_compose_contract() -> TestResult {
    let compose = read_repo_file("compose.recovery.yml")?;
    for required in [
        "foundation-postgres-recovery:local",
        "dockerfile: infra/postgres/Dockerfile.recovery",
        "pgbackrest --stanza=foundation archive-push %p",
        "archive_mode=on",
        "archive_timeout=60s",
        "foundation-backup:",
        "foundation-restore-drill:",
        "pgdata:/var/lib/postgresql/data:ro",
        "recovery_data:/var/lib/postgresql/data",
        "FOUNDATION_RECOVERY_R2_BUCKET",
        "FOUNDATION_RECOVERY_REPOSITORY_CIPHER_PASS",
    ] {
        assert!(
            compose.contains(required),
            "recovery Compose is missing {required}"
        );
    }
    assert!(!compose.contains("${R2_SECRET_ACCESS_KEY"));
    assert!(!compose.contains("latest"));

    let dockerfile = read_repo_file("infra/postgres/Dockerfile.recovery")?;
    assert!(dockerfile.contains("pgbackrest=2.58.0-r0"));
    assert!(dockerfile.contains(
        "COPY --chmod=0755 infra/postgres/recovery-entrypoint.sh /usr/local/bin/foundation-postgres-recovery-entrypoint"
    ));
    assert!(dockerfile
        .lines()
        .filter(|line| line.starts_with("FROM "))
        .all(|line| line.contains("@sha256:")));
    Ok(())
}

fn assert_pgbackrest_repository_contract() -> TestResult {
    let config = read_repo_file("infra/postgres/pgbackrest.conf")?;
    for required in [
        "repo1-type=s3",
        "pg1-user=foundation_admin",
        "repo1-s3-uri-style=path",
        "repo1-cipher-type=aes-256-cbc",
        "repo1-retention-full=35",
        "repo1-retention-full-type=time",
        "repo1-bundle=y",
        "repo1-block=y",
        "start-fast=y",
    ] {
        assert!(
            config.contains(required),
            "pgBackRest config is missing {required}"
        );
    }
    assert!(!config.contains("repo1-s3-key="));
    assert!(!config.contains("repo1-cipher-pass="));
    Ok(())
}

fn assert_postgres_restore_drill_contract() -> TestResult {
    let drill = read_repo_file("scripts/recovery/postgres-restore-drill.sh")?;
    for required in [
        "set -Eeuo pipefail",
        "trap cleanup EXIT",
        "local exit_code=$?",
        "logs --no-color",
        "failure-compose.log",
        "pgbackrest-backup check",
        "pgbackrest-backup backup --type=full",
        "pgbackrest-restore restore --type=name",
        "pg_create_restore_point",
        "recovery_probe",
        "pitr_smoke",
        "migration_state",
        "read_smoke",
        "evidence.json",
    ] {
        assert!(
            drill.contains(required),
            "restore drill is missing {required}"
        );
    }
    assert!(drill.contains("NOT IN ('pg_catalog', 'information_schema')"));
    assert!(drill.contains("recovery_drill.recovery_probe"));
    assert!(!drill.contains("catalog.recovery_probe"));
    assert!(!drill.contains("--type=time"));
    assert!(drill.contains("up -d --wait postgres"));
    for service in [
        "foundation-bootstrap",
        "foundation-migrate",
        "foundation-runtime-grants",
        "foundation-finalize",
        "foundation-backup",
    ] {
        let invocation = format!("run --rm -T --interactive=false --no-deps {service}");
        assert!(
            drill.contains(&invocation),
            "restore drill must isolate {service}"
        );
    }
    assert!(drill.contains("recovery run id must be 14 UTC digits"));
    assert!(drill.contains("-c \"INSERT INTO recovery_drill.recovery_probe"));
    assert!(!drill.contains("| \"${source_psql[@]}\""));
    assert!(!drill.contains("set -x"));
    Ok(())
}

fn assert_postgres_backup_scheduler_contract() -> TestResult {
    let backup = read_repo_file("scripts/recovery/run-postgres-backup.sh")?;
    for required in [
        "stanza-create",
        "pgbackrest-backup check",
        "backup_type=full",
        "backup_type=diff",
        "pgbackrest-backup expire",
        "backup-info.json",
    ] {
        assert!(
            backup.contains(required),
            "backup runner is missing {required}"
        );
    }
    assert_eq!(
        backup.matches("run --rm -T --interactive=false --no-deps foundation-backup").count(),
        7,
        "every scheduled backup operation must attach to the deployed database without starting dependencies"
    );
    assert!(!backup.contains("set -x"));

    let timer = read_repo_file("infra/systemd/foundation-postgres-backup.timer")?;
    assert!(timer.contains("OnCalendar=*-*-* 02:15:00"));
    assert!(timer.contains("Persistent=true"));
    assert!(timer.contains("RandomizedDelaySec=15m"));
    let service = read_repo_file("infra/systemd/foundation-postgres-backup.service")?;
    assert!(service.contains("EnvironmentFile=/etc/foundation-platform/recovery.env"));
    assert!(service.contains("TimeoutStartSec=6h"));
    assert!(service.contains("WorkingDirectory=/opt/foundation-platform/current"));
    assert!(service.contains(
        "ExecStart=/opt/foundation-platform/current/scripts/recovery/run-postgres-backup.sh"
    ));
    assert!(service.contains("ReadWritePaths=/var/lib/foundation-platform/recovery"));

    let release = read_repo_file("scripts/deploy/foundation-release.sh")?;
    for required in [
        "FOUNDATION_PLATFORM_RELEASE_ROOT",
        "FOUNDATION_PLATFORM_STATE_ROOT",
        "releases",
        "current",
        "previous",
        "install",
        "activate",
        "rollback",
        ".foundation-release-id",
        ".foundation-release-archive-sha256",
    ] {
        assert!(
            release.contains(required),
            "atomic release script is missing {required}"
        );
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let release_test = Command::new("bash")
        .arg("scripts/deploy/foundation-release-test.sh")
        .current_dir(root)
        .output()?;
    assert!(
        release_test.status.success(),
        "release test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&release_test.stdout),
        String::from_utf8_lossy(&release_test.stderr)
    );
    assert!(String::from_utf8_lossy(&release_test.stdout).contains("foundation-release-test=pass"));
    Ok(())
}

fn assert_postgres_recovery_lock_boundary() -> TestResult {
    assert_single_bucket_lock_rule(
        "infra/cloudflare/foundation-platform-lakehouse-prod.bucket-lock.json",
        "bronze-raw-30-days",
        "bronze/",
        2_592_000,
    )?;
    let cloudflare_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("infra/cloudflare");
    for forbidden in [
        "foundation-platform-r2-locks.json",
        "foundation-platform-postgres-recovery-prod.bucket-lock.json",
    ] {
        assert!(
            !cloudflare_dir.join(forbidden).exists(),
            "{forbidden} must not lock pgBackRest's mutable repository metadata"
        );
    }

    Ok(())
}

const SIGNED_OIDC_SMOKE_REQUIRED_CONTRACT: &[&str] = &[
    "set +x",
    "trap cleanup EXIT",
    "TASK9_SMOKE_TEMP_ROOT",
    "${FOUNDATION_CHECKOUT}/target",
    "export DOCKER_CONFIG=",
    "{\"auths\":{}}",
    "CURL_CONNECT_TIMEOUT_SECONDS",
    "CURL_MAX_TIME_SECONDS",
    "DOCKER_COMMAND_TIMEOUT_SECONDS",
    "PGCONNECT_TIMEOUT",
    "LOGIN_PAGE_ATTEMPTS",
    "MACHINE_TOKEN_ATTEMPTS",
    "ZITADEL_PROJECTION_ATTEMPTS",
    "zitadel_host_relay",
    "zitadel_internal_port=8080",
    "bounded_curl",
    "bounded_docker",
    "ghcr.io/zitadel/zitadel:v2.65.1@sha256:",
    "postgres:17-alpine@sha256:",
    "principal_kind",
    "issuer=\"http://127.0.0.1:",
    "--user 1000:1000",
    "${temp_dir}/admin.pat:/pat-out/pat",
    "--user 65534:65534",
    "--network \"container:${identity_container_id}\"",
    "--network \"container:${foundation_container_id}\"",
    "stage=\"identity_zitadel_relay_restart\"",
    "/oauth/v2/device_authorization",
    "zcurl -L -c",
    "device_interval=\"$(validate_device_interval",
    "validate_device_interval",
    "zcurl_projection_retry",
    "Errors.User.Machine.Secret.NotExisting",
    "Errors.User.NotFound",
    "Errors.Project.NotFound",
    "ports: !override",
    "compose port",
    "for login_page_attempt in $(seq 1 \"${LOGIN_PAGE_ATTEMPTS}\")",
    "sleep \"${device_interval}\"",
    "/oauth/v2/token",
    "/oauth/v2/keys",
    "assertion_1",
    "assertion_2",
    "assertion_3",
    "assertion_4",
    "assertion_5",
    "assertion_6",
    "assertion_7",
    "assertion_8",
    "expect_status 503 \"${status}\"",
    "expiry_ordered=1 expiry_matches_token=1 stored_jti=1",
    "expected_exp",
    "IDENTITY_RUNTIME_IMAGE",
    "FOUNDATION_RUNTIME_IMAGE",
    "ZITADEL_ISSUER_URL=\"${issuer}\"",
    "IDENTITY_API_BASE_URL=\"http://127.0.0.1:${identity_relay_port}\"",
    "docker image rm -f",
    "docker compose",
    "down --volumes --remove-orphans",
    "label=com.docker.compose.project=",
    "force_remove_compose_project",
    "task9_resources_remain",
    "cleanup_failed=1",
    "identity_provisioner_container",
    "identity_cross_probe_container",
    "foundation_cross_probe_container",
    "127.0.0.1::5432",
    "127.0.0.1::6379",
    "stage=\"zitadel_management_ready\"",
    "/management/v1/projects/_search",
    "i_compose up -d --no-deps identity-api >/dev/null 2>&1",
];

#[test]
fn signed_oidc_smoke_is_disposable_secret_safe_and_covers_all_boundaries() -> TestResult {
    let smoke = read_repo_file("scripts/smoke/identity-foundation-signed-oidc.sh")?;

    for required in SIGNED_OIDC_SMOKE_REQUIRED_CONTRACT {
        assert!(
            smoke.contains(required),
            "signed OIDC smoke is missing {required}"
        );
    }
    assert!(!smoke.contains("set -x"));
    assert!(smoke.contains("human_password=\"Aa1!$(random_hex 18)\""));
    assert!(smoke.contains("ZITADEL_FIRSTINSTANCE_ORG_HUMAN_PASSWORD=${human_password}"));
    assert!(!smoke.contains("Authorization: Bearer ${"));
    assert!(!smoke.contains("identity-platform-runtime:local"));
    assert!(!smoke.contains("foundation-platform-runtime:local"));
    assert!(!smoke.contains("zitadel_pat_volume"));
    assert!(!smoke.contains("chown 1000:1000 /pat-out"));
    assert!(!smoke.contains("bounded_docker run --rm"));
    assert!(!smoke.contains("-e PGPASSWORD=\"${"));
    assert!(!smoke.contains("free_port()"));
    assert!(!smoke.contains("ZITADEL_PUBLISH_ATTEMPTS"));
    assert!(smoke.contains("\"${cleanup_failed}\" == \"0\" && \"${stage}\" == \"complete\""));
    assert_eq!(
        smoke
            .matches("identity_port=\"$(resolve_identity_port)\"")
            .count(),
        2,
        "identity's Docker-assigned host port must be resolved after initial start and restart"
    );
    assert!(
        !smoke.lines().any(|line| {
            let trimmed = line.trim_start();
            trimmed.starts_with("curl ") || line.contains("$(curl ")
        }),
        "signed OIDC smoke has an unbounded curl invocation"
    );
    assert!(
        !smoke.lines().any(|line| {
            let trimmed = line.trim_start();
            trimmed.starts_with("docker ") || line.contains("$(docker ")
        }),
        "signed OIDC smoke has an unbounded docker invocation"
    );
    Ok(())
}

#[test]
fn foundation_baseline_migration_set_is_complete() -> TestResult {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let migration_dir = root.join("migrations");
    let mut migrations = fs::read_dir(migration_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("sql"))
        .collect::<Vec<_>>();
    migrations.sort();

    assert_eq!(migrations.len(), 4, "the launch migration set changed");
    let mut digest = Sha256::new();
    for path in migrations {
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or("migration filename is not UTF-8")?;
        digest.update(name.as_bytes());
        digest.update([0]);
        digest.update(fs::read(path)?);
        digest.update([0]);
    }
    let mut actual = String::with_capacity(64);
    for byte in digest.finalize() {
        write!(&mut actual, "{byte:02x}")?;
    }
    // Baseline hash. Rebased 2026-07-20 (three reviewed edits to migration 0001/0002):
    // (1) removed pg_dump's set_config('search_path','',false) — broke sqlx's
    //     _sqlx_migrations bookkeeping (42P01);
    // (2) CREATE SCHEMA catalog/serving_postgis -> IF NOT EXISTS — the Compose
    //     bootstrap pre-creates them, so the non-idempotent form hit 42P06.
    // (3) removed COMMENT ON EXTENSION postgis — a superuser-only op the
    //     least-privilege foundation_migrator cannot run (42501 on the Compose
    //     path). The extension COMMENT now lives once in the superuser bootstrap.
    // All behaviour-preserving; verified all 4 apply on live PG17 (bare + pre-seeded).
    assert_eq!(
        actual, "be6d934eca79848c2c60259df368b6232ba663705c5ef798e93313a949000caf",
        "the launch migration baseline was edited without review"
    );

    Ok(())
}

#[test]
fn foundation_baseline_contains_final_identity_and_storage_names() -> TestResult {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let migration_dir = root.join("migrations");
    let schema =
        read_normalized_sql(&migration_dir.join("20260719000001_foundation_platform_schema.sql"))?;
    let constraints = read_normalized_sql(
        &migration_dir.join("20260719000002_foundation_platform_constraints.sql"),
    )?;
    let foreign_keys = read_normalized_sql(
        &migration_dir.join("20260719000004_foundation_platform_foreign_keys.sql"),
    )?;
    let indexes =
        read_normalized_sql(&migration_dir.join("20260719000003_foundation_platform_indexes.sql"))?;

    assert!(schema.contains("CREATE SCHEMA IF NOT EXISTS catalog"));
    assert!(schema.contains("CREATE EXTENSION IF NOT EXISTS postgis"));
    assert!(schema.contains("foundation-platform-lakehouse-prod"));
    assert!(schema.contains("foundation-platform.spark_run_summary.v1"));
    assert!(schema.contains("foundation_platform.%"));
    assert!(schema.contains("reviewer_principal_id"));
    assert!(schema.contains("applied_by_principal_id"));
    assert!(constraints.contains("industrial_complex_pkey"));
    assert!(indexes.contains("industrial_complex_active_official_code_idx"));
    assert!(foreign_keys.contains("normalization_proposal_review_proposal_id_fkey"));

    Ok(())
}

#[test]
fn migrations_run_within_migrator_privileges_only() -> TestResult {
    // The sqlx migrations run as the least-privilege `foundation_migrator`
    // (NOSUPERUSER) on the real Compose deploy. Owner/superuser-only statements
    // silently pass the postgres-integration CI — which runs migrations as a
    // superuser — but fail the actual least-privilege path: `COMMENT ON EXTENSION
    // postgis` raised `42501 must be owner of extension postgis`. Extension, role,
    // and system lifecycle belong to the privileged bootstrap
    // (infra/compose/*.sql), never a migration. This guard makes that class of
    // drift mechanically impossible to reintroduce.
    //
    // Each needle maps to a privilege the migrator role provably lacks.
    const FORBIDDEN: &[(&str, &str)] = &[
        (
            "COMMENT ON EXTENSION",
            "extension COMMENT is owner-only (42501) — set it in the superuser bootstrap",
        ),
        ("ALTER EXTENSION", "ALTER EXTENSION is owner-only"),
        ("ALTER SYSTEM", "ALTER SYSTEM is superuser-only"),
        (
            "CREATE ROLE",
            "role lifecycle belongs to the bootstrap, not a migration",
        ),
        (
            "ALTER ROLE",
            "role lifecycle belongs to the bootstrap, not a migration",
        ),
        (
            "DROP ROLE",
            "role lifecycle belongs to the bootstrap, not a migration",
        ),
    ];
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut migrations = fs::read_dir(root.join("migrations"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|value| value.to_str()) == Some("sql"))
        .collect::<Vec<_>>();
    migrations.sort();

    for path in &migrations {
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        // Executable DDL only: drop full-line SQL comments so a migration's own
        // explanatory prose (which may name a forbidden phrase) is never matched.
        let statements = fs::read_to_string(path)?
            .lines()
            .map(|line| {
                if line.trim_start().starts_with("--") {
                    ""
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            .to_ascii_uppercase();

        for (needle, why) in FORBIDDEN {
            assert!(
                !statements.contains(needle),
                "{name} contains migrator-forbidden `{needle}` — {why}"
            );
        }
        // CREATE EXTENSION is tolerated ONLY in the idempotent `IF NOT EXISTS`
        // form — a no-op when the superuser bootstrap already provisioned it.
        for (idx, _) in statements.match_indices("CREATE EXTENSION") {
            assert!(
                statements[idx..].starts_with("CREATE EXTENSION IF NOT EXISTS"),
                "{name} has a non-idempotent CREATE EXTENSION — use CREATE EXTENSION IF NOT EXISTS \
                 (postgis install is superuser-only; the migrator only tolerates the no-op form)"
            );
        }
    }
    Ok(())
}

fn read_normalized_sql(path: &std::path::Path) -> Result<String, std::io::Error> {
    fs::read_to_string(path)
        .map(|contents| contents.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn read_repo_file(relative: &str) -> Result<String, std::io::Error> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    fs::read_to_string(root.join(relative))
}

fn assert_single_bucket_lock_rule(
    relative: &str,
    expected_id: &str,
    expected_prefix: &str,
    expected_max_age_seconds: u64,
) -> TestResult {
    let document: serde_json::Value = serde_json::from_str(&read_repo_file(relative)?)?;
    let rules = document["rules"]
        .as_array()
        .ok_or("bucket lock rules must be an array")?;
    assert_eq!(
        rules.len(),
        1,
        "{relative} must describe exactly one bucket"
    );
    let rule = &rules[0];
    assert_eq!(rule["id"], expected_id);
    assert_eq!(rule["enabled"], true);
    assert_eq!(rule["prefix"], expected_prefix);
    assert_eq!(rule["condition"]["type"], "Age");
    assert_eq!(rule["condition"]["maxAgeSeconds"], expected_max_age_seconds);
    Ok(())
}

fn assert_versioned_apt_installs(dockerfile: &str, contents: &str) {
    let mut install_continuation = false;
    for line in contents.lines() {
        let trimmed = line.trim();
        if let Some(packages) = trimmed
            .split_once("apt-get install -y --no-install-recommends")
            .map(|(_, packages)| packages)
        {
            install_continuation = trimmed.ends_with('\\');
            assert_versioned_packages(dockerfile, packages);
        } else if install_continuation {
            if trimmed.starts_with("&&") {
                install_continuation = false;
            } else {
                assert_versioned_packages(dockerfile, trimmed);
                install_continuation = trimmed.ends_with('\\');
            }
        }
    }
}

fn assert_versioned_packages(dockerfile: &str, packages: &str) {
    for package in packages.trim_end_matches('\\').split_whitespace() {
        assert!(
            package.contains('=') || package.starts_with("/tmp/"),
            "{dockerfile} installs an unversioned apt package: {package}"
        );
    }
}
