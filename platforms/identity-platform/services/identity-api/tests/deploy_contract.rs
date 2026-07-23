//! Independent Identity build and deploy contract.

use std::error::Error;
use std::fs;
use std::path::PathBuf;

type TestResult = Result<(), Box<dyn Error>>;

/// The Identity CI workflow moved from the workspace-local
/// `.github/workflows/ci.yml` to the monorepo root as
/// `.github/workflows/identity-ci.yml`, so it is reached by climbing out of
/// the workspace root that [`read_repo_file`] resolves against. (Secret
/// scanning moved to the root `secret-scan.yml` workflow.)
const IDENTITY_CI_WORKFLOW: &str = "../../.github/workflows/identity-ci.yml";

#[test]
fn compose_exposes_only_independent_least_privilege_identity_runtimes() -> TestResult {
    let compose = read_repo_file("docker-compose.yml")?;

    for required in [
        "identity_api:${IDENTITY_API_PASSWORD:?set IDENTITY_API_PASSWORD}",
        "identity_policy_worker:${IDENTITY_POLICY_WORKER_PASSWORD:?set IDENTITY_POLICY_WORKER_PASSWORD}",
        "identity_provisioner:${IDENTITY_PROVISIONER_PASSWORD:?set IDENTITY_PROVISIONER_PASSWORD}",
        "IDENTITY_WORKLOAD_PRINCIPAL_BINDINGS",
        "IDENTITY_BOOTSTRAP_ADMIN_ZITADEL_SUBJECT",
        "IDENTITY_BOOTSTRAP_ADMIN_EMAIL",
        "IDENTITY_BOOTSTRAP_ADMIN_DISPLAY_NAME",
        "identity-api:\n",
        "identity-policy-worker:\n",
        "identity-workload-provisioner:\n",
        "healthcheck:\n",
    ] {
        assert!(compose.contains(required), "Compose is missing {required}");
    }
    assert_eq!(
        compose
            .matches("dockerfile: services/identity-api/Dockerfile")
            .count(),
        1,
        "the shared runtime image must have exactly one Compose builder"
    );
    Ok(())
}

#[test]
fn runtime_images_are_locked_non_root_and_health_checked() -> TestResult {
    for dockerfile in [
        "services/identity-api/Dockerfile",
        "services/identity-policy-worker/Dockerfile",
    ] {
        let contents = read_repo_file(dockerfile)?;
        assert!(contents.contains("cargo build --locked --release"));
        assert!(contents.contains("USER 10001:10001"));
        assert!(contents.contains("HEALTHCHECK"));
        assert!(
            contents
                .lines()
                .filter(|line| line.starts_with("FROM "))
                .all(|line| line.contains("@sha256:")),
            "{dockerfile} has a mutable FROM"
        );
        assert!(!contents.contains("apt-get install"));
        assert!(!contents.contains("latest"));
        assert!(!contents.contains("FROM busybox:"));
        assert!(!contents.contains("/usr/local/bin/busybox"));
    }
    assert!(read_repo_file("services/identity-api/Dockerfile")?
        .contains("CMD [\"/usr/local/bin/identity-api\", \"--healthcheck\"]"));
    assert!(
        read_repo_file("services/identity-policy-worker/Dockerfile")?
            .contains("CMD [\"/usr/local/bin/identity-policy-worker\", \"--healthcheck\"]")
    );
    Ok(())
}

#[test]
fn compose_pins_external_images_and_runs_all_one_shots_non_root() -> TestResult {
    let compose = read_repo_file("docker-compose.yml")?;
    for image in compose
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("image: "))
    {
        assert!(
            image.ends_with(":local") || image.contains("@sha256:"),
            "mutable Compose image: {image}"
        );
    }
    assert_eq!(compose.matches("user: \"70:70\"").count(), 3);

    let smoke = read_repo_file("scripts/compose-smoke.sh")?;
    for service in [
        "identity-bootstrap",
        "identity-database-migrator",
        "identity-runtime-grants",
        "identity-workload-provisioner",
        "identity-finalize",
    ] {
        assert!(
            smoke.contains(format!("run_uid_probe {service}").as_str()),
            "Compose smoke does not prove non-root execution for {service}"
        );
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

    let ci = read_repo_file(IDENTITY_CI_WORKFLOW)?;
    assert!(ci.contains("timeout-minutes: 30"));
    assert!(ci.contains("scripts/compose-smoke.sh -- start-all"));
    assert!(ci.contains("scripts/compose-smoke.sh -- rerun-all"));
    assert!(
        ci.matches("docker compose run --rm -T --no-deps").count() >= 2,
        "CI worker health probes must be explicitly non-interactive"
    );
    assert!(
        ci.matches("--interactive=false").count() >= 2,
        "CI worker health probes must close stdin"
    );
    Ok(())
}

#[test]
fn api_health_probes_use_the_native_command_and_bind_aware_listener() -> TestResult {
    let compose = read_repo_file("docker-compose.yml")?;
    assert!(compose.contains("[\"CMD\", \"/usr/local/bin/identity-api\", \"--healthcheck\"]"));
    assert!(
        compose.contains("[\"CMD\", \"/usr/local/bin/identity-policy-worker\", \"--healthcheck\"]")
    );
    assert!(!compose.contains("/usr/local/bin/busybox"));

    let main = read_repo_file("services/identity-api/src/main.rs")?;
    assert!(main.contains("healthcheck_address(bind_address()?)"));
    assert!(main.contains("address.is_unspecified()"));
    for path in ["scripts/compose-smoke.sh", IDENTITY_CI_WORKFLOW] {
        let contents = read_repo_file(path)?;
        assert!(
            contents.contains("State.Health.Status"),
            "{path} does not consume the native Identity runtime health result"
        );
        assert!(!contents.contains("/usr/local/bin/busybox"));
    }
    Ok(())
}

#[test]
fn worker_healthcheck_validates_config_and_read_only_database_readiness() -> TestResult {
    let main = read_repo_file("services/identity-policy-worker/src/main.rs")?;
    for required in [
        "WorkerConfig::from_env()?",
        "check_worker_readiness(&config).await",
        "SELECT 1 FROM identity.outbox_event LIMIT 0",
        ".connect(&config.database_url)",
    ] {
        assert!(
            main.contains(required),
            "worker healthcheck is missing {required}"
        );
    }
    assert!(!main.contains(
        "if healthcheck_requested(std::env::args_os().nth(1).as_deref()) {\n        return Ok(());"
    ));

    let ci = read_repo_file(IDENTITY_CI_WORKFLOW)?;
    for required in [
        "Verify worker healthcheck rejects missing configuration",
        "Verify worker healthcheck rejects unreachable database",
        "Verify worker healthcheck accepts migrated database",
        "[ \"$status\" -eq 124 ]",
        "{{.State.Health.Status}}",
    ] {
        assert!(ci.contains(required), "CI is missing {required}");
    }
    Ok(())
}

#[test]
fn examples_and_ci_cover_independent_bootstrap_migration_and_compose() -> TestResult {
    let example = read_repo_file(".env.example")?;
    for required in [
        "IDENTITY_ADMIN_PASSWORD=REPLACE_WITH_",
        "IDENTITY_MIGRATOR_PASSWORD=REPLACE_WITH_",
        "IDENTITY_API_PASSWORD=REPLACE_WITH_",
        "IDENTITY_POLICY_WORKER_PASSWORD=REPLACE_WITH_",
        "IDENTITY_PROVISIONER_PASSWORD=REPLACE_WITH_",
        "IDENTITY_WORKLOAD_PRINCIPAL_BINDINGS_FILE=",
        "IDENTITY_BOOTSTRAP_ADMIN_ZITADEL_SUBJECT=",
        "IDENTITY_BOOTSTRAP_ADMIN_EMAIL=",
        "IDENTITY_BOOTSTRAP_ADMIN_DISPLAY_NAME=",
    ] {
        assert!(
            example.contains(required),
            ".env.example is missing {required}"
        );
    }

    let ci = read_repo_file(IDENTITY_CI_WORKFLOW)?;
    for required in [
        // ADR-0004: full-workspace fmt/clippy/test verification is owned by the
        // single SSOT command (replaces the standalone build/fmt/clippy/test jobs);
        // xtask clippy --all-targets + test --workspace --all-features compiles+checks
        // the whole workspace.
        "cargo xtask verify identity",
        "Run live role-grant least-privilege contract",
        "Build and start clean Compose stack",
        "NOT has_table_privilege('identity_api', 'identity.staff', 'UPDATE')",
        "docker compose down -v --remove-orphans",
    ] {
        assert!(ci.contains(required), "CI is missing {required}");
    }
    Ok(())
}

#[test]
fn runtime_role_cannot_modify_staff_profile_fields() -> TestResult {
    let grants = read_repo_file("infra/compose/grant-identity-runtime-access.sql")?;

    assert!(grants.contains("REVOKE UPDATE ON identity.staff FROM :\"identity_api_role\";"));
    assert!(grants.contains("GRANT SELECT, INSERT ON identity.staff TO :\"identity_api_role\";"));
    assert!(!grants
        .contains("GRANT SELECT, INSERT, UPDATE ON identity.staff TO :\"identity_api_role\";"));
    Ok(())
}

#[test]
fn provisioner_is_a_fail_closed_atomic_least_privilege_deployment_job() -> TestResult {
    let compose = read_repo_file("docker-compose.yml")?;
    let grants = read_repo_file("infra/compose/grant-identity-runtime-access.sql")?;
    let dockerfile = read_repo_file("services/identity-api/Dockerfile")?;
    let smoke = read_repo_file("scripts/compose-smoke.sh")?;

    for required in [
        "identity-service-provisioner --bin identity-service-provisioner",
        "/usr/local/bin/identity-service-provisioner",
    ] {
        assert!(
            dockerfile.contains(required),
            "runtime image is missing {required}"
        );
    }
    for required in [
        "identity-workload-provisioner:\n",
        "IDENTITY_PROVISIONER_DATABASE_URL",
        "IDENTITY_WORKLOAD_PRINCIPAL_BINDINGS",
        "identity-runtime-grants:\n        condition: service_completed_successfully",
        "identity-workload-provisioner:\n        condition: service_completed_successfully",
    ] {
        assert!(compose.contains(required), "Compose is missing {required}");
    }
    for required in [
        "GRANT SELECT, INSERT, UPDATE ON identity.service_principal",
        "GRANT SELECT, INSERT, DELETE ON identity.service_capability_grant",
    ] {
        assert!(
            grants.contains(required),
            "provisioner ACL is missing {required}"
        );
    }
    assert!(!grants.contains("GRANT ALL"));
    assert!(smoke.contains("verify_provisioner_acl"));
    assert!(smoke.contains("forbidden_sql_succeeded"));
    Ok(())
}

fn read_repo_file(relative: &str) -> Result<String, std::io::Error> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    fs::read_to_string(root.join(relative))
}
