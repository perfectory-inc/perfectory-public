use super::runtime_environment::{validate_catalog_driver, RuntimeEnvironment};
use super::{parse_command, Command};

#[test]
fn catalog_publisher_policy_allows_explicit_local_and_ci_adapters() {
    assert!(validate_catalog_driver(RuntimeEnvironment::Local, "r2").is_ok());
    assert!(validate_catalog_driver(RuntimeEnvironment::Ci, "log").is_ok());
}

#[test]
fn catalog_publisher_policy_rejects_log_adapter_outside_local_and_ci() {
    assert!(validate_catalog_driver(RuntimeEnvironment::Staging, "log").is_err());
    assert!(validate_catalog_driver(RuntimeEnvironment::Production, "log").is_err());
}

#[test]
fn parses_check_r2_runtime_target_command() {
    let command = parse_command(["foundation-outbox-publisher", "check-r2-runtime-target"])
        .expect("R2 runtime target command should parse");

    assert_eq!(command, Command::CheckR2RuntimeTarget);
}

#[test]
fn parses_canonical_release_proof_command() {
    let command = parse_command([
        "foundation-outbox-publisher",
        "write-canonical-release-proof",
    ])
    .expect("canonical release proof command should parse");

    assert_eq!(command, Command::WriteCanonicalReleaseProof);
}
