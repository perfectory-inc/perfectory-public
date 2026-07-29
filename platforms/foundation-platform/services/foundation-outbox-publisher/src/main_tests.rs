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
fn parses_collect_building_hub_bronze_catalog_recovery_inventory_command() {
    let command = parse_command([
        "foundation-outbox-publisher",
        "collect-building-hub-bronze-catalog-recovery-inventory",
    ])
    .expect("Hub recovery inventory command should parse");

    assert_eq!(
        command,
        Command::CollectBuildingHubBronzeCatalogRecoveryInventory
    );
}

#[test]
fn parses_compile_building_hub_bronze_catalog_recovery_manifest_command() {
    let command = parse_command([
        "foundation-outbox-publisher",
        "compile-building-hub-bronze-catalog-recovery-manifest",
    ])
    .expect("Hub recovery manifest command should parse");

    assert_eq!(
        command,
        Command::CompileBuildingHubBronzeCatalogRecoveryManifest
    );
}

#[test]
fn parses_collect_vworld_bronze_catalog_recovery_inventory_command() {
    let command = parse_command([
        "foundation-outbox-publisher",
        "collect-vworld-bronze-catalog-recovery-inventory",
    ])
    .expect("VWorld recovery inventory command should parse");

    assert_eq!(
        command,
        Command::CollectVWorldBronzeCatalogRecoveryInventory
    );
}

#[test]
fn parses_compile_vworld_bronze_catalog_recovery_manifest_command() {
    let command = parse_command([
        "foundation-outbox-publisher",
        "compile-vworld-bronze-catalog-recovery-manifest",
    ])
    .expect("recovery manifest compile command should parse");

    assert_eq!(command, Command::CompileVWorldBronzeCatalogRecoveryManifest);
}

#[test]
fn parses_recover_bronze_catalog_command() {
    let command = parse_command(["foundation-outbox-publisher", "recover-bronze-catalog"])
        .expect("Bronze Catalog recovery command should parse");

    assert_eq!(command, Command::RecoverBronzeCatalog);
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
