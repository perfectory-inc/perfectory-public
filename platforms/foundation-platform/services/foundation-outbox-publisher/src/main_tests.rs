use super::{parse_command, Command};

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
