use std::{fs, path::PathBuf};

use foundation_outbox::object_storage::ObjectStorageSmokeReport;

use super::{parse_webhook_endpoint_specs, write_r2_smoke_metrics};

#[test]
fn r2_smoke_metrics_writer_persists_operation_counts() -> anyhow::Result<()> {
    let path = PathBuf::from("target/outbox-publisher-main-tests/r2-smoke-metrics.prom");
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let report = ObjectStorageSmokeReport {
        key: "gold/_smoke/foundation-platform-r2-smoke-test.json".to_owned(),
        bytes_verified: 512,
        put_request_count: 1,
        get_request_count: 1,
        delete_request_count: 1,
    };

    write_r2_smoke_metrics(&path, &report)?;

    let metrics = fs::read_to_string(&path)?;
    assert!(metrics.contains(
        "foundation_platform_r2_smoke_request_total{source=\"live_r2_smoke\",operation=\"put\"} 1"
    ));
    assert!(metrics
        .contains("foundation_platform_r2_smoke_bytes_verified{source=\"live_r2_smoke\"} 512"));
    assert!(!metrics.contains("foundation-platform-r2-smoke-test.json"));
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn webhook_endpoint_specs_parse_name_url_pairs() -> anyhow::Result<()> {
    let specs = parse_webhook_endpoint_specs(
        "gongzzang=https://gongzzang.example.invalid/foundation-platform/events; \
             dawneer=https://dawneer.example.invalid/foundation-platform/events",
    )?;

    assert_eq!(specs.len(), 2);
    assert_eq!(specs[0].0, "gongzzang");
    assert_eq!(
        specs[0].1,
        "https://gongzzang.example.invalid/foundation-platform/events"
    );
    assert_eq!(specs[1].0, "dawneer");
    assert_eq!(
        specs[1].1,
        "https://dawneer.example.invalid/foundation-platform/events"
    );
    Ok(())
}

#[test]
fn webhook_endpoint_specs_reject_empty_name() -> anyhow::Result<()> {
    let error = match parse_webhook_endpoint_specs("=https://example.test/events") {
        Ok(specs) => anyhow::bail!("expected parse failure, got {specs:?}"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("name"));
    Ok(())
}

#[test]
fn webhook_endpoint_specs_reject_missing_url() -> anyhow::Result<()> {
    let error = match parse_webhook_endpoint_specs("gongzzang=") {
        Ok(specs) => anyhow::bail!("expected parse failure, got {specs:?}"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("url"));
    Ok(())
}
