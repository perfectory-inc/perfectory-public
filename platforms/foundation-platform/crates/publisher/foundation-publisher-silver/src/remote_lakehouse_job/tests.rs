use super::*;
use foundation_shared_kernel::ids::StaffId;

fn history_fixture() -> anyhow::Result<crate::building_register_floor_silver_export::HistoryWitness>
{
    crate::building_register_floor_silver_export::HistoryWitness::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/building_register_floor_silver_export/history_witness/fixture.json")
            .canonicalize()?,
        None,
    )
}

/// A FLOOR cycle on synthetic 2099 inputs, working under a state root with a space in it.
fn floor_config() -> anyhow::Result<FloorPipelineConfig> {
    let floor_source = floor_source::FloorSource {
        floor: "OPN20991231SYNTHETIC-FLOOR.zip".into(),
        title: "OPN20991231SYNTHETIC-TITLE.zip".into(),
        ingested_at: chrono::DateTime::parse_from_rfc3339("2099-12-31T12:00:00Z")?
            .with_timezone(&chrono::Utc),
        derivation: None,
        history: history_fixture()?,
    };
    Ok(FloorPipelineConfig {
        release_root: "/opt/foundation-platform/releases/synthetic".to_owned(),
        floor_source,
        local: floor_cycle::LocalExecution {
            project: format!("foundation-floor-{}", "1".repeat(32)),
            state_root: "/var/lib/floor state".into(),
            duckdb_root: "/var/lib/floor duckdb".into(),
            image: format!("sha256:{}", "a".repeat(64)),
            database_network: "runtime_default".into(),
            database_endpoint: "postgres:5432".into(),
            attempt: "/var/lib/floor state/attempt".into(),
        },
        audit: RemoteLakehouseAuditConfig::Disabled,
        native_execution_profile: crate::lakehouse_engine_contract::execution_profile()?,
    })
}

#[test]
fn floor_native_invocation_enforces_the_embedded_execution_profile() -> anyhow::Result<()> {
    let config = floor_config()?;
    let profile = crate::lakehouse_engine_contract::execution_profile()?;
    for script in [
        build_floor_inspection_script(&config, "target/lakehouse/r/source.json"),
        build_floor_export_script(&config),
    ] {
        for (name, value) in [
            ("MEMORY_MIB", u64::from(profile.memory_mib)),
            ("CPU_SLOTS", u64::from(profile.cpu_slots)),
            ("PIDS_LIMIT", u64::from(profile.pids_limit)),
            (
                "MEMORY_SWAP_MIB",
                u64::from(profile.memory_mib) + u64::from(profile.swap_mib),
            ),
        ] {
            assert!(script.contains(&format!(
                "export FOUNDATION_PLATFORM_NATIVE_{name}='{value}'"
            )));
        }
        assert!(script.contains(
            "-f compose.lakehouse.yml -f compose.lakehouse-native.yml -f '/var/lib/floor state/attempt/native-compose.json' --profile lakehouse-control run --rm --no-deps --pull never"
        ));
    }
    let limits = include_str!("../../../../../compose.lakehouse-native.yml");
    for setting in ["mem_limit:", "memswap_limit:", "cpus:", "pids_limit:"] {
        assert!(limits.contains(setting));
    }
    assert!(
        !limits.contains(":-"),
        "native limits must not have fallback defaults"
    );
    Ok(())
}

#[test]
fn the_cycle_scripts_start_no_spark_and_no_python() -> anyhow::Result<()> {
    let config = floor_config()?;
    for script in [
        build_floor_inspection_script(&config, "target/lakehouse/r/source.json"),
        build_floor_export_script(&config),
    ] {
        for absent in [
            "spark",
            ".py",
            "--packages",
            "ivy",
            "lakehouse-batch",
            "SUMMARY_BEGIN",
        ] {
            assert!(!script.contains(absent), "a FLOOR script holds {absent}");
        }
        assert!(script.contains("docker image inspect 'sha256:"));
        assert!(script.contains("export COMPOSE_PROJECT_NAME='foundation-floor-"));
    }
    Ok(())
}

#[test]
fn the_inspection_stages_the_exact_inputs_before_it_inspects_them() -> anyhow::Result<()> {
    let config = floor_config()?;
    let root = config.floor_source.root();
    let receipt = format!("{root}/source-check.x/source.json");
    let script = build_floor_inspection_script(&config, &receipt);
    let stage = script
        .find("lakehouse-control stage-building-register-floor-inputs")
        .context("exact input staging")?;
    let inspection = script
        .find(&format!(
            "-e FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_SOURCE_INSPECTION_PATH={}",
            shell_quote(&receipt)
        ))
        .context("inspection")?;
    let exporter = script
        .rfind("lakehouse-control export-building-register-floor-silver-handoff")
        .context("export")?;
    assert!(stage < inspection && inspection < exporter);
    assert_eq!(
        script
            .matches("FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID")
            .count(),
        1,
        "only the input staging receives the writer pair, by name"
    );
    assert!(!script.contains("s3 sync"));
    Ok(())
}

#[test]
fn the_export_writes_the_run_handoff_and_its_ready_summary() -> anyhow::Result<()> {
    let config = floor_config()?;
    let root = config.floor_source.root();
    let script = build_floor_export_script(&config);
    for line in [
        format!("OUTPUT_PATH='{root}/handoff'"),
        format!("SUMMARY_PATH='{root}/export-summary.json'"),
        format!("PROPOSAL_INPUT_PATH='{root}/proposals.jsonl'"),
        "OUTPUT_FORMAT='parquet'".to_owned(),
        format!("CHUNK_ROWS='{FLOOR_CHUNK_ROWS}'"),
        "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_REUSE_COMPLETED_HANDOFF=1".to_owned(),
    ] {
        assert!(script.contains(&line), "{line}");
    }
    assert!(!script.contains("SOURCE_INSPECTION_PATH"));
    assert!(!script.contains("WRITER_ACCESS_KEY_ID"));
    assert!(!script.contains("stage-building-register-floor-inputs"));
    assert_eq!(
        floor_cycle::host_path(&config, &format!("{root}/handoff")),
        format!(
            "/var/lib/floor state/{}/handoff",
            root.strip_prefix("target/lakehouse/")
                .context("state suffix")?
        )
    );
    Ok(())
}

#[test]
fn a_derivation_names_its_own_run_root() -> anyhow::Result<()> {
    let mut config = floor_config()?;
    let original = config.floor_source.root();
    config.floor_source.ingested_at += chrono::Duration::days(1);
    assert_eq!(config.floor_source.root(), original);
    config.floor_source.derivation = Some("floor 'fix' $(false)".into());
    assert_ne!(config.floor_source.root(), original);
    assert!(!build_floor_export_script(&config).contains("$(false)"));
    Ok(())
}

#[test]
fn audit_attribution_is_required_and_parsed_when_audit_is_on() -> anyhow::Result<()> {
    let staff_uuid = uuid::Uuid::parse_str("018f1111-1111-7111-8111-111111111111")?;
    let missing = RemoteLakehouseAuditConfig::from_lookup(&mut |name| {
        (name == "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT").then(|| "1".to_owned())
    })
    .expect_err("audit recording without staff attribution should fail");
    assert!(missing
        .to_string()
        .contains("FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID"));
    let nil = RemoteLakehouseAuditConfig::from_lookup(&mut |name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT" => Some("1".to_owned()),
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID" => {
            Some(uuid::Uuid::nil().to_string())
        }
        _ => None,
    })
    .expect_err("nil staff id should not be accepted for audit attribution");
    assert!(nil.to_string().contains("must not be nil"));
    let audit = RemoteLakehouseAuditConfig::from_lookup(&mut |name| match name {
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORD_AUDIT" => Some("true".to_owned()),
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_RECORDED_BY_STAFF_ID" => {
            Some(format!(" {staff_uuid} "))
        }
        "FOUNDATION_PLATFORM_REMOTE_LAKEHOUSE_REQUEST_ID" => {
            Some(" floor-cycle-2099-12-31 ".to_owned())
        }
        _ => None,
    })?;
    assert_eq!(
        audit,
        RemoteLakehouseAuditConfig::Record {
            recorded_by_staff_id: StaffId::new(staff_uuid),
            request_id: Some("floor-cycle-2099-12-31".to_owned())
        }
    );
    assert_eq!(
        RemoteLakehouseAuditConfig::from_lookup(&mut |_| None)?,
        RemoteLakehouseAuditConfig::Disabled
    );
    Ok(())
}

#[test]
fn historical_retention_summary_cannot_claim_a_new_write() -> anyhow::Result<()> {
    let witness = history_fixture()?.value()?;
    let source = floor_source::FloorSource {
        history: history_fixture()?,
        floor: format!(
            "{}.zip",
            witness["inputs"]["floor"]["provider_file_id"]
                .as_str()
                .context("floor ID")?
        ),
        title: format!(
            "{}.zip",
            witness["inputs"]["title"]["provider_file_id"]
                .as_str()
                .context("title ID")?
        ),
        ingested_at: chrono::DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")?
            .with_timezone(&chrono::Utc),
        derivation: None,
    };
    let value = serde_json::json!({
        "schema_version": "foundation-platform.floor-history-check.v1",
        "action": "already_retained", "persisted_row_count": 0,
        "inputs": witness["inputs"], "derivation": null,
        "source_snapshot_id": witness["source_snapshot_id"],
        "table_uuid": witness["table_uuid"], "snapshot_id": witness["snapshot_id"],
        "observed_head_snapshot_id": witness["snapshot_id"],
        "retained_row_count": witness["row_count"],
    });
    assert_eq!(
        floor_history::retained_outcome(&value.to_string(), &source)?,
        witness["row_count"].as_u64()
    );
    for key in [
        "action",
        "persisted_row_count",
        "source_snapshot_id",
        "table_uuid",
        "snapshot_id",
        "retained_row_count",
        "observed_head_snapshot_id",
    ] {
        let mut changed = value.clone();
        changed[key] = serde_json::json!("wrong");
        assert!(
            floor_history::retained_outcome(&changed.to_string(), &source).is_err(),
            "accepted {key}"
        );
    }
    assert_eq!(
        floor_history::retained_outcome("{\"schema_version\":\"scalar\"}", &source)?,
        None
    );
    let mut derived_source = source.clone();
    derived_source.derivation = Some("normalizer-v2".into());
    let mut mislabeled = value.clone();
    mislabeled["derivation"] = serde_json::json!("normalizer-v2");
    assert!(floor_history::retained_outcome(&mislabeled.to_string(), &derived_source).is_err());
    let mut unknown_kind = value.clone();
    unknown_kind["retention_kind"] = serde_json::json!("unknown");
    assert!(floor_history::retained_outcome(&unknown_kind.to_string(), &source).is_err());
    Ok(())
}

#[test]
fn audit_record_input_is_absent_when_audit_is_disabled() {
    let input = build_audit_record_input(
        "summary-json".to_owned(),
        &RemoteLakehouseAuditConfig::Disabled,
    );

    assert!(input.is_none());
}

#[test]
fn audit_record_input_preserves_summary_staff_and_request_id() {
    let staff_id = StaffId::new(uuid::Uuid::now_v7());
    let input = build_audit_record_input(
        "summary-json".to_owned(),
        &RemoteLakehouseAuditConfig::Record {
            recorded_by_staff_id: staff_id,
            request_id: Some("lakehouse-smoke".to_owned()),
        },
    )
    .expect("audit input should be built");

    assert_eq!(input.summary_json, "summary-json");
    assert_eq!(input.recorded_by_staff_id, staff_id);
    assert_eq!(input.request_id, Some("lakehouse-smoke".to_owned()));
}

#[test]
fn registered_retention_requires_its_commit_and_retained_time() -> anyhow::Result<()> {
    let witness = history_fixture()?.value()?;
    let config = floor_config()?;
    let source = &config.floor_source;
    let mut inputs = witness["inputs"].clone();
    for (role, name) in [("floor", &source.floor), ("title", &source.title)] {
        inputs[role]["provider_file_id"] =
            serde_json::json!(name.strip_suffix(".zip").context("source name")?);
        inputs[role]["provider_month"] = serde_json::json!("2099-12-01");
    }
    let identity = crate::building_register_floor_silver_export::source_identity_from_semantic(
        inputs.clone(),
        &source.history,
    )?;
    let value = serde_json::json!({
        "schema_version": "foundation-platform.floor-history-check.v1",
        "retention_kind": "registered_append",
        "action": "already_retained", "persisted_row_count": 0,
        "source_snapshot_id": identity, "inputs": inputs,
        "table_uuid": witness["table_uuid"], "snapshot_id": "123",
        "observed_head_snapshot_id": "124", "retained_row_count": 3,
        "retained_ingested_at_utc": "2026-10-21T10:00:00Z", "derivation": null,
    });
    assert_eq!(
        floor_history::retained_outcome(&value.to_string(), source)?,
        Some(3)
    );
    for (key, invalid) in [
        ("retained_row_count", serde_json::json!(0)),
        ("persisted_row_count", serde_json::json!(3)),
        ("retained_ingested_at_utc", serde_json::json!("2026-10-21")),
        (
            "source_snapshot_id",
            serde_json::json!("building-register-floor-content-v1-other"),
        ),
        ("table_uuid", serde_json::json!("other-table")),
        ("derivation", serde_json::json!("other-derivation")),
        ("snapshot_id", serde_json::json!("0")),
        ("observed_head_snapshot_id", serde_json::json!(-1)),
    ] {
        let mut changed = value.clone();
        changed[key] = invalid;
        assert!(
            floor_history::retained_outcome(&changed.to_string(), source).is_err(),
            "accepted {key}"
        );
    }
    let mut changed = value.clone();
    changed["inputs"]["floor"]["provider_file_id"] = serde_json::json!("OPN20991231OTHER");
    // Even an internally consistent receipt for another source cannot satisfy this request.
    changed["source_snapshot_id"] = serde_json::json!(
        crate::building_register_floor_silver_export::source_identity_from_semantic(
            changed["inputs"].clone(),
            &source.history
        )?
    );
    assert!(floor_history::retained_outcome(&changed.to_string(), source).is_err());
    Ok(())
}
