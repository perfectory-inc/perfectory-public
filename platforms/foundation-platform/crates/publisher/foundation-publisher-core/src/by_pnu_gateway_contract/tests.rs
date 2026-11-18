use super::*;
use anyhow::Context as _;
use serde_json::{json, Value};

fn written() -> anyhow::Result<Value> {
    Ok(serde_json::from_str(R2_CONNECTION_CONTRACT)?)
}

#[test]
fn a_lane_behind_a_front_door_is_read_on_its_origin_hostname() -> anyhow::Result<()> {
    let doors: Value = serde_json::from_str(CDN_FRONT_DOOR_CONTRACT)?;
    // A door names its gateway by a top-level block or a dotted path into one (the unit price lane's).
    let unit_price = ByPnuLane::from_unit("unit-price-by-pnu").context("the unit price lane")?;
    let lanes = [
        ("catalog", ByPnuLane::Parcel),
        ("buildings", ByPnuLane::Building),
    ];
    for (door, lane) in lanes.into_iter().chain([("unit_prices", unit_price)]) {
        let origin = doors["doors"][door]["origin_hostname"]
            .as_str()
            .with_context(|| format!("the {door} door names its origin"))?;
        assert_eq!(lane.worker_hostname()?, origin);
        assert_ne!(origin, lane.policy()?.public_hostname);
    }
    Ok(())
}

/// The contract's patch block with `change` applied.
fn patch_bounds(change: impl FnOnce(&mut Value)) -> anyhow::Result<ByPnuServingPatchPolicy> {
    let mut block = written()?["by_pnu_serving_patches"].clone();
    change(&mut block);
    Ok(serde_json::from_value(block)?)
}

#[test]
fn the_cumulative_bound_stays_under_the_measured_break_even_on_one_section_lanes(
) -> anyhow::Result<()> {
    // Root ADR-0274 §2: the contract as written holds, with the lanes it names.
    assert!(by_pnu_serving_patch_policy().is_ok());
    let one_section = [("parcel", 1), ("building", 1)];
    check_patch_bounds(&patch_bounds(|_| {})?, &one_section).map_err(anyhow::Error::msg)?;
    // A bound above the break-even is a patch that costs more than the full bake it avoids.
    let above = patch_bounds(|block| {
        block["max_cumulative_change_ratio"] = json!(
            block["cumulative_change_break_even_ratio"]
                .as_f64()
                .unwrap_or(1.0)
                + 0.01
        );
    })?;
    let refused = check_patch_bounds(&above, &one_section)
        .err()
        .unwrap_or_default();
    assert!(refused.contains("break-even"), "{refused}");
    // The break-even is the one-section measurement: a lane of more sections must be measured.
    let refused = check_patch_bounds(&patch_bounds(|_| {})?, &[("parcel", 1), ("building", 4)])
        .err()
        .unwrap_or_default();
    assert!(
        refused.contains("lane building serves 4 sections"),
        "{refused}"
    );
    // And the numbers carry their measurement.
    let reasonless =
        patch_bounds(|block| block["max_cumulative_change_ratio_reason"] = json!(" "))?;
    assert!(check_patch_bounds(&reasonless, &one_section).is_err());
    Ok(())
}

#[test]
fn a_lane_with_a_host_is_its_hosts_block_with_the_kind_in_its_paths() -> anyhow::Result<()> {
    // Root ADR-0193 §4: the contract's vectors hold this rule and the Worker's to one answer.
    let contract = written()?;
    let vectors = contract
        .pointer("/by_pnu_lane_rule/test_vectors")
        .and_then(Value::as_array)
        .context("the contract holds the lane rule's test vectors")?;
    assert!(!vectors.is_empty());
    for vector in vectors {
        let field = |name: &str| vector[name].as_str().map(str::to_owned).unwrap_or_default();
        let entry = json!({"host": vector["host"], "kind": vector["kind"], "section": "s", "columns": ["s_json"]});
        let lane =
            resolve_lane_entry(&field("lane"), &entry, &contract).map_err(anyhow::Error::msg)?;
        for (pointer, expected) in [
            ("/request_path/prefix", "request_prefix"),
            ("/request_path/capabilities", "capabilities"),
            ("/object_key/manifest_object", "manifest_object"),
            ("/section_packs/root", "pack_root"),
            (
                "/section_packs/scheduled_bake/capability",
                "scheduled_bake_capability",
            ),
        ] {
            assert_eq!(lane.pointer(pointer), Some(&vector[expected]), "{pointer}");
        }
        let host = &contract[vector["host"].as_str().unwrap_or_default()];
        // What does not differ per lane is the host's: the same Worker serves it.
        for pointer in [
            "/worker_name",
            "/public_hostname",
            "/section_packs/preview_worker",
        ] {
            assert_eq!(lane.pointer(pointer), host.pointer(pointer), "{pointer}");
        }
        assert_eq!(lane["pack_only"], json!(true));
        assert_eq!(lane["request_path"]["accepted_queries"], json!([]));
        assert_eq!(lane["section_packs"]["sections"], json!(["documents"]));
        // The kind line names nothing else: its table, baseline, bound and follow lane are derived.
        let snake = field("lane").replace('-', "_");
        assert_eq!(lane["gold_table"], json!(format!("gold.{snake}")));
        let host_lane = vector["host"]
            .as_str()
            .unwrap_or_default()
            .trim_end_matches("_by_pnu_gateway");
        assert_eq!(lane["latency_baseline_lane"], json!(host_lane));
        assert_eq!(lane["documents_follow_lane"], json!(host_lane));
        assert_eq!(
            lane["latency_p95_max_ms"],
            contract["by_pnu_lane_rule"]["latency_p95_max_ms"]
        );
        assert!(lane.get("section").is_none() && lane.get("columns").is_none());
    }
    Ok(())
}

#[test]
fn a_lane_with_a_host_that_writes_a_derived_field_or_names_no_host_block_is_refused(
) -> anyhow::Result<()> {
    let contract = written()?;
    let lane = json!({"host": "parcel_by_pnu_gateway", "kind": "zoning", "section": "zonings"});
    for (field, value) in [
        ("section_packs", json!({"root": "serving/elsewhere/packs"})),
        ("host", json!("by_pnu_lanes")),
        ("kind", json!("Zoning")),
        ("gold_table", json!("gold.elsewhere")),
        // Root ADR-0564 §6-1: only the versions table, and only beside its declaration.
        ("gold_table", json!("gold.parcel_zoning")),
        ("gold_table", json!("gold.parcel_zoning_versions")),
        ("versions_table", json!(false)),
    ] {
        let mut entry = lane.clone();
        entry[field] = value;
        assert!(
            resolve_lane_entry("parcel-zoning", &entry, &contract).is_err(),
            "{field}"
        );
    }
    // A kind that declares its versions table still bakes the replaced one, until its line
    // names the versions table as its Gold (the take-over, root ADR-0564 §6-4).
    let mut shadow = lane.clone();
    shadow["versions_table"] = json!(true);
    assert_eq!(
        resolve_lane_entry("parcel-zoning", &shadow, &contract).map_err(anyhow::Error::msg)?
            ["gold_table"],
        json!("gold.parcel_zoning")
    );
    shadow["gold_table"] = json!("gold.parcel_zoning_versions");
    assert_eq!(
        resolve_lane_entry("parcel-zoning", &shadow, &contract).map_err(anyhow::Error::msg)?
            ["gold_table"],
        json!("gold.parcel_zoning_versions")
    );
    shadow["gold_table"] = json!("gold.parcel_price_versions");
    assert!(resolve_lane_entry("parcel-zoning", &shadow, &contract).is_err());
    // A kind's document schema is the registry's (root ADR-0336): its line states none.
    for field in [
        "document_schema_version",
        "document_schema_version_reason",
        "document_schema_expands",
    ] {
        let mut entry = lane.clone();
        entry[field] = json!("a version");
        assert!(
            resolve_lane_entry("parcel-zoning", &entry, &contract).is_err(),
            "{field}"
        );
    }
    // A lane written whole (root ADR-0190) is read as it is written, its document schema
    // added from its subject; a schema stated on it is refused like a kind line's.
    let whole = json!({"pack_only": true, "worker_name": "w"});
    let resolved = resolve_lane_entry("unit-price", &whole, &contract)
        .map_err(|error| anyhow::anyhow!(error))?;
    assert_eq!(resolved["worker_name"], "w");
    let prices = crate::schema_registry::subject_of_lane("unit-price")?
        .context("the registry lists the unit price subject")?;
    assert_eq!(
        resolved["document_schema_version"],
        json!(prices.current().schema_version)
    );
    assert_eq!(resolved["document_schema_expands"], json!([]));
    let mut stated = whole.clone();
    stated["document_schema_version"] = json!(prices.current().schema_version);
    assert!(resolve_lane_entry("unit-price", &stated, &contract).is_err());
    Ok(())
}

#[test]
fn the_zoning_lane_is_served_by_the_parcel_worker_under_its_own_paths() -> anyhow::Result<()> {
    let lane = ByPnuLane::named("parcel-zoning")?.context("the contract names the zoning lane")?;
    let pack = lane
        .pack_lane()
        .context("a contract lane serves packs only")?;
    let policy = lane.policy()?;
    let parcel = ByPnuLane::Parcel.policy()?;
    assert_eq!(policy.worker_name, parcel.worker_name);
    assert_eq!(policy.request_path.prefix, "/parcels/zoning/by-pnu/");
    assert_eq!(lane.section_packs()?.root, "serving/parcels/zoning/packs");
    assert_eq!(pack.gold.table_name, "gold.parcel_zoning");
    let zoning = crate::schema_registry::subject_of_lane("parcel-zoning")?
        .context("the registry lists the zoning subject")?;
    assert_eq!(
        pack.document_schema_version,
        format!("{}.v1", zoning.subject)
    );
    assert!(pack.document_schema_expands.is_empty());
    // The forest ledger gained ownership_kind_name (#746) under the legacy string `.v2`, which
    // only added an optional field: a served `.v1` base takes its patches (root ADR-0320).
    let forest = ByPnuLane::named("parcel-forest-ledger")?
        .and_then(ByPnuLane::pack_lane)
        .context("the contract names the forest ledger lane")?;
    let ledger = crate::schema_registry::subject_of_lane("parcel-forest-ledger")?
        .context("the registry lists the forest ledger subject")?;
    assert_eq!(
        forest.document_schema_version,
        format!("{}.v2", ledger.subject)
    );
    assert_eq!(
        forest.document_schema_expands,
        [format!("{}.v1", ledger.subject)]
    );
    assert_eq!(forest.document_schema_revision, None);
    assert_eq!(lane.unit(), "parcel-zoning-by-pnu");
    Ok(())
}

#[test]
fn a_registered_lane_takes_its_document_schema_from_the_registry_alone() -> anyhow::Result<()> {
    // Root ADR-0336. 2026-10-27..29: the public auction lane's version string sat in this
    // contract, the web, the monitor's manifest record and the Worker's smoke, and its v1 -> v2
    // expansion broke each copy on its own. Its line now names none; the registry does.
    let auction = ByPnuLane::named("parcel-public-auction")?
        .and_then(ByPnuLane::pack_lane)
        .context("the contract names the public auction lane")?;
    let subject = crate::schema_registry::subject_of_lane("parcel-public-auction")?
        .context("the registry lists the public auction subject")?;
    assert_eq!(
        auction.document_schema_version,
        subject.current().schema_version
    );
    // It expands every other string of its major: the legacy revision 2's (root ADR-0340 made
    // revision 3, the first written as `.v1` with a schema_revision).
    assert_eq!(
        auction.document_schema_expands,
        [format!("{}.v2", subject.subject)]
    );
    assert_eq!(auction.document_schema_revision, Some(4));
    let contract = written()?;
    let line = &contract["by_pnu_lanes"]["parcel-public-auction"];
    for field in [
        "document_schema_version",
        "document_schema_version_reason",
        "document_schema_expands",
    ] {
        assert!(line.get(field).is_none(), "{field}");
        let mut stated = line.clone();
        stated[field] = json!(subject.current().schema_version);
        resolve_lane_entry("parcel-public-auction", &stated, &contract)
            .expect_err("a registered lane may not state its version");
    }
    Ok(())
}
