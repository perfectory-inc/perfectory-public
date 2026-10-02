use std::io::Write as _;

use serde_json::json;

use super::pmtiles_feature_ids::tests::{archive, tile, written};
use super::*;

fn summary(rows: usize) -> ServedSummary {
    serde_json::from_value(json!({
        "schema_version": SERVED_SUMMARY_V1,
        "unit": "complex",
        "feature_id_property": "complex_id",
        "geometry_srid": 5186,
        "canonical_iceberg_snapshot_id": "841361364657368625",
        "edits_through_change_seq": 2,
        "served_row_count": rows,
        "status": "ready",
    }))
    .unwrap_or_else(|error| panic!("summary fixture: {error}"))
}

#[test]
fn a_v1_summary_carrying_a_source_snapshot_binds_no_silver_source() -> anyhow::Result<()> {
    // The admin served-Gold job writes a v1 summary with `source_snapshot_id` as an extra field.
    let admin: ServedSummary = serde_json::from_value(json!({
        "schema_version": SERVED_SUMMARY_V1,
        "unit": "admin",
        "feature_id_property": "unit_id",
        "geometry_srid": 4326,
        "canonical_iceberg_snapshot_id": "841361364657368625",
        "edits_through_change_seq": 0,
        "served_row_count": 1,
        "status": "ready",
        "source_snapshot_id": "synthetic-admin-snapshot",
    }))?;
    admin.validate("admin")?;
    assert_eq!(admin.bound_silver_snapshot(), None);
    // Without a verdict in the environment this must not fail: a v1 bake needs none.
    assert!(crate::lakehouse_bake_verdict::silver_source(admin.bound_silver_snapshot())?.is_none());
    Ok(())
}

fn line(id: &str, code: &str) -> String {
    json!({
        "feature_id": id, "properties": {"official_complex_code": code},
        "geometry_wkb_hex": "0106000000", "geometry_srid": 5186, "origin": "source"
    })
    .to_string()
}

/// Streams `text` as a v1 handoff file and returns the GDAL CSV it produced.
fn prepare(
    text: &str,
    summary: &ServedSummary,
) -> anyhow::Result<(String, PreparedHandoff<Vec<u8>>)> {
    let file = written(text.as_bytes())?;
    let prepared = prepare_handoff(summary, file.path(), Vec::new())?;
    Ok((String::from_utf8(prepared.csv.clone())?, prepared))
}

#[test]
fn the_summary_must_be_a_ready_nonempty_snapshot_of_the_unit() {
    assert!(summary(1).validate("complex").is_ok());
    assert!(summary(1).validate("admin").is_err());
    assert!(
        summary(0).validate("complex").is_err(),
        "an empty snapshot would erase the layer"
    );
    let mut unknown = summary(1);
    unknown.schema_version = "foundation-platform.polygon_served_gold.v9".to_owned();
    assert!(unknown.validate("complex").is_err());
}

#[test]
fn a_v1_complex_handoff_still_bakes_as_before() -> anyhow::Result<()> {
    let text = format!("{}\n\n{}\n", line("a", "SYN\"1"), line("b", "SYN-2"));
    let (csv, prepared) = prepare(&text, &summary(2))?;
    assert_eq!(
        csv,
        "\"complex_id\",\"official_complex_code\",geometry\n\
         \"a\",\"SYN\"\"1\",0106000000\n\
         \"b\",\"SYN-2\",0106000000\n"
    );
    assert_eq!(prepared.columns, ["complex_id", "official_complex_code"]);
    assert_eq!(prepared.ids.len(), 2);
    assert!(prepared.ids.position(id_hash("a")).is_some());
    Ok(())
}

#[test]
fn served_rows_are_read_once_each_and_counted_against_the_summary() -> anyhow::Result<()> {
    let text = format!("{}\n{}\n", line("a", "SYN-1"), line("b", "SYN-2"));
    assert!(
        prepare(&text, &summary(3)).is_err(),
        "a short handoff is refused"
    );
    let repeated = format!("{}\n{}\n", line("a", "SYN-1"), line("a", "SYN-2"));
    let refusal = prepare(&repeated, &summary(2))
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        refusal.contains("served twice") && refusal.contains("\"a\""),
        "a repeat is refused and named: {refusal}"
    );
    let wrong_crs = json!({"feature_id": "a", "properties": {"official_complex_code": "x"},
        "geometry_wkb_hex": "01", "geometry_srid": 4326})
    .to_string();
    assert!(prepare(&wrong_crs, &summary(1)).is_err());
    Ok(())
}

#[test]
fn rows_with_different_property_sets_or_an_id_repeated_as_a_property_are_refused() {
    let other = json!({"feature_id": "b", "properties": {"other": "x"},
        "geometry_wkb_hex": "01", "geometry_srid": 5186})
    .to_string();
    let text = format!("{}\n{other}\n", line("a", "SYN-1"));
    assert!(prepare(&text, &summary(2)).is_err());
    let repeated = json!({"feature_id": "a", "properties": {"complex_id": "a"},
        "geometry_wkb_hex": "01", "geometry_srid": 5186})
    .to_string();
    assert!(prepare(&repeated, &summary(1)).is_err());
}

#[test]
fn the_summary_crs_must_be_one_the_bake_reprojects() {
    let mut bad = summary(1);
    bad.geometry_srid = 3857;
    assert!(bad.validate("complex").is_err());
}

fn parcel_line(id: &str) -> String {
    json!({"feature_id": id, "properties": {}, "geometry_wkb_hex": "0106000000",
        "geometry_srid": 4326, "origin": "source"})
    .to_string()
}

/// A v2 handoff in a fresh parts directory: the given parts, and a summary that names them.
fn v2(parts: &[(&str, &[&str])]) -> anyhow::Result<(tempfile::TempDir, ServedSummary)> {
    let directory = tempfile::tempdir()?;
    let mut named = Vec::new();
    for (path, ids) in parts {
        let body: String = ids.iter().map(|id| parcel_line(id) + "\n").collect();
        let target = directory.path().join(path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::File::create(&target)?.write_all(body.as_bytes())?;
        named.push(json!({"path": path, "rows": ids.len(),
            "sha256": format!("{:x}", Sha256::digest(body.as_bytes()))}));
    }
    let rows: usize = parts.iter().map(|(_, ids)| ids.len()).sum();
    let summary = serde_json::from_value(json!({
        "schema_version": SERVED_SUMMARY_V2,
        "unit": "parcels",
        "feature_id_property": "pnu",
        "geometry_srid": 4326,
        "canonical_iceberg_snapshot_id": "1",
        "source_snapshot_id": "2",
        "edits_through_change_seq": 0,
        "served_row_count": rows,
        "status": "ready",
        "output_path": "/parts",
        "handoff_parts": named,
    }))?;
    Ok((directory, summary))
}

#[test]
fn a_v2_handoff_streams_every_part_and_checks_each_against_the_summary() -> anyhow::Result<()> {
    let (directory, summary) = v2(&[
        ("part-00000.jsonl", &["9999900001", "9999900002"]),
        ("nested/part-00001.jsonl", &["9999900003"]),
        ("part-00002.jsonl", &[]),
    ])?;
    summary.validate("parcels")?;
    let prepared = prepare_handoff(&summary, directory.path(), Vec::new())?;
    assert_eq!(prepared.ids.len(), 3);
    assert_eq!(prepared.columns, ["pnu"]);
    assert!(String::from_utf8(prepared.csv)?.starts_with("\"pnu\",geometry\n\"9999900001\","));
    assert!(handoff_bytes(&summary, directory.path())? > 0);
    let repeated = v2(&[("a.jsonl", &["9999900001"]), ("b.jsonl", &["9999900001"])])?;
    assert!(prepare_handoff(&repeated.1, repeated.0.path(), Vec::new()).is_err());
    Ok(())
}

#[test]
fn a_part_with_the_wrong_sha256_or_row_count_is_refused() -> anyhow::Result<()> {
    let (directory, mut summary) = v2(&[("part-00000.jsonl", &["9999900001", "9999900002"])])?;
    let parts = summary.handoff_parts.as_mut().map(|parts| &mut parts[0]);
    let Some(part) = parts else {
        bail!("fixture has a part");
    };
    let truth = part.sha256.clone();
    part.sha256 = "0".repeat(64);
    let refusal = prepare_handoff(&summary, directory.path(), Vec::new())
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(refusal.contains("hashes to"), "wrong sha256: {refusal}");

    let (directory, mut summary) = v2(&[("part-00000.jsonl", &["9999900001", "9999900002"])])?;
    if let Some(parts) = summary.handoff_parts.as_mut() {
        parts[0].rows = 3;
        parts[0].sha256 = truth;
    }
    summary.served_row_count = 3;
    summary.validate("parcels")?;
    assert!(prepare_handoff(&summary, directory.path(), Vec::new()).is_err());
    Ok(())
}

#[test]
fn a_v2_summary_names_safe_distinct_parts_that_add_up() -> anyhow::Result<()> {
    let check = |edit: &dyn Fn(&mut ServedSummary)| -> anyhow::Result<bool> {
        let (_directory, mut summary) = v2(&[("part-00000.jsonl", &["9999900001"])])?;
        edit(&mut summary);
        Ok(summary.validate("parcels").is_ok())
    };
    assert!(check(&|_| {})?);
    for escape in ["../outside.jsonl", "/etc/parts.jsonl", "./part.jsonl", ""] {
        assert!(
            !check(&|summary| {
                if let Some(parts) = summary.handoff_parts.as_mut() {
                    parts[0].path = escape.to_owned();
                }
            })?,
            "{escape:?} is refused"
        );
    }
    assert!(!check(&|summary| {
        if let Some(parts) = summary.handoff_parts.as_mut() {
            parts[0].sha256 = parts[0].sha256.to_uppercase();
        }
    })?);
    assert!(!check(&|summary| summary.served_row_count = 2)?);
    assert!(!check(&|summary| summary.source_snapshot_id = None)?);
    assert!(!check(&|summary| summary.handoff_parts = Some(Vec::new()))?);
    assert!(!check(&|summary| {
        if let Some(parts) = summary.handoff_parts.as_mut() {
            parts.push(parts[0].clone());
        }
        summary.served_row_count = 2;
    })?);
    let mut v1_with_parts = summary(1);
    v1_with_parts.handoff_parts = Some(Vec::new());
    assert!(v1_with_parts.validate("complex").is_err());
    Ok(())
}

fn complex_layer() -> ServedLayer {
    ServedLayer {
        source_layer: "complex".to_owned(),
        feature_id_property: "complex_id".to_owned(),
        tile_min_zoom: 6,
        tile_max_zoom: 16,
        properties: vec!["complex_id".to_owned(), "official_complex_code".to_owned()],
    }
}

fn header(min: u8, max: u8, tile_type: u8) -> Vec<u8> {
    let mut header = vec![0_u8; 127];
    header[..7].copy_from_slice(b"PMTiles");
    header[7] = 3;
    header[97] = 2;
    header[98] = 2;
    header[99] = tile_type;
    header[100] = min;
    header[101] = max;
    header
}

#[test]
fn the_archive_header_must_be_mvt_over_the_layer_zooms() {
    assert!(check_pmtiles_header(&header(6, 16, 1), &complex_layer()).is_ok());
    assert!(check_pmtiles_header(&header(6, 14, 1), &complex_layer()).is_err());
    assert!(
        check_pmtiles_header(&header(6, 16, 2), &complex_layer()).is_err(),
        "not MVT"
    );
    let mut v2 = header(6, 16, 1);
    v2[7] = 2;
    assert!(check_pmtiles_header(&v2, &complex_layer()).is_err());
    let mut zstd = header(6, 16, 1);
    zstd[98] = 4;
    assert!(check_pmtiles_header(&zstd, &complex_layer()).is_err());
    assert!(check_pmtiles_header(&[0; 10], &complex_layer()).is_err());
}

/// The pmtiles fixture tiles carry `pnu` and `kind` in layer `parcel`; zoom 2 is tile ids 5..21.
fn parcel_layer(min: u8, max: u8) -> ServedLayer {
    ServedLayer {
        source_layer: "parcel".to_owned(),
        feature_id_property: "pnu".to_owned(),
        tile_min_zoom: min,
        tile_max_zoom: max,
        properties: vec!["pnu".to_owned()],
    }
}

fn served(ids: &[&str]) -> ServedIds {
    ServedIds::from_hashes(ids.iter().map(|id| id_hash(id)).collect())
        .unwrap_or_else(|_| panic!("distinct fixture ids"))
}

fn gate(archive_bytes: &[u8], layer: &ServedLayer, ids: &[&str]) -> anyhow::Result<String> {
    let file = written(archive_bytes)?;
    let columns = ["pnu".to_owned(), "kind".to_owned()];
    gate_archive(file.path(), layer, &columns, &served(ids), |missing| {
        Ok(ids
            .iter()
            .filter(|id| missing.contains(&id_hash(id)))
            .map(|id| (*id).to_owned())
            .collect())
    })
}

fn two_zoom_archive(min: u8, max: u8) -> Vec<u8> {
    archive(
        min,
        max,
        &[
            (1, tile("parcel", &["9999900009"])),
            (5, tile("parcel", &["9999900001", "9999900002"])),
            (9, tile("parcel", &["9999900002", "9999900003"])),
        ],
    )
}

#[test]
fn the_gate_passes_when_maxzoom_carries_exactly_the_served_ids() -> anyhow::Result<()> {
    let digest = gate(
        &two_zoom_archive(1, 2),
        &parcel_layer(1, 2),
        &["9999900001", "9999900002", "9999900003"],
    )?;
    assert_eq!(digest.len(), 64);
    assert_eq!(
        digest,
        served(&["9999900003", "9999900001", "9999900002"]).digest()
    );
    Ok(())
}

#[test]
fn a_served_id_missing_from_maxzoom_is_refused_and_named() {
    let refusal = gate(
        &two_zoom_archive(1, 2),
        &parcel_layer(1, 2),
        &["9999900001", "9999900002", "9999900003", "9999900004"],
    )
    .err()
    .map(|error| error.to_string())
    .unwrap_or_default();
    assert!(
        refusal.contains("miss 1 of the 4") && refusal.contains("9999900004"),
        "{refusal}"
    );
}

#[test]
fn an_extra_id_at_maxzoom_is_refused_and_named() {
    // 9999900009 is only at zoom 1, so it is not extra; 9999900003 is extra at zoom 2.
    let refusal = gate(
        &two_zoom_archive(1, 2),
        &parcel_layer(1, 2),
        &["9999900001", "9999900002"],
    )
    .err()
    .map(|error| error.to_string())
    .unwrap_or_default();
    assert!(
        refusal.contains("carry 1 feature") && refusal.contains("9999900003"),
        "{refusal}"
    );
}

#[test]
fn an_archive_whose_header_zooms_differ_from_the_layer_is_refused() {
    let ids = ["9999900001", "9999900002", "9999900003"];
    assert!(gate(&two_zoom_archive(1, 3), &parcel_layer(1, 2), &ids).is_err());
    assert!(gate(&two_zoom_archive(0, 2), &parcel_layer(1, 2), &ids).is_err());
}

#[test]
fn free_work_disk_below_the_minimum_is_refused() {
    let disk = WorkDisk {
        min_free_bytes: 1_000,
        min_free_bytes_per_handoff_byte: 8,
    };
    assert!(
        ensure_work_disk(1_000, 10, &disk).is_ok(),
        "the floor rules a small handoff"
    );
    assert!(ensure_work_disk(999, 10, &disk).is_err());
    assert!(ensure_work_disk(8_000, 1_000, &disk).is_ok());
    assert!(
        ensure_work_disk(7_999, 1_000, &disk).is_err(),
        "a large handoff needs its multiple"
    );
    assert!(ensure_work_disk(u64::MAX - 1, u64::MAX, &disk).is_err());
}

#[cfg(unix)]
#[test]
fn free_bytes_measures_the_filesystem_of_a_path() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    assert!(free_bytes(directory.path())? > 0);
    assert!(free_bytes(&directory.path().join("absent")).is_err());
    Ok(())
}

#[test]
fn the_contract_caps_every_container_and_the_runs_pass_the_caps() -> anyhow::Result<()> {
    let contract = BakeContract::parse(CONTRACT_JSON)?;
    for image in [&contract.images.gdal, &contract.images.tippecanoe] {
        let args = run_args(Path::new("/work"), image);
        let at = args
            .iter()
            .position(|arg| arg == "--memory")
            .context("--memory")?;
        assert_eq!(args[at + 1], OsString::from(&image.memory_limit));
        let swap = args
            .iter()
            .position(|arg| arg == "--memory-swap")
            .context("--memory-swap")?;
        assert_eq!(args[swap + 1], OsString::from(&image.memory_limit));
        assert_eq!(args.last(), Some(&OsString::from(&image.image)));
    }
    assert!(contract.work_disk.min_free_bytes > 0);
    Ok(())
}

#[test]
fn a_container_profile_without_a_memory_cap_is_refused() -> anyhow::Result<()> {
    let mut contract: Value = serde_json::from_str(CONTRACT_JSON)?;
    if let Some(gdal) = contract
        .pointer_mut("/images/gdal")
        .and_then(Value::as_object_mut)
    {
        gdal.remove("memory_limit");
    }
    assert!(BakeContract::parse(&contract.to_string()).is_err());
    for bad in ["lots", "0g", "8", "8gb", "-1g", ""] {
        let mut contract: Value = serde_json::from_str(CONTRACT_JSON)?;
        if let Some(tippecanoe) = contract
            .pointer_mut("/images/tippecanoe")
            .and_then(Value::as_object_mut)
        {
            tippecanoe.insert("memory_limit".to_owned(), json!(bad));
        }
        assert!(
            BakeContract::parse(&contract.to_string()).is_err(),
            "{bad:?} is refused"
        );
    }
    let mut no_disk: Value = serde_json::from_str(CONTRACT_JSON)?;
    if let Some(object) = no_disk.as_object_mut() {
        object.remove("work_disk");
    }
    assert!(BakeContract::parse(&no_disk.to_string()).is_err());
    Ok(())
}

#[test]
fn tippecanoe_keeps_the_measured_flags_and_adds_only_temp_and_parallel_read() {
    let args: Vec<String> = tippecanoe_args(&complex_layer(), &complex_layer().properties)
        .into_iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        args,
        [
            "-o",
            "/w/unit.pmtiles",
            "-t",
            "/w/tmp",
            "-P",
            "-l",
            "complex",
            "-Z",
            "6",
            "-z",
            "16",
            "--no-feature-limit",
            "--no-tile-size-limit",
            "--no-tiny-polygon-reduction",
            "--detect-shared-borders",
            "--force",
            "--quiet",
            "-y",
            "complex_id",
            "-y",
            "official_complex_code",
            "/w/served.geojsons",
        ]
    );
}
