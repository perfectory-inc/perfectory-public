use super::*;
use serde_json::json;

fn summary(rows: usize) -> ServedSummary {
    serde_json::from_value(json!({
        "schema_version": SERVED_SUMMARY_SCHEMA,
        "unit": "complex",
        "canonical_iceberg_snapshot_id": "841361364657368625",
        "edits_through_change_seq": 2,
        "served_row_count": rows,
        "status": "ready",
    }))
    .unwrap_or_else(|error| panic!("summary fixture: {error}"))
}

fn line(id: &str, code: &str) -> String {
    json!({
        "complex_id": id, "official_complex_code": code, "geometry_wkb_hex": "0106000000",
        "geometry_srid": 5186, "geometry_checksum_sha256": "a", "origin": "source"
    })
    .to_string()
}

fn layer() -> ServedLayer {
    ServedLayer {
        source_layer: "complex".to_owned(),
        feature_id_property: "complex_id".to_owned(),
        tile_min_zoom: 6,
        tile_max_zoom: 16,
        properties: vec!["complex_id".to_owned(), "official_complex_code".to_owned()],
    }
}

#[test]
fn the_summary_must_be_a_ready_nonempty_snapshot_of_the_unit() {
    assert!(summary(1).validate("complex").is_ok());
    assert!(summary(1).validate("admin").is_err());
    assert!(
        summary(0).validate("complex").is_err(),
        "an empty snapshot would erase the layer"
    );
}

#[test]
fn served_rows_are_read_once_each_and_counted_against_the_summary() -> anyhow::Result<()> {
    let text = format!("{}\n{}\n", line("a", "SYN-1"), line("b", "SYN-2"));
    assert_eq!(read_served_rows(&text, &summary(2))?.len(), 2);
    assert!(
        read_served_rows(&text, &summary(3)).is_err(),
        "a short handoff is refused"
    );
    let repeated = format!("{}\n{}\n", line("a", "SYN-1"), line("a", "SYN-2"));
    assert!(read_served_rows(&repeated, &summary(2)).is_err());
    let wrong_crs = json!({"complex_id": "a", "official_complex_code": "x",
        "geometry_wkb_hex": "01", "geometry_srid": 4326})
    .to_string();
    assert!(read_served_rows(&wrong_crs, &summary(1)).is_err());
    Ok(())
}

#[test]
fn the_gdal_csv_quotes_codes_and_keeps_hex_wkb() -> anyhow::Result<()> {
    let rows = read_served_rows(&line("a", "SYN\"1"), &summary(1))?;
    assert_eq!(
        gdal_csv(&rows),
        "complex_id,official_complex_code,geometry\na,\"SYN\"\"1\",0106000000\n"
    );
    Ok(())
}

fn header(min: u8, max: u8, tile_type: u8) -> Vec<u8> {
    let mut header = vec![0_u8; 127];
    header[..7].copy_from_slice(b"PMTiles");
    header[7] = 3;
    header[99] = tile_type;
    header[100] = min;
    header[101] = max;
    header
}

#[test]
fn the_archive_header_must_be_mvt_over_the_layer_zooms() {
    assert!(check_pmtiles_header(&header(6, 16, 1), &layer()).is_ok());
    assert!(check_pmtiles_header(&header(6, 14, 1), &layer()).is_err());
    assert!(
        check_pmtiles_header(&header(6, 16, 2), &layer()).is_err(),
        "not MVT"
    );
    let mut v2 = header(6, 16, 1);
    v2[7] = 2;
    assert!(check_pmtiles_header(&v2, &layer()).is_err());
    assert!(check_pmtiles_header(&[0; 10], &layer()).is_err());
}

#[test]
fn decoded_ids_are_collected_across_tiles_and_a_feature_without_one_is_refused(
) -> anyhow::Result<()> {
    let decoded = json!({"features": [
        {"features": [{"features": [{"properties": {"complex_id": "a"}}, {"properties": {"complex_id": "b"}}]}]},
        {"features": [{"features": [{"properties": {"complex_id": "a"}}]}]}
    ]});
    assert_eq!(
        decoded_feature_ids(&decoded, "complex_id")?,
        BTreeSet::from(["a".to_owned(), "b".to_owned()])
    );
    let broken = json!({"features": [{"features": [{"features": [{"properties": {}}]}]}]});
    assert!(decoded_feature_ids(&broken, "complex_id").is_err());
    Ok(())
}
