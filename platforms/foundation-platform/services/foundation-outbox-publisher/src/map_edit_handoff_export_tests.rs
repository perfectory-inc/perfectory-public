use super::*;
use serde_json::json;

// The reserved synthetic coordinate namespace of scripts/guard/public-fixture-safety.py; the range
// is half-open, so every corner stays strictly inside it.
fn square() -> Value {
    json!({"type": "Polygon", "coordinates": [[
        [127.1231, 36.1231], [127.1234, 36.1231], [127.1234, 36.1234], [127.1231, 36.1234], [127.1231, 36.1231]
    ]]})
}

fn edit<'a>(op: &'a str, geometry: Option<&'a Value>, properties: &'a Value) -> StoredEditView<'a> {
    StoredEditView {
        change_seq: 7,
        feature_id: "00000000-0000-5000-8000-000000000001",
        op,
        geometry,
        properties,
        editor: "00000000-0000-0000-0000-000000000000",
        edited_at: "2099-01-01T00:00:00Z",
    }
}

#[test]
fn the_projection_origin_lands_on_the_silver_false_origin() -> anyhow::Result<()> {
    let projection = SilverProjection::for_srid(SILVER_SRID)?;
    // EPSG:5186's own definition: lat_0=38, lon_0=127 maps to x_0=200000, y_0=600000.
    let (x, y) = projection.project(127.0, 38.0)?; // public-repository-safety: reviewed-runtime-coordinate
    assert!((x - 200_000.0).abs() < 1e-6, "x={x}");
    assert!((y - 600_000.0).abs() < 1e-6, "y={y}");
    Ok(())
}

#[test]
fn an_upsert_becomes_a_little_endian_wkb_multipolygon_in_the_silver_crs() -> anyhow::Result<()> {
    let projection = SilverProjection::for_srid(SILVER_SRID)?;
    let properties = json!({"official_complex_code": "SYN"});
    let geometry = square();
    let row = handoff_row(
        "complex",
        edit("upsert", Some(&geometry), &properties),
        &projection,
    )?;
    let wkb = (0..row.geometry_wkb_hex.as_deref().unwrap_or_default().len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(
                &row.geometry_wkb_hex.as_deref().unwrap_or_default()[index..index + 2],
                16,
            )
        })
        .collect::<Result<Vec<u8>, _>>()?;
    assert_eq!(
        &wkb[..9],
        &[1, 6, 0, 0, 0, 1, 0, 0, 0],
        "byte order, MultiPolygon, one part"
    );
    assert_eq!(
        &wkb[9..18],
        &[1, 3, 0, 0, 0, 1, 0, 0, 0],
        "byte order, Polygon, one ring"
    );
    assert_eq!(
        u32::from_le_bytes(wkb[18..22].try_into()?),
        5,
        "five positions"
    );
    let first_x = f64::from_le_bytes(wkb[22..30].try_into()?);
    let first_y = f64::from_le_bytes(wkb[30..38].try_into()?);
    let (x, y) = projection.project(127.1231, 36.1231)?;
    assert_eq!((first_x, first_y), (x, y));
    assert_eq!(wkb.len(), 22 + 5 * 16);
    assert_eq!(
        row.geometry_checksum_sha256.as_deref(),
        Some(format!("{:x}", Sha256::digest(&wkb)).as_str())
    );
    assert_eq!(row.geometry_srid, SILVER_SRID);
    assert_eq!(
        row.geometry_geojson.as_deref(),
        Some(serde_json::to_string(&geometry)?.as_str())
    );
    assert_eq!(row.properties_json, r#"{"official_complex_code":"SYN"}"#);
    Ok(())
}

#[test]
fn a_delete_carries_no_geometry() -> anyhow::Result<()> {
    let projection = SilverProjection::for_srid(SILVER_SRID)?;
    let properties = json!({});
    let row = handoff_row("complex", edit("delete", None, &properties), &projection)?;
    assert_eq!(
        (
            row.geometry_geojson,
            row.geometry_wkb_hex,
            row.geometry_checksum_sha256
        ),
        (None, None, None)
    );
    Ok(())
}

#[test]
fn an_invalid_or_mismatched_edit_is_refused_before_it_becomes_lakehouse_data() -> anyhow::Result<()>
{
    let projection = SilverProjection::for_srid(SILVER_SRID)?;
    let properties = json!({});
    let bow_tie = json!({"type": "Polygon", "coordinates": [[
        [127.1231, 36.1231], [127.1234, 36.1234], [127.1234, 36.1231], [127.1231, 36.1234], [127.1231, 36.1231]
    ]]});
    let geometry = square();
    for view in [
        edit("upsert", Some(&bow_tie), &properties),
        edit("upsert", None, &properties),
        edit("delete", Some(&geometry), &properties),
        edit("update", Some(&geometry), &properties),
    ] {
        assert!(handoff_row("complex", view, &projection).is_err());
    }
    let not_an_object = json!([1]);
    assert!(handoff_row("complex", edit("delete", None, &not_an_object), &projection).is_err());
    Ok(())
}

#[test]
fn a_unit_kept_in_epsg_4326_gets_the_saved_coordinates_unchanged() -> anyhow::Result<()> {
    let projection = SilverProjection::for_srid(4326)?;
    let properties = json!({"canonical_code": "SYN"});
    let geometry = square();
    let row = handoff_row(
        "admin",
        edit("upsert", Some(&geometry), &properties),
        &projection,
    )?;
    assert_eq!(row.geometry_srid, 4326);
    let hex = row.geometry_wkb_hex.unwrap_or_default();
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16))
        .collect::<Result<Vec<u8>, _>>()?;
    assert_eq!(f64::from_le_bytes(bytes[22..30].try_into()?), 127.1231);
    assert_eq!(f64::from_le_bytes(bytes[30..38].try_into()?), 36.1231);
    assert!(SilverProjection::for_srid(3857).is_err());
    Ok(())
}
