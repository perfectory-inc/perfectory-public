//! Exports the edit store's unfolded edits as the handoff the served-Gold Spark job reads
//! (root ADR-0112 §7).
//!
//! Silver keeps boundaries in their source CRS (EPSG:5186) and the Spark image has no projection
//! library (root ADR-0042), so an edit's polygon, saved in EPSG:4326, is reprojected here into the
//! Silver CRS and written as WKB. The original GeoJSON travels too, so the lakehouse ledger keeps
//! exactly what the administrator saved. Each geometry is checked again with the same OGC validity
//! rules the save path applied: this command is the last step before the edit becomes lakehouse data.

use std::io::Write as _;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, ensure, Context as _};
use catalog_domain::MapEditGeometry;
use proj4rs::{proj::Proj, transform::transform};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::public_data_control_support::{optional_env_value, required_env_value};

const PREFIX: &str = "FOUNDATION_PLATFORM_MAP_EDIT_HANDOFF";
const GATEWAY_BASE_URL_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL";
const WRITE_TOKEN_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN";
/// The Silver boundary CRS (`silver.industrial_complex_boundaries`: `geometry_srid = 5186`).
pub(crate) const SILVER_SRID: i32 = 5186;
const EPSG_5186_PROJ: &str =
    "+proj=tmerc +lat_0=38 +lon_0=127 +k=1 +x_0=200000 +y_0=600000 +ellps=GRS80 +units=m +no_defs";
const EPSG_4326_PROJ: &str = "+proj=longlat +ellps=GRS80 +no_defs";
const PAGE_SIZE: usize = 1000;
/// The edit store stops accepting edits at 5,000 pending per unit; more rows means a broken store.
const MAX_EDITS: usize = 5_000;

#[derive(Debug, Deserialize)]
struct EditsPage {
    unit: String,
    edits: Vec<StoredEdit>,
}

#[derive(Debug, Deserialize)]
struct StoredEdit {
    change_seq: u64,
    feature_id: String,
    op: String,
    geometry: Option<Value>,
    properties: Value,
    editor: String,
    edited_at: String,
}

/// One handoff row: the edit as saved, plus its polygon in the Silver CRS.
#[derive(Debug, Serialize, PartialEq)]
pub(crate) struct EditHandoffRow {
    pub(crate) unit: String,
    pub(crate) change_seq: u64,
    pub(crate) feature_id: String,
    pub(crate) op: String,
    /// The saved GeoJSON (EPSG:4326) as text; `None` for a delete.
    pub(crate) geometry_geojson: Option<String>,
    /// The polygon reprojected into the Silver CRS as little-endian WKB, hex; `None` for a delete.
    pub(crate) geometry_wkb_hex: Option<String>,
    pub(crate) geometry_srid: i32,
    pub(crate) geometry_checksum_sha256: Option<String>,
    pub(crate) properties_json: String,
    pub(crate) editor: String,
    pub(crate) edited_at: String,
}

/// Reprojects EPSG:4326 longitude/latitude into the Silver CRS.
pub(crate) struct SilverProjection {
    source: Proj,
    target: Proj,
}

impl SilverProjection {
    pub(crate) fn new() -> anyhow::Result<Self> {
        Ok(Self {
            source: Proj::from_proj_string(EPSG_4326_PROJ).context("EPSG:4326 projection")?,
            target: Proj::from_proj_string(EPSG_5186_PROJ).context("EPSG:5186 projection")?,
        })
    }

    pub(crate) fn project(&self, longitude: f64, latitude: f64) -> anyhow::Result<(f64, f64)> {
        let mut point = (longitude.to_radians(), latitude.to_radians(), 0.0_f64);
        transform(&self.source, &self.target, &mut point)
            .context("coordinate transformation to EPSG:5186 failed")?;
        ensure!(
            point.0.is_finite() && point.1.is_finite(),
            "coordinate transformation to EPSG:5186 produced non-finite output"
        );
        Ok((point.0, point.1))
    }
}

/// Encodes a checked GeoJSON Polygon/MultiPolygon as a little-endian WKB MultiPolygon in the
/// Silver CRS. Always a MultiPolygon, which is what the served table and the tiles carry.
pub(crate) fn multipolygon_wkb(
    geometry: &MapEditGeometry,
    projection: &SilverProjection,
) -> anyhow::Result<Vec<u8>> {
    let json = geometry.as_json();
    let polygons: Vec<&Value> = match json.get("type").and_then(Value::as_str) {
        Some("Polygon") => vec![json.get("coordinates").context("coordinates")?],
        Some("MultiPolygon") => json
            .get("coordinates")
            .and_then(Value::as_array)
            .context("coordinates")?
            .iter()
            .collect(),
        _ => bail!("not a polygonal geometry"),
    };
    let mut wkb = Vec::new();
    wkb.push(1_u8);
    wkb.extend_from_slice(&6_u32.to_le_bytes());
    wkb.extend_from_slice(&u32::try_from(polygons.len())?.to_le_bytes());
    for polygon in polygons {
        let rings = polygon.as_array().context("polygon rings")?;
        wkb.push(1_u8);
        wkb.extend_from_slice(&3_u32.to_le_bytes());
        wkb.extend_from_slice(&u32::try_from(rings.len())?.to_le_bytes());
        for ring in rings {
            let positions = ring.as_array().context("ring positions")?;
            wkb.extend_from_slice(&u32::try_from(positions.len())?.to_le_bytes());
            for position in positions {
                let pair = position.as_array().context("position")?;
                let (Some(lon), Some(lat)) = (
                    pair.first().and_then(Value::as_f64),
                    pair.get(1).and_then(Value::as_f64),
                ) else {
                    bail!("position is not two numbers");
                };
                let (x, y) = projection.project(lon, lat)?;
                wkb.extend_from_slice(&x.to_le_bytes());
                wkb.extend_from_slice(&y.to_le_bytes());
            }
        }
    }
    Ok(wkb)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Turns one stored edit into its handoff row, refusing anything the save path would have refused.
pub(crate) fn handoff_row(
    unit: &str,
    edit: StoredEditView<'_>,
    projection: &SilverProjection,
) -> anyhow::Result<EditHandoffRow> {
    let (geometry_geojson, geometry_wkb_hex, geometry_checksum_sha256) =
        match (edit.op, edit.geometry) {
            ("upsert", Some(geometry)) => {
                let checked = MapEditGeometry::parse(geometry.clone()).with_context(|| {
                    format!("edit {} carries an invalid polygon", edit.change_seq)
                })?;
                let wkb = multipolygon_wkb(&checked, projection)?;
                (
                    Some(serde_json::to_string(checked.as_json())?),
                    Some(hex(&wkb)),
                    Some(format!("{:x}", Sha256::digest(&wkb))),
                )
            }
            ("delete", None) => (None, None, None),
            (op, _) => bail!(
                "edit {} has op {op} with a mismatched geometry",
                edit.change_seq
            ),
        };
    ensure!(
        edit.properties.is_object(),
        "edit {} properties are not an object",
        edit.change_seq
    );
    Ok(EditHandoffRow {
        unit: unit.to_owned(),
        change_seq: edit.change_seq,
        feature_id: edit.feature_id.to_owned(),
        op: edit.op.to_owned(),
        geometry_geojson,
        geometry_wkb_hex,
        geometry_srid: SILVER_SRID,
        geometry_checksum_sha256,
        properties_json: serde_json::to_string(edit.properties)?,
        editor: edit.editor.to_owned(),
        edited_at: edit.edited_at.to_owned(),
    })
}

/// Borrowed view of a stored edit, so tests can build one without the wire type.
pub(crate) struct StoredEditView<'a> {
    pub(crate) change_seq: u64,
    pub(crate) feature_id: &'a str,
    pub(crate) op: &'a str,
    pub(crate) geometry: Option<&'a Value>,
    pub(crate) properties: &'a Value,
    pub(crate) editor: &'a str,
    pub(crate) edited_at: &'a str,
}

/// Reads every unfolded edit of `unit`, in change order, one page at a time.
async fn read_edits(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    unit: &str,
) -> anyhow::Result<Vec<StoredEdit>> {
    let mut edits: Vec<StoredEdit> = Vec::new();
    let mut after = 0_u64;
    loop {
        let url = format!(
            "{}/edits/{unit}?after={after}&limit={PAGE_SIZE}",
            base_url.trim_end_matches('/')
        );
        let page: EditsPage = client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .context("edit store unreachable")?
            .error_for_status()
            .context("edit store refused the edit listing")?
            .json()
            .await
            .context("edit store listing is not the expected JSON")?;
        ensure!(
            page.unit == unit,
            "edit store answered for unit {}",
            page.unit
        );
        let count = page.edits.len();
        for edit in page.edits {
            ensure!(
                edit.change_seq > after,
                "edit store listing is not in change order"
            );
            after = edit.change_seq;
            edits.push(edit);
        }
        ensure!(
            edits.len() <= MAX_EDITS,
            "edit store holds more edits than its own limit"
        );
        if count < PAGE_SIZE {
            return Ok(edits);
        }
    }
}

/// Writes the unit's unfolded edits as JSONL and prints how far they reach.
///
/// # Errors
/// Refuses missing configuration, an unreachable store, an invalid edit, or an existing output.
pub async fn run() -> anyhow::Result<()> {
    let unit = required_env_value(&format!("{PREFIX}_UNIT"))?;
    let output = PathBuf::from(required_env_value(&format!("{PREFIX}_OUTPUT"))?);
    let base_url = required_env_value(GATEWAY_BASE_URL_ENV)?;
    let token = required_env_value(WRITE_TOKEN_ENV)?;
    let timeout = optional_env_value(&format!("{PREFIX}_TIMEOUT_SECONDS"))?
        .map(|value| value.parse::<u64>())
        .transpose()
        .context("timeout must be an integer")?
        .unwrap_or(30);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let edits = read_edits(&client, &base_url, &token, &unit).await?;
    let projection = SilverProjection::new()?;
    // Never overwritten: a handoff is evidence of what one fold carried.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)
        .with_context(|| format!("{} already exists or cannot be created", output.display()))?;
    for edit in &edits {
        let row = handoff_row(
            &unit,
            StoredEditView {
                change_seq: edit.change_seq,
                feature_id: &edit.feature_id,
                op: &edit.op,
                geometry: edit.geometry.as_ref(),
                properties: &edit.properties,
                editor: &edit.editor,
                edited_at: &edit.edited_at,
            },
            &projection,
        )?;
        serde_json::to_writer(&mut file, &row)?;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    let latest = edits.last().map_or(0, |edit| edit.change_seq);
    println!(
        "map-edit-handoff-export-ok unit={unit} edits={} through_change_seq={latest} output={}",
        edits.len(),
        output.display()
    );
    Ok(())
}

#[cfg(test)]
#[path = "map_edit_handoff_export_tests.rs"]
mod tests;
