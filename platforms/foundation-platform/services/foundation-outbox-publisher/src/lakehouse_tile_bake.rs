//! Bakes a polygon unit's static PMTiles release from a served Gold snapshot and folds the admin
//! edits it contains (root ADR-0112 §7·§9).
//!
//! The input is what `industrial_complex_boundary_served_gold.py` wrote: the served rows (Silver
//! with every ledgered edit applied, EPSG:5186 WKB) and a summary naming the Gold snapshot and the
//! last edit it includes. In order:
//!
//! 1. start a `lakehouse_bake` build against the active validated static release;
//! 2. reproject and repair with GDAL (`-makevalid`), tile with tippecanoe — both pinned containers
//!    (`config/tile-bake-containers.contract.json`);
//! 3. gate: the archive is PMTiles v3 MVT over the layer's zoom range, and its maxzoom tiles carry
//!    exactly the served feature ids;
//! 4. upload create-only and rehash, record the result, promote (the unit is left with no fallback);
//! 5. record the fold in the edit store, which retires the folded edits from the customer overlay.
//!
//! No PostGIS is read or written. A failure before the promotion records the build as failed and
//! changes nothing that is served.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context as _};
use catalog_application::ports::{
    PromoteTileLayerStaticCommand, RecordVectorTileBuildResultCommand,
    RuntimeManifestPublicationCapability, StartLakehouseBakeCommand,
};
use catalog_application::{PromoteTileLayerStatic, VectorTileBuildLifecycle};
use catalog_domain::{
    static_file_asset_id_for_build, static_release_id_for_build, static_release_martin_source_id,
    static_release_pmtiles_object_key, BuildEvidenceDigest, CanonicalIcebergSnapshotId,
    PmtilesChecksum, RuntimeTilesUrlTemplate, ServingGeneration, ValidatedPmtilesArtifact,
    VectorTileBuildOutcome,
};
use catalog_infrastructure::PgCatalogUnitOfWork;
use foundation_outbox::object_storage::R2ObjectStorage;
use foundation_shared_kernel::ids::{StaffId, VectorTileBuildJobId, VectorTileReleaseId};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sqlx::{PgPool, Row as _};
use tokio::process::Command;
use uuid::Uuid;

use crate::boundary_static_release_publish::{
    bounded_failure_reason, create_only_upload_and_rehash, ensure_tool_success,
};
use crate::public_data_control_support::{optional_env_value, required_env_value};
use crate::static_release_url::public_tiles_base_url;
use crate::tile_derivative_object_storage::TileDerivativeR2Config;

const PREFIX: &str = "FOUNDATION_PLATFORM_LAKEHOUSE_TILE_BAKE";
const IMAGES_JSON: &str = include_str!("../../../config/tile-bake-containers.contract.json");
const SERVED_SUMMARY_SCHEMA: &str =
    "foundation-platform.industrial_complex_boundary_served_gold.v1";
const MAP_EDIT_GATEWAY_BASE_URL_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_GATEWAY_BASE_URL";
const MAP_EDIT_WRITE_TOKEN_ENV: &str = "FOUNDATION_PLATFORM_MAP_EDIT_WRITE_TOKEN";

#[derive(Deserialize)]
struct Images {
    images: ImageSet,
}

#[derive(Deserialize)]
struct ImageSet {
    gdal: Image,
    tippecanoe: Image,
}

#[derive(Deserialize)]
struct Image {
    image: String,
    #[serde(default)]
    version_banner: Option<String>,
}

/// What the served-Gold job recorded about the state it wrote.
#[derive(Debug, Deserialize)]
pub(crate) struct ServedSummary {
    schema_version: String,
    pub(crate) unit: String,
    pub(crate) canonical_iceberg_snapshot_id: String,
    pub(crate) edits_through_change_seq: u64,
    pub(crate) served_row_count: usize,
    status: String,
}

impl ServedSummary {
    pub(crate) fn validate(&self, unit: &str) -> anyhow::Result<()> {
        ensure!(
            self.schema_version == SERVED_SUMMARY_SCHEMA,
            "served summary schema is {}",
            self.schema_version
        );
        ensure!(self.status == "ready", "served summary is not ready");
        ensure!(
            self.unit == unit,
            "served summary is for unit {}, not {unit}",
            self.unit
        );
        ensure!(
            self.served_row_count > 0,
            "the served snapshot is empty; baking it would erase the layer"
        );
        Ok(())
    }
}

/// One served row, as the tile bake reads it.
#[derive(Debug, Deserialize)]
pub(crate) struct ServedRow {
    pub(crate) complex_id: String,
    pub(crate) official_complex_code: String,
    pub(crate) geometry_wkb_hex: String,
    pub(crate) geometry_srid: i32,
}

/// Parses the served handoff and returns its rows, refusing repeats and a count the summary did not
/// promise.
pub(crate) fn read_served_rows(
    text: &str,
    summary: &ServedSummary,
) -> anyhow::Result<Vec<ServedRow>> {
    let mut ids = BTreeSet::new();
    let mut rows = Vec::new();
    for (number, line) in text
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
    {
        let row: ServedRow = serde_json::from_str(line)
            .with_context(|| format!("served handoff line {} is not a served row", number + 1))?;
        ensure!(
            row.geometry_srid == 5186,
            "served row {} is not EPSG:5186",
            row.complex_id
        );
        ensure!(
            !row.geometry_wkb_hex.is_empty()
                && row.geometry_wkb_hex.bytes().all(|b| b.is_ascii_hexdigit()),
            "served row {} has no WKB",
            row.complex_id
        );
        ensure!(
            ids.insert(row.complex_id.clone()),
            "complex {} is served twice",
            row.complex_id
        );
        rows.push(row);
    }
    ensure!(
        rows.len() == summary.served_row_count,
        "served handoff holds {} rows, the summary promised {}",
        rows.len(),
        summary.served_row_count
    );
    Ok(rows)
}

/// The GDAL input: one CSV row per feature, geometry as hex WKB.
pub(crate) fn gdal_csv(rows: &[ServedRow]) -> String {
    let mut csv = String::from("complex_id,official_complex_code,geometry\n");
    for row in rows {
        let code = row.official_complex_code.replace('"', "\"\"");
        csv.push_str(&format!(
            "{},\"{code}\",{}\n",
            row.complex_id, row.geometry_wkb_hex
        ));
    }
    csv
}

/// The layer the active release serves, which the new archive must reproduce.
#[derive(Debug, Clone)]
pub(crate) struct ServedLayer {
    pub(crate) source_layer: String,
    pub(crate) feature_id_property: String,
    pub(crate) tile_min_zoom: u8,
    pub(crate) tile_max_zoom: u8,
    pub(crate) properties: Vec<String>,
}

/// Checks a PMTiles v3 header: MVT tiles over exactly the layer's zoom range.
pub(crate) fn check_pmtiles_header(header: &[u8], layer: &ServedLayer) -> anyhow::Result<()> {
    ensure!(
        header.len() >= 127,
        "archive is shorter than a PMTiles header"
    );
    ensure!(
        &header[..7] == b"PMTiles" && header[7] == 3,
        "archive is not PMTiles v3"
    );
    ensure!(header[99] == 1, "archive tiles are not MVT");
    ensure!(
        (header[100], header[101]) == (layer.tile_min_zoom, layer.tile_max_zoom),
        "archive zooms {}..{} differ from the layer's {}..{}",
        header[100],
        header[101],
        layer.tile_min_zoom,
        layer.tile_max_zoom
    );
    Ok(())
}

/// The feature ids `tippecanoe-decode` found in the decoded tiles.
pub(crate) fn decoded_feature_ids(
    decoded: &Value,
    id_property: &str,
) -> anyhow::Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    for tile in decoded
        .get("features")
        .and_then(Value::as_array)
        .context("decode has no tiles")?
    {
        for layer in tile
            .get("features")
            .and_then(Value::as_array)
            .context("tile has no layers")?
        {
            for feature in layer
                .get("features")
                .and_then(Value::as_array)
                .context("layer has no features")?
            {
                let id = feature
                    .pointer(&format!("/properties/{id_property}"))
                    .and_then(Value::as_str)
                    .with_context(|| format!("a feature has no {id_property}"))?;
                ids.insert(id.to_owned());
            }
        }
    }
    Ok(ids)
}

struct Config {
    unit_key: String,
    served_handoff: PathBuf,
    served_summary: PathBuf,
    work_root: PathBuf,
    public_tiles_base_url: String,
    operator_staff_id: StaffId,
    build_idempotency_key: String,
    promote_idempotency_key: String,
    tool_timeout: Duration,
    edit_store: Option<(String, String)>,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let confirm = format!("{PREFIX}_CONFIRM");
        ensure!(
            required_env_value(&confirm)? == "1",
            "{confirm}=1 is required; a lakehouse bake moves the serving pointer"
        );
        let var = |suffix: &str| required_env_value(&format!("{PREFIX}_{suffix}"));
        let timeout: u64 = var("TOOL_TIMEOUT_SECONDS")?
            .parse()
            .context("TOOL_TIMEOUT_SECONDS")?;
        ensure!(
            (1..=43_200).contains(&timeout),
            "TOOL_TIMEOUT_SECONDS must be in 1..=43200"
        );
        let edit_store = match (
            optional_env_value(MAP_EDIT_GATEWAY_BASE_URL_ENV)?,
            optional_env_value(MAP_EDIT_WRITE_TOKEN_ENV)?,
        ) {
            (Some(url), Some(token)) => Some((url, token)),
            (None, None) => None,
            _ => {
                bail!("{MAP_EDIT_GATEWAY_BASE_URL_ENV} and {MAP_EDIT_WRITE_TOKEN_ENV} go together")
            }
        };
        Ok(Self {
            unit_key: var("UNIT")?,
            served_handoff: PathBuf::from(var("SERVED_HANDOFF")?),
            served_summary: PathBuf::from(var("SERVED_SUMMARY")?),
            work_root: PathBuf::from(var("WORK_ROOT")?),
            public_tiles_base_url: public_tiles_base_url(&var("PUBLIC_TILES_BASE_URL")?)
                .context("PUBLIC_TILES_BASE_URL is not a public tile address")?,
            operator_staff_id: StaffId::new(
                Uuid::parse_str(&var("OPERATOR_STAFF_ID")?).context("OPERATOR_STAFF_ID")?,
            ),
            build_idempotency_key: var("BUILD_IDEMPOTENCY_KEY")?,
            promote_idempotency_key: var("PROMOTE_IDEMPOTENCY_KEY")?,
            tool_timeout: Duration::from_secs(timeout),
            edit_store,
        })
    }
}

struct ActiveStatic {
    release_id: VectorTileReleaseId,
    serving_generation: ServingGeneration,
    snapshot: String,
    layer: ServedLayer,
}

async fn read_active_static(pool: &PgPool, unit_key: &str) -> anyhow::Result<ActiveStatic> {
    let row = sqlx::query(
        "SELECT unit.active_release_id, unit.serving_generation, release.source_kind,
                release.canonical_iceberg_snapshot_id, layer.source_layer, layer.feature_id_property,
                layer.tile_min_zoom, layer.tile_max_zoom, layer.feature_filter_properties
         FROM catalog.vector_tile_publication_unit AS unit
         JOIN catalog.vector_tile_release AS release ON release.id = unit.active_release_id
         JOIN catalog.vector_tile_release_layer AS layer ON layer.release_id = release.id
         WHERE unit.unit_key = $1",
    )
    .bind(unit_key)
    .fetch_all(pool)
    .await?;
    ensure!(
        row.len() == 1,
        "the active {unit_key} release must have exactly one layer, has {}",
        row.len()
    );
    let row = &row[0];
    let kind: String = row.try_get("source_kind")?;
    ensure!(
        kind == "static_pmtiles",
        "the active {unit_key} release is {kind}; a lakehouse bake replaces a static one"
    );
    let filters: Value = row.try_get("feature_filter_properties")?;
    let feature_id_property: String = row.try_get("feature_id_property")?;
    let mut properties = vec![feature_id_property.clone()];
    properties.extend(
        filters
            .as_object()
            .map(|map| map.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default(),
    );
    Ok(ActiveStatic {
        release_id: VectorTileReleaseId::new(row.try_get("active_release_id")?),
        serving_generation: ServingGeneration::new(u64::try_from(
            row.try_get::<i64, _>("serving_generation")?,
        )?)
        .map_err(anyhow::Error::msg)?,
        snapshot: row.try_get("canonical_iceberg_snapshot_id")?,
        layer: ServedLayer {
            source_layer: row.try_get("source_layer")?,
            feature_id_property,
            tile_min_zoom: u8::try_from(row.try_get::<i16, _>("tile_min_zoom")?)?,
            tile_max_zoom: u8::try_from(row.try_get::<i16, _>("tile_max_zoom")?)?,
            properties,
        },
    })
}

async fn docker(
    args: Vec<OsString>,
    timeout: Duration,
    label: &str,
) -> anyhow::Result<std::process::Output> {
    let output = tokio::time::timeout(
        timeout,
        Command::new("docker")
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .with_context(|| format!("{label} exceeded its time bound"))?
    .with_context(|| format!("failed to start {label}"))?;
    ensure_tool_success(label, &output)?;
    Ok(output)
}

fn run_args(work: &Path, image: &str, entrypoint: Option<&str>) -> Vec<OsString> {
    let mut args: Vec<OsString> = vec![
        "run".into(),
        "--rm".into(),
        "--network".into(),
        "none".into(),
    ];
    // Run as the owner of the work directory, so the tools' outputs stay removable afterwards.
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if let Ok(meta) = std::fs::metadata(work) {
            args.push("-u".into());
            args.push(format!("{}:{}", meta.uid(), meta.gid()).into());
        }
    }
    args.push("-v".into());
    args.push(format!("{}:/w", work.display()).into());
    if let Some(entrypoint) = entrypoint {
        args.push("--entrypoint".into());
        args.push(entrypoint.into());
    }
    args.push(image.into());
    args
}

async fn build_archive(
    config: &Config,
    images: &ImageSet,
    work: &Path,
    rows: &[ServedRow],
    layer: &ServedLayer,
) -> anyhow::Result<PathBuf> {
    std::fs::write(work.join("served.csv"), gdal_csv(rows))?;
    let mut gdal = run_args(work, &images.gdal.image, None);
    gdal.extend(
        [
            "ogr2ogr",
            "-f",
            "GeoJSONSeq",
            "/w/served.geojsons",
            "/w/served.csv",
            "-oo",
            "GEOM_POSSIBLE_NAMES=geometry",
            "-oo",
            "KEEP_GEOM_COLUMNS=NO",
            "-s_srs",
            "EPSG:5186",
            "-t_srs",
            "EPSG:4326",
            "-makevalid",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-lco",
            "RS=NO",
            "-lco",
            "COORDINATE_PRECISION=7",
        ]
        .map(OsString::from),
    );
    docker(gdal, config.tool_timeout, "ogr2ogr").await?;

    let banner = docker(
        run_args(work, &images.tippecanoe.image, None)
            .into_iter()
            .chain(["--version".into()])
            .collect(),
        config.tool_timeout,
        "tippecanoe --version",
    )
    .await?;
    let reported = String::from_utf8_lossy(&banner.stderr).trim().to_owned()
        + String::from_utf8_lossy(&banner.stdout).trim();
    let expected = images
        .tippecanoe
        .version_banner
        .as_deref()
        .context("tippecanoe version_banner")?;
    ensure!(
        reported.contains(expected),
        "tippecanoe reports {reported:?}, the contract pins {expected:?}"
    );

    let mut tippecanoe = run_args(work, &images.tippecanoe.image, None);
    tippecanoe.extend(
        [
            "-o".to_owned(),
            "/w/unit.pmtiles".to_owned(),
            "-l".to_owned(),
            layer.source_layer.clone(),
            "-Z".to_owned(),
            layer.tile_min_zoom.to_string(),
            "-z".to_owned(),
            layer.tile_max_zoom.to_string(),
            "--no-feature-limit".to_owned(),
            "--no-tile-size-limit".to_owned(),
            "--no-tiny-polygon-reduction".to_owned(),
            "--detect-shared-borders".to_owned(),
            "--force".to_owned(),
            "--quiet".to_owned(),
        ]
        .map(OsString::from),
    );
    for property in &layer.properties {
        tippecanoe.push("-y".into());
        tippecanoe.push(property.into());
    }
    tippecanoe.push("/w/served.geojsons".into());
    docker(tippecanoe, config.tool_timeout, "tippecanoe").await?;
    Ok(work.join("unit.pmtiles"))
}

async fn gate_archive(
    config: &Config,
    images: &ImageSet,
    work: &Path,
    archive: &Path,
    rows: &[ServedRow],
    layer: &ServedLayer,
) -> anyhow::Result<String> {
    let mut header = vec![0_u8; 127];
    use std::io::Read as _;
    std::fs::File::open(archive)?.read_exact(&mut header)?;
    check_pmtiles_header(&header, layer)?;
    let mut decode = run_args(work, &images.tippecanoe.image, Some("tippecanoe-decode"));
    decode.extend(
        [
            "-Z".to_owned(),
            layer.tile_max_zoom.to_string(),
            "-z".to_owned(),
            layer.tile_max_zoom.to_string(),
            "/w/unit.pmtiles".to_owned(),
        ]
        .map(OsString::from),
    );
    let decoded = docker(decode, config.tool_timeout, "tippecanoe-decode").await?;
    let decoded: Value =
        serde_json::from_slice(&decoded.stdout).context("tippecanoe-decode output")?;
    let found = decoded_feature_ids(&decoded, &layer.feature_id_property)?;
    let served: BTreeSet<String> = rows.iter().map(|row| row.complex_id.clone()).collect();
    ensure!(
        found == served,
        "maxzoom tiles carry {} ids, the served snapshot {}; missing {:?}, extra {:?}",
        found.len(),
        served.len(),
        served.difference(&found).take(5).collect::<Vec<_>>(),
        found.difference(&served).take(5).collect::<Vec<_>>()
    );
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&found)?)))
}

async fn record_fold(
    config: &Config,
    through: u64,
    release_id: VectorTileReleaseId,
) -> anyhow::Result<()> {
    let Some((base_url, token)) = &config.edit_store else {
        ensure!(
            through == 0,
            "the snapshot folds edits through {through} but no edit store is configured"
        );
        return Ok(());
    };
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()?
        .post(format!("{}/folds/{}", base_url.trim_end_matches('/'), config.unit_key))
        .bearer_auth(token)
        .json(&serde_json::json!({"folded_through_change_seq": through, "release_id": release_id.to_string()}))
        .send()
        .await
        .context("edit store unreachable while recording the fold")?
        .error_for_status()
        .context("edit store refused the fold")?;
    Ok(())
}

/// Runs one lakehouse bake end to end.
///
/// # Errors
/// Refuses bad configuration or inputs; records the build as failed for any failure before
/// promotion; returns an error if the fold cannot be recorded after promotion (the tiles are
/// already correct, and re-running the fold call is safe).
pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let images: Images =
        serde_json::from_str(IMAGES_JSON).context("tile-bake container contract")?;
    let summary: ServedSummary =
        serde_json::from_str(&std::fs::read_to_string(&config.served_summary)?)
            .context("served summary")?;
    summary.validate(&config.unit_key)?;
    let rows = read_served_rows(&std::fs::read_to_string(&config.served_handoff)?, &summary)?;

    let pool = PgPool::connect(&required_env_value("DATABASE_URL")?).await?;
    let active = read_active_static(&pool, &config.unit_key).await?;
    ensure!(
        active.snapshot != summary.canonical_iceberg_snapshot_id,
        "the active release already serves Gold snapshot {}",
        active.snapshot
    );
    let uow = Arc::new(
        PgCatalogUnitOfWork::new(pool.clone())
            .with_runtime_manifest_publication(RuntimeManifestPublicationCapability::enabled()),
    );
    let lifecycle = VectorTileBuildLifecycle::new(uow.clone());
    let build_job_id = lifecycle
        .start_lakehouse_bake(StartLakehouseBakeCommand {
            unit_key: config.unit_key.clone(),
            input_release_id: active.release_id,
            canonical_iceberg_snapshot_id: CanonicalIcebergSnapshotId::new(
                summary.canonical_iceberg_snapshot_id.clone(),
            )
            .map_err(anyhow::Error::msg)?,
            idempotency_key: config.build_idempotency_key.clone(),
            operator_staff_id: config.operator_staff_id,
        })
        .await?;

    let built = bake_and_upload(
        &config,
        &images.images,
        build_job_id,
        &rows,
        &active.layer,
        &summary,
    )
    .await;
    let artifact = match built {
        Ok(artifact) => artifact,
        Err(error) => {
            lifecycle
                .record_result(RecordVectorTileBuildResultCommand {
                    build_job_id,
                    outcome: VectorTileBuildOutcome::Failed(bounded_failure_reason(&error)),
                    operator_staff_id: config.operator_staff_id,
                })
                .await
                .context("the lakehouse bake failed and its failure could not be recorded")?;
            return Err(error);
        }
    };
    let release_id = artifact.1.release_id;
    lifecycle
        .record_result(RecordVectorTileBuildResultCommand {
            build_job_id,
            outcome: VectorTileBuildOutcome::Validated {
                evidence: artifact.0,
                artifact: artifact.1,
            },
            operator_staff_id: config.operator_staff_id,
        })
        .await?;
    let manifest = PromoteTileLayerStatic::new(uow)
        .execute(PromoteTileLayerStaticCommand {
            unit_key: config.unit_key.clone(),
            build_job_id,
            expected_active_release_id: active.release_id,
            expected_serving_generation: active.serving_generation,
            idempotency_key: config.promote_idempotency_key.clone(),
            operator_staff_id: config.operator_staff_id,
        })
        .await?;
    // Only after the tiles that contain the edits are served may the overlay forget them.
    record_fold(&config, summary.edits_through_change_seq, release_id).await?;
    println!(
        "lakehouse-tile-bake-ok unit={} build_job_id={build_job_id} release_id={release_id} manifest_generation={} gold_snapshot={} folded_through_change_seq={}",
        config.unit_key,
        manifest.manifest_generation.value(),
        summary.canonical_iceberg_snapshot_id,
        summary.edits_through_change_seq
    );
    Ok(())
}

async fn bake_and_upload(
    config: &Config,
    images: &ImageSet,
    build_job_id: VectorTileBuildJobId,
    rows: &[ServedRow],
    layer: &ServedLayer,
    summary: &ServedSummary,
) -> anyhow::Result<(BuildEvidenceDigest, ValidatedPmtilesArtifact)> {
    let work = config
        .work_root
        .join(format!("{build_job_id}-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&work).with_context(|| format!("work directory {}", work.display()))?;
    let result = async {
        let archive = build_archive(config, images, &work, rows, layer).await?;
        let ids_digest = gate_archive(config, images, &work, &archive, rows, layer).await?;
        let release_id = static_release_id_for_build(build_job_id);
        let storage = TileDerivativeR2Config::from_env()?;
        let object_key = storage.release_key(&config.unit_key, &release_id.to_string())?;
        ensure!(
            object_key == static_release_pmtiles_object_key(&config.unit_key, release_id),
            "the storage prefix does not produce the release-addressed key"
        );
        let reader = R2ObjectStorage::from_config(storage.reader_config());
        let writer = R2ObjectStorage::from_config(storage.writer);
        let verified =
            create_only_upload_and_rehash(&writer, &reader, &archive, &object_key).await?;
        let source_id = static_release_martin_source_id(&config.unit_key, release_id);
        let evidence = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "kind": "lakehouse_bake",
            "build_job_id": build_job_id.to_string(),
            "release_id": release_id.to_string(),
            "gold_snapshot": summary.canonical_iceberg_snapshot_id,
            "edits_through_change_seq": summary.edits_through_change_seq,
            "served_row_count": summary.served_row_count,
            "maxzoom_feature_ids_sha256": ids_digest,
            "pmtiles_sha256": verified.checksum_sha256,
            "pmtiles_bytes": verified.size_bytes,
        }))?;
        Ok((
            BuildEvidenceDigest::new(format!("{:x}", Sha256::digest(evidence)))
                .map_err(anyhow::Error::msg)?,
            ValidatedPmtilesArtifact {
                release_id,
                file_asset_id: static_file_asset_id_for_build(build_job_id),
                object_key,
                tiles_url_template: RuntimeTilesUrlTemplate::new(format!(
                    "{}/{source_id}/{{z}}/{{x}}/{{y}}",
                    config.public_tiles_base_url
                ))
                .map_err(anyhow::Error::msg)?,
                checksum: PmtilesChecksum::new(verified.checksum_sha256)
                    .map_err(anyhow::Error::msg)?,
                size_bytes: verified.size_bytes,
            },
        ))
    }
    .await;
    // The archive is in R2 (or the attempt failed); a local copy is never used again.
    if let Err(error) = std::fs::remove_dir_all(&work) {
        tracing::warn!(path = %work.display(), error = %error, "failed to remove the bake work directory");
    }
    result
}

#[cfg(test)]
#[path = "lakehouse_tile_bake_tests.rs"]
mod tests;
