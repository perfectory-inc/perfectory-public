use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::public_data_control_support::{
    env_path, git_head, read_json, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};

const SOURCE_ROW_SCHEMA_VERSION: &str =
    "foundation-platform.official_administrative_scope_source_row.v1";
const EVIDENCE_SCHEMA_VERSION: &str =
    "foundation-platform.official_administrative_boundary_source_snapshot.v1";
const DEFAULT_INPUT_GEOJSON_PATH: &str = "target/source/official-administrative-boundary.geojson";
const DEFAULT_OUTPUT_PATH: &str = "target/source/official-administrative-boundary-snapshot.jsonl";
const DEFAULT_EVIDENCE_PATH: &str =
    "target/source/official-administrative-boundary-snapshot-evidence.json";
const DEFAULT_SOURCE_PROVIDER: &str = "official-administrative-boundary";
const DEFAULT_CODE_PROPERTY: &str = "EMD_CD";
const DEFAULT_NAME_PROPERTY: &str = "EMD_NM";
const FORBIDDEN_SOURCE_PROVIDERS: &[&str] = &[
    "VWorld",
    "data.go.kr",
    "provider-parcel",
    "vworld_parcel_boundaries_silver_handoff_jsonl",
];

pub fn write() -> anyhow::Result<()> {
    let config = WriteConfig::from_env()?;
    SourceSnapshotWriter::new(config)?.run()
}

struct WriteConfig {
    root: PathBuf,
    input_geojson_path: PathBuf,
    output_path: PathBuf,
    evidence_path: PathBuf,
    source_snapshot_id: String,
    source_provider: String,
    code_property: String,
    name_property: String,
    source_srid: i64,
    valid_from_utc: String,
}

impl WriteConfig {
    fn from_env() -> anyhow::Result<Self> {
        if !env_bool(
            "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_CONFIRM",
            false,
        )? {
            bail!(
                "ConfirmOfficialAdministrativeBoundarySourceWrite is required before writing official administrative boundary source snapshot"
            );
        }
        let source_snapshot_id = env_string(
            "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_SNAPSHOT_ID",
            "",
        )?;
        if source_snapshot_id.trim().is_empty() {
            bail!("SourceSnapshotId is required");
        }
        let valid_from_raw = env_string(
            "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_VALID_FROM_UTC",
            "",
        )?;
        if valid_from_raw.trim().is_empty() {
            bail!("ValidFromUtc is required");
        }
        let parsed_valid_from = chrono::DateTime::parse_from_rfc3339(valid_from_raw.trim())
            .context("ValidFromUtc must be an RFC3339 UTC timestamp")?;
        if parsed_valid_from.offset().local_minus_utc() != 0 {
            bail!("ValidFromUtc must use the UTC offset");
        }
        let valid_from_utc = parsed_valid_from
            .with_timezone(&chrono::Utc)
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let source_provider = env_string(
            "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_PROVIDER",
            DEFAULT_SOURCE_PROVIDER,
        )?;
        if source_provider.trim().is_empty() {
            bail!("SourceProvider is required");
        }
        if FORBIDDEN_SOURCE_PROVIDERS.contains(&source_provider.as_str()) {
            bail!("SourceProvider must not be provider parcel data");
        }
        let source_srid = env_string(
            "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_SRID",
            "4326",
        )?
        .parse::<i64>()
        .context("SourceSrid must be an integer")?;
        if source_srid != 4326 {
            bail!(
                "SourceSrid must be 4326; transform official SHP/GeoPackage inputs to WGS84 GeoJSON before this writer"
            );
        }

        let root = repo_root()?;
        Ok(Self {
            input_geojson_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_INPUT_GEOJSON_PATH",
                    DEFAULT_INPUT_GEOJSON_PATH,
                )?,
                "InputGeoJsonPath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_OUTPUT_PATH",
                    DEFAULT_OUTPUT_PATH,
                )?,
                "OutputPath",
            )?,
            evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_EVIDENCE_PATH",
                    DEFAULT_EVIDENCE_PATH,
                )?,
                "EvidencePath",
            )?,
            source_snapshot_id,
            source_provider,
            code_property: env_string(
                "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_CODE_PROPERTY",
                DEFAULT_CODE_PROPERTY,
            )?,
            name_property: env_string(
                "FOUNDATION_PLATFORM_OFFICIAL_ADMINISTRATIVE_BOUNDARY_SOURCE_NAME_PROPERTY",
                DEFAULT_NAME_PROPERTY,
            )?,
            source_srid,
            valid_from_utc,
            root,
        })
    }
}

struct SourceSnapshotWriter {
    config: WriteConfig,
}

impl SourceSnapshotWriter {
    fn new(config: WriteConfig) -> anyhow::Result<Self> {
        if !config.input_geojson_path.is_file() {
            bail!(
                "official administrative boundary GeoJSON missing: {}",
                repo_relative_path(&config.root, &config.input_geojson_path)
            );
        }
        if config.output_path.is_file() {
            bail!(
                "official administrative boundary source snapshot already exists: {}",
                repo_relative_path(&config.root, &config.output_path)
            );
        }
        if config.evidence_path.is_file() {
            bail!(
                "official administrative boundary source snapshot evidence already exists: {}",
                repo_relative_path(&config.root, &config.evidence_path)
            );
        }
        Ok(Self { config })
    }

    fn run(&self) -> anyhow::Result<()> {
        let geojson = read_json(
            &self.config.input_geojson_path,
            "official administrative boundary GeoJSON",
        )?;
        let features = geojson_features(&geojson)?;
        let rows = build_rows(&self.config, features)?;
        write_jsonl(&self.config.output_path, &rows)?;

        let legal_dong_count = rows
            .iter()
            .filter(|row| row.scope_kind == "legal_dong")
            .count();
        let sigungu_count = rows
            .iter()
            .filter(|row| row.scope_kind == "sigungu")
            .count();
        let evidence = Evidence {
            schema_version: EVIDENCE_SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(&self.config.root),
            status: "ready",
            source_provider: self.config.source_provider.clone(),
            source_snapshot_id: self.config.source_snapshot_id.clone(),
            source_srid: self.config.source_srid,
            code_property: self.config.code_property.clone(),
            name_property: self.config.name_property.clone(),
            input_geojson_path: repo_relative_path(
                &self.config.root,
                &self.config.input_geojson_path,
            ),
            input_geojson_sha256: file_sha256(&self.config.input_geojson_path)?,
            output_path: repo_relative_path(&self.config.root, &self.config.output_path),
            row_schema_version: SOURCE_ROW_SCHEMA_VERSION,
            feature_count: features.len(),
            source_row_count: rows.len(),
            legal_dong_count,
            sigungu_count,
            completion_claim_allowed: false,
            production_cutover_allowed: false,
            national_rollout_allowed: false,
            next_gates: vec!["administrative-spatial-scope-registry"],
        };
        write_json_file(&self.config.evidence_path, &evidence)?;

        println!(
            "official-administrative-boundary-source-snapshot-written status=ready rows={} legal_dongs={} sigungus={} path={}",
            rows.len(),
            legal_dong_count,
            sigungu_count,
            repo_relative_path(&self.config.root, &self.config.output_path)
        );
        Ok(())
    }
}

fn geojson_features(geojson: &JsonValue) -> anyhow::Result<&Vec<JsonValue>> {
    if json_string(geojson, "type") != "FeatureCollection" {
        bail!("official administrative boundary input must be a GeoJSON FeatureCollection");
    }
    let Some(features) = geojson.get("features").and_then(JsonValue::as_array) else {
        bail!("official administrative boundary input must contain at least one feature");
    };
    if features.is_empty() {
        bail!("official administrative boundary input must contain at least one feature");
    }
    Ok(features)
}

fn build_rows(config: &WriteConfig, features: &[JsonValue]) -> anyhow::Result<Vec<SourceRow>> {
    let mut seen_legal_dong_codes = BTreeSet::new();
    let mut legal_dong_rows = Vec::new();
    let mut sigungu_bounds_by_code: BTreeMap<String, Bounds> = BTreeMap::new();
    let mut sigungu_feature_counts: BTreeMap<String, usize> = BTreeMap::new();

    for feature in features {
        if json_string(feature, "type") != "Feature" {
            bail!("official administrative boundary input contains a non-Feature row");
        }
        let properties = feature
            .get("properties")
            .context("official administrative boundary feature missing properties")?;
        let geometry = feature
            .get("geometry")
            .context("official administrative boundary feature missing geometry")?;
        let geometry_type = json_string(geometry, "type");
        if !matches!(geometry_type.as_str(), "Polygon" | "MultiPolygon") {
            bail!("official administrative boundary geometry must be Polygon or MultiPolygon");
        }

        let source_code = json_string(properties, &config.code_property);
        let legal_dong_code = canonical_legal_dong_code(&source_code)?;
        if !seen_legal_dong_codes.insert(legal_dong_code.clone()) {
            bail!("duplicate legal_dong source code: {legal_dong_code}");
        }

        let mut bounds = Bounds::empty();
        add_coordinate_bounds(geometry.get("coordinates"), &mut bounds);
        let bbox = Bbox::from_bounds(&bounds)?;
        let sigungu_code = legal_dong_code[0..5].to_owned();
        sigungu_bounds_by_code
            .entry(sigungu_code.clone())
            .or_insert_with(Bounds::empty)
            .merge(&bounds);
        *sigungu_feature_counts
            .entry(sigungu_code.clone())
            .or_default() += 1;

        legal_dong_rows.push(SourceRow {
            schema_version: SOURCE_ROW_SCHEMA_VERSION,
            scope_kind: "legal_dong".to_owned(),
            canonical_code: legal_dong_code,
            parent_scope_kind: "sigungu".to_owned(),
            parent_canonical_code: sigungu_code,
            valid_from_utc: config.valid_from_utc.clone(),
            valid_to_utc: None,
            status: "active".to_owned(),
            geometry_srid: 4326,
            bbox,
            source_provider: config.source_provider.clone(),
            source_snapshot_id: config.source_snapshot_id.clone(),
            source_name: json_string(properties, &config.name_property),
            source_feature_count: None,
        });
    }

    legal_dong_rows.sort_by(|left, right| left.canonical_code.cmp(&right.canonical_code));
    let mut rows = Vec::new();
    for (sigungu_code, bounds) in sigungu_bounds_by_code {
        rows.push(SourceRow {
            schema_version: SOURCE_ROW_SCHEMA_VERSION,
            scope_kind: "sigungu".to_owned(),
            canonical_code: sigungu_code.clone(),
            parent_scope_kind: String::new(),
            parent_canonical_code: String::new(),
            valid_from_utc: config.valid_from_utc.clone(),
            valid_to_utc: None,
            status: "active".to_owned(),
            geometry_srid: 4326,
            bbox: Bbox::from_bounds(&bounds)?,
            source_provider: config.source_provider.clone(),
            source_snapshot_id: config.source_snapshot_id.clone(),
            source_name: String::new(),
            source_feature_count: sigungu_feature_counts.get(&sigungu_code).copied(),
        });
    }
    rows.extend(legal_dong_rows);
    Ok(rows)
}

fn canonical_legal_dong_code(source_code: &str) -> anyhow::Result<String> {
    let trimmed = source_code.trim();
    if is_fixed_digits(trimmed, 10) {
        return Ok(trimmed.to_owned());
    }
    if is_fixed_digits(trimmed, 8) {
        return Ok(format!("{trimmed}00"));
    }
    bail!("legal_dong source code must be eight-digit EMD or ten-digit legal_dong code");
}

fn add_coordinate_bounds(value: Option<&JsonValue>, bounds: &mut Bounds) {
    let Some(value) = value else {
        return;
    };
    let Some(items) = value.as_array() else {
        return;
    };
    if items.len() >= 2 {
        if let (Some(x), Some(y)) = (number_like(&items[0]), number_like(&items[1])) {
            bounds.include(x, y);
            return;
        }
    }
    for item in items {
        add_coordinate_bounds(Some(item), bounds);
    }
}

fn number_like(value: &JsonValue) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn write_jsonl(path: &Path, rows: &[SourceRow]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create source directory {}", parent.display()))?;
    }
    let mut output = String::new();
    for row in rows {
        output.push_str(&serde_json::to_string(row)?);
        output.push('\n');
    }
    fs::write(path, output).with_context(|| format!("failed to write {}", path.display()))
}

fn json_string(value: &JsonValue, field: &str) -> String {
    value
        .get(field)
        .and_then(|value| match value {
            JsonValue::String(text) => Some(text.to_owned()),
            JsonValue::Number(number) => Some(number.to_string()),
            JsonValue::Bool(flag) => Some(flag.to_string()),
            _ => None,
        })
        .unwrap_or_default()
}

fn is_fixed_digits(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn file_sha256(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn env_string(name: &str, default: &str) -> anyhow::Result<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn env_bool(name: &str, default: bool) -> anyhow::Result<bool> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(default),
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => bail!("{name} must be a boolean"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => bail!("invalid {name} environment variable: {error}"),
    }
}

fn repo_root() -> anyhow::Result<PathBuf> {
    let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
    let root = fs::canonicalize(&root)
        .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
    Ok(normalize_windows_verbatim_path(root))
}

fn normalize_windows_verbatim_path(path: PathBuf) -> PathBuf {
    const VERBATIM_PREFIX: &str = r"\\?\";
    let raw = path.to_string_lossy();
    if let Some(stripped) = raw.strip_prefix(VERBATIM_PREFIX) {
        PathBuf::from(stripped)
    } else {
        path
    }
}

struct Bounds {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

impl Bounds {
    fn empty() -> Self {
        Self {
            min_x: f64::INFINITY,
            min_y: f64::INFINITY,
            max_x: f64::NEG_INFINITY,
            max_y: f64::NEG_INFINITY,
        }
    }

    fn include(&mut self, x: f64, y: f64) {
        self.min_x = self.min_x.min(x);
        self.min_y = self.min_y.min(y);
        self.max_x = self.max_x.max(x);
        self.max_y = self.max_y.max(y);
    }

    fn merge(&mut self, other: &Self) {
        self.include(other.min_x, other.min_y);
        self.include(other.max_x, other.max_y);
    }
}

#[derive(Serialize)]
struct Bbox {
    min_x: String,
    min_y: String,
    max_x: String,
    max_y: String,
}

impl Bbox {
    fn from_bounds(bounds: &Bounds) -> anyhow::Result<Self> {
        for value in [bounds.min_x, bounds.min_y, bounds.max_x, bounds.max_y] {
            if !value.is_finite() {
                bail!("geometry bounds are not finite");
            }
        }
        if bounds.min_x >= bounds.max_x || bounds.min_y >= bounds.max_y {
            bail!("geometry bbox min values must be lower than max values");
        }
        Ok(Self {
            min_x: format!("{:.6}", bounds.min_x),
            min_y: format!("{:.6}", bounds.min_y),
            max_x: format!("{:.6}", bounds.max_x),
            max_y: format!("{:.6}", bounds.max_y),
        })
    }
}

#[derive(Serialize)]
struct SourceRow {
    schema_version: &'static str,
    scope_kind: String,
    canonical_code: String,
    parent_scope_kind: String,
    parent_canonical_code: String,
    valid_from_utc: String,
    valid_to_utc: Option<String>,
    status: String,
    geometry_srid: i64,
    bbox: Bbox,
    source_provider: String,
    source_snapshot_id: String,
    source_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_feature_count: Option<usize>,
}

#[derive(Serialize)]
struct Evidence {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'static str,
    source_provider: String,
    source_snapshot_id: String,
    source_srid: i64,
    code_property: String,
    name_property: String,
    input_geojson_path: String,
    input_geojson_sha256: String,
    output_path: String,
    row_schema_version: &'static str,
    feature_count: usize,
    source_row_count: usize,
    legal_dong_count: usize,
    sigungu_count: usize,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    next_gates: Vec<&'static str>,
}
