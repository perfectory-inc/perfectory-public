use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::public_data_control_support::{
    env_path, git_head, repo_relative_path, resolve_repo_path, utc_now, write_json_file,
};

const SOURCE_ROW_SCHEMA_VERSION: &str =
    "foundation-platform.official_administrative_scope_source_row.v1";
const REGISTRY_ROW_SCHEMA_VERSION: &str =
    "foundation-platform.administrative_spatial_scope_unit.v1";
const REGISTRY_EVIDENCE_SCHEMA_VERSION: &str =
    "foundation-platform.administrative_spatial_scope_registry_evidence.v1";
const DEFAULT_SOURCE_PATH: &str = "target/source/official-administrative-boundary-snapshot.jsonl";
const DEFAULT_REGISTRY_PATH: &str = "target/audit/administrative-spatial-scope-registry.jsonl";
const DEFAULT_EVIDENCE_PATH: &str =
    "target/audit/administrative-spatial-scope-registry-evidence.json";
const FORBIDDEN_SOURCE_PROVIDERS: &[&str] = &[
    "VWorld",
    "data.go.kr",
    "provider-parcel",
    "vworld_parcel_boundaries_silver_handoff_jsonl",
];

pub fn check() -> anyhow::Result<()> {
    let config = CheckConfig::from_env()?;
    verify_registry(&config)
}

pub fn write() -> anyhow::Result<()> {
    let config = WriteConfig::from_env()?;
    RegistryWriter::new(config)?.run()
}

struct CheckConfig {
    root: PathBuf,
    registry_path: PathBuf,
    evidence_path: PathBuf,
}

impl CheckConfig {
    fn from_env() -> anyhow::Result<Self> {
        let root = repo_root()?;
        Ok(Self {
            registry_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_PATH",
                    DEFAULT_REGISTRY_PATH,
                )?,
                "RegistryPath",
            )?,
            evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_EVIDENCE_PATH",
                    DEFAULT_EVIDENCE_PATH,
                )?,
                "EvidencePath",
            )?,
            root,
        })
    }
}

struct WriteConfig {
    root: PathBuf,
    source_path: PathBuf,
    source_snapshot_id: String,
    output_path: PathBuf,
    evidence_path: PathBuf,
}

impl WriteConfig {
    fn from_env() -> anyhow::Result<Self> {
        if !env_bool(
            "FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_CONFIRM",
            false,
        )? {
            bail!(
                "ConfirmAdministrativeScopeRegistryWrite is required before writing administrative spatial scope registry"
            );
        }
        let source_snapshot_id = env::var(
            "FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_SOURCE_SNAPSHOT_ID",
        )
        .unwrap_or_default();
        if source_snapshot_id.trim().is_empty() {
            bail!("SourceSnapshotId is required");
        }
        let root = repo_root()?;
        Ok(Self {
            source_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_SOURCE_PATH",
                    DEFAULT_SOURCE_PATH,
                )?,
                "SourcePath",
            )?,
            output_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_OUTPUT_PATH",
                    DEFAULT_REGISTRY_PATH,
                )?,
                "OutputPath",
            )?,
            evidence_path: resolve_repo_path(
                &root,
                &env_path(
                    "FOUNDATION_PLATFORM_ADMINISTRATIVE_SPATIAL_SCOPE_REGISTRY_EVIDENCE_PATH",
                    DEFAULT_EVIDENCE_PATH,
                )?,
                "EvidencePath",
            )?,
            source_snapshot_id,
            root,
        })
    }
}

struct RegistryWriter {
    config: WriteConfig,
}

impl RegistryWriter {
    fn new(config: WriteConfig) -> anyhow::Result<Self> {
        if !config.source_path.is_file() {
            bail!(
                "official administrative boundary source snapshot missing: {}",
                repo_relative_path(&config.root, &config.source_path)
            );
        }
        if config.output_path.is_file() {
            bail!(
                "administrative spatial scope registry already exists: {}",
                repo_relative_path(&config.root, &config.output_path)
            );
        }
        if config.evidence_path.is_file() {
            bail!(
                "administrative spatial scope registry evidence already exists: {}",
                repo_relative_path(&config.root, &config.evidence_path)
            );
        }
        Ok(Self { config })
    }

    fn run(&self) -> anyhow::Result<()> {
        let source_rows = read_jsonl_rows(&self.config.source_path, "source", true)?;
        let registry_rows = build_registry_rows(&source_rows, &self.config.source_snapshot_id)?;
        write_registry_jsonl(&self.config.output_path, &registry_rows)?;

        let check_config = CheckConfig {
            root: self.config.root.clone(),
            registry_path: self.config.output_path.clone(),
            evidence_path: self.config.evidence_path.clone(),
        };
        verify_registry(&check_config)?;

        let active_legal_dong_count = registry_rows
            .iter()
            .filter(|row| row.scope_kind == "legal_dong" && row.status == "active")
            .count();
        println!(
            "administrative-spatial-scope-registry-written status=ready rows={} active_legal_dongs={} path={}",
            registry_rows.len(),
            active_legal_dong_count,
            repo_relative_path(&self.config.root, &self.config.output_path)
        );
        Ok(())
    }
}

fn verify_registry(config: &CheckConfig) -> anyhow::Result<()> {
    if !config.registry_path.is_file() {
        let report = RegistryEvidence::skipped(&config.root, &config.registry_path);
        write_json_file(&config.evidence_path, &report)?;
        println!(
            "administrative-spatial-scope-registry-ok status=skipped report={}",
            repo_relative_path(&config.root, &config.evidence_path)
        );
        return Ok(());
    }

    let mut blockers = Vec::new();
    let rows = read_jsonl_rows(&config.registry_path, "registry", false).unwrap_or_else(|error| {
        blockers.push(error.to_string());
        Vec::new()
    });
    validate_registry_rows(&rows, &mut blockers);

    let status = if blockers.is_empty() {
        "ready"
    } else {
        "blocked"
    };
    let active_legal_dong_count = rows
        .iter()
        .filter(|row| json_string(row, "scope_kind") == "legal_dong")
        .filter(|row| json_string(row, "status") == "active")
        .count();
    let report = RegistryEvidence {
        schema_version: REGISTRY_EVIDENCE_SCHEMA_VERSION,
        generated_at_utc: utc_now(),
        git_head: git_head(&config.root),
        status,
        registry_path: repo_relative_path(&config.root, &config.registry_path),
        registry_sha256: Some(file_sha256(&config.registry_path)?),
        row_schema_version: Some(REGISTRY_ROW_SCHEMA_VERSION),
        row_count: rows.len(),
        active_legal_dong_count,
        source_authority: Some("administrative_spatial_scope_registry"),
        completion_claim_allowed: false,
        production_cutover_allowed: false,
        national_rollout_allowed: false,
        blockers: blockers.clone(),
        next_gates: if blockers.is_empty() {
            vec!["national-data-collection-scope"]
        } else {
            Vec::new()
        },
    };
    write_json_file(&config.evidence_path, &report)?;

    if !blockers.is_empty() {
        println!(
            "administrative-spatial-scope-registry-blocked status=blocked blockers={} report={}",
            blockers.len(),
            repo_relative_path(&config.root, &config.evidence_path)
        );
        for blocker in blockers {
            println!("blocker={blocker}");
        }
        bail!("administrative spatial scope registry blocked");
    }

    println!(
        "administrative-spatial-scope-registry-ok status=ready rows={} active_legal_dongs={} report={}",
        rows.len(),
        active_legal_dong_count,
        repo_relative_path(&config.root, &config.evidence_path)
    );
    Ok(())
}

fn validate_registry_rows(rows: &[JsonValue], blockers: &mut Vec<String>) {
    if rows.is_empty() {
        blockers.push("registry JSONL must contain at least one scope unit".to_owned());
    }

    let mut seen_ids = BTreeSet::new();
    let mut known_kinds = BTreeMap::new();
    for row in rows {
        let scope_unit_id = json_string(row, "scope_unit_id");
        if !scope_unit_id.trim().is_empty() {
            known_kinds.insert(scope_unit_id, json_string(row, "scope_kind"));
        }
    }

    let mut active_codes = BTreeSet::new();
    let mut active_legal_dong_count = 0usize;
    let mut validity_windows = Vec::new();
    for row in rows {
        for field in [
            "schema_version",
            "scope_unit_id",
            "scope_kind",
            "canonical_code",
            "valid_from_utc",
            "status",
            "geometry_srid",
            "bbox",
            "source_provider",
            "source_snapshot_id",
            "row_checksum_sha256",
        ] {
            add_if(
                blockers,
                property_missing_or_blank(row, field),
                &format!("scope registry row missing {field}"),
            );
        }

        let schema = json_string(row, "schema_version");
        let scope_unit_id = json_string(row, "scope_unit_id");
        let scope_kind = json_string(row, "scope_kind");
        let canonical_code = json_string(row, "canonical_code");
        let parent_scope_unit_id = json_string(row, "parent_scope_unit_id");
        let status = json_string(row, "status");
        let source_provider = json_string(row, "source_provider");
        let checksum = json_string(row, "row_checksum_sha256");

        add_if(
            blockers,
            schema != REGISTRY_ROW_SCHEMA_VERSION,
            "scope registry row schema mismatch",
        );
        if !scope_unit_id.trim().is_empty() && !seen_ids.insert(scope_unit_id.clone()) {
            blockers.push(format!("duplicate scope_unit_id: {scope_unit_id}"));
        }
        add_if(
            blockers,
            !matches!(
                scope_kind.as_str(),
                "sido" | "sigungu" | "legal_dong" | "ri" | "collection_tile"
            ),
            "scope_kind is not allowed",
        );
        add_if(
            blockers,
            !matches!(
                status.as_str(),
                "active" | "retired" | "superseded" | "provisional"
            ),
            "scope status is not allowed",
        );
        add_if(
            blockers,
            json_i64(row, "geometry_srid", 0) != 4326,
            "geometry_srid must be EPSG 4326",
        );
        add_if(
            blockers,
            FORBIDDEN_SOURCE_PROVIDERS.contains(&source_provider.as_str()),
            "scope source_provider must not be provider parcel data",
        );
        add_if(
            blockers,
            !is_lowercase_sha256(&checksum),
            "row_checksum_sha256 must be lowercase sha256",
        );
        if is_lowercase_sha256(&checksum) && checksum != scope_checksum(row) {
            blockers.push("row_checksum_sha256 mismatch".to_owned());
        }

        let valid_from = parse_required_utc(row.get("valid_from_utc"));
        if valid_from.is_none() {
            blockers.push("valid_from_utc must be a UTC timestamp".to_owned());
        }
        let valid_to_raw = string_property(row.get("valid_to_utc"));
        let valid_to = if valid_to_raw.trim().is_empty() {
            None
        } else {
            let parsed = parse_required_utc(row.get("valid_to_utc"));
            if parsed.is_none() {
                blockers.push("valid_to_utc must be a UTC timestamp when present".to_owned());
            }
            parsed
        };
        if let (Some(valid_from), Some(valid_to)) = (valid_from, valid_to) {
            if valid_from >= valid_to {
                blockers.push("valid_from_utc must be earlier than valid_to_utc".to_owned());
            }
        }
        if let Some(valid_from) = valid_from {
            validity_windows.push(ValidityWindow {
                key: format!("{scope_kind}:{canonical_code}"),
                valid_from,
                valid_to,
            });
        }

        validate_registry_hierarchy(
            &scope_kind,
            &canonical_code,
            &parent_scope_unit_id,
            &known_kinds,
            blockers,
        );
        if scope_kind == "legal_dong" && status == "active" {
            active_legal_dong_count += 1;
            let active_code_key = format!("{scope_kind}:{canonical_code}");
            if !active_codes.insert(active_code_key.clone()) {
                blockers.push(format!("duplicate active scope code: {active_code_key}"));
            }
        }
        validate_bbox(
            row.get("bbox").unwrap_or(&JsonValue::Null),
            "bbox",
            blockers,
        );
    }

    if !rows.is_empty() && active_legal_dong_count == 0 {
        blockers.push("registry must contain at least one active legal_dong".to_owned());
    }
    validate_non_overlapping_windows(validity_windows, blockers);
}

fn validate_registry_hierarchy(
    scope_kind: &str,
    canonical_code: &str,
    parent_scope_unit_id: &str,
    known_kinds: &BTreeMap<String, String>,
    blockers: &mut Vec<String>,
) {
    match scope_kind {
        "sigungu" => {
            add_if(
                blockers,
                !is_fixed_digits(canonical_code, 5),
                "sigungu canonical_code must be five digits",
            );
            add_if(
                blockers,
                !parent_scope_unit_id.trim().is_empty()
                    && known_kinds.get(parent_scope_unit_id).map(String::as_str) != Some("sido"),
                "sigungu parent_scope_unit_id must reference a sido when present",
            );
        }
        "legal_dong" => {
            add_if(
                blockers,
                !is_fixed_digits(canonical_code, 10),
                "legal_dong canonical_code must be ten digits",
            );
            add_if(
                blockers,
                parent_scope_unit_id.trim().is_empty(),
                "legal_dong parent_scope_unit_id is required",
            );
            add_if(
                blockers,
                !parent_scope_unit_id.trim().is_empty()
                    && !known_kinds.contains_key(parent_scope_unit_id),
                "parent_scope_unit_id must reference an existing scope unit",
            );
            add_if(
                blockers,
                !parent_scope_unit_id.trim().is_empty()
                    && known_kinds.contains_key(parent_scope_unit_id)
                    && known_kinds.get(parent_scope_unit_id).map(String::as_str) != Some("sigungu"),
                "legal_dong parent_scope_unit_id must reference a sigungu",
            );
        }
        "ri" => {
            add_if(
                blockers,
                parent_scope_unit_id.trim().is_empty(),
                "ri parent_scope_unit_id is required",
            );
            add_if(
                blockers,
                !parent_scope_unit_id.trim().is_empty()
                    && known_kinds.get(parent_scope_unit_id).map(String::as_str)
                        != Some("legal_dong"),
                "ri parent_scope_unit_id must reference a legal_dong",
            );
        }
        _ => {}
    }
}

fn build_registry_rows(
    source_rows: &[JsonValue],
    source_snapshot_id: &str,
) -> anyhow::Result<Vec<RegistryRow>> {
    let mut blockers = Vec::new();
    if source_rows.is_empty() {
        blockers.push("source JSONL must contain at least one scope row".to_owned());
    }

    let mut source_kinds = BTreeMap::new();
    for row in source_rows {
        let scope_kind = json_string(row, "scope_kind");
        let canonical_code = json_string(row, "canonical_code");
        if !scope_kind.trim().is_empty() && !canonical_code.trim().is_empty() {
            source_kinds.insert(scope_unit_id(&scope_kind, &canonical_code), scope_kind);
        }
    }

    let mut registry_rows = Vec::new();
    for row in source_rows {
        validate_source_row(row, source_snapshot_id, &source_kinds, &mut blockers);
        if blockers.is_empty() {
            registry_rows.push(registry_row_from_source(row)?);
        }
    }

    if !blockers.is_empty() {
        for blocker in &blockers {
            println!("blocker={blocker}");
        }
        bail!(
            "administrative spatial scope registry source validation blocked: {}",
            blockers.len()
        );
    }

    registry_rows.sort_by(|left, right| {
        scope_kind_rank(&left.scope_kind)
            .cmp(&scope_kind_rank(&right.scope_kind))
            .then(left.canonical_code.cmp(&right.canonical_code))
    });
    Ok(registry_rows)
}

fn validate_source_row(
    row: &JsonValue,
    source_snapshot_id: &str,
    source_kinds: &BTreeMap<String, String>,
    blockers: &mut Vec<String>,
) {
    for field in [
        "schema_version",
        "scope_kind",
        "canonical_code",
        "valid_from_utc",
        "status",
        "geometry_srid",
        "bbox",
        "source_provider",
        "source_snapshot_id",
    ] {
        add_if(
            blockers,
            property_missing_or_blank(row, field),
            &format!("source row missing {field}"),
        );
    }

    let schema = json_string(row, "schema_version");
    let scope_kind = json_string(row, "scope_kind");
    let canonical_code = json_string(row, "canonical_code");
    let parent_scope_kind = json_string(row, "parent_scope_kind");
    let parent_canonical_code = json_string(row, "parent_canonical_code");
    let status = json_string(row, "status");
    let source_provider = json_string(row, "source_provider");
    let row_source_snapshot_id = json_string(row, "source_snapshot_id");

    add_if(
        blockers,
        schema != SOURCE_ROW_SCHEMA_VERSION,
        "source row schema mismatch",
    );
    add_if(
        blockers,
        !matches!(
            scope_kind.as_str(),
            "sido" | "sigungu" | "legal_dong" | "ri"
        ),
        "source scope_kind is not allowed",
    );
    add_if(
        blockers,
        !matches!(
            status.as_str(),
            "active" | "retired" | "superseded" | "provisional"
        ),
        "source status is not allowed",
    );
    add_if(
        blockers,
        json_i64(row, "geometry_srid", 0) != 4326,
        "source geometry_srid must be EPSG 4326",
    );
    add_if(
        blockers,
        FORBIDDEN_SOURCE_PROVIDERS.contains(&source_provider.as_str()),
        "scope source_provider must not be provider parcel data",
    );
    add_if(
        blockers,
        row_source_snapshot_id != source_snapshot_id,
        "source_snapshot_id must match SourceSnapshotId",
    );
    validate_source_code(&scope_kind, &canonical_code, blockers);

    let has_parent_kind = !parent_scope_kind.trim().is_empty();
    let has_parent_code = !parent_canonical_code.trim().is_empty();
    add_if(
        blockers,
        has_parent_kind ^ has_parent_code,
        "parent_scope_kind and parent_canonical_code must be supplied together",
    );
    add_if(
        blockers,
        !allowed_parent(&scope_kind, &parent_scope_kind),
        "parent scope kind is invalid for scope_kind",
    );
    if has_parent_kind && has_parent_code {
        let parent_id = scope_unit_id(&parent_scope_kind, &parent_canonical_code);
        add_if(
            blockers,
            !source_kinds.contains_key(&parent_id),
            "parent_scope_unit_id must reference an existing source scope unit",
        );
        add_if(
            blockers,
            source_kinds.contains_key(&parent_id)
                && source_kinds.get(&parent_id).map(String::as_str)
                    != Some(parent_scope_kind.as_str()),
            "parent_scope_unit_id must reference the declared parent scope kind",
        );
    }
    validate_bbox(
        row.get("bbox").unwrap_or(&JsonValue::Null),
        "source bbox",
        blockers,
    );
}

fn validate_source_code(scope_kind: &str, canonical_code: &str, blockers: &mut Vec<String>) {
    match scope_kind {
        "sido" => add_if(
            blockers,
            !is_fixed_digits(canonical_code, 2),
            "sido canonical_code must be two digits",
        ),
        "sigungu" => add_if(
            blockers,
            !is_fixed_digits(canonical_code, 5),
            "sigungu canonical_code must be five digits",
        ),
        "legal_dong" => add_if(
            blockers,
            !is_fixed_digits(canonical_code, 10),
            "legal_dong canonical_code must be ten digits",
        ),
        _ => {}
    }
}

fn registry_row_from_source(row: &JsonValue) -> anyhow::Result<RegistryRow> {
    let scope_kind = json_string(row, "scope_kind");
    let canonical_code = json_string(row, "canonical_code");
    let parent_scope_kind = json_string(row, "parent_scope_kind");
    let parent_canonical_code = json_string(row, "parent_canonical_code");
    let bbox = row.get("bbox").unwrap_or(&JsonValue::Null);
    let mut registry_row = RegistryRow {
        schema_version: REGISTRY_ROW_SCHEMA_VERSION,
        scope_unit_id: scope_unit_id(&scope_kind, &canonical_code),
        scope_kind,
        canonical_code,
        parent_scope_unit_id: if parent_scope_kind.trim().is_empty() {
            String::new()
        } else {
            scope_unit_id(&parent_scope_kind, &parent_canonical_code)
        },
        valid_from_utc: json_string(row, "valid_from_utc"),
        valid_to_utc: optional_string(row.get("valid_to_utc")),
        status: json_string(row, "status"),
        geometry_srid: 4326,
        bbox: RegistryBbox {
            min_x: decimal_string(bbox.get("min_x").unwrap_or(&JsonValue::Null))?,
            min_y: decimal_string(bbox.get("min_y").unwrap_or(&JsonValue::Null))?,
            max_x: decimal_string(bbox.get("max_x").unwrap_or(&JsonValue::Null))?,
            max_y: decimal_string(bbox.get("max_y").unwrap_or(&JsonValue::Null))?,
        },
        source_provider: json_string(row, "source_provider"),
        source_snapshot_id: json_string(row, "source_snapshot_id"),
        row_checksum_sha256: String::new(),
    };
    registry_row.row_checksum_sha256 = registry_checksum(&registry_row);
    Ok(registry_row)
}

fn write_registry_jsonl(path: &Path, rows: &[RegistryRow]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create administrative scope registry directory {}",
                parent.display()
            )
        })?;
    }
    let mut output = String::new();
    for row in rows {
        output.push_str(&serde_json::to_string(row)?);
        output.push('\n');
    }
    fs::write(path, output).with_context(|| format!("failed to write {}", path.display()))
}

fn read_jsonl_rows(path: &Path, label: &str, strict: bool) -> anyhow::Result<Vec<JsonValue>> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut rows = Vec::new();
    let mut blockers = Vec::new();
    for (index, raw_line) in content.lines().enumerate() {
        let line_number = index + 1;
        let line = if line_number == 1 {
            raw_line.trim_start_matches('\u{feff}')
        } else {
            raw_line
        };
        if line.trim().is_empty() {
            blockers.push(format!(
                "{label} JSONL line {line_number} must not be blank"
            ));
            continue;
        }
        match serde_json::from_str(line) {
            Ok(row) => rows.push(row),
            Err(_) => blockers.push(format!(
                "{label} JSONL line {line_number} is not valid JSON"
            )),
        }
    }
    if strict && rows.is_empty() {
        blockers.push(format!("{label} JSONL must contain at least one scope row"));
    }
    if blockers.is_empty() {
        Ok(rows)
    } else {
        bail!(blockers.join("; "))
    }
}

fn validate_bbox(bbox: &JsonValue, label: &str, blockers: &mut Vec<String>) {
    let mut parsed = Vec::new();
    for field in ["min_x", "min_y", "max_x", "max_y"] {
        let value = string_property(bbox.get(field));
        add_if(
            blockers,
            !is_decimal(&value),
            &format!("{label}.{field} must be decimal"),
        );
        if let Ok(number) = value.parse::<f64>() {
            parsed.push(number);
        }
    }
    if parsed.len() == 4 {
        add_if(
            blockers,
            parsed[0] >= parsed[2] || parsed[1] >= parsed[3],
            &format!("{label} min values must be lower than max values"),
        );
    }
}

fn validate_non_overlapping_windows(mut windows: Vec<ValidityWindow>, blockers: &mut Vec<String>) {
    windows.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then(left.valid_from.cmp(&right.valid_from))
    });
    let mut previous_by_key: BTreeMap<String, ValidityWindow> = BTreeMap::new();
    for window in windows {
        if let Some(previous) = previous_by_key.get(&window.key) {
            let previous_to = previous.valid_to.unwrap_or(DateTime::<Utc>::MAX_UTC);
            if window.valid_from < previous_to {
                blockers.push(format!(
                    "scope validity windows must not overlap: {}",
                    window.key
                ));
                previous_by_key.insert(window.key.clone(), window);
                continue;
            }
        }
        previous_by_key.insert(window.key.clone(), window);
    }
}

fn allowed_parent(scope_kind: &str, parent_scope_kind: &str) -> bool {
    match scope_kind {
        "sigungu" => parent_scope_kind.trim().is_empty() || parent_scope_kind == "sido",
        "legal_dong" => parent_scope_kind == "sigungu",
        "ri" => parent_scope_kind == "legal_dong",
        _ => parent_scope_kind.trim().is_empty(),
    }
}

fn scope_checksum(row: &JsonValue) -> String {
    let bbox = row.get("bbox").unwrap_or(&JsonValue::Null);
    let input = [
        json_string(row, "scope_unit_id"),
        json_string(row, "scope_kind"),
        json_string(row, "canonical_code"),
        json_string(row, "parent_scope_unit_id"),
        utc_timestamp_string(row.get("valid_from_utc")),
        utc_timestamp_string(row.get("valid_to_utc")),
        json_string(row, "status"),
        string_property(row.get("geometry_srid")),
        string_property(bbox.get("min_x")),
        string_property(bbox.get("min_y")),
        string_property(bbox.get("max_x")),
        string_property(bbox.get("max_y")),
        json_string(row, "source_provider"),
        json_string(row, "source_snapshot_id"),
    ]
    .join("|");
    sha256_hex(input.as_bytes())
}

fn registry_checksum(row: &RegistryRow) -> String {
    let input = [
        row.scope_unit_id.as_str(),
        row.scope_kind.as_str(),
        row.canonical_code.as_str(),
        row.parent_scope_unit_id.as_str(),
        row.valid_from_utc.as_str(),
        row.valid_to_utc.as_deref().unwrap_or_default(),
        row.status.as_str(),
        "4326",
        row.bbox.min_x.as_str(),
        row.bbox.min_y.as_str(),
        row.bbox.max_x.as_str(),
        row.bbox.max_y.as_str(),
        row.source_provider.as_str(),
        row.source_snapshot_id.as_str(),
    ]
    .join("|");
    sha256_hex(input.as_bytes())
}

fn file_sha256(path: &Path) -> anyhow::Result<String> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(sha256_hex(&bytes))
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn parse_required_utc(value: Option<&JsonValue>) -> Option<DateTime<Utc>> {
    let raw = string_property(value);
    if raw.trim().is_empty() || !(raw.ends_with('Z') || raw.ends_with("+00:00")) {
        return None;
    }
    DateTime::parse_from_rfc3339(&raw)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn utc_timestamp_string(value: Option<&JsonValue>) -> String {
    string_property(value)
}

fn json_string(value: &JsonValue, field: &str) -> String {
    string_property(value.get(field))
}

fn string_property(value: Option<&JsonValue>) -> String {
    match value {
        Some(JsonValue::String(text)) => text.to_owned(),
        Some(JsonValue::Number(number)) => number.to_string(),
        Some(JsonValue::Bool(flag)) => flag.to_string(),
        Some(JsonValue::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn optional_string(value: Option<&JsonValue>) -> Option<String> {
    let value = string_property(value);
    if value.trim().is_empty() {
        None
    } else {
        Some(value)
    }
}

fn json_i64(value: &JsonValue, field: &str, default: i64) -> i64 {
    value
        .get(field)
        .and_then(|raw| {
            raw.as_i64()
                .or_else(|| raw.as_u64().and_then(|raw| raw.try_into().ok()))
                .or_else(|| raw.as_str().and_then(|raw| raw.parse().ok()))
        })
        .unwrap_or(default)
}

fn property_missing_or_blank(value: &JsonValue, name: &str) -> bool {
    value.get(name).is_none_or(|property| match property {
        JsonValue::Null => true,
        JsonValue::String(text) => text.trim().is_empty(),
        _ => false,
    })
}

fn decimal_string(value: &JsonValue) -> anyhow::Result<String> {
    let raw = string_property(Some(value));
    let parsed = raw
        .parse::<f64>()
        .with_context(|| format!("bbox coordinate must be decimal: {raw}"))?;
    Ok(format!("{parsed:.6}"))
}

fn scope_unit_id(scope_kind: &str, canonical_code: &str) -> String {
    format!("scope:{}:{canonical_code}", scope_kind_slug(scope_kind))
}

fn scope_kind_slug(scope_kind: &str) -> &str {
    match scope_kind {
        "legal_dong" => "legal-dong",
        "collection_tile" => "collection-tile",
        value => value,
    }
}

fn scope_kind_rank(scope_kind: &str) -> u8 {
    match scope_kind {
        "sido" => 10,
        "sigungu" => 20,
        "legal_dong" => 30,
        "ri" => 40,
        "collection_tile" => 50,
        _ => 255,
    }
}

fn is_fixed_digits(value: &str, expected_len: usize) -> bool {
    value.len() == expected_len && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_decimal(value: &str) -> bool {
    let value = value.trim();
    if value.is_empty() {
        return false;
    }
    let value = value.strip_prefix('-').unwrap_or(value);
    if let Some((head, tail)) = value.split_once('.') {
        !head.is_empty()
            && !tail.is_empty()
            && head.bytes().all(|byte| byte.is_ascii_digit())
            && tail.bytes().all(|byte| byte.is_ascii_digit())
    } else {
        value.bytes().all(|byte| byte.is_ascii_digit())
    }
}

fn add_if(blockers: &mut Vec<String>, condition: bool, message: &str) {
    if condition {
        blockers.push(message.to_owned());
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

#[derive(Clone)]
struct ValidityWindow {
    key: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
struct RegistryBbox {
    min_x: String,
    min_y: String,
    max_x: String,
    max_y: String,
}

#[derive(Serialize)]
struct RegistryRow {
    schema_version: &'static str,
    scope_unit_id: String,
    scope_kind: String,
    canonical_code: String,
    parent_scope_unit_id: String,
    valid_from_utc: String,
    valid_to_utc: Option<String>,
    status: String,
    geometry_srid: i64,
    bbox: RegistryBbox,
    source_provider: String,
    source_snapshot_id: String,
    row_checksum_sha256: String,
}

#[derive(Serialize)]
struct RegistryEvidence<'a> {
    schema_version: &'static str,
    generated_at_utc: String,
    git_head: String,
    status: &'a str,
    registry_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    registry_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    row_schema_version: Option<&'static str>,
    row_count: usize,
    active_legal_dong_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_authority: Option<&'static str>,
    completion_claim_allowed: bool,
    production_cutover_allowed: bool,
    national_rollout_allowed: bool,
    blockers: Vec<String>,
    next_gates: Vec<&'static str>,
}

impl RegistryEvidence<'static> {
    fn skipped(root: &Path, registry_path: &Path) -> Self {
        Self {
            schema_version: REGISTRY_EVIDENCE_SCHEMA_VERSION,
            generated_at_utc: utc_now(),
            git_head: git_head(root),
            status: "skipped",
            registry_path: repo_relative_path(root, registry_path),
            registry_sha256: None,
            row_schema_version: None,
            row_count: 0,
            active_legal_dong_count: 0,
            source_authority: None,
            completion_claim_allowed: false,
            production_cutover_allowed: false,
            national_rollout_allowed: false,
            blockers: vec![
                "administrative spatial scope registry has not been produced".to_owned(),
            ],
            next_gates: vec!["administrative-spatial-scope-registry"],
        }
    }
}
