//! Builds the injected address resolution the industrial-complex Bronze JSONL producer requires.
//!
//! Root ADR-0033 made the address an injected input and nothing produced it: the only run that
//! existed used synthetic addresses. This command is that producer. It reads Bronze objects and one
//! district authority, applies the three resolution rules in
//! [`lakehouse_application::industrial_complex_address_resolution_plan`], and writes the resolution
//! JSONL that `export-industrial-complex-bronze-raw-jsonl` consumes.
//!
//! Inputs, all local:
//!
//! | input | Bronze source |
//! |---|---|
//! | complexes to resolve | `vworldkr__sandan_profile` (the same workbook the producer decodes) |
//! | district code + address text | `industrylandorkr__industrial_complex_list` |
//! | notices | `industrylandorkr__industrial_complex_notice` |
//! | district authority | the `.dbf` beside the V-World `boundary_sigungu` shapefile |
//!
//! The output is append-only for the same reason the producer's is: a rerun that quietly replaced
//! it would destroy the record of what an earlier export was built from.

use std::{
    collections::BTreeSet,
    env, fs,
    io::Write as _,
    path::{Path, PathBuf},
};

mod sigungu_authority_decoder;
#[cfg(test)]
mod tests;

use anyhow::{bail, Context};
use chrono::Utc;
use lakehouse_application::{
    resolve_industrial_complex_addresses, IndustrialComplexAddressResolution,
    IndustrialComplexAddressResolutionInput, ResolvedComplexAddress, SourceComplexRecord,
    SourceNoticeRecord,
};
use serde_json::{json, Value as JsonValue};
use zip::ZipArchive;

use crate::industrial_complex_bronze_raw_jsonl_export::{
    decode_profile_rows_from_bronze_zip, locate_single_bronze_object,
};

const PREFIX: &str = "FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_ADDRESS_RESOLUTION";
const DEFAULT_PROFILE_SOURCE_SLUG: &str = "vworldkr__sandan_profile";
const COMPLEX_LIST_SOURCE_SLUG: &str = "industrylandorkr__industrial_complex_list";
const NOTICE_SOURCE_SLUG: &str = "industrylandorkr__industrial_complex_notice";
const DETAIL_SOURCE_SLUG: &str = "industrylandorkr__industrial_complex_detail";

/// JSON pointer to the records array inside an ILIS bulk response.
const ILIS_RECORDS_POINTER: &str = "/result/dataList";

/// JSON pointer to the single record inside an ILIS detail response.
///
/// The detail endpoint nests one more level than the bulk endpoints (`/result/data`, not
/// `/result/dataList`), which is exactly the kind of drift the collector's shape check exists to
/// surface rather than let through as a zero-record object.
const ILIS_DETAIL_RECORD_POINTER: &str = "/result/data";

/// Attribute-table entry name inside the V-World boundary archive.
const AUTHORITY_DBF_EXTENSION: &str = ".dbf";

struct AddressResolutionBuildConfig {
    bronze_local_object_root: PathBuf,
    profile_source_slug: String,
    profile_object: Option<String>,
    profile_sheet_name: Option<String>,
    complex_list_object: Option<String>,
    notice_object: Option<String>,
    sigungu_authority_path: PathBuf,
    output_path: PathBuf,
    summary_path: Option<PathBuf>,
}

/// Runs the address resolution build.
///
/// # Errors
/// Returns an error when an input cannot be read or decoded, when the learned district migration
/// contradicts itself, or when the output path already holds a previous resolution.
pub fn run() -> anyhow::Result<()> {
    let config = AddressResolutionBuildConfig::from_env()?;
    let (resolution, inputs) = build_resolution(&config)?;
    write_resolution_jsonl(&config.output_path, &resolution.resolved)?;
    if let Some(summary_path) = config.summary_path.as_ref() {
        crate::public_data_control_support::write_json_file(
            summary_path,
            &summary_json(&config, &resolution, &inputs),
        )?;
    }
    tracing::info!(
        target_complex_count = inputs.target_complex_count,
        resolved = resolution.resolved.len(),
        unresolved = resolution.unresolved.len(),
        learned_migrations = resolution.learned_code_migrations.len(),
        output_path = %config.output_path.display(),
        "industrial-complex address resolution built"
    );
    Ok(())
}

/// Counts of what each input contributed, carried into the summary.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolutionInputCounts {
    target_complex_count: usize,
    source_complex_count: usize,
    source_detail_complex_count: usize,
    source_notice_count: usize,
    sigungu_authority_count: usize,
    profile_object_key: String,
    complex_list_object_key: String,
    notice_object_key: String,
}

fn build_resolution(
    config: &AddressResolutionBuildConfig,
) -> anyhow::Result<(IndustrialComplexAddressResolution, ResolutionInputCounts)> {
    let authority = read_sigungu_authority(&config.sigungu_authority_path)?;

    let profile_path = locate_single_bronze_object(
        &config.bronze_local_object_root,
        config.profile_source_slug.as_str(),
        config.profile_object.as_deref(),
        "zip",
    )?;
    let profile_records = decode_profile_rows_from_bronze_zip(
        &profile_path,
        config.profile_sheet_name.as_deref(),
        None,
    )?;
    let target_complex_codes = profile_records
        .iter()
        .map(|record| record.official_complex_code.trim().to_owned())
        .collect::<Vec<_>>();

    let complex_list_path = locate_single_bronze_object(
        &config.bronze_local_object_root,
        COMPLEX_LIST_SOURCE_SLUG,
        config.complex_list_object.as_deref(),
        "json",
    )?;
    let mut source_complexes = read_source_complexes(&complex_list_path)?;
    // The bulk list omits a handful of complexes the profile carries. Each detail object is one
    // provider request the operator paid for, so they are merged in here rather than left for a
    // second resolution pass; a detail that repeats a listed complex is skipped so the list stays
    // the authority for everything it does carry.
    let detail_objects = list_detail_objects(&config.bronze_local_object_root)?;
    let detail_complexes = read_detail_complexes(&detail_objects)?;
    let listed: BTreeSet<String> = source_complexes
        .iter()
        .map(|record| record.official_complex_code.trim().to_owned())
        .collect();
    let detail_complex_count = detail_complexes.len();
    source_complexes.extend(
        detail_complexes
            .into_iter()
            .filter(|record| !listed.contains(record.official_complex_code.trim())),
    );

    let notice_path = locate_single_bronze_object(
        &config.bronze_local_object_root,
        NOTICE_SOURCE_SLUG,
        config.notice_object.as_deref(),
        "json",
    )?;
    let source_notices = read_source_notices(&notice_path)?;

    let resolution =
        resolve_industrial_complex_addresses(&IndustrialComplexAddressResolutionInput {
            target_complex_codes: &target_complex_codes,
            source_complexes: &source_complexes,
            source_notices: &source_notices,
            sigungu_authority: &authority,
            notice_dataset: NOTICE_SOURCE_SLUG,
        })?;

    let counts = ResolutionInputCounts {
        target_complex_count: target_complex_codes.len(),
        source_complex_count: source_complexes.len(),
        source_detail_complex_count: detail_complex_count,
        source_notice_count: source_notices.len(),
        sigungu_authority_count: authority.len(),
        profile_object_key: bronze_object_key(&config.bronze_local_object_root, &profile_path)?,
        complex_list_object_key: bronze_object_key(
            &config.bronze_local_object_root,
            &complex_list_path,
        )?,
        notice_object_key: bronze_object_key(&config.bronze_local_object_root, &notice_path)?,
    };
    Ok((resolution, counts))
}

/// Reads the district authority from a `.dbf`, or from the archive that carries one.
fn read_sigungu_authority(path: &Path) -> anyhow::Result<BTreeSet<String>> {
    let bytes = if path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("zip"))
    {
        read_single_dbf_entry(path)?
    } else {
        fs::read(path)
            .with_context(|| format!("failed to read the district authority {}", path.display()))?
    };
    sigungu_authority_decoder::decode_sigungu_authority(&bytes)
        .with_context(|| format!("invalid district authority {}", path.display()))
}

fn read_single_dbf_entry(archive_path: &Path) -> anyhow::Result<Vec<u8>> {
    let file = fs::File::open(archive_path).with_context(|| {
        format!(
            "failed to open the district authority archive {}",
            archive_path.display()
        )
    })?;
    let mut archive = ZipArchive::new(file).with_context(|| {
        format!(
            "failed to read the district authority archive {}",
            archive_path.display()
        )
    })?;
    let mut indexes = Vec::new();
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .with_context(|| format!("failed to inspect archive entry {index}"))?;
        if !entry.is_dir()
            && entry
                .name()
                .to_ascii_lowercase()
                .ends_with(AUTHORITY_DBF_EXTENSION)
        {
            indexes.push(index);
        }
    }
    let [dbf_index] = indexes.as_slice() else {
        bail!(
            "the district authority archive must contain exactly one {AUTHORITY_DBF_EXTENSION} \
             entry, found {}",
            indexes.len()
        );
    };
    let mut entry = archive
        .by_index(*dbf_index)
        .with_context(|| format!("failed to open archive entry {dbf_index}"))?;
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut entry, &mut bytes)
        .context("failed to read the district authority out of its archive")?;
    Ok(bytes)
}

fn read_source_complexes(path: &Path) -> anyhow::Result<Vec<SourceComplexRecord>> {
    let records = read_ilis_records(path)?;
    Ok(records
        .iter()
        .filter_map(|record| {
            Some(SourceComplexRecord {
                official_complex_code: json_text(record, "danji_cd")?,
                administrative_code: json_text(record, "addr_cd"),
                // `danji_loc` verbatim. Normalizing the official address text here would make the
                // exported address something no source ever published.
                address_text: json_text(record, "danji_loc"),
                source_dataset: COMPLEX_LIST_SOURCE_SLUG.to_owned(),
            })
        })
        .collect())
}

/// Lists every per-complex detail object collected so far, or nothing when none has been.
///
/// Absence is normal: the detail fallback only runs after a resolution build has named the
/// complexes the bulk list omits, so the first build legitimately has none.
fn list_detail_objects(bronze_local_object_root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let source_root = bronze_local_object_root
        .join("bronze")
        .join(format!("source={DETAIL_SOURCE_SLUG}"));
    if !source_root.is_dir() {
        return Ok(Vec::new());
    }
    let mut objects = Vec::new();
    collect_json_objects(&source_root, &mut objects)?;
    objects.sort();
    Ok(objects)
}

fn collect_json_objects(dir: &Path, found: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let path = entry
            .with_context(|| format!("failed to read entry in {}", dir.display()))?
            .path();
        if path.is_dir() {
            collect_json_objects(&path, found)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
        {
            found.push(path);
        }
    }
    Ok(())
}

/// Reads the per-complex detail objects, which answer with one record rather than a list.
fn read_detail_complexes(paths: &[PathBuf]) -> anyhow::Result<Vec<SourceComplexRecord>> {
    let mut records = Vec::with_capacity(paths.len());
    for path in paths {
        let bytes = fs::read(path).with_context(|| {
            format!(
                "failed to read the ILIS detail Bronze object {}",
                path.display()
            )
        })?;
        let payload = serde_json::from_slice::<JsonValue>(&bytes).with_context(|| {
            format!(
                "the ILIS detail Bronze object {} is not JSON",
                path.display()
            )
        })?;
        let Some(record) = payload.pointer(ILIS_DETAIL_RECORD_POINTER) else {
            bail!(
                "the ILIS detail Bronze object {} carries no record at {ILIS_DETAIL_RECORD_POINTER}",
                path.display()
            );
        };
        let Some(official_complex_code) = json_text(record, "danji_cd") else {
            bail!(
                "the ILIS detail Bronze object {} carries no danji_cd; the join is by code, so a \
                 record without one cannot be attached to a complex",
                path.display()
            );
        };
        records.push(SourceComplexRecord {
            official_complex_code,
            administrative_code: json_text(record, "addr_cd"),
            address_text: json_text(record, "danji_loc"),
            source_dataset: DETAIL_SOURCE_SLUG.to_owned(),
        });
    }
    Ok(records)
}

fn read_source_notices(path: &Path) -> anyhow::Result<Vec<SourceNoticeRecord>> {
    let records = read_ilis_records(path)?;
    Ok(records
        .iter()
        .filter_map(|record| {
            let official_complex_code = json_text(record, "danji_cd")?;
            let notice_record_id = json_text(record, "ref_seq").map_or_else(
                || format!("danji_cd={official_complex_code}"),
                |seq| format!("ref_seq={seq}"),
            );
            Some(SourceNoticeRecord {
                official_complex_code,
                administrative_code: json_text(record, "noti_addr"),
                notice_record_id,
            })
        })
        .collect())
}

fn read_ilis_records(path: &Path) -> anyhow::Result<Vec<JsonValue>> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read the ILIS Bronze object {}", path.display()))?;
    let payload = serde_json::from_slice::<JsonValue>(&bytes)
        .with_context(|| format!("the ILIS Bronze object {} is not JSON", path.display()))?;
    let Some(JsonValue::Array(records)) = payload.pointer(ILIS_RECORDS_POINTER) else {
        bail!(
            "the ILIS Bronze object {} carries no array at {ILIS_RECORDS_POINTER}",
            path.display()
        );
    };
    Ok(records.clone())
}

/// Reads one field as trimmed text, treating JSON `null`, absence, and blank alike.
///
/// ILIS writes numbers unquoted for some columns (`ref_seq`) and strings for others, so a
/// string-only read would silently drop the numeric ones.
fn json_text(record: &JsonValue, field: &str) -> Option<String> {
    let value = record.get(field)?;
    let text = match value {
        JsonValue::String(text) => text.trim().to_owned(),
        JsonValue::Number(number) => number.to_string(),
        _ => return None,
    };
    (!text.is_empty()).then_some(text)
}

/// Writes the resolution JSONL, refusing to replace an earlier one.
fn write_resolution_jsonl(
    output_path: &Path,
    resolved: &[ResolvedComplexAddress],
) -> anyhow::Result<()> {
    if output_path.exists() {
        bail!(
            "the address resolution already exists and is append-only evidence: {}",
            output_path.display()
        );
    }
    if resolved.is_empty() {
        bail!(
            "the address resolution derived zero addresses; an empty resolution would fail the \
             producer anyway, so nothing was written"
        );
    }
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }
    let mut payload = Vec::new();
    for resolution in resolved {
        let line = json!({
            "official_complex_code": resolution.official_complex_code,
            "administrative_code": resolution.administrative_code,
            "administrative_code_granularity": resolution.granularity.wire_name(),
            "address_text": resolution.address_text,
            "address_source_dataset": resolution.address_source_dataset,
            "address_source_record_id": format!(
                "industrylandorkr:{}",
                resolution.address_source_record_id
            ),
            "resolution_tier": resolution.resolution_tier.wire_name(),
        });
        payload
            .write_all(serde_json::to_string(&line)?.as_bytes())
            .context("failed to buffer a resolution line")?;
        payload.write_all(b"\n").context("failed to buffer EOL")?;
    }
    fs::write(output_path, &payload)
        .with_context(|| format!("failed to write {}", output_path.display()))
}

fn summary_json(
    config: &AddressResolutionBuildConfig,
    resolution: &IndustrialComplexAddressResolution,
    inputs: &ResolutionInputCounts,
) -> JsonValue {
    let mut evidence_limitations = vec![
        "local_bronze_derivation_only",
        "does_not_run_the_producer",
        "does_not_approve_production_cutover",
    ];
    if resolution
        .tier_counts()
        .get("modal_notice_code")
        .is_some_and(|count| *count > 0)
    {
        evidence_limitations.push("some_addresses_resolved_by_the_modal_notice_heuristic");
    }
    if !resolution.learned_code_migrations.is_empty() {
        evidence_limitations.push("some_addresses_resolved_through_a_learned_code_migration");
    }
    if !resolution.unresolved.is_empty() {
        evidence_limitations.push("some_complexes_have_no_sourced_address");
    }
    json!({
        "schema_version": "foundation-platform.industrial_complex_address_resolution.v1",
        "generated_at_utc": Utc::now().to_rfc3339(),
        "status": "ready",
        "completion_claim_allowed": false,
        "production_cutover_allowed": false,
        "inputs": {
            "bronze_local_object_root": config.bronze_local_object_root.display().to_string(),
            "profile_object_key": inputs.profile_object_key,
            "complex_list_object_key": inputs.complex_list_object_key,
            "notice_object_key": inputs.notice_object_key,
            "sigungu_authority_path": config.sigungu_authority_path.display().to_string(),
            "sigungu_authority_count": inputs.sigungu_authority_count,
            "target_complex_count": inputs.target_complex_count,
            "source_complex_count": inputs.source_complex_count,
            "source_detail_complex_count": inputs.source_detail_complex_count,
            "source_notice_count": inputs.source_notice_count,
        },
        "output": {
            "path": config.output_path.display().to_string(),
            "resolved_count": resolution.resolved.len(),
            "unresolved_count": resolution.unresolved.len(),
            "tier_counts": resolution.tier_counts(),
            "unresolved_counts": resolution.unresolved_counts(),
            "unresolved_complex_codes": resolution
                .unresolved
                .iter()
                .map(|unresolved| json!({
                    "official_complex_code": unresolved.official_complex_code,
                    "reason": unresolved.reason.wire_name(),
                }))
                .collect::<Vec<_>>(),
            // Published because it is derived, not typed: every pair is checkable against the
            // notices this run read.
            "learned_code_migrations": resolution.learned_code_migrations,
        },
        "evidence_limitations": evidence_limitations,
    })
}

fn bronze_object_key(root: &Path, object_path: &Path) -> anyhow::Result<String> {
    let relative = object_path.strip_prefix(root).with_context(|| {
        format!(
            "object {} is not under root {}",
            object_path.display(),
            root.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

impl AddressResolutionBuildConfig {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            bronze_local_object_root: PathBuf::from(required_env(&format!(
                "{PREFIX}_BRONZE_ROOT"
            ))?),
            profile_source_slug: optional_env(&format!("{PREFIX}_PROFILE_SOURCE_SLUG"))?
                .unwrap_or_else(|| DEFAULT_PROFILE_SOURCE_SLUG.to_owned()),
            profile_object: optional_env(&format!("{PREFIX}_PROFILE_OBJECT"))?,
            profile_sheet_name: optional_env(&format!("{PREFIX}_PROFILE_SHEET_NAME"))?,
            complex_list_object: optional_env(&format!("{PREFIX}_COMPLEX_LIST_OBJECT"))?,
            notice_object: optional_env(&format!("{PREFIX}_NOTICE_OBJECT"))?,
            sigungu_authority_path: PathBuf::from(
                required_env(&format!("{PREFIX}_SIGUNGU_AUTHORITY_PATH")).context(
                    "the district authority is what makes an administrative code checkable; \
                     without it every resolution would be an unverified copy of the source",
                )?,
            ),
            output_path: PathBuf::from(required_env(&format!("{PREFIX}_OUTPUT_PATH"))?),
            summary_path: optional_env(&format!("{PREFIX}_SUMMARY_PATH"))?.map(PathBuf::from),
        })
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    optional_env(name)?.with_context(|| format!("{name} is required"))
}

fn optional_env(name: &str) -> anyhow::Result<Option<String>> {
    match env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}
