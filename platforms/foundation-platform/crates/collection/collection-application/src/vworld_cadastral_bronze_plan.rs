//! Planning helpers for `VWorld` cadastral 2D Data API Bronze ingestion pages.

use std::{collections::BTreeMap, fmt::Write as _};

use chrono::NaiveDate;
use collection_domain::{build_bronze_object_key, BronzeObjectKeyParts};
use foundation_shared_kernel::ids::IngestionRunId;
use foundation_shared_kernel::ObjectKey;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::{
    plan_public_data_bronze_page, PublicDataBronzePagePlan, PublicDataBronzePagePlanInput,
    PublicDataBronzePageRequest, PublicDataBronzePlanError, PublicDataPageRequest,
    PublicDataPartitionField, PublicDataSchemaObservation,
};

/// Provider operation for the cadastral 2D Data API. `GetFeature` is the ONLY operation this lane
/// ever issues, so it is a per-lane constant: it stays in the lineage (`source_partition_key` +
/// `request_params`) for traceability but is intentionally dropped from the object key, which would
/// otherwise carry a redundant `operation=GetFeature` segment (Task 3 / T1.1, ADR 0016 / OD-2).
const OPERATION: &str = "GetFeature";
const LOGICAL_ITEMS_POINTER: &str = "/response/result/featureCollection/features";
const VWORLD_CADASTRAL_KEY_SUFFIX: &str = "properties.pnu";
/// Number of leading sha256 hex chars kept in the `filter_fingerprint=` fallback scope key. A SHORT,
/// intentional fingerprint (not a bare 64-hex sha) for filters that cannot be reduced to a single
/// clean `field:=:value`, so the new key passes the future Bronze semantic guard (no bare 64-hex).
const FILTER_FINGERPRINT_HEX_LEN: usize = 12;

/// Request parameters for one `VWorld` cadastral 2D Data API page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldCadastralPageRequest {
    /// Dataset id, usually `LP_PA_CBND_BUBUN` for cadastral parcel boundaries.
    pub dataset: String,
    /// Required attribute filter such as `emdCd:=:11680103` or `pnu:=:9999900801105800001`.
    pub attr_filter: Option<String>,
    /// Optional provider columns requested from the 2D Data API.
    pub columns: Vec<String>,
    /// Whether geometry should be returned.
    pub geometry: bool,
    /// Whether attributes should be returned.
    pub attribute: bool,
    /// Optional coordinate reference system, for example `EPSG:4326`.
    pub crs: Option<String>,
    /// One-based page number.
    pub page: u32,
    /// Requested page size. `VWorld` documents a maximum of 1000.
    pub size: u32,
}

impl VWorldCadastralPageRequest {
    /// Returns the canonical provider partition key represented by this request.
    ///
    /// # Errors
    ///
    /// Returns `VWorldCadastralBronzePlanError` when any request parameter is invalid.
    pub fn source_partition_key(&self) -> Result<String, VWorldCadastralBronzePlanError> {
        self.to_public_data_request()?.source_partition_key()
    }

    fn to_public_data_request(
        &self,
    ) -> Result<PublicDataBronzePageRequest, VWorldCadastralBronzePlanError> {
        validate_request(self)?;
        let scope_field = attr_filter_scope_field(self.attr_filter.as_deref().unwrap_or(""));
        let mut query_params = BTreeMap::from([
            ("service".to_owned(), "data".to_owned()),
            ("request".to_owned(), OPERATION.to_owned()),
            ("data".to_owned(), self.dataset.clone()),
            ("format".to_owned(), "json".to_owned()),
            (
                "geometry".to_owned(),
                bool_query_value(self.geometry).to_owned(),
            ),
            (
                "attribute".to_owned(),
                bool_query_value(self.attribute).to_owned(),
            ),
        ]);
        if let Some(attr_filter) = &self.attr_filter {
            query_params.insert("attrFilter".to_owned(), attr_filter.clone());
        }
        if !self.columns.is_empty() {
            query_params.insert("columns".to_owned(), self.columns.join(","));
        }
        if let Some(crs) = &self.crs {
            query_params.insert("crs".to_owned(), crs.clone());
        }

        // Object-key partition fields: dataset (a real, varying, human-readable layer id) + the
        // human-readable scope parsed from `attr_filter` (`pnu=`/`emd=` or `filter_fingerprint=`).
        // Dropped vs the old key (Task 3 / T1.1, ADR 0016 / OD-2 / D-D):
        //   - `operation=GetFeature` — a per-lane constant; kept in lineage (see `OPERATION`).
        //   - `filter_kind=attr`     — a `const`, zero information.
        //   - `filter_sha256=<64hex>`— opaque; replaced by the human-readable scope below.
        //   - `size=NNNNNN`          — a request knob; `num_of_rows` still carries it for lineage.
        Ok(PublicDataBronzePageRequest {
            operation: OPERATION.to_owned(),
            partition_fields: vec![
                PublicDataPartitionField {
                    name: "dataset".to_owned(),
                    value: self.dataset.clone(),
                },
                scope_field,
            ],
            query_params,
            format_query_param: None,
            page_param_name: "page".to_owned(),
            size_param_name: "size".to_owned(),
            page_no: self.page,
            num_of_rows: self.size,
        })
    }
}

/// Input required to plan one immutable `VWorld` cadastral Bronze page object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VWorldCadastralBronzePagePlanInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as metadata; object keys are not partitioned by date.
    pub ingest_date: NaiveDate,
    /// Ingestion run id used in the object key.
    pub ingestion_run_id: IngestionRunId,
    /// Provider request parameters.
    pub request: VWorldCadastralPageRequest,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: JsonValue,
}

/// Planned metadata for one immutable `VWorld` cadastral Bronze page.
pub type VWorldCadastralBronzePagePlan = PublicDataBronzePagePlan;

/// Observed field statistics for one `VWorld` cadastral payload field path.
pub type VWorldCadastralSchemaObservation = PublicDataSchemaObservation;

/// Error returned while planning a `VWorld` cadastral Bronze page.
pub type VWorldCadastralBronzePlanError = PublicDataBronzePlanError;

/// Builds the canonical Bronze object key for one `VWorld` cadastral 2D Data API page.
///
/// The cadastral object key intentionally DIFFERS from the generic public-data object key in one
/// way: it drops the leading `operation=GetFeature` segment (a per-lane constant — see
/// [`OPERATION`]). The provider operation stays in `source_partition_key` + `request_params` for
/// lineage, but the object key keeps only the meaningful, varying Hive partitions
/// (`dataset=<layer>` + the human-readable `attr_filter` scope) so it reads cleanly and passes the
/// future Bronze semantic guard (Task 3 / T1.1, ADR 0016 / OD-2 / D-D).
///
/// # Errors
///
/// Returns `VWorldCadastralBronzePlanError` when request parameters or key parts are invalid.
pub fn build_vworld_cadastral_bronze_object_key(
    source_slug: &str,
    request: &VWorldCadastralPageRequest,
) -> Result<ObjectKey, VWorldCadastralBronzePlanError> {
    let public_request = request.to_public_data_request()?;
    cadastral_object_key(source_slug, &public_request)
}

/// Builds the cadastral object key from the already-validated public-data request, dropping the
/// `operation=` segment the generic builder would prepend. Only `dataset=<layer>` + the
/// `attr_filter` scope (`pnu=`/`emd=`/`filter_fingerprint=`) appear as partitions; the page is the
/// leaf filename.
fn cadastral_object_key(
    source_slug: &str,
    request: &PublicDataBronzePageRequest,
) -> Result<ObjectKey, VWorldCadastralBronzePlanError> {
    // Validate via the shared path; we then build the object key from the same partition_fields but
    // WITHOUT the operation segment (the generic `source_partition_key` keeps it for lineage).
    let _ = request.source_partition_key()?;
    let mut partition_path = String::new();
    for field in &request.partition_fields {
        if !partition_path.is_empty() {
            partition_path.push('/');
        }
        let _ = write!(&mut partition_path, "{}={}", field.name, field.value);
    }
    let leaf_name = format!("page-{:06}", request.page_no);
    Ok(build_bronze_object_key(BronzeObjectKeyParts {
        source_slug,
        partition_path: &partition_path,
        leaf_name: &leaf_name,
        extension: "json",
    })?)
}

/// Plans object metadata for one `VWorld` cadastral raw response page.
///
/// Reuses the shared public-data planner for checksum / schema profiling / lineage, then overrides
/// the object key with the cadastral-specific [`cadastral_object_key`] so the `operation=GetFeature`
/// segment is dropped from the key (kept only in `source_partition_key` lineage).
///
/// # Errors
///
/// Returns `VWorldCadastralBronzePlanError` when request parameters cannot be represented in the
/// canonical Bronze object layout.
pub fn plan_vworld_cadastral_bronze_page(
    input: VWorldCadastralBronzePagePlanInput<'_>,
) -> Result<VWorldCadastralBronzePagePlan, VWorldCadastralBronzePlanError> {
    let source_slug = input.source_slug;
    let public_request = input.request.to_public_data_request()?;
    let mut plan = plan_public_data_bronze_page(PublicDataBronzePagePlanInput {
        source_slug,
        ingest_date: input.ingest_date,
        ingestion_run_id: input.ingestion_run_id,
        request: public_request.clone(),
        raw_payload: input.raw_payload,
        payload: input.payload,
        logical_items_pointer: LOGICAL_ITEMS_POINTER,
        candidate_key_field_suffixes: vec![VWORLD_CADASTRAL_KEY_SUFFIX.to_owned()],
    })?;
    // Drop `operation=GetFeature` from the OBJECT KEY only; `source_partition_key` (lineage) keeps
    // it. dedupe_key is built from `source_partition_key`, so it is unaffected.
    plan.object_key = cadastral_object_key(source_slug, &public_request)?;
    Ok(plan)
}

impl PublicDataPageRequest for VWorldCadastralPageRequest {
    fn compile_bronze_page_plan(
        &self,
        source_slug: &str,
        ingest_date: NaiveDate,
        ingestion_run_id: IngestionRunId,
        raw_payload: Vec<u8>,
        payload: JsonValue,
    ) -> Result<PublicDataBronzePagePlan, PublicDataBronzePlanError> {
        plan_vworld_cadastral_bronze_page(VWorldCadastralBronzePagePlanInput {
            source_slug,
            ingest_date,
            ingestion_run_id,
            request: self.clone(),
            raw_payload,
            payload,
        })
    }
}

fn validate_request(
    request: &VWorldCadastralPageRequest,
) -> Result<(), VWorldCadastralBronzePlanError> {
    validate_dataset(&request.dataset)?;
    if request.attr_filter.is_none() {
        return Err(PublicDataBronzePlanError::InvalidRequest(
            "attr_filter is required for VWorld cadastral requests".to_owned(),
        ));
    }
    if request.page == 0 || request.size == 0 {
        return Err(PublicDataBronzePlanError::InvalidRequest(
            "page and size must be greater than zero".to_owned(),
        ));
    }
    if request.size > 1_000 {
        return Err(PublicDataBronzePlanError::InvalidRequest(
            "size must not exceed 1000".to_owned(),
        ));
    }
    if let Some(attr_filter) = &request.attr_filter {
        validate_filter_value("attr_filter", attr_filter)?;
    }
    for column in &request.columns {
        validate_identifier("column", column)?;
    }
    if let Some(crs) = &request.crs {
        validate_crs(crs)?;
    }
    Ok(())
}

fn validate_dataset(dataset: &str) -> Result<(), VWorldCadastralBronzePlanError> {
    if !dataset.is_empty()
        && dataset
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Ok(());
    }
    Err(PublicDataBronzePlanError::InvalidRequest(
        "dataset must contain only uppercase ASCII letters, digits, and '_'".to_owned(),
    ))
}

fn validate_identifier(
    field: &'static str,
    value: &str,
) -> Result<(), VWorldCadastralBronzePlanError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Ok(());
    }
    Err(PublicDataBronzePlanError::InvalidRequest(format!(
        "{field} must contain only ASCII letters, digits, and '_'"
    )))
}

fn validate_filter_value(
    field: &'static str,
    value: &str,
) -> Result<(), VWorldCadastralBronzePlanError> {
    if value.trim() != value || value.is_empty() || value.contains('\n') || value.contains('\r') {
        return Err(PublicDataBronzePlanError::InvalidRequest(format!(
            "{field} must be non-empty single-line text without surrounding whitespace"
        )));
    }
    Ok(())
}

fn validate_crs(crs: &str) -> Result<(), VWorldCadastralBronzePlanError> {
    if let Some(raw) = crs.strip_prefix("EPSG:") {
        if !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()) {
            return Ok(());
        }
    }
    Err(PublicDataBronzePlanError::InvalidRequest(
        "crs must use EPSG:<digits>".to_owned(),
    ))
}

/// Derives the human-readable object-key scope partition for a cadastral `attr_filter`
/// (Task 3 / T1.1, ADR 0016 / OD-2 / D-D).
///
/// REFUSAL-TO-MISLABEL is the core rule: a scope key is emitted ONLY for a filter that is
/// unambiguously a single `field:=:value` over a known clean field; anything else falls back to a
/// short `filter_fingerprint=<12 hex>`. Concretely:
///
/// - `pnu:=:<value>`   → `pnu=<value>`   (only when `<value>` is clean — ASCII digits)
/// - `emdCd:=:<value>` → `emd=<value>`   (provider field `emdCd` renamed to `emd`; digits only)
/// - ANYTHING ELSE     → `filter_fingerprint=<12hex>`, including:
///   - compound / multi-clause filters (e.g. `... AND ...`) — not a single `field:op:value`;
///   - operators other than `=` (e.g. `:LIKE:`);
///   - a value containing `:` (the `field:op:value` split is then ambiguous — 4+ parts);
///   - an unknown field, or a `value` that is not clean digits.
///
/// When uncertain whether a filter is a clean single field, this ALWAYS falls back to the
/// fingerprint (the safe direction) rather than guessing a `pnu=`/`emd=` label.
fn attr_filter_scope_field(attr_filter: &str) -> PublicDataPartitionField {
    if let Some((name, value)) = parse_single_field_equals(attr_filter) {
        return PublicDataPartitionField {
            name,
            value: value.to_owned(),
        };
    }
    PublicDataPartitionField {
        name: "filter_fingerprint".to_owned(),
        value: attr_filter_fingerprint(attr_filter),
    }
}

/// Parses a single, unambiguous `field:=:value` expression into a `(scope_name, value)` pair for the
/// known clean fields, or returns `None` for anything that cannot be cleanly reduced (so the caller
/// fingerprints instead of guessing). Splitting on `:` MUST yield EXACTLY three parts — a value that
/// itself contains `:` yields four+ parts and is therefore refused here.
fn parse_single_field_equals(attr_filter: &str) -> Option<(String, &str)> {
    let mut parts = attr_filter.split(':');
    let field = parts.next()?;
    let op = parts.next()?;
    let value = parts.next()?;
    // Exactly three parts: a fourth part means the value contained ':' (ambiguous) → refuse.
    if parts.next().is_some() {
        return None;
    }
    if op != "=" {
        return None;
    }
    let scope_name = match field {
        "pnu" => "pnu",
        "emdCd" => "emd",
        _ => return None,
    };
    if !is_clean_scope_value(value) {
        return None;
    }
    Some((scope_name.to_owned(), value))
}

/// A scope value is "clean" only when it is a non-empty run of ASCII digits (PNU / region codes are
/// numeric). This keeps the segment a safe single Hive `key=value` token and refuses to label a
/// value that could contain `=`, `/`, whitespace, or other surprises.
fn is_clean_scope_value(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

/// Returns the first [`FILTER_FINGERPRINT_HEX_LEN`] hex chars of the sha256 of the canonical
/// `attrFilter=<raw>` string — a SHORT, intentional fingerprint (not a bare 64-hex sha) used as the
/// fallback scope for filters that cannot be reduced to a single clean field.
fn attr_filter_fingerprint(attr_filter: &str) -> String {
    let canonical = format!("attrFilter={attr_filter}");
    let mut hex = String::with_capacity(FILTER_FINGERPRINT_HEX_LEN);
    for byte in Sha256::digest(canonical.as_bytes())
        .iter()
        .take(FILTER_FINGERPRINT_HEX_LEN / 2)
    {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

const fn bool_query_value(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

#[cfg(test)]
mod tests {
    use super::{attr_filter_fingerprint, attr_filter_scope_field, FILTER_FINGERPRINT_HEX_LEN};

    /// A single clean `pnu:=:<19 digits>` filter → `pnu=<value>` scope key.
    #[test]
    fn single_pnu_filter_yields_pnu_scope_key() {
        let field = attr_filter_scope_field("pnu:=:9999900801105800001");
        assert_eq!(field.name, "pnu");
        assert_eq!(field.value, "9999900801105800001");
    }

    /// A single clean `emdCd:=:<code>` filter → `emd=<value>` (provider field `emdCd` renamed `emd`).
    #[test]
    fn single_emd_filter_yields_emd_scope_key() {
        let field = attr_filter_scope_field("emdCd:=:11680103");
        assert_eq!(field.name, "emd");
        assert_eq!(field.value, "11680103");
    }

    /// A compound / multi-clause filter is NOT a single `field:op:value`, so it must fall back to a
    /// 12-hex `filter_fingerprint` — and must NEVER be mislabeled `pnu=`/`emd=`.
    #[test]
    fn compound_filter_falls_back_to_12_hex_fingerprint_not_mislabeled() {
        let field = attr_filter_scope_field("emdCd:=:11680103 AND jibun:LIKE:580");
        assert_eq!(field.name, "filter_fingerprint");
        assert_eq!(field.value.len(), FILTER_FINGERPRINT_HEX_LEN);
        assert_eq!(field.value.len(), 12);
        assert!(
            field
                .value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
            "fingerprint must be lowercase hex: {}",
            field.value
        );
        assert_ne!(field.name, "pnu");
        assert_ne!(field.name, "emd");
    }

    /// An operator other than `=` (e.g. `LIKE`) cannot be a clean equals-scope → fingerprint.
    #[test]
    fn non_equals_operator_falls_back_to_fingerprint() {
        let field = attr_filter_scope_field("pnu:LIKE:282001");
        assert_eq!(field.name, "filter_fingerprint");
        assert_eq!(field.value.len(), 12);
    }

    /// A value that itself contains `:` makes the `field:op:value` split ambiguous (4+ parts), so the
    /// SAFE direction is to fingerprint — never guess the value boundary and label it `pnu=`.
    #[test]
    fn value_containing_colon_falls_back_to_fingerprint_not_mislabeled() {
        let field = attr_filter_scope_field("pnu:=:a:b");
        assert_eq!(
            field.name, "filter_fingerprint",
            "a value containing ':' must fingerprint, not label pnu="
        );
        assert_ne!(field.name, "pnu");
        assert_eq!(field.value.len(), 12);
    }

    /// An unknown field is not a known clean scope → fingerprint (refuse to invent a scope name).
    #[test]
    fn unknown_field_falls_back_to_fingerprint() {
        let field = attr_filter_scope_field("ldCode:=:11680");
        assert_eq!(field.name, "filter_fingerprint");
        assert_eq!(field.value.len(), 12);
    }

    /// A non-numeric value for a known field is not "clean" (could carry surprises) → fingerprint.
    #[test]
    fn non_numeric_value_falls_back_to_fingerprint() {
        let field = attr_filter_scope_field("pnu:=:abc");
        assert_eq!(field.name, "filter_fingerprint");
        assert_eq!(field.value.len(), 12);
    }

    /// The fingerprint is the first 12 hex chars of sha256("attrFilter=<raw>") — deterministic, and
    /// distinct per filter (so two different compound filters do not collide on the key).
    #[test]
    fn fingerprint_is_deterministic_and_filter_specific() {
        let one = attr_filter_fingerprint("emdCd:=:11680103 AND jibun:LIKE:580");
        let two = attr_filter_fingerprint("emdCd:=:11680103 AND jibun:LIKE:580");
        let other = attr_filter_fingerprint("emdCd:=:11680104 AND jibun:LIKE:580");
        assert_eq!(one, two);
        assert_ne!(one, other);
        assert_eq!(one.len(), 12);
    }
}
