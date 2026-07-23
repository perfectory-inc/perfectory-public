//! Generic planning helpers for JSON public-data Bronze ingestion pages.

use std::{collections::BTreeMap, fmt::Write as _};

use chrono::NaiveDate;
use collection_domain::{
    build_bronze_object_key, canonical_page_size, operation_collapses_into_slug,
    BronzeObjectKeyError, BronzeObjectKeyParts, SchemaObservedType, SnapshotBasis,
    SnapshotGranularity,
};
use foundation_shared_kernel::ids::IngestionRunId;
use foundation_shared_kernel::ObjectKey;
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// One canonical partition segment used in a Bronze object key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataPartitionField {
    /// Provider-neutral partition name, for example `sigungu`, `bjdong`, `lawd`, or `month`.
    pub name: String,
    /// Provider partition value represented by this page.
    pub value: String,
}

/// One fixed provider query parameter stored in Bronze metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataFixedQueryParam {
    /// Provider query parameter name.
    pub name: String,
    /// Provider query parameter value.
    pub value: String,
}

/// Generic request parameters for one JSON public-data page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataBronzePageRequest {
    /// Provider operation or path segment, for example `getBrTitleInfo`.
    pub operation: String,
    /// Ordered provider-neutral partition fields included in the Bronze object key.
    pub partition_fields: Vec<PublicDataPartitionField>,
    /// Provider query parameters other than `serviceKey`, `_type`, `pageNo`, and `numOfRows`.
    pub query_params: BTreeMap<String, String>,
    /// Optional fixed response-format parameter, for example `_type=json`.
    pub format_query_param: Option<PublicDataFixedQueryParam>,
    /// Provider page-number query parameter name.
    pub page_param_name: String,
    /// Provider page-size query parameter name.
    pub size_param_name: String,
    /// One-based page number.
    pub page_no: u32,
    /// Requested page size.
    pub num_of_rows: u32,
}

impl PublicDataBronzePageRequest {
    /// Returns the canonical provider partition key represented by this request.
    ///
    /// This is the value stored in the `bronze_object.source_partition_key` metadata column. It
    /// keeps the full provider scope (`operation=.../{fields}.../page=NNNNNN`) for lineage and is
    /// distinct from the readable physical object-key path (which carries the page in the leaf
    /// filename, not a partition segment).
    ///
    /// # Errors
    ///
    /// Returns `PublicDataBronzePlanError` when any request parameter is invalid.
    pub fn source_partition_key(&self) -> Result<String, PublicDataBronzePlanError> {
        validate_request(self)?;

        let mut key = format!("operation={}", self.operation);
        for field in &self.partition_fields {
            let _ = write!(&mut key, "/{}={}", field.name, field.value);
        }
        let _ = write!(&mut key, "/page={:06}", self.page_no);
        Ok(key)
    }

    /// Returns the readable physical object-key `(partition_path, leaf_name)` for this page request
    /// (ADR 0019). The leaf is the deterministic `page-NNNNNN` sequence id; the partition path holds
    /// the meaningful low-cardinality Hive segments. A leading `operation=` segment is dropped when
    /// the operation 1:1-maps to the source slug's `dataset_slug` (ADR 0016 T1.2 / D-D), via the
    /// single collection-domain [`operation_collapses_into_slug`] rule.
    ///
    /// # Errors
    ///
    /// Returns `PublicDataBronzePlanError` when any request parameter is invalid.
    pub fn object_key_partition_and_leaf(
        &self,
        source_slug: &str,
    ) -> Result<(String, String), PublicDataBronzePlanError> {
        validate_request(self)?;

        let mut partition_path = String::new();
        if !operation_collapses_into_slug(&self.operation, source_slug) {
            let _ = write!(&mut partition_path, "operation={}", self.operation);
        }
        for field in &self.partition_fields {
            if let Some(period) = request_month_period(field)? {
                if !partition_path.is_empty() {
                    partition_path.push('/');
                }
                let _ = write!(&mut partition_path, "period={period}");
                break;
            }
        }
        for field in &self.partition_fields {
            if request_month_period(field)?.is_some() {
                continue;
            }
            if !partition_path.is_empty() {
                partition_path.push('/');
            }
            let _ = write!(&mut partition_path, "{}={}", field.name, field.value);
        }
        let leaf_name = format!("page-{:06}", self.page_no);
        Ok((partition_path, leaf_name))
    }
}

/// Input required to plan one immutable JSON public-data Bronze page object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicDataBronzePagePlanInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as run context. The readable object key (ADR 0019) is not
    /// partitioned by date and does not use this field.
    pub ingest_date: NaiveDate,
    /// Ingestion run that owns this object. Recorded on the `bronze_object` row by the service
    /// layer; the readable object key (ADR 0019) does not carry the run id.
    pub ingestion_run_id: IngestionRunId,
    /// Provider request parameters.
    pub request: PublicDataBronzePageRequest,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: JsonValue,
    /// JSON pointer to the logical records array or object inside the provider response.
    pub logical_items_pointer: &'a str,
    /// Field-path suffixes that should be scored as candidate source keys when non-null.
    pub candidate_key_field_suffixes: Vec<String>,
}

/// A data.go.kr page request that can compile itself into the shared public-data Bronze page plan.
///
/// This is the single seam every data.go.kr page lane (building-register, real-transaction, and the
/// upcoming V-World cadastral / NED / land lanes) plugs into so the [`BronzeCommitter`] can OWN the
/// key-compile through ONE generic commit path instead of a per-lane `commit_<lane>_page` method
/// (ADR 0016). A lane's request type carries its own provider partition shape and validation; the
/// trait's [`compile_bronze_page_plan`](PublicDataPageRequest::compile_bronze_page_plan) is the only
/// lane-specific step (it supplies the lane's `logical_items_pointer` + candidate-key suffixes and
/// validates the lane-specific request fields), and it returns the shared
/// [`PublicDataBronzePagePlan`] every lane already produces.
///
/// [`BronzeCommitter`]: crate::bronze_committer::BronzeCommitter
pub trait PublicDataPageRequest {
    /// Compiles this lane request + payload into the shared public-data Bronze page plan.
    ///
    /// The committer calls this as its owned key-compile step (it does not pre-build a plan), so the
    /// later per-lane rules (page-size validation, operation-collapse, the cadastral scope-key, and
    /// the reserved-partition-key semantic guard) attach inside the lane's compile or the shared
    /// `plan_public_data_bronze_page` it delegates to — never as a separate commit method.
    ///
    /// # Errors
    ///
    /// Returns [`PublicDataBronzePlanError`] when the raw request cannot be compiled into a valid
    /// Bronze object key/plan (for example a malformed region code).
    fn compile_bronze_page_plan(
        &self,
        source_slug: &str,
        ingest_date: NaiveDate,
        ingestion_run_id: IngestionRunId,
        raw_payload: Vec<u8>,
        payload: JsonValue,
    ) -> Result<PublicDataBronzePagePlan, PublicDataBronzePlanError>;
}

/// Planned metadata for one immutable JSON public-data Bronze page.
#[derive(Clone, Debug, PartialEq)]
pub struct PublicDataBronzePagePlan {
    /// Provider-neutral object key for the raw Bronze payload.
    pub object_key: ObjectKey,
    /// Canonical source coverage identity for skip / coverage / dedupe.
    pub source_identity_key: String,
    /// Provider partition represented by the page.
    pub source_partition_key: String,
    /// Idempotency key scoped to the source catalog entry.
    pub dedupe_key: String,
    /// Lowercase SHA-256 checksum of the raw payload.
    pub checksum_sha256: String,
    /// Raw payload size in bytes.
    pub size_bytes: u64,
    /// Number of logical records at `logical_items_pointer`.
    pub logical_record_count: u64,
    /// Request parameters stored with the Bronze object metadata.
    pub request_params: JsonValue,
    /// Human-readable source period bucket, when the request has a month scope.
    pub snapshot_period: Option<String>,
    /// Canonical source as-of date.
    pub snapshot_date: NaiveDate,
    /// Granularity of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_granularity: SnapshotGranularity,
    /// Provenance of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_basis: SnapshotBasis,
    /// Raw payload bytes to write to object storage.
    pub raw_payload: Vec<u8>,
    /// Schema observations extracted from the parsed payload.
    pub schema_observations: Vec<PublicDataSchemaObservation>,
}

/// Observed field statistics for one public-data payload field path.
#[derive(Clone, Debug, PartialEq)]
pub struct PublicDataSchemaObservation {
    /// JSON field path, with arrays represented by `[]`.
    pub field_path: String,
    /// Observed JSON-like type.
    pub observed_type: SchemaObservedType,
    /// Number of non-null samples.
    pub nonnull_count: u64,
    /// Number of null samples.
    pub null_count: u64,
    /// Representative sample values.
    pub sample_values: JsonValue,
    /// Heuristic usefulness as a source key.
    pub candidate_key_score: f64,
}

/// Error returned while planning a generic public-data Bronze page.
#[derive(Debug, Error)]
pub enum PublicDataBronzePlanError {
    /// The canonical Bronze object key could not be built.
    #[error(transparent)]
    ObjectKey(#[from] BronzeObjectKeyError),
    /// A request parameter was invalid.
    #[error("invalid public-data Bronze request: {0}")]
    InvalidRequest(String),
}

/// Builds the canonical Bronze object key for one JSON public-data API page.
///
/// # Errors
///
/// Returns `PublicDataBronzePlanError` when request parameters or key parts are invalid.
pub fn build_public_data_bronze_object_key(
    source_slug: &str,
    request: &PublicDataBronzePageRequest,
) -> Result<ObjectKey, PublicDataBronzePlanError> {
    let (partition_path, leaf_name) = request.object_key_partition_and_leaf(source_slug)?;
    Ok(build_bronze_object_key(BronzeObjectKeyParts {
        source_slug,
        partition_path: &partition_path,
        leaf_name: &leaf_name,
        extension: "json",
    })?)
}

/// Plans object metadata for one generic public-data raw response page.
///
/// # Errors
///
/// Returns `PublicDataBronzePlanError` when request parameters cannot be represented in the
/// canonical Bronze object layout.
pub fn plan_public_data_bronze_page(
    input: PublicDataBronzePagePlanInput,
) -> Result<PublicDataBronzePagePlan, PublicDataBronzePlanError> {
    validate_logical_items_pointer(input.logical_items_pointer)?;

    // Canonical page-size SSOT enforcement (ADR 0016 acceptance #7 / D-A): a pinned operation whose
    // request page size differs from its fixed canonical would map "page N" onto a different slice of
    // provider rows between runs and silently collide different bytes onto the same physical
    // `page-NNNNNN` object key. Fail at PLAN time before any object is written.
    if let Some(canonical) = canonical_page_size(&input.request.operation) {
        if input.request.num_of_rows != canonical {
            return Err(PublicDataBronzePlanError::InvalidRequest(format!(
                "operation '{}' requires canonical page size {canonical} (ADR 0016 D-A) but the request used {}",
                input.request.operation, input.request.num_of_rows
            )));
        }
    }

    let source_partition_key = input.request.source_partition_key()?;
    let source_identity_key = source_identity_key(&input.request)?;
    let object_key = build_public_data_bronze_object_key(input.source_slug, &input.request)?;
    let checksum_sha256 = sha256_hex(&input.raw_payload);
    let logical_record_count = count_logical_items(&input.payload, input.logical_items_pointer);
    let dedupe_key = format!(
        "{}:{}:sha256={}",
        input.source_slug, source_identity_key, checksum_sha256
    );
    let request_params = request_params_json(&input.request);
    let snapshot = snapshot_metadata(&input.request, input.ingest_date)?;
    let schema_observations = observe_schema(&input.payload, &input.candidate_key_field_suffixes);

    Ok(PublicDataBronzePagePlan {
        object_key,
        source_identity_key,
        source_partition_key,
        dedupe_key,
        checksum_sha256,
        size_bytes: input.raw_payload.len() as u64,
        logical_record_count,
        request_params,
        snapshot_period: snapshot.period,
        snapshot_date: snapshot.date,
        snapshot_granularity: snapshot.granularity,
        snapshot_basis: snapshot.basis,
        raw_payload: input.raw_payload,
        schema_observations,
    })
}

struct SnapshotMetadata {
    period: Option<String>,
    date: NaiveDate,
    granularity: SnapshotGranularity,
    basis: SnapshotBasis,
}

fn source_identity_key(
    request: &PublicDataBronzePageRequest,
) -> Result<String, PublicDataBronzePlanError> {
    validate_request(request)?;
    let mut key = String::new();
    for field in &request.partition_fields {
        if !key.is_empty() {
            key.push('/');
        }
        let _ = write!(&mut key, "{}={}", field.name, field.value);
    }
    let _ = write!(
        &mut key,
        "/page={:06}/page_size={}",
        request.page_no, request.num_of_rows
    );
    Ok(key)
}

fn snapshot_metadata(
    request: &PublicDataBronzePageRequest,
    fallback_date: NaiveDate,
) -> Result<SnapshotMetadata, PublicDataBronzePlanError> {
    for field in &request.partition_fields {
        if let Some(period) = request_month_period(field)? {
            return Ok(SnapshotMetadata {
                date: first_day_of_period(&period)?,
                period: Some(period),
                granularity: SnapshotGranularity::Month,
                basis: SnapshotBasis::RequestMonth,
            });
        }
    }
    Ok(SnapshotMetadata {
        period: None,
        date: fallback_date,
        granularity: SnapshotGranularity::Day,
        basis: SnapshotBasis::CollectedAtFallback,
    })
}

fn request_month_period(
    field: &PublicDataPartitionField,
) -> Result<Option<String>, PublicDataBronzePlanError> {
    if matches!(field.name.as_str(), "deal_ymd" | "month") {
        return Ok(Some(yyyymm_to_period(&field.value)?));
    }
    Ok(None)
}

fn yyyymm_to_period(value: &str) -> Result<String, PublicDataBronzePlanError> {
    if value.len() != 6 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(PublicDataBronzePlanError::InvalidRequest(format!(
            "month scope value must be YYYYMM, got {value:?}"
        )));
    }
    let period = format!("{}-{}", &value[0..4], &value[4..6]);
    let _ = first_day_of_period(&period)?;
    Ok(period)
}

fn first_day_of_period(period: &str) -> Result<NaiveDate, PublicDataBronzePlanError> {
    if period.len() != 7 || period.as_bytes().get(4) != Some(&b'-') {
        return Err(PublicDataBronzePlanError::InvalidRequest(format!(
            "period must be YYYY-MM, got {period:?}"
        )));
    }
    let year = period[0..4].parse::<i32>().map_err(|_| {
        PublicDataBronzePlanError::InvalidRequest(format!(
            "period year must be numeric, got {period:?}"
        ))
    })?;
    let month = period[5..7].parse::<u32>().map_err(|_| {
        PublicDataBronzePlanError::InvalidRequest(format!(
            "period month must be numeric, got {period:?}"
        ))
    })?;
    NaiveDate::from_ymd_opt(year, month, 1).ok_or_else(|| {
        PublicDataBronzePlanError::InvalidRequest(format!(
            "period must contain a valid month, got {period:?}"
        ))
    })
}

fn request_params_json(request: &PublicDataBronzePageRequest) -> JsonValue {
    let mut params = JsonMap::new();
    params.insert(
        "operation".to_owned(),
        JsonValue::String(request.operation.clone()),
    );
    for (key, value) in &request.query_params {
        params.insert(key.clone(), JsonValue::String(value.clone()));
    }
    if let Some(format_query_param) = &request.format_query_param {
        params.insert(
            format_query_param.name.clone(),
            JsonValue::String(format_query_param.value.clone()),
        );
    }
    params.insert(
        request.page_param_name.clone(),
        JsonValue::from(request.page_no),
    );
    params.insert(
        request.size_param_name.clone(),
        JsonValue::from(request.num_of_rows),
    );
    JsonValue::Object(params)
}

fn validate_request(
    request: &PublicDataBronzePageRequest,
) -> Result<(), PublicDataBronzePlanError> {
    validate_operation(&request.operation)?;
    if request.page_no == 0 || request.num_of_rows == 0 {
        return Err(PublicDataBronzePlanError::InvalidRequest(
            "pageNo and numOfRows must be greater than zero".to_owned(),
        ));
    }
    if request.partition_fields.is_empty() {
        return Err(PublicDataBronzePlanError::InvalidRequest(
            "at least one partition field is required".to_owned(),
        ));
    }
    for field in &request.partition_fields {
        validate_identifier("partition field name", &field.name)?;
        validate_object_key_value("partition field value", &field.value)?;
    }
    validate_identifier("page parameter name", &request.page_param_name)?;
    validate_identifier("size parameter name", &request.size_param_name)?;
    if request.page_param_name == request.size_param_name {
        return Err(PublicDataBronzePlanError::InvalidRequest(
            "page and size parameter names must be distinct".to_owned(),
        ));
    }
    if let Some(format_query_param) = &request.format_query_param {
        validate_identifier("format query parameter name", &format_query_param.name)?;
        validate_query_value(&format_query_param.name, &format_query_param.value)?;
        if format_query_param.name == request.page_param_name
            || format_query_param.name == request.size_param_name
        {
            return Err(PublicDataBronzePlanError::InvalidRequest(
                "format query parameter must not reuse page or size parameter names".to_owned(),
            ));
        }
    }
    for (name, value) in &request.query_params {
        validate_identifier("query parameter name", name)?;
        validate_query_value(name, value)?;
        if name == &request.page_param_name || name == &request.size_param_name {
            return Err(PublicDataBronzePlanError::InvalidRequest(format!(
                "query parameter {name} must not duplicate page or size parameter names"
            )));
        }
        if request
            .format_query_param
            .as_ref()
            .is_some_and(|format_param| format_param.name == *name)
        {
            return Err(PublicDataBronzePlanError::InvalidRequest(format!(
                "query parameter {name} must not duplicate the format query parameter"
            )));
        }
    }
    Ok(())
}

fn validate_operation(operation: &str) -> Result<(), PublicDataBronzePlanError> {
    if !operation.is_empty() && operation.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Ok(());
    }
    Err(PublicDataBronzePlanError::InvalidRequest(
        "operation must contain only ASCII letters and digits".to_owned(),
    ))
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), PublicDataBronzePlanError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Ok(());
    }
    Err(PublicDataBronzePlanError::InvalidRequest(format!(
        "{field} must contain only ASCII letters, digits, '_' and '-'"
    )))
}

fn validate_object_key_value(
    field: &'static str,
    value: &str,
) -> Result<(), PublicDataBronzePlanError> {
    if value.is_empty() {
        return Err(PublicDataBronzePlanError::InvalidRequest(format!(
            "{field} must not be empty"
        )));
    }
    if value.trim() != value {
        return Err(PublicDataBronzePlanError::InvalidRequest(format!(
            "{field} must not contain leading or trailing whitespace"
        )));
    }
    if value.contains('/') || value.contains('\\') || value.contains("..") {
        return Err(PublicDataBronzePlanError::InvalidRequest(format!(
            "{field} must not contain path separators or traversal markers"
        )));
    }
    Ok(())
}

fn validate_query_value(name: &str, value: &str) -> Result<(), PublicDataBronzePlanError> {
    if value.trim() == value {
        return Ok(());
    }
    Err(PublicDataBronzePlanError::InvalidRequest(format!(
        "query parameter {name} must not contain leading or trailing whitespace"
    )))
}

fn validate_logical_items_pointer(pointer: &str) -> Result<(), PublicDataBronzePlanError> {
    if pointer.starts_with('/') {
        return Ok(());
    }
    Err(PublicDataBronzePlanError::InvalidRequest(
        "logical_items_pointer must be a JSON pointer".to_owned(),
    ))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut checksum, byte| {
            let _ = write!(&mut checksum, "{byte:02x}");
            checksum
        })
}

fn count_logical_items(payload: &JsonValue, pointer: &str) -> u64 {
    match payload.pointer(pointer) {
        Some(JsonValue::Array(items)) => items.len() as u64,
        Some(JsonValue::Object(_)) => 1,
        _ => 0,
    }
}

fn observe_schema(
    payload: &JsonValue,
    candidate_key_field_suffixes: &[String],
) -> Vec<PublicDataSchemaObservation> {
    let mut fields = BTreeMap::new();
    collect_schema("", payload, &mut fields);
    fields
        .into_iter()
        .map(|(field_path, accumulator)| {
            accumulator.into_observation(field_path, candidate_key_field_suffixes)
        })
        .collect()
}

fn collect_schema(path: &str, value: &JsonValue, fields: &mut BTreeMap<String, FieldAccumulator>) {
    match value {
        JsonValue::Object(object) => {
            record_field(path, value, fields);
            for (key, nested) in object {
                let next_path = if path.is_empty() {
                    key.to_owned()
                } else {
                    format!("{path}.{key}")
                };
                collect_schema(&next_path, nested, fields);
            }
        }
        JsonValue::Array(items) => {
            record_field(path, value, fields);
            let next_path = format!("{path}[]");
            for item in items {
                collect_schema(&next_path, item, fields);
            }
        }
        _ => record_field(path, value, fields),
    }
}

fn record_field(path: &str, value: &JsonValue, fields: &mut BTreeMap<String, FieldAccumulator>) {
    if path.is_empty() {
        return;
    }
    fields.entry(path.to_owned()).or_default().record(value);
}

#[derive(Clone, Debug, Default)]
struct FieldAccumulator {
    observed_type: Option<SchemaObservedType>,
    nonnull_count: u64,
    null_count: u64,
    sample_values: Vec<JsonValue>,
}

impl FieldAccumulator {
    fn record(&mut self, value: &JsonValue) {
        let observed_type = observed_type(value);
        if observed_type == SchemaObservedType::Null {
            self.null_count += 1;
            return;
        }
        self.nonnull_count += 1;
        self.observed_type = Some(match self.observed_type {
            None => observed_type,
            Some(existing) if existing == observed_type => existing,
            Some(_) => SchemaObservedType::Mixed,
        });
        if self.sample_values.len() < 3 {
            self.sample_values.push(value.clone());
        }
    }

    fn into_observation(
        self,
        field_path: String,
        candidate_key_field_suffixes: &[String],
    ) -> PublicDataSchemaObservation {
        let candidate_key_score = if self.null_count == 0
            && candidate_key_field_suffixes
                .iter()
                .any(|suffix| field_path.ends_with(suffix))
        {
            1.0
        } else {
            0.0
        };
        PublicDataSchemaObservation {
            field_path,
            observed_type: self.observed_type.unwrap_or(SchemaObservedType::Null),
            nonnull_count: self.nonnull_count,
            null_count: self.null_count,
            sample_values: JsonValue::Array(self.sample_values),
            candidate_key_score,
        }
    }
}

const fn observed_type(value: &JsonValue) -> SchemaObservedType {
    match value {
        JsonValue::Null => SchemaObservedType::Null,
        JsonValue::Bool(_) => SchemaObservedType::Boolean,
        JsonValue::Number(_) => SchemaObservedType::Number,
        JsonValue::String(_) => SchemaObservedType::String,
        JsonValue::Array(_) => SchemaObservedType::Array,
        JsonValue::Object(_) => SchemaObservedType::Object,
    }
}
