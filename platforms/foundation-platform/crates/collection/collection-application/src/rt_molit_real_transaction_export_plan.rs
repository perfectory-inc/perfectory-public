//! Planning helpers for `rt.molit.go.kr` real-transaction CSV export Bronze files.

use std::fmt::Write as _;

use chrono::{Datelike, NaiveDate};
use collection_domain::{
    build_bronze_object_key, BronzeObjectKeyError, BronzeObjectKeyParts, SnapshotBasis,
    SnapshotGranularity,
};
use foundation_shared_kernel::ObjectKey;
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Scope selected on the `rt.molit.go.kr` condition-based export form.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RtMolitExportScope {
    /// Nationwide export for the requested contract date range.
    Nationwide,
    /// City/province export for the requested contract date range.
    Sido {
        /// Provider city/province code, for example `11000` for Seoul.
        sido_code: String,
    },
    /// City/county/district export for the requested contract date range.
    Sigungu {
        /// Provider city/province code, for example `11000` for Seoul.
        sido_code: String,
        /// Provider city/county/district code, for example `11680` for Gangnam-gu.
        sigungu_code: String,
    },
    /// Legal district export for the requested contract date range.
    Emd {
        /// Provider city/province code.
        sido_code: String,
        /// Provider city/county/district code.
        sigungu_code: String,
        /// Provider legal district code.
        emd_code: String,
    },
}

impl RtMolitExportScope {
    const fn scope_name(&self) -> &'static str {
        match self {
            Self::Nationwide => "nationwide",
            Self::Sido { .. } => "sido",
            Self::Sigungu { .. } => "sigungu",
            Self::Emd { .. } => "emd",
        }
    }

    fn partition_path(&self) -> String {
        match self {
            Self::Nationwide => "scope=nationwide".to_owned(),
            Self::Sido { sido_code } => format!("sido={sido_code}"),
            Self::Sigungu {
                sido_code,
                sigungu_code,
            } => format!("sido={sido_code}/sigungu={sigungu_code}"),
            Self::Emd {
                sido_code,
                sigungu_code,
                emd_code,
            } => format!("sido={sido_code}/sigungu={sigungu_code}/emd={emd_code}"),
        }
    }

    fn request_params(&self, params: &mut JsonMap<String, JsonValue>) {
        params.insert(
            "scope".to_owned(),
            JsonValue::String(self.scope_name().to_owned()),
        );
        match self {
            Self::Nationwide => {}
            Self::Sido { sido_code } => {
                params.insert("srhSidoCd".to_owned(), JsonValue::String(sido_code.clone()));
            }
            Self::Sigungu {
                sido_code,
                sigungu_code,
            } => {
                params.insert("srhSidoCd".to_owned(), JsonValue::String(sido_code.clone()));
                params.insert(
                    "srhSggCd".to_owned(),
                    JsonValue::String(sigungu_code.clone()),
                );
            }
            Self::Emd {
                sido_code,
                sigungu_code,
                emd_code,
            } => {
                params.insert("srhSidoCd".to_owned(), JsonValue::String(sido_code.clone()));
                params.insert(
                    "srhSggCd".to_owned(),
                    JsonValue::String(sigungu_code.clone()),
                );
                params.insert("srhEmdCd".to_owned(), JsonValue::String(emd_code.clone()));
            }
        }
    }
}

/// Provider request represented by one `rt.molit.go.kr` CSV export.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RtMolitRealTransactionExportRequest {
    /// `rt.molit.go.kr` thing code, for example `A` for apartment.
    pub thing_code: String,
    /// Deal type code, for example `1` for trade.
    pub deal_type_code: String,
    /// Inclusive contract start date.
    pub contract_from: NaiveDate,
    /// Inclusive contract end date.
    pub contract_to: NaiveDate,
    /// Geographic export scope.
    pub scope: RtMolitExportScope,
    /// Provider response format. The Bronze lane currently accepts only `csv`.
    pub response_format: String,
}

/// Input required to plan one immutable `rt.molit.go.kr` CSV export Bronze object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RtMolitRealTransactionExportPlanInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Provider request parameters.
    pub request: RtMolitRealTransactionExportRequest,
    /// Raw provider CSV bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
}

/// Planned metadata and bytes for one immutable `rt.molit.go.kr` CSV export.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RtMolitRealTransactionExportPlan {
    /// Provider-neutral object key for the raw Bronze payload.
    pub object_key: ObjectKey,
    /// Canonical source coverage identity for skip / coverage / dedupe.
    pub source_identity_key: String,
    /// Provider partition represented by the export.
    pub source_partition_key: String,
    /// Idempotency key scoped to the source catalog entry.
    pub dedupe_key: String,
    /// Lowercase SHA-256 checksum of the raw payload.
    pub checksum_sha256: String,
    /// Raw payload size in bytes.
    pub size_bytes: u64,
    /// Request parameters stored with the Bronze object metadata.
    pub request_params: JsonValue,
    /// Human-readable source period bucket, when the request is a full month.
    pub snapshot_period: Option<String>,
    /// Canonical source as-of date.
    pub snapshot_date: NaiveDate,
    /// Granularity of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_granularity: SnapshotGranularity,
    /// Provenance of [`snapshot_date`](Self::snapshot_date).
    pub snapshot_basis: SnapshotBasis,
    /// Raw provider CSV bytes to write to object storage.
    pub raw_payload: Vec<u8>,
}

/// Error returned while planning an `rt.molit.go.kr` CSV export.
#[derive(Debug, Error)]
pub enum RtMolitRealTransactionExportPlanError {
    /// The canonical Bronze object key could not be built.
    #[error(transparent)]
    ObjectKey(#[from] BronzeObjectKeyError),
    /// A request parameter was invalid.
    #[error("invalid rt.molit.go.kr real-transaction export request: {0}")]
    InvalidRequest(String),
}

/// Plans object metadata for one immutable `rt.molit.go.kr` CSV export payload.
///
/// # Errors
///
/// Returns [`RtMolitRealTransactionExportPlanError`] when request parameters cannot be represented
/// in the canonical Bronze object layout.
pub fn plan_rt_molit_real_transaction_export(
    input: RtMolitRealTransactionExportPlanInput<'_>,
) -> Result<RtMolitRealTransactionExportPlan, RtMolitRealTransactionExportPlanError> {
    validate_request(&input.request)?;

    let (partition_path, snapshot_period, snapshot_date, snapshot_granularity) =
        object_partition_and_snapshot(&input.request);
    let object_key = build_bronze_object_key(BronzeObjectKeyParts {
        source_slug: input.source_slug,
        partition_path: &partition_path,
        leaf_name: "export",
        extension: "csv",
    })?;
    let checksum_sha256 = sha256_hex(&input.raw_payload);
    let source_identity_key = source_identity_key(&input.request);
    let source_partition_key = source_partition_key(&input.request);
    let dedupe_key = format!(
        "{}:{}:sha256={}",
        input.source_slug, source_identity_key, checksum_sha256
    );

    Ok(RtMolitRealTransactionExportPlan {
        object_key,
        source_identity_key,
        source_partition_key,
        dedupe_key,
        checksum_sha256,
        size_bytes: input.raw_payload.len() as u64,
        request_params: request_params_json(&input.request),
        snapshot_period,
        snapshot_date,
        snapshot_granularity,
        snapshot_basis: SnapshotBasis::RequestMonth,
        raw_payload: input.raw_payload,
    })
}

fn object_partition_and_snapshot(
    request: &RtMolitRealTransactionExportRequest,
) -> (String, Option<String>, NaiveDate, SnapshotGranularity) {
    if request_is_full_calendar_month(request) {
        let period = format!(
            "{:04}-{:02}",
            request.contract_from.year(),
            request.contract_from.month()
        );
        return (
            format!("period={period}/{}", request.scope.partition_path()),
            Some(period),
            request.contract_from,
            SnapshotGranularity::Month,
        );
    }

    (
        format!(
            "contract_from={}/contract_to={}/{}",
            request.contract_from,
            request.contract_to,
            request.scope.partition_path()
        ),
        None,
        request.contract_from,
        SnapshotGranularity::Day,
    )
}

fn source_identity_key(request: &RtMolitRealTransactionExportRequest) -> String {
    format!(
        "contract_from={}/contract_to={}/{}/format={}",
        request.contract_from,
        request.contract_to,
        request.scope.partition_path(),
        request.response_format
    )
}

fn source_partition_key(request: &RtMolitRealTransactionExportRequest) -> String {
    format!(
        "thing={}/deal_type={}/contract_from={}/contract_to={}/{}",
        request.thing_code,
        request.deal_type_code,
        request.contract_from,
        request.contract_to,
        request.scope.partition_path()
    )
}

fn request_params_json(request: &RtMolitRealTransactionExportRequest) -> JsonValue {
    let mut params = JsonMap::new();
    params.insert(
        "srhThingNo".to_owned(),
        JsonValue::String(request.thing_code.clone()),
    );
    params.insert(
        "srhDelngSecd".to_owned(),
        JsonValue::String(request.deal_type_code.clone()),
    );
    params.insert("srhAddrGbn".to_owned(), JsonValue::String("1".to_owned()));
    params.insert("srhLfstsSecd".to_owned(), JsonValue::String("1".to_owned()));
    params.insert(
        "srhFromDt".to_owned(),
        JsonValue::String(request.contract_from.to_string()),
    );
    params.insert(
        "srhToDt".to_owned(),
        JsonValue::String(request.contract_to.to_string()),
    );
    params.insert(
        "format".to_owned(),
        JsonValue::String(request.response_format.clone()),
    );
    request.scope.request_params(&mut params);
    JsonValue::Object(params)
}

fn validate_request(
    request: &RtMolitRealTransactionExportRequest,
) -> Result<(), RtMolitRealTransactionExportPlanError> {
    validate_ascii_code("thing_code", &request.thing_code)?;
    validate_ascii_code("deal_type_code", &request.deal_type_code)?;
    if request.response_format != "csv" {
        return Err(RtMolitRealTransactionExportPlanError::InvalidRequest(
            "response_format must be csv".to_owned(),
        ));
    }
    if request.contract_from > request.contract_to {
        return Err(RtMolitRealTransactionExportPlanError::InvalidRequest(
            "contract_from must be on or before contract_to".to_owned(),
        ));
    }
    validate_scope(&request.scope)?;
    Ok(())
}

fn validate_scope(scope: &RtMolitExportScope) -> Result<(), RtMolitRealTransactionExportPlanError> {
    match scope {
        RtMolitExportScope::Nationwide => Ok(()),
        RtMolitExportScope::Sido { sido_code } => validate_fixed_digits("sido_code", sido_code, 5),
        RtMolitExportScope::Sigungu {
            sido_code,
            sigungu_code,
        } => {
            validate_fixed_digits("sido_code", sido_code, 5)?;
            validate_fixed_digits("sigungu_code", sigungu_code, 5)?;
            validate_parent_prefix(sido_code, sigungu_code, "sigungu_code")
        }
        RtMolitExportScope::Emd {
            sido_code,
            sigungu_code,
            emd_code,
        } => {
            validate_fixed_digits("sido_code", sido_code, 5)?;
            validate_fixed_digits("sigungu_code", sigungu_code, 5)?;
            validate_fixed_digits("emd_code", emd_code, 10)?;
            validate_parent_prefix(sido_code, sigungu_code, "sigungu_code")?;
            validate_parent_prefix(sigungu_code, emd_code, "emd_code")
        }
    }
}

fn validate_fixed_digits(
    field: &'static str,
    value: &str,
    len: usize,
) -> Result<(), RtMolitRealTransactionExportPlanError> {
    if value.len() == len && value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(());
    }
    Err(RtMolitRealTransactionExportPlanError::InvalidRequest(
        format!("{field} must be exactly {len} digits"),
    ))
}

fn validate_parent_prefix(
    parent_code: &str,
    child_code: &str,
    child_field: &'static str,
) -> Result<(), RtMolitRealTransactionExportPlanError> {
    if child_code.starts_with(&parent_code[..2]) {
        return Ok(());
    }
    Err(RtMolitRealTransactionExportPlanError::InvalidRequest(
        format!("{child_field} must share the sido prefix"),
    ))
}

fn validate_ascii_code(
    field: &'static str,
    value: &str,
) -> Result<(), RtMolitRealTransactionExportPlanError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Ok(());
    }
    Err(RtMolitRealTransactionExportPlanError::InvalidRequest(
        format!("{field} must contain only ASCII letters, digits, and '_'"),
    ))
}

fn request_is_full_calendar_month(request: &RtMolitRealTransactionExportRequest) -> bool {
    request.contract_from.day() == 1
        && request.contract_from.year() == request.contract_to.year()
        && request.contract_from.month() == request.contract_to.month()
        && request.contract_to.day()
            == days_in_month(request.contract_to.year(), request.contract_to.month())
}

const fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

const fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}
