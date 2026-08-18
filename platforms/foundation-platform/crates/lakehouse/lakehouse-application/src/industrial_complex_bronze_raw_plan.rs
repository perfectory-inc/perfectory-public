//! Bronze-to-JSONL normalization for the industrial-complex profile source.
//!
//! The Spark job `industrial_complex_bronze_to_silver` reads
//! `bronze.industrial_complexes_raw_jsonl`. The profile source carries the complex identity,
//! classification, dates, and area, but carries no administrative location at all. Rather than let
//! a producer paper over that with an empty string, an [`IndustrialComplexBronzeRawRow`] owns an
//! [`IndustrialComplexAddress`] by value: there is no row shape that can exist without a sourced
//! address text and its provenance. See root ADR-0033.
//!
//! What the address may leave out is the administrative code. Region is not a requirement of this
//! pipeline today, and for one complex in 1,442 no source states a current district code at all;
//! the row writes `null` for `sido_code`, `sigungu_code`, and `primary_bjdong_code` rather than a
//! blank or a zero, and only [`IndustrialComplexAddress::try_new_without_administrative_code`]
//! produces that shape. See root ADR-0035.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use lakehouse_domain::{
    bronze_industrial_complexes_raw_jsonl_columns, INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES,
    INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES, INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES,
};
use serde_json::{Map as JsonMap, Value as JsonValue};
use thiserror::Error;

/// Number of unresolved complex codes named in a [`IndustrialComplexBronzeRawPlanError`] message.
const MISSING_ADDRESS_SAMPLE_LIMIT: usize = 5;

/// Length of the city/county/district prefix inside a ten-digit administrative code.
const SIGUNGU_CODE_LEN: usize = 5;

/// Snapshot month whose full table backs every `*_OBSERVED` label list below.
///
/// The tables are evidence for this month and no other. A run over a different snapshot matches
/// labels against a table nobody has counted for it, so the answer belongs in the export summary
/// rather than in a comment — see [`industrial_complex_labels_measured_for`].
pub const INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD: &str = "202506";

/// Whether the `*_OBSERVED` label tables were measured against `snapshot_period`.
///
/// `false` does not stop an export: a later month is still decodable, and an unmapped label fails
/// loudly on its own. It means the run leans on label mappings that no snapshot has confirmed for
/// the month in hand, which the producer records as an evidence limitation.
#[must_use]
pub fn industrial_complex_labels_measured_for(snapshot_period: &str) -> bool {
    snapshot_period.trim() == INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD
}

/// `lrstt_ty` labels counted in the whole [`INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD`] table
/// (1,442 rows).
///
/// These four are the entire observed domain; the counts are the measurement, not an estimate.
const COMPLEX_KIND_LABELS_OBSERVED: &[(&str, &str)] = &[
    ("국가", "national"),            // 94 rows
    ("일반", "general"),             // 812 rows
    ("도시첨단", "urban_high_tech"), // 51 rows
    ("농공", "agricultural"),        // 485 rows
];

/// `lrstt_ty` spellings that no measured snapshot has produced yet.
///
/// Kept because another month may spell the classification out in full. Nothing has confirmed
/// them, which is why they are not in [`COMPLEX_KIND_LABELS_OBSERVED`].
const COMPLEX_KIND_LABELS_PRESUMED: &[(&str, &str)] = &[
    ("국가산업단지", "national"),
    ("일반산업단지", "general"),
    ("도시첨단산업단지", "urban_high_tech"),
    ("농공단지", "agricultural"),
    ("농공산업단지", "agricultural"),
];

/// `make_sttus_nm` labels counted in the whole [`INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD`] table
/// (1,442 rows).
///
/// `준비중` and `보상중` map to `planned` on measured evidence rather than on the wording of the
/// label. Per-status counts of `strwrk_de` (착공일자), `compet_cnfm_de` (준공인가일자), and the mean
/// `make_procs_rt` (조성공정율) in that table:
///
/// | `make_sttus_nm` | rows | `strwrk_de` | `compet_cnfm_de` | mean `make_procs_rt` |
/// |---|---:|---:|---:|---:|
/// | `조성완료` | 1069 | 1069 | 1069 | 100.0 |
/// | `조성중` | 289 | 289 | 1 | 59.9 |
/// | `준비중` | 47 | 43 | 0 | 0.0 |
/// | `보상중` | 37 | 37 | 0 | 0.0 |
///
/// Nothing has been built under either label: site-formation progress is 0% and no complex has a
/// completion approval. `developing` is reserved for `조성중`, whose progress is 59.9%; calling a
/// 0%-progress complex `developing` would assert site works that the source says have not started.
///
/// Known tension: most `준비중`/`보상중` rows do carry a `strwrk_de`. With `make_procs_rt` at 0 that
/// column reads as a planned or designated date rather than an actual start, so it does not move
/// the mapping. A later snapshot that shows progress under these labels should reopen this.
const COMPLEX_STATUS_LABELS_OBSERVED: &[(&str, &str)] = &[
    ("조성완료", "operating"), // 1069 rows
    ("조성중", "developing"),  // 289 rows
    ("준비중", "planned"),     // 47 rows
    ("보상중", "planned"),     // 37 rows
];

/// `lttot_sttus_nm` labels counted in the whole
/// [`INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD`] table (1,442 rows).
///
/// Three labels, every row filled. This is the only one of the eight columns this producer took
/// from the profile source that is an enumeration; the counts are the measurement, not an estimate.
///
/// There is no presumed table beside it. A presumed table exists for `lrstt_ty` and
/// `make_sttus_nm` because other months plausibly spell the same classification out in full; here
/// there is no longer spelling to guess at, and inventing one would put an unmeasured mapping in
/// the path of a real load.
const COMPLEX_LOT_SALES_STATUS_LABELS_OBSERVED: &[(&str, &str)] = &[
    ("분양완료", "completed"), // 992 rows
    ("분양중", "in_progress"), // 294 rows
    ("분양계획", "planned"),   // 156 rows
];

/// `make_sttus_nm` labels that no measured snapshot has produced yet.
///
/// Kept because another month may use them. Nothing has confirmed them, which is why they are not
/// in [`COMPLEX_STATUS_LABELS_OBSERVED`].
const COMPLEX_STATUS_LABELS_PRESUMED: &[(&str, &str)] = &[
    ("계획", "planned"),
    ("계획중", "planned"),
    ("미개발", "planned"),
    ("개발중", "developing"),
    ("개발완료", "operating"),
    ("변경", "changed"),
    ("해제", "abolished"),
    ("폐지", "abolished"),
];

/// How precise the administrative code an address source supplied actually is.
///
/// Korean legal-division codes are ten digits whatever they name: a city/county/district code is
/// the five-digit district followed by five zeros, and an eup/myeon/dong code fills all ten. Both
/// pass `^[0-9]{10}$`, so a shape check alone cannot tell them apart, and a sigungu code written
/// into a field named `primary_bjdong_code` is a lie the format cannot catch. Carrying the
/// granularity makes the difference part of the value instead of part of the reader's assumption.
///
/// This is not a data defect to be fixed later. An industrial complex is a polygon over many
/// eup/myeon/dong and sometimes over several provinces — `서울특별시 구로구 구로동, 금천구 가산동
/// 일원, 인천광역시 부평구 청천동 …` is one complex — so for most complexes there is no single
/// legal-dong to name. See root ADR-0034.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndustrialComplexAddressGranularity {
    /// The code names a city/county/district (`sigungu`); its last five digits are zeros.
    Sigungu,
    /// The code names an eup/myeon/dong (`bjdong`); its last five digits are not all zeros.
    LegalDong,
}

impl IndustrialComplexAddressGranularity {
    /// Returns the stable wire value used in the injected address resolution.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Sigungu => "sigungu",
            Self::LegalDong => "legal_dong",
        }
    }

    /// Parses a wire value from the injected address resolution.
    ///
    /// # Errors
    /// Returns [`IndustrialComplexBronzeRawPlanError::InvalidAddress`] for any other value. There
    /// is deliberately no default: a resolution that does not say how precise its code is has not
    /// said what the code means.
    pub fn from_wire(value: &str) -> Result<Self, IndustrialComplexBronzeRawPlanError> {
        match value.trim() {
            "sigungu" => Ok(Self::Sigungu),
            "legal_dong" => Ok(Self::LegalDong),
            other => Err(IndustrialComplexBronzeRawPlanError::InvalidAddress(
                format!(
                "administrative_code_granularity must be \"sigungu\" or \"legal_dong\": {other:?}"
            ),
            )),
        }
    }

    /// Returns whether a ten-digit administrative code has this granularity's shape.
    #[must_use]
    fn matches_code(self, administrative_code: &str) -> bool {
        let names_a_dong = !administrative_code.ends_with("00000");
        match self {
            Self::Sigungu => !names_a_dong,
            Self::LegalDong => names_a_dong,
        }
    }
}

/// A ten-digit administrative code together with what it actually names.
///
/// Private fields and one validating constructor, so holding a value of this type is evidence that
/// the code was checked: ten ASCII digits, a district prefix that is not all zeros, and a shape
/// that agrees with the declared granularity (root ADR-0034).
#[derive(Clone, Debug, Eq, PartialEq)]
struct SourcedAdministrativeCode {
    code: String,
    granularity: IndustrialComplexAddressGranularity,
}

impl SourcedAdministrativeCode {
    fn try_new(
        code: &str,
        granularity: IndustrialComplexAddressGranularity,
    ) -> Result<Self, IndustrialComplexBronzeRawPlanError> {
        let code = code.trim();
        if code.len() != 10 || !code.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(IndustrialComplexBronzeRawPlanError::InvalidAddress(
                format!("administrative_code must be exactly 10 ASCII digits: {code:?}"),
            ));
        }
        // `0000000000` is ten ASCII digits and ends in five zeros, so both checks below would pass
        // it and the row would carry `sido_code` `00` — a filler that reads as a real province.
        // No Korean district code starts with `00`, so an all-zero district prefix is not a code
        // that was found anywhere; it is one that was made up in place of `null`.
        if code[0..SIGUNGU_CODE_LEN].bytes().all(|byte| byte == b'0') {
            return Err(IndustrialComplexBronzeRawPlanError::InvalidAddress(
                format!(
                    "administrative_code {code:?} has an all-zero district prefix, which names no \
                     district; an unknown location is null, not a zero code"
                ),
            ));
        }
        // The shape check is what makes the granularity evidence rather than a label. A sigungu
        // code declared `legal_dong` would otherwise put five zeros into the dong position and
        // read downstream as "dong 00000 of this district".
        if !granularity.matches_code(code) {
            return Err(IndustrialComplexBronzeRawPlanError::InvalidAddress(
                format!(
                    "administrative_code {code:?} does not have {} granularity: a sigungu code \
                     ends in five zeros and a legal-dong code does not",
                    granularity.wire_name()
                ),
            ));
        }
        Ok(Self {
            code: code.to_owned(),
            granularity,
        })
    }
}

/// Administrative location of one industrial complex, proven by an external address source.
///
/// The fields are private and the only constructors validate every one of them, so a value of this
/// type is evidence that a location was sourced. `sido_code` and `sigungu_code` are derived from
/// the administrative code rather than accepted separately, which is the same derivation the
/// Catalog handoff uses and leaves no way to inject three mutually inconsistent codes.
///
/// The administrative code itself is optional and the address text is not. A complex whose official
/// address text every source publishes but whose district code no source states is representable —
/// [`Self::try_new_without_administrative_code`] is the only way to build one, and the resulting
/// row writes `null` for all three code columns. There is no constructor that accepts a blank or
/// zero code, so "unknown" cannot be spelled as a value (root ADR-0035).
///
/// [`Self::primary_bjdong_code`] returns `Option` because a sigungu-granularity source has not
/// named a legal dong. That is the whole point: the only way to put a value in the
/// `primary_bjdong_code` column is to hold a code that actually names one (root ADR-0034).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexAddress {
    administrative_code: Option<SourcedAdministrativeCode>,
    address_text: String,
    source_dataset: String,
    source_record_id: String,
}

impl IndustrialComplexAddress {
    /// Builds a validated address whose source named an administrative code.
    ///
    /// # Errors
    /// Returns [`IndustrialComplexBronzeRawPlanError::InvalidAddress`] when the administrative code
    /// is not exactly ten ASCII digits, when its district prefix is all zeros, when the code's
    /// shape contradicts the declared granularity, or when the address text or either provenance
    /// field is blank.
    pub fn try_new(
        administrative_code: &str,
        granularity: IndustrialComplexAddressGranularity,
        address_text: &str,
        source_dataset: &str,
        source_record_id: &str,
    ) -> Result<Self, IndustrialComplexBronzeRawPlanError> {
        let administrative_code =
            SourcedAdministrativeCode::try_new(administrative_code, granularity)?;
        Self::build(
            Some(administrative_code),
            address_text,
            source_dataset,
            source_record_id,
        )
    }

    /// Builds a validated address whose source published the words but no administrative code.
    ///
    /// The absence has to be stated by calling this constructor rather than by passing something
    /// empty to [`Self::try_new`]: an argument that could be blank is an argument a caller can
    /// leave blank by accident, and the whole point is that "we do not know" is a different fact
    /// from any code.
    ///
    /// # Errors
    /// Returns [`IndustrialComplexBronzeRawPlanError::InvalidAddress`] when the address text or
    /// either provenance field is blank. The address text stays required: this loosening is about
    /// the region codes, not about locations nobody published at all.
    pub fn try_new_without_administrative_code(
        address_text: &str,
        source_dataset: &str,
        source_record_id: &str,
    ) -> Result<Self, IndustrialComplexBronzeRawPlanError> {
        Self::build(None, address_text, source_dataset, source_record_id)
    }

    fn build(
        administrative_code: Option<SourcedAdministrativeCode>,
        address_text: &str,
        source_dataset: &str,
        source_record_id: &str,
    ) -> Result<Self, IndustrialComplexBronzeRawPlanError> {
        Ok(Self {
            administrative_code,
            address_text: require_address_part("address_text", address_text)?,
            source_dataset: require_address_part("address_source_dataset", source_dataset)?,
            source_record_id: require_address_part("address_source_record_id", source_record_id)?,
        })
    }

    /// Returns the two-digit province/city code derived from the administrative code.
    #[must_use]
    pub fn sido_code(&self) -> Option<&str> {
        self.administrative_code
            .as_ref()
            .map(|sourced| &sourced.code[0..2])
    }

    /// Returns the five-digit city/county/district code derived from the administrative code.
    #[must_use]
    pub fn sigungu_code(&self) -> Option<&str> {
        self.administrative_code
            .as_ref()
            .map(|sourced| &sourced.code[0..SIGUNGU_CODE_LEN])
    }

    /// Returns the ten-digit legal-dong code, or `None` when the source named only a district or
    /// no code at all.
    #[must_use]
    pub fn primary_bjdong_code(&self) -> Option<&str> {
        let sourced = self.administrative_code.as_ref()?;
        match sourced.granularity {
            IndustrialComplexAddressGranularity::LegalDong => Some(sourced.code.as_str()),
            IndustrialComplexAddressGranularity::Sigungu => None,
        }
    }

    /// Returns the ten-digit administrative code exactly as the source supplied it, when there was
    /// one.
    #[must_use]
    pub fn administrative_code(&self) -> Option<&str> {
        self.administrative_code
            .as_ref()
            .map(|sourced| sourced.code.as_str())
    }

    /// Returns how precise [`Self::administrative_code`] is, when there is one.
    #[must_use]
    pub fn granularity(&self) -> Option<IndustrialComplexAddressGranularity> {
        self.administrative_code
            .as_ref()
            .map(|sourced| sourced.granularity)
    }

    /// Returns the official address text.
    #[must_use]
    pub const fn address_text(&self) -> &str {
        self.address_text.as_str()
    }

    /// Returns the dataset the address was read from.
    #[must_use]
    pub const fn source_dataset(&self) -> &str {
        self.source_dataset.as_str()
    }

    /// Returns the record id the address was read from inside [`Self::source_dataset`].
    #[must_use]
    pub const fn source_record_id(&self) -> &str {
        self.source_record_id.as_str()
    }
}

/// Injected `official_complex_code` to [`IndustrialComplexAddress`] resolution.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndustrialComplexAddressBook {
    entries: BTreeMap<String, IndustrialComplexAddress>,
}

impl IndustrialComplexAddressBook {
    /// Builds an empty address book.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one resolution.
    ///
    /// # Errors
    /// Returns [`IndustrialComplexBronzeRawPlanError::DuplicateAddress`] when the same
    /// `official_complex_code` is resolved twice, because the second entry would silently win.
    pub fn insert(
        &mut self,
        official_complex_code: &str,
        address: IndustrialComplexAddress,
    ) -> Result<(), IndustrialComplexBronzeRawPlanError> {
        let key = require_address_part("official_complex_code", official_complex_code)?;
        if self.entries.contains_key(&key) {
            return Err(IndustrialComplexBronzeRawPlanError::DuplicateAddress(key));
        }
        self.entries.insert(key, address);
        Ok(())
    }

    /// Returns the resolution for one `official_complex_code`.
    #[must_use]
    pub fn get(&self, official_complex_code: &str) -> Option<&IndustrialComplexAddress> {
        self.entries.get(official_complex_code)
    }

    /// Returns the number of resolutions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the address book holds no resolutions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// One decoded row of the industrial-complex profile Bronze table.
///
/// Every optional field is `None` when the source cell was blank; blank is never carried forward as
/// an empty string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexBronzeSourceRecord {
    /// Source-side official industrial-complex code (`krihs_irstt_code`).
    pub official_complex_code: String,
    /// Official industrial-complex name (`krihs_irstt_nm`).
    pub complex_name: String,
    /// Source classification label (`lrstt_ty`).
    pub complex_kind_label: String,
    /// Source status label (`make_sttus_nm`).
    pub status_label: String,
    /// Managing agency name (`manage_instt_nm`).
    pub management_agency_name: Option<String>,
    /// Developing operator name (`bsms_opertn_entrps_nm`).
    pub developer_name: Option<String>,
    /// Designation date (`appn_de`), `yyyymmdd` or `yyyy-mm-dd`.
    pub designated_date_raw: Option<String>,
    /// Site-works start date (`strwrk_de`), `yyyymmdd` or `yyyy-mm-dd`.
    pub construction_start_date_raw: Option<String>,
    /// Completion confirmation date (`compet_cnfm_de`), `yyyymmdd` or `yyyy-mm-dd`.
    pub completion_date_raw: Option<String>,
    /// Official designated area in square meters (`appn_ar`).
    pub official_area_sqm_raw: Option<String>,
    /// Site-formation progress percentage (`make_procs_rt`).
    pub development_progress_percent_raw: Option<String>,
    /// Source lot-sales status label (`lttot_sttus_nm`).
    pub lot_sales_status_label: Option<String>,
    /// Business period (`bsms_pd`), normally `YYYY-MM~YYYY-MM`.
    pub business_period_raw: Option<String>,
    /// Statute the designation was made under (`appn_basis_law`).
    pub designation_basis_law_raw: Option<String>,
    /// Development method (`devlop_mth`).
    pub development_method_raw: Option<String>,
    /// Stated development purpose (`make_purps_cn`).
    pub development_purpose_raw: Option<String>,
    /// Industry types the complex set out to attract (`invite_upj`).
    pub invited_industries_raw: Option<String>,
    /// Snapshot month of the source table (`sta_ym`), `yyyymm`.
    pub snapshot_period: String,
    /// One-based row number inside the decoded sheet, used only in error messages.
    pub source_row_number: u64,
}

/// Input required to normalize decoded profile rows into Bronze JSONL rows.
pub struct IndustrialComplexBronzeRawRowsInput<'a> {
    /// Decoded source rows in sheet order.
    pub records: &'a [IndustrialComplexBronzeSourceRecord],
    /// Injected administrative locations keyed by `official_complex_code`.
    pub addresses: &'a IndustrialComplexAddressBook,
    /// Bronze object key the rows were decoded from, used as row lineage.
    pub bronze_object_key: &'a str,
    /// Bronze source slug, used as the `source_snapshot_id` prefix.
    pub source_slug: &'a str,
    /// UTC timestamp when the rows entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// One `bronze.industrial_complexes_raw_jsonl` record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexBronzeRawRow {
    /// Source-side official industrial-complex code.
    pub official_complex_code: String,
    /// Official industrial-complex name.
    pub complex_name: String,
    /// `complex_kind` wire value.
    pub complex_kind: String,
    /// `status` wire value.
    pub status: String,
    /// Sourced administrative location; a row without one cannot be built.
    pub address: IndustrialComplexAddress,
    /// Managing agency name.
    pub management_agency_name: Option<String>,
    /// Developing operator name.
    pub developer_name: Option<String>,
    /// Designation date as `yyyy-mm-dd`.
    pub designated_date: Option<String>,
    /// Site-works start date as `yyyy-mm-dd`.
    pub construction_start_date: Option<String>,
    /// Completion confirmation date as `yyyy-mm-dd`.
    pub completion_date: Option<String>,
    /// Official designated area in square meters with two decimal places.
    pub official_area_sqm: Option<String>,
    /// Site-formation progress percentage with two decimal places, `0.00` to `100.00`.
    pub development_progress_percent: Option<String>,
    /// `lot_sales_status` wire value.
    pub lot_sales_status: Option<String>,
    /// Business period exactly as the source wrote it.
    pub business_period_raw: Option<String>,
    /// First month of the business period as `yyyy-MM`, when the raw text parses.
    pub business_period_start_month: Option<String>,
    /// Last month of the business period as `yyyy-MM`, when the raw text parses.
    pub business_period_end_month: Option<String>,
    /// Statute the designation was made under, verbatim.
    pub designation_basis_law_raw: Option<String>,
    /// Development method, verbatim.
    pub development_method_raw: Option<String>,
    /// Stated development purpose, verbatim.
    pub development_purpose_raw: Option<String>,
    /// Industry types the complex set out to attract, verbatim.
    pub invited_industries_raw: Option<String>,
    /// Stable lineage id pointing at the Bronze object and the source row key.
    pub source_record_id: String,
    /// Source-snapshot lineage id derived from the source slug and snapshot month.
    pub source_snapshot_id: String,
    /// UTC timestamp from which this fact is valid, derived from the snapshot month.
    pub valid_from_utc: DateTime<Utc>,
    /// UTC timestamp when this fact entered the lakehouse flow.
    pub ingested_at_utc: DateTime<Utc>,
}

/// Error returned while normalizing profile rows into Bronze JSONL rows.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IndustrialComplexBronzeRawPlanError {
    /// A source field cannot be represented as a Bronze JSONL value.
    #[error("invalid industrial-complex Bronze row: {0}")]
    InvalidInput(String),
    /// An injected address failed validation.
    #[error("invalid industrial-complex address resolution: {0}")]
    InvalidAddress(String),
    /// The same `official_complex_code` was resolved twice by the address source.
    #[error("duplicate industrial-complex address resolution for official_complex_code {0}")]
    DuplicateAddress(String),
    /// The same `official_complex_code` appears twice in the Bronze source.
    #[error("duplicate industrial-complex source row for official_complex_code {0}")]
    DuplicateSourceRow(String),
    /// The source rows carry more than one snapshot month.
    #[error("industrial-complex Bronze source mixes snapshot periods: {0}")]
    MixedSnapshotPeriods(String),
    /// At least one source row has no injected address.
    #[error(
        "{missing_count} of {row_count} industrial-complex rows have no sourced address \
         (official_complex_code {sample}); an industrial complex without a sourced location is \
         not representable, so nothing was written"
    )]
    MissingAddresses {
        /// Number of source rows with no resolution.
        missing_count: usize,
        /// Number of source rows examined.
        row_count: usize,
        /// First few unresolved `official_complex_code` values.
        sample: String,
    },
    /// A JSONL column has no mapping to a row field.
    #[error("bronze.industrial_complexes_raw_jsonl column {0} has no producer mapping")]
    UnmappedColumn(String),
    /// JSON serialization failed.
    #[error("failed to serialize industrial-complex Bronze row: {0}")]
    Serialization(String),
}

/// Normalizes decoded profile rows into `bronze.industrial_complexes_raw_jsonl` rows.
///
/// # Errors
/// Returns [`IndustrialComplexBronzeRawPlanError`] when the source is empty, mixes snapshot
/// months, repeats an `official_complex_code`, carries a label or date the contract does not
/// admit, or when any row has no injected address.
pub fn normalize_industrial_complex_bronze_raw_rows(
    input: &IndustrialComplexBronzeRawRowsInput<'_>,
) -> Result<Vec<IndustrialComplexBronzeRawRow>, IndustrialComplexBronzeRawPlanError> {
    if input.records.is_empty() {
        return Err(IndustrialComplexBronzeRawPlanError::InvalidInput(
            "Bronze source decoded zero rows".to_owned(),
        ));
    }
    let bronze_object_key = require_text("bronze_object_key", input.bronze_object_key)?;
    let source_slug = require_text("source_slug", input.source_slug)?;
    let snapshot_period = single_snapshot_period(input.records)?;
    let source_snapshot_id = format!("{source_slug}-{snapshot_period}");
    let valid_from_utc = snapshot_period_start_utc(snapshot_period.as_str())?;

    assert_addresses_are_sourced(input)?;

    let mut seen_codes = BTreeSet::<String>::new();
    let mut rows = Vec::with_capacity(input.records.len());
    for record in input.records {
        let official_complex_code = require_row_text(
            record,
            "official_complex_code",
            record.official_complex_code.as_str(),
        )?;
        if !seen_codes.insert(official_complex_code.clone()) {
            return Err(IndustrialComplexBronzeRawPlanError::DuplicateSourceRow(
                official_complex_code,
            ));
        }
        let address = input
            .addresses
            .get(official_complex_code.as_str())
            .ok_or_else(|| IndustrialComplexBronzeRawPlanError::MissingAddresses {
                missing_count: 1,
                row_count: input.records.len(),
                sample: official_complex_code.clone(),
            })?
            .clone();

        rows.push(normalize_one_row(
            record,
            official_complex_code,
            address,
            &RowLineage {
                bronze_object_key: bronze_object_key.as_str(),
                source_snapshot_id: source_snapshot_id.as_str(),
                valid_from_utc,
                ingested_at_utc: input.ingested_at_utc,
            },
        )?);
    }
    Ok(rows)
}

/// Lineage every row of one export shares.
struct RowLineage<'a> {
    bronze_object_key: &'a str,
    source_snapshot_id: &'a str,
    valid_from_utc: DateTime<Utc>,
    ingested_at_utc: DateTime<Utc>,
}

/// Turns one decoded source record into one Bronze JSONL row.
fn normalize_one_row(
    record: &IndustrialComplexBronzeSourceRecord,
    official_complex_code: String,
    address: IndustrialComplexAddress,
    lineage: &RowLineage<'_>,
) -> Result<IndustrialComplexBronzeRawRow, IndustrialComplexBronzeRawPlanError> {
    // Parsed once and destructured, so the two derived columns cannot come from two different
    // readings of the same text: they are the halves of one parse or they are both absent.
    let business_period = business_period_months(record.business_period_raw.as_deref());
    let lot_sales_status = optional_text(record.lot_sales_status_label.as_deref())
        .map(|label| {
            map_label(
                record,
                "lot_sales_status",
                label.as_str(),
                &[COMPLEX_LOT_SALES_STATUS_LABELS_OBSERVED],
                INDUSTRIAL_COMPLEX_LOT_SALES_STATUS_WIRE_VALUES,
            )
        })
        .transpose()?;
    let bronze_object_key = lineage.bronze_object_key;

    Ok(IndustrialComplexBronzeRawRow {
        source_record_id: format!(
            "foundation-platform:bronze:{bronze_object_key}#{official_complex_code}"
        ),
        complex_name: require_row_text(record, "complex_name", record.complex_name.as_str())?,
        complex_kind: map_label(
            record,
            "complex_kind",
            record.complex_kind_label.as_str(),
            &[COMPLEX_KIND_LABELS_OBSERVED, COMPLEX_KIND_LABELS_PRESUMED],
            INDUSTRIAL_COMPLEX_KIND_WIRE_VALUES,
        )?,
        status: map_label(
            record,
            "status",
            record.status_label.as_str(),
            &[
                COMPLEX_STATUS_LABELS_OBSERVED,
                COMPLEX_STATUS_LABELS_PRESUMED,
            ],
            INDUSTRIAL_COMPLEX_STATUS_WIRE_VALUES,
        )?,
        official_complex_code,
        address,
        management_agency_name: optional_text(record.management_agency_name.as_deref()),
        developer_name: optional_text(record.developer_name.as_deref()),
        designated_date: normalize_optional_date(
            record,
            "designated_date",
            record.designated_date_raw.as_deref(),
        )?,
        // The same parser the designation date goes through, deliberately. `strwrk_de` has the
        // identical `yyyymmdd` shape, and a second date parser is a second place for the two to
        // disagree about what a date is.
        construction_start_date: normalize_optional_date(
            record,
            "construction_start_date",
            record.construction_start_date_raw.as_deref(),
        )?,
        completion_date: normalize_optional_date(
            record,
            "completion_date",
            record.completion_date_raw.as_deref(),
        )?,
        official_area_sqm: normalize_optional_area(
            record,
            record.official_area_sqm_raw.as_deref(),
        )?,
        development_progress_percent: normalize_optional_progress_percent(
            record,
            record.development_progress_percent_raw.as_deref(),
        )?,
        lot_sales_status,
        business_period_start_month: business_period.as_ref().map(|(start, _)| start.clone()),
        business_period_end_month: business_period.map(|(_, end)| end),
        business_period_raw: optional_text(record.business_period_raw.as_deref()),
        designation_basis_law_raw: optional_text(record.designation_basis_law_raw.as_deref()),
        development_method_raw: optional_text(record.development_method_raw.as_deref()),
        development_purpose_raw: optional_text(record.development_purpose_raw.as_deref()),
        invited_industries_raw: optional_text(record.invited_industries_raw.as_deref()),
        source_snapshot_id: lineage.source_snapshot_id.to_owned(),
        valid_from_utc: lineage.valid_from_utc,
        ingested_at_utc: lineage.ingested_at_utc,
    })
}

/// Serializes one row as a single-line `bronze.industrial_complexes_raw_jsonl` record.
///
/// The emitted keys are exactly the exported transport columns, so a new Silver column that the
/// Spark job does not derive fails here instead of surfacing as a missing-column Spark error.
///
/// # Errors
/// Returns [`IndustrialComplexBronzeRawPlanError`] when a transport column has no mapping or JSON
/// serialization fails.
pub fn industrial_complex_bronze_raw_row_to_jsonl(
    row: &IndustrialComplexBronzeRawRow,
) -> Result<String, IndustrialComplexBronzeRawPlanError> {
    let mut record = JsonMap::new();
    for column in bronze_industrial_complexes_raw_jsonl_columns() {
        let value = column_value(row, column).ok_or_else(|| {
            IndustrialComplexBronzeRawPlanError::UnmappedColumn(column.to_owned())
        })?;
        record.insert(column.to_owned(), value);
    }
    serde_json::to_string(&JsonValue::Object(record))
        .map_err(|error| IndustrialComplexBronzeRawPlanError::Serialization(error.to_string()))
}

fn column_value(row: &IndustrialComplexBronzeRawRow, column: &str) -> Option<JsonValue> {
    let value = match column {
        "official_complex_code" => JsonValue::String(row.official_complex_code.clone()),
        "complex_name" => JsonValue::String(row.complex_name.clone()),
        "complex_kind" => JsonValue::String(row.complex_kind.clone()),
        "status" => JsonValue::String(row.status.clone()),
        // All three are `null` when no source stated an administrative code, and `sido_code` and
        // `sigungu_code` carry only what a real code derives into. `primary_bjdong_code` is `null`
        // even when a code exists, unless that code actually names a dong. Never `""` and never a
        // zero code: `required: false` on these columns in the `silver.industrial_complexes`
        // contract is what makes the honest answer representable (root ADR-0034, ADR-0035).
        "sido_code" => optional_str_json(row.address.sido_code()),
        "sigungu_code" => optional_str_json(row.address.sigungu_code()),
        "primary_bjdong_code" => optional_str_json(row.address.primary_bjdong_code()),
        "address_text" => JsonValue::String(row.address.address_text().to_owned()),
        "management_agency_name" => optional_string_json(row.management_agency_name.as_ref()),
        "developer_name" => optional_string_json(row.developer_name.as_ref()),
        "designated_date" => optional_string_json(row.designated_date.as_ref()),
        "construction_start_date" => optional_string_json(row.construction_start_date.as_ref()),
        "completion_date" => optional_string_json(row.completion_date.as_ref()),
        "official_area_sqm" => optional_string_json(row.official_area_sqm.as_ref()),
        "development_progress_percent" => {
            optional_string_json(row.development_progress_percent.as_ref())
        }
        "lot_sales_status" => optional_string_json(row.lot_sales_status.as_ref()),
        "business_period_raw" => optional_string_json(row.business_period_raw.as_ref()),
        "business_period_start_month" => {
            optional_string_json(row.business_period_start_month.as_ref())
        }
        "business_period_end_month" => optional_string_json(row.business_period_end_month.as_ref()),
        "designation_basis_law_raw" => optional_string_json(row.designation_basis_law_raw.as_ref()),
        "development_method_raw" => optional_string_json(row.development_method_raw.as_ref()),
        "development_purpose_raw" => optional_string_json(row.development_purpose_raw.as_ref()),
        "invited_industries_raw" => optional_string_json(row.invited_industries_raw.as_ref()),
        "source_record_id" => JsonValue::String(row.source_record_id.clone()),
        "source_snapshot_id" => JsonValue::String(row.source_snapshot_id.clone()),
        "valid_from_utc" => JsonValue::String(timestamp_json(row.valid_from_utc)),
        "ingested_at_utc" => JsonValue::String(timestamp_json(row.ingested_at_utc)),
        _ => return None,
    };
    Some(value)
}

fn assert_addresses_are_sourced(
    input: &IndustrialComplexBronzeRawRowsInput<'_>,
) -> Result<(), IndustrialComplexBronzeRawPlanError> {
    let missing = input
        .records
        .iter()
        .map(|record| record.official_complex_code.trim())
        .filter(|code| input.addresses.get(code).is_none())
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    Err(IndustrialComplexBronzeRawPlanError::MissingAddresses {
        missing_count: missing.len(),
        row_count: input.records.len(),
        sample: missing
            .iter()
            .take(MISSING_ADDRESS_SAMPLE_LIMIT)
            .copied()
            .collect::<Vec<_>>()
            .join(", "),
    })
}

fn single_snapshot_period(
    records: &[IndustrialComplexBronzeSourceRecord],
) -> Result<String, IndustrialComplexBronzeRawPlanError> {
    let periods = records
        .iter()
        .map(|record| record.snapshot_period.trim().to_owned())
        .collect::<BTreeSet<_>>();
    let mut periods = periods.into_iter();
    let (Some(period), None) = (periods.next(), periods.next()) else {
        return Err(IndustrialComplexBronzeRawPlanError::MixedSnapshotPeriods(
            records
                .iter()
                .map(|record| record.snapshot_period.trim().to_owned())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
                .join(", "),
        ));
    };
    Ok(period)
}

fn snapshot_period_start_utc(
    period: &str,
) -> Result<DateTime<Utc>, IndustrialComplexBronzeRawPlanError> {
    let invalid = || {
        IndustrialComplexBronzeRawPlanError::InvalidInput(format!(
            "snapshot_period must be yyyymm: {period:?}"
        ))
    };
    if period.len() != 6 || !period.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let year = period[0..4].parse::<i32>().map_err(|_| invalid())?;
    let month = period[4..6].parse::<u32>().map_err(|_| invalid())?;
    NaiveDate::from_ymd_opt(year, month, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|naive| naive.and_utc())
        .ok_or_else(invalid)
}

/// Resolves a source label through the observed table first, then the presumed one.
///
/// Both are searched, but they stay separate values so a reader can tell a measured mapping from
/// an unconfirmed one without trusting a comment.
fn map_label(
    record: &IndustrialComplexBronzeSourceRecord,
    column: &str,
    label: &str,
    tables: &[&[(&str, &str)]],
    domain: &[&str],
) -> Result<String, IndustrialComplexBronzeRawPlanError> {
    let label = label.trim();
    let wire = tables
        .iter()
        .flat_map(|table| table.iter())
        .find(|(source_label, _)| *source_label == label)
        .map(|(_, wire)| (*wire).to_owned())
        .ok_or_else(|| {
            IndustrialComplexBronzeRawPlanError::InvalidInput(format!(
                "row {} official_complex_code {}: {column} has no mapping for source label {label:?}",
                record.source_row_number,
                record.official_complex_code.trim()
            ))
        })?;
    if !domain.contains(&wire.as_str()) {
        return Err(IndustrialComplexBronzeRawPlanError::InvalidInput(format!(
            "{column} wire value {wire:?} is outside the exported value domain"
        )));
    }
    Ok(wire)
}

fn normalize_optional_date(
    record: &IndustrialComplexBronzeSourceRecord,
    column: &str,
    raw: Option<&str>,
) -> Result<Option<String>, IndustrialComplexBronzeRawPlanError> {
    let Some(raw) = optional_text(raw) else {
        return Ok(None);
    };
    let invalid = || {
        IndustrialComplexBronzeRawPlanError::InvalidInput(format!(
            "row {} official_complex_code {}: {column} must be yyyymmdd or yyyy-mm-dd: {raw:?}",
            record.source_row_number,
            record.official_complex_code.trim()
        ))
    };
    let digits = raw.replace('-', "");
    if digits.len() != 8 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let year = digits[0..4].parse::<i32>().map_err(|_| invalid())?;
    let month = digits[4..6].parse::<u32>().map_err(|_| invalid())?;
    let day = digits[6..8].parse::<u32>().map_err(|_| invalid())?;
    let date = NaiveDate::from_ymd_opt(year, month, day).ok_or_else(invalid)?;
    Ok(Some(date.format("%Y-%m-%d").to_string()))
}

/// Normalizes `appn_ar` to two decimal places.
///
/// A blank cell and a recorded `0` are both `None`: the Silver contract admits an absent area but
/// gates `official_area_sqm > 0`, and no industrial complex occupies zero square meters. A negative
/// area is a decode error, not an absence, so it fails.
fn normalize_optional_area(
    record: &IndustrialComplexBronzeSourceRecord,
    raw: Option<&str>,
) -> Result<Option<String>, IndustrialComplexBronzeRawPlanError> {
    let Some(raw) = optional_text(raw) else {
        return Ok(None);
    };
    let invalid = || {
        IndustrialComplexBronzeRawPlanError::InvalidInput(format!(
            "row {} official_complex_code {}: official_area_sqm must be a non-negative number: {raw:?}",
            record.source_row_number,
            record.official_complex_code.trim()
        ))
    };
    let parsed = raw.replace(',', "").parse::<f64>().map_err(|_| invalid())?;
    if !parsed.is_finite() || parsed < 0.0 {
        return Err(invalid());
    }
    if parsed == 0.0 {
        return Ok(None);
    }
    Ok(Some(format!("{parsed:.2}")))
}

/// Normalizes `make_procs_rt` to two decimal places.
///
/// Unlike [`normalize_optional_area`], a recorded `0` is kept. Zero square meters is a defect —
/// nothing occupies no land — but zero percent progress is what 84 complexes in the measured table
/// actually report: `준비중` and `보상중` both average 0.0. Folding it to absence would turn "site
/// works have not started" into "the source said nothing".
///
/// Out of range is a decode error rather than an absence. The column is a percentage of one
/// complex's own site formation, so 120 is not a number the source can mean.
fn normalize_optional_progress_percent(
    record: &IndustrialComplexBronzeSourceRecord,
    raw: Option<&str>,
) -> Result<Option<String>, IndustrialComplexBronzeRawPlanError> {
    let Some(raw) = optional_text(raw) else {
        return Ok(None);
    };
    let invalid = || {
        IndustrialComplexBronzeRawPlanError::InvalidInput(format!(
            "row {} official_complex_code {}: development_progress_percent must be a number \
             between 0 and 100: {raw:?}",
            record.source_row_number,
            record.official_complex_code.trim()
        ))
    };
    let parsed = raw.replace(',', "").parse::<f64>().map_err(|_| invalid())?;
    if !parsed.is_finite() || !(0.0..=100.0).contains(&parsed) {
        return Err(invalid());
    }
    Ok(Some(format!("{parsed:.2}")))
}

/// Splits `bsms_pd` into its start and end month, or returns `None` when it does not split.
///
/// The whole value is `YYYY-MM~YYYY-MM` for 1,440 of the 1,441 rows that have one. The remaining
/// row reads `2020-~2024-` — the years are there and the months are not — and there is nothing to
/// recover from it without choosing a month the source never wrote. That row keeps its raw text
/// and takes `None` here, which is why the two derived columns are `Option` and why they are
/// always `None` together: a start month beside an absent end month would describe a period that
/// nobody stated.
///
/// This never fails. A shape the split does not recognize is not a broken contract — the source is
/// entitled to write what it likes in a free-text period — and the raw column carries it either
/// way. The producer counts how many rows parsed so the ones that did not are visible in the export
/// summary rather than silently uniform.
fn business_period_months(raw: Option<&str>) -> Option<(String, String)> {
    let raw = optional_text(raw)?;
    let (start, end) = raw.split_once('~')?;
    Some((month_text(start)?, month_text(end)?))
}

/// Returns `yyyy-MM` when `value` is exactly that, and `None` otherwise.
fn month_text(value: &str) -> Option<String> {
    let value = value.trim();
    let (year, month) = value.split_once('-')?;
    let shaped = year.len() == 4
        && month.len() == 2
        && year.bytes().all(|byte| byte.is_ascii_digit())
        && month.bytes().all(|byte| byte.is_ascii_digit());
    if !shaped {
        return None;
    }
    // A month number outside 1..=12 is not a month, and passing it through would put a value into
    // a column whose whole promise is that it holds one.
    let month_number = month.parse::<u32>().ok()?;
    (1..=12).contains(&month_number).then(|| value.to_owned())
}

fn optional_string_json(value: Option<&String>) -> JsonValue {
    value.map_or(JsonValue::Null, |value| JsonValue::String(value.clone()))
}

fn optional_str_json(value: Option<&str>) -> JsonValue {
    value.map_or(JsonValue::Null, |value| JsonValue::String(value.to_owned()))
}

fn optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn timestamp_json(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn require_text(label: &str, value: &str) -> Result<String, IndustrialComplexBronzeRawPlanError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(IndustrialComplexBronzeRawPlanError::InvalidInput(format!(
            "{label} must be non-empty"
        )));
    }
    Ok(value.to_owned())
}

fn require_row_text(
    record: &IndustrialComplexBronzeSourceRecord,
    label: &str,
    value: &str,
) -> Result<String, IndustrialComplexBronzeRawPlanError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(IndustrialComplexBronzeRawPlanError::InvalidInput(format!(
            "row {}: {label} must be non-empty",
            record.source_row_number
        )));
    }
    Ok(value.to_owned())
}

fn require_address_part(
    label: &str,
    value: &str,
) -> Result<String, IndustrialComplexBronzeRawPlanError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(IndustrialComplexBronzeRawPlanError::InvalidAddress(
            format!("{label} must be non-empty"),
        ));
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        industrial_complex_labels_measured_for, COMPLEX_KIND_LABELS_OBSERVED,
        COMPLEX_KIND_LABELS_PRESUMED, COMPLEX_STATUS_LABELS_OBSERVED,
        COMPLEX_STATUS_LABELS_PRESUMED, INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD,
    };

    /// The whole `202506` table, measured by reading the Bronze object: every distinct `lrstt_ty`
    /// and `make_sttus_nm` with its row count. A label may only move into an `OBSERVED` list when a
    /// snapshot has actually produced it, and this is that record.
    const MEASURED_KIND_LABELS: &[(&str, u32)] =
        &[("국가", 94), ("일반", 812), ("도시첨단", 51), ("농공", 485)];
    const MEASURED_STATUS_LABELS: &[(&str, u32)] = &[
        ("조성완료", 1069),
        ("조성중", 289),
        ("준비중", 47),
        ("보상중", 37),
    ];
    const MEASURED_ROW_COUNT: u32 = 1442;

    /// The snapshot month the row counts below were read from. `MEASURED_*` and
    /// `INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD` must name the same month, or the tables
    /// claim evidence from a table nobody counted.
    const MEASURED_PERIOD: &str = "202506";

    #[test]
    fn the_measured_period_is_the_one_the_label_tables_claim() {
        assert!(industrial_complex_labels_measured_for(MEASURED_PERIOD));
        assert!(industrial_complex_labels_measured_for(&format!(
            "  {MEASURED_PERIOD}  "
        )));
        // Any other month is outside the measurement, which the producer has to say out loud.
        for unmeasured in ["202505", "202507", "202606", ""] {
            assert!(
                !industrial_complex_labels_measured_for(unmeasured),
                "{unmeasured} is not the measured snapshot"
            );
        }
    }

    #[test]
    fn observed_label_tables_hold_exactly_what_the_measured_snapshot_produced() {
        assert_eq!(
            INDUSTRIAL_COMPLEX_LABELS_MEASURED_SNAPSHOT_PERIOD,
            MEASURED_PERIOD
        );
        assert_eq!(
            COMPLEX_KIND_LABELS_OBSERVED
                .iter()
                .map(|(label, _)| *label)
                .collect::<Vec<_>>(),
            MEASURED_KIND_LABELS
                .iter()
                .map(|(label, _)| *label)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            COMPLEX_STATUS_LABELS_OBSERVED
                .iter()
                .map(|(label, _)| *label)
                .collect::<Vec<_>>(),
            MEASURED_STATUS_LABELS
                .iter()
                .map(|(label, _)| *label)
                .collect::<Vec<_>>()
        );
        for measured in [MEASURED_KIND_LABELS, MEASURED_STATUS_LABELS] {
            assert_eq!(
                measured.iter().map(|(_, count)| count).sum::<u32>(),
                MEASURED_ROW_COUNT
            );
        }
    }

    #[test]
    fn zero_progress_statuses_are_planned_not_developing() {
        // `준비중`/`보상중` carry mean make_procs_rt 0.0 and no compet_cnfm_de in the measured
        // table; only `조성중` (59.9%) is site work in progress.
        let mapping = |label: &str| {
            COMPLEX_STATUS_LABELS_OBSERVED
                .iter()
                .find(|(source_label, _)| *source_label == label)
                .map(|(_, wire)| *wire)
        };
        assert_eq!(mapping("준비중"), Some("planned"));
        assert_eq!(mapping("보상중"), Some("planned"));
        assert_eq!(mapping("조성중"), Some("developing"));
        assert_eq!(mapping("조성완료"), Some("operating"));
    }

    #[test]
    fn a_label_is_listed_as_observed_or_presumed_but_never_both() {
        for (observed, presumed) in [
            (COMPLEX_KIND_LABELS_OBSERVED, COMPLEX_KIND_LABELS_PRESUMED),
            (
                COMPLEX_STATUS_LABELS_OBSERVED,
                COMPLEX_STATUS_LABELS_PRESUMED,
            ),
        ] {
            for (label, _) in observed {
                assert!(
                    !presumed
                        .iter()
                        .any(|(presumed_label, _)| presumed_label == label),
                    "{label} is listed as both measured and presumed"
                );
            }
        }
    }
}
