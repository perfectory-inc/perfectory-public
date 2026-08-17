//! Derivation of the injected address resolution for the industrial-complex Bronze JSONL producer.
//!
//! Root ADR-0033 made "an industrial complex without a sourced address" unrepresentable and left the
//! address as an injected input. This module is where that input is actually derived, from Bronze
//! objects only:
//!
//! - the industrial-complex profile workbook, which supplies the set of complexes to resolve;
//! - the ILIS complex list, which supplies a district code and the official address text;
//! - the ILIS notices, which supply district codes for complexes whose own code is superseded;
//! - a city/county/district authority, which is what makes "this code is current" checkable.
//!
//! Three rules, applied in order. They are not equally strong and the output says which one
//! produced each value (root ADR-0034):
//!
//! | tier | rule | strength |
//! |---|---|---|
//! | [`ResolutionTier::SourceCodeInAuthority`] | the complex's own code is in the authority | exact |
//! | [`ResolutionTier::ModalNoticeCode`] | the most frequent valid code among that complex's own notices | heuristic |
//! | [`ResolutionTier::LearnedCodeMigration`] | a superseded code mapped through migrations observed in the tier above | derived |
//! | [`ResolutionTier::AddressTextOnly`] | no rule produced a code; the official address text alone | none |
//!
//! The fourth row is not a fourth rule. It is what the derivation says when the three rules find
//! nothing: the complex keeps its published address text and its region columns are `null`. Before
//! root ADR-0035 that outcome failed the whole export, because region was a requirement; it no
//! longer is, and a made-up district code would still be forbidden.
//!
//! The migration table is **learned, never typed**. A constant list of "old code -> new code" pairs
//! in this file would be an unfalsifiable claim about the outside world; a table derived from what
//! the notices actually showed can be recomputed, and a contradiction in it stops the run instead of
//! silently picking a winner.
//!
//! The join is by code throughout — `krihs_irstt_code` = `danji_cd`. Never by name (two complexes
//! share a name often enough that a name join is a guess) and never by geometry (root ADR-0020).

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::industrial_complex_bronze_raw_plan::IndustrialComplexAddressGranularity;

/// Length of a Korean legal-division code in its full ten-digit form.
const ADMINISTRATIVE_CODE_LEN: usize = 10;

/// Length of the city/county/district prefix inside that code.
const SIGUNGU_CODE_LEN: usize = 5;

/// The five trailing digits a district-granularity code carries.
const SIGUNGU_CODE_SUFFIX: &str = "00000";

/// How one resolution was derived.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ResolutionTier {
    /// The complex's own administrative code was found in the district authority, unchanged.
    SourceCodeInAuthority,
    /// The most frequent valid district code among that complex's own notices.
    ///
    /// A heuristic and labelled as one. A complex can genuinely straddle two districts, and the
    /// notices carry provider-side typos, so the mode is the best available answer rather than a
    /// proof.
    ModalNoticeCode,
    /// A superseded code mapped through the learned migration table.
    LearnedCodeMigration,
    /// No rule produced an administrative code; the source published only the address words.
    ///
    /// Distinct from the three above rather than a fourth way of getting a code, because there is
    /// no code. The resolution still carries the official address text and its provenance, and the
    /// exported row writes `null` for every region column (root ADR-0035).
    AddressTextOnly,
}

impl ResolutionTier {
    /// Returns the stable wire value written into the resolution file.
    ///
    /// The values name the method rather than a tier number, because `T2` on its own does not tell
    /// a later reader that the value is a heuristic.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::SourceCodeInAuthority => "source_code_in_authority",
            Self::ModalNoticeCode => "modal_notice_code",
            Self::LearnedCodeMigration => "learned_code_migration",
            Self::AddressTextOnly => "address_text_only",
        }
    }

    /// Returns whether a line of this tier carries an administrative code.
    ///
    /// The reader checks both directions. A line that says `address_text_only` and still carries a
    /// code is claiming a rule produced a value it also says was never produced, and a line of any
    /// other tier with no code names a rule that produces codes and then shows none.
    #[must_use]
    pub const fn carries_an_administrative_code(self) -> bool {
        !matches!(self, Self::AddressTextOnly)
    }

    /// Parses a wire value read back out of a resolution file.
    ///
    /// This is the reader half of [`Self::wire_name`], here rather than in the producer so the
    /// writer and the reader cannot spell the same tier differently.
    ///
    /// # Errors
    /// Returns [`IndustrialComplexAddressResolutionError::UnknownResolutionTier`] for any other
    /// value. There is deliberately no default: a line that does not say how it was resolved has
    /// not said how much its value can be trusted.
    pub fn from_wire(value: &str) -> Result<Self, IndustrialComplexAddressResolutionError> {
        match value.trim() {
            "source_code_in_authority" => Ok(Self::SourceCodeInAuthority),
            "modal_notice_code" => Ok(Self::ModalNoticeCode),
            "learned_code_migration" => Ok(Self::LearnedCodeMigration),
            "address_text_only" => Ok(Self::AddressTextOnly),
            other => Err(
                IndustrialComplexAddressResolutionError::UnknownResolutionTier(other.to_owned()),
            ),
        }
    }

    /// Returns whether a line of this tier may name `dataset` as where its value came from.
    ///
    /// Only the notice tier reads a code out of a notice, and only it may say so. A
    /// `modal_notice_code` line attributed to the complex list — or a `source_code_in_authority`
    /// line attributed to the notices — is not a mis-labelling: it is a claim about where the value
    /// came from that the value contradicts. The complex tiers do not pin one dataset, because the
    /// complexes arrive from two (the bulk list, and the per-complex detail endpoint for the ones
    /// the list omits).
    #[must_use]
    pub fn admits_source_dataset(self, dataset: &str) -> bool {
        let names_the_notices = dataset.trim().ends_with(NOTICE_DATASET_SLUG_SUFFIX);
        match self {
            Self::ModalNoticeCode => names_the_notices,
            Self::SourceCodeInAuthority | Self::LearnedCodeMigration | Self::AddressTextOnly => {
                !names_the_notices
            }
        }
    }
}

/// Dataset-slug suffix that identifies the notice dataset in a resolution line's provenance.
pub const NOTICE_DATASET_SLUG_SUFFIX: &str = "industrial_complex_notice";

/// Why one complex could not be resolved at all.
///
/// Both variants are about the address text, not the administrative code: a complex with no words
/// and no code has no location to record, while a complex with words and no code is resolved with
/// [`ResolutionTier::AddressTextOnly`] (root ADR-0035).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnresolvedReason {
    /// The address source does not list this complex at all.
    MissingFromSourceList,
    /// The source lists it but carries no address text, so there is nothing to record as the
    /// official address.
    AddressTextAbsent,
}

impl UnresolvedReason {
    /// Returns the stable wire value written into the run summary.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::MissingFromSourceList => "missing_from_source_list",
            Self::AddressTextAbsent => "address_text_absent",
        }
    }
}

/// Why one resolved complex carries no administrative code.
///
/// Kept apart from [`UnresolvedReason`] because these complexes did resolve: the summary says how
/// each of them came to have no code, which is the difference between a source that never stated
/// one and a source whose code no longer exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MissingAdministrativeCodeReason {
    /// The source lists the complex but carries no administrative code.
    SourceCodeAbsent,
    /// The source's code is not in the authority, and neither the notices nor the learned
    /// migrations offer a current one.
    SourceCodeUnknownAndNoNoticeEvidence,
}

impl MissingAdministrativeCodeReason {
    /// Returns the stable wire value written into the run summary.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::SourceCodeAbsent => "source_code_absent",
            Self::SourceCodeUnknownAndNoNoticeEvidence => {
                "source_code_unknown_and_no_notice_evidence"
            }
        }
    }
}

/// One complex as the address source lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceComplexRecord {
    /// `danji_cd`, which is the same code as the profile's `krihs_irstt_code`.
    pub official_complex_code: String,
    /// `addr_cd` as the source supplied it, or `None` when blank.
    pub administrative_code: Option<String>,
    /// `danji_loc` verbatim. Never normalized: the official address text is the source's wording.
    pub address_text: Option<String>,
    /// Bronze source slug this record was read from.
    ///
    /// Carried per record rather than once per run because the complexes come from two datasets:
    /// the bulk list, and the per-complex detail endpoint for the handful the list omits. A run
    /// that named one dataset for all of them would attribute an address to an object that does
    /// not contain it.
    pub source_dataset: String,
}

/// One notice as the address source lists it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceNoticeRecord {
    /// `danji_cd` of the complex the notice is about.
    pub official_complex_code: String,
    /// `noti_addr` as the source supplied it, or `None` when blank.
    pub administrative_code: Option<String>,
    /// Stable per-notice record id used as provenance when this notice decides the address.
    pub notice_record_id: String,
}

/// Everything the derivation reads.
#[derive(Clone, Copy, Debug)]
pub struct IndustrialComplexAddressResolutionInput<'a> {
    /// The complexes that must be resolved, in profile order.
    pub target_complex_codes: &'a [String],
    /// The address source's complex list.
    pub source_complexes: &'a [SourceComplexRecord],
    /// The address source's notices.
    pub source_notices: &'a [SourceNoticeRecord],
    /// Current five-digit city/county/district codes. This is the authority the codes are checked
    /// against; without it "is this code current?" has no answer.
    pub sigungu_authority: &'a BTreeSet<String>,
    /// Bronze source slug the notices came from, recorded as provenance.
    pub notice_dataset: &'a str,
}

/// The administrative code a resolution ended up with, or why it has none.
///
/// One field rather than a code plus a nullable reason, so a resolution cannot claim both or
/// neither.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedAdministrativeCode {
    /// A rule produced a ten-digit code, and the code says what it names.
    Sourced {
        /// Ten-digit administrative code.
        code: String,
        /// How precise `code` is.
        granularity: IndustrialComplexAddressGranularity,
    },
    /// No rule produced a code for this complex.
    Missing(MissingAdministrativeCodeReason),
}

/// One resolved address, ready to be written as a resolution line.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedComplexAddress {
    /// `official_complex_code` the resolution is for.
    pub official_complex_code: String,
    /// The administrative code, or the reason there is none.
    pub administrative_code: ResolvedAdministrativeCode,
    /// Official address text, verbatim from the source.
    pub address_text: String,
    /// Bronze source slug the value came from.
    pub address_source_dataset: String,
    /// Record id inside that dataset.
    pub address_source_record_id: String,
    /// Which rule produced the code.
    pub resolution_tier: ResolutionTier,
}

/// One complex that could not be resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnresolvedComplex {
    /// `official_complex_code` that stayed unresolved.
    pub official_complex_code: String,
    /// Why.
    pub reason: UnresolvedReason,
}

/// The derivation's whole output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndustrialComplexAddressResolution {
    /// Resolutions, ordered by `official_complex_code`.
    pub resolved: Vec<ResolvedComplexAddress>,
    /// Complexes with no resolution, ordered by `official_complex_code`.
    pub unresolved: Vec<UnresolvedComplex>,
    /// The superseded-to-current district migrations learned from the notice evidence.
    ///
    /// Published as part of the output precisely because it is derived: a reader can check every
    /// pair against the notices rather than trust a table.
    pub learned_code_migrations: BTreeMap<String, String>,
}

impl IndustrialComplexAddressResolution {
    /// Returns how many resolutions each tier produced, keyed by tier wire name.
    #[must_use]
    pub fn tier_counts(&self) -> BTreeMap<&'static str, u64> {
        let mut counts = BTreeMap::new();
        for resolution in &self.resolved {
            *counts
                .entry(resolution.resolution_tier.wire_name())
                .or_insert(0) += 1;
        }
        counts
    }

    /// Returns how many complexes each unresolved reason accounts for, keyed by reason wire name.
    #[must_use]
    pub fn unresolved_counts(&self) -> BTreeMap<&'static str, u64> {
        let mut counts = BTreeMap::new();
        for unresolved in &self.unresolved {
            *counts.entry(unresolved.reason.wire_name()).or_insert(0) += 1;
        }
        counts
    }

    /// Returns the resolutions that carry no administrative code, with the reason for each.
    ///
    /// Named individually rather than counted, because "which complexes are these?" is the
    /// question a reader of the evidence actually has.
    #[must_use]
    pub fn missing_administrative_codes(&self) -> Vec<(&str, MissingAdministrativeCodeReason)> {
        self.resolved
            .iter()
            .filter_map(|resolution| match resolution.administrative_code {
                ResolvedAdministrativeCode::Missing(reason) => {
                    Some((resolution.official_complex_code.as_str(), reason))
                }
                ResolvedAdministrativeCode::Sourced { .. } => None,
            })
            .collect()
    }
}

/// Error returned while deriving the address resolution.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IndustrialComplexAddressResolutionError {
    /// The district authority is empty, so no code could be checked against anything.
    #[error(
        "the city/county/district authority is empty; without it no administrative code can be \
         checked, and every resolution would be an unverified copy of the source"
    )]
    EmptyAuthority,
    /// An authority entry is not a five-digit district code.
    #[error("the city/county/district authority carries a non-district code: {0:?}")]
    InvalidAuthorityCode(String),
    /// The address source lists one complex twice, so "its" address is ambiguous.
    #[error("the address source lists official_complex_code {0} twice")]
    DuplicateSourceComplex(String),
    /// A resolution line names a tier this derivation does not produce.
    #[error(
        "resolution_tier must be one of source_code_in_authority, modal_notice_code, \
         learned_code_migration: {0:?}"
    )]
    UnknownResolutionTier(String),
    /// Two notices imply different current codes for one superseded code.
    ///
    /// The migration table is learned rather than typed, and a learned table with a contradiction
    /// in it is not evidence. Failing here is the point: a silently chosen winner would put roughly
    /// half the affected complexes in the wrong district.
    #[error(
        "the learned district migration is contradictory: superseded code {superseded} maps to \
         both {first} and {second}; the notice evidence does not agree with itself, so nothing was \
         resolved from it"
    )]
    ContradictoryLearnedMigration {
        /// The superseded code with more than one current code.
        superseded: String,
        /// The first current code observed for it.
        first: String,
        /// The conflicting current code.
        second: String,
    },
}

/// Derives the address resolution for every target complex.
///
/// # Errors
/// Returns [`IndustrialComplexAddressResolutionError`] when the authority is empty or malformed,
/// when the source lists a complex twice, or when the learned migration table contradicts itself.
pub fn resolve_industrial_complex_addresses(
    input: &IndustrialComplexAddressResolutionInput<'_>,
) -> Result<IndustrialComplexAddressResolution, IndustrialComplexAddressResolutionError> {
    let authority = validated_authority(input.sigungu_authority)?;
    let complexes = index_source_complexes(input.source_complexes)?;
    let notices_by_complex = index_notices(input.source_notices);

    // Pass one: the exact tier, plus the notice mode for everything it cannot answer. Every
    // (superseded code -> chosen code) pair the notice mode produces is an observation, and the
    // observations ARE the migration table.
    let mut decisions = BTreeMap::<&str, Decision>::new();
    let mut learned = LearnedMigrations::default();
    for code in input.target_complex_codes {
        let decision = decide_from_source_and_notices(
            code.as_str(),
            &complexes,
            &notices_by_complex,
            &authority,
            &mut learned,
        )?;
        decisions.insert(code.as_str(), decision);
    }

    // Pass two: the learned migrations are only complete once every notice observation is in, so a
    // complex with no notices of its own is answered here or not at all.
    let mut resolved = Vec::new();
    let mut unresolved = Vec::new();
    for code in input.target_complex_codes {
        // A repeated target code was already decided on its first appearance; the profile producer
        // rejects duplicates, so a second visit is unreachable rather than a silent skip.
        let Some(decision) = decisions.remove(code.as_str()) else {
            continue;
        };
        let record = complexes.get(code.as_str()).copied();
        let outcome = settle(decision, &learned);
        match outcome.and_then(|decided| resolution_for(input, code, record, decided)) {
            Ok(resolution) => resolved.push(resolution),
            Err(reason) => unresolved.push(UnresolvedComplex {
                official_complex_code: code.clone(),
                reason,
            }),
        }
    }
    resolved.sort_by(|left, right| left.official_complex_code.cmp(&right.official_complex_code));
    unresolved.sort_by(|left, right| left.official_complex_code.cmp(&right.official_complex_code));
    Ok(IndustrialComplexAddressResolution {
        resolved,
        unresolved,
        learned_code_migrations: learned.into_table(),
    })
}

enum Decision {
    Resolved {
        administrative_code: String,
        tier: ResolutionTier,
        notice_record_id: Option<String>,
    },
    PendingMigration(String),
    NoCode(MissingAdministrativeCodeReason),
    Unresolved(UnresolvedReason),
}

/// Applies the first two rules to one complex, recording whatever migrations it observes.
///
/// The third rule cannot run here: it reads a table that is only complete once every complex has
/// been through this pass.
fn decide_from_source_and_notices(
    code: &str,
    complexes: &BTreeMap<&str, &SourceComplexRecord>,
    notices_by_complex: &BTreeMap<&str, Vec<&SourceNoticeRecord>>,
    authority: &BTreeSet<String>,
    learned: &mut LearnedMigrations,
) -> Result<Decision, IndustrialComplexAddressResolutionError> {
    let Some(record) = complexes.get(code) else {
        return Ok(Decision::Unresolved(
            UnresolvedReason::MissingFromSourceList,
        ));
    };
    let Some(source_code) = normalized_code(record.administrative_code.as_deref()) else {
        return Ok(Decision::NoCode(
            MissingAdministrativeCodeReason::SourceCodeAbsent,
        ));
    };
    let own_notices = notices_by_complex.get(code).map_or(&[][..], Vec::as_slice);
    if authority.contains(sigungu_prefix(&source_code)) {
        // This complex is in a district the authority knows, and some of its own notices may still
        // be filed under a code the authority does not. Each such pair is one more observation of
        // the same superseded-to-current migration the notice-mode rule produces, and it is the
        // evidence that lets a complex with no notices of its own be resolved at all.
        observe_superseded_notice_codes(own_notices, authority, source_code.as_str(), learned)?;
        return Ok(Decision::Resolved {
            administrative_code: source_code,
            tier: ResolutionTier::SourceCodeInAuthority,
            notice_record_id: None,
        });
    }
    if let Some(chosen) = modal_valid_notice_code(own_notices, authority) {
        learned.observe(source_code.as_str(), chosen.code.as_str())?;
        return Ok(Decision::Resolved {
            administrative_code: chosen.code,
            tier: ResolutionTier::ModalNoticeCode,
            notice_record_id: Some(chosen.notice_record_id),
        });
    }
    Ok(Decision::PendingMigration(source_code))
}

/// Turns a pass-one decision into an outcome now that the learned migrations are complete.
///
/// A complex whose code no rule could establish is not an error here: it keeps whatever address the
/// source published and its region columns are `null` (root ADR-0035).
fn settle(decision: Decision, learned: &LearnedMigrations) -> Result<Decided, UnresolvedReason> {
    match decision {
        Decision::Unresolved(reason) => Err(reason),
        Decision::NoCode(reason) => Ok(Decided::without_code(reason)),
        Decision::PendingMigration(source_code) => {
            Ok(learned.current_code_for(source_code.as_str()).map_or_else(
                || {
                    Decided::without_code(
                        MissingAdministrativeCodeReason::SourceCodeUnknownAndNoNoticeEvidence,
                    )
                },
                |current| Decided {
                    administrative_code: ResolvedAdministrativeCode::Sourced {
                        granularity: granularity_of(current),
                        code: current.to_owned(),
                    },
                    tier: ResolutionTier::LearnedCodeMigration,
                    notice_record_id: None,
                },
            ))
        }
        Decision::Resolved {
            administrative_code,
            tier,
            notice_record_id,
        } => Ok(Decided {
            administrative_code: ResolvedAdministrativeCode::Sourced {
                granularity: granularity_of(administrative_code.as_str()),
                code: administrative_code,
            },
            tier,
            notice_record_id,
        }),
    }
}

/// A code and the rule that produced it, before the address text is attached.
struct Decided {
    administrative_code: ResolvedAdministrativeCode,
    tier: ResolutionTier,
    notice_record_id: Option<String>,
}

impl Decided {
    /// The outcome for a complex no rule could give a code, which is still an address.
    const fn without_code(reason: MissingAdministrativeCodeReason) -> Self {
        Self {
            administrative_code: ResolvedAdministrativeCode::Missing(reason),
            tier: ResolutionTier::AddressTextOnly,
            notice_record_id: None,
        }
    }
}

/// Attaches the source's address text and provenance to a decided code.
///
/// A complex whose code resolved but whose address text is blank stays unresolved: the exported row
/// carries `address_text`, and a code without the words that go with it is half a location.
fn resolution_for(
    input: &IndustrialComplexAddressResolutionInput<'_>,
    code: &str,
    record: Option<&SourceComplexRecord>,
    decided: Decided,
) -> Result<ResolvedComplexAddress, UnresolvedReason> {
    let record = record.ok_or(UnresolvedReason::MissingFromSourceList)?;
    let address_text = record
        .address_text
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .ok_or(UnresolvedReason::AddressTextAbsent)?;
    // The notice that decided a `modal_notice_code` value is where that value came from; every
    // other tier came from the complex record, which names its own dataset because the complexes
    // arrive from two of them.
    let (dataset, record_id) = match (decided.tier, decided.notice_record_id) {
        (ResolutionTier::ModalNoticeCode, Some(notice_record_id)) => {
            (input.notice_dataset, notice_record_id)
        }
        _ => (record.source_dataset.as_str(), format!("danji_cd={code}")),
    };
    Ok(ResolvedComplexAddress {
        official_complex_code: code.to_owned(),
        // Every code this derivation can produce is district-granularity: the source's own codes end
        // in five zeros and so do the notice codes. The value says so rather than the reader
        // assuming it (root ADR-0034).
        administrative_code: decided.administrative_code,
        address_text: address_text.to_owned(),
        address_source_dataset: dataset.to_owned(),
        address_source_record_id: record_id,
        resolution_tier: decided.tier,
    })
}

/// Reads the granularity off the code's own shape.
fn granularity_of(administrative_code: &str) -> IndustrialComplexAddressGranularity {
    if administrative_code.ends_with(SIGUNGU_CODE_SUFFIX) {
        IndustrialComplexAddressGranularity::Sigungu
    } else {
        IndustrialComplexAddressGranularity::LegalDong
    }
}

/// Records every code among a complex's notices that the authority does not know as having been
/// superseded by that complex's own current code.
fn observe_superseded_notice_codes(
    notices: &[&SourceNoticeRecord],
    authority: &BTreeSet<String>,
    current_code: &str,
    learned: &mut LearnedMigrations,
) -> Result<(), IndustrialComplexAddressResolutionError> {
    for notice in notices {
        let Some(code) = normalized_code(notice.administrative_code.as_deref()) else {
            continue;
        };
        if authority.contains(sigungu_prefix(&code)) {
            continue;
        }
        learned.observe(code.as_str(), current_code)?;
    }
    Ok(())
}

/// One notice code chosen by the modal rule, with the notice that carried it.
struct ChosenNoticeCode {
    code: String,
    notice_record_id: String,
}

/// Returns the most frequent authority-valid code among a complex's own notices.
///
/// Ties break on the lowest code so the derivation is deterministic; a tie means the complex really
/// does straddle two districts, and picking either one is a choice the tier label already flags as a
/// heuristic.
fn modal_valid_notice_code(
    notices: &[&SourceNoticeRecord],
    authority: &BTreeSet<String>,
) -> Option<ChosenNoticeCode> {
    let mut counts = BTreeMap::<String, (u64, String)>::new();
    for notice in notices {
        let Some(code) = normalized_code(notice.administrative_code.as_deref()) else {
            continue;
        };
        if !authority.contains(sigungu_prefix(&code)) {
            continue;
        }
        let entry = counts
            .entry(code)
            .or_insert_with(|| (0, notice.notice_record_id.clone()));
        entry.0 += 1;
    }
    counts
        .into_iter()
        .max_by(|left, right| {
            // Higher count wins; on a tie the lower code wins, so `max_by` compares the code
            // reversed.
            left.1
                 .0
                .cmp(&right.1 .0)
                .then_with(|| right.0.cmp(&left.0))
        })
        .map(|(code, (_, notice_record_id))| ChosenNoticeCode {
            code,
            notice_record_id,
        })
}

/// The superseded-to-current district migrations observed while applying the notice-mode rule.
#[derive(Default)]
struct LearnedMigrations {
    table: BTreeMap<String, String>,
}

impl LearnedMigrations {
    fn observe(
        &mut self,
        superseded: &str,
        current: &str,
    ) -> Result<(), IndustrialComplexAddressResolutionError> {
        if let Some(existing) = self.table.get(superseded) {
            if existing != current {
                return Err(
                    IndustrialComplexAddressResolutionError::ContradictoryLearnedMigration {
                        superseded: superseded.to_owned(),
                        first: existing.clone(),
                        second: current.to_owned(),
                    },
                );
            }
            return Ok(());
        }
        self.table.insert(superseded.to_owned(), current.to_owned());
        Ok(())
    }

    fn current_code_for(&self, superseded: &str) -> Option<&str> {
        self.table.get(superseded).map(String::as_str)
    }

    fn into_table(self) -> BTreeMap<String, String> {
        self.table
    }
}

fn validated_authority(
    authority: &BTreeSet<String>,
) -> Result<BTreeSet<String>, IndustrialComplexAddressResolutionError> {
    if authority.is_empty() {
        return Err(IndustrialComplexAddressResolutionError::EmptyAuthority);
    }
    let mut validated = BTreeSet::new();
    for code in authority {
        let code = code.trim();
        if code.len() != SIGUNGU_CODE_LEN || !code.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(
                IndustrialComplexAddressResolutionError::InvalidAuthorityCode(code.to_owned()),
            );
        }
        validated.insert(code.to_owned());
    }
    Ok(validated)
}

fn index_source_complexes(
    records: &[SourceComplexRecord],
) -> Result<BTreeMap<&str, &SourceComplexRecord>, IndustrialComplexAddressResolutionError> {
    let mut indexed = BTreeMap::new();
    for record in records {
        let code = record.official_complex_code.trim();
        if indexed.insert(code, record).is_some() {
            return Err(
                IndustrialComplexAddressResolutionError::DuplicateSourceComplex(code.to_owned()),
            );
        }
    }
    Ok(indexed)
}

fn index_notices(records: &[SourceNoticeRecord]) -> BTreeMap<&str, Vec<&SourceNoticeRecord>> {
    let mut indexed = BTreeMap::<&str, Vec<&SourceNoticeRecord>>::new();
    for record in records {
        indexed
            .entry(record.official_complex_code.trim())
            .or_default()
            .push(record);
    }
    indexed
}

/// Returns a trimmed ten-digit code, or `None` when the value is blank or malformed.
fn normalized_code(value: Option<&str>) -> Option<String> {
    let value = value.map(str::trim)?;
    if value.len() != ADMINISTRATIVE_CODE_LEN || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(value.to_owned())
}

fn sigungu_prefix(code: &str) -> &str {
    &code[0..SIGUNGU_CODE_LEN]
}
