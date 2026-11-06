//! The `media` block of `data-go-kr-onbid.contract.json`: which response fields are media, how a
//! thumbnail's original is asked for, and the bounds of the media and ladder steps.

use std::collections::BTreeMap;

use serde::Deserialize;

/// How 물건상세 photos and appraisal attachments are mirrored to the shared content-addressed store
/// (ADR-0296 §2 / ADR-0297). Consumed by `onbid_media`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct Media {
    /// Object-key prefix the content-addressed media lands under (`<prefix>/sha256/<hash>.<ext>`).
    pub(crate) object_prefix: String,
    /// The only host whose URLs are fetched (the provider serves all media from `www.onbid.co.kr`).
    pub(crate) url_host: String,
    /// Downloads attempted per KST day; the day's total across runs.
    pub(crate) daily_download_limit: u32,
    /// Downloads attempted in one collect run — bounds one run's wall time (attempts ×
    /// `download_timeout_seconds`) so a high daily budget cannot make a run exceed its time limit
    /// (ADR-0300 H: every collection job declares a per-run bound).
    pub(crate) per_run_limit: u32,
    /// Per-download timeout (seconds); short because photos are small — a hung link must not let the
    /// media step eat the job's time limit.
    pub(crate) download_timeout_seconds: u64,
    /// A single file larger than this is not stored (safety valve; root: the 5.89 GB single-PUT stall).
    pub(crate) max_file_bytes: u64,
    /// Downloads of one condition's media in flight at once (root ADR-0330 §3).
    pub(crate) concurrency: usize,
    /// No new condition's media is started once this many seconds of the media step have passed,
    /// so the step's wall time is bounded whatever the counts (root ADR-0330 §3).
    pub(crate) run_time_budget_seconds: u64,
    /// Which response fields are media, and the `media_kind` each lands as (root ADR-0340 §4).
    pub(crate) fields: MediaFields,
    /// The bound of the photos' rendition ladder step (root ADR-0427 §2).
    pub(crate) renditions: RenditionBudget,
}

/// How much of a run the photos' rendition ladders may take (root ADR-0427 §2, ADR-0300 H): no line
/// starts once `per_run_limit` photo sources were taken or `run_time_budget_seconds` passed.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RenditionBudget {
    pub(crate) per_run_limit: u32,
    pub(crate) concurrency: usize,
    pub(crate) run_time_budget_seconds: u64,
}

/// The media fields of the 물건상세 and 공고상세 responses, by shape (root ADR-0340 §4).
#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct MediaFields {
    /// `[{urlAdr}]` arrays: field → media_kind.
    pub(crate) list: BTreeMap<String, String>,
    /// `|`-joined URL strings: field → media_kind.
    pub(crate) pipe: BTreeMap<String, String>,
    /// `list` fields whose URL is a thumbnail: field → how its full-size original is asked for
    /// (root ADR-0424).
    #[serde(default)]
    pub(crate) originals: BTreeMap<String, OriginalRule>,
}

/// How a thumbnail URL becomes its original's: on a URL whose path ends with `path_suffix`, each
/// `query` key is set to its value (root ADR-0424). Its `media_kind` is the Silver loader's.
#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct OriginalRule {
    pub(crate) path_suffix: String,
    pub(crate) query: BTreeMap<String, String>,
    /// The provider's thumbnail's longest side: an "original" no larger than this means the rule
    /// stopped reaching originals (root ADR-0427 §4).
    pub(crate) thumbnail_longest_px: u32,
}
