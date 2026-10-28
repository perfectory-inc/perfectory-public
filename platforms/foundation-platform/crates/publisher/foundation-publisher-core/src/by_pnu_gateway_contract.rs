//! Typed access to the by-PNU gateway sections of the R2 connection contract.
//!
//! The contract JSON is the SSOT (root ADR-0096 parcels, ADR-0100 buildings, ADR-0141 patches):
//! the exports, the manifest publisher, and the by-PNU gateway Worker
//! (`services/foundation-by-pnu-gateway`, both lanes) all read the same blocks, so the key grammar the uploader
//! writes under and the grammar the Workers resolve are one fact. Every lane shares one shape.
//!
//! Besides the parcel and building lanes, the contract's `by_pnu_lanes` names lanes that serve
//! from section packs only, each a Gold panel of one kind of data (root ADR-0190). Nothing of such
//! a lane is written in code: its manifest unit, environment prefix and commands follow from its
//! name. A lane that names a `host` and a `kind` (root ADR-0193 §4) writes only what differs per
//! lane; its gateway block is the host lane's, with the kind in its paths (`resolve_lane_entry`).

use std::collections::BTreeMap;
use std::sync::OnceLock;

use lakehouse_domain::{
    industrial_complex_lakehouse_contract_by_table_name, LakehouseTableContract,
};
use serde::Deserialize;

const R2_CONNECTION_CONTRACT: &str =
    include_str!("../../../../config/r2-connections.contract.json");
const RUNTIME_SECRETS_CONTRACT: &str =
    include_str!("../../../../config/runtime-secrets.contract.json");
/// The runtime-secrets group that holds the Cloudflare analytics variables.
const CLOUDFLARE_ANALYTICS_GROUP: &str = "cloudflare-analytics";

/// The by-PNU serving lanes. Everything a lane differs by is named here once: the parcel and
/// building lanes in code, every other lane in the contract's `by_pnu_lanes`.
#[derive(Clone, Copy, Debug)]
pub enum ByPnuLane {
    Parcel,
    Building,
    /// A lane the contract's `by_pnu_lanes` names, served from section packs only (root ADR-0190).
    Pack(&'static PackLane),
}

impl PartialEq for ByPnuLane {
    fn eq(&self, other: &Self) -> bool {
        self.unit() == other.unit()
    }
}

impl Eq for ByPnuLane {}

impl ByPnuLane {
    /// Every lane: the parcel and building lanes, then the contract's in name order.
    ///
    /// # Errors
    /// Returns an error when the contract cannot be read.
    pub fn all() -> anyhow::Result<Vec<Self>> {
        let mut lanes = vec![Self::Parcel, Self::Building];
        lanes.extend(contract()?.by_pnu_lanes.values().map(Self::Pack));
        Ok(lanes)
    }

    /// The lane a name (`parcel`, `building`, or a `by_pnu_lanes` key) names.
    ///
    /// # Errors
    /// Returns an error when the contract cannot be read.
    pub fn named(name: &str) -> anyhow::Result<Option<Self>> {
        Ok(Self::all()?.into_iter().find(|lane| lane.name() == name))
    }

    /// The lane's name: the word in its commands and its bake script's argument.
    pub fn name(self) -> &'static str {
        match self {
            Self::Parcel => "parcel",
            Self::Building => "building",
            Self::Pack(lane) => &lane.name,
        }
    }

    /// The manifest's `unit` and the state's lane name.
    pub fn unit(self) -> &'static str {
        match self {
            Self::Parcel => "parcel-by-pnu",
            Self::Building => "building-by-pnu",
            Self::Pack(lane) => &lane.unit,
        }
    }

    /// The lane a manifest `unit` names.
    pub fn from_unit(unit: &str) -> Option<Self> {
        Self::all()
            .ok()?
            .into_iter()
            .find(|lane| lane.unit() == unit)
    }

    /// The word error messages use for one object of the lane.
    pub fn noun(self) -> &'static str {
        match self {
            Self::Parcel => "parcel",
            Self::Building => "building",
            Self::Pack(lane) => &lane.noun,
        }
    }

    /// The prefix of every environment variable the lane's commands read.
    pub fn env_prefix(self) -> &'static str {
        match self {
            Self::Parcel => "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING",
            Self::Building => "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING",
            Self::Pack(lane) => &lane.env_prefix,
        }
    }

    /// The contract lane, when this is one: it serves from section packs only.
    pub const fn pack_lane(self) -> Option<&'static PackLane> {
        match self {
            Self::Pack(lane) => Some(lane),
            Self::Parcel | Self::Building => None,
        }
    }

    /// One environment variable of the lane.
    pub fn env(self, name: &str) -> String {
        format!("{}_{name}", self.env_prefix())
    }

    /// The lane's gateway block.
    pub fn policy(self) -> anyhow::Result<&'static ByPnuGatewayPolicy> {
        let contract = contract()?;
        Ok(match self {
            Self::Parcel => &contract.parcel_by_pnu_gateway,
            Self::Building => &contract.building_by_pnu_gateway,
            Self::Pack(lane) => &lane.gateway,
        })
    }

    /// The lane's section packs block (root ADR-0147).
    pub fn section_packs(self) -> anyhow::Result<&'static LaneSectionPacks> {
        use anyhow::Context as _;
        self.policy()?
            .section_packs
            .as_ref()
            .with_context(|| format!("the {} lane serves no section packs", self.noun()))
    }
}

#[derive(Debug, Deserialize)]
struct R2ConnectionContract {
    schema_version: u64,
    parcel_by_pnu_gateway: ByPnuGatewayPolicy,
    building_by_pnu_gateway: ByPnuGatewayPolicy,
    by_pnu_serving_patches: ByPnuServingPatchPolicy,
    by_pnu_section_packs: SectionPackPolicy,
    /// The `by_pnu_lanes` entries as written; `contract()` resolves them into `by_pnu_lanes`.
    #[serde(default, rename = "by_pnu_lanes")]
    lane_entries: BTreeMap<String, serde_json::Value>,
    #[serde(skip)]
    by_pnu_lanes: BTreeMap<String, PackLane>,
}

/// The gateway blocks a derived lane may name as its `host` (root ADR-0193 §4).
pub const LANE_HOSTS: [&str; 2] = ["parcel_by_pnu_gateway", "building_by_pnu_gateway"];

/// The only fields a lane with a `host` writes by hand (root ADR-0193 §4): the kind line. A kind
/// built from its own Silver names its Spark `producer` (root ADR-0287); the bake serves it alike.
const DERIVED_LANE_HAND_FIELDS: [&str; 11] = [
    "decision",
    "host",
    "kind",
    "section",
    "columns",
    "producer",
    "document_key_suffix",
    "documents_follow_lane",
    "documents_follow_lane_reason",
    "document_schema_version",
    "document_schema_version_reason",
];

/// The job capability every kind lane's scheduled bake shares: the bake reads the lanes from the
/// contract, so one capability covers every line added to it.
pub const KIND_LANE_BAKE_CAPABILITY: &str = "by_pnu_kind_section_pack_patches";

/// A lane entry as the pack-only machinery reads it. An entry without a `host` is written whole
/// (root ADR-0190). An entry with one (a kind line, root ADR-0193 §4) is its host's gateway block
/// with `{kind}/` inserted before the `by-pnu` segment of its request path and object keys and
/// before the `packs` segment of its pack root, one `documents` section, the kind lanes' bake
/// capability, its document schema and Gold table by name, the host lane as its latency baseline
/// and the documents it follows (unless the line names another), and the rule's latency bound.
/// The Worker (`lane.ts`) applies the same paths; the contract's `by_pnu_lane_rule.test_vectors`
/// hold both to them.
pub fn resolve_lane_entry(
    name: &str,
    entry: &serde_json::Value,
    contract: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    use serde_json::{json, Value};
    let Some(fields) = entry.as_object() else {
        return Err(format!("by_pnu_lanes.{name} is not an object"));
    };
    let Some(host) = fields.get("host") else {
        return Ok(entry.clone());
    };
    if let Some(field) = fields
        .keys()
        .find(|field| !DERIVED_LANE_HAND_FIELDS.contains(&field.as_str()))
    {
        return Err(format!(
            "by_pnu_lanes.{name}.{field} follows from its host and kind (root ADR-0193 §4); a \
             lane with a host writes only {DERIVED_LANE_HAND_FIELDS:?}"
        ));
    }
    let host = host
        .as_str()
        .filter(|host| LANE_HOSTS.contains(host))
        .ok_or_else(|| format!("by_pnu_lanes.{name}.host must be one of {LANE_HOSTS:?}"))?;
    let kind = fields
        .get("kind")
        .and_then(Value::as_str)
        .filter(|kind| lane_grammar(kind))
        .ok_or_else(|| format!("by_pnu_lanes.{name}.kind must be lowercase words joined by -"))?;
    let mut block = contract
        .get(host)
        .cloned()
        .ok_or_else(|| format!("the contract has no {host}"))?;
    let snake = name.replace('-', "_");
    let host_lane = host.trim_end_matches("_by_pnu_gateway");
    let rule = contract
        .get("by_pnu_lane_rule")
        .ok_or("the contract has no by_pnu_lane_rule")?;
    for (pointer, segment) in [
        ("/request_path/prefix", "by-pnu"),
        ("/request_path/capabilities", "by-pnu"),
        ("/object_key/root", "by-pnu"),
        ("/object_key/manifest_object", "by-pnu"),
        ("/object_key/manifest_history_dir", "by-pnu"),
        ("/section_packs/root", "packs"),
    ] {
        let path = block
            .pointer_mut(pointer)
            .ok_or_else(|| format!("{host}{pointer} is missing"))?;
        let derived = path
            .as_str()
            .and_then(|path| with_kind(path, segment, kind))
            .ok_or_else(|| {
                format!("{host}{pointer} has no {segment} segment to put a kind before")
            })?;
        *path = Value::String(derived);
    }
    let derived = block
        .as_object_mut()
        .ok_or_else(|| format!("{host} is not an object"))?;
    derived.insert("pack_only".to_owned(), json!(true));
    // A kind's document is `.v1` until it gains, loses or renames a field; its line then states the
    // later version of the same name and why (contracts/by-pnu-documents holds every version).
    let document = format!("foundation-platform.{snake}_by_pnu.v");
    let version = match fields.get("document_schema_version") {
        None => format!("{document}1"),
        Some(stated) => stated
            .as_str()
            .filter(|stated| {
                stated
                    .strip_prefix(document.as_str())
                    .and_then(|number| number.parse::<u32>().ok())
                    .is_some_and(|number| number > 1)
                    && fields
                        .get("document_schema_version_reason")
                        .and_then(Value::as_str)
                        .is_some_and(|reason| !reason.trim().is_empty())
            })
            .ok_or_else(|| {
                format!(
                    "by_pnu_lanes.{name}.document_schema_version must be {document}<n> with n > 1, \
                     beside a document_schema_version_reason"
                )
            })?
            .to_owned(),
    };
    derived.insert("document_schema_version".to_owned(), json!(version));
    derived.insert("gold_table".to_owned(), json!(format!("gold.{snake}")));
    derived.insert("latency_baseline_lane".to_owned(), json!(host_lane));
    derived.insert("documents_follow_lane".to_owned(), json!(host_lane));
    derived.insert(
        "latency_p95_max_ms".to_owned(),
        rule.get("latency_p95_max_ms")
            .cloned()
            .ok_or("by_pnu_lane_rule names no latency_p95_max_ms")?,
    );
    if let Some(Value::Object(request_path)) = derived.get_mut("request_path") {
        request_path.insert("accepted_queries".to_owned(), json!([]));
        request_path.remove("accepted_queries_reason");
    }
    if let Some(Value::Object(packs)) = derived.get_mut("section_packs") {
        packs.insert("sections".to_owned(), json!(["documents"]));
        packs.insert("anchor_section".to_owned(), json!("documents"));
        packs.remove("sections_reason");
        if let Some(Value::Object(bake)) = packs.get_mut("scheduled_bake") {
            bake.insert("capability".to_owned(), json!(KIND_LANE_BAKE_CAPABILITY));
        }
    }
    for (field, value) in fields {
        if !["host", "kind", "section", "columns"].contains(&field.as_str()) {
            derived.insert(field.clone(), value.clone());
        }
    }
    Ok(block)
}

/// `path` with `{kind}/` inserted before its last `segment` segment.
fn with_kind(path: &str, segment: &str, kind: &str) -> Option<String> {
    let mut segments = path.split('/').collect::<Vec<_>>();
    let at = segments.iter().rposition(|part| *part == segment)?;
    segments.insert(at, kind);
    Some(segments.join("/"))
}

/// Lowercase words of letters and digits, each starting with a letter, joined by `-`.
fn lane_grammar(name: &str) -> bool {
    name.split('-').all(|word| {
        word.bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
            && word
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    })
}

/// One `by_pnu_lanes` entry as the contract writes it: a gateway block like the parcel and
/// building lanes', and what makes it a lane of its own Gold panel.
#[derive(Debug, Deserialize)]
struct PackLaneEntry {
    /// Always true: a contract lane has no object lane, so its first publish is the pack-only
    /// cut-over gate.
    pack_only: bool,
    gold_table: String,
    document_schema_version: String,
    /// The live lane whose preview serves the same PNUs: the pack-only gate (나) measures this
    /// lane's preview against it.
    latency_baseline_lane: String,
    /// Gate (나)'s absolute bound on this lane's own cold and warm p95: the baseline's documents
    /// are not this lane's, so the increase over them alone is not the bound.
    latency_p95_max_ms: f64,
    /// A lane whose document is narrower than a PNU (root ADR-0204): the Gold column whose value
    /// follows the PNU in the document key.
    #[serde(default)]
    document_key_suffix: Option<KeySuffixEntry>,
    #[serde(flatten)]
    gateway: ByPnuGatewayPolicy,
}

#[derive(Debug, Deserialize)]
struct KeySuffixEntry {
    column: String,
    pattern: String,
    reason: String,
}

/// What follows the PNU in a lane's document key (root ADR-0204): a Gold column's value, of the
/// contract's pattern. The key is `<pnu><suffix>`; the request path names it `<pnu>/<suffix>`.
#[derive(Debug)]
pub struct KeySuffix {
    pub column: &'static str,
    pub pattern: regex::Regex,
}

impl KeySuffix {
    /// Whether `suffix` is one of this lane's suffixes.
    #[must_use]
    pub fn admits(&self, suffix: &str) -> bool {
        !suffix.contains('/') && self.pattern.is_match(suffix)
    }
}

/// A lane the contract names, with what follows from its name resolved once.
#[derive(Debug)]
pub struct PackLane {
    name: String,
    unit: String,
    noun: String,
    env_prefix: String,
    /// The Gold panel the lane bakes: one row per PNU, every content column a `*_json` field.
    pub gold: &'static LakehouseTableContract,
    pub document_schema_version: String,
    pub latency_baseline_lane: String,
    pub latency_p95_max_ms: f64,
    /// The document key's suffix, for a lane keyed narrower than a PNU (root ADR-0204).
    pub key_suffix: Option<KeySuffix>,
    gateway: ByPnuGatewayPolicy,
}

/// The Gold columns a pack lane's document does not carry: its key and its lineage.
pub const PACK_LANE_NON_CONTENT_COLUMNS: [&str; 4] = [
    "pnu",
    "row_digest",
    "source_snapshot_id",
    "published_at_utc",
];

/// Resolves one entry, refusing a name, a baseline, a Gold table or a document the pack-only
/// machinery cannot serve.
fn pack_lane(name: &str, entry: PackLaneEntry) -> Result<PackLane, String> {
    if !lane_grammar(name) || ["parcel", "building"].contains(&name) {
        return Err(format!(
            "by_pnu_lanes names a lane {name:?} outside the lane grammar"
        ));
    }
    if !entry.pack_only {
        return Err(format!(
            "by_pnu_lanes.{name} must be pack_only: it has no object lane"
        ));
    }
    if !["parcel", "building"].contains(&entry.latency_baseline_lane.as_str()) {
        return Err(format!(
            "by_pnu_lanes.{name}.latency_baseline_lane must be a lane with a live preview, not {:?}",
            entry.latency_baseline_lane
        ));
    }
    if !(entry.latency_p95_max_ms.is_finite() && entry.latency_p95_max_ms > 0.0) {
        return Err(format!(
            "by_pnu_lanes.{name}.latency_p95_max_ms must be a positive bound"
        ));
    }
    let gold = industrial_complex_lakehouse_contract_by_table_name(&entry.gold_table).ok_or_else(
        || {
            format!(
                "by_pnu_lanes.{name}.gold_table {} is no lakehouse contract",
                entry.gold_table
            )
        },
    )?;
    let columns = gold
        .columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let content = columns
        .iter()
        .filter(|column| !PACK_LANE_NON_CONTENT_COLUMNS.contains(column))
        .collect::<Vec<_>>();
    if !PACK_LANE_NON_CONTENT_COLUMNS
        .iter()
        .all(|column| columns.contains(column))
        || content.is_empty()
        || !content.iter().any(|column| column.ends_with("_json"))
    {
        return Err(format!(
            "{} is not a by-PNU panel: a pack-only document carries every column but \
             {PACK_LANE_NON_CONTENT_COLUMNS:?}, at least one a *_json field",
            entry.gold_table
        ));
    }
    if entry.document_schema_version.trim().is_empty() || entry.gateway.section_packs.is_none() {
        return Err(format!(
            "by_pnu_lanes.{name} names no document schema or no section packs"
        ));
    }
    let key_suffix = match entry.document_key_suffix {
        None => None,
        Some(suffix) => {
            let column = gold
                .columns
                .iter()
                .map(|column| column.name)
                .find(|column| *column == suffix.column)
                .filter(|column| {
                    !column.ends_with("_json") && !PACK_LANE_NON_CONTENT_COLUMNS.contains(column)
                })
                .ok_or_else(|| {
                    format!(
                        "by_pnu_lanes.{name}.document_key_suffix.column {:?} is no plain content \
                         column of {}",
                        suffix.column, entry.gold_table
                    )
                })?;
            if suffix.reason.trim().is_empty() {
                return Err(format!(
                    "by_pnu_lanes.{name}.document_key_suffix names no reason"
                ));
            }
            let pattern =
                regex::Regex::new(&format!("^(?:{})$", suffix.pattern)).map_err(|error| {
                    format!("by_pnu_lanes.{name}.document_key_suffix.pattern: {error}")
                })?;
            Some(KeySuffix { column, pattern })
        }
    };
    Ok(PackLane {
        name: name.to_owned(),
        unit: format!("{name}-by-pnu"),
        noun: name.replace('-', " "),
        env_prefix: format!(
            "FOUNDATION_PLATFORM_{}_BY_PNU_SERVING",
            name.replace('-', "_").to_ascii_uppercase()
        ),
        gold,
        document_schema_version: entry.document_schema_version,
        latency_baseline_lane: entry.latency_baseline_lane,
        latency_p95_max_ms: entry.latency_p95_max_ms,
        key_suffix,
        gateway: entry.gateway,
    })
}

#[derive(Debug, Deserialize)]
pub struct ByPnuGatewayPolicy {
    /// The live Worker's script name, which a canary step asks Workers analytics about.
    pub worker_name: String,
    pub public_hostname: String,
    pub object_key: ByPnuObjectKeyPolicy,
    pub request_path: ByPnuRequestPath,
    pub content_type: String,
    pub cache_control: String,
    pub manifest_cache_control: String,
    /// The response header in which the lane's Worker names the version that produced each
    /// answer (root ADR-0157); a lane with a canary must name one.
    #[serde(default)]
    pub version_header: Option<String>,
    /// The lane's section packs (root ADR-0147); only the lanes that serve from packs name one.
    #[serde(default)]
    pub section_packs: Option<LaneSectionPacks>,
    /// The lane answers from section packs or not at all (root ADR-0190, ADR-0274): its Worker
    /// has no object path, so no publish may leave its manifest without packs.
    #[serde(default)]
    pub pack_only: bool,
}

/// One lane's section packs: where they live and which sections a document is split into.
#[derive(Debug, Deserialize)]
pub struct LaneSectionPacks {
    pub root: String,
    /// Every section a document is split into, in the order the manifest lists them.
    pub sections: Vec<String>,
    /// The section whose entry decides whether a PNU answers at all (document, tombstone, absent).
    pub anchor_section: String,
    /// The cut-over gate's preview Worker: its own name and hostname, never a live route.
    pub preview_worker: Option<PreviewWorker>,
    /// The live Worker's gradual rollout of the pack path.
    pub canary: Option<CanaryPolicy>,
    /// The scheduled check of the live hostname after the cut-over.
    pub monitor: Option<MonitorPolicy>,
    /// The scheduled job that keeps the packs current, and the capability its job entry must
    /// declare before the first pack publish (root ADR-0147, runbook 7절).
    pub scheduled_bake: ScheduledPackBake,
}

/// One gradual deployment step's hold before its health is judged, and the traffic it needs. The
/// step percentages are read by `scripts/ops/by-pnu-gateway-canary.sh`.
#[derive(Debug, Deserialize)]
pub struct CanaryPolicy {
    pub hold_seconds: u64,
    pub min_requests_per_step: u64,
    /// The least share of the new version's pinned answers Workers analytics must have counted
    /// before its CPU, cut-offs and errors are judged (root ADR-0157): analytics is sampled and
    /// late, so it never decides whether the reads reached the version.
    pub analytics_min_coverage: f64,
    /// The reads a step sends to each of its two versions (pinned by version override), so a step
    /// is judged on enough requests whatever the live traffic is.
    pub synthetic_load: LoadTestPolicy,
}

/// The hourly synthetic read of the live hostname.
#[derive(Debug, Deserialize)]
pub struct MonitorPolicy {
    pub pnus: usize,
    pub latency_p95_max_ms: f64,
}

/// The Worker the latency probe reads the unpublished generation through (root ADR-0147 §6).
#[derive(Debug, Deserialize)]
pub struct PreviewWorker {
    /// The Workers script name, which the probe asks Workers analytics about.
    pub worker_name: String,
    /// Its hostname: a pack-only lane's probe reads its baseline lane's preview there.
    #[serde(default)]
    pub public_hostname: Option<String>,
}

/// The job (`orchestration/jobs.v1.json`) that bakes a lane's daily changes as pack patches.
#[derive(Debug, Deserialize)]
pub struct ScheduledPackBake {
    pub job: String,
    pub capability: String,
}

/// The pack format and the cut-over gate shared by every lane (root ADR-0147 §2, §6).
#[derive(Debug, Deserialize)]
pub struct SectionPackPolicy {
    pub magic: String,
    pub format_version: u16,
    /// The format of a pack whose keys are longer than a PNU (root ADR-0204): its header states
    /// `key_length`.
    pub wide_key_format_version: u16,
    /// The longest key a pack index holds.
    pub max_key_bytes: usize,
    /// The `schema_version` of the manifest's `section_packs` block.
    pub manifest_section_packs_schema_version: u32,
    /// The block's `schema_version` when a section names a parts index (root ADR-0163).
    pub manifest_section_packs_parted_schema_version: u32,
    /// How a large dong is cut into parts read whole (root ADR-0163).
    pub parts: PackPartsPolicy,
    /// What the gateway reads in one GET (root ADR-0147 Revision).
    pub read_path: PackReadPathPolicy,
    /// Digits of the PNU one pack covers: the legal dong.
    pub unit_prefix_length: usize,
    pub compression: String,
    pub suffix: String,
    pub generation_dir_pattern: String,
    pub patch_dir_pattern: String,
    pub content_type: String,
    pub cache_control: String,
    pub max_index_entries: usize,
    /// Days a superseded generation is kept after its successor went live (root ADR-0242).
    pub superseded_generation_keep_days: i64,
    /// The oldest a lane's served manifest may be (its `published_at_utc`) before the hourly
    /// monitor fails (root ADR-0306): what users get, not what a job reported.
    pub freshness_budget_hours: f64,
    pub preview_query_parameter: String,
    /// Where the gate's Cloudflare analytics credentials come from.
    pub cloudflare_analytics: CloudflareAnalyticsPolicy,
    pub cutover_gate: CutoverGatePolicy,
}

/// Parts of a large dong (root ADR-0163): a dong of total size S is cut into
/// `ceil(S / part_target_bytes)` parts, a PNU into part `fnv1a32(pnu) mod K`.
#[derive(Debug, Deserialize)]
pub struct PackPartsPolicy {
    pub part_target_bytes: u64,
    pub hash: String,
    pub hash_test_vectors: Vec<HashTestVector>,
    pub unit_pattern: String,
    pub index_file_name: String,
    pub index_schema_version: String,
}

/// One input and its `fnv1a32`, which the publisher and the Worker are both held to.
#[derive(Debug, Deserialize)]
pub struct HashTestVector {
    pub input: String,
    pub fnv1a32: u32,
}

/// The gateway's read bounds the bake must respect.
#[derive(Debug, Deserialize)]
pub struct PackReadPathPolicy {
    /// The bound a part of a dong cut into parts is held to (root ADR-0163); since root ADR-0233
    /// only a patch over a generation baked in parts is.
    pub whole_pack_max_bytes: u64,
    pub request_cpu_budget: RequestCpuBudget,
    /// The response header in which every 503 names its class (manifest-unavailable,
    /// r2-unavailable, ...): a monitor or canary failure records it beside the status.
    pub outage_header: String,
}

/// The bytes one request copies, bounded so its Worker CPU does not grow with the data (root
/// ADR-0233): every pack writer refuses a document or a head past them.
#[derive(Debug, Deserialize)]
pub struct RequestCpuBudget {
    /// One document's gzip member, the most one answer copies.
    pub document_max_bytes: u64,
    /// A pack's prefix, header and index, what the Worker reads and keeps of every pack.
    pub pack_head_max_bytes: u64,
}

/// The environment variables that carry the analytics account and its read-only token. The file
/// that holds them is not named here: `config/runtime-secrets.contract.json` owns every host
/// environment file (root ADR-0153), see [`CloudflareAnalyticsPolicy::env_file`].
#[derive(Debug, Deserialize)]
pub struct CloudflareAnalyticsPolicy {
    pub token_scope: String,
    pub account_id_env: String,
    pub api_token_env: String,
    /// The zone of the live hostname, whose client responses a canary step counts.
    pub zone_id_env: String,
}

/// What the first publish of section packs must be shown (root ADR-0147 §6).
#[derive(Debug, Deserialize)]
pub struct CutoverGatePolicy {
    pub evidence_schema_version: String,
    pub latency_sample_size: usize,
    /// Seeds the sample the live check and the latency probe share, so it is the same every run.
    pub sample_seed: String,
    /// How many PNUs per million a bake records as sample candidates; above the sample size.
    pub sample_candidates_per_million: u64,
    /// How many PNUs the latency probe reads at once; each read is still timed on its own.
    pub probe_concurrency: usize,
    /// The most CPU the Worker may spend on a request at p99, from Workers analytics for the probe
    /// window or the canary step; the Worker's own `cpu_limit_ms` is above it.
    pub worker_cpu_p99_max_ms: f64,
    /// The most a canary step's new version may add to the old version's CPU p99 (root ADR-0157).
    pub worker_cpu_p99_max_increase_ms: f64,
    /// How many sample PNUs the probe also reads from the preview without gzip (the Worker's
    /// decompressing path); every one must answer 200, uncompressed, with the same content.
    pub no_gzip_sample_size: usize,
    /// What the pack path must keep, asserted on the probe and the load phase alike.
    pub slo: ServingSlo,
    /// The load phase the probe runs against the preview after the paired reads.
    pub load_test: LoadTestPolicy,
    /// Owner decisions that accept one refused latency verdict, each pinned to its evidence file
    /// (root ADR-0162).
    #[serde(default)]
    pub latency_waivers: Vec<LatencyWaiver>,
}

/// An owner's acceptance of one measured latency verdict the gate refused (root ADR-0162).
#[derive(Debug, Deserialize)]
pub struct LatencyWaiver {
    /// The lane's unit (`parcel-by-pnu`, `building-by-pnu`).
    pub lane: String,
    pub generation: u64,
    /// The sha256 of the latency evidence file the decision was made on; another measurement,
    /// even of the same generation, is not waived.
    pub latency_evidence_sha256: String,
    pub decided_on: String,
    pub reason: String,
}

/// The pack path's service level objectives against the object path it replaces.
#[derive(Debug, Deserialize)]
pub struct ServingSlo {
    /// The least share of reads that answer 200.
    pub availability_min: f64,
    pub latency_max_increase_ms: ColdAndWarm,
    /// The most of the cold reads the preview may have answered without reading R2 (its edge copy,
    /// isolate memory, or no `Server-Timing` at all): above it the cold bound measured a cache.
    pub cold_reads_not_from_r2_max_share: f64,
}

/// A bound for the cold reads (first of their legal dong) and one for the warm reads.
#[derive(Debug, Deserialize)]
pub struct ColdAndWarm {
    pub cold: LatencyBound,
    pub warm: LatencyBound,
}

/// The most the pack path may add to the object path, per percentile.
#[derive(Debug, Deserialize)]
pub struct LatencyBound {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

/// A paced load against the preview: this many requests a second, this long, at most this many
/// in flight.
#[derive(Debug, Deserialize)]
pub struct LoadTestPolicy {
    pub requests_per_second: u32,
    pub duration_seconds: u64,
    pub max_in_flight: usize,
}

#[derive(Debug, Deserialize)]
pub struct ByPnuObjectKeyPolicy {
    pub root: String,
    pub generation_dir_pattern: String,
    pub patch_dir_pattern: String,
    pub pnu_pattern: String,
    pub suffix: String,
    pub manifest_object: String,
    pub manifest_history_dir: String,
}

#[derive(Debug, Deserialize)]
pub struct ByPnuRequestPath {
    pub prefix: String,
    pub capabilities: String,
}

/// How patch generations are bounded (root ADR-0141 §5). `max_patches` and
/// `max_cumulative_change_ratio` choose between a patch and a full bake; the bake script reads
/// the same block.
///
/// `max_patches` and `pnu_prefix_length` bind only a manifest being **written**. A reader (the
/// gateway Workers, the publisher reading the live manifest, the state report) accepts up to
/// `manifest_patch_ceiling` patches and the prefix length the manifest itself declares, so
/// tuning either value never takes a live lane down or blocks the full bake that repairs it.
#[derive(Debug, Deserialize)]
pub struct ByPnuServingPatchPolicy {
    pub manifest_schema_version: u32,
    pub max_patches: usize,
    /// The most patches any reader accepts. Not a tuning knob: it only ever rises.
    pub manifest_patch_ceiling: usize,
    pub max_cumulative_change_ratio: f64,
    /// Where a patch stops being cheaper than a full bake, measured (root ADR-0274 §2); the bound
    /// may not exceed it.
    pub cumulative_change_break_even_ratio: f64,
    /// The measurement behind both numbers.
    pub max_cumulative_change_ratio_reason: String,
    pub max_delta_fraction: f64,
    pub pnu_prefix_length: usize,
    pub tombstone_schema_version: String,
    pub tombstone_max_bytes: usize,
}

/// Refuses patch bounds that cannot all hold, a cumulative bound above the measured break-even, and a
/// lane of more than one section while the break-even is the one-section measurement (root ADR-0274
/// §2). `section_counts` is each lane's (name, number of sections).
fn check_patch_bounds(
    patches: &ByPnuServingPatchPolicy,
    section_counts: &[(&str, usize)],
) -> Result<(), String> {
    if patches.max_patches == 0
        || patches.max_patches > patches.manifest_patch_ceiling
        || !(patches.max_cumulative_change_ratio > 0.0
            && patches.max_cumulative_change_ratio <= patches.max_delta_fraction
            && patches.max_delta_fraction <= 1.0)
        || !PNU_PREFIX_LENGTHS.contains(&patches.pnu_prefix_length)
    {
        return Err("by_pnu_serving_patches holds bounds that cannot all hold".to_owned());
    }
    if patches.max_cumulative_change_ratio_reason.trim().is_empty() {
        return Err(
            "by_pnu_serving_patches.max_cumulative_change_ratio needs its reason".to_owned(),
        );
    }
    if !(patches.max_cumulative_change_ratio <= patches.cumulative_change_break_even_ratio
        && patches.cumulative_change_break_even_ratio <= 1.0)
    {
        return Err(format!(
            "by_pnu_serving_patches.max_cumulative_change_ratio {} is above the measured break-even {}: \
             past it a patch costs more than the full bake it avoids (root ADR-0274 §2)",
            patches.max_cumulative_change_ratio, patches.cumulative_change_break_even_ratio
        ));
    }
    if let Some((lane, sections)) = section_counts.iter().find(|(_, sections)| *sections > 1) {
        return Err(format!(
            "lane {lane} serves {sections} sections, but the patch break-even was measured on lanes \
             of one section (root ADR-0274 §2); measure it before such a lane bakes"
        ));
    }
    Ok(())
}

/// Bytes of a section pack before its header (root ADR-0147 §2): magic, format version, reserved,
/// header length, index length. The pack module lays them out; the contract check needs the size.
pub const PACK_PREFIX_BYTES: usize = 20;

/// The prefix lengths a PNU (19 digits) can have.
pub const PNU_PREFIX_LENGTHS: std::ops::RangeInclusive<usize> = 1..=19;

fn contract() -> anyhow::Result<&'static R2ConnectionContract> {
    static CONTRACT: OnceLock<Result<R2ConnectionContract, String>> = OnceLock::new();
    CONTRACT
        .get_or_init(|| {
            let mut contract: R2ConnectionContract =
                serde_json::from_str(R2_CONNECTION_CONTRACT)
                    .map_err(|error| format!("invalid R2 connection contract: {error}"))?;
            let written: serde_json::Value = serde_json::from_str(R2_CONNECTION_CONTRACT)
                .map_err(|error| format!("invalid R2 connection contract: {error}"))?;
            for (name, entry) in std::mem::take(&mut contract.lane_entries) {
                let entry: PackLaneEntry =
                    serde_json::from_value(resolve_lane_entry(&name, &entry, &written)?)
                        .map_err(|error| format!("by_pnu_lanes.{name}: {error}"))?;
                let lane = pack_lane(&name, entry)?;
                contract.by_pnu_lanes.insert(name, lane);
            }
            if contract.schema_version != 2 {
                return Err(format!(
                    "R2 connection contract schema must be 2, got {}",
                    contract.schema_version
                ));
            }
            let mut sections = Vec::new();
            for (name, gateway) in [
                ("parcel", &contract.parcel_by_pnu_gateway),
                ("building", &contract.building_by_pnu_gateway),
            ] {
                if let Some(packs) = &gateway.section_packs {
                    sections.push((name, packs.sections.len()));
                }
            }
            for (name, lane) in &contract.by_pnu_lanes {
                if let Some(packs) = &lane.gateway.section_packs {
                    sections.push((name.as_str(), packs.sections.len()));
                }
            }
            check_patch_bounds(&contract.by_pnu_serving_patches, &sections)?;
            check_section_packs(&contract)?;
            Ok(contract)
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
}

/// Refuses a pack block whose parts cannot all hold: the format is read by two languages, so a
/// value either side cannot honour is a contract error, not a runtime surprise.
fn check_section_packs(contract: &R2ConnectionContract) -> Result<(), String> {
    let packs = &contract.by_pnu_section_packs;
    let gate = &packs.cutover_gate;
    if packs.magic.len() != 8
        || packs.format_version == 0
        || packs.wide_key_format_version == 0
        || packs.wide_key_format_version == packs.format_version
        || !(20..=255).contains(&packs.max_key_bytes)
        || packs.compression != "gzip"
        || !(1..=19).contains(&packs.unit_prefix_length)
        || packs.max_index_entries == 0
        || gate.latency_sample_size == 0
        || gate.sample_seed.is_empty()
        || !(1..=1_000_000).contains(&gate.sample_candidates_per_million)
        || gate.probe_concurrency == 0
        || gate.worker_cpu_p99_max_ms.is_nan()
        || gate.worker_cpu_p99_max_ms <= 0.0
        || gate.worker_cpu_p99_max_increase_ms.is_nan()
        || gate.worker_cpu_p99_max_increase_ms < 0.0
        || !(gate.slo.availability_min > 0.0 && gate.slo.availability_min <= 1.0)
        || !(0.0..1.0).contains(&gate.slo.cold_reads_not_from_r2_max_share)
        || [
            &gate.slo.latency_max_increase_ms.cold,
            &gate.slo.latency_max_increase_ms.warm,
        ]
        .iter()
        .any(|bound| !(bound.p50 >= 0.0 && bound.p95 >= 0.0 && bound.p99 >= 0.0))
        || gate.load_test.requests_per_second == 0
        || gate.load_test.duration_seconds == 0
        || gate.load_test.max_in_flight == 0
    {
        return Err("by_pnu_section_packs holds values the pack format cannot honour".to_owned());
    }
    // The parts are computed by two languages (root ADR-0163): the hash must be the one both
    // implement, and the test vectors must hold for the publisher's, here, at every start.
    let parts = &packs.parts;
    if parts.hash != "fnv1a32"
        || parts.hash_test_vectors.is_empty()
        || parts
            .hash_test_vectors
            .iter()
            .any(|vector| crate::r2_layout::by_pnu_packs::fnv1a32(&vector.input) != vector.fnv1a32)
        || parts.part_target_bytes == 0
        || packs.read_path.request_cpu_budget.document_max_bytes == 0
        || packs.read_path.request_cpu_budget.pack_head_max_bytes == 0
        || parts.part_target_bytes > packs.read_path.whole_pack_max_bytes
        || parts.index_file_name.is_empty()
        || parts.index_schema_version.is_empty()
        || packs.manifest_section_packs_parted_schema_version
            <= packs.manifest_section_packs_schema_version
        || regex::Regex::new(&format!("^(?:{})$", parts.unit_pattern)).is_err()
    {
        return Err(
            "by_pnu_section_packs.parts holds values the publisher and the Worker cannot both \
             honour"
                .to_owned(),
        );
    }
    for gateway in [
        &contract.parcel_by_pnu_gateway,
        &contract.building_by_pnu_gateway,
    ]
    .into_iter()
    .chain(contract.by_pnu_lanes.values().map(|lane| &lane.gateway))
    {
        let Some(lane) = &gateway.section_packs else {
            continue;
        };
        let mut seen = std::collections::BTreeSet::new();
        let named = lane.sections.iter().all(|section| {
            !section.is_empty()
                && section
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
                && seen.insert(section.as_str())
        });
        let synthetic_starved = lane.canary.as_ref().is_some_and(|canary| {
            let load = &canary.synthetic_load;
            load.requests_per_second == 0
                || load.duration_seconds == 0
                || load.max_in_flight == 0
                // The pinned reads alone must reach the step's minimum on the new version.
                || u64::from(load.requests_per_second) * load.duration_seconds
                    < canary.min_requests_per_step
                || canary.hold_seconds < load.duration_seconds
                || !(canary.analytics_min_coverage > 0.0 && canary.analytics_min_coverage <= 1.0)
        });
        // A step counts its pinned reads by the version each answer names.
        if lane.canary.is_some() && gateway.version_header.as_deref().is_none_or(str::is_empty) {
            return Err(format!(
                "{} has a canary and names no version_header its answers carry",
                gateway.worker_name
            ));
        }
        if !named
            || synthetic_starved
            || !seen.contains(lane.anchor_section.as_str())
            || lane.root.ends_with('/')
            || lane.scheduled_bake.job.is_empty()
            || lane.scheduled_bake.capability.is_empty()
        {
            return Err(format!(
                "section_packs under {} must name distinct lowercase sections including its anchor",
                lane.root
            ));
        }
    }
    Ok(())
}

impl CloudflareAnalyticsPolicy {
    /// The root-only file a unit's `EnvironmentFile` reads the variables from, as the
    /// runtime-secrets contract names it (its `cloudflare-analytics` group).
    ///
    /// # Errors
    /// Returns an error when that contract cannot be read or has no such group.
    pub fn env_file(&self) -> anyhow::Result<&'static str> {
        runtime_secret_group_path(CLOUDFLARE_ANALYTICS_GROUP)
    }
}

#[derive(Debug, Deserialize)]
struct RuntimeSecretsContract {
    groups: Vec<RuntimeSecretGroup>,
}

#[derive(Debug, Deserialize)]
struct RuntimeSecretGroup {
    name: String,
    path: String,
}

/// The host path of one runtime-secrets group (root ADR-0153: that contract is the one source of
/// host environment files).
fn runtime_secret_group_path(group: &str) -> anyhow::Result<&'static str> {
    static CONTRACT: OnceLock<Result<RuntimeSecretsContract, String>> = OnceLock::new();
    let contract = CONTRACT
        .get_or_init(|| {
            serde_json::from_str(RUNTIME_SECRETS_CONTRACT)
                .map_err(|error| format!("invalid runtime-secrets contract: {error}"))
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    contract
        .groups
        .iter()
        .find(|entry| entry.name == group)
        .map(|entry| entry.path.as_str())
        .ok_or_else(|| anyhow::anyhow!("the runtime-secrets contract has no group {group:?}"))
}

/// The pack format and cut-over gate.
pub fn section_pack_policy() -> anyhow::Result<&'static SectionPackPolicy> {
    Ok(&contract()?.by_pnu_section_packs)
}

/// The patch bounds shared by both lanes.
pub fn by_pnu_serving_patch_policy() -> anyhow::Result<&'static ByPnuServingPatchPolicy> {
    Ok(&contract()?.by_pnu_serving_patches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context as _;
    use serde_json::{json, Value};

    fn written() -> anyhow::Result<Value> {
        Ok(serde_json::from_str(R2_CONNECTION_CONTRACT)?)
    }

    /// The contract's patch block with `change` applied.
    fn patch_bounds(change: impl FnOnce(&mut Value)) -> anyhow::Result<ByPnuServingPatchPolicy> {
        let mut block = written()?["by_pnu_serving_patches"].clone();
        change(&mut block);
        Ok(serde_json::from_value(block)?)
    }

    #[test]
    fn the_cumulative_bound_stays_under_the_measured_break_even_on_one_section_lanes(
    ) -> anyhow::Result<()> {
        // Root ADR-0274 §2: the contract as written holds, with the lanes it names.
        assert!(by_pnu_serving_patch_policy().is_ok());
        let one_section = [("parcel", 1), ("building", 1)];
        check_patch_bounds(&patch_bounds(|_| {})?, &one_section).map_err(anyhow::Error::msg)?;
        // A bound above the break-even is a patch that costs more than the full bake it avoids.
        let above = patch_bounds(|block| {
            block["max_cumulative_change_ratio"] = json!(
                block["cumulative_change_break_even_ratio"]
                    .as_f64()
                    .unwrap_or(1.0)
                    + 0.01
            );
        })?;
        let refused = check_patch_bounds(&above, &one_section)
            .err()
            .unwrap_or_default();
        assert!(refused.contains("break-even"), "{refused}");
        // The break-even is the one-section measurement: a lane of more sections must be measured.
        let refused = check_patch_bounds(&patch_bounds(|_| {})?, &[("parcel", 1), ("building", 4)])
            .err()
            .unwrap_or_default();
        assert!(
            refused.contains("lane building serves 4 sections"),
            "{refused}"
        );
        // And the numbers carry their measurement.
        let reasonless =
            patch_bounds(|block| block["max_cumulative_change_ratio_reason"] = json!(" "))?;
        assert!(check_patch_bounds(&reasonless, &one_section).is_err());
        Ok(())
    }

    #[test]
    fn a_lane_with_a_host_is_its_hosts_block_with_the_kind_in_its_paths() -> anyhow::Result<()> {
        // Root ADR-0193 §4: the contract's vectors hold this rule and the Worker's to one answer.
        let contract = written()?;
        let vectors = contract
            .pointer("/by_pnu_lane_rule/test_vectors")
            .and_then(Value::as_array)
            .context("the contract holds the lane rule's test vectors")?;
        assert!(!vectors.is_empty());
        for vector in vectors {
            let field = |name: &str| vector[name].as_str().map(str::to_owned).unwrap_or_default();
            let entry = json!({"host": vector["host"], "kind": vector["kind"], "section": "s", "columns": ["s_json"]});
            let lane = resolve_lane_entry(&field("lane"), &entry, &contract)
                .map_err(anyhow::Error::msg)?;
            for (pointer, expected) in [
                ("/request_path/prefix", "request_prefix"),
                ("/request_path/capabilities", "capabilities"),
                ("/object_key/manifest_object", "manifest_object"),
                ("/section_packs/root", "pack_root"),
                ("/document_schema_version", "document_schema_version"),
                (
                    "/section_packs/scheduled_bake/capability",
                    "scheduled_bake_capability",
                ),
            ] {
                assert_eq!(lane.pointer(pointer), Some(&vector[expected]), "{pointer}");
            }
            let host = &contract[vector["host"].as_str().unwrap_or_default()];
            // What does not differ per lane is the host's: the same Worker serves it.
            for pointer in [
                "/worker_name",
                "/public_hostname",
                "/section_packs/preview_worker",
            ] {
                assert_eq!(lane.pointer(pointer), host.pointer(pointer), "{pointer}");
            }
            assert_eq!(lane["pack_only"], json!(true));
            assert_eq!(lane["request_path"]["accepted_queries"], json!([]));
            assert_eq!(lane["section_packs"]["sections"], json!(["documents"]));
            // The kind line names nothing else: its table, baseline, bound and follow lane are derived.
            let snake = field("lane").replace('-', "_");
            assert_eq!(lane["gold_table"], json!(format!("gold.{snake}")));
            let host_lane = vector["host"]
                .as_str()
                .unwrap_or_default()
                .trim_end_matches("_by_pnu_gateway");
            assert_eq!(lane["latency_baseline_lane"], json!(host_lane));
            assert_eq!(lane["documents_follow_lane"], json!(host_lane));
            assert_eq!(
                lane["latency_p95_max_ms"],
                contract["by_pnu_lane_rule"]["latency_p95_max_ms"]
            );
            assert!(lane.get("section").is_none() && lane.get("columns").is_none());
        }
        Ok(())
    }

    #[test]
    fn a_lane_with_a_host_that_writes_a_derived_field_or_names_no_host_block_is_refused(
    ) -> anyhow::Result<()> {
        let contract = written()?;
        let lane = json!({"host": "parcel_by_pnu_gateway", "kind": "zoning", "section": "zonings"});
        for (field, value) in [
            ("section_packs", json!({"root": "serving/elsewhere/packs"})),
            ("host", json!("by_pnu_lanes")),
            ("kind", json!("Zoning")),
            ("gold_table", json!("gold.elsewhere")),
        ] {
            let mut entry = lane.clone();
            entry[field] = value;
            assert!(
                resolve_lane_entry("parcel-zoning", &entry, &contract).is_err(),
                "{field}"
            );
        }
        // A stated document version is a later version of the lane's own name, with its reason.
        let reason = json!("a field was added");
        for (version, reason) in [
            ("foundation-platform.parcel_zoning_by_pnu.v2", None),
            ("foundation-platform.parcel_zoning_by_pnu.v1", Some(&reason)),
            ("foundation-platform.parcel_price_by_pnu.v2", Some(&reason)),
            (
                "foundation-platform.parcel_zoning_by_pnu.v2x",
                Some(&reason),
            ),
        ] {
            let mut entry = lane.clone();
            entry["document_schema_version"] = json!(version);
            if let Some(reason) = reason {
                entry["document_schema_version_reason"] = reason.clone();
            }
            assert!(
                resolve_lane_entry("parcel-zoning", &entry, &contract).is_err(),
                "{version}"
            );
        }
        let mut bumped = lane.clone();
        bumped["document_schema_version"] = json!("foundation-platform.parcel_zoning_by_pnu.v2");
        bumped["document_schema_version_reason"] = reason;
        assert_eq!(
            resolve_lane_entry("parcel-zoning", &bumped, &contract).map_err(anyhow::Error::msg)?
                ["document_schema_version"],
            json!("foundation-platform.parcel_zoning_by_pnu.v2")
        );
        // A lane written whole (root ADR-0190) is read as it is written.
        let whole = json!({"pack_only": true, "worker_name": "w"});
        assert_eq!(
            resolve_lane_entry("unit-price", &whole, &contract),
            Ok(whole)
        );
        Ok(())
    }

    #[test]
    fn the_zoning_lane_is_served_by_the_parcel_worker_under_its_own_paths() -> anyhow::Result<()> {
        let lane =
            ByPnuLane::named("parcel-zoning")?.context("the contract names the zoning lane")?;
        let pack = lane
            .pack_lane()
            .context("a contract lane serves packs only")?;
        let policy = lane.policy()?;
        let parcel = ByPnuLane::Parcel.policy()?;
        assert_eq!(policy.worker_name, parcel.worker_name);
        assert_eq!(policy.request_path.prefix, "/parcels/zoning/by-pnu/");
        assert_eq!(lane.section_packs()?.root, "serving/parcels/zoning/packs");
        assert_eq!(pack.gold.table_name, "gold.parcel_zoning");
        assert_eq!(
            pack.document_schema_version,
            "foundation-platform.parcel_zoning_by_pnu.v1"
        );
        // The forest ledger gained ownership_kind_name (#746): its line states v2.
        let forest = ByPnuLane::named("parcel-forest-ledger")?
            .and_then(ByPnuLane::pack_lane)
            .context("the contract names the forest ledger lane")?;
        assert_eq!(
            forest.document_schema_version,
            "foundation-platform.parcel_forest_ledger_by_pnu.v2"
        );
        assert_eq!(lane.unit(), "parcel-zoning-by-pnu");
        Ok(())
    }
}
