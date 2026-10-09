//! Typed access to the by-PNU gateway sections of the R2 connection contract.
//!
//! The contract JSON is the SSOT (root ADR-0096 parcels, ADR-0100 buildings, ADR-0141 patches):
//! the exports, the manifest publisher, and the by-PNU gateway Worker
//! (`services/foundation-by-pnu-gateway`, both lanes) all read the same blocks, so the key grammar the uploader
//! writes under and the grammar the Workers resolve are one fact. Both lanes share one shape.

use std::sync::OnceLock;

use serde::Deserialize;

const R2_CONNECTION_CONTRACT: &str = include_str!("../../../config/r2-connections.contract.json");
const RUNTIME_SECRETS_CONTRACT: &str =
    include_str!("../../../config/runtime-secrets.contract.json");
/// The runtime-secrets group that holds the Cloudflare analytics variables.
const CLOUDFLARE_ANALYTICS_GROUP: &str = "cloudflare-analytics";

/// The two by-PNU serving lanes. Everything a lane differs by is named here once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ByPnuLane {
    Parcel,
    Building,
}

impl ByPnuLane {
    /// The manifest's `unit` and the state's lane name.
    pub(crate) const fn unit(self) -> &'static str {
        match self {
            Self::Parcel => "parcel-by-pnu",
            Self::Building => "building-by-pnu",
        }
    }

    /// The lane a manifest `unit` names.
    pub(crate) fn from_unit(unit: &str) -> Option<Self> {
        [Self::Parcel, Self::Building]
            .into_iter()
            .find(|lane| lane.unit() == unit)
    }

    /// The word error messages use for one object of the lane.
    pub(crate) const fn noun(self) -> &'static str {
        match self {
            Self::Parcel => "parcel",
            Self::Building => "building",
        }
    }

    /// The prefix of every environment variable the lane's commands read.
    pub(crate) const fn env_prefix(self) -> &'static str {
        match self {
            Self::Parcel => "FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING",
            Self::Building => "FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING",
        }
    }

    /// One environment variable of the lane.
    pub(crate) fn env(self, name: &str) -> String {
        format!("{}_{name}", self.env_prefix())
    }

    /// The lane's gateway block.
    pub(crate) fn policy(self) -> anyhow::Result<&'static ByPnuGatewayPolicy> {
        let contract = contract()?;
        Ok(match self {
            Self::Parcel => &contract.parcel_by_pnu_gateway,
            Self::Building => &contract.building_by_pnu_gateway,
        })
    }

    /// The lane's section packs block (root ADR-0147).
    pub(crate) fn section_packs(self) -> anyhow::Result<&'static LaneSectionPacks> {
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
}

#[derive(Debug, Deserialize)]
pub(crate) struct ByPnuGatewayPolicy {
    /// The live Worker's script name, which a canary step asks Workers analytics about.
    pub(crate) worker_name: String,
    pub(crate) public_hostname: String,
    pub(crate) object_key: ByPnuObjectKeyPolicy,
    pub(crate) request_path: ByPnuRequestPath,
    pub(crate) content_type: String,
    pub(crate) cache_control: String,
    pub(crate) manifest_cache_control: String,
    /// The response header in which the lane's Worker names the version that produced each
    /// answer (root ADR-0157); a lane with a canary must name one.
    #[serde(default)]
    pub(crate) version_header: Option<String>,
    /// The lane's section packs (root ADR-0147); only the lanes that serve from packs name one.
    #[serde(default)]
    pub(crate) section_packs: Option<LaneSectionPacks>,
}

/// One lane's section packs: where they live and which sections a document is split into.
#[derive(Debug, Deserialize)]
pub(crate) struct LaneSectionPacks {
    pub(crate) root: String,
    /// Every section a document is split into, in the order the manifest lists them.
    pub(crate) sections: Vec<String>,
    /// The section whose entry decides whether a PNU answers at all (document, tombstone, absent).
    pub(crate) anchor_section: String,
    /// The cut-over gate's preview Worker: its own name and hostname, never a live route.
    pub(crate) preview_worker: Option<PreviewWorker>,
    /// The live Worker's gradual rollout of the pack path.
    pub(crate) canary: Option<CanaryPolicy>,
    /// The scheduled check of the live hostname after the cut-over.
    pub(crate) monitor: Option<MonitorPolicy>,
    /// The scheduled job that keeps the packs current, and the capability its job entry must
    /// declare before the first pack publish (root ADR-0147, runbook 7절).
    pub(crate) scheduled_bake: ScheduledPackBake,
}

/// One gradual deployment step's hold before its health is judged, and the traffic it needs. The
/// step percentages are read by `scripts/ops/by-pnu-gateway-canary.sh`.
#[derive(Debug, Deserialize)]
pub(crate) struct CanaryPolicy {
    pub(crate) hold_seconds: u64,
    pub(crate) min_requests_per_step: u64,
    /// The least share of the new version's pinned answers Workers analytics must have counted
    /// before its CPU, cut-offs and errors are judged (root ADR-0157): analytics is sampled and
    /// late, so it never decides whether the reads reached the version.
    pub(crate) analytics_min_coverage: f64,
    /// The reads a step sends to each of its two versions (pinned by version override), so a step
    /// is judged on enough requests whatever the live traffic is.
    pub(crate) synthetic_load: LoadTestPolicy,
}

/// The hourly synthetic read of the live hostname.
#[derive(Debug, Deserialize)]
pub(crate) struct MonitorPolicy {
    pub(crate) pnus: usize,
    pub(crate) latency_p95_max_ms: f64,
}

/// The Worker the latency probe reads the unpublished generation through (root ADR-0147 §6).
#[derive(Debug, Deserialize)]
pub(crate) struct PreviewWorker {
    /// The Workers script name, which the probe asks Workers analytics about.
    pub(crate) worker_name: String,
}

/// The job (`orchestration/jobs.v1.json`) that bakes a lane's daily changes as pack patches.
#[derive(Debug, Deserialize)]
pub(crate) struct ScheduledPackBake {
    pub(crate) job: String,
    pub(crate) capability: String,
}

/// The pack format and the cut-over gate shared by every lane (root ADR-0147 §2, §6).
#[derive(Debug, Deserialize)]
pub(crate) struct SectionPackPolicy {
    pub(crate) magic: String,
    pub(crate) format_version: u16,
    /// The `schema_version` of the manifest's `section_packs` block.
    pub(crate) manifest_section_packs_schema_version: u32,
    /// The block's `schema_version` when a section names a parts index (root ADR-0163).
    pub(crate) manifest_section_packs_parted_schema_version: u32,
    /// How a large dong is cut into parts read whole (root ADR-0163).
    pub(crate) parts: PackPartsPolicy,
    /// What the gateway reads in one GET (root ADR-0147 Revision).
    pub(crate) read_path: PackReadPathPolicy,
    /// Digits of the PNU one pack covers: the legal dong.
    pub(crate) unit_prefix_length: usize,
    pub(crate) compression: String,
    pub(crate) suffix: String,
    pub(crate) generation_dir_pattern: String,
    pub(crate) patch_dir_pattern: String,
    pub(crate) content_type: String,
    pub(crate) cache_control: String,
    pub(crate) max_index_entries: usize,
    pub(crate) preview_query_parameter: String,
    /// Where the gate's Cloudflare analytics credentials come from.
    pub(crate) cloudflare_analytics: CloudflareAnalyticsPolicy,
    pub(crate) cutover_gate: CutoverGatePolicy,
}

/// Parts of a large dong (root ADR-0163): a dong of total size S is cut into
/// `ceil(S / part_target_bytes)` parts, a PNU into part `fnv1a32(pnu) mod K`.
#[derive(Debug, Deserialize)]
pub(crate) struct PackPartsPolicy {
    pub(crate) part_target_bytes: u64,
    pub(crate) hash: String,
    pub(crate) hash_test_vectors: Vec<HashTestVector>,
    pub(crate) unit_pattern: String,
    pub(crate) index_file_name: String,
    pub(crate) index_schema_version: String,
}

/// One input and its `fnv1a32`, which the publisher and the Worker are both held to.
#[derive(Debug, Deserialize)]
pub(crate) struct HashTestVector {
    pub(crate) input: String,
    pub(crate) fnv1a32: u32,
}

/// The gateway's read bounds the bake must respect.
#[derive(Debug, Deserialize)]
pub(crate) struct PackReadPathPolicy {
    /// A pack at most this long is read whole in one GET; the bake refuses a larger part.
    pub(crate) whole_pack_max_bytes: u64,
}

/// The environment variables that carry the analytics account and its read-only token. The file
/// that holds them is not named here: `config/runtime-secrets.contract.json` owns every host
/// environment file (root ADR-0153), see [`CloudflareAnalyticsPolicy::env_file`].
#[derive(Debug, Deserialize)]
pub(crate) struct CloudflareAnalyticsPolicy {
    pub(crate) token_scope: String,
    pub(crate) account_id_env: String,
    pub(crate) api_token_env: String,
    /// The zone of the live hostname, whose client responses a canary step counts.
    pub(crate) zone_id_env: String,
}

/// What the first publish of section packs must be shown (root ADR-0147 §6).
#[derive(Debug, Deserialize)]
pub(crate) struct CutoverGatePolicy {
    pub(crate) evidence_schema_version: String,
    pub(crate) latency_sample_size: usize,
    /// Seeds the sample the live check and the latency probe share, so it is the same every run.
    pub(crate) sample_seed: String,
    /// How many PNUs per million a bake records as sample candidates; above the sample size.
    pub(crate) sample_candidates_per_million: u64,
    /// How many PNUs the latency probe reads at once; each read is still timed on its own.
    pub(crate) probe_concurrency: usize,
    /// The most CPU the Worker may spend on a request at p99, from Workers analytics for the probe
    /// window or the canary step; the Worker's own `cpu_limit_ms` is above it.
    pub(crate) worker_cpu_p99_max_ms: f64,
    /// The most a canary step's new version may add to the old version's CPU p99 (root ADR-0157).
    pub(crate) worker_cpu_p99_max_increase_ms: f64,
    /// How many sample PNUs the probe also reads from the preview without gzip (the Worker's
    /// decompressing path); every one must answer 200, uncompressed, with the same content.
    pub(crate) no_gzip_sample_size: usize,
    /// What the pack path must keep, asserted on the probe and the load phase alike.
    pub(crate) slo: ServingSlo,
    /// The load phase the probe runs against the preview after the paired reads.
    pub(crate) load_test: LoadTestPolicy,
    /// Owner decisions that accept one refused latency verdict, each pinned to its evidence file
    /// (root ADR-0162).
    #[serde(default)]
    pub(crate) latency_waivers: Vec<LatencyWaiver>,
}

/// An owner's acceptance of one measured latency verdict the gate refused (root ADR-0162).
#[derive(Debug, Deserialize)]
pub(crate) struct LatencyWaiver {
    /// The lane's unit (`parcel-by-pnu`, `building-by-pnu`).
    pub(crate) lane: String,
    pub(crate) generation: u64,
    /// The sha256 of the latency evidence file the decision was made on; another measurement,
    /// even of the same generation, is not waived.
    pub(crate) latency_evidence_sha256: String,
    pub(crate) decided_on: String,
    pub(crate) reason: String,
}

/// The pack path's service level objectives against the object path it replaces.
#[derive(Debug, Deserialize)]
pub(crate) struct ServingSlo {
    /// The least share of reads that answer 200.
    pub(crate) availability_min: f64,
    pub(crate) latency_max_increase_ms: ColdAndWarm,
    /// The most of the cold reads the preview may have answered without reading R2 (its edge copy,
    /// isolate memory, or no `Server-Timing` at all): above it the cold bound measured a cache.
    pub(crate) cold_reads_not_from_r2_max_share: f64,
}

/// A bound for the cold reads (first of their legal dong) and one for the warm reads.
#[derive(Debug, Deserialize)]
pub(crate) struct ColdAndWarm {
    pub(crate) cold: LatencyBound,
    pub(crate) warm: LatencyBound,
}

/// The most the pack path may add to the object path, per percentile.
#[derive(Debug, Deserialize)]
pub(crate) struct LatencyBound {
    pub(crate) p50: f64,
    pub(crate) p95: f64,
    pub(crate) p99: f64,
}

/// A paced load against the preview: this many requests a second, this long, at most this many
/// in flight.
#[derive(Debug, Deserialize)]
pub(crate) struct LoadTestPolicy {
    pub(crate) requests_per_second: u32,
    pub(crate) duration_seconds: u64,
    pub(crate) max_in_flight: usize,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ByPnuObjectKeyPolicy {
    pub(crate) root: String,
    pub(crate) generation_dir_pattern: String,
    pub(crate) patch_dir_pattern: String,
    pub(crate) pnu_pattern: String,
    pub(crate) suffix: String,
    pub(crate) manifest_object: String,
    pub(crate) manifest_history_dir: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ByPnuRequestPath {
    pub(crate) prefix: String,
    pub(crate) capabilities: String,
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
pub(crate) struct ByPnuServingPatchPolicy {
    pub(crate) manifest_schema_version: u32,
    pub(crate) max_patches: usize,
    /// The most patches any reader accepts. Not a tuning knob: it only ever rises.
    pub(crate) manifest_patch_ceiling: usize,
    pub(crate) max_cumulative_change_ratio: f64,
    pub(crate) max_delta_fraction: f64,
    pub(crate) pnu_prefix_length: usize,
    pub(crate) tombstone_schema_version: String,
    pub(crate) tombstone_max_bytes: usize,
}

/// Bytes of a section pack before its header (root ADR-0147 §2): magic, format version, reserved,
/// header length, index length. The pack module lays them out; the contract check needs the size.
pub(crate) const PACK_PREFIX_BYTES: usize = 20;

/// The prefix lengths a PNU (19 digits) can have.
pub(crate) const PNU_PREFIX_LENGTHS: std::ops::RangeInclusive<usize> = 1..=19;

fn contract() -> anyhow::Result<&'static R2ConnectionContract> {
    static CONTRACT: OnceLock<Result<R2ConnectionContract, String>> = OnceLock::new();
    CONTRACT
        .get_or_init(|| {
            let contract: R2ConnectionContract = serde_json::from_str(R2_CONNECTION_CONTRACT)
                .map_err(|error| format!("invalid R2 connection contract: {error}"))?;
            if contract.schema_version != 2 {
                return Err(format!(
                    "R2 connection contract schema must be 2, got {}",
                    contract.schema_version
                ));
            }
            let patches = &contract.by_pnu_serving_patches;
            if patches.max_patches == 0
                || patches.max_patches > patches.manifest_patch_ceiling
                || !(patches.max_cumulative_change_ratio > 0.0
                    && patches.max_cumulative_change_ratio <= patches.max_delta_fraction
                    && patches.max_delta_fraction <= 1.0)
                || !PNU_PREFIX_LENGTHS.contains(&patches.pnu_prefix_length)
            {
                return Err("by_pnu_serving_patches holds bounds that cannot all hold".to_owned());
            }
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
    ] {
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
    pub(crate) fn env_file(&self) -> anyhow::Result<&'static str> {
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
pub(crate) fn section_pack_policy() -> anyhow::Result<&'static SectionPackPolicy> {
    Ok(&contract()?.by_pnu_section_packs)
}

/// The patch bounds shared by both lanes.
pub(crate) fn by_pnu_serving_patch_policy() -> anyhow::Result<&'static ByPnuServingPatchPolicy> {
    Ok(&contract()?.by_pnu_serving_patches)
}
