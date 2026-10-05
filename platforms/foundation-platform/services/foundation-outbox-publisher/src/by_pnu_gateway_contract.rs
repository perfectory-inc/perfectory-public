//! Typed access to the by-PNU gateway sections of the R2 connection contract.
//!
//! The contract JSON is the SSOT (root ADR-0096 parcels, ADR-0100 buildings, ADR-0141 patches):
//! the exports, the manifest publisher, and the `foundation-parcel-gateway` and
//! `foundation-building-gateway` Workers all read the same blocks, so the key grammar the uploader
//! writes under and the grammar the Workers resolve are one fact. Both lanes share one shape.

use std::sync::OnceLock;

use serde::Deserialize;

const R2_CONNECTION_CONTRACT: &str = include_str!("../../../config/r2-connections.contract.json");

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
    pub(crate) public_hostname: String,
    pub(crate) object_key: ByPnuObjectKeyPolicy,
    pub(crate) request_path: ByPnuRequestPath,
    pub(crate) content_type: String,
    pub(crate) cache_control: String,
    pub(crate) manifest_cache_control: String,
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
    /// The scheduled job that keeps the packs current, and the capability its job entry must
    /// declare before the first pack publish (root ADR-0147, runbook 7절).
    pub(crate) scheduled_bake: ScheduledPackBake,
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

/// The environment variables (from the contract's `env_file`) that carry the analytics account
/// and its read-only token.
#[derive(Debug, Deserialize)]
pub(crate) struct CloudflareAnalyticsPolicy {
    pub(crate) account_id_env: String,
    pub(crate) api_token_env: String,
}

/// What the first publish of section packs must be shown (root ADR-0147 §6).
#[derive(Debug, Deserialize)]
pub(crate) struct CutoverGatePolicy {
    pub(crate) evidence_schema_version: String,
    pub(crate) latency_max_increase_ms: LatencyBound,
    pub(crate) latency_sample_size: usize,
    /// Seeds the sample the live check and the latency probe share, so it is the same every run.
    pub(crate) sample_seed: String,
    /// How many PNUs per million a bake records as sample candidates; above the sample size.
    pub(crate) sample_candidates_per_million: u64,
    /// How many PNUs the latency probe reads at once; each read is still timed on its own.
    pub(crate) probe_concurrency: usize,
    /// The most CPU the preview Worker may spend on a request at p99, from Workers analytics for
    /// the probe window; the account's plan limit is twice this.
    pub(crate) worker_cpu_p99_max_ms: f64,
}

/// The most the cold first read may slow down, per percentile.
#[derive(Debug, Deserialize)]
pub(crate) struct LatencyBound {
    pub(crate) p50: f64,
    pub(crate) p95: f64,
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
        || !(gate.latency_max_increase_ms.p50 >= 0.0 && gate.latency_max_increase_ms.p95 >= 0.0)
    {
        return Err("by_pnu_section_packs holds values the pack format cannot honour".to_owned());
    }
    for lane in [
        &contract.parcel_by_pnu_gateway,
        &contract.building_by_pnu_gateway,
    ] {
        let Some(lane) = &lane.section_packs else {
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
        if !named
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

/// The pack format and cut-over gate.
pub(crate) fn section_pack_policy() -> anyhow::Result<&'static SectionPackPolicy> {
    Ok(&contract()?.by_pnu_section_packs)
}

/// The patch bounds shared by both lanes.
pub(crate) fn by_pnu_serving_patch_policy() -> anyhow::Result<&'static ByPnuServingPatchPolicy> {
    Ok(&contract()?.by_pnu_serving_patches)
}
