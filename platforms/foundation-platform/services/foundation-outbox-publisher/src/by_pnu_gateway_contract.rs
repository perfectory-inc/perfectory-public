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
}

#[derive(Debug, Deserialize)]
struct R2ConnectionContract {
    schema_version: u64,
    parcel_by_pnu_gateway: ByPnuGatewayPolicy,
    building_by_pnu_gateway: ByPnuGatewayPolicy,
    by_pnu_serving_patches: ByPnuServingPatchPolicy,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ByPnuGatewayPolicy {
    pub(crate) public_hostname: String,
    pub(crate) object_key: ByPnuObjectKeyPolicy,
    pub(crate) request_path: ByPnuRequestPath,
    pub(crate) content_type: String,
    pub(crate) cache_control: String,
    pub(crate) manifest_cache_control: String,
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
            Ok(contract)
        })
        .as_ref()
        .map_err(|message| anyhow::anyhow!(message.clone()))
}

/// The patch bounds shared by both lanes.
pub(crate) fn by_pnu_serving_patch_policy() -> anyhow::Result<&'static ByPnuServingPatchPolicy> {
    Ok(&contract()?.by_pnu_serving_patches)
}
