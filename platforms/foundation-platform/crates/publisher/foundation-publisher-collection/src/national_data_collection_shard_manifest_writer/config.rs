use std::{fs, path::PathBuf};

use anyhow::{bail, Context};

use crate::public_data_control_support::{env_path, repo_relative_path};

use super::support::*;
use super::FIXED_PAGE_COUNT_FALLBACK_REQUEST_CAP_CEILING;

const DEFAULT_APPROVAL_PATH: &str = "target/audit/national-data-collection-rollout-approval.json";
const DEFAULT_PILOT_EVIDENCE_PATH: &str = "target/audit/national-data-collection-run-evidence.json";
const DEFAULT_SCOPE_JSONL_PATH: &str = "target/audit/national-data-collection-scope.jsonl";
const DEFAULT_SCOPE_EVIDENCE_PATH: &str =
    "target/audit/national-data-collection-scope-evidence.json";
const DEFAULT_ENDPOINT_CATALOG_PATH: &str = "docs/catalog/public-source-endpoint-catalog.v1.json";
const DEFAULT_OUTPUT_PATH: &str = "target/audit/national-data-collection-shard-manifest.json";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProviderSet {
    All,
    BuildingRegister,
    VWorld,
    LandRegister,
    RealTransaction,
}

impl ProviderSet {
    fn parse(raw: &str) -> anyhow::Result<Self> {
        match raw {
            "all" => Ok(Self::All),
            "building-register" => Ok(Self::BuildingRegister),
            "vworld" => Ok(Self::VWorld),
            "land-register" => Ok(Self::LandRegister),
            "real-transaction" => Ok(Self::RealTransaction),
            other => bail!("ProviderSet is not supported: {other}"),
        }
    }

    pub(super) const fn includes_building_register(self) -> bool {
        matches!(self, Self::All | Self::BuildingRegister)
    }

    pub(super) const fn includes_vworld_cadastral(self) -> bool {
        matches!(self, Self::All | Self::VWorld)
    }

    pub(super) const fn includes_land_register(self) -> bool {
        matches!(self, Self::All | Self::VWorld | Self::LandRegister)
    }
}

#[derive(Debug)]
pub(super) struct WriterConfig {
    pub(super) root: PathBuf,
    pub(super) approval_path: PathBuf,
    pub(super) pilot_evidence_path: PathBuf,
    pub(super) scope_jsonl_path: PathBuf,
    pub(super) scope_evidence_path: PathBuf,
    pub(super) endpoint_catalog_path: PathBuf,
    pub(super) page_count_plan_path: Option<PathBuf>,
    pub(super) output_path: PathBuf,
    pub(super) request_cap: u64,
    pub(super) shard_size: usize,
    pub(super) building_max_pages: u64,
    pub(super) building_page_window_size: u64,
    pub(super) vworld_max_pages: u64,
    pub(super) land_register_max_pages: u64,
    pub(super) real_transaction_start_deal_ymd: String,
    pub(super) real_transaction_end_deal_ymd: String,
    pub(super) real_transaction_operations: Vec<String>,
    pub(super) real_transaction_max_pages: u64,
    pub(super) provider_set: ProviderSet,
    pub(super) confirm_fixed_page_count_fallback: bool,
    pub(super) confirm_national_shard_manifest: bool,
}

impl WriterConfig {
    pub(super) fn from_env() -> anyhow::Result<Self> {
        let root = env_path("FOUNDATION_PLATFORM_REPO_ROOT", ".")?;
        let root = fs::canonicalize(&root)
            .with_context(|| format!("failed to resolve repo root {}", root.display()))?;
        let page_count_plan_path = optional_env_path(
            &root,
            "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_PAGE_COUNT_PLAN_PATH",
            "PageCountPlanPath",
        )?;
        let config = Self {
            approval_path: env_repo_path(
                &root,
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_APPROVAL_PATH",
                DEFAULT_APPROVAL_PATH,
                "ApprovalPath",
            )?,
            pilot_evidence_path: env_repo_path(
                &root,
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_PILOT_EVIDENCE_PATH",
                DEFAULT_PILOT_EVIDENCE_PATH,
                "PilotEvidencePath",
            )?,
            scope_jsonl_path: env_repo_path(
                &root,
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SCOPE_JSONL_PATH",
                DEFAULT_SCOPE_JSONL_PATH,
                "ScopeJsonlPath",
            )?,
            scope_evidence_path: env_repo_path(
                &root,
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SCOPE_EVIDENCE_PATH",
                DEFAULT_SCOPE_EVIDENCE_PATH,
                "ScopeEvidencePath",
            )?,
            endpoint_catalog_path: env_repo_path(
                &root,
                "FOUNDATION_PLATFORM_PUBLIC_SOURCE_ENDPOINT_CATALOG_PATH",
                DEFAULT_ENDPOINT_CATALOG_PATH,
                "EndpointCatalogPath",
            )?,
            page_count_plan_path,
            output_path: env_repo_path(
                &root,
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SHARD_MANIFEST_OUTPUT_PATH",
                DEFAULT_OUTPUT_PATH,
                "OutputPath",
            )?,
            request_cap: env_u64(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_REQUEST_CAP",
                1,
            )?,
            shard_size: usize::try_from(env_u64(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_SHARD_SIZE",
                100,
            )?)
            .context("ShardSize is too large")?,
            building_max_pages: env_u64(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_BUILDING_MAX_PAGES",
                1,
            )?,
            building_page_window_size: env_u64(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_BUILDING_PAGE_WINDOW_SIZE",
                0,
            )?,
            vworld_max_pages: env_u64(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_VWORLD_MAX_PAGES",
                1,
            )?,
            land_register_max_pages: env_u64(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_LAND_REGISTER_MAX_PAGES",
                1,
            )?,
            real_transaction_start_deal_ymd: env_string(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_REAL_TRANSACTION_START_DEAL_YMD",
                "",
            )?,
            real_transaction_end_deal_ymd: env_string(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_REAL_TRANSACTION_END_DEAL_YMD",
                "",
            )?,
            real_transaction_operations: env_string_list(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_REAL_TRANSACTION_OPERATIONS",
            )?,
            real_transaction_max_pages: env_u64(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_REAL_TRANSACTION_MAX_PAGES",
                0,
            )?,
            provider_set: ProviderSet::parse(&env_string(
                "FOUNDATION_PLATFORM_NATIONAL_DATA_COLLECTION_PROVIDER_SET",
                "all",
            )?)?,
            confirm_fixed_page_count_fallback: env_bool(
                "FOUNDATION_PLATFORM_CONFIRM_FIXED_PAGE_COUNT_FALLBACK",
                false,
            )?,
            confirm_national_shard_manifest: env_bool(
                "FOUNDATION_PLATFORM_CONFIRM_NATIONAL_SHARD_MANIFEST",
                false,
            )?,
            root,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if !self.confirm_national_shard_manifest {
            bail!(
                "ConfirmNationalShardManifest is required before writing national shard manifest"
            );
        }
        if self.request_cap < 1
            || self.shard_size < 1
            || self.building_max_pages < 1
            || self.vworld_max_pages < 1
            || self.land_register_max_pages < 1
        {
            bail!("RequestCap, ShardSize, and provider max-pages must be positive");
        }
        if self.output_path.is_file() {
            bail!(
                "national shard manifest already exists: {}",
                repo_relative_path(&self.root, &self.output_path)
            );
        }
        require_file(
            &self.approval_path,
            "National rollout approval artifact missing",
        )?;
        require_file(
            &self.pilot_evidence_path,
            "National pilot evidence artifact missing",
        )?;
        require_file(
            &self.scope_jsonl_path,
            "National data collection scope JSONL missing",
        )?;
        require_file(
            &self.scope_evidence_path,
            "National scope evidence artifact missing",
        )?;
        require_file(
            &self.endpoint_catalog_path,
            "Public source endpoint catalog missing",
        )?;
        if let Some(path) = &self.page_count_plan_path {
            require_file(path, "National page count plan missing")?;
        }
        if self.building_page_window_size > 0 && self.page_count_plan_path.is_none() {
            bail!("BuildingPageWindowSize requires PageCountPlanPath");
        }
        if matches!(
            self.provider_set,
            ProviderSet::All | ProviderSet::VWorld | ProviderSet::LandRegister
        ) && self.page_count_plan_path.is_none()
        {
            if !self.confirm_fixed_page_count_fallback {
                bail!("VWorld national collection requires PageCountPlanPath; fixed page-count fallback is allowed only for bounded proof manifests with ConfirmFixedPageCountFallback");
            }
            if self.request_cap > FIXED_PAGE_COUNT_FALLBACK_REQUEST_CAP_CEILING {
                bail!(
                    "VWorld fixed page-count fallback requires RequestCap <= {}",
                    FIXED_PAGE_COUNT_FALLBACK_REQUEST_CAP_CEILING
                );
            }
        }
        Ok(())
    }
}
