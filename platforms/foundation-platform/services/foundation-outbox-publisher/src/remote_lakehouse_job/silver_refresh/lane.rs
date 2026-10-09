//! The lanes a Silver refresh knows, and what each lane's contract says about running it.
//!
//! The list of lanes is this enum: each needs its export's environment, which is code. Everything
//! else a lane runs with (its Silver table, the Bronze sources that make one release, the export
//! command, the Spark size and write mode) is the `silver_refresh` block of the lane's source
//! contract, compiled in from the same release (root ADR-0169 §2–3). A VWorld land lane's block
//! also holds the rule that reads its region and vintage from a ZIP member name and how many
//! regions make a release (step 3, `land.rs`).
use std::collections::BTreeMap;

use anyhow::{bail, ensure, Context};
use serde::Deserialize;

use super::land::{Completeness, LandRule};

/// One Silver table the refresh keeps current from its Bronze sources: hub.go.kr registers and
/// VWorld land datasets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) enum Lane {
    Titles,
    Units,
    UnitAreas,
    ApartmentPrice,
    ExclusiveUnit,
    LandCharacteristic,
    LandForestLedger,
    LandIndividualPrice,
    LandRightRegistration,
    LandTransferHistory,
    LandUsePlan,
    LandUseZoneCode,
}

impl Lane {
    pub(in crate::remote_lakehouse_job) const ALL: [Self; 12] = [
        Self::Titles,
        Self::Units,
        Self::UnitAreas,
        Self::ApartmentPrice,
        Self::ExclusiveUnit,
        Self::LandCharacteristic,
        Self::LandForestLedger,
        Self::LandIndividualPrice,
        Self::LandRightRegistration,
        Self::LandTransferHistory,
        Self::LandUsePlan,
        Self::LandUseZoneCode,
    ];

    /// The systemd instance name (`foundation-silver-refresh@<id>.service`): hyphens, because
    /// the job list and the release admission accept no other instance spelling.
    pub(in crate::remote_lakehouse_job) const fn id(self) -> &'static str {
        match self {
            Self::Titles => "building-register-titles",
            Self::Units => "building-register-units",
            Self::UnitAreas => "building-register-unit-areas",
            Self::ApartmentPrice => "building-register-apartment-price",
            Self::ExclusiveUnit => "building-register-exclusive-unit",
            Self::LandCharacteristic => "land-characteristic",
            Self::LandForestLedger => "land-forest-ledger",
            Self::LandIndividualPrice => "land-individual-price",
            Self::LandRightRegistration => "land-right-registration",
            Self::LandTransferHistory => "land-transfer-history",
            Self::LandUsePlan => "land-use-plan",
            Self::LandUseZoneCode => "land-use-zone-code",
        }
    }

    pub(in crate::remote_lakehouse_job) fn parse(raw: &str) -> anyhow::Result<Self> {
        Self::ALL
            .into_iter()
            .find(|lane| lane.id() == raw)
            .with_context(|| {
                format!(
                    "unknown Silver refresh lane {raw:?}; the lanes are {}",
                    Self::ALL.map(Self::id).join(", ")
                )
            })
    }

    /// The Silver table this lane writes, the one its contract must name.
    pub(in crate::remote_lakehouse_job) fn table(self) -> String {
        format!("silver.{}", self.id().replace('-', "_"))
    }

    pub(in crate::remote_lakehouse_job) const fn contract_source(
        self,
    ) -> (&'static str, &'static str) {
        match self {
            Self::Titles => (
                "hub-building-register-title-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/hub-building-register-title-source-objects.json"),
            ),
            Self::Units => (
                "hub-building-register-unit-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/hub-building-register-unit-source-objects.json"),
            ),
            Self::UnitAreas => (
                "hub-building-register-unit-area-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/hub-building-register-unit-area-source-objects.json"),
            ),
            Self::ApartmentPrice => (
                "hub-building-register-apartment-price-source-objects.json",
                foundation_outbox_publisher::building_register_apartment_price_silver_export::CONTRACT_JSON,
            ),
            Self::ExclusiveUnit => (
                "hub-building-register-exclusive-unit-source-objects.json",
                foundation_outbox_publisher::building_register_exclusive_unit_silver_export::CONTRACT_JSON,
            ),
            Self::LandCharacteristic => (
                "vworld-land-characteristic-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/vworld-land-characteristic-source-objects.json"),
            ),
            Self::LandForestLedger => (
                "vworld-land-forest-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/vworld-land-forest-source-objects.json"),
            ),
            Self::LandIndividualPrice => (
                "vworld-land-individual-price-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/vworld-land-individual-price-source-objects.json"),
            ),
            Self::LandRightRegistration => (
                "vworld-land-right-registration-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/vworld-land-right-registration-source-objects.json"),
            ),
            Self::LandTransferHistory => (
                "vworld-land-transfer-history-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/vworld-land-transfer-history-source-objects.json"),
            ),
            Self::LandUsePlan => (
                "vworld-land-use-plan-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/vworld-land-use-plan-source-objects.json"),
            ),
            Self::LandUseZoneCode => (
                "vworld-land-use-zone-code-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/vworld-land-use-zone-code-source-objects.json"),
            ),
        }
    }

    pub(in crate::remote_lakehouse_job) fn contract(self) -> anyhow::Result<LaneContract> {
        let (name, text) = self.contract_source();
        LaneContract::parse(self, text).with_context(|| {
            format!("infra/lakehouse/contracts/{name} is not a valid lane contract")
        })
    }
}

/// How the lane's export hands rows to Spark, which decides the load identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) enum ExportKind {
    /// 표제부: one local JSONL file from one staged ZIP.
    Title,
    /// 전유부: local Parquet parts from the unit, title and basis ZIPs of one export day.
    Unit,
    /// 전유공용면적: local Parquet parts from one staged ZIP.
    UnitArea,
    /// A national hub ZIP streamed from R2 into gzip JSONL parts under an R2 manifest.
    HubParts { env_prefix: &'static str },
    /// A VWorld land CSV ZIP streamed from R2 into one gzip JSONL handoff per Bronze object
    /// (`land_use_silver_export`, root ADR-0083); a release is one object per region.
    Land { env_prefix: &'static str },
}

impl ExportKind {
    fn parse(command: &str) -> anyhow::Result<Self> {
        Ok(match command {
            "export-building-register-title-silver-handoff" => Self::Title,
            "export-building-register-unit-silver-handoff" => Self::Unit,
            "export-building-register-unit-area-silver-handoff" => Self::UnitArea,
            "export-building-register-apartment-price-silver-handoff" => Self::HubParts {
                env_prefix: "FOUNDATION_PLATFORM_APARTMENT_PRICE",
            },
            "export-building-register-exclusive-unit-silver-handoff" => Self::HubParts {
                env_prefix: "FOUNDATION_PLATFORM_EXCLUSIVE_UNIT",
            },
            other => match crate::land_use_silver_export::export_command(other) {
                Some(export) => Self::Land {
                    env_prefix: export.env_prefix,
                },
                None => bail!("export {other:?} is not one the Silver refresh can run"),
            },
        })
    }

    /// The source roles the export reads, in the names the contract gives them. A land lane
    /// names one source and no roles: its release is regions, not roles.
    pub(in crate::remote_lakehouse_job) const fn roles(self) -> &'static [&'static str] {
        match self {
            Self::Title => &["title"],
            Self::Unit => &["basis", "title", "unit"],
            Self::UnitArea => &["unit_area"],
            Self::HubParts { .. } => &["source"],
            Self::Land { .. } => &[],
        }
    }

    /// Whether one load is one `source_snapshot_id` (a run) rather than one handoff part.
    pub(in crate::remote_lakehouse_job) const fn loads_as_one_run(self) -> bool {
        matches!(self, Self::Title | Self::Unit | Self::UnitArea)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(in crate::remote_lakehouse_job) enum WriteMode {
    Append,
    Overwrite,
}

impl WriteMode {
    pub(in crate::remote_lakehouse_job) const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Overwrite => "overwrite",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(in crate::remote_lakehouse_job) struct SparkRunner {
    pub master: String,
    pub driver_memory: String,
    pub iceberg_write_mode: WriteMode,
    pub input_file_batch_size: u32,
}

#[derive(Deserialize)]
struct Document {
    schema_version: u32,
    #[serde(default)]
    handoff_prefix: Option<String>,
    #[serde(default)]
    handoff_suffix: Option<String>,
    /// The hand-measured release a lane contract no longer carries (root ADR-0169 §2): present,
    /// it would read as the release while the ledger decides.
    #[serde(default)]
    selected_vintage: Option<serde::de::IgnoredAny>,
    #[serde(default)]
    objects: Option<serde::de::IgnoredAny>,
    #[serde(default)]
    granularity_counts: Option<serde::de::IgnoredAny>,
    silver_refresh: Block,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Block {
    #[serde(rename = "$comment", default)]
    _comment: Option<String>,
    table: String,
    /// Hub lanes: role name to Bronze source slug.
    #[serde(default)]
    roles: Option<BTreeMap<String, String>>,
    /// Land lanes: the one Bronze source slug.
    #[serde(default)]
    source: Option<String>,
    /// Land lanes: the ZIP member the export reads, with its region and vintage as named groups.
    #[serde(default)]
    member_name: Option<String>,
    /// Land lanes: what makes a vintage a release.
    #[serde(default)]
    completeness: Option<CompletenessBlock>,
    export: String,
    #[serde(default)]
    export_chunk_rows: Option<u32>,
    spark: SparkRunner,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompletenessBlock {
    load_granularity: String,
    count: usize,
}

/// A lane's contract after the checks that keep a wrong value from reaching a run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct LaneContract {
    pub lane: Lane,
    pub table: String,
    /// Role name to Bronze source slug; every role must have an object for a release to count.
    /// Empty for a land lane, whose release is `land`'s.
    pub roles: BTreeMap<String, String>,
    pub export: String,
    pub kind: ExportKind,
    pub export_chunk_rows: Option<u32>,
    /// The R2 prefix of the hub handoff parts (the layout's `handoff_prefix`), for part lanes.
    pub handoff_prefix: Option<String>,
    /// How a land lane finds its release in the ledger and names its handoffs.
    pub land: Option<LandRule>,
    pub spark: SparkRunner,
}

fn checked_prefix(prefix: Option<String>) -> anyhow::Result<String> {
    let prefix = prefix.context("the lane's contract states handoff_prefix")?;
    ensure!(
        prefix.starts_with("silver-handoff/")
            && !prefix.ends_with('/')
            && !prefix.contains([',', '@']),
        "handoff_prefix must be a silver-handoff/ key prefix"
    );
    Ok(prefix)
}

fn checked_slug(slug: &str, provider: &str) -> anyhow::Result<()> {
    let rest = slug.strip_prefix(provider).unwrap_or_default();
    ensure!(
        !rest.is_empty()
            && rest
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "{slug:?} is not a {provider} Bronze source slug"
    );
    Ok(())
}

impl LaneContract {
    pub(in crate::remote_lakehouse_job) fn parse(lane: Lane, text: &str) -> anyhow::Result<Self> {
        let document: Document = serde_json::from_str(text)?;
        ensure!(document.schema_version == 1, "unsupported schema_version");
        ensure!(
            document.selected_vintage.is_none()
                && document.objects.is_none()
                && document.granularity_counts.is_none(),
            "a lane contract names no release (selected_vintage, objects, granularity_counts): the Bronze ledger does"
        );
        let block = document.silver_refresh;
        let kind = ExportKind::parse(&block.export)?;
        ensure!(
            block.table == lane.table(),
            "silver_refresh.table is {} but lane {} writes {}",
            block.table,
            lane.id(),
            lane.table()
        );
        let spark = block.spark;
        ensure!(
            spark.master.starts_with("local[")
                && spark.master.ends_with(']')
                && spark.driver_memory.len() >= 2
                && spark.driver_memory.ends_with('g')
                && spark.driver_memory[..spark.driver_memory.len() - 1]
                    .bytes()
                    .all(|b| b.is_ascii_digit()),
            "spark.master must be local[N] and spark.driver_memory <N>g"
        );
        if let ExportKind::Land { .. } = kind {
            let export = crate::land_use_silver_export::export_command(&block.export)
                .context("a land lane runs a land export")?;
            ensure!(
                export.table == block.table,
                "{} writes {}, not {}",
                block.export,
                export.table,
                block.table
            );
            ensure!(
                block.roles.is_none() && block.export_chunk_rows.is_none(),
                "a land lane names one source and no roles or chunk size"
            );
            // gold.parcel_panel reads one source_snapshot_id per land table, so a new release
            // replaces the table: its first load batch overwrites and the rest append to it.
            ensure!(
                spark.iceberg_write_mode == WriteMode::Overwrite && spark.input_file_batch_size > 0,
                "{} holds one release for gold.parcel_panel: a land lane overwrites in batches of input_file_batch_size",
                block.table
            );
            let source = block.source.context("a land lane states its source")?;
            checked_slug(&source, "vworldkr__")?;
            let completeness = block
                .completeness
                .context("a land lane states its completeness")?;
            let completeness = match (completeness.load_granularity.as_str(), completeness.count) {
                ("sido", count) if count > 0 => Completeness::Sido { count },
                ("national", 1) => Completeness::National,
                (granularity, count) => {
                    bail!("completeness {granularity} {count} is not sido N or national 1")
                }
            };
            // The label the hand loads wrote (`vworldkr-land-use-plan:<vintage>`), so a release
            // they loaded is the same release here.
            let label = source.replace("__", "-").replace('_', "-");
            let handoff_suffix = document
                .handoff_suffix
                .context("a land lane's contract states handoff_suffix")?;
            ensure!(
                handoff_suffix == ".jsonl.gz",
                "a land handoff is gzip JSONL (.jsonl.gz), which the export compresses and Spark reads"
            );
            let rule = LandRule {
                source,
                member_name: block
                    .member_name
                    .context("a land lane states its member_name rule")?,
                completeness,
                snapshot_label: label,
                handoff_prefix: checked_prefix(document.handoff_prefix)?,
                handoff_suffix,
            };
            rule.member_regex()?;
            return Ok(Self {
                lane,
                table: block.table,
                roles: BTreeMap::new(),
                export: block.export,
                kind,
                export_chunk_rows: None,
                handoff_prefix: Some(rule.handoff_prefix.clone()),
                land: Some(rule),
                spark,
            });
        }
        ensure!(
            block.source.is_none() && block.member_name.is_none() && block.completeness.is_none(),
            "a hub lane reads roles from the ledger month, not land member names"
        );
        let block_roles = block
            .roles
            .context("a hub lane names the roles of its release")?;
        let roles: Vec<&str> = block_roles.keys().map(String::as_str).collect();
        ensure!(
            roles == kind.roles(),
            "{} reads the roles {:?}, the contract names {roles:?}",
            block.export,
            kind.roles()
        );
        for slug in block_roles.values() {
            checked_slug(slug, "hubgokr__")?;
        }
        let handoff_prefix = if kind.loads_as_one_run() {
            // One run is one load identity; a split input would write only its first batch
            // (silver_scalar_handoff_to_lakehouse.py refuses it before writing).
            ensure!(
                spark.input_file_batch_size == 0,
                "a one-run load must be one input batch (input_file_batch_size 0)"
            );
            ensure!(
                spark.iceberg_write_mode == WriteMode::Overwrite,
                "{} holds one source_snapshot_id for gold.building_panel, so a new release replaces it (overwrite)",
                block.table
            );
            ensure!(
                block.export_chunk_rows.is_some() == (kind != ExportKind::Title),
                "export_chunk_rows is for the Parquet exports and only them"
            );
            None
        } else {
            ensure!(
                spark.input_file_batch_size > 0
                    && spark.iceberg_write_mode == WriteMode::Append
                    && block.export_chunk_rows.is_none(),
                "a part lane appends its parts in batches of input_file_batch_size and has no chunk size"
            );
            Some(checked_prefix(document.handoff_prefix)?)
        };
        Ok(Self {
            lane,
            table: block.table,
            roles: block_roles,
            export: block.export,
            kind,
            export_chunk_rows: block.export_chunk_rows,
            handoff_prefix,
            land: None,
            spark,
        })
    }
}
