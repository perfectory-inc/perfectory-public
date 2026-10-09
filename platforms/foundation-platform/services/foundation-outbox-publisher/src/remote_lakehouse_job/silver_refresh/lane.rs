//! The lanes a Silver refresh knows, and what each lane's contract says about running it.
//!
//! The list of lanes is this enum: each needs its export's environment, which is code. Everything
//! else a lane runs with (its Silver table, the Bronze sources that make one release, the export
//! command, the Spark size and write mode) is the `silver_refresh` block of the lane's source
//! contract, compiled in from the same release (root ADR-0169 §2–3).
use std::collections::BTreeMap;

use anyhow::{bail, ensure, Context};
use serde::Deserialize;

/// One Silver table the refresh keeps current from its hub.go.kr Bronze sources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) enum Lane {
    Titles,
    Units,
    UnitAreas,
    ApartmentPrice,
    ExclusiveUnit,
}

impl Lane {
    pub(in crate::remote_lakehouse_job) const ALL: [Self; 5] = [
        Self::Titles,
        Self::Units,
        Self::UnitAreas,
        Self::ApartmentPrice,
        Self::ExclusiveUnit,
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

    const fn contract_source(self) -> (&'static str, &'static str) {
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
                include_str!("../../../../../infra/lakehouse/contracts/hub-building-register-apartment-price-source-objects.json"),
            ),
            Self::ExclusiveUnit => (
                "hub-building-register-exclusive-unit-source-objects.json",
                include_str!("../../../../../infra/lakehouse/contracts/hub-building-register-exclusive-unit-source-objects.json"),
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
            other => bail!("export {other:?} is not one the Silver refresh can run"),
        })
    }

    /// The source roles the export reads, in the names the contract gives them.
    pub(in crate::remote_lakehouse_job) const fn roles(self) -> &'static [&'static str] {
        match self {
            Self::Title => &["title"],
            Self::Unit => &["basis", "title", "unit"],
            Self::UnitArea => &["unit_area"],
            Self::HubParts { .. } => &["source"],
        }
    }

    /// Whether one load is one `source_snapshot_id` (a run) rather than one handoff part.
    pub(in crate::remote_lakehouse_job) const fn loads_as_one_run(self) -> bool {
        !matches!(self, Self::HubParts { .. })
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
    /// The hand-measured release a lane contract no longer carries (root ADR-0169 §2): present,
    /// it would read as the release while the ledger decides.
    #[serde(default)]
    selected_vintage: Option<serde::de::IgnoredAny>,
    #[serde(default)]
    objects: Option<serde::de::IgnoredAny>,
    silver_refresh: Block,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Block {
    #[serde(rename = "$comment", default)]
    _comment: Option<String>,
    table: String,
    roles: BTreeMap<String, String>,
    export: String,
    #[serde(default)]
    export_chunk_rows: Option<u32>,
    spark: SparkRunner,
}

/// A lane's contract after the checks that keep a wrong value from reaching a run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::remote_lakehouse_job) struct LaneContract {
    pub lane: Lane,
    pub table: String,
    /// Role name to Bronze source slug; every role must have an object for a release to count.
    pub roles: BTreeMap<String, String>,
    pub export: String,
    pub kind: ExportKind,
    pub export_chunk_rows: Option<u32>,
    /// The R2 prefix of the hub handoff parts (the layout's `handoff_prefix`), for part lanes.
    pub handoff_prefix: Option<String>,
    pub spark: SparkRunner,
}

impl LaneContract {
    pub(in crate::remote_lakehouse_job) fn parse(lane: Lane, text: &str) -> anyhow::Result<Self> {
        let document: Document = serde_json::from_str(text)?;
        ensure!(document.schema_version == 1, "unsupported schema_version");
        ensure!(
            document.selected_vintage.is_none() && document.objects.is_none(),
            "a lane contract names no release (selected_vintage, objects): the Bronze ledger does"
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
        let roles: Vec<&str> = block.roles.keys().map(String::as_str).collect();
        ensure!(
            roles == kind.roles(),
            "{} reads the roles {:?}, the contract names {roles:?}",
            block.export,
            kind.roles()
        );
        for slug in block.roles.values() {
            ensure!(
                slug.starts_with("hubgokr__")
                    && slug[9..]
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                "{slug:?} is not a hub.go.kr Bronze source slug"
            );
        }
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
            let prefix = document
                .handoff_prefix
                .context("a part lane's contract states handoff_prefix")?;
            ensure!(
                prefix.starts_with("silver-handoff/")
                    && !prefix.ends_with('/')
                    && !prefix.contains([',', '@']),
                "handoff_prefix must be a silver-handoff/ key prefix"
            );
            Some(prefix)
        };
        Ok(Self {
            lane,
            table: block.table,
            roles: block.roles,
            export: block.export,
            kind,
            export_chunk_rows: block.export_chunk_rows,
            handoff_prefix,
            spark,
        })
    }
}
