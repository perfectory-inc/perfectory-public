//! Exact input authentication against the existing Bronze ledger; no second inventory.
use super::{
    input_evidence::FileEvidence, source_inputs::ExactInputs, ExportConfig, HistoryWitness,
    SourceSelector,
};
use anyhow::{ensure, Context};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use collection_application::ports::{BronzeIngestRepository, BronzeIngestUnitOfWork};
use collection_domain::{BronzeObject, SnapshotBasis, SnapshotGranularity};
use collection_infrastructure::{PgBronzeIngestRepository, PgBronzeIngestUnitOfWork};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::building_register_source_role::SourceRole;

const VERIFY_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_FLOOR_VERIFY_BRONZE_LEDGER";
const RETAINED_TIME_ENV: &str = "FOUNDATION_PLATFORM_BUILDING_REGISTER_RETAINED_INGESTED_AT_UTC";

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(super) struct CommittedInput {
    bronze_object_id: uuid::Uuid,
    source_catalog_id: uuid::Uuid,
    provider_file_id: String,
    object_key: String,
    checksum_sha256: String,
    size_bytes: u64,
    snapshot_date: NaiveDate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SemanticInput {
    role_slug: String,
    provider_file_id: String,
    provider_month: NaiveDate,
    size_bytes: u64,
    checksum_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SemanticInputs {
    floor: SemanticInput,
    title: SemanticInput,
}

#[derive(Serialize)]
struct ContentIdentity {
    schema_version: u8,
    inputs: SemanticInputs,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HistoricalBinding {
    schema_version: u8,
    table_name: String,
    table_uuid: uuid::Uuid,
    snapshot_id: String,
    operation: String,
    source_snapshot_id: String,
    bronze_object_key: String,
    valid_from_utc: DateTime<Utc>,
    ingested_at_utc: DateTime<Utc>,
    row_count: u64,
    inputs: SemanticInputs,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
/// Authenticated exact FLOOR/title ledger evidence, serialized without a second inventory.
pub struct CommittedInputs {
    pub(super) floor: CommittedInput,
    pub(super) title: CommittedInput,
    #[serde(skip)]
    pub(super) history: HistoryWitness,
}

pub(super) async fn from_env(
    exact: Option<&ExactInputs>,
) -> anyhow::Result<Option<CommittedInputs>> {
    match std::env::var(VERIFY_ENV) {
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Ok(value) if value == "1" => {}
        _ => anyhow::bail!("{VERIFY_ENV} must be 1 when supplied"),
    }
    let exact = exact.context("committed FLOOR export requires exact floor/title inputs")?;
    let history = HistoryWitness::from_lookup(&mut |key| std::env::var(key).ok(), true)?;
    // A retry may not silently choose a different time. The scheduler owns the retained time.
    super::parse_utc_env(RETAINED_TIME_ENV)?;
    let pool = PgPool::connect(&super::required_env("DATABASE_URL")?)
        .await
        .context("cannot read Foundation Bronze ledger")?;
    let floor = load(&pool, SourceRole::Floor, &exact.floor).await?;
    let title = load(&pool, SourceRole::Title, &exact.title).await?;
    pool.close().await;
    Ok(Some(CommittedInputs::from_objects(
        &floor, &title, history,
    )?))
}

async fn load(pool: &PgPool, role: SourceRole, name: &str) -> anyhow::Result<BronzeObject> {
    let repo = PgBronzeIngestRepository::new(pool.clone());
    let source = repo
        .find_source_catalog_by_slug(role.slug())
        .await?
        .context("committed Bronze source is missing")?;
    let key = format!("bronze/source={}/{name}", role.slug());
    // Reuse the existing SELECT. A later run failure does not invalidate a committed object.
    let row = PgBronzeIngestUnitOfWork::new(pool.clone())
        .find_bronze_object_by_object_key(source.id, &key)
        .await?
        .context("exact committed Bronze object is missing")?;
    ensure!(
        source.slug == role.slug()
            && row.source_catalog_id == source.id
            && row.object_key.as_str() == key,
        "committed Bronze source mismatch"
    );
    Ok(row)
}

impl CommittedInput {
    pub(super) fn from_object(row: &BronzeObject, key: &str, name: &str) -> anyhow::Result<Self> {
        let provider_date = crate::building_register_snapshot::object_date(name)?;
        ensure!(
            row.object_key.as_str() == key
                && row.provider_file_id.as_deref() == name.strip_suffix(".zip"),
            "committed Bronze object/provider identity mismatch"
        );
        ensure!(
            !row.id.as_uuid().is_nil() && !row.source_catalog_id.as_uuid().is_nil(),
            "committed Bronze IDs must not be nil"
        );
        ensure!(
            row.size_bytes > 0
                && row.checksum_sha256.len() == 64
                && row
                    .checksum_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "committed Bronze byte evidence is invalid"
        );
        ensure!(
            row.snapshot_granularity == SnapshotGranularity::Month
                && row.snapshot_basis == SnapshotBasis::ProviderFilePeriod
                && row.snapshot_date.day() == 1
                && row.snapshot_date.year() == provider_date.year()
                && row.snapshot_date.month() == provider_date.month()
                && row.snapshot_period.as_deref()
                    == Some(row.snapshot_date.format("%Y-%m").to_string().as_str()),
            "committed HUB provider month is invalid"
        );
        Ok(Self {
            bronze_object_id: row.id.as_uuid(),
            source_catalog_id: row.source_catalog_id.as_uuid(),
            provider_file_id: row
                .provider_file_id
                .clone()
                .context("committed provider ID missing")?,
            object_key: key.to_owned(),
            checksum_sha256: row.checksum_sha256.clone(),
            size_bytes: row.size_bytes,
            snapshot_date: row.snapshot_date,
        })
    }

    fn semantic(&self, role: SourceRole) -> SemanticInput {
        SemanticInput {
            role_slug: role.slug().to_owned(),
            provider_file_id: self.provider_file_id.clone(),
            provider_month: self.snapshot_date,
            size_bytes: self.size_bytes,
            checksum_sha256: self.checksum_sha256.clone(),
        }
    }
}

impl CommittedInputs {
    /// Invocation-owned historical evidence used by selection, child pins and retained checks.
    #[must_use]
    pub const fn history(&self) -> &HistoryWitness {
        &self.history
    }

    pub(super) fn floor_object_key(&self) -> &str {
        &self.floor.object_key
    }

    pub(super) fn from_objects(
        floor: &BronzeObject,
        title: &BronzeObject,
        history: HistoryWitness,
    ) -> anyhow::Result<Self> {
        let parse = |row: &BronzeObject, role: SourceRole| -> anyhow::Result<CommittedInput> {
            let prefix = format!("bronze/source={}/", role.slug());
            let name = row
                .object_key
                .as_str()
                .strip_prefix(prefix.as_str())
                .context("committed Bronze input source path mismatch")?;
            CommittedInput::from_object(row, row.object_key.as_str(), name)
        };
        let floor = parse(floor, SourceRole::Floor)?;
        let title = parse(title, SourceRole::Title)?;
        ensure!(
            floor.snapshot_date == title.snapshot_date,
            "FLOOR/title provider months differ"
        );
        let floor_name = format!("{}.zip", floor.provider_file_id);
        let title_name = format!("{}.zip", title.provider_file_id);
        crate::building_register_snapshot::validate_object_name(
            &title_name,
            crate::building_register_snapshot::object_date(&floor_name)?,
        )?;
        Ok(Self {
            floor,
            title,
            history,
        })
    }

    pub(super) fn source_names(&self) -> (String, String) {
        (
            format!("{}.zip", self.floor.provider_file_id),
            format!("{}.zip", self.title.provider_file_id),
        )
    }

    pub(super) fn retained_ingested_at_utc(&self) -> anyhow::Result<Option<DateTime<Utc>>> {
        Ok(self
            .historical_binding()?
            .map(|binding| binding.ingested_at_utc))
    }

    pub(super) fn expected_evidence(&self, config: &ExportConfig) -> Vec<FileEvidence> {
        [&self.floor, &self.title]
            .into_iter()
            .map(|input| FileEvidence {
                path: config.bronze_local_object_root.join(&input.object_key),
                size_bytes: input.size_bytes,
                sha256: input.checksum_sha256.clone(),
            })
            .collect()
    }

    pub(super) fn semantic(&self) -> SemanticInputs {
        SemanticInputs {
            floor: self.floor.semantic(SourceRole::Floor),
            title: self.title.semantic(SourceRole::Title),
        }
    }

    pub(super) fn historical_binding(&self) -> anyhow::Result<Option<HistoricalBinding>> {
        self.semantic().historical_binding(&self.history)
    }

    pub(super) fn source_snapshot_id(&self) -> anyhow::Result<String> {
        self.semantic().source_snapshot_id(&self.history)
    }

    pub(super) fn valid_from_utc(&self) -> anyhow::Result<DateTime<Utc>> {
        if let Some(binding) = self.historical_binding()? {
            return Ok(binding.valid_from_utc);
        }
        Ok(self
            .floor
            .snapshot_date
            .and_time(chrono::NaiveTime::MIN)
            .and_utc())
    }

    pub(super) fn verify(
        &self,
        config: &ExportConfig,
        evidence: &[FileEvidence],
    ) -> anyhow::Result<()> {
        ensure!(
            config.source_selector == SourceSelector::Exact(SourceRole::Floor.slug().to_owned())
                && config.title_source_slug.as_deref() == Some(SourceRole::Title.slug()),
            "committed FLOOR export source roles differ"
        );
        ensure!(
            config.max_rows.is_none(),
            "committed FLOOR export must cover the complete source"
        );
        ensure!(
            evidence.len() == 2,
            "committed FLOOR export requires two physical inputs"
        );
        for input in [&self.floor, &self.title] {
            let expected_path = config.bronze_local_object_root.join(&input.object_key);
            let observed = evidence
                .iter()
                .find(|item| item.path == expected_path)
                .context("committed Bronze input path mismatch")?;
            ensure!(
                (observed.size_bytes, &observed.sha256)
                    == (input.size_bytes, &input.checksum_sha256),
                "selected bytes differ from committed Bronze ledger: {}",
                input.object_key
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

impl HistoricalBinding {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        use super::history_witness::valid_sha256;
        let binding = self;
        ensure!(
            binding.schema_version == 1
                && binding.table_name == "silver.building_register_floors"
                && !binding.table_uuid.is_nil()
                && binding
                    .snapshot_id
                    .parse::<i64>()
                    .is_ok_and(|id| id > 0 && id.to_string() == binding.snapshot_id)
                && binding.operation == "overwrite"
                && binding
                    .source_snapshot_id
                    .strip_prefix("building-register-floor-selection-v2-")
                    .is_some_and(valid_sha256)
                && binding.row_count > 0
                && binding
                    .valid_from_utc
                    .timestamp_subsec_nanos()
                    .is_multiple_of(1000)
                && binding
                    .ingested_at_utc
                    .timestamp_subsec_nanos()
                    .is_multiple_of(1000)
                && binding.inputs.floor.role_slug == SourceRole::Floor.slug()
                && binding.inputs.title.role_slug == SourceRole::Title.slug()
                && binding.inputs.floor.provider_month == binding.inputs.title.provider_month,
            "invalid fixed historical FLOOR binding"
        );
        for input in [&binding.inputs.floor, &binding.inputs.title] {
            ensure!(
                input
                    .provider_file_id
                    .strip_prefix("OPN")
                    .is_some_and(|suffix| !suffix.is_empty()
                        && suffix
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte)))
                    && input.provider_month.day() == 1
                    && input.size_bytes > 0
                    && valid_sha256(&input.checksum_sha256),
                "invalid historical FLOOR input evidence"
            );
        }
        let floor = &binding.inputs.floor;
        ensure!(
            binding.bronze_object_key
                == format!(
                    "bronze/source={}/{}--sha256-{}.zip",
                    floor.role_slug, floor.provider_file_id, floor.checksum_sha256
                ),
            "historical FLOOR object key differs from input evidence"
        );
        Ok(())
    }
}

impl SemanticInputs {
    pub(super) fn historical_binding(
        &self,
        history: &HistoryWitness,
    ) -> anyhow::Result<Option<HistoricalBinding>> {
        let binding = history.binding();
        let semantic = self;
        if semantic == &binding.inputs {
            return Ok(Some(binding.clone()));
        }
        ensure!(
            semantic.floor.provider_month > binding.inputs.floor.provider_month
                && semantic.title.provider_month > binding.inputs.title.provider_month,
            "historical FLOOR/title source has no matching fixed binding"
        );
        Ok(None)
    }

    pub(super) fn source_snapshot_id(&self, history: &HistoryWitness) -> anyhow::Result<String> {
        if let Some(binding) = self.historical_binding(history)? {
            return Ok(binding.source_snapshot_id);
        }
        Ok(format!(
            "building-register-floor-content-v1-{:x}",
            Sha256::digest(serde_json::to_vec(&ContentIdentity {
                schema_version: 1,
                inputs: self.clone(),
            })?)
        ))
    }
}

pub(super) fn source_identity_from_semantic(
    value: serde_json::Value,
    history: &HistoryWitness,
) -> anyhow::Result<String> {
    let inputs: SemanticInputs = serde_json::from_value(value)?;
    inputs.source_snapshot_id(history)
}
