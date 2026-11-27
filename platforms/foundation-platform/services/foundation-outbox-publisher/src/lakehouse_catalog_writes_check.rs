//! `check-lakehouse-catalog-writes`: this binary's writer path against an Iceberg REST catalog that
//! vends storage credentials, holding no storage key of its own (root ADR-0624; test T8 of the
//! Lakekeeper dev rig, `infra/aws-lakekeeper-dev`, and a step of its upgrade canary).
//!
//! iceberg-rust's own OAuth2 client takes a token with the principal's client credentials, the
//! catalog's `load-table` answer carries a credential for that one table, and `OpenDAL` writes with
//! it. The committer and the REST client the lanes run ([`LakehouseDataCommitter`],
//! [`IcebergRestCatalog`]) then do to one `_smoke` table what a lane does: create it, append, make
//! and remove a tag, remove a branch `main` has left behind, publish a load branch. Last, the same
//! append with a wrong static key, which must fail: a key set beside a vending catalog wins over the
//! vended one (iceberg-catalog-rest 0.11 `load_table`), the trap a lane's static key would set.
//!
//! Each printed line is one JSON check. No token, secret or credential is printed. A failed check
//! fails the command.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    env,
    fs::File,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, ensure, Context};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use foundation_publisher_core::lakehouse_data_commit::{
    AppendOutcome, DataAppend, LakehouseDataCommitter, StagedFile,
};
use lakehouse_domain::{
    LakehouseColumn, LakehouseLayer, LakehouseLoadUnit, LakehousePhysicalFormat,
    LakehouseRowVersions, LakehouseServingRole, LakehouseTableContract, LakehouseWriteDistribution,
};
use lakehouse_infrastructure::{
    IcebergRestCatalog, LakehouseCatalogConfig, LakehouseCatalogProvider,
};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use serde_json::{json, Value};

use crate::public_data_control_support::required_env_value;

const CATALOG_URI_ENV: &str = "LAKEHOUSE_CHECK_CATALOG_URI";
const WAREHOUSE_ENV: &str = "LAKEHOUSE_CHECK_WAREHOUSE";
const TOKEN_URI_ENV: &str = "LAKEHOUSE_CHECK_TOKEN_URI";
const SCOPE_ENV: &str = "LAKEHOUSE_CHECK_SCOPE";
const CLIENT_ID_ENV: &str = "LAKEHOUSE_CHECK_CLIENT_ID";
const CLIENT_SECRET_ENV: &str = "LAKEHOUSE_CHECK_CLIENT_SECRET";
const TABLE_ENV: &str = "LAKEHOUSE_CHECK_TABLE";

/// Storage keys this check runs without: with none set, the vended credential is the only one.
const STATIC_KEYS: &[&str] = &[
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
    "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY",
    "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID",
    "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY",
];

/// A key pair that names no account: the negative case's static key.
const WRONG_KEY_ID: &str = "W109T8WRONGKEYID0000";
const WRONG_SECRET: &str = "w109-t8-wrong-secret-not-a-key";

const TAG: &str = "w109_t8_tag";
const STALE_BRANCH: &str = "w109_t8_stale";
const LOAD_BRANCH: &str = "w109_t8_load";
const ROWS: i64 = 3;

const COLUMNS: &[LakehouseColumn] = &[
    LakehouseColumn {
        name: "id",
        logical_type: "long",
        required: false,
    },
    LakehouseColumn {
        name: "name",
        logical_type: "string",
        required: false,
    },
    LakehouseColumn {
        name: "batch",
        logical_type: "string",
        required: false,
    },
];

/// The check's own table shape: synthetic rows only, created under the `_smoke` name it is given.
const SMOKE: LakehouseTableContract = LakehouseTableContract {
    table_name: "silver.catalog_writes_check_smoke",
    layer: LakehouseLayer::Silver,
    physical_format: LakehousePhysicalFormat::Parquet,
    serving_role: LakehouseServingRole::Canonical,
    current_row_predicate: None,
    row_versions: LakehouseRowVersions::Events {
        canonical_reader: None,
    },
    columns: COLUMNS,
    partition_spec: &[],
    sort_order: &[],
    write_distribution: LakehouseWriteDistribution::Hash,
    quality_gates: &[],
    load: LakehouseLoadUnit::Derived,
};

struct Settings {
    catalog_uri: String,
    warehouse: String,
    token_uri: String,
    scope: String,
    client_id: String,
    client_secret: String,
    table: String,
}

impl Settings {
    fn from_env() -> anyhow::Result<Self> {
        let table = required_env_value(TABLE_ENV)?;
        ensure!(
            table.ends_with("_smoke") && table.split('.').count() == 2,
            "{TABLE_ENV} must be namespace.<name>_smoke: this check writes synthetic rows only to a smoke table"
        );
        Ok(Self {
            catalog_uri: required_env_value(CATALOG_URI_ENV)?,
            warehouse: required_env_value(WAREHOUSE_ENV)?,
            token_uri: required_env_value(TOKEN_URI_ENV)?,
            scope: required_env_value(SCOPE_ENV)?,
            client_id: required_env_value(CLIENT_ID_ENV)?,
            client_secret: required_env_value(CLIENT_SECRET_ENV)?,
            table,
        })
    }

    /// iceberg-rust's own catalog properties: its OAuth2 client, and no storage key.
    fn vended(&self) -> HashMap<String, String> {
        [
            ("uri", self.catalog_uri.clone()),
            ("warehouse", self.warehouse.clone()),
            (
                "credential",
                format!("{}:{}", self.client_id, self.client_secret),
            ),
            ("oauth2-server-uri", self.token_uri.clone()),
            ("scope", self.scope.clone()),
            (
                "header.x-iceberg-access-delegation",
                "vended-credentials".to_owned(),
            ),
            // No credential from the environment or instance metadata either: only the vended one.
            ("s3.disable-config-load", "true".to_owned()),
            ("s3.disable-ec2-metadata", "true".to_owned()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
    }
}

/// The checks of one run; a failed one fails the command at the end.
#[derive(Default)]
struct Checks {
    failed: Vec<String>,
}

impl Checks {
    fn record(&mut self, name: &str, ok: bool, detail: Value) -> bool {
        println!("{}", json!({"check": name, "ok": ok, "detail": detail}));
        if !ok {
            self.failed.push(name.to_owned());
        }
        ok
    }
}

/// An error's chain as text, cut short: it may name a path or a status, never a secret this check
/// holds (the wrong key pair is a literal of this file).
fn told(error: &anyhow::Error) -> String {
    format!("{error:#}").chars().take(300).collect()
}

pub(crate) async fn run() -> anyhow::Result<()> {
    let held: Vec<&str> = STATIC_KEYS
        .iter()
        .copied()
        .filter(|name| env::var_os(name).is_some())
        .collect();
    ensure!(
        held.is_empty(),
        "{held:?} set: this check runs with the vended credential only"
    );
    let settings = Settings::from_env()?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut checks = Checks::default();
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let staging = tempfile::tempdir()?;
    let table = settings.table.as_str();

    let committer = match LakehouseDataCommitter::open(settings.vended()).await {
        Ok(committer) => committer,
        Err(error) => {
            checks.record(
                "open the catalog with client credentials and no storage key",
                false,
                json!(told(&error)),
            );
            bail!("the catalog did not open");
        }
    };
    let created = committer.create_table_if_absent_named(&SMOKE, table).await;
    checks.record(
        "create the table (or find it)",
        created.is_ok(),
        json!(created.as_ref().map_err(told)),
    );

    let first = append(
        &committer,
        staging.path(),
        table,
        &format!("t8-{run_id}-1"),
        0,
    )
    .await;
    let Some(first) = checks_snapshot(&mut checks, "append through the vended credential", first)
    else {
        bail!("failed checks: {:?}", checks.failed);
    };

    let jwt = token(&http, &settings).await?;
    let rest = IcebergRestCatalog::new(LakehouseCatalogConfig {
        provider: LakehouseCatalogProvider::IcebergRest,
        catalog_uri: settings.catalog_uri.clone(),
        warehouse: settings.warehouse.clone(),
        catalog_token: Some(jwt.clone()),
    })?;

    let tagged = rest.create_tag(table, TAG, first).await;
    checks.record(
        "create_tag",
        tagged.is_ok(),
        json!(tagged.as_ref().map_err(ToString::to_string)),
    );
    let refs = refs_of(&rest, table).await?;
    checks.record(
        "the tag names the first snapshot",
        refs.get(TAG) == Some(&(first, "tag".to_owned())),
        json!(refs),
    );

    let status = set_branch(&http, &settings, &jwt, STALE_BRANCH, first).await?;
    checks.record("make a branch (raw REST)", status == 200, json!(status));
    let second = append(
        &committer,
        staging.path(),
        table,
        &format!("t8-{run_id}-2"),
        ROWS,
    )
    .await;
    let Some(second) = checks_snapshot(&mut checks, "append again: main leaves the branch", second)
    else {
        bail!("failed checks: {:?}", checks.failed);
    };
    let removed = rest.remove_stale_branch(table, STALE_BRANCH).await;
    checks.record(
        "remove_stale_branch removes a branch main has left",
        matches!(removed, Ok(true)),
        json!(removed.as_ref().map_err(ToString::to_string)),
    );

    let status = set_branch(&http, &settings, &jwt, LOAD_BRANCH, second).await?;
    checks.record(
        "make a load branch (raw REST)",
        status == 200,
        json!(status),
    );
    let published = rest.publish_branch(table, LOAD_BRANCH).await;
    checks.record(
        "publish_branch",
        matches!(published, Ok(head) if head == second),
        json!(published.as_ref().map_err(ToString::to_string)),
    );
    let untagged = rest.remove_tag(table, TAG, first).await;
    checks.record(
        "remove_tag",
        untagged.is_ok(),
        json!(untagged.as_ref().map_err(ToString::to_string)),
    );
    let refs = refs_of(&rest, table).await?;
    checks.record(
        "main is the second snapshot and only main is left",
        refs.len() == 1 && refs.get("main") == Some(&(second, "branch".to_owned())),
        json!(refs),
    );

    let mut wrong = settings.vended();
    wrong.insert("s3.access-key-id".to_owned(), WRONG_KEY_ID.to_owned());
    wrong.insert("s3.secret-access-key".to_owned(), WRONG_SECRET.to_owned());
    let refused = match LakehouseDataCommitter::open(wrong).await {
        Ok(committer) => append(
            &committer,
            staging.path(),
            table,
            &format!("t8-{run_id}-3"),
            2 * ROWS,
        )
        .await
        .map(|_| ()),
        Err(error) => Err(error),
    };
    checks.record(
        "with a wrong static key set beside the catalog the append fails (the key wins over the vended one)",
        refused.is_err(),
        json!(refused.as_ref().map_err(told).err()),
    );
    let refs = refs_of(&rest, table).await?;
    checks.record(
        "and commits nothing",
        refs.get("main") == Some(&(second, "branch".to_owned())),
        json!(refs),
    );

    println!("{}", json!({"note": "done", "failed": checks.failed}));
    ensure!(
        checks.failed.is_empty(),
        "failed checks: {:?}",
        checks.failed
    );
    Ok(())
}

/// The new snapshot of an append, recorded as a check.
fn checks_snapshot(
    checks: &mut Checks,
    name: &str,
    outcome: anyhow::Result<AppendOutcome>,
) -> Option<i64> {
    match outcome {
        Ok(AppendOutcome::Appended {
            snapshot,
            added_records,
            data_files,
            ..
        }) => {
            checks.record(
                name,
                added_records == u64::try_from(ROWS).unwrap_or(0),
                json!({"snapshot": snapshot.snapshot_id, "rows": added_records, "files": data_files}),
            );
            Some(snapshot.snapshot_id)
        }
        Ok(AppendOutcome::AlreadyIngested) => {
            checks.record(name, false, json!("already ingested"));
            None
        }
        Err(error) => {
            checks.record(name, false, json!(told(&error)));
            None
        }
    }
}

/// One staged file of `ROWS` synthetic rows, appended in a batch of its own.
async fn append(
    committer: &LakehouseDataCommitter,
    staging: &Path,
    table: &str,
    batch: &str,
    start: i64,
) -> anyhow::Result<AppendOutcome> {
    let field = |id: i32, name: &str, kind: DataType| {
        Field::new(name, kind, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            id.to_string(),
        )]))
    };
    let schema = Arc::new(Schema::new(vec![
        field(1, "id", DataType::Int64),
        field(2, "name", DataType::Utf8),
        field(3, "batch", DataType::Utf8),
    ]));
    let ids: Vec<i64> = (start..start + ROWS).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("synthetic-{id}")).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.clone())),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(vec![batch; ids.len()])),
    ];
    let records = RecordBatch::try_new(Arc::clone(&schema), columns)?;
    let local_path = staging.join(format!("{batch}.parquet"));
    let mut writer = ArrowWriter::try_new(File::create(&local_path)?, schema, None)?;
    writer.write(&records)?;
    writer.close()?;
    let files = [StagedFile {
        local_path,
        name: batch.to_owned(),
    }];
    committer
        .append(DataAppend {
            table_name: table,
            contract: &SMOKE,
            files: &files,
            batch: &[batch.to_owned()],
            already_ingested: &BTreeSet::new(),
            summary: BTreeMap::new(),
        })
        .await
}

/// The table's refs: name to (snapshot, kind).
async fn refs_of(
    rest: &IcebergRestCatalog,
    table: &str,
) -> anyhow::Result<BTreeMap<String, (i64, String)>> {
    let refs = rest
        .load_snapshot_refs(table)
        .await?
        .with_context(|| format!("{table} is not in the catalog"))?;
    Ok(refs
        .refs
        .into_iter()
        .map(|(name, reference)| (name, (reference.snapshot_id, reference.kind)))
        .collect())
}

/// An access token of the principal's client (client credentials). Never printed.
async fn token(http: &reqwest::Client, settings: &Settings) -> anyhow::Result<String> {
    let response = http
        .post(&settings.token_uri)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", settings.client_id.as_str()),
            ("client_secret", settings.client_secret.as_str()),
            ("scope", settings.scope.as_str()),
        ])
        .send()
        .await
        .context("the token endpoint did not answer")?;
    let status = response.status();
    ensure!(status.is_success(), "the token endpoint answered {status}");
    let answer: Value = response.json().await?;
    answer["access_token"]
        .as_str()
        .map(str::to_owned)
        .context("the token endpoint's answer holds no access token")
}

/// Makes `branch` at `snapshot` with one REST commit: iceberg-rust 0.11 has no public call for refs
/// (its `TableCommit` builder is crate-private), and the lanes' branches come from the engine that
/// loads them. Returns the catalog's status.
async fn set_branch(
    http: &reqwest::Client,
    settings: &Settings,
    jwt: &str,
    branch: &str,
    snapshot: i64,
) -> anyhow::Result<u16> {
    let base = settings.catalog_uri.trim_end_matches('/');
    let config: Value = http
        .get(format!("{base}/v1/config"))
        .query(&[("warehouse", settings.warehouse.as_str())])
        .bearer_auth(jwt)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let prefix = config["overrides"]["prefix"]
        .as_str()
        .or_else(|| config["defaults"]["prefix"].as_str())
        .context("the catalog's config names no prefix")?;
    let (namespace, name) = settings
        .table
        .split_once('.')
        .context("the table is namespace.name")?;
    let response = http
        .post(format!("{base}/v1/{prefix}/namespaces/{namespace}/tables/{name}"))
        .bearer_auth(jwt)
        .json(&json!({
            "requirements": [{"type": "assert-ref-snapshot-id", "ref": branch, "snapshot-id": null}],
            "updates": [{"action": "set-snapshot-ref", "ref-name": branch, "type": "branch", "snapshot-id": snapshot}]
        }))
        .send()
        .await?;
    Ok(response.status().as_u16())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_check_writes_only_to_a_smoke_table_and_holds_no_storage_key() {
        let settings = Settings {
            catalog_uri: "http://catalog.invalid:8181/catalog".to_owned(),
            warehouse: "warehouse".to_owned(),
            token_uri: "https://issuer.invalid/oauth2/token".to_owned(),
            scope: "catalog".to_owned(),
            client_id: "client".to_owned(),
            client_secret: "secret".to_owned(),
            table: "silver.w109_rust_smoke".to_owned(),
        };
        let properties = settings.vended();
        assert!(
            properties.keys().all(|key| !key.contains("access-key")
                && !key.contains("secret")
                && !key.contains("session-token")),
            "{:?}",
            properties.keys().collect::<Vec<_>>()
        );
        assert_eq!(properties["credential"], "client:secret");
        assert_eq!(properties["s3.disable-config-load"], "true");
    }
}
