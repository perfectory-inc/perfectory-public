//! The commit of a table of appended row versions (root ADR-0564 §3), against a real
//! Iceberg table: rows diffed against one head are appended only while `main` is that head, an
//! append of no file records the new summary, and a file is one 시도 (`truncate(2, pnu)`).
//!
//! What this prevents: a producer diffs against snapshot S, a compaction or another run commits,
//! and the library re-applies the append to the new head as it does for every append (`iceberg`
//! 0.11.0 reloads the table before each commit attempt). The rows of S's difference then sit on
//! top of rows they were never compared with. On 2026-10-16 `silver.industrial_complexes` held
//! three copies of every complex (4,326 rows) because loads appended with nothing deciding which
//! row was current; an unpinned difference is the same table one race later.

use std::sync::atomic::{AtomicUsize, Ordering};

use foundation_publisher_core::lakehouse_data_commit::HeadMoved;
use iceberg::{table::Table, Namespace, TableCommit};

use super::*;

impl Lineage {
    async fn append_at(
        &self,
        committer: &LakehouseDataCommitter,
        files: &[StagedFile],
        batch: &str,
        expected_head: Option<i64>,
    ) -> anyhow::Result<AppendOutcome> {
        committer
            .append_at_head(
                DataAppend {
                    table_name: self.contract.table_name,
                    contract: self.contract,
                    files,
                    batch: &[batch.to_owned()],
                    already_ingested: &BTreeSet::new(),
                    summary: summary(),
                },
                expected_head,
            )
            .await
    }

    async fn table(&self) -> anyhow::Result<Table> {
        Ok(self
            .catalog
            .load_table(&TableIdent::from_strs(["silver", "parcel_lineage"])?)
            .await?)
    }

    /// Whether a staged file named `name` was uploaded under the table's `data/`.
    async fn uploaded(&self, name: &str) -> anyhow::Result<bool> {
        let location = format!("{}/data/{name}.parquet", self.shape.location);
        Ok(std::fs::exists(
            location.strip_prefix("file://").unwrap_or(&location),
        )?)
    }
}

fn moved(outcome: anyhow::Result<AppendOutcome>) -> anyhow::Result<HeadMoved> {
    let error = outcome.err().context("the append was refused")?;
    error
        .downcast_ref::<HeadMoved>()
        .cloned()
        .with_context(|| format!("the refusal is HeadMoved, got {error:#}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_diffed_against_a_head_are_appended_only_while_main_is_that_head() -> anyhow::Result<()>
{
    let lineage = Lineage::new().await?;
    let stage = lineage.stage()?;
    let committer = &lineage.committer;

    // An empty table has no head: the first build expects none.
    let first = stage.write(&lineage.shape, &lineage_rows("run-a", 0, 500), "run-a")?;
    let (snapshot_a, parent, _) = appended(
        lineage
            .append_at(committer, &[first], "run-a", None)
            .await?,
    )?;
    assert_eq!(parent, None);

    // A second first build (it also saw no head) is refused before it uploads anything.
    let second = stage.write(&lineage.shape, &lineage_rows("run-b", 500, 100), "run-b")?;
    let refused = moved(
        lineage
            .append_at(committer, std::slice::from_ref(&second), "run-b", None)
            .await,
    )?;
    assert_eq!(
        (refused.expected, refused.found),
        (None, Some(snapshot_a.snapshot_id))
    );
    assert!(!lineage.uploaded("run-b").await?);
    assert_eq!(lineage.snapshot_count().await?, 1);

    // Another commit moves main (a compaction, another run): the difference against A is stale.
    let other = stage.write(&lineage.shape, &lineage_rows("run-c", 600, 50), "run-c")?;
    let (snapshot_c, _, _) = appended(lineage.append(&[other], "run-c", &[]).await?)?;
    let refused = moved(
        lineage
            .append_at(
                committer,
                std::slice::from_ref(&second),
                "run-b",
                Some(snapshot_a.snapshot_id),
            )
            .await,
    )?;
    assert_eq!(refused.found, Some(snapshot_c.snapshot_id));
    assert_eq!(lineage.snapshot_count().await?, 2);

    // Planned again from the head it is at, it lands on it.
    let (snapshot_b, parent, added) = appended(
        lineage
            .append_at(committer, &[second], "run-b", Some(snapshot_c.snapshot_id))
            .await?,
    )?;
    assert_eq!((parent, added), (Some(snapshot_c.snapshot_id), 100));
    let scanned = scan_snapshot_rows(lineage.contract, &LocalFiles, &snapshot_b).await?;
    scanned.ensure_complete()?;
    assert_eq!(scanned.rows.len(), 650);
    Ok(())
}

/// A catalog that lets another writer commit just before its `trigger`-th load of the table: the
/// commit that lands between the append's own check of the head and the library's reload.
#[derive(Debug)]
struct Racing {
    inner: Arc<dyn Catalog>,
    loads: AtomicUsize,
    trigger: usize,
    contract: &'static LakehouseTableContract,
    rival: StagedFile,
}

#[async_trait]
impl Catalog for Racing {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        if self.loads.fetch_add(1, Ordering::SeqCst) + 1 == self.trigger {
            LakehouseDataCommitter::new(Arc::clone(&self.inner))
                .append(DataAppend {
                    table_name: self.contract.table_name,
                    contract: self.contract,
                    files: std::slice::from_ref(&self.rival),
                    batch: &["rival".to_owned()],
                    already_ingested: &BTreeSet::new(),
                    summary: BTreeMap::new(),
                })
                .await
                .map_err(|error| {
                    iceberg::Error::new(iceberg::ErrorKind::Unexpected, format!("{error:#}"))
                })?;
        }
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_commit_that_lands_after_the_head_was_checked_still_refuses_the_append(
) -> anyhow::Result<()> {
    let lineage = Lineage::new().await?;
    let stage = lineage.stage()?;
    let first = stage.write(&lineage.shape, &lineage_rows("run-a", 0, 500), "run-a")?;
    let (snapshot_a, _, _) = appended(
        lineage
            .append_at(&lineage.committer, &[first], "run-a", None)
            .await?,
    )?;

    // The append loads the table once to check the head and build its manifest entries; the
    // library loads it again at the commit. The rival commits just before that second load.
    let rival = stage.write(&lineage.shape, &lineage_rows("rival", 900, 10), "rival")?;
    let racing = LakehouseDataCommitter::new(Arc::new(Racing {
        inner: Arc::clone(&lineage.catalog),
        loads: AtomicUsize::new(0),
        trigger: 2,
        contract: lineage.contract,
        rival,
    }));
    let late = stage.write(&lineage.shape, &lineage_rows("run-b", 500, 100), "run-b")?;
    let refused = moved(
        lineage
            .append_at(&racing, &[late], "run-b", Some(snapshot_a.snapshot_id))
            .await,
    )?;
    assert_eq!(refused.expected, Some(snapshot_a.snapshot_id));

    // Main holds the rival's commit and nothing of the refused append.
    let table = lineage.table().await?;
    let head = table
        .metadata()
        .current_snapshot()
        .context("main has a head")?;
    assert_eq!(refused.found, Some(head.snapshot_id()));
    assert_eq!(head.parent_snapshot_id(), Some(snapshot_a.snapshot_id));
    assert_eq!(table.metadata().snapshots().count(), 2);
    assert_eq!(
        head.summary()
            .additional_properties
            .get("foundation.ingest-batch-objects")
            .map(String::as_str),
        Some("rival")
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_append_of_no_file_records_the_summary_and_changes_no_row() -> anyhow::Result<()> {
    let lineage = Lineage::new().await?;
    let stage = lineage.stage()?;
    let first = stage.write(&lineage.shape, &lineage_rows("run-a", 0, 500), "run-a")?;
    let (snapshot_a, _, _) = appended(
        lineage
            .append_at(&lineage.committer, &[first], "run-a", None)
            .await?,
    )?;

    // The inputs moved and no key changed: only the new pins are recorded.
    let (recorded, parent, added) = appended(
        lineage
            .append_at(
                &lineage.committer,
                &[],
                "pins-2",
                Some(snapshot_a.snapshot_id),
            )
            .await?,
    )?;
    assert_eq!((parent, added), (Some(snapshot_a.snapshot_id), 0));
    let table = lineage.table().await?;
    let head = table
        .metadata()
        .current_snapshot()
        .context("main has a head")?;
    assert_eq!(head.snapshot_id(), recorded.snapshot_id);
    let properties = &head.summary().additional_properties;
    assert_eq!(
        properties
            .get("foundation.ingest-batch-objects")
            .map(String::as_str),
        Some("pins-2")
    );
    assert!(properties.contains_key("foundation.source-iceberg-snapshots"));
    let scanned = scan_snapshot_rows(lineage.contract, &LocalFiles, &recorded).await?;
    scanned.ensure_complete()?;
    assert_eq!((scanned.rows.len(), scanned.data_file_count), (500, 1));

    // A blind append still names its rows: a batch with none has nothing to say on any head.
    let message = format!(
        "{:#}",
        lineage
            .append(&[], "empty", &[])
            .await
            .err()
            .context("refused")?
    );
    assert!(message.contains("names no staged file"), "{message}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_is_one_truncated_partition_or_refused() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let contract = contract("gold.parcel_panel")?;
    // A 시도: the first two digits of the PNU (root ADR-0564 §2).
    let catalog = catalog_partitioned(
        &root.path().join("warehouse"),
        contract,
        &[("pnu", Transform::Truncate(2))],
    )
    .await?;
    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let shape = committer.table_shape(contract.table_name).await?;
    let rows = |sido: &str, count: u32| {
        let columns = contract
            .columns
            .iter()
            .map(|column| match (column.name, column.logical_type) {
                ("pnu", _) => {
                    format!("'{sido}999' || lpad(CAST(i AS VARCHAR), 14, '0') AS pnu")
                }
                (name, "long") => format!("CAST(i % 97 AS BIGINT) AS {name}"),
                (name, _) => format!("'{name}-' || CAST(i % 7 AS VARCHAR) AS {name}"),
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("SELECT {columns} FROM range(0, {count}) t(i) ORDER BY pnu")
    };
    let append = |files: Vec<StagedFile>, batch: &'static str| {
        let committer = committer.clone();
        async move {
            committer
                .append_at_head(
                    DataAppend {
                        table_name: contract.table_name,
                        contract,
                        files: &files,
                        batch: &[batch.to_owned()],
                        already_ingested: &BTreeSet::new(),
                        summary: BTreeMap::new(),
                    },
                    None,
                )
                .await
        }
    };

    let two = stage.write(
        &shape,
        &format!(
            "SELECT * FROM (({}) UNION ALL ({})) ORDER BY pnu",
            rows("98", 5),
            rows("99", 5)
        ),
        "two-sido",
    )?;
    let message = format!(
        "{:#}",
        append(vec![two], "two-sido")
            .await
            .err()
            .context("refused")?
    );
    assert!(message.contains("is not one partition"), "{message}");

    let files = vec![
        stage.write(&shape, &rows("98", 300), "sido-98")?,
        stage.write(&shape, &rows("99", 200), "sido-99")?,
    ];
    let (snapshot, _, added) = appended(append(files, "two-files").await?)?;
    assert_eq!(added, 500);
    let scanned = scan_snapshot_rows(contract, &LocalFiles, &snapshot).await?;
    scanned.ensure_complete()?;
    assert_eq!((scanned.rows.len(), scanned.data_file_count), (500, 2));
    // The manifest carries the 시도 as the file's partition.
    let table = catalog
        .load_table(&TableIdent::from_strs(["gold", "parcel_panel"])?)
        .await?;
    let head = table
        .metadata()
        .current_snapshot()
        .context("main has a head")?;
    let manifests = table.manifest_list_reader(head).load().await?;
    let mut partitions = Vec::new();
    for manifest_file in manifests.entries() {
        let manifest = table.manifest_reader().read(manifest_file).await?;
        for entry in manifest.entries() {
            partitions.push(format!("{:?}", entry.data_file().partition()));
        }
    }
    partitions.sort();
    assert_eq!(partitions.len(), 2);
    assert!(
        partitions[0].contains("98") && partitions[1].contains("99"),
        "{partitions:?}"
    );
    Ok(())
}
