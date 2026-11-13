//! The publisher's data commit against a real Iceberg table (root ADR-0560 §8): `DuckDB` stages,
//! `lakehouse_data_commit` appends through apache/iceberg-rust, and the publisher's own snapshot
//! scan reads the rows back. The catalog is the library's in-memory one over local files, so the
//! metadata, manifest lists and manifests are the real formats; the REST catalog and R2 are not
//! in it (the scratch-table probe of ADR-0560 covered those).
//!
//! Every PNU here is synthetic (`99999…`).

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::Range,
    path::Path,
    sync::Arc,
};

use anyhow::Context;
use async_trait::async_trait;
use foundation_publisher_core::{
    lakehouse_data_commit::{
        AppendOutcome, DataAppend, LakehouseDataCommitter, StagedFile, TableShape,
    },
    lakehouse_snapshot_scan::{scan_snapshot_rows, LakehouseByteReader},
};
use iceberg::{
    io::LocalFsStorageFactory,
    memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE},
    spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec},
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use lakehouse_infrastructure::IcebergSnapshotManifestList;

use super::DuckDbParquetStage;

mod pinned_head;

/// Reads the objects of a table whose location is a local directory.
struct LocalFiles;

#[async_trait]
impl LakehouseByteReader for LocalFiles {
    async fn read(&self, location: &str) -> anyhow::Result<Vec<u8>> {
        let path = location.strip_prefix("file://").unwrap_or(location);
        std::fs::read(path).with_context(|| format!("failed to read {location}"))
    }

    async fn read_range(&self, location: &str, range: Range<u64>) -> anyhow::Result<Vec<u8>> {
        let body = self.read(location).await?;
        let (start, end) = (usize::try_from(range.start)?, usize::try_from(range.end)?);
        body.get(start..end)
            .map(<[u8]>::to_vec)
            .with_context(|| format!("{location} has no bytes {start}..{end}"))
    }
}

fn contract(name: &str) -> anyhow::Result<&'static LakehouseTableContract> {
    industrial_complex_lakehouse_contracts()
        .iter()
        .find(|contract| contract.table_name == name)
        .with_context(|| format!("no contract {name}"))
}

/// A catalog over `warehouse`, holding `contract`'s table created as the lakehouse creates it:
/// field ids in column order, the contract's identity partition columns.
async fn catalog_with(
    warehouse: &Path,
    contract: &LakehouseTableContract,
) -> anyhow::Result<Arc<dyn Catalog>> {
    let identity = contract
        .partition_spec
        .iter()
        .map(|column| (*column, Transform::Identity))
        .collect::<Vec<_>>();
    catalog_partitioned(warehouse, contract, &identity).await
}

/// [`catalog_with`], the table partitioned by `partitions` (column, transform) instead of the
/// contract's identity columns.
async fn catalog_partitioned(
    warehouse: &Path,
    contract: &LakehouseTableContract,
    partitions: &[(&str, Transform)],
) -> anyhow::Result<Arc<dyn Catalog>> {
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "memory",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_owned(),
                warehouse
                    .to_str()
                    .context("the warehouse path must be UTF-8")?
                    .to_owned(),
            )]),
        )
        .await?;
    let (namespace, table) = contract
        .table_name
        .split_once('.')
        .context("namespace.table")?;
    let namespace = NamespaceIdent::new(namespace.to_owned());
    catalog.create_namespace(&namespace, HashMap::new()).await?;
    let mut fields = Vec::new();
    let mut partition = UnboundPartitionSpec::builder();
    for (index, column) in contract.columns.iter().enumerate() {
        let id = i32::try_from(index + 1)?;
        let kind = Type::Primitive(match column.logical_type {
            "string" => PrimitiveType::String,
            "long" => PrimitiveType::Long,
            "timestamp" => PrimitiveType::Timestamptz,
            other => anyhow::bail!("this test has no Iceberg type for {other}"),
        });
        fields.push(Arc::new(if column.required {
            NestedField::required(id, column.name, kind)
        } else {
            NestedField::optional(id, column.name, kind)
        }));
        if let Some((_, transform)) = partitions.iter().find(|(name, _)| *name == column.name) {
            partition = partition.add_partition_field(id, column.name, *transform)?;
        }
    }
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(table.to_owned())
                .schema(Schema::builder().with_fields(fields).build()?)
                .partition_spec(partition.build())
                .build(),
        )
        .await?;
    Ok(Arc::new(catalog))
}

/// `count` steward lineage rows of run `run`, in the table's sort order.
fn lineage_rows(run: &str, start: u32, count: u32) -> String {
    format!(
        "SELECT CASE WHEN i % 4 = 0 THEN NULL ELSE '99999' || lpad(CAST(i AS VARCHAR), 14, '0') END AS predecessor_pnu, \
                '99999' || lpad(CAST(i + 5000 AS VARCHAR), 14, '0') AS successor_pnu, \
                'renumbered' AS relation, CAST(NULL AS VARCHAR) AS cardinality, \
                '2026-01-01' AS effective_date, 'official' AS grade, 'steward' AS evidence_kind, \
                '{{}}' AS evidence_ref, 'from-1' AS from_snapshot_id, 'to-1' AS to_snapshot_id, \
                'steward-v1' AS rules_version, '{run}' AS derivation_run_id, \
                to_timestamp(1794441600) AS derived_at_utc \
         FROM range({start}, {end}) t(i) ORDER BY to_snapshot_id, successor_pnu",
        end = start + count
    )
}

fn summary() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "foundation.content-fingerprint".to_owned(),
            "rows:500;digest_sum:1".to_owned(),
        ),
        (
            "foundation.source-iceberg-snapshots".to_owned(),
            r#"{"silver.land_transfer_history":"123456789012345678"}"#.to_owned(),
        ),
        ("foundation.keys-changed".to_owned(), "500".to_owned()),
        (
            "foundation.build-source-sha".to_owned(),
            "0000000".to_owned(),
        ),
    ])
}

fn appended(
    outcome: AppendOutcome,
) -> anyhow::Result<(IcebergSnapshotManifestList, Option<i64>, u64)> {
    match outcome {
        AppendOutcome::Appended {
            snapshot,
            parent_snapshot_id,
            added_records,
            ..
        } => Ok((snapshot, parent_snapshot_id, added_records)),
        AppendOutcome::AlreadyIngested => anyhow::bail!("the batch was not appended"),
    }
}

struct Lineage {
    contract: &'static LakehouseTableContract,
    catalog: Arc<dyn Catalog>,
    committer: LakehouseDataCommitter,
    shape: TableShape,
    root: tempfile::TempDir,
}

impl Lineage {
    async fn new() -> anyhow::Result<Self> {
        let root = tempfile::tempdir()?;
        let contract = contract("silver.parcel_lineage")?;
        let catalog = catalog_with(&root.path().join("warehouse"), contract).await?;
        let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
        Ok(Self {
            contract,
            shape: committer.table_shape(contract.table_name).await?,
            committer,
            catalog,
            root,
        })
    }

    /// The transform's `DuckDB`: owned by the test body, never borrowed across an await.
    fn stage(&self) -> anyhow::Result<DuckDbParquetStage> {
        DuckDbParquetStage::open(&self.root.path().join("stage"))
    }

    async fn append(
        &self,
        files: &[StagedFile],
        batch: &str,
        already_ingested: &[&str],
    ) -> anyhow::Result<AppendOutcome> {
        self.committer
            .append(DataAppend {
                table_name: self.contract.table_name,
                contract: self.contract,
                files,
                batch: &[batch.to_owned()],
                already_ingested: &already_ingested
                    .iter()
                    .map(|identity| (*identity).to_owned())
                    .collect::<BTreeSet<_>>(),
                summary: summary(),
            })
            .await
    }

    async fn snapshot_count(&self) -> anyhow::Result<usize> {
        let table = self
            .catalog
            .load_table(&TableIdent::from_strs(["silver", "parcel_lineage"])?)
            .await?;
        Ok(table.metadata().snapshots().count())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn duckdb_staged_rows_are_appended_with_their_record_and_read_back() -> anyhow::Result<()> {
    let lineage = Lineage::new().await?;
    let stage = lineage.stage()?;

    let first = stage.write(&lineage.shape, &lineage_rows("run-a", 0, 500), "run-a")?;
    let (snapshot_a, parent, added) = appended(lineage.append(&[first], "run-a", &[]).await?)?;
    assert_eq!((parent, added), (None, 500));

    let scanned = scan_snapshot_rows(lineage.contract, &LocalFiles, &snapshot_a).await?;
    scanned.ensure_complete()?;
    assert_eq!(scanned.rows.len(), 500);
    assert_eq!(
        (scanned.data_file_count, scanned.deleted_record_count),
        (1, 0)
    );
    let row = scanned
        .rows
        .iter()
        .find(|row| row["successor_pnu"] == "9999900000000005001")
        .context("the staged row is in the scan")?;
    assert_eq!(row["predecessor_pnu"], "9999900000000000001");
    assert_eq!(row["derivation_run_id"], "run-a");
    assert!(row["cardinality"].is_null());
    assert!(
        row["derived_at_utc"]
            .as_str()
            .is_some_and(|text| text.starts_with("2026-11-12")),
        "{}",
        row["derived_at_utc"]
    );

    // The summary carries the batch record the re-run guard reads and the job's own properties.
    let table = lineage
        .catalog
        .load_table(&TableIdent::from_strs(["silver", "parcel_lineage"])?)
        .await?;
    let recorded = &table
        .metadata()
        .current_snapshot()
        .context("a current snapshot")?
        .summary()
        .additional_properties;
    assert_eq!(recorded["foundation.ingest-batch-objects"], "run-a");
    assert_eq!(recorded["foundation.ingest-batch-token"].len(), 64);
    for (key, value) in summary() {
        assert_eq!(recorded.get(&key), Some(&value), "{key}");
    }
    assert_eq!(recorded["added-records"], "500");

    // A second batch lands on top, and the first snapshot still reads as it was (a pinned read).
    let second = stage.write(&lineage.shape, &lineage_rows("run-b", 500, 300), "run-b")?;
    let (snapshot_b, parent, added) =
        appended(lineage.append(&[second], "run-b", &["run-a"]).await?)?;
    assert_eq!((parent, added), (Some(snapshot_a.snapshot_id), 300));
    let both = scan_snapshot_rows(lineage.contract, &LocalFiles, &snapshot_b).await?;
    both.ensure_complete()?;
    assert_eq!((both.rows.len(), both.data_file_count), (800, 2));
    let pinned = scan_snapshot_rows(lineage.contract, &LocalFiles, &snapshot_a).await?;
    assert_eq!(pinned.rows.len(), 500);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_main_already_records_is_not_appended_again() -> anyhow::Result<()> {
    let lineage = Lineage::new().await?;
    let stage = lineage.stage()?;
    let first = stage.write(&lineage.shape, &lineage_rows("run-a", 0, 10), "run-a")?;
    appended(
        lineage
            .append(std::slice::from_ref(&first), "run-a", &[])
            .await?,
    )?;

    // The guard's answer: nothing is uploaded, nothing is committed.
    let again = lineage
        .append(std::slice::from_ref(&first), "run-a", &["run-a"])
        .await?;
    assert_eq!(again, AppendOutcome::AlreadyIngested);
    assert_eq!(lineage.snapshot_count().await?, 1);

    // Without the guard's answer the file `main` names is still not written over or added twice.
    let restaged = stage.write(&lineage.shape, &lineage_rows("run-a", 0, 11), "run-a")?;
    let refused = lineage.append(&[restaged], "run-a", &[]).await;
    let message = format!("{:#}", refused.err().context("the append is refused")?);
    assert!(message.contains("already names"), "{message}");
    assert_eq!(lineage.snapshot_count().await?, 1);
    let table = lineage
        .catalog
        .load_table(&TableIdent::from_strs(["silver", "parcel_lineage"])?)
        .await?;
    let head = table.metadata().current_snapshot().context("head")?;
    let snapshot = IcebergSnapshotManifestList {
        table_name: lineage.contract.table_name.to_owned(),
        snapshot_id: head.snapshot_id(),
        snapshot_timestamp_ms: head.timestamp_ms(),
        manifest_list_location: head.manifest_list().to_owned(),
        metadata_location: String::new(),
    };
    let scanned = scan_snapshot_rows(lineage.contract, &LocalFiles, &snapshot).await?;
    scanned.ensure_complete()?;
    assert_eq!(scanned.rows.len(), 10);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_file_that_is_not_the_tables_is_refused_before_anything_is_written() -> anyhow::Result<()>
{
    let lineage = Lineage::new().await?;
    let stage = lineage.stage()?;
    let refused = |outcome: anyhow::Result<AppendOutcome>| -> anyhow::Result<String> {
        Ok(format!(
            "{:#}",
            outcome.err().context("the append is refused")?
        ))
    };

    // No Iceberg field ids: another engine could read it only through a name mapping.
    let directory = tempfile::tempdir()?;
    let plain = directory.path().join("plain.parquet");
    {
        let other = DuckDbParquetStage::open(directory.path())?;
        let staged = other.write(&lineage.shape, &lineage_rows("run-x", 0, 5), "with-ids")?;
        // Rewrite it without ids through the workspace's own Parquet writer.
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&staged.local_path)?,
        )?
        .build()?;
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(arrow_schema::Schema::new(
            batches[0]
                .schema()
                .fields()
                .iter()
                .map(|field| field.as_ref().clone().with_metadata(HashMap::new()))
                .collect::<Vec<_>>(),
        ));
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&plain)?,
            Arc::clone(&schema),
            None,
        )?;
        for batch in &batches {
            writer.write(&arrow_array::RecordBatch::try_new(
                Arc::clone(&schema),
                batch.columns().to_vec(),
            )?)?;
        }
        writer.close()?;
    }
    let message = refused(
        lineage
            .append(
                &[StagedFile {
                    local_path: plain,
                    name: "run-x".to_owned(),
                }],
                "run-x",
                &[],
            )
            .await,
    )?;
    assert!(message.contains("carries no Iceberg field id"), "{message}");

    // A null in a column the table requires.
    let null_grade = lineage_rows("run-y", 0, 5).replace(
        "'official' AS grade",
        "CASE WHEN i = 3 THEN NULL ELSE 'official' END AS grade",
    );
    let staged = stage.write(&lineage.shape, &null_grade, "run-y")?;
    let message = refused(lineage.append(&[staged], "run-y", &[]).await)?;
    assert!(message.contains("required column grade"), "{message}");

    // Another type under a field id.
    let long_grade =
        lineage_rows("run-z", 0, 5).replace("'official' AS grade", "CAST(1 AS BIGINT) AS grade");
    let staged = stage.write(&lineage.shape, &long_grade, "run-z")?;
    let message = refused(lineage.append(&[staged], "run-z", &[]).await)?;
    assert!(message.contains("column grade is"), "{message}");

    // Part of a batch already loaded, an append of no file, and a batch record passed by hand.
    let staged = stage.write(&lineage.shape, &lineage_rows("run-w", 0, 5), "run-w")?;
    let partly = lineage
        .committer
        .append(DataAppend {
            table_name: lineage.contract.table_name,
            contract: lineage.contract,
            files: std::slice::from_ref(&staged),
            batch: &["run-v".to_owned(), "run-w".to_owned()],
            already_ingested: &BTreeSet::from(["run-v".to_owned()]),
            summary: summary(),
        })
        .await;
    assert!(refused(partly)?.contains("whole or not at all"));
    assert!(refused(lineage.append(&[], "run-w", &[]).await)?.contains("names no staged file"));

    assert_eq!(lineage.snapshot_count().await?, 0);
    let data = lineage
        .root
        .path()
        .join("warehouse/silver/parcel_lineage/data");
    assert!(
        !data.exists() || std::fs::read_dir(&data)?.next().is_none(),
        "a refused append uploaded a file"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_file_is_one_partition_or_refused() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let contract = contract("gold.parcel_panel")?;
    assert_eq!(contract.partition_spec, ["source_snapshot_id"]);
    let catalog = catalog_with(&root.path().join("warehouse"), contract).await?;
    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let shape = committer.table_shape(contract.table_name).await?;
    let rows = |partition: &str, start: u32, count: u32| {
        let columns = contract
            .columns
            .iter()
            .map(|column| match (column.name, column.logical_type) {
                ("pnu", _) => "'99999' || lpad(CAST(i AS VARCHAR), 14, '0') AS pnu".to_owned(),
                ("source_snapshot_id", _) => format!("'{partition}' AS source_snapshot_id"),
                (name, "long") => format!("CAST(i % 97 AS BIGINT) AS {name}"),
                (name, _) => format!("'{name}-' || CAST(i % 7 AS VARCHAR) AS {name}"),
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT {columns} FROM range({start}, {}) t(i) ORDER BY pnu",
            start + count
        )
    };
    let append = |files: Vec<StagedFile>, batch: &'static str| {
        let committer = committer.clone();
        async move {
            committer
                .append(DataAppend {
                    table_name: contract.table_name,
                    contract,
                    files: &files,
                    batch: &[batch.to_owned()],
                    already_ingested: &BTreeSet::new(),
                    summary: BTreeMap::new(),
                })
                .await
        }
    };

    let mixed = stage.write(
        &shape,
        &format!(
            "({}) UNION ALL ({})",
            rows("snap-a", 0, 5),
            rows("snap-b", 5, 5)
        ),
        "mixed",
    )?;
    let message = format!(
        "{:#}",
        append(vec![mixed], "mixed")
            .await
            .err()
            .context("refused")?
    );
    assert!(message.contains("is not one partition"), "{message}");

    let files = vec![
        stage.write(&shape, &rows("snap-a", 0, 1000), "batch-a")?,
        stage.write(&shape, &rows("snap-b", 1000, 500), "batch-b")?,
    ];
    let (snapshot, _, added) = appended(append(files, "two-partitions").await?)?;
    assert_eq!(added, 1500);
    let scanned = scan_snapshot_rows(contract, &LocalFiles, &snapshot).await?;
    scanned.ensure_complete()?;
    assert_eq!((scanned.rows.len(), scanned.data_file_count), (1500, 2));
    Ok(())
}
