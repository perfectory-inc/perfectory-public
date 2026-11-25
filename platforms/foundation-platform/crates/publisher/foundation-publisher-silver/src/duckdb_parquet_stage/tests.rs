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
    spec::{ListType, NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec},
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
};
use lakehouse_domain::{industrial_complex_lakehouse_contracts, LakehouseTableContract};
use lakehouse_infrastructure::IcebergSnapshotManifestList;

use super::DuckDbParquetStage;

mod engine_config;
mod pinned_head;
mod table_at;

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
            "array<string>" => {
                // Element ids follow the columns', as an engine that creates the table gives them.
                let element = i32::try_from(contract.columns.len() + index + 1)?;
                fields.push(Arc::new(NestedField::new(
                    id,
                    column.name,
                    Type::List(ListType::new(Arc::new(NestedField::list_element(
                        element,
                        Type::Primitive(PrimitiveType::String),
                        false,
                    )))),
                    column.required,
                )));
                continue;
            }
            "string" => PrimitiveType::String,
            "long" => PrimitiveType::Long,
            "int" => PrimitiveType::Int,
            "double" => PrimitiveType::Double,
            "date" => PrimitiveType::Date,
            "boolean" => PrimitiveType::Boolean,
            "timestamp" => PrimitiveType::Timestamptz,
            decimal if decimal.starts_with("decimal(") => {
                let (precision, scale) = decimal
                    .trim_start_matches("decimal(")
                    .trim_end_matches(')')
                    .split_once(',')
                    .context("decimal(P,S)")?;
                PrimitiveType::Decimal {
                    precision: precision.trim().parse()?,
                    scale: scale.trim().parse()?,
                }
            }
            other => anyhow::bail!("this test has no Iceberg type for {other}"),
        });
        fields.push(Arc::new(if column.required {
            NestedField::required(id, column.name, kind)
        } else {
            NestedField::optional(id, column.name, kind)
        }));
        if let Some((_, transform)) = partitions.iter().find(|(name, _)| *name == column.name) {
            // Iceberg keeps a column's own name for its identity partition alone.
            let name = if *transform == Transform::Identity {
                column.name.to_owned()
            } else {
                format!("{}_{transform}", column.name).replace(['[', ']'], "_")
            };
            partition = partition.add_partition_field(id, name, *transform)?;
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

/// A hub table keeps every source column in `raw_columns`, a list of text, and is partitioned by
/// its edition (`vintage`): one release's rows are one file of one partition (root ADR-0599).
#[tokio::test(flavor = "multi_thread")]
async fn a_list_of_text_column_is_staged_and_appended() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let contract = contract("silver.building_register_master")?;
    assert_eq!(contract.partition_spec, ["vintage"]);
    let catalog = catalog_with(&root.path().join("warehouse"), contract).await?;
    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let shape = committer.table_shape(contract.table_name).await?;
    let lists = shape
        .fields
        .iter()
        .filter(|field| field.element_id.is_some())
        .map(|field| field.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(lists, ["raw_columns"]);
    let columns = contract
        .columns
        .iter()
        .map(|column| match (column.name, column.logical_type) {
            ("pnu", _) => "'99999' || lpad(CAST(i AS VARCHAR), 14, '0') AS pnu".to_owned(),
            ("vintage", _) => "'209901' AS vintage".to_owned(),
            // A null element and an empty list are values of the column; a null list is not.
            (name, "array<string>") => format!(
                "CASE WHEN i % 5 = 0 THEN []::VARCHAR[] ELSE ['a', NULL, CAST(i AS VARCHAR)] END AS {name}"
            ),
            (name, "long") => format!("CAST(i AS BIGINT) AS {name}"),
            (name, "timestamp") => format!("to_timestamp(4070908800) AS {name}"),
            (name, _) => format!("'{name}-' || CAST(i % 7 AS VARCHAR) AS {name}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let file = stage.write(
        &shape,
        &format!("SELECT {columns} FROM range(0, 40) t(i) ORDER BY pnu"),
        "release",
    )?;
    let outcome = committer
        .append(DataAppend {
            table_name: contract.table_name,
            contract,
            files: &[file],
            batch: &["release".to_owned()],
            already_ingested: &BTreeSet::new(),
            summary: BTreeMap::new(),
        })
        .await?;
    let (_, _, added) = appended(outcome)?;
    assert_eq!(added, 40);
    Ok(())
}

/// The unit areas are partitioned by `bucket(32, mgm_bldrgst_pk)`: a release's changes are written
/// one file set per bucket, and a file of two buckets is refused (its bounds cannot say so).
#[tokio::test(flavor = "multi_thread")]
async fn rows_of_a_hash_partitioned_table_are_staged_one_bucket_a_file() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let contract = contract("silver.building_register_unit_areas")?;
    assert_eq!(contract.partition_spec, ["bucket(32, mgm_bldrgst_pk)"]);
    let catalog = catalog_partitioned(
        &root.path().join("warehouse"),
        contract,
        &[("mgm_bldrgst_pk", Transform::Bucket(32))],
    )
    .await?;
    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let shape = committer.table_shape(contract.table_name).await?;
    let columns = contract
        .columns
        .iter()
        .map(|column| match (column.name, column.logical_type) {
            ("mgm_bldrgst_pk", _) => "'99999-' || CAST(i AS VARCHAR) AS mgm_bldrgst_pk".to_owned(),
            ("pnu", _) => "'99999' || lpad(CAST(i AS VARCHAR), 14, '0') AS pnu".to_owned(),
            (name, "long") => format!("CAST(i AS BIGINT) AS {name}"),
            (name, "int") => format!("CAST(i % 9 AS INTEGER) AS {name}"),
            (name, "double") => format!("CAST(i AS DOUBLE) AS {name}"),
            (name, "timestamp") => format!("to_timestamp(4070908800) AS {name}"),
            (name, _) => format!("'{name}-' || CAST(i % 7 AS VARCHAR) AS {name}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    stage.connection().execute_batch(&format!(
        "CREATE TABLE unit_rows AS SELECT {columns} FROM range(0, 2000) t(i)"
    ))?;
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

    // Planted: every bucket in one file.
    let mixed = stage.write(&shape, "SELECT * FROM unit_rows", "mixed")?;
    let message = format!(
        "{:#}",
        append(vec![mixed], "mixed")
            .await
            .err()
            .context("refused")?
    );
    assert!(message.contains("is not one partition"), "{message}");

    let files = stage.write_bucketed(
        &shape,
        "unit_rows",
        "mgm_bldrgst_pk",
        32,
        "areas",
        256 * 1024 * 1024,
    )?;
    // 2,000 keys over 32 buckets: every bucket holds some.
    assert_eq!(files.len(), 32);
    let (_, _, added) = appended(append(files, "release").await?)?;
    assert_eq!(added, 2000);
    Ok(())
}

#[test]
fn a_stage_opens_only_where_its_jobs_declared_disk_fits() {
    const GIB: u64 = 1 << 30;
    // 54 GiB free, a third of it the spill ceiling: a job that declares 30 GiB fits, 40 do not.
    let ceiling = super::disk_share(54 * GIB);
    assert_eq!(ceiling, 18 * GIB);
    assert!(super::leaves_room(54 * GIB, ceiling, 30 * GIB));
    assert!(!super::leaves_room(54 * GIB, ceiling, 40 * GIB));
    // A disk already under the spill ceiling fits nothing more.
    assert!(!super::leaves_room(10 * GIB, 18 * GIB, 1));
}

/// One synthetic row of `contract` (PNU under the synthetic 99999 code, 2099 times), every column
/// filled by its type.
fn synthetic_row(contract: &LakehouseTableContract) -> anyhow::Result<String> {
    let columns = contract
        .columns
        .iter()
        .map(|column| {
            let value = match (column.name, column.logical_type) {
                ("pnu", _) => "'9999900000000000000'",
                (_, "string") => "'synthetic'",
                (_, "long") => "CAST(99001 AS BIGINT)",
                (_, "int") => "CAST(1 AS INTEGER)",
                (_, "double") => "CAST(1.5 AS DOUBLE)",
                (_, "date") => "DATE '2099-01-01'",
                (_, "boolean") => "true",
                (_, "timestamp") => "to_timestamp(4071006245)",
                (_, other) => anyhow::bail!("this test has no value for {other}"),
            };
            Ok(format!("{value} AS {}", column.name))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("SELECT {}", columns.join(", ")))
}

/// `file` appended as `object` into `table` while it is empty, as a publisher Silver load appends
/// it: a commit that records the object, pinned on the head the rows were compared against (none
/// yet). Returns the commit's parent and its added rows.
async fn first_append(
    catalog: &Arc<dyn Catalog>,
    table: &'static LakehouseTableContract,
    file: StagedFile,
    object: &str,
) -> anyhow::Result<(Option<i64>, u64)> {
    let short = short_name(table)?;
    let object = object.to_owned();
    let (snapshot, parent, added) = appended(
        LakehouseDataCommitter::new(Arc::clone(catalog))
            .append_at_head(
                DataAppend {
                    table_name: table.table_name,
                    contract: table,
                    files: std::slice::from_ref(&file),
                    batch: std::slice::from_ref(&object),
                    already_ingested: &BTreeSet::new(),
                    summary: BTreeMap::new(),
                },
                None,
            )
            .await?,
    )?;
    // The table's own record of the commit: the publisher's snapshot scan reads no `double`
    // column, and some of these tables hold one.
    let loaded = catalog
        .load_table(&TableIdent::from_strs(["silver", short])?)
        .await?;
    let current = loaded
        .metadata()
        .current_snapshot()
        .context("a current snapshot")?;
    assert_eq!(current.snapshot_id(), snapshot.snapshot_id, "{short}");
    let recorded = &current.summary().additional_properties;
    assert_eq!(
        recorded["foundation.ingest-batch-objects"], object,
        "{short}"
    );
    Ok((parent, added))
}

fn short_name(table: &LakehouseTableContract) -> anyhow::Result<&'static str> {
    Ok(table
        .table_name
        .split_once('.')
        .context("namespace.table")?
        .1)
}

/// `table` created empty under `root`, and its write shape.
async fn empty_table(
    root: &Path,
    table: &'static LakehouseTableContract,
) -> anyhow::Result<(Arc<dyn Catalog>, TableShape)> {
    let catalog = catalog_with(&root.join(short_name(table)?), table).await?;
    let shape = LakehouseDataCommitter::new(Arc::clone(&catalog))
        .table_shape(table.table_name)
        .await?;
    Ok((catalog, shape))
}

/// The 차세대 온비드 Silver load's first object into each of its eight empty tables (root
/// ADR-0389), as `load-data-go-kr-onbid-silver` appends it.
#[tokio::test(flavor = "multi_thread")]
async fn an_onbid_object_is_the_first_append_into_each_empty_table() -> anyhow::Result<()> {
    let tables = [
        contract("silver.onbid_real_estate_item")?,
        contract("silver.onbid_item_bid_result")?,
        contract("silver.onbid_real_estate_item_photo")?,
        contract("silver.onbid_real_estate_item_detail")?,
        contract("silver.onbid_item_bid_info")?,
        contract("silver.onbid_announcement")?,
        contract("silver.onbid_announcement_bid_result")?,
        contract("silver.onbid_announcement_detail")?,
    ];
    let root = tempfile::tempdir()?;
    // One stage for the whole test: its files live as long as it does.
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    for table in tables {
        let (catalog, shape) = empty_table(root.path(), table).await?;
        let name = format!("{}-first", short_name(table)?);
        let file = stage.write(&shape, &synthetic_row(table)?, &name)?;
        let object = "bronze/source=synthetic/onbid-20990102t030405z.gz";
        let first = first_append(&catalog, table, file, object).await?;
        assert_eq!(first, (None, 1), "{}", table.table_name);
    }
    Ok(())
}

/// The 법원경매정보 `DuckDB` load's first object into each of the tables its documents write, as
/// `load-courtauction-go-kr-silver` appends it.
///
/// Into a catalog that holds none of them yet: the load makes each table before its first commit.
#[tokio::test(flavor = "multi_thread")]
async fn a_court_auction_object_is_the_first_append_into_each_empty_table() -> anyhow::Result<()> {
    let tables = [
        contract("silver.court_auction_code")?,
        contract("silver.court_auction_appraisal_report")?,
        contract("silver.court_auction_appraisal_report_read")?,
        contract("silver.court_auction_sale_specification_read")?,
        contract("silver.court_auction_dividend_notice")?,
        contract("silver.court_auction_dividend_object")?,
        contract("silver.court_auction_sale_notice")?,
        contract("silver.court_auction_sale_notice_item")?,
        contract("silver.court_auction_case")?,
        contract("silver.court_auction_case_related")?,
        contract("silver.court_auction_case_object")?,
        contract("silver.court_auction_case_party")?,
        contract("silver.court_auction_case_document_read")?,
        contract("silver.court_auction_survey")?,
        contract("silver.court_auction_survey_object")?,
        contract("silver.court_auction_appraisal")?,
        contract("silver.court_auction_case_filing")?,
        contract("silver.court_auction_case_delivery")?,
        contract("silver.court_auction_survey_tenant")?,
        contract("silver.court_auction_item_photo")?,
        contract("silver.court_auction_item_spec")?,
        contract("silver.court_auction_item_schedule")?,
        contract("silver.court_auction_item_object")?,
        contract("silver.court_auction_item_annex")?,
        contract("silver.court_auction_appraisal_point")?,
        contract("silver.court_auction_item_detail")?,
        contract("silver.court_auction_item")?,
        contract("silver.court_auction_sale_result")?,
    ];
    let root = tempfile::tempdir()?;
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    root.path()
                        .join("warehouse")
                        .to_str()
                        .context("the warehouse path must be UTF-8")?
                        .to_owned(),
                )]),
            )
            .await?,
    );
    catalog
        .create_namespace(&NamespaceIdent::new("silver".to_owned()), HashMap::new())
        .await?;
    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    // One stage for the whole test: its files live as long as it does.
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    for table in tables {
        assert!(
            committer.create_table_if_absent(table).await?,
            "{}",
            table.table_name
        );
        let shape = committer.table_shape(table.table_name).await?;
        let name = format!("{}-first", short_name(table)?);
        let file = stage.write(&shape, &synthetic_row(table)?, &name)?;
        let object = "bronze/source=synthetic/ca-20990102t030405z.gz";
        let first = first_append(&catalog, table, file, object).await?;
        assert_eq!(first, (None, 1), "{}", table.table_name);
    }
    Ok(())
}

/// The data.go.kr file data Silver load's first edition into each of its fourteen empty tables
/// (root ADR-0389), as `load-data-go-kr-file-data-silver` appends it.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_data_edition_is_the_first_append_into_each_empty_table() -> anyhow::Result<()> {
    let tables = [
        contract("silver.highway_access_point")?,
        contract("silver.household_count")?,
        contract("silver.dong_population")?,
        contract("silver.railway_station")?,
        contract("silver.port")?,
        contract("silver.airport")?,
        contract("silver.commercial_district")?,
        contract("silver.adm_dong_code_linkage")?,
        contract("silver.school_location")?,
        contract("silver.university")?,
        contract("silver.subway_station")?,
        contract("silver.standard_land_price")?,
        contract("silver.elevator_installation")?,
        contract("silver.knowledge_industry_center")?,
    ];
    let root = tempfile::tempdir()?;
    // One stage for the whole test: its files live as long as it does.
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    for table in tables {
        let (catalog, shape) = empty_table(root.path(), table).await?;
        let name = format!("{}-first", short_name(table)?);
        let file = stage.write(&shape, &synthetic_row(table)?, &name)?;
        let object = "bronze/source=synthetic/file-20990102t030405z.csv";
        let first = first_append(&catalog, table, file, object).await?;
        assert_eq!(first, (None, 1), "{}", table.table_name);
    }
    Ok(())
}

/// The registered factory Silver load's first edition into its empty table (root ADR-0389), as
/// `load-registered-factory-silver` appends it.
#[tokio::test(flavor = "multi_thread")]
async fn a_registered_factory_edition_is_the_first_append_into_the_empty_table(
) -> anyhow::Result<()> {
    let table = contract("silver.registered_factory")?;
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let (catalog, shape) = empty_table(root.path(), table).await?;
    let file = stage.write(&shape, &synthetic_row(table)?, "registered-factory-first")?;
    let object = "bronze/source=synthetic/factory-20990102t030405z.zip";
    let first = first_append(&catalog, table, file, object).await?;
    assert_eq!(first, (None, 1));
    Ok(())
}

/// 2026-11-20 11:43Z: the first file data load on `DuckDB` failed with `TableNotFound` on
/// `silver.elevator_installation`, a table merged that day that no load had made yet. The Spark
/// job created its table; a publisher load does too, on its first commit (root ADR-0564 §1), and
/// leaves a table that exists as it is.
#[tokio::test(flavor = "multi_thread")]
async fn a_load_makes_the_table_no_load_has_made_yet() -> anyhow::Result<()> {
    let table = contract("silver.elevator_installation")?;
    let root = tempfile::tempdir()?;
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    root.path()
                        .join("warehouse")
                        .to_str()
                        .context("the warehouse path must be UTF-8")?
                        .to_owned(),
                )]),
            )
            .await?,
    );
    catalog
        .create_namespace(&NamespaceIdent::new("silver".to_owned()), HashMap::new())
        .await?;
    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    assert!(committer.table_shape(table.table_name).await.is_err());
    assert!(committer.create_table_if_absent(table).await?);
    assert!(!committer.create_table_if_absent(table).await?);
    let shape = committer.table_shape(table.table_name).await?;
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let file = stage.write(&shape, &synthetic_row(table)?, "elevator-first")?;
    let object = "bronze/source=synthetic/file-20990102t030405z.zip";
    let first = first_append(&catalog, table, file, object).await?;
    assert_eq!(first, (None, 1));
    Ok(())
}

/// The parcel registry's first step into its empty table (root ADR-0389), as
/// `load-parcel-registry` appends it: a commit that records the run's id.
#[tokio::test(flavor = "multi_thread")]
async fn a_parcel_registry_step_is_the_first_append_into_the_empty_table() -> anyhow::Result<()> {
    let table = contract("silver.parcel_registry")?;
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let (catalog, shape) = empty_table(root.path(), table).await?;
    let file = stage.write(&shape, &synthetic_row(table)?, "parcel-registry-first")?;
    let run = "parcel-registry-0123456789abcdef01234567";
    let first = first_append(&catalog, table, file, run).await?;
    assert_eq!(first, (None, 1));
    Ok(())
}

/// The official parcel number changes' first file into the empty table (root ADR-0389), as
/// `load-parcel-number-change-history` appends it: a commit that records the file.
#[tokio::test(flavor = "multi_thread")]
async fn a_parcel_number_change_file_is_the_first_append_into_the_empty_table() -> anyhow::Result<()>
{
    let table = contract("silver.parcel_number_change_history")?;
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let (catalog, shape) = empty_table(root.path(), table).await?;
    let file = stage.write(&shape, &synthetic_row(table)?, "parcel-number-change-first")?;
    let object = "bronze/source=synthetic/30527-1--sha256-synthetic.zip";
    let first = first_append(&catalog, table, file, object).await?;
    assert_eq!(first, (None, 1));
    Ok(())
}

/// Root ADR-0599 §4: what must outlive a snapshot's summary (a release's edition, root ADR-0328 §3)
/// is a table property written by the commit that appends the release's rows. An append refused
/// before its commit (`main` is not where the rows were compared) records neither the rows nor
/// the property, and uploads nothing.
#[tokio::test(flavor = "multi_thread")]
async fn an_append_records_its_table_properties_in_its_own_commit() -> anyhow::Result<()> {
    let table = contract("silver.parcel_number_change_history")?;
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let (catalog, shape) = empty_table(root.path(), table).await?;
    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    let ident = TableIdent::from_strs(["silver", short_name(table)?])?;
    let none = BTreeSet::new();
    let edition = |name: &str| {
        BTreeMap::from([(format!("foundation.edition.{name}"), "recorded".to_owned())])
    };

    // Planted: the rows were compared with a head `main` is not at.
    let early = stage.write(&shape, &synthetic_row(table)?, "release-early")?;
    let object = "release-209901".to_owned();
    let refused = committer
        .append_at_head_recording(
            release_append(table, &early, &object, &none),
            Some(1),
            edition("209901"),
        )
        .await;
    assert!(refused.is_err(), "an append on a head main is not at");
    let untouched = catalog.load_table(&ident).await?;
    assert!(untouched.metadata().current_snapshot().is_none());
    assert!(!untouched
        .metadata()
        .properties()
        .contains_key("foundation.edition.209901"));
    let data = root.path().join(short_name(table)?);
    let uploaded = walk(&data)?
        .into_iter()
        .filter(|path| path.extension().is_some_and(|kind| kind == "parquet"))
        .count();
    assert_eq!(uploaded, 0, "a refused append uploads nothing");

    let file = stage.write(&shape, &synthetic_row(table)?, "release-209901")?;
    let (snapshot, parent, added) = appended(
        committer
            .append_at_head_recording(
                release_append(table, &file, &object, &none),
                None,
                edition("209901"),
            )
            .await?,
    )?;
    assert_eq!((parent, added), (None, 1));
    let loaded = catalog.load_table(&ident).await?;
    let metadata = loaded.metadata();
    assert_eq!(metadata.current_snapshot_id(), Some(snapshot.snapshot_id));
    // One commit: the property is in the metadata the snapshot's commit wrote.
    assert_eq!(
        metadata
            .properties()
            .get("foundation.edition.209901")
            .map(String::as_str),
        Some("recorded")
    );
    assert_eq!(
        metadata
            .current_snapshot()
            .context("a snapshot")?
            .summary()
            .additional_properties["foundation.edition"],
        "209901"
    );
    assert_eq!(metadata.history().len(), 1, "one commit, not two");
    Ok(())
}

/// A release's append of one file, recording the release and its edition.
fn release_append<'a>(
    table: &'static LakehouseTableContract,
    file: &'a StagedFile,
    object: &'a String,
    none: &'a BTreeSet<String>,
) -> DataAppend<'a> {
    DataAppend {
        table_name: table.table_name,
        contract: table,
        files: std::slice::from_ref(file),
        batch: std::slice::from_ref(object),
        already_ingested: none,
        summary: BTreeMap::from([("foundation.edition".to_owned(), "209901".to_owned())]),
    }
}

/// Every file under `root`.
fn walk(root: &Path) -> anyhow::Result<Vec<std::path::PathBuf>> {
    let mut found = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                found.push(path);
            }
        }
    }
    Ok(found)
}

/// One synthetic row of each ILIS notice table (notice `99…`, 2099 times), in contract order.
const NOTICE_READ: &str = "SELECT CAST(99001 AS BIGINT) AS notice_sequence, to_timestamp(4071006245) AS requested_at_utc, \
     200 AS http_status, 'read' AS read_outcome, '999990' AS official_complex_code, \
     'synthetic notice' AS notice_title, NULL::VARCHAR AS notice_number, DATE '2099-01-01' AS notice_date, \
     '2' AS notice_type_code, NULL::VARCHAR AS notice_type_raw, '2' AS complex_kind_code, NULL::VARCHAR AS complex_kind_raw, \
     1 AS file_count, 1 AS files_stored, 'bronze/source=synthetic/detail-20990102t030405z-000.gz' AS source_record_id, \
     1 AS source_line_number, '20990102T030405Z' AS source_snapshot_id, to_timestamp(4071006245) AS ingested_at_utc";
const NOTICE_ATTACHMENT: &str = "SELECT CAST(99001 AS BIGINT) AS notice_sequence, repeat('a', 64) AS sha256, 1 AS file_sequence, \
     '999990' AS official_complex_code, 'synthetic file' AS file_title, '.pdf' AS file_extension_raw, \
     CAST(1234 AS BIGINT) AS provider_file_size, '/smUpload/209901' AS provider_file_path, 'f1' AS provider_file_id, \
     'https://example.invalid/f1' AS source_url, 'pdf' AS media_format, CAST(1234 AS BIGINT) AS size_bytes, \
     to_timestamp(4071006246) AS fetched_at_utc, 'bronze/source=synthetic/detail-20990102t030405z-000.gz' AS source_record_id, \
     1 AS source_line_number, '20990102T030405Z' AS source_snapshot_id, to_timestamp(4071006245) AS ingested_at_utc";

/// One synthetic row of each industrial land sale notice table (root ADR-0603), in contract order.
const SALE_NOTICE_READ: &str = "SELECT 'ilis' AS notice_source, '99001' AS notice_id, repeat('f', 64) AS list_fingerprint,      to_timestamp(4071006245) AS requested_at_utc, 200 AS http_status, 'read' AS read_outcome, 1 AS file_count,      1 AS files_stored, 'bronze/source=synthetic/detail-20990102t030405z-000.gz' AS source_record_id,      1 AS source_line_number, '20990102T030405Z' AS source_snapshot_id, to_timestamp(4071006245) AS ingested_at_utc";
const SALE_NOTICE_ATTACHMENT: &str = "SELECT 'ilis' AS notice_source, '99001' AS notice_id, repeat('a', 64) AS sha256,      1 AS file_sequence, 'synthetic file' AS file_title, 'f1' AS provider_file_id, CAST(1234 AS BIGINT) AS provider_file_size,      'https://example.invalid/f1' AS source_url, 'pdf' AS media_format, CAST(1234 AS BIGINT) AS size_bytes,      to_timestamp(4071006246) AS fetched_at_utc, 'bronze/source=synthetic/detail-20990102t030405z-000.gz' AS source_record_id,      1 AS source_line_number, '20990102T030405Z' AS source_snapshot_id, to_timestamp(4071006245) AS ingested_at_utc";

/// The ILIS notice Silver load's first object into each of its empty tables (root ADR-0389), as
/// `load-industrial-complex-notice-silver` appends it, and the industrial land sale notice load's
/// (`load-industrial-land-sale-notice-silver`, root ADR-0603) the same way: a commit that records the object, pinned
/// on the head the rows were computed against (none yet). An object with no row of a table is
/// recorded too (an empty commit), and an object `main` records is not appended again.
#[tokio::test(flavor = "multi_thread")]
// One loop over the four tables; the first-load guard reads the append from this body itself
// (test_silver_first_load_iceberg.py), so it is not split into a helper.
#[allow(clippy::too_many_lines)]
async fn a_notice_object_is_the_first_append_into_each_empty_table_and_one_with_no_row_is_recorded(
) -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    for (table, rows, key, value) in [
        (
            contract("silver.industrial_complex_notice_detail")?,
            NOTICE_READ,
            "notice_sequence",
            serde_json::json!(99001),
        ),
        (
            contract("silver.industrial_complex_notice_attachment")?,
            NOTICE_ATTACHMENT,
            "notice_sequence",
            serde_json::json!(99001),
        ),
        (
            contract("silver.industrial_land_sale_notice_read")?,
            SALE_NOTICE_READ,
            "notice_id",
            serde_json::json!("99001"),
        ),
        (
            contract("silver.industrial_land_sale_notice_attachment")?,
            SALE_NOTICE_ATTACHMENT,
            "notice_id",
            serde_json::json!("99001"),
        ),
    ] {
        let short = table
            .table_name
            .split_once('.')
            .context("namespace.table")?
            .1;
        let catalog = catalog_with(&root.path().join(short), table).await?;
        let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
        let shape = committer.table_shape(table.table_name).await?;
        let file = stage.write(&shape, rows, &format!("{short}-first"))?;
        let first = "bronze/source=synthetic/detail-20990102t030405z-000.gz".to_owned();
        let (snapshot, parent, added) = appended(
            committer
                .append_at_head(
                    DataAppend {
                        table_name: table.table_name,
                        contract: table,
                        files: std::slice::from_ref(&file),
                        batch: std::slice::from_ref(&first),
                        already_ingested: &BTreeSet::new(),
                        summary: BTreeMap::new(),
                    },
                    None,
                )
                .await?,
        )?;
        assert_eq!((parent, added), (None, 1), "{short}");
        let scanned = scan_snapshot_rows(table, &LocalFiles, &snapshot).await?;
        scanned.ensure_complete()?;
        assert_eq!(scanned.rows.len(), 1, "{short}");
        assert_eq!(scanned.rows[0][key], value, "{short}");

        // The next object holds no row of this table: its commit only records it.
        let second = "bronze/source=synthetic/detail-20990102t030405z-001.gz".to_owned();
        let held = BTreeSet::from([first.clone()]);
        let (empty, parent, added) = appended(
            committer
                .append_at_head(
                    DataAppend {
                        table_name: table.table_name,
                        contract: table,
                        files: &[],
                        batch: std::slice::from_ref(&second),
                        already_ingested: &held,
                        summary: BTreeMap::new(),
                    },
                    Some(snapshot.snapshot_id),
                )
                .await?,
        )?;
        assert_eq!((parent, added), (Some(snapshot.snapshot_id), 0), "{short}");
        let loaded = catalog
            .load_table(&TableIdent::from_strs(["silver", short])?)
            .await?;
        let recorded = &loaded
            .metadata()
            .current_snapshot()
            .context("a current snapshot")?
            .summary()
            .additional_properties;
        assert_eq!(
            recorded["foundation.ingest-batch-objects"], second,
            "{short}"
        );
        let still = scan_snapshot_rows(table, &LocalFiles, &empty).await?;
        assert_eq!(still.rows.len(), 1, "{short}: the empty commit adds no row");

        // Recorded objects are not appended again.
        let both = BTreeSet::from([first.clone(), second.clone()]);
        let again = committer
            .append_at_head(
                DataAppend {
                    table_name: table.table_name,
                    contract: table,
                    files: &[],
                    batch: std::slice::from_ref(&second),
                    already_ingested: &both,
                    summary: BTreeMap::new(),
                },
                Some(empty.snapshot_id),
            )
            .await?;
        assert_eq!(again, AppendOutcome::AlreadyIngested, "{short}");
    }
    Ok(())
}

/// The Silver-to-Postgres loads read millions of rows through this (root ADR-0565): each row is one
/// JSON object, handed over as it streams, and the first refusal stops the read.
#[test]
fn rows_stream_as_json_objects_and_a_refusal_stops_the_read() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let stage = DuckDbParquetStage::open(root.path())?;
    let rows = stage.json_rows(
        "SELECT range AS n, '99999' || range::VARCHAR AS pnu, NULL::VARCHAR AS absent FROM range(5000)",
    )?;
    assert_eq!(rows.len(), 5000, "every vector of the stream");
    assert_eq!(rows[1], r#"{"n":1,"pnu":"999991","absent":null}"#);

    let mut seen = 0;
    let refused = stage.each_json_row("SELECT range AS n FROM range(5000)", |_| {
        seen += 1;
        anyhow::ensure!(seen < 3000, "the reader refused row {seen}");
        Ok(())
    });
    assert!(refused.is_err());
    assert_eq!(seen, 3000, "nothing after the refusal");
    Ok(())
}

/// The industrial-complex Silver load (`load-industrial-complex-silver`, root ADR-0559): the stage's
/// rows of a synthetic export are the first append into the empty table of the contract, its two
/// decimal columns included, recorded under the export's input objects.
#[tokio::test(flavor = "multi_thread")]
async fn an_industrial_complex_export_is_the_first_append_into_the_empty_table(
) -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let table = contract("silver.industrial_complexes")?;
    let catalog = catalog_with(&root.path().join("industrial_complexes"), table).await?;
    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    let shape = committer.table_shape(table.table_name).await?;
    let export = root.path().join("export.jsonl");
    std::fs::write(
        &export,
        include_str!("../industrial_complex_silver_stage/fixtures/export.jsonl"),
    )?;
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    let staged = crate::industrial_complex_silver_stage::stage_export(
        &stage,
        &shape,
        table,
        &export,
        None,
        "industrial-complexes-first",
    )?;
    assert_eq!((staged.rows, staged.changed), (2, 2));
    let inputs = vec!["bronze/source=synthetic/profile-20991231DS99999-1.zip".to_owned()];
    let (snapshot, parent, added) = appended(
        committer
            .append_at_head(
                DataAppend {
                    table_name: table.table_name,
                    contract: table,
                    files: staged.file.as_slice(),
                    batch: &inputs,
                    already_ingested: &BTreeSet::new(),
                    summary: BTreeMap::new(),
                },
                None,
            )
            .await?,
    )?;
    assert_eq!((parent, added), (None, 2), "both complexes are added");
    let scanned = scan_snapshot_rows(table, &LocalFiles, &snapshot).await?;
    scanned.ensure_complete()?;
    assert_eq!(scanned.rows.len(), 2);
    let codes = scanned
        .rows
        .iter()
        .map(|row| {
            row["official_complex_code"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        codes,
        BTreeSet::from(["99999A".to_owned(), "99999B".to_owned()])
    );
    Ok(())
}

/// The FLOOR cycle's load (`floor_load`, root ADR-0599): a synthetic edition written as the native
/// export writes it (instants as zone-less milliseconds) is checked, staged one file per bucket of
/// the table's `bucket(16, mgm_bldrgst_pk)`, and is the first append into the empty table, recorded
/// under the edition.
#[tokio::test(flavor = "multi_thread")]
async fn a_floor_edition_is_the_first_append_into_the_empty_table() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let table = contract("silver.building_register_floors")?;
    let catalog = catalog_partitioned(
        &root.path().join("building_register_floors"),
        table,
        &[("mgm_bldrgst_pk", Transform::Bucket(16))],
    )
    .await?;
    let committer = LakehouseDataCommitter::new(Arc::clone(&catalog));
    let shape = committer.table_shape(table.table_name).await?;
    let stage = DuckDbParquetStage::open(&root.path().join("stage"))?;
    // The stage reads and writes only its own directory: the handoff is put there.
    let handoff = std::fs::read_dir(root.path().join("stage"))?
        .next()
        .context("the stage directory")??
        .path()
        .join("handoff");
    std::fs::create_dir_all(&handoff)?;
    let edition = format!("building-register-floor-content-v1-{}", "9".repeat(64));
    stage.connection().execute_batch(&format!(
        "COPY (SELECT 'floor-' || i AS floor_row_id, '99999-' || CAST(i % 7 AS VARCHAR) AS mgm_bldrgst_pk, \
         NULL::VARCHAR AS floor_type_code_raw, NULL::VARCHAR AS floor_type_name_raw, \
         NULL::VARCHAR AS floor_number_raw, NULL::VARCHAR AS floor_label_raw, 'above_ground' AS floor_kind, \
         CAST(i AS INTEGER) AS floor_number, CAST(i AS INTEGER) AS floor_index, \
         NULL::VARCHAR AS floor_display_ko, 'accepted' AS normalization_status, \
         'synthetic' AS normalization_reason, 'record-' || i AS source_record_id, \
         '{edition}' AS source_snapshot_id, 'bronze/source=synthetic/20991231.zip' AS bronze_object_key, \
         CAST(i AS BIGINT) AS source_line_number, \
         CAST(TIMESTAMP '2099-12-01 00:00:00' AS TIMESTAMP_MS) AS valid_from_utc, \
         NULL::TIMESTAMP_MS AS valid_to_utc, \
         CAST(TIMESTAMP '2099-12-31 12:00:00' AS TIMESTAMP_MS) AS ingested_at_utc, \
         sha256('row-' || i) AS row_checksum_sha256 FROM range(40) t(i)) \
         TO '{}/part-00000.parquet' (FORMAT parquet)",
        super::sql_path(&handoff)?
    ))?;
    let staged = crate::building_register_floor_silver_export::floor_load::stage_edition(
        &stage,
        table,
        &shape,
        &handoff,
        &edition,
        Some(40),
    )?;
    assert_eq!(staged.rows, 40);
    // Seven keys: at most seven of the sixteen buckets hold rows, one file each.
    assert!(
        (1..=7).contains(&staged.files.len()),
        "{}",
        staged.files.len()
    );
    let (snapshot, parent, added) = appended(
        committer
            .append_at_head(
                DataAppend {
                    table_name: table.table_name,
                    contract: table,
                    files: &staged.files,
                    batch: std::slice::from_ref(&edition),
                    already_ingested: &BTreeSet::new(),
                    summary: BTreeMap::new(),
                },
                None,
            )
            .await?,
    )?;
    assert_eq!(
        (parent, added),
        (None, 40),
        "the edition is the table's first commit"
    );
    let scanned = scan_snapshot_rows(table, &LocalFiles, &snapshot).await?;
    scanned.ensure_complete()?;
    assert_eq!(scanned.rows.len(), 40);
    assert!(scanned
        .rows
        .iter()
        .all(|row| row["source_snapshot_id"] == edition.as_str()));
    // A release check refuses an edition of another identity before anything is staged.
    assert!(
        crate::building_register_floor_silver_export::floor_load::stage_edition(
            &stage,
            table,
            &shape,
            &handoff,
            "building-register-floor-content-v1-other",
            Some(40),
        )
        .is_err()
    );
    Ok(())
}

/// What `DuckDbParquetStage::release_replaced_tables` relies on: in a database file, a table that
/// `CREATE OR REPLACE TABLE … AS` replaced gives its blocks to later tables once a checkpoint ran,
/// so a build that replaces its table once per part keeps the file near one part's tables, not the
/// sum of them (2026-11-22: the served parcel boundaries' stage held every 시도's at once).
#[test]
fn a_checkpoint_lets_the_next_part_reuse_a_replaced_tables_blocks() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let connection = duckdb::Connection::open(directory.path().join("stage.duckdb"))?;
    let part = |n: u32| {
        format!(
            "CREATE OR REPLACE TABLE built AS SELECT range AS pnu, \
             (md5(range::VARCHAR || '-{n}') || md5(range::VARCHAR || '+{n}'))::BLOB AS geometry_wkb \
             FROM range(300000)"
        )
    };
    let blocks = |connection: &duckdb::Connection| -> anyhow::Result<(i64, i64)> {
        Ok(connection.query_row(
            "SELECT total_blocks, used_blocks FROM pragma_database_size()",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    };
    connection.execute_batch(&part(0))?;
    connection.execute_batch("CHECKPOINT")?;
    let (_, one_part) = blocks(&connection)?;
    assert!(one_part > 4, "a part fills blocks: {one_part}");
    for n in 1..8 {
        connection.execute_batch(&part(n))?;
        connection.execute_batch("CHECKPOINT")?;
    }
    let (total, used) = blocks(&connection)?;
    assert!(
        total <= 3 * one_part,
        "eight parts replaced with a checkpoint after each hold {total} blocks, one part {one_part} (used {used})"
    );
    Ok(())
}
