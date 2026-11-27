//! Appends Parquet files another engine staged to an Iceberg table, as one data snapshot.
//!
//! The publisher read Iceberg and moved tags and branches, but every data snapshot was a Spark
//! submit: a transform moved to `DuckDB` still needed a Spark session to commit (root ADR-0559
//! §4). This is the commit (root ADR-0560): apache/iceberg-rust writes the manifest, the manifest
//! list and the commit, and re-applies the append when `main` moved underneath it. What is ours is
//! the thin part the library does not publish — the manifest entry of a Parquet file it did not
//! write, read from that file's footer — and the rules a staged file must meet.
//!
//! It only appends, and only to `main`: a table whose contract allows a replacing write, a load
//! staged on a branch and a `MERGE` stay on the Spark commit path (ADR-0560 §4, §5). Reads, tags
//! and branch publishes stay in `lakehouse_infrastructure::IcebergRestCatalog`; nothing here reads
//! rows.
//!
//! A table of appended row versions (root ADR-0564 §3) is appended to with
//! [`LakehouseDataCommitter::append_at_head`]: its rows were diffed against one snapshot of `main`,
//! so they are appended only while `main` is still that snapshot.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

use anyhow::{bail, ensure, Context};
use async_trait::async_trait;
use bytes::Bytes;
use foundation_outbox::object_storage::R2ObjectStorageConfig;
use iceberg::{
    io::FileWrite,
    spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, NestedField,
        NullOrder, PrimitiveLiteral, PrimitiveType, Schema, SortDirection, SortField, SortOrder,
        Struct, Transform, Type, UnboundPartitionSpec,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    transform::create_transform_function,
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use lakehouse_domain::LakehouseTableContract;
use lakehouse_infrastructure::{IcebergSnapshotManifestList, LakehouseCatalogConfig};
use parquet::{
    basic::{ConvertedType, LogicalType, TimeUnit, Type as PhysicalType},
    column::reader::ColumnReader,
    file::{
        metadata::ParquetMetaData,
        reader::{FileReader, SerializedFileReader},
        statistics::Statistics,
    },
    schema::types::ColumnDescriptor,
};
use sha2::{Digest, Sha256};

/// The most one upload request carries. OpenDAL sends each `write` as one request, and
/// iceberg-storage-opendal 0.11 bounds every request by OpenDAL's default 10 s IO timeout, with no
/// property to raise it: the `opendal.io-timeout-ms` and `s3.multipart.part-size-bytes` properties
/// (apache/iceberg-rust#3263, #3179, issue #2977) are on its `main`, after 0.11.0. A whole staged
/// file in one `write` timed out on every retry (`silver.elevator_installation`, 2026-11-21 and
/// 11-22); 8 MiB is over S3's 5 MiB part minimum and takes about 1.5 s at the 6 MB/s the
/// collectors measure against R2.
const UPLOAD_PART_BYTES: u64 = 8 * 1024 * 1024;

/// The summary property that names the batch for a reader of the snapshot log, and the one the
/// re-run guard reads (`lakehouse_ingest.py`: `INGEST_BATCH_TOKEN_KEY`, `INGEST_BATCH_OBJECTS_KEY`;
/// read back by `IcebergRestCatalog::load_ingested_batch_objects`).
const INGEST_BATCH_TOKEN_KEY: &str = "foundation.ingest-batch-token";
const INGEST_BATCH_OBJECTS_KEY: &str = "foundation.ingest-batch-objects";
/// `,` separates identities inside the summary value (`lakehouse_ingest.py`: `OBJECT_NAME_SEPARATOR`).
const OBJECT_NAME_SEPARATOR: char = ',';

/// One column of the table as its current Iceberg schema holds it: what a staged Parquet file
/// must carry as its field id (`DuckDB`: `COPY … (FORMAT parquet, FIELD_IDS {…})`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableField {
    /// Iceberg field id.
    pub id: i32,
    /// Column name.
    pub name: String,
    /// Whether the table refuses a null in it.
    pub required: bool,
    /// The field id of the elements, when the column is a list of plain values (`array<string>`):
    /// a staged file carries it on the list's `element`.
    pub element_id: Option<i32>,
}

/// Where a table's data files go and which columns they carry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableShape {
    /// The table's storage location, without a trailing slash.
    pub location: String,
    /// The columns of the current schema, in schema order.
    pub fields: Vec<TableField>,
}

/// One Parquet file a transform staged on local disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedFile {
    /// Where the file is now.
    pub local_path: PathBuf,
    /// Its name under the table's `data/`, without the extension. Derived from the batch, so
    /// staging the same batch again names the same object and a dead run leaves at most one
    /// batch of uncommitted files behind (root ADR-0560 §2, §7).
    pub name: String,
}

/// One append: every file of it becomes visible in one snapshot, or none does.
pub struct DataAppend<'a> {
    /// `namespace.table` in the catalog (a `_smoke` copy is another table of the same contract).
    pub table_name: &'a str,
    /// The contract the table was created from.
    pub contract: &'a LakehouseTableContract,
    /// The staged files. An append of no file is refused: a batch with no row records itself
    /// through the Spark path's `record` write until this path proves an empty snapshot.
    pub files: &'a [StagedFile],
    /// The load identities of the batch, in the unit the contract's `load` declares (a run id, an
    /// object key). Recorded as `foundation.ingest-batch-objects` in the same commit as the rows.
    pub batch: &'a [String],
    /// What `main` already records as loaded
    /// (`IcebergRestCatalog::load_ingested_batch_objects`): the re-run guard.
    pub already_ingested: &'a BTreeSet<String>,
    /// The other summary properties the job committed with on Spark, in the same value form
    /// (`foundation.content-fingerprint`, `foundation.source-iceberg-snapshots`,
    /// `foundation.keys-changed`, `foundation.build-source-sha`).
    pub summary: BTreeMap<String, String>,
}

/// What an append did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    /// The rows are in `snapshot`, which is now `main`.
    Appended {
        /// The new snapshot and its manifest list, ready to be read.
        snapshot: IcebergSnapshotManifestList,
        /// The snapshot `main` named before, if any.
        parent_snapshot_id: Option<i64>,
        /// Rows the staged files hold.
        added_records: u64,
        /// The storage locations the files were written to.
        data_files: Vec<String>,
    },
    /// `main` already records every identity of the batch: nothing was uploaded or committed.
    AlreadyIngested,
}

/// `main` is not at the snapshot the caller's rows were computed against, so nothing was appended
/// ([`LakehouseDataCommitter::append_at_head`]). The caller plans again from the new head; when the
/// new head records the caller's own batch, an earlier attempt's commit landed and its response
/// was lost.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadMoved {
    /// `namespace.table`.
    pub table_name: String,
    /// The snapshot the rows were diffed against (`None`: a table with no snapshot yet).
    pub expected: Option<i64>,
    /// What `main` names now.
    pub found: Option<i64>,
}

impl std::fmt::Display for HeadMoved {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} moved from snapshot {:?} to {:?} since its rows were computed: nothing was appended",
            self.table_name, self.expected, self.found
        )
    }
}

impl std::error::Error for HeadMoved {}

/// A catalog that loads a table only while its `main` is at one snapshot.
///
/// The library's commit reloads the table before every attempt and re-applies the append to
/// whatever head it finds (`iceberg` 0.11.0 `Transaction::do_commit`): right for a blind append,
/// wrong for rows that are a difference against one head. Its actions cannot be given a
/// requirement of ours (`TransactionAction` is crate-private), so the reload itself is where the
/// head is checked. When it passes, the requirement the library sends
/// (`RefSnapshotIdMatch(main, that head)`) makes the catalog refuse a commit that raced in after.
#[derive(Debug)]
struct HeadPinned {
    inner: Arc<dyn Catalog>,
    expected: Option<i64>,
    /// The head a reload found instead, once one did.
    moved_to: Mutex<Option<Option<i64>>>,
}

#[async_trait]
impl Catalog for HeadPinned {
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
        let loaded = self.inner.load_table(table).await?;
        let found = loaded.metadata().current_snapshot_id();
        if found != self.expected {
            *self.moved_to.lock().unwrap_or_else(PoisonError::into_inner) = Some(found);
            // Not retryable: the rows are a difference against `expected`, and no retry makes
            // `main` that snapshot again.
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!(
                    "main is at snapshot {found:?}, not the {:?} the rows were computed against",
                    self.expected
                ),
            ));
        }
        Ok(loaded)
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

/// Commits staged Parquet files to Iceberg tables of one catalog.
#[derive(Clone)]
pub struct LakehouseDataCommitter {
    catalog: Arc<dyn Catalog>,
}

impl LakehouseDataCommitter {
    /// Commits through `catalog` (a test hands it a catalog over local files).
    #[must_use]
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self { catalog }
    }

    /// Commits through the configured lakehouse REST catalog with the writer token and the writer
    /// key pair — the same variables `IcebergRestCatalog` and `LakehouseObjectReader` read.
    ///
    /// # Errors
    /// Returns an error when a variable is missing or the catalog refuses the configuration.
    pub async fn from_env() -> anyhow::Result<Self> {
        let catalog = LakehouseCatalogConfig::from_env()
            .context("failed to configure the lakehouse catalog for data commits")?;
        let storage = R2ObjectStorageConfig::from_env()
            .context("failed to configure lakehouse R2 writes for data commits")?;
        let mut properties = HashMap::from([
            ("uri".to_owned(), catalog.catalog_uri),
            ("warehouse".to_owned(), catalog.warehouse),
            ("s3.endpoint".to_owned(), storage.endpoint),
            ("s3.access-key-id".to_owned(), storage.access_key_id),
            ("s3.secret-access-key".to_owned(), storage.secret_access_key),
            ("s3.region".to_owned(), storage.region),
            ("s3.path-style-access".to_owned(), "true".to_owned()),
            // The keys above are the whole credential: no instance metadata, no profile files.
            ("s3.disable-ec2-metadata".to_owned(), "true".to_owned()),
            ("s3.disable-config-load".to_owned(), "true".to_owned()),
        ]);
        if let Some(token) = catalog.catalog_token {
            properties.insert("token".to_owned(), token);
        }
        Ok(Self::new(
            crate::lakehouse_rest_catalog::open(properties).await?,
        ))
    }

    /// Creates `contract`'s table when the catalog does not hold it yet, as Spark's
    /// `platform_contracts.ensure_contract_table` creates one: field ids in column order, the
    /// contract's partition fields (a column itself, or `truncate(N, column)` named
    /// `<column>_trunc`), its sort order ascending with nulls first, format version 2, zstd Parquet
    /// and the contract's write distribution. Returns whether it created the table; a table that
    /// exists is left as it is (the deploy's lakehouse migration brings it to its contract).
    ///
    /// A table of appended row versions is born on its first commit (root ADR-0564 §1): nothing
    /// else creates it, and the lakehouse migration leaves an absent table to the load that makes
    /// it.
    ///
    /// # Errors
    /// Refuses a column type or partition entry this has no Iceberg form for, and returns the
    /// catalog's refusal.
    pub async fn create_table_if_absent(
        &self,
        contract: &LakehouseTableContract,
    ) -> anyhow::Result<bool> {
        self.create_table_if_absent_named(contract, contract.table_name)
            .await
    }

    /// [`Self::create_table_if_absent`] under another `namespace.table` of the same contract (a
    /// scratch or `_smoke` copy, as [`DataAppend::table_name`] allows).
    ///
    /// # Errors
    /// As [`Self::create_table_if_absent`].
    pub async fn create_table_if_absent_named(
        &self,
        contract: &LakehouseTableContract,
        table_name: &str,
    ) -> anyhow::Result<bool> {
        let ident = table_ident(table_name)?;
        if self.catalog.table_exists(&ident).await? {
            return Ok(false);
        }
        let (schema, partition, sort_order) = contract_layout(contract)?;
        let range = contract.write_distribution.mode() == "range";
        let mut properties = HashMap::from([
            (
                "write.parquet.compression-codec".to_owned(),
                "zstd".to_owned(),
            ),
            (
                "write.distribution-mode".to_owned(),
                contract.write_distribution.mode().to_owned(),
            ),
        ]);
        if range {
            properties.insert(
                "write.merge.distribution-mode".to_owned(),
                "range".to_owned(),
            );
        }
        let creation = TableCreation::builder()
            .name(ident.name().to_owned())
            .schema(schema)
            .partition_spec(partition)
            .sort_order(sort_order)
            .properties(properties)
            .build();
        match self.catalog.create_table(ident.namespace(), creation).await {
            Ok(_) => Ok(true),
            // Another run created it between the check and the create.
            Err(error) if self.catalog.table_exists(&ident).await.unwrap_or(false) => {
                tracing::info!(table = table_name, %error, "the table was created by another run");
                Ok(false)
            }
            Err(error) => {
                Err(anyhow::Error::new(error).context(format!("failed to create {table_name}")))
            }
        }
    }

    /// The table's location and the field ids a staged file must carry.
    ///
    /// # Errors
    /// Returns an error when the table cannot be loaded or holds a column that is not a plain value.
    pub async fn table_shape(&self, table_name: &str) -> anyhow::Result<TableShape> {
        let table = self.load(table_name).await?;
        let mut fields = Vec::new();
        for field in table.metadata().current_schema().as_struct().fields() {
            let element_id = match &*field.field_type {
                Type::Primitive(_) => None,
                // A list of plain values is staged as Parquet's three-level list.
                Type::List(list) if matches!(*list.element_field.field_type, Type::Primitive(_)) => {
                    Some(list.element_field.id)
                }
                _ => bail!(
                    "{table_name}.{} is neither a plain column nor a list of plain values: it has no staging rule yet",
                    field.name
                ),
            };
            fields.push(TableField {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
                element_id,
            });
        }
        Ok(TableShape {
            location: table.metadata().location().trim_end_matches('/').to_owned(),
            fields,
        })
    }

    /// Uploads the staged files under the table's `data/` and appends them to `main` in one
    /// snapshot that carries the batch record and `summary`.
    ///
    /// # Errors
    /// Refuses, before anything is uploaded, a batch `main` partly holds, an empty append, a file
    /// whose columns are not the table's (set, field id, name, type), a null in a required column,
    /// a missing bound on a column the contract sorts by, and a file that is not one partition.
    /// Returns the library's error when the catalog refuses the commit after its retries.
    pub async fn append(&self, request: DataAppend<'_>) -> anyhow::Result<AppendOutcome> {
        ensure!(
            !request.files.is_empty(),
            "an append to {} names no staged file",
            request.table_name
        );
        self.append_on(request, None, BTreeMap::new()).await
    }

    /// [`Self::append`], only while `main` is still at `expected_head` (the snapshot the staged
    /// rows were computed against; `None` for a table with no snapshot yet): the commit of a table
    /// of appended row versions (root ADR-0564 §3). Its rows are the keys whose content differs
    /// from that snapshot's current rows, so appended over any other head they could bury a newer
    /// version of a key under an older one.
    ///
    /// The request may name no file: the inputs moved and no key changed, and the snapshot only
    /// records the new summary (the input pins), as an empty `MERGE` did, so the next plan starts
    /// from them.
    ///
    /// # Errors
    /// Everything [`Self::append`] refuses, and [`HeadMoved`] (downcast it) when `main` is at
    /// another snapshot before the upload, at the commit, or because another commit won the race:
    /// nothing is committed, and the files this call uploaded are deleted unless `main` names them
    /// (an earlier attempt's commit that landed with its response lost).
    pub async fn append_at_head(
        &self,
        request: DataAppend<'_>,
        expected_head: Option<i64>,
    ) -> anyhow::Result<AppendOutcome> {
        self.append_on(request, Some(expected_head), BTreeMap::new())
            .await
    }

    /// [`Self::append_at_head`], setting `table_properties` on the table in the same catalog
    /// commit as the snapshot (root ADR-0599 §4): a record that must outlive the snapshot's
    /// summary, such as a release's edition (`foundation.edition.<edition>`, root ADR-0328 §3),
    /// is written with the rows or not at all.
    ///
    /// # Errors
    /// What [`Self::append_at_head`] returns.
    pub async fn append_at_head_recording(
        &self,
        request: DataAppend<'_>,
        expected_head: Option<i64>,
        table_properties: BTreeMap<String, String>,
    ) -> anyhow::Result<AppendOutcome> {
        self.append_on(request, Some(expected_head), table_properties)
            .await
    }

    /// Deletes the objects of `uploaded` that `main` names in no snapshot: what an append refused
    /// at the commit left in the table's location. `main` may name them when an earlier attempt's
    /// commit landed and its response was lost; those stay. Nothing here fails the caller: an
    /// object that cannot be deleted is one more orphan, as before.
    async fn delete_unnamed(&self, table_name: &str, uploaded: &[String]) {
        let Ok(table) = self.load(table_name).await else {
            return;
        };
        let targets: BTreeSet<&str> = uploaded.iter().map(String::as_str).collect();
        let Ok(named) = referenced_by_main(&table, &targets).await else {
            return;
        };
        for target in uploaded {
            if !named.contains(target) {
                // An orphan left is not a failure of the append.
                let _ = table.file_io().delete(target).await;
            }
        }
    }

    /// `pinned`: the head the append is valid on, when it is valid on one head only.
    /// `table_properties`: set on the table in the same commit.
    async fn append_on(
        &self,
        request: DataAppend<'_>,
        pinned: Option<Option<i64>>,
        table_properties: BTreeMap<String, String>,
    ) -> anyhow::Result<AppendOutcome> {
        let table_name = request.table_name;
        let batch = batch_identities(request.batch)?;
        let held: Vec<&String> = batch
            .iter()
            .filter(|identity| request.already_ingested.contains(*identity))
            .collect();
        if held.len() == batch.len() {
            return Ok(AppendOutcome::AlreadyIngested);
        }
        ensure!(
            held.is_empty(),
            "{table_name} already holds {held:?} of batch {batch:?}: a batch is appended whole or not at all"
        );
        for key in request.summary.keys() {
            ensure!(
                key != INGEST_BATCH_OBJECTS_KEY && key != INGEST_BATCH_TOKEN_KEY,
                "{key} is the batch record: it is written from the batch, not passed in"
            );
        }

        let table = self.load(table_name).await?;
        if let Some(expected) = pinned {
            let found = table.metadata().current_snapshot_id();
            if found != expected {
                // Before anything is uploaded.
                return Err(HeadMoved {
                    table_name: table_name.to_owned(),
                    expected,
                    found,
                }
                .into());
            }
        }
        let location = table.metadata().location().trim_end_matches('/').to_owned();
        let mut data_files = Vec::with_capacity(request.files.len());
        let mut uploads = Vec::with_capacity(request.files.len());
        for staged in request.files {
            ensure!(
                !staged.name.is_empty()
                    && staged
                        .name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'),
                "staged file name {:?} must be ASCII letters, digits, '-' or '_'",
                staged.name
            );
            let target = format!("{location}/data/{}.parquet", staged.name);
            data_files.push(manifest_entry(
                &table,
                request.contract,
                &staged.local_path,
                &target,
            )?);
            uploads.push((staged.local_path.as_path(), target));
        }
        // Every file is checked before the first is uploaded: a refused batch leaves nothing, and
        // a file `main` already names is never written over (the name comes from the batch, so
        // only a batch the guard above was not told about could reach it).
        let targets: BTreeSet<&str> = uploads.iter().map(|(_, target)| target.as_str()).collect();
        let referenced = referenced_by_main(&table, &targets).await?;
        ensure!(
            referenced.is_empty(),
            "{table_name} already names {referenced:?}: the batch is loaded, and a data file a snapshot names is not rewritten"
        );
        let mut locations = Vec::with_capacity(uploads.len());
        for (local, target) in uploads {
            let staged = File::open(local)
                .with_context(|| format!("failed to read staged file {}", local.display()))?;
            let mut writer = table.file_io().new_output(&target)?.writer().await?;
            upload_in_parts(writer.as_mut(), BufReader::new(staged), UPLOAD_PART_BYTES)
                .await
                .with_context(|| format!("failed to write {target}"))?;
            locations.push(target);
        }

        let added_records = data_files.iter().map(DataFile::record_count).sum();
        let parent_snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
        let mut summary: HashMap<String, String> = request.summary.into_iter().collect();
        summary.insert(INGEST_BATCH_TOKEN_KEY.to_owned(), batch_token(&batch));
        summary.insert(
            INGEST_BATCH_OBJECTS_KEY.to_owned(),
            batch.join(&OBJECT_NAME_SEPARATOR.to_string()),
        );
        let transaction = Transaction::new(&table);
        let transaction = transaction
            .fast_append()
            .add_data_files(data_files)
            .set_snapshot_properties(summary)
            .apply(transaction)?;
        let transaction = if table_properties.is_empty() {
            transaction
        } else {
            table_properties
                .into_iter()
                .fold(
                    transaction.update_table_properties(),
                    |action, (key, value)| action.set(key, value),
                )
                .apply(transaction)?
        };
        let committed = match pinned {
            None => transaction.commit(self.catalog.as_ref()).await,
            Some(expected) => {
                let catalog = HeadPinned {
                    inner: Arc::clone(&self.catalog),
                    expected,
                    moved_to: Mutex::new(None),
                };
                let committed = transaction.commit(&catalog).await;
                let moved_to = *catalog
                    .moved_to
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if let (Err(_), Some(found)) = (&committed, moved_to) {
                    self.delete_unnamed(table_name, &locations).await;
                    return Err(HeadMoved {
                        table_name: table_name.to_owned(),
                        expected,
                        found,
                    }
                    .into());
                }
                committed
            }
        }
        .with_context(|| format!("failed to commit the append to {table_name}"))?;
        let snapshot = committed
            .metadata()
            .current_snapshot()
            .with_context(|| format!("{table_name} has no current snapshot after the append"))?;
        Ok(AppendOutcome::Appended {
            snapshot: IcebergSnapshotManifestList {
                table_name: table_name.to_owned(),
                snapshot_id: snapshot.snapshot_id(),
                snapshot_timestamp_ms: snapshot.timestamp_ms(),
                manifest_list_location: snapshot.manifest_list().to_owned(),
                metadata_location: committed
                    .metadata_location()
                    .map(str::to_owned)
                    .unwrap_or_default(),
            },
            parent_snapshot_id,
            added_records,
            data_files: locations,
        })
    }

    async fn load(&self, table_name: &str) -> anyhow::Result<Table> {
        let ident = table_ident(table_name)?;
        self.catalog
            .load_table(&ident)
            .await
            .with_context(|| format!("failed to load {table_name} for a data commit"))
    }
}

fn table_ident(table_name: &str) -> anyhow::Result<TableIdent> {
    let (namespace, table) = table_name
        .rsplit_once('.')
        .filter(|(namespace, table)| !namespace.is_empty() && !table.is_empty())
        .with_context(|| format!("{table_name} is not namespace.table"))?;
    Ok(TableIdent::new(
        NamespaceIdent::from_strs(namespace.split('.'))?,
        table.to_owned(),
    ))
}

/// The Iceberg type of a contract column's logical type.
fn iceberg_type(logical_type: &str) -> anyhow::Result<PrimitiveType> {
    Ok(match logical_type {
        "string" => PrimitiveType::String,
        "long" => PrimitiveType::Long,
        "int" | "integer" => PrimitiveType::Int,
        "double" => PrimitiveType::Double,
        "boolean" => PrimitiveType::Boolean,
        "date" => PrimitiveType::Date,
        "timestamp" => PrimitiveType::Timestamptz,
        "binary" => PrimitiveType::Binary,
        other => match decimal_type(other) {
            Some((precision, scale)) => PrimitiveType::Decimal { precision, scale },
            None => bail!("no Iceberg type here for the contract type {other}"),
        },
    })
}

/// `decimal(P,S)` of a contract column, as its precision and scale.
fn decimal_type(logical_type: &str) -> Option<(u32, u32)> {
    let (precision, scale) = logical_type
        .strip_prefix("decimal(")?
        .strip_suffix(')')?
        .split_once(',')?;
    Some((precision.trim().parse().ok()?, scale.trim().parse().ok()?))
}

/// A contract's schema (field ids in column order), partition spec and sort order.
fn contract_layout(
    contract: &LakehouseTableContract,
) -> anyhow::Result<(Schema, UnboundPartitionSpec, SortOrder)> {
    let mut fields = Vec::with_capacity(contract.columns.len());
    let mut ids = HashMap::new();
    for (index, column) in contract.columns.iter().enumerate() {
        let id = i32::try_from(index + 1)?;
        let kind = if column.logical_type == "array<string>" {
            // A list's elements take an id after the columns'.
            Type::List(iceberg::spec::ListType::new(Arc::new(
                NestedField::list_element(
                    i32::try_from(contract.columns.len() + index + 1)?,
                    Type::Primitive(PrimitiveType::String),
                    false,
                ),
            )))
        } else {
            Type::Primitive(iceberg_type(column.logical_type)?)
        };
        fields.push(Arc::new(if column.required {
            NestedField::required(id, column.name, kind)
        } else {
            NestedField::optional(id, column.name, kind)
        }));
        ids.insert(column.name, id);
    }
    let id_of = |name: &str| -> anyhow::Result<i32> {
        ids.get(name).copied().with_context(|| {
            format!(
                "{} names {name}, which is not its column",
                contract.table_name
            )
        })
    };
    let mut partition = UnboundPartitionSpec::builder();
    for entry in contract.partition_spec {
        let (column, transform, name) = match entry
            .strip_prefix("truncate(")
            .and_then(|rest| rest.strip_suffix(')'))
        {
            Some(arguments) => {
                let (width, column) = arguments
                    .split_once(',')
                    .with_context(|| format!("{entry} is not truncate(width, column)"))?;
                let column = column.trim();
                (
                    column,
                    Transform::Truncate(width.trim().parse()?),
                    format!("{column}_trunc"),
                )
            }
            None => (*entry, Transform::Identity, (*entry).to_owned()),
        };
        partition = partition.add_partition_field(id_of(column)?, name, transform)?;
    }
    let mut order = SortOrder::builder();
    for column in contract.sort_order {
        order.with_sort_field(
            SortField::builder()
                .source_id(id_of(column)?)
                .transform(Transform::Identity)
                .direction(SortDirection::Ascending)
                .null_order(NullOrder::First)
                .build(),
        );
    }
    let order = if contract.sort_order.is_empty() {
        SortOrder::unsorted_order()
    } else {
        order.with_order_id(1).build_unbound()?
    };
    Ok((
        Schema::builder().with_fields(fields).build()?,
        partition.build(),
        order,
    ))
}

/// Which of `targets` the table's current snapshot already names as live data files.
async fn referenced_by_main(
    table: &Table,
    targets: &BTreeSet<&str>,
) -> anyhow::Result<Vec<String>> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(Vec::new());
    };
    let manifests = table.manifest_list_reader(snapshot).load().await?;
    let mut referenced = Vec::new();
    for manifest_file in manifests.entries() {
        let manifest = table.manifest_reader().read(manifest_file).await?;
        referenced.extend(
            manifest
                .entries()
                .iter()
                .filter(|entry| entry.is_alive() && targets.contains(entry.file_path()))
                .map(|entry| entry.file_path().to_owned()),
        );
    }
    Ok(referenced)
}

/// The batch's identities, sorted and distinct, each fit for the summary value.
fn batch_identities(batch: &[String]) -> anyhow::Result<Vec<String>> {
    ensure!(
        !batch.is_empty(),
        "an append names the batch it loads: the re-run guard reads it"
    );
    for identity in batch {
        ensure!(
            !identity.is_empty() && !identity.contains(OBJECT_NAME_SEPARATOR),
            "batch identity {identity:?} must be non-empty and hold no {OBJECT_NAME_SEPARATOR:?}: the summary separates identities with it"
        );
    }
    Ok(batch
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

/// The batch's name: the digest of its sorted identities (`lakehouse_ingest.ingest_batch_token`).
fn batch_token(sorted_batch: &[String]) -> String {
    Sha256::digest(sorted_batch.join("\n").as_bytes())
        .iter()
        .fold(String::with_capacity(64), |mut hex, byte| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
            hex
        })
}

/// Whether a Parquet column of this physical type and annotation holds the Iceberg primitive.
///
/// A writer may annotate with the logical type, the older converted type, or both (`DuckDB` writes
/// a string column with the converted type alone), so either is read.
fn holds(column: &ColumnDescriptor, primitive: &PrimitiveType) -> bool {
    let logical = column.logical_type_ref();
    let converted = column.converted_type();
    match (primitive, column.physical_type()) {
        (PrimitiveType::String, PhysicalType::BYTE_ARRAY) => {
            matches!(logical, Some(LogicalType::String)) || converted == ConvertedType::UTF8
        }
        (PrimitiveType::Long, PhysicalType::INT64) | (PrimitiveType::Int, PhysicalType::INT32) => {
            matches!(logical, Some(LogicalType::Integer(_)))
                || (logical.is_none()
                    && matches!(
                        converted,
                        ConvertedType::NONE | ConvertedType::INT_32 | ConvertedType::INT_64
                    ))
        }
        // Bytes with no annotation (`DuckDB` writes a BLOB so): an annotated byte array is text or
        // another type, not the table's bytes.
        (PrimitiveType::Binary, PhysicalType::BYTE_ARRAY) => {
            logical.is_none() && converted == ConvertedType::NONE
        }
        (PrimitiveType::Date, PhysicalType::INT32) => {
            matches!(logical, Some(LogicalType::Date)) || converted == ConvertedType::DATE
        }
        // The converted type cannot say "not UTC", so it only ever names an instant.
        (PrimitiveType::Timestamptz, PhysicalType::INT64) => match logical {
            Some(LogicalType::Timestamp(time)) => {
                time.is_adjusted_to_u_t_c && time.unit == TimeUnit::MICROS
            }
            Some(_) => false,
            None => converted == ConvertedType::TIMESTAMP_MICROS,
        },
        (PrimitiveType::Timestamp, PhysicalType::INT64) => matches!(
            logical,
            Some(LogicalType::Timestamp(time))
                if !time.is_adjusted_to_u_t_c && time.unit == TimeUnit::MICROS
        ),
        (PrimitiveType::Double, PhysicalType::DOUBLE)
        | (PrimitiveType::Float, PhysicalType::FLOAT)
        | (PrimitiveType::Boolean, PhysicalType::BOOLEAN) => true,
        // The unscaled value in the integer the precision fits (`DuckDB` writes a decimal of up to 9
        // digits as INT32 and up to 18 as INT64), annotated with the same precision and scale.
        (
            PrimitiveType::Decimal { precision, scale },
            PhysicalType::INT32 | PhysicalType::INT64,
        ) => {
            let (want_precision, want_scale) = (i64::from(*precision), i64::from(*scale));
            let annotated = match logical {
                Some(LogicalType::Decimal(decimal)) => {
                    Some((i64::from(decimal.precision), i64::from(decimal.scale)))
                }
                Some(_) => None,
                None => (converted == ConvertedType::DECIMAL).then(|| {
                    (
                        i64::from(column.type_precision()),
                        i64::from(column.type_scale()),
                    )
                }),
            };
            annotated == Some((want_precision, want_scale))
        }
        _ => false,
    }
}

/// What the footer says about one column across the file's row groups.
#[derive(Default)]
struct ColumnFacts {
    compressed_bytes: u64,
    values: u64,
    /// `None` once any row group omits the count.
    nulls: Option<u64>,
    /// `None` once any row group omits a bound.
    bounds: Option<(Datum, Datum)>,
    row_groups: usize,
    /// The column is the elements of a list: its counts are of elements, empty lists and null
    /// lists together, so they say nothing about the list column itself.
    repeated: bool,
}

/// The manifest entry of one Parquet file another engine wrote: counts, sizes and bounds from its
/// footer, column by Iceberg field id, and its partition.
fn manifest_entry(
    table: &Table,
    contract: &LakehouseTableContract,
    local: &Path,
    target: &str,
) -> anyhow::Result<DataFile> {
    let file = File::open(local)
        .with_context(|| format!("failed to open staged file {}", local.display()))?;
    let file_bytes = file.metadata()?.len();
    let reader = SerializedFileReader::new(file)
        .with_context(|| format!("{} is not a Parquet file", local.display()))?;
    let footer = reader.metadata();
    let schema = table.metadata().current_schema();
    let fields = schema.as_struct().fields();
    let facts = column_facts(footer, fields, local)?;

    for field in fields {
        let Type::Primitive(_) = &*field.field_type else {
            // A list column: its leaf is its elements. Whether a list itself is null is not in the
            // footer (the leaf counts null elements, empty lists and null lists as one), so a
            // required list is the stager's to check before it writes (`check_release`).
            let Type::List(list) = &*field.field_type else {
                bail!("{}: {} is not a plain column", local.display(), field.name);
            };
            ensure!(
                facts.contains_key(&list.element_field.id),
                "{}: column {} is missing",
                local.display(),
                field.name
            );
            continue;
        };
        let fact = facts
            .get(&field.id)
            .with_context(|| format!("{}: column {} is missing", local.display(), field.name))?;
        if field.required {
            ensure!(
                fact.nulls == Some(0),
                "{}: required column {} holds nulls or does not count them ({:?})",
                local.display(),
                field.name,
                fact.nulls
            );
        }
    }
    for sorted in contract.sort_order {
        let Some(field) = schema.field_by_name(sorted) else {
            // A sort term that is not a plain column (a transform) has no bound to carry.
            continue;
        };
        let fact = &facts[&field.id];
        ensure!(
            fact.nulls.is_some() && (fact.bounds.is_some() || fact.nulls == Some(fact.values)),
            "{}: column {} is one the contract sorts by, so a scan skips files on its bounds, and the footer carries none",
            local.display(),
            field.name
        );
    }

    let spec = table.metadata().default_partition_spec();
    let mut partition = Vec::with_capacity(spec.fields().len());
    for part in spec.fields() {
        if matches!(part.transform, Transform::Bucket(_)) {
            // A hash keeps no order: the bounds say nothing, so every value is asked.
            partition.push(Some(Literal::Primitive(one_bucket(
                &reader,
                part.source_id,
                part.transform,
                local,
            )?)));
            continue;
        }
        let fact = facts
            .get(&part.source_id)
            .with_context(|| format!("partition {} names a column the file lacks", part.name))?;
        let (Some((low, high)), Some(0)) = (&fact.bounds, fact.nulls) else {
            bail!(
                "{}: holds a null or no bounds for partition {} ({:?}, nulls {:?}), so it is not one partition",
                local.display(),
                part.name,
                fact.bounds,
                fact.nulls
            );
        };
        let (Some(low), Some(high)) = (
            partition_value(part.transform, low),
            partition_value(part.transform, high),
        ) else {
            bail!(
                "{}: partition {} is {}, and only identity, text truncate and text bucket partitions are appended here (root ADR-0560 §2, ADR-0564 §2)",
                local.display(),
                part.name,
                part.transform
            );
        };
        ensure!(
            low == high,
            "{}: holds more than one value of partition {} ({low:?} to {high:?}), so it is not one partition",
            local.display(),
            part.name
        );
        partition.push(Some(Literal::Primitive(low)));
    }

    let mut column_sizes = HashMap::new();
    let mut value_counts = HashMap::new();
    let mut null_value_counts = HashMap::new();
    let mut lower_bounds = HashMap::new();
    let mut upper_bounds = HashMap::new();
    for (id, fact) in facts {
        column_sizes.insert(id, fact.compressed_bytes);
        if fact.repeated {
            // No count and no bound of a repeated field: they are not the column's.
            continue;
        }
        value_counts.insert(id, fact.values);
        if let Some(nulls) = fact.nulls {
            null_value_counts.insert(id, nulls);
        }
        if let Some((low, high)) = fact.bounds {
            lower_bounds.insert(id, low);
            upper_bounds.insert(id, high);
        }
    }
    let split_offsets = footer
        .row_groups()
        .iter()
        .filter_map(parquet::file::metadata::RowGroupMetaData::file_offset)
        .collect::<Vec<_>>();
    Ok(DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(target.to_owned())
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::from_iter(partition))
        .partition_spec_id(spec.spec_id())
        .record_count(u64::try_from(footer.file_metadata().num_rows())?)
        .file_size_in_bytes(file_bytes)
        .column_sizes(column_sizes)
        .value_counts(value_counts)
        .null_value_counts(null_value_counts)
        .lower_bounds(lower_bounds)
        .upper_bounds(upper_bounds)
        .split_offsets(Some(split_offsets))
        .build()?)
}

/// The bucket a text falls in among `buckets`, by Iceberg's own transform (`bucket(N, column)`):
/// what a stager splits its rows by, and what [`one_bucket`] asks of every value it wrote.
///
/// # Errors
/// The transform cannot be made.
pub fn text_bucket(buckets: u32) -> anyhow::Result<impl Fn(&str) -> anyhow::Result<i32>> {
    let function = create_transform_function(&Transform::Bucket(buckets))?;
    Ok(move |text: &str| {
        match function
            .transform_literal_result(&Datum::string(text))?
            .literal()
        {
            PrimitiveLiteral::Int(bucket) => Ok(*bucket),
            other => bail!("a bucket is a number, not {other:?}"),
        }
    })
}

/// The one bucket every value of a text column of the file falls in, by Iceberg's own transform.
///
/// # Errors
/// The column is not text, holds a null, or holds values of more than one bucket.
fn one_bucket(
    reader: &SerializedFileReader<File>,
    source_id: i32,
    transform: Transform,
    local: &Path,
) -> anyhow::Result<PrimitiveLiteral> {
    let descriptor = reader.metadata().file_metadata().schema_descr();
    let leaf = (0..descriptor.num_columns())
        .find(|index| {
            let column = descriptor.column(*index);
            let info = column.self_type().get_basic_info();
            info.has_id() && info.id() == source_id
        })
        .with_context(|| format!("{}: no column of field id {source_id}", local.display()))?;
    let function = create_transform_function(&transform)?;
    let mut bucket: Option<Datum> = None;
    let mut values = Vec::new();
    for index in 0..reader.num_row_groups() {
        let group = reader.get_row_group(index)?;
        let ColumnReader::ByteArrayColumnReader(mut column) = group.get_column_reader(leaf)? else {
            bail!(
                "{}: a bucket partition is appended for a text column only",
                local.display()
            );
        };
        loop {
            values.clear();
            let (records, read, _) = column.read_records(8192, None, None, &mut values)?;
            if records == 0 {
                break;
            }
            ensure!(
                read == records,
                "{}: the bucket partition's column holds a null",
                local.display()
            );
            for value in &values {
                let found = function.transform_literal_result(&Datum::string(value.as_utf8()?))?;
                match &bucket {
                    None => bucket = Some(found),
                    Some(held) => ensure!(
                        *held == found,
                        "{}: holds values of buckets {held} and {found}, so it is not one partition",
                        local.display()
                    ),
                }
            }
        }
    }
    Ok(bucket
        .with_context(|| format!("{}: holds no row", local.display()))?
        .literal()
        .clone())
}

/// The partition value `transform` gives a column value, for the transforms a staged file may be
/// partitioned by: the value itself, or the first `width` characters of a text (Iceberg truncates
/// text by Unicode code points; a 시도 is `truncate(2, pnu)`, root ADR-0564 §2). `None` for any other
/// transform or type: the file is then refused.
fn partition_value(transform: Transform, value: &Datum) -> Option<PrimitiveLiteral> {
    match (transform, value.literal()) {
        (Transform::Identity, literal) => Some(literal.clone()),
        (Transform::Truncate(width), PrimitiveLiteral::String(text)) => Some(
            PrimitiveLiteral::String(text.chars().take(usize::try_from(width).ok()?).collect()),
        ),
        _ => None,
    }
}

/// Per field id, what every row group of the file says. Refuses a file whose columns are not
/// exactly the table's: an id-less column (another engine could read it only through a name
/// mapping), an id the table lacks, another name or type under an id, a column twice or missing.
fn column_facts(
    footer: &ParquetMetaData,
    fields: &[Arc<NestedField>],
    local: &Path,
) -> anyhow::Result<BTreeMap<i32, ColumnFacts>> {
    // The leaf of a plain column carries the column's id; the leaf of a list, its element's.
    let by_id: HashMap<i32, (&Arc<NestedField>, &Type, bool)> = fields
        .iter()
        .map(|field| match &*field.field_type {
            Type::List(list) => (
                list.element_field.id,
                (field, &*list.element_field.field_type, true),
            ),
            other => (field.id, (field, other, false)),
        })
        .collect();
    let descriptor = footer.file_metadata().schema_descr();
    ensure!(
        descriptor.num_columns() == fields.len(),
        "{}: holds {} columns and the table has {}",
        local.display(),
        descriptor.num_columns(),
        fields.len()
    );
    let mut facts: BTreeMap<i32, ColumnFacts> = BTreeMap::new();
    let mut primitives = HashMap::new();
    for index in 0..descriptor.num_columns() {
        let column = descriptor.column(index);
        let info = column.self_type().get_basic_info();
        ensure!(
            info.has_id(),
            "{}: column {} carries no Iceberg field id",
            local.display(),
            column.name()
        );
        let (field, leaf_type, repeated) = *by_id.get(&info.id()).with_context(|| {
            format!(
                "{}: field id {} ({}) is not in the table schema",
                local.display(),
                info.id(),
                column.name()
            )
        })?;
        // A list's leaf is named `element`; the column's name is the root of its path.
        let named = column.path().parts().first().map(String::as_str);
        ensure!(
            named == Some(field.name.as_str())
                && column.path().parts().len() == if repeated { 3 } else { 1 },
            "{}: field id {} is {} in the file and {} in the table",
            local.display(),
            info.id(),
            column.path().string(),
            field.name
        );
        let Type::Primitive(primitive) = leaf_type else {
            bail!("{}: {} is not a plain column", local.display(), field.name);
        };
        ensure!(
            holds(&column, primitive),
            "{}: column {} is {:?}/{:?}/{:?} in the file and {primitive} in the table",
            local.display(),
            field.name,
            column.physical_type(),
            column.logical_type_ref(),
            column.converted_type()
        );
        ensure!(
            facts
                .insert(
                    info.id(),
                    ColumnFacts {
                        repeated,
                        ..ColumnFacts::default()
                    }
                )
                .is_none(),
            "{}: field id {} appears twice",
            local.display(),
            info.id()
        );
        primitives.insert(index, (info.id(), primitive.clone()));
    }
    for row_group in footer.row_groups() {
        for (index, chunk) in row_group.columns().iter().enumerate() {
            let (id, primitive) = &primitives[&index];
            let fact = facts.entry(*id).or_default();
            fact.compressed_bytes += u64::try_from(chunk.compressed_size())?;
            fact.values += u64::try_from(chunk.num_values())?;
            let statistics = chunk.statistics();
            let nulls = statistics.and_then(Statistics::null_count_opt);
            fact.nulls = match (fact.row_groups, fact.nulls, nulls) {
                (0, _, nulls) => nulls,
                (_, Some(seen), Some(nulls)) => Some(seen + nulls),
                _ => None,
            };
            let all_null = nulls == Some(u64::try_from(chunk.num_values())?);
            let bounds = statistics.and_then(|statistics| bounds_of(statistics, primitive));
            fact.bounds = match (fact.row_groups, fact.bounds.take(), bounds) {
                (0, _, bounds) => bounds,
                (_, Some((low, high)), Some((min, max))) => Some((
                    if min < low { min } else { low },
                    if max > high { max } else { high },
                )),
                // A row group of nulls only has no bound and narrows none.
                (_, seen, None) if all_null => seen,
                _ => None,
            };
            fact.row_groups += 1;
        }
    }
    Ok(facts)
}

/// A column chunk's bounds as Iceberg values. Parquet's plain encoding of a statistic and
/// Iceberg's single-value serialization are the same bytes for every type `holds` admits.
///
/// A decimal is the exception: Parquet's statistic is the little-endian integer of its physical type
/// and Iceberg's is the big-endian two's complement of the unscaled value (iceberg 0.11
/// `Datum::try_from_bytes`), so its bytes are turned around first.
///
/// A binary column records none: its values are whole objects (a parcel's WKB geometry runs to
/// kilobytes), which would sit in every manifest entry and narrow no read; Iceberg's bounds are
/// optional, and Spark's default metrics keep 16 bytes of them at most.
fn bounds_of(statistics: &Statistics, primitive: &PrimitiveType) -> Option<(Datum, Datum)> {
    if *primitive == PrimitiveType::Binary {
        return None;
    }
    let low = statistic_datum(statistics.min_bytes_opt()?, primitive)?;
    let high = statistic_datum(statistics.max_bytes_opt()?, primitive)?;
    (!low.is_nan() && !high.is_nan()).then_some((low, high))
}

/// One Parquet statistic as an Iceberg value of `primitive`.
fn statistic_datum(bytes: &[u8], primitive: &PrimitiveType) -> Option<Datum> {
    if matches!(primitive, PrimitiveType::Decimal { .. }) {
        let unscaled = match bytes.len() {
            4 => i128::from(i32::from_le_bytes(bytes.try_into().ok()?)),
            8 => i128::from(i64::from_le_bytes(bytes.try_into().ok()?)),
            _ => return None,
        };
        return Datum::try_from_bytes(&unscaled.to_be_bytes(), primitive.clone()).ok();
    }
    Datum::try_from_bytes(bytes, primitive.clone()).ok()
}

/// Writes a staged file through `writer` in requests of `part_bytes`: every one but the last is
/// exactly that long (R2's multipart rule), so each finishes inside the IO timeout and one part, not
/// the file, is held in memory. Returns the bytes written.
async fn upload_in_parts(
    writer: &mut dyn FileWrite,
    mut staged: impl Read,
    part_bytes: u64,
) -> anyhow::Result<u64> {
    let mut written = 0_u64;
    loop {
        let mut part = Vec::with_capacity(usize::try_from(part_bytes)?);
        staged.by_ref().take(part_bytes).read_to_end(&mut part)?;
        let length = u64::try_from(part.len())?;
        if length > 0 {
            writer.write(Bytes::from(part)).await?;
            written += length;
        }
        if length < part_bytes {
            break;
        }
    }
    writer.close().await?;
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `FileWrite` that keeps the size of every request it was handed.
    #[derive(Default)]
    struct Requests {
        sizes: Vec<usize>,
        body: Vec<u8>,
        closed: bool,
    }

    #[async_trait]
    impl FileWrite for Requests {
        async fn write(&mut self, bs: Bytes) -> iceberg::Result<()> {
            self.sizes.push(bs.len());
            self.body.extend_from_slice(&bs);
            Ok(())
        }

        async fn close(&mut self) -> iceberg::Result<iceberg::io::FileMetadata> {
            self.closed = true;
            Ok(iceberg::io::FileMetadata {
                size: u64::try_from(self.body.len()).unwrap_or(u64::MAX),
            })
        }
    }

    fn payload(length: usize) -> Vec<u8> {
        (0..length)
            .map(|at| u8::try_from(at % 251).unwrap_or(0))
            .collect()
    }

    #[tokio::test]
    async fn a_file_larger_than_a_part_goes_up_in_equal_parts_and_a_short_last_one(
    ) -> anyhow::Result<()> {
        for (length, sizes) in [
            (2_560, vec![1_024_usize, 1_024, 512]),
            (2_048, vec![1_024, 1_024]),
            (100, vec![100]),
        ] {
            let mut requests = Requests::default();
            let written = upload_in_parts(&mut requests, payload(length).as_slice(), 1_024).await?;
            assert_eq!(written, u64::try_from(length)?);
            assert_eq!(
                requests.sizes, sizes,
                "no request is longer than a part, none empty"
            );
            assert_eq!(requests.body, payload(length));
            assert!(requests.closed);
        }
        Ok(())
    }

    #[tokio::test]
    async fn the_parts_make_the_whole_file_through_the_file_io() -> anyhow::Result<()> {
        let file_io = iceberg::io::FileIO::new_with_memory();
        let target = "memory://staged/elevator-installation-synthetic.parquet";
        let body = payload(5 * 1_024 + 7);
        let mut writer = file_io.new_output(target)?.writer().await?;
        upload_in_parts(writer.as_mut(), body.as_slice(), 1_024).await?;
        assert_eq!(file_io.new_input(target)?.read().await?.to_vec(), body);
        Ok(())
    }

    #[test]
    fn the_batch_record_is_what_the_spark_path_writes() -> anyhow::Result<()> {
        // lakehouse_ingest.ingest_batch_token(["b", "a"]) == sha256("a\nb")
        let batch = batch_identities(&["b".to_owned(), "a".to_owned(), "a".to_owned()])?;
        assert_eq!(batch, ["a", "b"]);
        assert_eq!(
            batch_token(&batch),
            "7e18f737311b2dc3b2f269dd78396b0351f14fb66efa879f768cb23181883c78"
        );
        assert!(batch_identities(&[]).is_err());
        assert!(batch_identities(&["a,b".to_owned()]).is_err());
        Ok(())
    }

    /// `silver.industrial_complexes` carries decimal(18,2) and decimal(5,2) columns: a staged
    /// file's unscaled integer must carry the column's own precision and scale, and its bounds turn
    /// from Parquet's little-endian integer into Iceberg's big-endian unscaled value.
    #[test]
    fn a_decimal_column_holds_only_its_own_precision_and_scale() -> anyhow::Result<()> {
        use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};

        let schema = parse_message_type(
            "message staged { optional int64 area (DECIMAL(18,2)); optional int32 percent (DECIMAL(5,2));              optional int64 other (DECIMAL(18,3)); optional int64 count; }",
        )?;
        let descriptor = SchemaDescriptor::new(Arc::new(schema));
        let decimal = |precision, scale| PrimitiveType::Decimal { precision, scale };
        assert!(holds(&descriptor.column(0), &decimal(18, 2)));
        assert!(holds(&descriptor.column(1), &decimal(5, 2)));
        assert!(!holds(&descriptor.column(2), &decimal(18, 2)));
        assert!(!holds(&descriptor.column(3), &decimal(18, 2)));
        assert!(!holds(&descriptor.column(0), &PrimitiveType::Long));
        assert_eq!(iceberg_type("decimal(18,2)")?, decimal(18, 2));
        assert!(iceberg_type("decimal(18)").is_err());

        let area = statistic_datum(&123_450_i64.to_le_bytes(), &decimal(18, 2))
            .context("a decimal(18,2) bound")?;
        assert_eq!(area.literal(), &PrimitiveLiteral::Int128(123_450));
        let percent = statistic_datum(&(-1_i32).to_le_bytes(), &decimal(5, 2))
            .context("a decimal(5,2) bound")?;
        assert_eq!(percent.literal(), &PrimitiveLiteral::Int128(-1));
        assert!(statistic_datum(&[0; 3], &decimal(5, 2)).is_none());
        Ok(())
    }

    /// `gold.parcel_boundary_served_versions` keeps each parcel's geometry as WKB bytes
    /// (`binary`): the table is created with an Iceberg binary column, a staged `DuckDB` BLOB
    /// (bytes with no annotation) holds it and a text column does not, and no bound is recorded.
    #[test]
    fn a_binary_column_is_bytes_with_no_annotation_and_records_no_bounds() -> anyhow::Result<()> {
        use parquet::data_type::ByteArray;
        use parquet::schema::{parser::parse_message_type, types::SchemaDescriptor};

        let schema = parse_message_type(
            "message staged { required binary geometry_wkb; optional binary pnu (UTF8); }",
        )?;
        let descriptor = SchemaDescriptor::new(Arc::new(schema));
        assert_eq!(iceberg_type("binary")?, PrimitiveType::Binary);
        assert!(holds(&descriptor.column(0), &PrimitiveType::Binary));
        assert!(!holds(&descriptor.column(1), &PrimitiveType::Binary));
        assert!(!holds(&descriptor.column(0), &PrimitiveType::String));

        let statistics = Statistics::new(
            Some(ByteArray::from(vec![1_u8, 2])),
            Some(ByteArray::from(vec![9_u8; 4_096])),
            None,
            Some(0),
            false,
        );
        assert!(bounds_of(&statistics, &PrimitiveType::Binary).is_none());
        assert!(bounds_of(&statistics, &PrimitiveType::String).is_some());

        // The creation the first shadow build refused (2026-11-22 18:11Z).
        let served = lakehouse_domain::industrial_complex_lakehouse_contracts()
            .iter()
            .find(|contract| contract.table_name == "gold.parcel_boundary_served_versions")
            .context("the served parcel boundaries' versions contract")?;
        let (schema, _, _) = contract_layout(served)?;
        let geometry = schema
            .field_by_name("geometry_wkb")
            .context("its geometry column")?;
        assert_eq!(*geometry.field_type, Type::Primitive(PrimitiveType::Binary));
        Ok(())
    }
}
