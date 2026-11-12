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

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use foundation_outbox::object_storage::R2ObjectStorageConfig;
use iceberg::{
    spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, NestedField,
        PrimitiveType, Struct, Transform, Type,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    Catalog, CatalogBuilder, NamespaceIdent, TableIdent,
};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;
use lakehouse_domain::LakehouseTableContract;
use lakehouse_infrastructure::{IcebergSnapshotManifestList, LakehouseCatalogConfig};
use parquet::{
    basic::{LogicalType, TimeUnit, Type as PhysicalType},
    file::{
        metadata::ParquetMetaData,
        reader::{FileReader, SerializedFileReader},
        statistics::Statistics,
    },
    schema::types::ColumnDescriptor,
};
use sha2::{Digest, Sha256};

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
        /// The new snapshot, ready for `lakehouse_snapshot_scan`.
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
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                customized_credential_load: None,
            }))
            .load("lakehouse", properties)
            .await
            .context("failed to open the lakehouse REST catalog for data commits")?;
        Ok(Self::new(Arc::new(catalog)))
    }

    /// The table's location and the field ids a staged file must carry.
    ///
    /// # Errors
    /// Returns an error when the table cannot be loaded or holds a column that is not a plain value.
    pub async fn table_shape(&self, table_name: &str) -> anyhow::Result<TableShape> {
        let table = self.load(table_name).await?;
        let mut fields = Vec::new();
        for field in table.metadata().current_schema().as_struct().fields() {
            ensure!(
                matches!(*field.field_type, Type::Primitive(_)),
                "{table_name}.{} is not a plain column: a nested column has no staging rule yet",
                field.name
            );
            fields.push(TableField {
                id: field.id,
                name: field.name.clone(),
                required: field.required,
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
        ensure!(
            !request.files.is_empty(),
            "an append to {table_name} names no staged file"
        );
        for key in request.summary.keys() {
            ensure!(
                key != INGEST_BATCH_OBJECTS_KEY && key != INGEST_BATCH_TOKEN_KEY,
                "{key} is the batch record: it is written from the batch, not passed in"
            );
        }

        let table = self.load(table_name).await?;
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
            let body = std::fs::read(local)
                .with_context(|| format!("failed to read staged file {}", local.display()))?;
            table
                .file_io()
                .new_output(&target)?
                .write(Bytes::from(body))
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
        let committed = transaction
            .fast_append()
            .add_data_files(data_files)
            .set_snapshot_properties(summary)
            .apply(transaction)?
            .commit(self.catalog.as_ref())
            .await
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
        let (namespace, table) = table_name
            .rsplit_once('.')
            .filter(|(namespace, table)| !namespace.is_empty() && !table.is_empty())
            .with_context(|| format!("{table_name} is not namespace.table"))?;
        let ident = TableIdent::new(
            NamespaceIdent::from_strs(namespace.split('.'))?,
            table.to_owned(),
        );
        self.catalog
            .load_table(&ident)
            .await
            .with_context(|| format!("failed to load {table_name} for a data commit"))
    }
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

/// The Iceberg primitive a Parquet column of this physical and logical type holds, when the pair
/// is one this path has a rule for.
fn holds(column: &ColumnDescriptor, primitive: &PrimitiveType) -> bool {
    let logical = column.logical_type_ref();
    match (primitive, column.physical_type()) {
        (PrimitiveType::String, PhysicalType::BYTE_ARRAY) => {
            matches!(logical, Some(LogicalType::String))
        }
        (PrimitiveType::Long, PhysicalType::INT64) | (PrimitiveType::Int, PhysicalType::INT32) => {
            matches!(logical, None | Some(LogicalType::Integer(_)))
        }
        (PrimitiveType::Date, PhysicalType::INT32) => matches!(logical, Some(LogicalType::Date)),
        (PrimitiveType::Timestamptz, PhysicalType::INT64) => matches!(
            logical,
            Some(LogicalType::Timestamp(time))
                if time.is_adjusted_to_u_t_c && time.unit == TimeUnit::MICROS
        ),
        (PrimitiveType::Timestamp, PhysicalType::INT64) => matches!(
            logical,
            Some(LogicalType::Timestamp(time))
                if !time.is_adjusted_to_u_t_c && time.unit == TimeUnit::MICROS
        ),
        (PrimitiveType::Double, PhysicalType::DOUBLE)
        | (PrimitiveType::Float, PhysicalType::FLOAT)
        | (PrimitiveType::Boolean, PhysicalType::BOOLEAN) => true,
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
        ensure!(
            part.transform == Transform::Identity,
            "{}: partition {} is {}, and only identity partitions are appended here (root ADR-0560 §2)",
            local.display(),
            part.name,
            part.transform
        );
        let fact = facts
            .get(&part.source_id)
            .with_context(|| format!("partition {} names a column the file lacks", part.name))?;
        match (&fact.bounds, fact.nulls) {
            (Some((low, high)), Some(0)) if low == high => {
                partition.push(Some(Literal::Primitive(low.literal().clone())));
            }
            (bounds, nulls) => bail!(
                "{}: holds more than one value of partition {} ({bounds:?}, nulls {nulls:?}), so it is not one partition",
                local.display(),
                part.name
            ),
        }
    }

    let mut column_sizes = HashMap::new();
    let mut value_counts = HashMap::new();
    let mut null_value_counts = HashMap::new();
    let mut lower_bounds = HashMap::new();
    let mut upper_bounds = HashMap::new();
    for (id, fact) in facts {
        column_sizes.insert(id, fact.compressed_bytes);
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

/// Per field id, what every row group of the file says. Refuses a file whose columns are not
/// exactly the table's: an id-less column (another engine could read it only through a name
/// mapping), an id the table lacks, another name or type under an id, a column twice or missing.
fn column_facts(
    footer: &ParquetMetaData,
    fields: &[Arc<NestedField>],
    local: &Path,
) -> anyhow::Result<BTreeMap<i32, ColumnFacts>> {
    let by_id: HashMap<i32, &Arc<NestedField>> =
        fields.iter().map(|field| (field.id, field)).collect();
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
        let field = by_id.get(&info.id()).with_context(|| {
            format!(
                "{}: field id {} ({}) is not in the table schema",
                local.display(),
                info.id(),
                column.name()
            )
        })?;
        ensure!(
            field.name == column.name(),
            "{}: field id {} is {} in the file and {} in the table",
            local.display(),
            field.id,
            column.name(),
            field.name
        );
        let Type::Primitive(primitive) = &*field.field_type else {
            bail!("{}: {} is not a plain column", local.display(), field.name);
        };
        ensure!(
            holds(&column, primitive),
            "{}: column {} is {:?}/{:?} in the file and {primitive} in the table",
            local.display(),
            field.name,
            column.physical_type(),
            column.logical_type_ref()
        );
        ensure!(
            facts.insert(field.id, ColumnFacts::default()).is_none(),
            "{}: field id {} appears twice",
            local.display(),
            field.id
        );
        primitives.insert(index, (field.id, primitive.clone()));
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
fn bounds_of(statistics: &Statistics, primitive: &PrimitiveType) -> Option<(Datum, Datum)> {
    let low = Datum::try_from_bytes(statistics.min_bytes_opt()?, primitive.clone()).ok()?;
    let high = Datum::try_from_bytes(statistics.max_bytes_opt()?, primitive.clone()).ok()?;
    (!low.is_nan() && !high.is_nan()).then_some((low, high))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
