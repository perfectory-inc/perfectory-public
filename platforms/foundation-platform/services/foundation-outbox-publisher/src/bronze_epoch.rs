//! `rekey-bronze-epoch`: the Bronze epoch of root ADR-0626 §8 N2, done once for the whole class.
//!
//! What moves: every Bronze object whose leaf carries a run stamp (G5) and every object of a source
//! the naming registry renamed (`naming.renames.sources`), each to the key today's writer builds
//! ([`rekey`]); and the objects of an id whose provider the naming registry does not hold and the
//! ledger never recorded (debris, `rtmolitkr__*`: root ADR-0626 F1), which are deleted. The writers and readers of the moved sources are off in the
//! release that ships this (orchestration/jobs.v1.json), and the run refuses while any of their
//! runs is in progress or was a moment ago.
//!
//! The order, each step resumable, so a run started again goes on where the last one stopped:
//!
//! 1. **Evidence first.** The manifest of every move (old key, new key, size, `ETag`, last
//!    modification, SHA-256) is written create-only to
//!    `control/evidence/r2_move/<date>-bronze-epoch.jsonl.gz` before anything is copied (root
//!    ADR-0626 §6-2). A later run reads it back and holds the ledger to it.
//! 2. **Copy, create-only, rehashed.** `create_only_copy_and_rehash` copies each object in the
//!    bucket and reads every byte back against the ledger's SHA-256 and size; the new key's `ETag`
//!    must equal the old one (a single `CopyObject` keeps the MD5 `ETag`, W108 3c).
//! 3. **The ledger, in place.** One transaction per source: its `source_catalog` row takes the new
//!    id in place (`ON CONFLICT (slug)` would split an insert, F4) and each object row takes its new
//!    key and the fields built from it, keeping `bronze_object_id`. The version trigger keeps every
//!    replaced row (ADR-0264), which is the way back.
//! 4. **Verify.** Every moved row names its new key, and every new key holds the manifest's size
//!    and `ETag`. The report is written create-only beside the manifest.
//! 5. **Delete only after verifying**, and only an object past the bucket's lock on `bronze/`
//!    (infra/cloudflare/…bucket-lock.json): the run deletes what the lock lets go and reports the
//!    rest, and the next run deletes more. Once every old key is gone the run has nothing to do.
//!
//! Silver and Gold rows that hold the old keys as data are not rewritten: their component's
//! Lakekeeper switch rebuilds them from Bronze (root ADR-0624 10-6); until then readers match the
//! old ids through the naming registry and the ledger's former keys.

mod rekey;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Duration, Utc};
use foundation_outbox::object_storage::{
    create_only_copy_and_rehash, CreateOnlyCopyObjectRequest, ObjectWriteMode, PutObjectRequest,
    R2InventoryRequest, R2ObjectStorageConfig, StreamingObjectRehash, SINGLE_COPY_MAX_BYTES,
};
use foundation_outbox::{errors::PublishError, ObjectStorageService, R2ObjectStorage};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use sqlx::{PgPool, Row as _};
use uuid::Uuid;

use crate::bronze_ledger_objects::ledger_from_env;
use rekey::{rekey, LedgerRow};

/// Where the epoch's evidence lives (root ADR-0626 §6-2).
const EVIDENCE_PREFIX: &str = "control/evidence/r2_move/";
const MANIFEST_SUFFIX: &str = "-bronze-epoch.jsonl.gz";
const REPORT_SUFFIX: &str = "-bronze-epoch.report.json";
const MANIFEST_SCHEMA: &str = "foundation-platform.bronze_epoch_manifest.v1";
const REPORT_SCHEMA: &str = "foundation-platform.bronze_epoch_report.v1";
const LIST_PAGE: i32 = 1000;
const COPIES_IN_FLIGHT: usize = 8;
/// After its lock lets an object go, how long the run still waits before deleting it.
const LOCK_MARGIN_HOURS: i64 = 1;
const BUCKET_LOCK: &str =
    include_str!("../../../infra/cloudflare/foundation-platform-lakehouse-prod.bucket-lock.json");

/// One move of the manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum Move {
    /// An object the ledger records, to its new key.
    Rekey {
        bronze_object_id: Uuid,
        source_catalog_id: Uuid,
        old_slug: String,
        new_slug: String,
        old_key: String,
        new_key: String,
        size_bytes: i64,
        etag: String,
        last_modified: String,
        checksum_sha256: String,
    },
    /// An object of an id no registry declares and the ledger never recorded.
    Delete {
        slug: String,
        old_key: String,
        size_bytes: i64,
        etag: String,
        last_modified: String,
    },
}

impl Move {
    fn old_key(&self) -> &str {
        match self {
            Self::Rekey { old_key, .. } | Self::Delete { old_key, .. } => old_key,
        }
    }

    fn last_modified(&self) -> &str {
        match self {
            Self::Rekey { last_modified, .. } | Self::Delete { last_modified, .. } => last_modified,
        }
    }

    fn bronze_object_id(&self) -> Option<Uuid> {
        match self {
            Self::Rekey {
                bronze_object_id, ..
            } => Some(*bronze_object_id),
            Self::Delete { .. } => None,
        }
    }
}

/// The sources whose rows `moves` moves.
fn planned_sources(moves: &[Move]) -> Vec<Uuid> {
    moves
        .iter()
        .filter_map(|entry| match entry {
            Move::Rekey {
                source_catalog_id, ..
            } => Some(*source_catalog_id),
            Move::Delete { .. } => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// One object as R2 lists it.
#[derive(Clone, Debug)]
struct Listed {
    size_bytes: i64,
    etag: String,
    last_modified: String,
}

fn etag(raw: Option<&str>) -> String {
    raw.unwrap_or_default().trim_matches('"').to_owned()
}

/// Whether a copy keeps `original`'s `ETag`: a single `CopyObject` keeps a single-part object's MD5,
/// and an object uploaded in parts (`<md5>-<parts>`) gets the copy's own (W108 3c). Such an object's
/// bytes are proven by the copy's full SHA-256 readback alone.
fn etag_carries_over(original: &str) -> bool {
    !original.contains('-')
}

async fn list(storage: &R2ObjectStorage, prefix: &str) -> anyhow::Result<BTreeMap<String, Listed>> {
    let report = storage
        .inventory_audit(R2InventoryRequest::new(Some(prefix), Some(LIST_PAGE))?)
        .await
        .with_context(|| format!("failed to list {prefix}"))?;
    Ok(report
        .objects()
        .iter()
        .map(|object| {
            (
                object.key.clone(),
                Listed {
                    size_bytes: object.size_bytes,
                    etag: etag(object.e_tag.as_deref()),
                    last_modified: object.last_modified.clone().unwrap_or_default(),
                },
            )
        })
        .collect())
}

/// `bronze/source=<slug>/…` → `<slug>`.
fn slug_of(key: &str) -> Option<&str> {
    key.strip_prefix("bronze/source=")?
        .split_once('/')
        .map(|(slug, _)| slug)
}

/// How long the bucket's lock keeps an object under `bronze/` (the declared rule).
fn bronze_lock() -> anyhow::Result<Duration> {
    let lock: JsonValue =
        serde_json::from_str(BUCKET_LOCK).context("the bucket lock rules are not valid")?;
    let seconds = lock["rules"]
        .as_array()
        .context("the bucket lock lists no rules")?
        .iter()
        .find(|rule| rule["prefix"] == "bronze/" && rule["enabled"] == true)
        .and_then(|rule| rule["condition"]["maxAgeSeconds"].as_i64())
        .context("the bucket lock holds no enabled age rule on bronze/")?;
    Ok(Duration::seconds(seconds))
}

fn instant(text: &str, what: &str) -> anyhow::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .map(|at| at.with_timezone(&Utc))
        .with_context(|| format!("{what} {text:?} is not an instant"))
}

/// Every row of the sources the epoch may move: a renamed source's, or a source that holds a run
/// stamp in a key (the Rust rule decides which rows move; this only narrows the read).
async fn ledger_rows(pool: &PgPool, renamed: &[String]) -> anyhow::Result<Vec<LedgerRow>> {
    let rows = sqlx::query(
        "SELECT bo.id, bo.source_catalog_id, sc.slug, bo.object_key, bo.dedupe_key, \
                bo.source_identity_key, bo.source_partition_key, bo.provider_file_id, \
                bo.provider_file_name, bo.request_params, bo.checksum_sha256, bo.size_bytes, \
                bo.content_type, bo.collected_at \
         FROM catalog.bronze_object bo \
         JOIN catalog.source_catalog sc ON sc.id = bo.source_catalog_id \
         WHERE sc.slug = ANY($1) OR bo.source_catalog_id IN ( \
             SELECT DISTINCT source_catalog_id FROM catalog.bronze_object \
             WHERE object_key ~* '[0-9]{8}t[0-9]{6}z') \
         ORDER BY sc.slug, bo.created_at, bo.object_key",
    )
    .bind(renamed)
    .fetch_all(pool)
    .await
    .context("failed to read the ledger rows of the epoch's sources")?;
    rows.iter()
        .map(|row| {
            Ok(LedgerRow {
                id: row.try_get("id")?,
                source_catalog_id: row.try_get("source_catalog_id")?,
                slug: row.try_get("slug")?,
                object_key: row.try_get("object_key")?,
                dedupe_key: row.try_get("dedupe_key")?,
                source_identity_key: row.try_get("source_identity_key")?,
                source_partition_key: row.try_get("source_partition_key")?,
                provider_file_id: row.try_get("provider_file_id")?,
                provider_file_name: row.try_get("provider_file_name")?,
                request_params: row.try_get("request_params")?,
                checksum_sha256: row.try_get("checksum_sha256")?,
                size_bytes: row.try_get("size_bytes")?,
                content_type: row.try_get("content_type")?,
                collected_at: row.try_get("collected_at")?,
            })
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()
        .context("the ledger answered a row of another shape")
}

/// Plans the epoch from the ledger and the listing. Pure, so the refusals are tested.
///
/// # Errors
/// A row the rule cannot move, an object the listing lacks or holds at another size, two moves to
/// one key or one dedupe key, or a new id a source row already holds.
fn plan_moves(
    rows: &[LedgerRow],
    listing: &BTreeMap<String, Listed>,
    renamed: &BTreeMap<String, String>,
    known_slugs: &BTreeSet<String>,
) -> anyhow::Result<Vec<Move>> {
    let mut moves = Vec::new();
    let mut keys: BTreeSet<String> = rows.iter().map(|row| row.object_key.clone()).collect();
    let mut dedupe: BTreeSet<(Uuid, String)> = rows
        .iter()
        .map(|row| (row.source_catalog_id, row.dedupe_key.clone()))
        .collect();
    for row in rows {
        let Some(after) = rekey(row, renamed)? else {
            continue;
        };
        if after.slug != row.slug {
            ensure!(
                !known_slugs.contains(&after.slug),
                "a source row already holds {}: a writer ran under the new id before the epoch renamed {} (root ADR-0626 F4)",
                after.slug,
                row.slug
            );
        }
        let listed = listing.get(&row.object_key).with_context(|| {
            format!(
                "{}: the ledger records it and R2 lists no such object",
                row.object_key
            )
        })?;
        ensure!(
            listed.size_bytes == row.size_bytes && !listed.etag.is_empty(),
            "{}: R2 lists {} bytes (ETag {:?}), the ledger {}",
            row.object_key,
            listed.size_bytes,
            listed.etag,
            row.size_bytes
        );
        ensure!(
            keys.insert(after.object_key.clone()),
            "{}: its new key {} is another object's",
            row.object_key,
            after.object_key
        );
        ensure!(
            dedupe.insert((row.source_catalog_id, after.dedupe_key.clone())),
            "{}: its new dedupe key {} is another row's",
            row.object_key,
            after.dedupe_key
        );
        moves.push(Move::Rekey {
            bronze_object_id: row.id,
            source_catalog_id: row.source_catalog_id,
            old_slug: row.slug.clone(),
            new_slug: after.slug,
            old_key: row.object_key.clone(),
            new_key: after.object_key,
            size_bytes: row.size_bytes,
            etag: listed.etag.clone(),
            last_modified: listed.last_modified.clone(),
            checksum_sha256: row.checksum_sha256.clone(),
        });
    }
    for (key, listed) in listing {
        let Some(slug) = slug_of(key) else {
            continue;
        };
        // Debris is an id outside the registry (its provider is no key of it) that the ledger never
        // recorded; a registered id's objects are never deleted here, recorded or not.
        if collection_domain::is_canonical_source_slug(slug) || known_slugs.contains(slug) {
            continue;
        }
        moves.push(Move::Delete {
            slug: slug.to_owned(),
            old_key: key.clone(),
            size_bytes: listed.size_bytes,
            etag: listed.etag.clone(),
            last_modified: listed.last_modified.clone(),
        });
    }
    Ok(moves)
}

fn gzip_lines(header: &JsonValue, moves: &[Move]) -> anyhow::Result<Vec<u8>> {
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    writeln!(gzip, "{}", serde_json::to_string(header)?)?;
    for entry in moves {
        writeln!(gzip, "{}", serde_json::to_string(entry)?)?;
    }
    Ok(gzip.finish()?)
}

fn read_manifest(bytes: &[u8]) -> anyhow::Result<Vec<Move>> {
    let mut text = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut text)
        .context("the manifest is not gzip text")?;
    let mut lines = text.lines();
    let header: JsonValue = serde_json::from_str(lines.next().context("an empty manifest")?)?;
    ensure!(
        header["schema_version"] == MANIFEST_SCHEMA,
        "the manifest is not a {MANIFEST_SCHEMA}"
    );
    let moves = lines
        .map(serde_json::from_str)
        .collect::<Result<Vec<Move>, _>>()
        .context("the manifest holds a line of another shape")?;
    ensure!(
        header["moves"].as_u64() == Some(u64::try_from(moves.len())?),
        "the manifest's header counts {} moves and it holds {}",
        header["moves"],
        moves.len()
    );
    Ok(moves)
}

/// The one manifest under the evidence prefix, if there is one.
fn manifest_key_of(evidence: &BTreeMap<String, Listed>) -> anyhow::Result<Option<String>> {
    let found: Vec<&String> = evidence
        .keys()
        .filter(|key| key.ends_with(MANIFEST_SUFFIX))
        .collect();
    ensure!(found.len() <= 1, "two Bronze epoch manifests: {found:?}");
    Ok(found.first().map(|key| (*key).clone()))
}

pub(crate) async fn put_create_only(
    storage: &R2ObjectStorage,
    key: &str,
    body: Vec<u8>,
    content_type: &str,
) -> anyhow::Result<bool> {
    match storage
        .put_object(PutObjectRequest {
            key: key.to_owned(),
            body,
            content_type: content_type.to_owned(),
            cache_control: "no-store, max-age=0".to_owned(),
            write_mode: ObjectWriteMode::CreateOnly,
            sha256: None,
        })
        .await
    {
        Ok(()) => Ok(true),
        Err(PublishError::ObjectAlreadyExists { .. }) => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed to write {key}")),
    }
}

/// Refuses while a run of an affected source is in progress or ended a moment ago: two of the line
/// writer's close intervals, so a run that is between two objects is seen.
pub(crate) async fn refuse_while_active(pool: &PgPool, sources: &[Uuid]) -> anyhow::Result<()> {
    let quiet = Duration::seconds(i64::try_from(
        crate::bronze_line_objects::close_after_seconds()? * 2,
    )?);
    let active: Vec<(String, DateTime<Utc>, bool)> = sqlx::query_as(
        "SELECT sc.slug, max(greatest(ir.started_at, coalesce(ir.finished_at, ir.started_at))), \
                bool_or(ir.status = 'running' AND ir.started_at > now() - interval '1 day') \
         FROM catalog.ingestion_run ir JOIN catalog.source_catalog sc ON sc.id = ir.source_catalog_id \
         WHERE ir.source_catalog_id = ANY($1) GROUP BY sc.slug",
    )
    .bind(sources)
    .fetch_all(pool)
    .await
    .context("failed to read the affected sources' runs")?;
    let now = Utc::now();
    let busy: Vec<String> = active
        .into_iter()
        .filter(|(_, last, running)| *running || now - *last < quiet)
        .map(|(slug, last, running)| format!("{slug} (last {last}, running {running})"))
        .collect();
    ensure!(
        busy.is_empty(),
        "a writer of an epoch source is running or ran within {} minutes, so nothing moves: {}; the next run tries again",
        quiet.num_minutes(),
        busy.join(", ")
    );
    Ok(())
}

/// The row's current key, by its id.
async fn current_keys(pool: &PgPool, ids: &[Uuid]) -> anyhow::Result<BTreeMap<Uuid, String>> {
    let rows: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT id, object_key FROM catalog.bronze_object WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(pool)
            .await
            .context("failed to read the moved rows' keys")?;
    Ok(rows.into_iter().collect())
}

/// One source's ledger rows, in one transaction: the source row takes the new id in place, each
/// object row its new key and the fields built from it (the same rule the plan used).
async fn update_source(
    pool: &PgPool,
    rows: &[&LedgerRow],
    renamed: &BTreeMap<String, String>,
) -> anyhow::Result<usize> {
    let mut tx = pool.begin().await?;
    let mut updated = 0;
    for (index, row) in rows.iter().enumerate() {
        let after = rekey(row, renamed)?.context("a planned row no longer moves")?;
        if index == 0 && after.slug != row.slug {
            let (old_dataset, new_dataset) = (
                row.slug.split_once("__").map_or("", |(_, d)| d),
                after.slug.split_once("__").map_or("", |(_, d)| d),
            );
            let done = sqlx::query(
                "UPDATE catalog.source_catalog SET slug = $1, \
                        dataset_name = CASE WHEN dataset_name = $3 THEN $4 ELSE dataset_name END, \
                        name = CASE WHEN name = $3 THEN $4 ELSE name END, \
                        updated_at = now(), version = version + 1 \
                 WHERE id = $2 AND slug = $5",
            )
            .bind(&after.slug)
            .bind(row.source_catalog_id)
            .bind(old_dataset)
            .bind(new_dataset)
            .bind(&row.slug)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("failed to rename the source {}", row.slug))?;
            ensure!(
                done.rows_affected() == 1,
                "the source {} was not renamed in place",
                row.slug
            );
        }
        let done = sqlx::query(
            "UPDATE catalog.bronze_object SET object_key = $1, dedupe_key = $2, \
                    source_identity_key = $3, source_partition_key = $4, provider_file_id = $5, \
                    provider_file_name = $6, request_params = $7 \
             WHERE id = $8 AND object_key = $9",
        )
        .bind(&after.object_key)
        .bind(&after.dedupe_key)
        .bind(&after.source_identity_key)
        .bind(&after.source_partition_key)
        .bind(&after.provider_file_id)
        .bind(&after.provider_file_name)
        .bind(&after.request_params)
        .bind(row.id)
        .bind(&row.object_key)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("failed to move the ledger row of {}", row.object_key))?;
        ensure!(
            done.rows_affected() == 1,
            "the ledger row of {} moved under the epoch",
            row.object_key
        );
        updated += 1;
    }
    tx.commit().await?;
    Ok(updated)
}

/// Runs the epoch, or what is left of it.
///
/// # Errors
/// A refusal of the plan or the precondition, a copy whose bytes differ, a ledger row that moved,
/// or a verification that fails. Nothing after a failed step runs.
pub async fn run() -> anyhow::Result<()> {
    let ledger = ledger_from_env().await?;
    let storage = R2ObjectStorage::from_config(
        R2ObjectStorageConfig::from_env()
            .context("the Bronze epoch copies with the lakehouse writer key")?,
    );
    let renamed = collection_domain::renamed_source_slugs();
    let now = Utc::now();
    let evidence = list(&storage, EVIDENCE_PREFIX).await?;
    let listing = list(&storage, "bronze/").await?;
    let rows = ledger_rows(&ledger, &renamed.keys().cloned().collect::<Vec<_>>()).await?;

    // What still needs moving, planned from the ledger as it stands. Before the manifest exists, its
    // writers must be quiet, so no row lands after it; once it exists, it must hold every such row.
    let known: BTreeSet<String> = sqlx::query_scalar("SELECT slug FROM catalog.source_catalog")
        .fetch_all(&ledger)
        .await?
        .into_iter()
        .collect();
    let planned = plan_moves(&rows, &listing, renamed, &known)?;
    let (manifest_key, moves) = if let Some(key) = manifest_key_of(&evidence)? {
        let moves = read_manifest(&storage.get_object_bytes(&key).await?)?;
        let held: BTreeSet<Uuid> = moves.iter().filter_map(Move::bronze_object_id).collect();
        let unheld: Vec<&str> = planned
            .iter()
            .filter(|entry| {
                entry
                    .bronze_object_id()
                    .is_some_and(|id| !held.contains(&id))
            })
            .map(Move::old_key)
            .collect();
        ensure!(
            unheld.is_empty(),
            "{} ledger rows of the old form landed after the manifest {key} (the first: {:?}); a writer of a moved source ran under the old code, and the epoch moves nothing until that is decided",
            unheld.len(),
            unheld.first()
        );
        (key, moves)
    } else {
        refuse_while_active(&ledger, &planned_sources(&planned)).await?;
        let moves = planned;
        let key = format!(
            "{EVIDENCE_PREFIX}{}{MANIFEST_SUFFIX}",
            now.format("%Y-%m-%d")
        );
        let header = json!({
            "schema_version": MANIFEST_SCHEMA,
            "decision": "root ADR-0626 §4, §8 N2",
            "created_at_utc": now.to_rfc3339(),
            "moves": moves.len(),
            "rekey": moves.iter().filter(|m| matches!(m, Move::Rekey { .. })).count(),
            "delete": moves.iter().filter(|m| matches!(m, Move::Delete { .. })).count(),
        });
        put_create_only(
            &storage,
            &key,
            gzip_lines(&header, &moves)?,
            "application/gzip",
        )
        .await?;
        let moves = read_manifest(&storage.get_object_bytes(&key).await?)?;
        (key, moves)
    };

    // The ledger as the manifest says it stands: each moved row under its old or its new key.
    let by_id: BTreeMap<Uuid, &LedgerRow> = rows.iter().map(|row| (row.id, row)).collect();
    let ids: Vec<Uuid> = moves
        .iter()
        .filter_map(|m| match m {
            Move::Rekey {
                bronze_object_id, ..
            } => Some(*bronze_object_id),
            Move::Delete { .. } => None,
        })
        .collect();
    let current = current_keys(&ledger, &ids).await?;
    let mut pending: BTreeMap<Uuid, Vec<&Move>> = BTreeMap::new();
    for entry in &moves {
        if let Move::Rekey {
            bronze_object_id,
            source_catalog_id,
            old_key,
            new_key,
            ..
        } = entry
        {
            match current.get(bronze_object_id).map(String::as_str) {
                Some(key) if key == new_key => {}
                Some(key) if key == old_key => pending.entry(*source_catalog_id).or_default().push(entry),
                found => bail!("{old_key}: the ledger row {bronze_object_id} names {found:?}, neither key of the manifest"),
            }
        }
    }

    let mut copied = 0_usize;
    let mut updated = 0_usize;
    if !pending.is_empty() {
        refuse_while_active(&ledger, &pending.keys().copied().collect::<Vec<_>>()).await?;
        let todo: Vec<&Move> = pending.values().flatten().copied().collect();
        for chunk in todo.chunks(COPIES_IN_FLIGHT) {
            let copies: Vec<_> = chunk
                .iter()
                .map(|entry| copy_one(&storage, entry))
                .collect();
            futures_util::future::try_join_all(copies).await?;
            copied += chunk.len();
        }
        // The copies' ETags against the manifest, from one listing of each new prefix.
        let mut seen = BTreeMap::new();
        for prefix in todo
            .iter()
            .filter_map(|m| match m {
                Move::Rekey { new_slug, .. } => Some(format!("bronze/source={new_slug}/")),
                Move::Delete { .. } => None,
            })
            .collect::<BTreeSet<_>>()
        {
            seen.extend(list(&storage, &prefix).await?);
        }
        for entry in &todo {
            if let Move::Rekey {
                new_key,
                size_bytes,
                etag,
                ..
            } = entry
            {
                let found = seen
                    .get(new_key)
                    .with_context(|| format!("{new_key} is absent after its copy"))?;
                ensure!(
                    found.size_bytes == *size_bytes
                        && (&found.etag == etag || !etag_carries_over(etag)),
                    "{new_key}: {} bytes, ETag {:?}; the manifest says {size_bytes}, {etag:?}",
                    found.size_bytes,
                    found.etag
                );
            }
        }
        for entries in pending.values() {
            let source_rows = entries
                .iter()
                .map(|entry| match entry {
                    Move::Rekey {
                        bronze_object_id,
                        old_key,
                        ..
                    } => by_id
                        .get(bronze_object_id)
                        .copied()
                        .filter(|row| &row.object_key == old_key)
                        .with_context(|| {
                            format!("{old_key}: its ledger row is not the planned one")
                        }),
                    Move::Delete { old_key, .. } => {
                        bail!("{old_key}: a delete is not a ledger move")
                    }
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            updated += update_source(&ledger, &source_rows, renamed).await?;
        }
    }

    // Verify every move against the ledger and R2, then write the report once.
    let current = current_keys(&ledger, &ids).await?;
    let mut prefixes = BTreeSet::new();
    for entry in &moves {
        if let Move::Rekey { new_slug, .. } = entry {
            prefixes.insert(format!("bronze/source={new_slug}/"));
        }
    }
    let mut after = BTreeMap::new();
    for prefix in &prefixes {
        after.extend(list(&storage, prefix).await?);
    }
    let mut bytes = 0_i64;
    let mut parts_originals = 0_usize;
    let mut per_source: BTreeMap<&str, usize> = BTreeMap::new();
    for entry in &moves {
        if let Move::Rekey {
            bronze_object_id,
            new_slug,
            new_key,
            size_bytes,
            etag,
            ..
        } = entry
        {
            ensure!(
                current.get(bronze_object_id) == Some(new_key),
                "{new_key}: the ledger row {bronze_object_id} does not name it"
            );
            let found = after
                .get(new_key)
                .with_context(|| format!("{new_key}: R2 lists no such object"))?;
            ensure!(
                found.size_bytes == *size_bytes
                    && (&found.etag == etag || !etag_carries_over(etag)),
                "{new_key}: R2 lists {} bytes, ETag {:?}; the manifest {size_bytes}, {etag:?}",
                found.size_bytes,
                found.etag
            );
            if !etag_carries_over(etag) {
                parts_originals += 1;
            }
            bytes += size_bytes;
            *per_source.entry(new_slug).or_default() += 1;
        }
    }
    let report_key = manifest_key.replace(MANIFEST_SUFFIX, REPORT_SUFFIX);
    let rekeyed = ids.len();
    let reported = put_create_only(
        &storage,
        &report_key,
        serde_json::to_vec_pretty(&json!({
            "schema_version": REPORT_SCHEMA,
            "manifest": manifest_key,
            "verified_at_utc": Utc::now().to_rfc3339(),
            "rekeyed_objects": rekeyed,
            "rekeyed_bytes": bytes,
            "deleted_ids": moves.iter().filter(|m| matches!(m, Move::Delete { .. })).count(),
            "objects_per_new_source": per_source,
            "etag_not_comparable": parts_originals,
            "checks": "every moved ledger row names its new key, and every new key holds the manifest's size after a full-byte SHA-256 readback, and its ETag unless the original was uploaded in parts (etag_not_comparable, W108 3c)",
        }))?,
        "application/json",
    )
    .await?;

    // Delete what was verified, as the bucket's lock lets it go.
    let release = bronze_lock()? + Duration::hours(LOCK_MARGIN_HOURS);
    let mut deleted = 0_usize;
    let mut held = 0_usize;
    let mut next: Option<DateTime<Utc>> = None;
    for entry in &moves {
        let key = entry.old_key();
        if !listing.contains_key(key) {
            continue;
        }
        let free_at = instant(entry.last_modified(), key)? + release;
        if free_at <= now {
            storage.delete_object(key).await?;
            deleted += 1;
        } else {
            held += 1;
            next = Some(next.map_or(free_at, |at: DateTime<Utc>| at.min(free_at)));
        }
    }
    let summary = json!({
        "manifest": manifest_key,
        "moves": moves.len(),
        "rekeyed": rekeyed,
        "copied_now": copied,
        "ledger_rows_moved_now": updated,
        "report_written_now": reported,
        "old_keys_deleted_now": deleted,
        "old_keys_held_by_the_lock": held,
        "next_release_utc": next.map(|at| at.to_rfc3339()),
    });
    println!("bronze-epoch-summary-json {summary}");
    let changed = copied + updated + deleted > 0 || reported;
    println!(
        "foundation-job-outcome {}",
        if changed { "changed" } else { "unchanged" }
    );
    ledger.close().await;
    Ok(())
}

async fn copy_one(storage: &R2ObjectStorage, entry: &Move) -> anyhow::Result<()> {
    let Move::Rekey {
        old_key,
        new_key,
        size_bytes,
        checksum_sha256,
        ..
    } = entry
    else {
        return Ok(());
    };
    let size = u64::try_from(*size_bytes).context("a negative size")?;
    create_only_copy_and_rehash(
        storage,
        storage,
        CreateOnlyCopyObjectRequest {
            source_key: old_key.clone(),
            destination_key: new_key.clone(),
            size_bytes: size,
            single_copy_max_bytes: SINGLE_COPY_MAX_BYTES,
        },
        &StreamingObjectRehash {
            checksum_sha256: checksum_sha256.clone(),
            size_bytes: size,
            observed_e_tag: None,
            observed_last_modified: None,
        },
    )
    .await
    .with_context(|| format!("failed to copy {old_key} to {new_key}"))?;
    Ok(())
}

#[cfg(test)]
mod tests;
