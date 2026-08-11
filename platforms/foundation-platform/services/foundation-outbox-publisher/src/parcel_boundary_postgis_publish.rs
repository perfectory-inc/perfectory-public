//! Materialises one succeeded parcel-boundary mirror rebuild into the append-only PostGIS parcel
//! serving projection. The mirror and its rebuild-run ledger remain the evidence SSOT; this command
//! only creates a projection bound to one existing Catalog revision.
//!
//! **Why the mirror rather than the Silver handoff shards.** They are the same rows read at two
//! points of one pipeline: the shards are what `rebuild-postgis-parcel-boundary-mirror-national`
//! reads, and `serving_postgis.parcel_boundary_mirror` is what it writes. Reading the shards again
//! here would rebuild that command's whole path — the R2 fetch, the JSONL row contract, the `COPY`
//! staging, and the geometry repair and reprojection into EPSG:5179 — and two implementations of one
//! repair are two answers to "what is this parcel's geometry" with nothing comparing them. The
//! mirror already holds the answer in this projection's own SRID, behind the same `st_isvalid` and
//! `st_srid` constraints this table carries, and `parcel_boundary_mirror_rebuild_run` gives that
//! materialisation an identity this command can name and verify instead of trusting a file path.
//! The one existing producer of the target table — the local v2 seed — already selects from the
//! mirror, so reading the shards would also be a second shape for one relationship. What the mirror
//! costs is honest: it is `UNLOGGED` and a national rebuild replaces it, so a run row can outlive
//! its rows. That is checked rather than assumed, in [`materialise`].
//!
//! [ADR-0016](../../../../../docs/adr/0016-a-postgis-projection-load-is-a-fact-with-an-identity.md)
//! 남은 부채 3 forbids a parcels loader on two grounds: nothing in production writes
//! `catalog.parcel`, and the canonical Silver lives outside PostgreSQL so a loader has no dependency
//! to read it through. Neither reaches this command. It writes the serving projection and its two
//! ledgers, touches no Catalog fact table, and its input is a serving table a production command
//! already fills.

use anyhow::{bail, Context};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::public_data_control_support::{optional_bool_env, required_env_value};

const CONFIRM_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_CONFIRM";
const DATA_REVISION_ENV: &str = "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_DATA_REVISION";
const CANONICAL_SNAPSHOT_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_CANONICAL_ICEBERG_SNAPSHOT_ID";
const SOURCE_RECORD_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_SOURCE_RECORD_ID";
const MIRROR_REBUILD_RUN_ENV: &str =
    "FOUNDATION_PLATFORM_PARCEL_BOUNDARY_POSTGIS_PUBLISH_MIRROR_REBUILD_RUN_ID";
/// The `catalog.vector_tile_publication_unit.unit_key` this command materialises.
///
/// `serving_postgis.parcel_boundary_current` — the view Martin reads — selects on this same string,
/// and the promotion gate compares the load's unit against the manifest's. Declared once here so a
/// publish cannot land under a unit no tile source reads.
const PARCEL_UNIT_KEY: &str = "parcels";

pub async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
    let pool = PgPool::connect(&config.database_url)
        .await
        .context("failed to connect to DATABASE_URL for parcel boundary PostGIS publish")?;
    let opened = open_load(&pool, &config).await?;
    match materialise(&pool, &config, &opened).await {
        Ok(publication_rows) => {
            println!(
                "parcel-boundary-postgis-publish-ok publication_rows={} data_revision={} projection_load_id={} mirror_rebuild_run_id={}",
                publication_rows,
                config.data_revision,
                opened.projection_load_id,
                config.mirror_rebuild_run_id
            );
            Ok(())
        }
        // Both arms return the original failure, and the operator learns from the message whether
        // the ledger now agrees with it. A run that ended without saying either would leave a
        // `running` row that no later run can close and no gate can explain.
        Err(error) => Err(match close_failed_load(&pool, &opened, &error).await {
            Ok(()) => error.context(format!(
                "projection load {} was closed as failed",
                opened.projection_load_id
            )),
            Err(close_error) => error.context(format!(
                "projection load {} could not be closed as failed: {close_error:#}",
                opened.projection_load_id
            )),
        }),
    }
}

struct Config {
    database_url: String,
    data_revision: Uuid,
    canonical_snapshot_id: String,
    source_record_id: Uuid,
    /// The `serving_postgis.parcel_boundary_mirror_rebuild_run` whose rows this publishes.
    ///
    /// Named rather than resolved, for the reason ADR-0016 §5 gives for the promotion's load id: the
    /// ledger exists to let several materialisations coexist, so picking "the latest succeeded one"
    /// here would be a silent choice among facts that nothing downstream could catch.
    mirror_rebuild_run_id: Uuid,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        if optional_bool_env(CONFIRM_ENV)? != Some(true) {
            bail!("{CONFIRM_ENV}=1 is required before writing parcel boundary PostGIS");
        }
        let canonical_snapshot_id = required_env_value(CANONICAL_SNAPSHOT_ENV)?;
        if !is_positive_digits(&canonical_snapshot_id) {
            bail!("{CANONICAL_SNAPSHOT_ENV} must be a positive decimal snapshot id");
        }
        Ok(Self {
            database_url: required_env_value("DATABASE_URL")?,
            data_revision: uuid_env(DATA_REVISION_ENV)?,
            canonical_snapshot_id,
            source_record_id: uuid_env(SOURCE_RECORD_ENV)?,
            mirror_rebuild_run_id: uuid_env(MIRROR_REBUILD_RUN_ENV)?,
        })
    }
}

/// The load this run opened, and the row count the rebuild it names recorded for itself.
struct OpenedLoad {
    projection_load_id: Uuid,
    recorded_row_count: i64,
}

/// Refuses everything that can still be refused for free, then opens the load.
///
/// Two transactions, and the split is the point. The administrative publisher is one transaction and
/// ADR-0016 records what that costs: a failure rolls the opened load back with the geometry, so
/// `failed` has no production writer and a projection that broke halfway leaves no trace. Splitting
/// where refusal is still free keeps both properties — a rejected *input* leaves no ledger row at
/// all, and a failure *during* materialisation is closed as `failed` with its reason. That is the
/// pre-validate-then-commit shape ADR-0016 남은 부채 2 names as the precedent to follow.
///
/// What the split gives up is bounded: a process killed between the two commits leaves a `running`
/// load. The promotion gate refuses a `running` load exactly as it refuses a `failed` one, so such a
/// row is never served — it is unexplained, not dangerous.
async fn open_load(pool: &PgPool, config: &Config) -> anyhow::Result<OpenedLoad> {
    let mut transaction = pool.begin().await?;
    // `publication_revision_publisher_only` covers INSERT as well as UPDATE and DELETE, because a
    // role grant alone would let the API mint a revision claiming any snapshot. Registering one is
    // publishing one, so the capability is taken here.
    sqlx::query("SELECT set_config('foundation.temporal_publisher', 'on', true)")
        .execute(&mut *transaction)
        .await?;
    let recorded_row_count = verify_mirror_rebuild_run(&mut transaction, config).await?;
    verify_source_record(&mut transaction, config).await?;
    let publication_unit_id = ensure_parcel_unit(&mut transaction).await?;
    register_revision(&mut transaction, config, publication_unit_id).await?;

    // Opened before a single geometry lands, so the rows this run writes carry the identity of the
    // run that wrote them. Keying the projection on the load rather than on the revision is what
    // makes a re-publish of one revision a second, separately serviceable fact.
    let projection_load_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO serving_postgis.spatial_projection_load
            (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id, status)
         VALUES ($1, $2, $3, $4, 'running')",
    )
    .bind(projection_load_id)
    .bind(publication_unit_id)
    .bind(config.data_revision)
    .bind(&config.canonical_snapshot_id)
    .execute(&mut *transaction)
    .await
    .context("failed to open the parcel PostGIS projection load")?;
    transaction.commit().await?;
    Ok(OpenedLoad {
        projection_load_id,
        recorded_row_count,
    })
}

/// Verifies the mirror rebuild this publish copies, and returns the row count it recorded.
///
/// The recorded count is compared against the rows actually present only after the load is open (see
/// [`materialise`]). The run ledger is a logged table and the mirror is not, so a run row outliving
/// its rows is the expected disagreement here — and it deserves a `failed` load naming it rather
/// than a refusal that leaves nothing behind for the next operator to read.
async fn verify_mirror_rebuild_run(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
) -> anyhow::Result<i64> {
    let row = sqlx::query(
        "SELECT status, loaded_row_count
           FROM serving_postgis.parcel_boundary_mirror_rebuild_run
          WHERE id = $1",
    )
    .bind(config.mirror_rebuild_run_id)
    .fetch_optional(&mut **transaction)
    .await?
    .with_context(|| {
        format!(
            "{MIRROR_REBUILD_RUN_ENV}={} names no parcel boundary mirror rebuild run",
            config.mirror_rebuild_run_id
        )
    })?;
    let status: String = row.try_get("status")?;
    if status != "succeeded" {
        bail!(
            "parcel boundary mirror rebuild {} is '{status}', not 'succeeded'",
            config.mirror_rebuild_run_id
        );
    }
    Ok(row.try_get("loaded_row_count")?)
}

/// Answers for the revision's provenance before the foreign key has to.
///
/// `catalog.publication_revision.source_record_id` references this row, so an absent record is
/// refused either way; answering first only decides which sentence the operator reads.
async fn verify_source_record(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
) -> anyhow::Result<()> {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM catalog.source_record WHERE id = $1)",
    )
    .bind(config.source_record_id)
    .fetch_one(&mut **transaction)
    .await?;
    if !exists {
        bail!(
            "{SOURCE_RECORD_ENV}={} names no catalog source record",
            config.source_record_id
        );
    }
    Ok(())
}

/// Provisions the `parcels` publication unit if this deployment has not published one yet.
///
/// A load names its unit by foreign key, so the unit has to exist by the time the load is opened.
/// Creating it has a price worth stating: `catalog.promote_vector_tile_runtime_manifest` counts every
/// publication unit and refuses a manifest that does not select all of them, so a deployment that
/// publishes parcels cannot promote anything until parcels is promoted too. That is the same bargain
/// the administrative publisher makes, and it is the honest one — a unit nothing selects is a unit
/// nothing serves, and the gate says so instead of the pointer quietly omitting it.
async fn ensure_parcel_unit(transaction: &mut Transaction<'_, Postgres>) -> anyhow::Result<Uuid> {
    sqlx::query(
        "INSERT INTO catalog.vector_tile_publication_unit (id, unit_key)
         VALUES (gen_random_uuid(), $1) ON CONFLICT (unit_key) DO NOTHING",
    )
    .bind(PARCEL_UNIT_KEY)
    .execute(&mut **transaction)
    .await?;
    Ok(sqlx::query_scalar(
        "SELECT id FROM catalog.vector_tile_publication_unit WHERE unit_key = $1",
    )
    .bind(PARCEL_UNIT_KEY)
    .fetch_one(&mut **transaction)
    .await?)
}

/// Registers the parcels publication revision, or proves the registered one is the same fact.
///
/// `derived_from_administrative_revision` stays NULL. A parcels revision asserts nothing about
/// administrative boundaries, and that separation is what ADR-0017 split the ledgers for; before it,
/// a parcels revision could only be recorded as an administrative boundary fact.
///
/// Read back whole after the conflict clause. `ON CONFLICT (id) DO NOTHING` keeps whatever an
/// existing row already says while every input here was checked against the *environment*, and the
/// load's composite foreign key would then refuse the difference as a bare constraint violation
/// naming a key. ADR-0016 §6 records the same shape one table over, on the release row.
async fn register_revision(
    transaction: &mut Transaction<'_, Postgres>,
    config: &Config,
    publication_unit_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO catalog.publication_revision
            (id, publication_unit_id, canonical_iceberg_snapshot_id, source_record_id)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(config.data_revision)
    .bind(publication_unit_id)
    .bind(&config.canonical_snapshot_id)
    .bind(config.source_record_id)
    .execute(&mut **transaction)
    .await
    .context(
        "failed to register the parcels publication revision; \
         one unit carries one revision per canonical snapshot",
    )?;
    let stored = sqlx::query(
        "SELECT publication_unit_id, canonical_iceberg_snapshot_id, source_record_id
           FROM catalog.publication_revision WHERE id = $1",
    )
    .bind(config.data_revision)
    .fetch_one(&mut **transaction)
    .await?;
    let stored_unit: Uuid = stored.try_get("publication_unit_id")?;
    let stored_snapshot: String = stored.try_get("canonical_iceberg_snapshot_id")?;
    let stored_source_record: Uuid = stored.try_get("source_record_id")?;
    if stored_unit != publication_unit_id
        || stored_snapshot != config.canonical_snapshot_id
        || stored_source_record != config.source_record_id
    {
        bail!(
            "publication revision {} already exists and describes a different publication \
             (stored unit {stored_unit}, snapshot {stored_snapshot}, source record {stored_source_record}); \
             a changed publication needs a new revision id",
            config.data_revision
        );
    }
    Ok(())
}

/// Copies the named mirror rebuild into the projection and closes the load with what landed.
///
/// Returns the published row count. Every count is read back out of the tables rather than taken
/// from `rows_affected`, because what the ledger has to record is what is in the table.
async fn materialise(pool: &PgPool, config: &Config, opened: &OpenedLoad) -> anyhow::Result<i64> {
    let mut transaction = pool.begin().await?;
    let mirror_rows = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM serving_postgis.parcel_boundary_mirror WHERE rebuild_run_id = $1",
    )
    .bind(config.mirror_rebuild_run_id)
    .fetch_one(&mut *transaction)
    .await?;
    if mirror_rows != opened.recorded_row_count {
        bail!(
            "parcel boundary mirror rebuild {} recorded {} row(s) and the mirror now holds {mirror_rows}; \
             the mirror is replaced wholesale by every national rebuild, so this load would publish a \
             different materialisation than the one it names",
            config.mirror_rebuild_run_id,
            opened.recorded_row_count
        );
    }

    // `complex_id` and `parcel_id` are left unset. ADR-0024 keeps whether a serving projection
    // carries industrial-complex membership an open question and neither column has a production
    // producer in the mirror either, so filling them would forward a NULL under the appearance of a
    // fact — or, if the mirror ever gained a value, put a frozen membership claim in a serving row
    // that ADR-0020 says belongs to the dated membership ledger. `source_record_id` is the
    // operator's, not the mirror's: it is the revision's verified provenance, and the mirror's own
    // column is filled by nothing. Geometry needs no repair or reprojection here — the mirror stores
    // it in this table's SRID under the same validity constraint, and a row that broke either would
    // abort this statement and close the load as failed.
    sqlx::query(
        "INSERT INTO serving_postgis.parcel_boundary_publication
            (pnu, data_revision, canonical_iceberg_snapshot_id, source_record_id,
             source_object_key, geometry_checksum_sha256, geom, properties, projection_load_id)
         SELECT mirror.pnu, $2, $3, $4, mirror.source_object_key,
                mirror.geometry_checksum_sha256, mirror.geom, mirror.properties, $5
           FROM serving_postgis.parcel_boundary_mirror AS mirror
          WHERE mirror.rebuild_run_id = $1",
    )
    .bind(config.mirror_rebuild_run_id)
    .bind(config.data_revision)
    .bind(&config.canonical_snapshot_id)
    .bind(config.source_record_id)
    .bind(opened.projection_load_id)
    .execute(&mut *transaction)
    .await
    .context("failed to append parcel boundary geometry")?;

    let publication_rows = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM serving_postgis.parcel_boundary_publication
          WHERE projection_load_id = $1",
    )
    .bind(opened.projection_load_id)
    .fetch_one(&mut *transaction)
    .await?;
    if publication_rows != mirror_rows {
        bail!("parcel boundary publish offered {mirror_rows} row(s) and landed {publication_rows}");
    }
    close_succeeded_load(&mut transaction, opened, publication_rows).await?;
    transaction.commit().await?;
    Ok(publication_rows)
}

/// Closes the load `succeeded` with the count that landed, and proves it closed something.
///
/// `rejected_row_count` is zero and cannot be anything else. The mirror's primary key is `pnu`, so
/// one materialisation cannot offer two rows for one parcel, and the insert above carries no
/// conflict clause to drop one; the administrative projection derives a rejection count because its
/// source is a file with no such key. The equality the caller asserts is what would catch that
/// assumption breaking, rather than arithmetic over a difference the schema forbids.
async fn close_succeeded_load(
    transaction: &mut Transaction<'_, Postgres>,
    opened: &OpenedLoad,
    publication_rows: i64,
) -> anyhow::Result<()> {
    let closed = sqlx::query(
        "UPDATE serving_postgis.spatial_projection_load
            SET status = 'succeeded',
                loaded_row_count = $2,
                rejected_row_count = 0,
                finished_at = now()
          WHERE id = $1 AND status = 'running'",
    )
    .bind(opened.projection_load_id)
    .bind(publication_rows)
    .execute(&mut **transaction)
    .await
    .context("failed to close the parcel PostGIS projection load")?
    .rows_affected();
    if closed != 1 {
        bail!(
            "projection load {} was not 'running' when this run tried to close it",
            opened.projection_load_id
        );
    }
    Ok(())
}

/// Closes a load whose materialisation could not complete, with the reason it could not.
///
/// On its own connection from the pool. The materialisation transaction has already rolled back by
/// the time this runs, so the geometry it wrote is gone and the load closes carrying nothing — which
/// is the honest state: a `failed` load the promotion gate refuses by name, holding the sentence
/// that explains it.
async fn close_failed_load(
    pool: &PgPool,
    opened: &OpenedLoad,
    error: &anyhow::Error,
) -> anyhow::Result<()> {
    let closed = sqlx::query(
        "UPDATE serving_postgis.spatial_projection_load
            SET status = 'failed',
                error_message = left($2, 4000),
                finished_at = now()
          WHERE id = $1 AND status = 'running'",
    )
    .bind(opened.projection_load_id)
    .bind(format!("{error:#}"))
    .execute(pool)
    .await?
    .rows_affected();
    if closed != 1 {
        bail!(
            "projection load {} was not 'running'",
            opened.projection_load_id
        );
    }
    Ok(())
}

fn uuid_env(name: &str) -> anyhow::Result<Uuid> {
    Uuid::parse_str(&required_env_value(name)?).with_context(|| format!("{name} must be a UUID"))
}

fn is_positive_digits(value: &str) -> bool {
    !value.is_empty() && value != "0" && value.bytes().all(|byte| byte.is_ascii_digit())
}
