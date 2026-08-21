//! The body every `promote-<unit>-runtime` command shares.
//!
//! Promoting a validated PostGIS projection is one procedure: compare-and-swap the runtime pointer,
//! verify the load and its lineage, write an immutable release, and re-state every *other*
//! publication unit's current selection so the next manifest is complete.
//! `catalog.promote_vector_tile_runtime_manifest` refuses an incomplete one, so that last part is
//! not per-unit work — it is the same work whichever unit is being promoted.
//!
//! It lives here rather than once per unit because a second copy of it is a second set of answers
//! to questions the schema asks only once: which generation follows, which units must be carried,
//! what a re-used release id means. The administrative promotion and the industrial-complex
//! promotion differ in five facts, and [`UnitPromotion`] is the list of them. Everything a copy
//! would have duplicated is below.
//!
//! Environment-variable names stay literal `const`s in the calling command. They are the operator's
//! contract, and a name assembled from a prefix at run time cannot be found by searching for it.

use std::collections::BTreeMap;

use anyhow::{bail, Context};
use serde_json::{Map, Value};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::public_data_control_support::{optional_env_value, required_env_value};

/// The environment variables one unit's promotion reads.
///
/// Every field is a literal declared by the calling command, so `grep` finds the operator-facing
/// name in the module that owns it.
pub(crate) struct EnvNames {
    pub confirm: &'static str,
    pub data_revision: &'static str,
    pub canonical_snapshot: &'static str,
    pub source_record: &'static str,
    pub source_file_asset: &'static str,
    pub expected_manifest: &'static str,
    pub release_id: &'static str,
    pub manifest_id: &'static str,
    pub tiles_url: &'static str,
    pub projection_load: &'static str,
}

/// Which ledger holds the data revision this unit's release names.
///
/// Two exist, and which one applies is a property of the unit rather than a choice.
/// `20260731000002` split them: `catalog.administrative_boundary_revision` is the administrative
/// *domain fact* ledger and carries a validation status, while `catalog.publication_revision` is
/// the per-unit publication ledger every later unit registers in and has no status column —
/// a publication revision is not a claim that anything was validated.
pub(crate) enum RevisionLedger {
    /// `catalog.administrative_boundary_revision`, whose status must be `validated` or `published`.
    AdministrativeBoundary,
    /// `catalog.publication_revision`, which must belong to this unit and carry no administrative
    /// lineage.
    PublicationRevision,
}

/// Everything that differs between one publication unit's promotion and another's.
pub(crate) struct UnitPromotion {
    /// `catalog.vector_tile_publication_unit.unit_key`, which is also the Martin source id, the
    /// layer id, and the path segment the tiles URL template has to end on.
    pub unit_key: &'static str,
    /// The first token of the command's success line, so an operator's log grep keeps naming the
    /// command they ran rather than the unit key it happened to promote.
    pub ok_prefix: &'static str,
    /// Names this unit in error text, in the same words the command's own documentation uses.
    pub label: &'static str,
    pub env: EnvNames,
    pub revision_ledger: RevisionLedger,
    /// `catalog.vector_tile_release_layer.feature_id_property`: the property a client keys a
    /// feature on. One per layer, and it is the layer's own identity, never a claim about
    /// something else (ADR-0024).
    pub feature_id_property: &'static str,
    /// `(min, max)` for `tile_min_zoom`/`tile_max_zoom`: the zooms tiles are actually cut over,
    /// which is the range `scripts/tiles/martin-dynamic.yaml` declares for the same source.
    pub tile_zoom: (i16, i16),
    /// `(min, max)` for `render_min_zoom`/`render_max_zoom`: the zooms a client should draw the
    /// layer at. Above `tile_zoom.1` the client overzooms the last cut tile, so the two maxima are
    /// different questions and only coincide for a layer that should stop being drawn there.
    pub render_zoom: (i16, i16),
    /// `catalog.vector_tile_release_layer.feature_filter_properties`.
    pub feature_filter_properties: BTreeMap<String, String>,
}

/// Promotes one validated PostGIS projection load as a complete runtime-manifest unit.
///
/// # Errors
/// Returns an error when the confirmation or provenance environment is incomplete, when the
/// runtime pointer is not the one the caller expected, when the named projection load is not a
/// succeeded load of this unit at this revision and snapshot, when a re-used release id describes a
/// different publication, or when the CAS gate refuses the manifest.
pub(crate) async fn run(unit: &UnitPromotion) -> anyhow::Result<()> {
    let config = Config::from_env(unit)?;
    let pool = PgPool::connect(&config.database_url)
        .await
        .with_context(|| {
            format!(
                "failed to connect to DATABASE_URL for {} runtime promotion",
                unit.label
            )
        })?;
    let mut transaction = pool.begin().await?;
    let (current_manifest, current_generation) = current_manifest(&mut transaction).await?;
    if current_manifest != config.expected_manifest_id {
        bail!(
            "runtime manifest compare-and-swap precondition failed: expected {:?}, current {:?}",
            config.expected_manifest_id,
            current_manifest
        );
    }
    verify_inputs(&mut transaction, unit, &config).await?;
    let publication_unit_id = publication_unit_id(&mut transaction, unit).await?;
    insert_release(&mut transaction, unit, &config, publication_unit_id).await?;
    let next_generation = current_generation.map_or(1, |value| value + 1);
    sqlx::query(
        "INSERT INTO catalog.vector_tile_runtime_manifest (id, manifest_generation, published_at)
         VALUES ($1, $2, now())",
    )
    .bind(config.manifest_id)
    .bind(next_generation)
    .execute(&mut *transaction)
    .await
    .with_context(|| format!("failed to create the {} runtime manifest", unit.label))?;

    let units = sqlx::query(
        "SELECT id, active_release_id, active_data_revision, serving_generation
           FROM catalog.vector_tile_publication_unit ORDER BY unit_key",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in units {
        let unit_id: Uuid = row.try_get("id")?;
        let (release_id, data_revision, serving_generation) = if unit_id == publication_unit_id {
            let active_release = row.try_get::<Option<Uuid>, _>("active_release_id")?;
            let current_serving_generation = row.try_get::<i64, _>("serving_generation")?;
            (
                config.release_id,
                config.data_revision,
                if active_release.is_some() {
                    current_serving_generation + 1
                } else {
                    1
                },
            )
        } else {
            (
                row.try_get::<Option<Uuid>, _>("active_release_id")?
                    .context("every publication unit must have an active release")?,
                row.try_get::<Option<Uuid>, _>("active_data_revision")?
                    .context("every publication unit must have an active data revision")?,
                // Unchanged, not `+ 1`. This unit re-selects the release it already serves, and
                // `20260730000003_serving_generation_tracks_one_unit_source_selection.sql` requires a
                // re-selected release to hold its generation — the value tracks one unit's source
                // selection, and carrying it forward changes nothing about it. The `+ 1` here was
                // correct under the previous rule and was left behind when that rule was narrowed.
                row.try_get::<i64, _>("serving_generation")?,
            )
        };
        let canonical_snapshot = sqlx::query_scalar::<_, String>(
            "SELECT canonical_iceberg_snapshot_id FROM catalog.vector_tile_release WHERE id = $1",
        )
        .bind(release_id)
        .fetch_one(&mut *transaction)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.vector_tile_runtime_manifest_unit
                (manifest_id, publication_unit_id, release_id, serving_generation,
                 data_revision, canonical_iceberg_snapshot_id)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(config.manifest_id)
        .bind(unit_id)
        .bind(release_id)
        .bind(serving_generation)
        .bind(data_revision)
        .bind(canonical_snapshot)
        .execute(&mut *transaction)
        .await?;
    }

    let promoted_generation =
        sqlx::query_scalar::<_, i64>("SELECT catalog.promote_vector_tile_runtime_manifest($1, $2)")
            .bind(current_manifest)
            .bind(config.manifest_id)
            .fetch_one(&mut *transaction)
            .await
            .with_context(|| format!("{} runtime manifest CAS failed", unit.label))?;
    transaction.commit().await?;
    println!(
        "{} manifest_generation={} manifest_id={} release_id={}",
        unit.ok_prefix, promoted_generation, config.manifest_id, config.release_id
    );
    Ok(())
}

struct Config {
    database_url: String,
    data_revision: Uuid,
    canonical_snapshot_id: String,
    source_record_id: Uuid,
    source_file_asset_id: Uuid,
    expected_manifest_id: Option<Uuid>,
    release_id: Uuid,
    manifest_id: Uuid,
    tiles_url_template: String,
    /// The `serving_postgis.spatial_projection_load` row this release serves rows out of.
    ///
    /// Named rather than resolved. One revision can carry several succeeded loads — that is the
    /// state the ledger exists to represent — and the promotion gate accepts any of them, so
    /// resolution here would be a silent choice among facts that nothing downstream could catch.
    projection_load_id: Uuid,
}

impl Config {
    fn from_env(unit: &UnitPromotion) -> anyhow::Result<Self> {
        let confirm = unit.env.confirm;
        if !matches!(
            std::env::var(confirm)
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "1" | "true" | "yes"
        ) {
            bail!("{confirm}=1 is required before runtime promotion");
        }
        let parse_uuid = |name: &str| -> anyhow::Result<Uuid> {
            Uuid::parse_str(&required_env_value(name)?)
                .with_context(|| format!("{name} must be a UUID"))
        };
        let canonical_snapshot_env = unit.env.canonical_snapshot;
        let canonical_snapshot_id = required_env_value(canonical_snapshot_env)?;
        if canonical_snapshot_id.is_empty()
            || canonical_snapshot_id == "0"
            || !canonical_snapshot_id
                .bytes()
                .all(|byte| byte.is_ascii_digit())
        {
            bail!("{canonical_snapshot_env} must be a positive decimal snapshot id");
        }
        let tiles_url_env = unit.env.tiles_url;
        let tiles_url_template = required_env_value(tiles_url_env)?;
        // The same shape `vector_tile_release_route_check` enforces in SQL, stated here so the
        // operator is told which segment is wrong rather than reading a constraint name.
        let route_suffix = format!("/{}/{{z}}/{{x}}/{{y}}", unit.unit_key);
        if !tiles_url_template.ends_with(&route_suffix) || !tiles_url_template.starts_with("http") {
            bail!("{tiles_url_env} must end with {route_suffix}");
        }
        let expected_manifest_env = unit.env.expected_manifest;
        Ok(Self {
            database_url: required_env_value("DATABASE_URL")?,
            data_revision: parse_uuid(unit.env.data_revision)?,
            canonical_snapshot_id,
            source_record_id: parse_uuid(unit.env.source_record)?,
            source_file_asset_id: parse_uuid(unit.env.source_file_asset)?,
            expected_manifest_id: optional_env_value(expected_manifest_env)?
                .map(|value| {
                    Uuid::parse_str(&value)
                        .with_context(|| format!("{expected_manifest_env} must be a UUID"))
                })
                .transpose()?,
            release_id: parse_uuid(unit.env.release_id)?,
            manifest_id: parse_uuid(unit.env.manifest_id)?,
            tiles_url_template,
            projection_load_id: parse_uuid(unit.env.projection_load)?,
        })
    }
}

async fn current_manifest(
    transaction: &mut Transaction<'_, Postgres>,
) -> anyhow::Result<(Option<Uuid>, Option<i64>)> {
    let row = sqlx::query(
        "SELECT pointer.manifest_id, manifest.manifest_generation
           FROM catalog.vector_tile_runtime_manifest_pointer AS pointer
           JOIN catalog.vector_tile_runtime_manifest AS manifest
             ON manifest.id = pointer.manifest_id
          WHERE pointer.singleton = true",
    )
    .fetch_optional(&mut **transaction)
    .await?;
    match row {
        Some(row) => Ok((
            Some(row.try_get("manifest_id")?),
            Some(row.try_get("manifest_generation")?),
        )),
        None => Ok((None, None)),
    }
}

/// Resolves the publication unit this promotion serves.
///
/// Resolved, never created. The unit is minted by the *publisher*, because
/// `catalog.promote_vector_tile_runtime_manifest` requires every manifest to select every unit and
/// a unit that exists before it has a release makes the next promotion of every other unit fail.
/// By the time a load exists to promote, the publisher has already minted it.
async fn publication_unit_id(
    transaction: &mut Transaction<'_, Postgres>,
    unit: &UnitPromotion,
) -> anyhow::Result<Uuid> {
    sqlx::query_scalar("SELECT id FROM catalog.vector_tile_publication_unit WHERE unit_key = $1")
        .bind(unit.unit_key)
        .fetch_optional(&mut **transaction)
        .await?
        .with_context(|| {
            format!(
                "publication unit '{}' does not exist; publish before promoting",
                unit.unit_key
            )
        })
}

async fn verify_inputs(
    transaction: &mut Transaction<'_, Postgres>,
    unit: &UnitPromotion,
    config: &Config,
) -> anyhow::Result<()> {
    verify_revision(transaction, unit, config).await?;
    let source_count =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM catalog.source_record WHERE id = $1")
            .bind(config.source_record_id)
            .fetch_one(&mut **transaction)
            .await?;
    let file_count =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM catalog.file_asset WHERE id = $1")
            .bind(config.source_file_asset_id)
            .fetch_one(&mut **transaction)
            .await?;
    if source_count != 1 || file_count != 1 {
        bail!("runtime promotion lineage source_record/file_asset is missing");
    }
    // `catalog.promote_vector_tile_runtime_manifest` refuses on the same conditions and is the
    // authority. Answering first only changes which sentence the operator reads: the gate can say no
    // more than "this manifest selects a dynamic source with no succeeded PostGIS projection load",
    // because by then the load row it wanted is simply absent from the join.
    let projection_load_env = unit.env.projection_load;
    let load = sqlx::query(
        "SELECT load.status, unit.unit_key, load.data_revision, load.canonical_iceberg_snapshot_id
           FROM serving_postgis.spatial_projection_load AS load
           JOIN catalog.vector_tile_publication_unit AS unit
             ON unit.id = load.publication_unit_id
          WHERE load.id = $1",
    )
    .bind(config.projection_load_id)
    .fetch_optional(&mut **transaction)
    .await?
    .with_context(|| {
        format!(
            "{projection_load_env}={} names no PostGIS projection load",
            config.projection_load_id
        )
    })?;
    let status: String = load.try_get("status")?;
    let load_unit_key: String = load.try_get("unit_key")?;
    let load_revision: Uuid = load.try_get("data_revision")?;
    let load_snapshot: String = load.try_get("canonical_iceberg_snapshot_id")?;
    if status != "succeeded" {
        bail!(
            "PostGIS projection load {} is '{status}', not 'succeeded'",
            config.projection_load_id
        );
    }
    if load_unit_key != unit.unit_key {
        bail!(
            "PostGIS projection load {} materialised unit '{load_unit_key}', not '{}'",
            config.projection_load_id,
            unit.unit_key
        );
    }
    if load_revision != config.data_revision {
        bail!(
            "PostGIS projection load {} carries revision {load_revision}, not {}",
            config.projection_load_id,
            config.data_revision
        );
    }
    if load_snapshot != config.canonical_snapshot_id {
        bail!(
            "PostGIS projection load {} was materialised from snapshot {load_snapshot}, not {}",
            config.projection_load_id,
            config.canonical_snapshot_id
        );
    }
    Ok(())
}

/// Confirms the revision this promotion names exists in this unit's ledger and says what the
/// promotion claims it says.
async fn verify_revision(
    transaction: &mut Transaction<'_, Postgres>,
    unit: &UnitPromotion,
    config: &Config,
) -> anyhow::Result<()> {
    match unit.revision_ledger {
        RevisionLedger::AdministrativeBoundary => {
            let row = sqlx::query(
                "SELECT canonical_iceberg_snapshot_id, source_record_id, status
                   FROM catalog.administrative_boundary_revision WHERE id = $1",
            )
            .bind(config.data_revision)
            .fetch_one(&mut **transaction)
            .await
            .context("administrative boundary revision does not exist")?;
            if row.try_get::<String, _>("canonical_iceberg_snapshot_id")?
                != config.canonical_snapshot_id
                || row.try_get::<Uuid, _>("source_record_id")? != config.source_record_id
                || !matches!(
                    row.try_get::<String, _>("status")?.as_str(),
                    "validated" | "published"
                )
            {
                bail!("administrative revision provenance/status is not promotable");
            }
        }
        RevisionLedger::PublicationRevision => {
            // Joined to the unit rather than looked up by id alone. `publication_revision_scoped_key`
            // exists so a release cannot name another unit's revision, and reading the revision
            // without that join would hand the whole check back to the schema — which refuses the
            // release, not the promotion, and says so in a constraint name.
            let row = sqlx::query(
                "SELECT revision.canonical_iceberg_snapshot_id, revision.source_record_id,
                        revision.derived_from_administrative_revision
                   FROM catalog.publication_revision AS revision
                   JOIN catalog.vector_tile_publication_unit AS unit
                     ON unit.id = revision.publication_unit_id
                  WHERE revision.id = $1 AND unit.unit_key = $2",
            )
            .bind(config.data_revision)
            .bind(unit.unit_key)
            .fetch_optional(&mut **transaction)
            .await?
            .with_context(|| {
                format!(
                    "publication revision {} does not exist for unit '{}'",
                    config.data_revision, unit.unit_key
                )
            })?;
            if row.try_get::<String, _>("canonical_iceberg_snapshot_id")?
                != config.canonical_snapshot_id
                || row.try_get::<Uuid, _>("source_record_id")? != config.source_record_id
            {
                bail!(
                    "{} revision provenance is not promotable: the revision was registered against \
                     another canonical snapshot or source record",
                    unit.label
                );
            }
            // An industrial-complex or parcels revision asserts nothing about an administrative
            // boundary, which is the distinction `20260731000002` split the ledgers for. A row that
            // carries the lineage anyway is an administrative fact wearing this unit's name.
            if row
                .try_get::<Option<Uuid>, _>("derived_from_administrative_revision")?
                .is_some()
            {
                bail!(
                    "publication revision {} carries administrative lineage; a {} revision has none",
                    config.data_revision,
                    unit.label
                );
            }
        }
    }
    Ok(())
}

async fn insert_release(
    transaction: &mut Transaction<'_, Postgres>,
    unit: &UnitPromotion,
    config: &Config,
    publication_unit_id: Uuid,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO catalog.vector_tile_release
            (id, publication_unit_id, data_revision, canonical_iceberg_snapshot_id,
             source_record_id, source_file_asset_ids, source_kind, martin_source_id,
             tiles_url_template, postgis_projection_revision)
         VALUES ($1, $2, $3, $4, $5, $6, 'dynamic_postgis', $7, $8, $9)
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(config.release_id)
    .bind(publication_unit_id)
    .bind(config.data_revision)
    .bind(&config.canonical_snapshot_id)
    .bind(config.source_record_id)
    .bind(vec![config.source_file_asset_id])
    .bind(unit.unit_key)
    .bind(&config.tiles_url_template)
    // Was `$3` — the data revision bound twice. The column now holds the identity of the load that
    // materialised the rows this release serves, which is a different fact from the revision those
    // rows describe, and the promotion gate reads it.
    .bind(config.projection_load_id)
    .execute(&mut **transaction)
    .await?;
    // `DO NOTHING` above means a re-promote under a release id that already exists keeps whatever
    // that row already said. Every one of those fields was verified against the *environment*, not
    // against the row, and the gate cannot catch the difference: an earlier succeeded load for the
    // same unit, revision and snapshot satisfies every one of its conditions, so the pointer would
    // move while the view kept serving the earlier load's rows. Read the row back and compare whole.
    let stored = sqlx::query(
        "SELECT data_revision, canonical_iceberg_snapshot_id, postgis_projection_revision,
                source_record_id, tiles_url_template
           FROM catalog.vector_tile_release WHERE id = $1",
    )
    .bind(config.release_id)
    .fetch_one(&mut **transaction)
    .await?;
    let stored_revision: Uuid = stored.try_get("data_revision")?;
    let stored_snapshot: String = stored.try_get("canonical_iceberg_snapshot_id")?;
    let stored_load: Option<Uuid> = stored.try_get("postgis_projection_revision")?;
    let stored_source_record: Uuid = stored.try_get("source_record_id")?;
    let stored_url: String = stored.try_get("tiles_url_template")?;
    if stored_revision != config.data_revision
        || stored_snapshot != config.canonical_snapshot_id
        || stored_load != Some(config.projection_load_id)
        || stored_source_record != config.source_record_id
        || stored_url != config.tiles_url_template
    {
        bail!(
            "release {} already exists and describes a different publication \
             (stored revision {stored_revision}, snapshot {stored_snapshot}, projection load {stored_load:?}); \
             a changed publication needs a new release id",
            config.release_id
        );
    }
    let (tile_min_zoom, tile_max_zoom) = unit.tile_zoom;
    let (render_min_zoom, render_max_zoom) = unit.render_zoom;
    sqlx::query(
        "INSERT INTO catalog.vector_tile_release_layer
            (release_id, layer_id, source_layer, feature_id_property,
             tile_min_zoom, tile_max_zoom, render_min_zoom, render_max_zoom,
             feature_filter_properties)
         VALUES ($1, $2, $2, $3, $4, $5, $6, $7, $8::jsonb)
         ON CONFLICT (release_id, layer_id) DO NOTHING",
    )
    .bind(config.release_id)
    .bind(unit.unit_key)
    .bind(unit.feature_id_property)
    .bind(tile_min_zoom)
    .bind(tile_max_zoom)
    .bind(render_min_zoom)
    .bind(render_max_zoom)
    .bind(Value::Object(
        unit.feature_filter_properties
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect::<Map<_, _>>(),
    ))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
