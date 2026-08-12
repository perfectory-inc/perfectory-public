//! Sealed parcel-source fixture shared by spatial publication transaction tests.

use sqlx::{Postgres, Transaction};
use uuid::Uuid;

const ICEBERG_TABLE_UUID: Uuid = Uuid::from_u128(0x2f7b_f2d1_3e08_4d1a_936e_556d_8ebf_d055);
const EXECUTION_EVIDENCE_SHA256: &str =
    "1f49789e450b42af7ccf193988a0205588ccef66bc28a165332c2e782b9b4959";

pub async fn seed_parcel_source_evidence(
    tx: &mut Transaction<'_, Postgres>,
    revision_id: Uuid,
    snapshot: &str,
    source_record_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let run_id = derived_id(revision_id, 10);
    let evidence_id = derived_id(revision_id, 11);
    let source_file_asset_id = derived_id(revision_id, 12);

    sqlx::query(
        "INSERT INTO catalog.file_asset
            (id, object_key, mime_type, size_bytes, checksum_sha256,
             source_record_id, visibility)
         VALUES ($1, $2, 'application/json', 1, repeat('b', 64), $3, 'internal')",
    )
    .bind(source_file_asset_id)
    .bind(format!(
        "silver/parcel-boundaries/{source_file_asset_id}/manifest.json"
    ))
    .bind(source_record_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO serving_postgis.parcel_boundary_mirror_rebuild_run
            (id, source_snapshot_id, source_table, source_record_id, source_file_asset_id,
             srid, status, loaded_row_count, rejected_row_count, quality_report, started_at)
         VALUES ($1, 'iceberg:' || $2::text, 'silver.parcel_boundaries', $3, $4,
                 5179, 'planned', 0, 0,
                 jsonb_build_object(
                     'schema_version', 'foundation-platform.parcel_publication_quality.v1',
                     'object_count', 1,
                     'expected_row_count', 1,
                     'loaded_row_count', 1,
                     'invalid_srid_count', 0,
                     'invalid_geometry_count', 0,
                     'empty_geometry_count', 0,
                     'nonpositive_area_count', 0,
                     'source_srid', 'EPSG:4326',
                     'target_srid', 'EPSG:5179',
                     'geometry_repair_strategy', 'postgis-make-valid-v1'
                 ), now())",
    )
    .bind(run_id)
    .bind(snapshot)
    .bind(source_record_id)
    .bind(source_file_asset_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
            SET status = 'running', updated_at = now(), version = version + 1
          WHERE id = $1 AND status = 'planned'",
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO serving_postgis.parcel_boundary_mirror
            (pnu, rebuild_run_id, source_snapshot_id, source_table,
             source_record_id, source_file_asset_id, source_object_key, source_row_id,
             geometry_checksum_sha256, properties, geom)
         VALUES ('9999900101100010001', $1, 'iceberg:' || $2::text,
                 'silver.parcel_boundaries', $3, $4, $5, '9999900101100010001',
                 repeat('c', 64), '{}'::jsonb,
                 public.st_multi(public.st_transform(
                     public.st_setsrid(public.st_geomfromtext(
                         'POLYGON((127.1231 36.1231,127.1232 36.1231,127.1232 36.1232,127.1231 36.1232,127.1231 36.1231))'
                     ), 4326), 5179)))",
    )
    .bind(run_id)
    .bind(snapshot)
    .bind(source_record_id)
    .bind(source_file_asset_id)
    .bind(format!("silver/parcel-boundaries/{run_id}/part-0001.parquet"))
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "UPDATE serving_postgis.parcel_boundary_mirror_rebuild_run
            SET status = 'succeeded', loaded_row_count = 1, finished_at = now(),
                updated_at = now(), version = version + 1
          WHERE id = $1 AND status = 'running'",
    )
    .bind(run_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query("SELECT set_config('foundation.parcel_publication_evidence_sealer', 'on', true)")
        .execute(&mut **tx)
        .await?;
    sqlx::query(
        "INSERT INTO catalog.parcel_publication_source_evidence
            (id, mirror_rebuild_run_id, mirror_rebuild_run_status,
             iceberg_table_uuid, iceberg_logical_table, iceberg_snapshot_id,
             source_record_id, source_file_asset_id,
             execution_evidence_schema_version, execution_evidence_object_key,
             execution_evidence_sha256, source_row_count, projection_content_sha256,
             quality_schema_version)
         VALUES ($1, $2, 'succeeded', $3, 'silver.parcel_boundaries', $4::bigint, $5, $6,
                 'foundation-platform.parcel_publication_execution_evidence.v1', $7,
                 $8, 1, repeat('e', 64),
                 'foundation-platform.parcel_publication_quality.v1')",
    )
    .bind(evidence_id)
    .bind(run_id)
    .bind(ICEBERG_TABLE_UUID)
    .bind(snapshot)
    .bind(source_record_id)
    .bind(source_file_asset_id)
    .bind(format!("evidence/parcel-publication/{evidence_id}.json"))
    .bind(EXECUTION_EVIDENCE_SHA256)
    .execute(&mut **tx)
    .await?;

    Ok(evidence_id)
}

const fn derived_id(revision: Uuid, slot: u128) -> Uuid {
    Uuid::from_u128(revision.as_u128() ^ (slot << 96))
}
