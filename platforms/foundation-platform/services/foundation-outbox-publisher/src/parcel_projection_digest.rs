//! Canonical digest of a parcel projection source or materialised target.

use anyhow::bail;
use futures_util::TryStreamExt;
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

const CONTENT_DIGEST_PREFIX: &[u8] = b"perfectory.parcel-projection-content.v1\0";

pub enum ProjectionRows {
    Source(Uuid),
    Target(Uuid),
}

pub async fn stream_projection_digest(
    transaction: &mut Transaction<'_, Postgres>,
    rows_to_read: ProjectionRows,
) -> anyhow::Result<(i64, String)> {
    let (sql, id) = match rows_to_read {
        ProjectionRows::Source(run_id) => (
            "SELECT pnu::text AS pnu, public.st_asewkb(geom, 'NDR') AS ewkb
               FROM serving_postgis.parcel_boundary_mirror
              WHERE rebuild_run_id = $1
              ORDER BY pnu COLLATE \"C\"",
            run_id,
        ),
        ProjectionRows::Target(load_id) => (
            "SELECT pnu::text AS pnu, public.st_asewkb(geom, 'NDR') AS ewkb
               FROM serving_postgis.parcel_boundary_publication
              WHERE projection_load_id = $1
              ORDER BY pnu COLLATE \"C\"",
            load_id,
        ),
    };
    let mut rows = sqlx::query(sql).bind(id).fetch(&mut **transaction);
    let mut digest = Sha256::new();
    digest.update(CONTENT_DIGEST_PREFIX);
    let mut row_count = 0_i64;
    while let Some(row) = rows.try_next().await? {
        let pnu: String = row.try_get("pnu")?;
        if pnu.len() != 19 || !pnu.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("projection digest requires an ASCII 19-byte PNU, got {pnu:?}");
        }
        let ewkb: Vec<u8> = row.try_get("ewkb")?;
        digest.update(pnu.as_bytes());
        digest.update([0]);
        digest.update(Sha256::digest(ewkb));
        row_count += 1;
    }
    Ok((row_count, format!("{:x}", digest.finalize())))
}
