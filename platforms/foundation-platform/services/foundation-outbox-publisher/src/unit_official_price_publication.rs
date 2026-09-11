//! Atomic append-only publication shared by the loader and its `PostgreSQL` integration tests.

use anyhow::bail;
use sqlx::{Connection, Executor, PgConnection};

/// Creates the connection-local staging table with the catalog's value constraints.
///
/// # Errors
/// Returns a database error if the staging table cannot be created.
pub async fn prepare_stage(conn: &mut PgConnection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TEMP TABLE unit_official_price_stage
        (LIKE catalog.unit_official_price INCLUDING CONSTRAINTS) ON COMMIT PRESERVE ROWS",
    )
    .await?;
    Ok(())
}

/// Appends and selects one complete source batch, returning inserted and published counts.
///
/// # Errors
/// Rejects empty, mixed, duplicated or changed batches and propagates database failures.
pub async fn promote(conn: &mut PgConnection) -> anyhow::Result<(u64, i64)> {
    let (staged, snapshots): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), COUNT(DISTINCT source_snapshot_id) FROM unit_official_price_stage",
    )
    .fetch_one(&mut *conn)
    .await?;
    if staged == 0 || snapshots != 1 {
        bail!("publication requires a non-empty batch with exactly one source_snapshot_id");
    }
    let conflicting: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM (
        SELECT pnu,dong_name,ho_name,base_date FROM unit_official_price_stage
        GROUP BY pnu,dong_name,ho_name,base_date HAVING COUNT(*) > 1
    ) conflicts",
    )
    .fetch_one(&mut *conn)
    .await?;
    if conflicting != 0 {
        bail!("unit reference-date identities are duplicated: conflicting={conflicting}");
    }
    let source: String =
        sqlx::query_scalar("SELECT source_snapshot_id FROM unit_official_price_stage LIMIT 1")
            .fetch_one(&mut *conn)
            .await?;
    let mut transaction = conn.begin().await?;
    // Serialize publication IDs with commits; a reader sees one complete source batch.
    transaction
        .execute(
            "LOCK TABLE catalog.unit_official_price, catalog.unit_official_price_publication
         IN SHARE ROW EXCLUSIVE MODE",
        )
        .await?;
    let existing: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM catalog.unit_official_price_publication
         WHERE source_snapshot_id = $1)",
    )
    .bind(&source)
    .fetch_one(&mut *transaction)
    .await?;
    let inserted = if existing {
        // Symmetric comparison also rejects a retry which omits old rows.
        let differs: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                (SELECT pnu,dong_name,ho_name,base_date,price_won,source_snapshot_id
                 FROM unit_official_price_stage
                 EXCEPT SELECT pnu,dong_name,ho_name,base_date,price_won,source_snapshot_id
                 FROM catalog.unit_official_price WHERE source_snapshot_id = $1)
                UNION ALL
                (SELECT pnu,dong_name,ho_name,base_date,price_won,source_snapshot_id
                 FROM catalog.unit_official_price WHERE source_snapshot_id = $1
                 EXCEPT SELECT pnu,dong_name,ho_name,base_date,price_won,source_snapshot_id
                 FROM unit_official_price_stage))",
        )
        .bind(&source)
        .fetch_one(&mut *transaction)
        .await?;
        if differs {
            bail!("immutable unit price source batch disagrees with its previous publication");
        }
        0
    } else {
        transaction
            .execute(
                "INSERT INTO catalog.unit_official_price
             (pnu,dong_name,ho_name,base_date,price_won,source_snapshot_id)
             SELECT pnu,dong_name,ho_name,base_date,price_won,source_snapshot_id
             FROM unit_official_price_stage",
            )
            .await?
            .rows_affected()
    };
    // Reselecting an older batch is an append-only rollback, not a mutation of its facts.
    sqlx::query(
        "INSERT INTO catalog.unit_official_price_publication (source_snapshot_id,row_count)
         VALUES ($1,$2)",
    )
    .bind(&source)
    .bind(staged)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok((inserted, staged))
}
