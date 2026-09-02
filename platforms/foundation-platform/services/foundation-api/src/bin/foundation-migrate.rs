//! One-shot `SQLx` migration runner for the independent Foundation database.

use std::env;
use std::error::Error;
use std::io;

use sqlx::postgres::PgPoolOptions;

// The library's embedded migrator — defined once in `foundation_api` (see `src/lib.rs`), never
// re-embedded here. `/readyz` compares the running database against the same static, so the
// runner and the probe cannot disagree about which migrations exist.
use foundation_api::MIGRATOR;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url = env::var("FOUNDATION_MIGRATOR_DATABASE_URL").map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "FOUNDATION_MIGRATOR_DATABASE_URL is required",
        )
    })?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;
    MIGRATOR.run(&pool).await?;
    pool.close().await;
    Ok(())
}
