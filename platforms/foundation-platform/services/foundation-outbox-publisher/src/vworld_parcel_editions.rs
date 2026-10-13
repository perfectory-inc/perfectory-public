//! `vworld-parcel-editions`: the cadastral parcel editions the Bronze ledger holds, as one JSON
//! line on stdout (root ADR-0224). `vworld_parcel_editions.py --editions` reads it, so the shell
//! loaders and the legal-dong pairing take an edition's objects from the ledger, not the contract.
//!
//! `VWORLD_PARCEL_SOURCE_CONTRACT` names the contract (default: the repository path relative to
//! the working directory) and `DATABASE_URL` the Foundation database. Skipped partial editions and
//! refused objects go to stderr.

use anyhow::Context;
use foundation_publisher_silver::vworld_parcel_source_contract::{editions, read_ledger};
use sqlx::{Connection, PgConnection};

use crate::public_data_control_support::{optional_env_value, required_env_value};

pub async fn run() -> anyhow::Result<()> {
    let path = optional_env_value("VWORLD_PARCEL_SOURCE_CONTRACT")?.unwrap_or_else(|| {
        "infra/lakehouse/contracts/vworld-parcel-source-objects.json".to_owned()
    });
    let contract = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read the parcel source contract {path}"))?;
    let mut conn = PgConnection::connect(required_env_value("DATABASE_URL")?.as_str())
        .await
        .context("failed to connect to DATABASE_URL for the parcel editions")?;
    let found = editions(&contract, &read_ledger(&mut conn, &contract).await?)?;
    for key in ["skipped", "refused"] {
        for line in found[key].as_array().into_iter().flatten() {
            eprintln!(
                "vworld-parcel-editions {key}: {}",
                line.as_str().unwrap_or_default()
            );
        }
    }
    println!("{found}");
    Ok(())
}
