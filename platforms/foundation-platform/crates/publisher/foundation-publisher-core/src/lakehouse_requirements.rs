//! What the publisher's lakehouse connections need from the environment.
//!
//! Each list is the requirements of the typed configurations the connection loads, so a command
//! that opens it can say what its unit must hold (`describe-requirements`).

use foundation_outbox::object_storage::LAKEHOUSE_WRITER_REQUIREMENTS;
use foundation_shared_kernel::env_source::{merged, Requirement};
use lakehouse_infrastructure::LakehouseCatalogConfig;

/// What `LakehouseDataCommitter::from_env` needs: the catalog and the writer key pair it loads.
#[must_use]
pub fn data_committer() -> Vec<Requirement> {
    merged(&[
        LakehouseCatalogConfig::WRITER_REQUIREMENTS,
        LAKEHOUSE_WRITER_REQUIREMENTS,
    ])
}
