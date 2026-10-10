//! `clear-staging-namespace` (root ADR-0177): empties the lakehouse bucket's `staging/` namespace.
//!
//! The staging smoke runs it first, so every run writes its sample afresh (an object left by the
//! previous run would turn the fresh write it means to exercise into an already-exists recovery)
//! and staging never holds more than one run's objects.
//!
//! It refuses outside `FOUNDATION_PLATFORM_RUNTIME_ENV=staging`, and in staging the R2 client can
//! only list and delete under `staging/` (the client's namespace, not this command, is what makes
//! that so), so it cannot remove a production object however it is called.

use anyhow::{ensure, Context};
use foundation_outbox::{
    object_storage::{R2InventoryRequest, R2KeyNamespace, STAGING_KEY_PREFIX},
    R2ObjectStorage,
};

use crate::runtime_environment::RuntimeEnvironment;

const LIST_PAGE_KEYS: i32 = 1000;

pub async fn clear() -> anyhow::Result<()> {
    let environment = RuntimeEnvironment::from_env_with_execution_context()?;
    ensure!(
        environment == RuntimeEnvironment::Staging,
        "clear-staging-namespace runs only with {}=staging, not {}",
        crate::runtime_environment::RUNTIME_ENVIRONMENT_ENV,
        environment.wire_name()
    );
    let storage = R2ObjectStorage::from_env().context("staging R2 client")?;
    ensure!(
        matches!(storage.namespace(), R2KeyNamespace::Staging { .. }),
        "the R2 client is not in the staging namespace"
    );
    let listing = storage
        .inventory_audit(R2InventoryRequest::new(None, Some(LIST_PAGE_KEYS))?)
        .await
        .context("list the staging namespace")?;
    let mut bytes = 0_i64;
    for object in listing.objects() {
        storage
            .delete_object(&object.key)
            .await
            .with_context(|| format!("delete {STAGING_KEY_PREFIX}{}", object.key))?;
        bytes += object.size_bytes;
    }
    println!(
        "staging-namespace cleared prefix={STAGING_KEY_PREFIX} objects={} bytes={bytes}",
        listing.objects().len()
    );
    Ok(())
}
