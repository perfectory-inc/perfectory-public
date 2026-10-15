//! What the publisher's tests share: the embedded migrations and the environment lock.

use std::sync::OnceLock;

use tokio::sync::{Mutex, MutexGuard};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

/// The platform's migrations, embedded once for every test that stands up a disposable
/// database: each embedding site is a compile-time file read the build-coupling guard counts.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../migrations");

/// Serializes synchronous tests that mutate process-global environment variables.
pub fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.get_or_init(|| Mutex::new(())).blocking_lock()
}

/// Serializes asynchronous tests that mutate process-global environment variables.
pub async fn async_env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await
}
