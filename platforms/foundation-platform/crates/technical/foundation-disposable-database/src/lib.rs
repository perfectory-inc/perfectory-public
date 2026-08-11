//! Provisions a throwaway `PostgreSQL` database for one test and removes it afterwards.
//!
//! Some Foundation invariants are global to a database rather than local to the rows a test wrote.
//! `catalog.promote_vector_tile_runtime_manifest` is the clearest: it refuses a manifest whose unit
//! count differs from `count(*)` over `catalog.vector_tile_publication_unit`, so a publication unit
//! any other test left behind makes every promotion fail — and it fails reading as "promotion is
//! broken" rather than as "a test leaked". Cleanup code cannot close that hole, because the test
//! that leaks is usually the one that panicked before reaching it.
//!
//! A test that touches such state takes its own database instead. This lives in its own crate
//! because two workspace members now need it: the Catalog infrastructure tests, and the outbox
//! publisher tests that drive a command binary which connects to `DATABASE_URL` itself.

use std::{env, error::Error, future::Future, str::FromStr};

use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection, PgPool,
};
use uuid::Uuid;

/// Boxed error type shared by the disposable-database helpers and their callers.
pub type TestError = Box<dyn Error + Send + Sync>;
/// Result alias for tests that run against a disposable database.
pub type TestResult<T = ()> = Result<T, TestError>;

/// Creates a database named after `label`, runs `body` against a pool for it, and drops it.
///
/// The database is dropped whether `body` succeeded, failed, or panicked. When both the body and
/// the cleanup fail, both errors are reported: a cleanup failure that hid the real one would leave
/// the next run debugging a leaked database instead of the assertion that broke.
///
/// `DATABASE_URL` supplies the server and the credentials, and its database is used only to issue
/// `CREATE DATABASE`. The caller applies migrations — this crate does not own a schema.
///
/// # Errors
///
/// Returns an error when `DATABASE_URL` is absent or unparseable, when the database cannot be
/// created or connected to, when `body` returns one, or when the database cannot be dropped.
pub async fn run_in_disposable_database<T, F, Fut>(label: &str, body: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(PgPool) -> Fut + Send + 'static,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let database = DisposableDatabase::create(label).await?;
    let body_result = tokio::spawn(body(database.pool.clone())).await;
    let cleanup_result = database.drop_database().await;
    let body_result = match body_result {
        Ok(result) => result,
        Err(error) => Err(Box::new(error) as TestError),
    };

    match (body_result, cleanup_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(original_error), Ok(())) => Err(original_error),
        (Ok(_), Err(cleanup_error)) => Err(cleanup_error),
        (Err(original_error), Err(cleanup_error)) => Err(combined_error(
            "test body and disposable database cleanup both failed",
            original_error.as_ref(),
            cleanup_error.as_ref(),
        )),
    }
}

/// Creates a disposable database and hands back its own connection URL.
///
/// For tests whose subject is a command binary rather than a function: the binary reads
/// `DATABASE_URL` from its own environment and opens its own pool, so a `PgPool` cannot be handed
/// to it. The returned guard drops the database when it goes out of scope.
///
/// # Errors
///
/// Returns an error when `DATABASE_URL` is absent or unparseable, or when the database cannot be
/// created.
pub async fn disposable_database_url(label: &str) -> TestResult<DisposableDatabaseUrl> {
    let admin_options = admin_options()?;
    let name = create_database(&admin_options, label).await?;
    let url = connection_url(&name)?;
    Ok(DisposableDatabaseUrl {
        admin_options,
        name,
        url,
    })
}

/// A disposable database addressed by URL, dropped when this value is dropped.
pub struct DisposableDatabaseUrl {
    admin_options: PgConnectOptions,
    name: String,
    url: String,
}

impl DisposableDatabaseUrl {
    /// The connection URL of this database, suitable for a child process's `DATABASE_URL`.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Opens a pool against this database for the test's own setup and assertions.
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be connected to.
    pub async fn pool(&self) -> TestResult<PgPool> {
        Ok(PgPoolOptions::new()
            .max_connections(5)
            .connect_with(self.admin_options.clone().database(self.name.as_str()))
            .await?)
    }

    /// Drops the database, reporting a failure the destructor would have to swallow.
    ///
    /// # Errors
    ///
    /// Returns an error when the administrative connection or the `DROP DATABASE` fails.
    pub async fn drop_database(self) -> TestResult {
        let mut admin = PgConnection::connect_with(&self.admin_options).await?;
        admin
            .execute(format!(r#"DROP DATABASE "{}" WITH (FORCE)"#, self.name).as_str())
            .await?;
        Ok(())
    }
}

struct DisposableDatabase {
    admin_options: PgConnectOptions,
    name: String,
    pool: PgPool,
}

impl DisposableDatabase {
    async fn create(label: &str) -> TestResult<Self> {
        let admin_options = admin_options()?;
        let name = create_database(&admin_options, label).await?;

        let pool_result = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(admin_options.clone().database(name.as_str()))
            .await;
        let pool = match pool_result {
            Ok(pool) => pool,
            Err(connect_error) => {
                let cleanup_result = drop_database(&admin_options, &name).await;
                return match cleanup_result {
                    Ok(()) => Err(connect_error.into()),
                    Err(cleanup_error) => Err(combined_error(
                        "disposable database connection and cleanup both failed",
                        &connect_error,
                        cleanup_error.as_ref(),
                    )),
                };
            }
        };
        Ok(Self {
            admin_options,
            name,
            pool,
        })
    }

    async fn drop_database(self) -> TestResult {
        self.pool.close().await;
        drop_database(&self.admin_options, &self.name).await
    }
}

async fn create_database(admin_options: &PgConnectOptions, label: &str) -> TestResult<String> {
    let name = format!("{}_{}", database_label(label)?, Uuid::new_v4().simple());
    let mut admin = PgConnection::connect_with(admin_options).await?;
    admin
        .execute(format!(r#"CREATE DATABASE "{name}""#).as_str())
        .await?;
    Ok(name)
}

async fn drop_database(admin_options: &PgConnectOptions, name: &str) -> TestResult {
    let mut admin = PgConnection::connect_with(admin_options).await?;
    admin
        .execute(format!(r#"DROP DATABASE "{name}" WITH (FORCE)"#).as_str())
        .await?;
    Ok(())
}

/// Rewrites `DATABASE_URL` to name `database`, keeping every other component as the harness wrote it.
///
/// The URL cannot be rebuilt from the parsed options, because `PgConnectOptions` does not expose the
/// password — and a disposable database is worth nothing to a test whose subject cannot reach it.
/// Only the path is replaced, so credentials, host, port and any query string survive verbatim
/// rather than being reconstructed from the fields this crate happens to know about.
fn connection_url(database: &str) -> TestResult<String> {
    let source = env::var("DATABASE_URL")?;
    let (base, query) = match source.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (source.as_str(), None),
    };
    let authority_start = base
        .find("://")
        .map(|index| index + "://".len())
        .ok_or_else(|| std::io::Error::other("DATABASE_URL must carry a scheme"))?;
    let path_start = base[authority_start..]
        .find('/')
        .map_or(base.len(), |index| authority_start + index);
    let rewritten = format!("{}/{database}", &base[..path_start]);
    Ok(match query {
        Some(query) => format!("{rewritten}?{query}"),
        None => rewritten,
    })
}

fn combined_error(
    context: &str,
    primary: &(dyn Error + Send + Sync),
    cleanup: &(dyn Error + Send + Sync),
) -> TestError {
    std::io::Error::other(format!("{context}: primary={primary}; cleanup={cleanup}")).into()
}

fn database_label(label: &str) -> TestResult<String> {
    if label.is_empty()
        || !label
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(std::io::Error::other(
            "disposable database label must use ASCII letters, digits, or underscores",
        )
        .into());
    }

    Ok(label.chars().take(30).collect())
}

fn admin_options() -> TestResult<PgConnectOptions> {
    let database_url = env::var("DATABASE_URL")?;
    Ok(PgConnectOptions::from_str(database_url.as_str())?)
}
