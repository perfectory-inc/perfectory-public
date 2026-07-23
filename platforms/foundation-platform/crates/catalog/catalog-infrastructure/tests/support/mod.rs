use std::{env, error::Error, future::Future, str::FromStr};

use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Connection, Executor, PgConnection, PgPool,
};
use uuid::Uuid;

pub type TestError = Box<dyn Error + Send + Sync>;
pub type TestResult<T = ()> = Result<T, TestError>;

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

pub async fn database_count_with_prefix(label: &str) -> TestResult<i64> {
    let mut admin = PgConnection::connect_with(&admin_options()?).await?;
    let prefix = format!("{}_", database_label(label)?);
    let count =
        sqlx::query_scalar("SELECT count(*) FROM pg_database WHERE left(datname, length($1)) = $1")
            .bind(prefix)
            .fetch_one(&mut admin)
            .await?;
    Ok(count)
}

struct DisposableDatabase {
    admin_options: PgConnectOptions,
    name: String,
    pool: PgPool,
}

impl DisposableDatabase {
    async fn create(label: &str) -> TestResult<Self> {
        let admin_options = admin_options()?;
        let name = format!("{}_{}", database_label(label)?, Uuid::new_v4().simple());
        let mut admin = PgConnection::connect_with(&admin_options).await?;
        admin
            .execute(format!(r#"CREATE DATABASE "{name}""#).as_str())
            .await?;

        let pool_result = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(admin_options.clone().database(name.as_str()))
            .await;
        let pool = match pool_result {
            Ok(pool) => pool,
            Err(connect_error) => {
                let cleanup_result = admin
                    .execute(format!(r#"DROP DATABASE "{name}" WITH (FORCE)"#).as_str())
                    .await;
                return match cleanup_result {
                    Ok(_) => Err(connect_error.into()),
                    Err(cleanup_error) => Err(combined_error(
                        "disposable database connection and cleanup both failed",
                        &connect_error,
                        &cleanup_error,
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
        let mut admin = PgConnection::connect_with(&self.admin_options).await?;
        admin
            .execute(format!(r#"DROP DATABASE "{}" WITH (FORCE)"#, self.name).as_str())
            .await?;
        Ok(())
    }
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
