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

    match body_result {
        Err(original_error) => Err(original_error),
        Ok(value) => {
            cleanup_result?;
            Ok(value)
        }
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
        let name = format!("{label}_{}", Uuid::new_v4().simple());
        let mut admin = PgConnection::connect_with(&admin_options).await?;
        admin
            .execute(format!(r#"CREATE DATABASE "{name}""#).as_str())
            .await?;

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(admin_options.clone().database(name.as_str()))
            .await?;
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

fn admin_options() -> TestResult<PgConnectOptions> {
    let database_url = env::var("DATABASE_URL")?;
    Ok(PgConnectOptions::from_str(database_url.as_str())?)
}
