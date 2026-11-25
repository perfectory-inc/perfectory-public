// Fixture for tools/semgrep/rules/r2-read-paths.yml (`opengrep scan --test`). Not compiled.
use aws_sdk_s3::config;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::{Client, Config};

fn built_outside_its_place(conf: Config, sdk: &SdkConfig) {
    // ruleid: r2-client-is-built-in-its-areas-place
    let client = Client::from_conf(conf);
    // ruleid: r2-client-is-built-in-its-areas-place
    let aliased = S3Client::new(sdk);
    // ruleid: r2-client-is-built-in-its-areas-place
    let builder = config::Builder::from(sdk);
    // ruleid: r2-client-is-built-in-its-areas-place
    let other = aws_sdk_s3::Config::builder();
}

fn not_an_s3_client() {
    // ok: r2-client-is-built-in-its-areas-place
    let http = reqwest::Client::new();
    // Client::from_conf belongs in foundation-outbox (a comment naming it is not a client)
    // ok: r2-client-is-built-in-its-areas-place
    let storage = R2ObjectStorage::from_config(config);
}

fn a_duckdb_of_its_own_reading_r2(connection: &Connection, key: &str, secret: &str) {
    // ruleid: duckdb-reads-r2-through-the-stage-opener
    connection.execute_batch(&format!("CREATE SECRET objects (TYPE s3, KEY_ID '{key}', SECRET '{secret}')"));
    // ruleid: duckdb-reads-r2-through-the-stage-opener
    connection.execute_batch("SET http_retries = 3");
}

fn through_the_stage_opener(work: &Path) -> anyhow::Result<DuckDbParquetStage> {
    // ok: duckdb-reads-r2-through-the-stage-opener
    DuckDbParquetStage::open_reading_lakehouse(work, &LakehouseRead::from_env()?)
}
