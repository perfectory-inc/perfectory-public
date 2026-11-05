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
