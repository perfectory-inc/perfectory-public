//! Foundation Platform API process entry point.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    foundation_api::run().await
}
