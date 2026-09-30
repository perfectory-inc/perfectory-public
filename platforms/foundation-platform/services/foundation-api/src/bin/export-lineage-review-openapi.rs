//! Writes the deterministic steward API `OpenAPI` document (root ADR-0115, ADR-0116).

use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("output path argument is required"))?;
    let mut document = serde_json::to_string(&foundation_api::lineage_review_openapi_document())?;
    document.push('\n');
    std::fs::write(output, document)?;
    Ok(())
}
