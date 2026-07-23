//! Writes the deterministic Identity v1 `OpenAPI` document.

use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("output path argument is required"))?;
    let mut document = serde_json::to_string_pretty(&identity_api::openapi_document())?;
    document.push('\n');
    std::fs::write(output, document)?;
    Ok(())
}
