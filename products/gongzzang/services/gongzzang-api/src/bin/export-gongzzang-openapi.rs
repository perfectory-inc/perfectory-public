//! Writes the deterministic Gongzzang API `OpenAPI` document.
//!
//! `packages/api-types` generates the web's TypeScript types from the committed
//! `services/gongzzang-api/openapi.json`, so the listing value lists are spelled once, in Rust.
//! Regenerate from the area root with
//! `cargo run -p gongzzang-api --bin export-gongzzang-openapi -- services/gongzzang-api/openapi.json`.

use std::path::PathBuf;

use listing_domain::repository::CardSearchSort;
use shared_kernel::contact_visibility::ContactVisibility;
use shared_kernel::listing_status::ListingStatus;
use shared_kernel::listing_type::ListingType;
use shared_kernel::transaction_type::TransactionType;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(title = "Gongzzang API", version = "0.1.0"),
    components(schemas(
        ListingType,
        TransactionType,
        ListingStatus,
        ContactVisibility,
        CardSearchSort
    ))
)]
struct GongzzangApiDoc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("output path argument is required")?;
    let mut document = serde_json::to_string(&GongzzangApiDoc::openapi())?;
    document.push('\n');
    std::fs::write(output, document)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    /// The web types are generated from the committed file; a stale file is a stale web.
    /// Read at run time (cargo runs a test from its package directory).
    #[test]
    fn committed_openapi_is_the_exact_generated_document() -> Result<(), Box<dyn Error>> {
        let committed: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string("openapi.json")?)?;
        let generated = serde_json::to_value(GongzzangApiDoc::openapi())?;
        assert_eq!(
            committed, generated,
            "openapi.json is stale: rerun export-gongzzang-openapi"
        );
        Ok(())
    }

    /// The schema spells the serde names; the parsers and the database read `as_str`.
    #[test]
    fn every_schema_enum_is_the_as_str_list() -> Result<(), Box<dyn Error>> {
        let document = serde_json::to_value(GongzzangApiDoc::openapi())?;
        let expected: [(&str, Vec<&str>); 5] = [
            (
                "ListingType",
                ListingType::ALL.map(ListingType::as_str).to_vec(),
            ),
            (
                "TransactionType",
                TransactionType::ALL.map(TransactionType::as_str).to_vec(),
            ),
            (
                "ListingStatus",
                ListingStatus::ALL.map(ListingStatus::as_str).to_vec(),
            ),
            (
                "ContactVisibility",
                ContactVisibility::ALL
                    .map(ContactVisibility::as_str)
                    .to_vec(),
            ),
            (
                "CardSearchSort",
                CardSearchSort::ALL.map(CardSearchSort::as_str).to_vec(),
            ),
        ];
        for (name, values) in expected {
            let schema = &document["components"]["schemas"][name]["enum"];
            assert_eq!(schema, &serde_json::json!(values), "{name}");
        }
        Ok(())
    }
}
