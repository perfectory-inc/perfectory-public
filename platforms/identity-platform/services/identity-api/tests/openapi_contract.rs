//! Generated Identity `OpenAPI` artifact parity.

use std::error::Error;

#[test]
fn committed_identity_openapi_is_the_exact_generated_document() -> Result<(), Box<dyn Error>> {
    let committed: serde_json::Value =
        serde_json::from_str(include_str!("../../../docs/openapi/identity.v1.json"))?;
    let generated = serde_json::to_value(identity_api::openapi_document())?;

    assert_eq!(committed, generated);
    assert_eq!(
        committed["info"]["contact"]["email"],
        "engineering@perfectory.invalid"
    );
    Ok(())
}
