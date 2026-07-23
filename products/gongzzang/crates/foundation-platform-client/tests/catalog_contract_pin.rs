//! Foundation Catalog provider-contract pin verification.

use std::collections::BTreeSet;
use std::error::Error;

use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

const PROVIDER_SNAPSHOT: &[u8] = include_bytes!("../openapi/catalog.v1.json");
const CONSUMER_PIN: &str =
    include_str!("../../../docs/architecture/foundation-platform-catalog-api-contract.v1.pin.json");

#[derive(Debug, Deserialize)]
struct ContractPin {
    source_path: String,
    snapshot_path: String,
    source_sha256: String,
    endpoints: Vec<EndpointPin>,
}

#[derive(Debug, Deserialize)]
struct EndpointPin {
    operation_id: String,
    transport_module: String,
    adapter_module: String,
    method: String,
    path_template: String,
    success_shape: String,
    required_response_fields: Vec<String>,
}

#[test]
fn catalog_snapshot_hash_and_consumed_surface_match_the_pin() -> Result<(), Box<dyn Error>> {
    let pin: ContractPin = serde_json::from_str(CONSUMER_PIN)?;
    let openapi: Value = serde_json::from_slice(PROVIDER_SNAPSHOT)?;

    assert_eq!(pin.source_path, "docs/openapi/catalog.v1.json");
    assert_eq!(
        pin.snapshot_path,
        "crates/foundation-platform-client/openapi/catalog.v1.json"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(PROVIDER_SNAPSHOT)),
        pin.source_sha256,
        "provider snapshot bytes changed without updating the reviewed pin"
    );

    for endpoint in pin.endpoints {
        assert_eq!(
            endpoint.transport_module,
            "crates/foundation-platform-client/src/catalog.rs"
        );
        assert!(endpoint
            .adapter_module
            .starts_with("services/gongzzang-api/src/"));

        let operation =
            &openapi["paths"][&endpoint.path_template][endpoint.method.to_ascii_lowercase()];
        assert!(!operation.is_null(), "missing pinned endpoint {endpoint:?}");
        assert_eq!(operation["operationId"], endpoint.operation_id);

        let response_schema =
            &operation["responses"]["200"]["content"]["application/json"]["schema"];
        let schema_ref = match endpoint.success_shape.as_str() {
            "object" => response_schema["$ref"].as_str(),
            "array" => response_schema["items"]["$ref"].as_str(),
            other => return Err(format!("unsupported pinned success shape {other}").into()),
        }
        .ok_or("pinned response schema reference is missing")?;
        let schema_name = schema_ref
            .strip_prefix("#/components/schemas/")
            .ok_or("pinned response does not reference a component schema")?;
        let required = openapi["components"]["schemas"][schema_name]["required"]
            .as_array()
            .ok_or("pinned response component has no required field set")?
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>();
        for field in endpoint.required_response_fields {
            assert!(
                required.contains(field.as_str()),
                "{schema_name}.{field} is no longer required by the provider contract"
            );
        }
    }

    Ok(())
}
