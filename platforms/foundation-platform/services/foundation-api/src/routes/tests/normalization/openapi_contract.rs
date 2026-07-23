use utoipa::OpenApi;

type JsonObject = serde_json::Map<String, serde_json::Value>;
type TestResult = Result<(), Box<dyn std::error::Error>>;

const SUBMIT_PATH: &str = "/internal/normalization/proposals";
const STAFF_PATHS: [&str; 4] = [
    "/catalog/v1/normalization/proposals/{id}/approve",
    "/catalog/v1/normalization/proposals/{id}/reject",
    "/catalog/v1/normalization/proposals/{id}/apply",
    "/catalog/v1/normalization/applications/{id}/rollback",
];

#[test]
fn generated_normalization_openapi_owns_every_route_and_schema() -> TestResult {
    let document = crate::routes::normalization::NormalizationApiDoc::openapi();
    let value = serde_json::to_value(document)?;
    let paths = value["paths"]
        .as_object()
        .ok_or("normalization OpenAPI paths must be an object")?;
    for path in std::iter::once(SUBMIT_PATH).chain(STAFF_PATHS) {
        assert!(paths.contains_key(path), "missing OpenAPI path: {path}");
    }

    assert_schema_inventory(&value)?;
    assert_security_contract(&value, paths)?;
    assert_response_contract(paths)
}

fn assert_schema_inventory(value: &serde_json::Value) -> TestResult {
    let schemas = value["components"]["schemas"]
        .as_object()
        .ok_or("normalization OpenAPI schemas must be an object")?;
    for schema in [
        "NormalizationProposalSubmission",
        "NormalizationReviewRequest",
        "NormalizationReviewResult",
        "NormalizationApplyRequest",
        "NormalizationApplyResult",
        "NormalizationRollbackRequest",
        "NormalizationRollbackResult",
        "FoundationSubmissionResult",
        "ApiErrorResponse",
        "InternalApiErrorResponse",
        "IntakeError",
    ] {
        assert!(
            schemas.contains_key(schema),
            "missing OpenAPI schema: {schema}"
        );
    }
    Ok(())
}

fn assert_security_contract(value: &serde_json::Value, paths: &JsonObject) -> TestResult {
    let security_schemes = value["components"]["securitySchemes"]
        .as_object()
        .ok_or("normalization OpenAPI security schemes must be an object")?;
    for scheme in ["normalization_service_bearer", "normalization_staff_bearer"] {
        assert_eq!(security_schemes[scheme]["type"], "http");
        assert_eq!(security_schemes[scheme]["scheme"], "bearer");
    }

    assert_operation_security(paths, SUBMIT_PATH, "normalization_service_bearer")?;
    for path in STAFF_PATHS {
        assert_operation_security(paths, path, "normalization_staff_bearer")?;
    }

    for path in std::iter::once(SUBMIT_PATH).chain(STAFF_PATHS) {
        for status in ["401", "403", "503"] {
            assert_empty_response(paths, path, status)?;
        }
    }
    Ok(())
}

fn assert_response_contract(paths: &JsonObject) -> TestResult {
    assert_response_schema(paths, SUBMIT_PATH, "202", "FoundationSubmissionResult")?;
    for status in ["422", "500"] {
        assert_response_schema(paths, SUBMIT_PATH, status, "IntakeError")?;
    }

    for path in [
        "/catalog/v1/normalization/proposals/{id}/approve",
        "/catalog/v1/normalization/proposals/{id}/reject",
    ] {
        assert_response_schema(paths, path, "200", "NormalizationReviewResult")?;
    }
    assert_response_schema(
        paths,
        "/catalog/v1/normalization/proposals/{id}/apply",
        "200",
        "NormalizationApplyResult",
    )?;
    assert_response_schema(
        paths,
        "/catalog/v1/normalization/applications/{id}/rollback",
        "200",
        "NormalizationRollbackResult",
    )?;

    for (path, statuses) in [
        (
            "/catalog/v1/normalization/proposals/{id}/approve",
            &["400"][..],
        ),
        (
            "/catalog/v1/normalization/proposals/{id}/reject",
            &["400"][..],
        ),
        (
            "/catalog/v1/normalization/proposals/{id}/apply",
            &["400", "404", "409"][..],
        ),
        (
            "/catalog/v1/normalization/applications/{id}/rollback",
            &["400", "404", "409"][..],
        ),
    ] {
        for status in statuses {
            assert_response_schema(paths, path, status, "ApiErrorResponse")?;
        }
        assert_response_schema(paths, path, "500", "InternalApiErrorResponse")?;
    }
    Ok(())
}

fn assert_operation_security(
    paths: &JsonObject,
    path: &str,
    scheme: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let requirements = paths[path]["post"]["security"]
        .as_array()
        .ok_or("normalization operation security must be an array")?;
    assert!(requirements.iter().any(|requirement| {
        requirement
            .as_object()
            .is_some_and(|object| object.contains_key(scheme))
    }));
    Ok(())
}

fn assert_response_schema(
    paths: &JsonObject,
    path: &str,
    status: &str,
    schema: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema_ref = paths[path]["post"]["responses"][status]["content"]["application/json"]
        ["schema"]["$ref"]
        .as_str()
        .ok_or("normalization error response must reference a JSON schema")?;
    assert_eq!(schema_ref, format!("#/components/schemas/{schema}"));
    Ok(())
}

fn assert_empty_response(
    paths: &JsonObject,
    path: &str,
    status: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = paths[path]["post"]["responses"][status]
        .as_object()
        .ok_or("normalization authorization response must be documented")?;
    assert!(response.contains_key("description"));
    assert!(!response.contains_key("content"));
    Ok(())
}
