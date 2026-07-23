//! Deterministic `OpenAPI` document for Identity v1.

use utoipa::openapi::info::LicenseBuilder;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::openapi::OpenApi;
use utoipa::{Modify, OpenApi as OpenApiDerive};

use crate::error::ApiErrorResponse;
use crate::routes::{HealthResponse, ReadinessResponse};
use identity_contracts::{
    AssignStaffRoleRequest, PolicyDecisionRequest, PolicyDecisionResponse, PrincipalId,
    ResourceAction, StaffRoleResponse, VerifyStaffSessionResponse,
};

#[derive(OpenApiDerive)]
#[openapi(
    info(
        title = "Identity Platform API",
        version = "1.0.0",
        description = "Versioned staff identity, role assignment, and policy decision API.",
        contact(name = "Perfectory", email = "engineering@perfectory.invalid")
    ),
    paths(
        crate::routes::live,
        crate::routes::ready,
        crate::routes::policy::decide,
        crate::routes::staff::verify_session,
        crate::routes::staff::revoke_session,
        crate::routes::staff::assign_role
    ),
    components(schemas(
        ApiErrorResponse,
        AssignStaffRoleRequest,
        HealthResponse,
        PolicyDecisionRequest,
        PolicyDecisionResponse,
        PrincipalId,
        ReadinessResponse,
        ResourceAction,
        StaffRoleResponse,
        VerifyStaffSessionResponse
    )),
    modifiers(&BearerSecurity, &IdentityLicense)
)]
struct IdentityApiDoc;

const PROPRIETARY_LICENSE_ID: &str = "LicenseRef-Proprietary";

struct IdentityLicense;

impl Modify for IdentityLicense {
    fn modify(&self, openapi: &mut OpenApi) {
        openapi.info.license = Some(
            LicenseBuilder::new()
                .name(PROPRIETARY_LICENSE_ID)
                .identifier(Some(PROPRIETARY_LICENSE_ID))
                .build(),
        );
    }
}

struct BearerSecurity;

impl Modify for BearerSecurity {
    fn modify(&self, openapi: &mut OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearerAuth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
        }
    }
}

/// Returns the deterministic Identity v1 `OpenAPI` model.
#[must_use]
pub fn openapi_document() -> OpenApi {
    IdentityApiDoc::openapi()
}
