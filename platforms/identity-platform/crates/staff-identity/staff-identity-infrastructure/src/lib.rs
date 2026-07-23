//! `PostgreSQL` and Zitadel adapters for staff identity.

mod row_map;

/// PostgreSQL-backed staff identity adapters.
pub mod postgres;
/// Zitadel OIDC bearer verifier.
pub mod zitadel;

pub use postgres::{PgStaffRepository, PgStaffSessionUnitOfWork};
pub use zitadel::ZitadelOidcVerifier;
