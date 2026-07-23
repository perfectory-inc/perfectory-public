//! `PostgreSQL` adapters for Identity authorization.

/// PostgreSQL-backed authorization adapters.
pub mod postgres;

pub use postgres::{PgEffectiveRoleReader, PgIdentityBootstrapUnitOfWork, PgRoleGrantUnitOfWork};
