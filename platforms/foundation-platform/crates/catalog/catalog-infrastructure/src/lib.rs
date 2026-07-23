//! Catalog infrastructure: `SQLx` repository, unit of work (atomic mutation plus outbox),
//! ACL adapters.

pub mod industrial_complex_transaction;
pub mod parcel_marker_anchor_rebuild;
mod row_map;
pub mod sqlx_repository;
pub mod unit_of_work;

pub use industrial_complex_transaction::{
    IndustrialComplexMutationReceipt, IndustrialComplexSnapshot,
    PgIndustrialComplexTransactionParticipant,
};
pub use parcel_marker_anchor_rebuild::PgParcelMarkerAnchorRebuilder;
pub use sqlx_repository::{BuildingUnitRow, PgCatalogRepository};
pub use unit_of_work::PgCatalogUnitOfWork;
