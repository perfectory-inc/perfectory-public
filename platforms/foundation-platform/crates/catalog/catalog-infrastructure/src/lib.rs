//! Catalog infrastructure: `SQLx` repository, unit of work (atomic mutation plus outbox),
//! ACL adapters.

pub mod industrial_complex_transaction;
pub mod job_switch_board;
pub mod operator_action_ledger;
pub mod real_estate_broker_roster;
mod row_map;
pub mod sqlx_repository;
pub mod unit_of_work;

pub use industrial_complex_transaction::{
    IndustrialComplexMutationReceipt, IndustrialComplexSnapshot,
    PgIndustrialComplexTransactionParticipant,
};
pub use job_switch_board::{FileJobSwitchBoard, UnconfiguredJobSwitchBoard};
pub use operator_action_ledger::PgOperatorActionLedger;
pub use real_estate_broker_roster::PgRealEstateBrokerRoster;
pub use sqlx_repository::PgCatalogRepository;
pub use unit_of_work::{PgCatalogUnitOfWork, RELEASE_IS_LAKEHOUSE_MADE_SQL};
