//! PNU-based parcel information lookup port.
//!
//! Gongzzang owns the listing denormalization need, but Foundation Platform owns
//! canonical Catalog parcel data. This crate keeps only the Gongzzang-facing
//! port and projection shape; runtime HTTP adapters live in `services/gongzzang-api`.

#![forbid(unsafe_code)]

pub mod info;
pub mod lookup;
pub mod noop_lookup;

pub use info::{GosiYearMonth, ParcelInfo};
pub use lookup::{LookupError, ParcelInfoLookup};
pub use noop_lookup::NoOpParcelInfoLookup;
