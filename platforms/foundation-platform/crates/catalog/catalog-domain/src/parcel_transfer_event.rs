//! Per-parcel cadastral transfer events from the `AL_D157` history ledger.

use chrono::{DateTime, Utc};

/// One event in a parcel's official cadastral transfer timeline.
#[derive(Clone, Debug, PartialEq)]
pub struct ParcelTransferEvent {
    /// Parcel Number Unit identifier.
    pub pnu: String,
    /// Provider event sequence, unique within one parcel.
    pub transfer_history_seq: i64,
    /// Provider transfer reason code, unchanged.
    pub reason_code: Option<String>,
    /// Provider transfer reason, unchanged.
    pub reason: Option<String>,
    /// Transfer date exactly as the provider wrote it.
    pub moved_at: Option<String>,
    /// Erasure date exactly as the provider wrote it.
    pub erased_at: Option<String>,
    /// Cadastral land category at the time of the event.
    pub land_category: Option<String>,
    /// Official parcel area at the time of the event.
    pub area_m2: Option<f64>,
    /// Provider closure sequence, unchanged.
    pub closure_seq: Option<String>,
    /// Source snapshot identifier carried by the handoff.
    pub source_snapshot_id: String,
    /// UTC timestamp when this event entered the serving catalog.
    pub loaded_at: DateTime<Utc>,
}
