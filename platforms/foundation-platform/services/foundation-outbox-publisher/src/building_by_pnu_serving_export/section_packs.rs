//! A by-PNU lane served from section packs (root ADR-0147, ADR-0151).
//!
//! | command                                          | does                                    |
//! |--------------------------------------------------|-----------------------------------------|
//! | `export-building-by-pnu-section-packs`           | Gold → packs, a base or a patch         |
//! | `verify-building-by-pnu-section-pack-equality`   | gate (가): every PNU, packs = objects    |
//! | `probe-building-by-pnu-section-pack-latency`     | gate (나): cold first read, live vs pack |
//! | `publish-building-by-pnu-section-packs`          | the manifest's `section_packs` block     |
//! | `inspect-building-by-pnu-section-packs`          | one PNU's fragments and answer          |
//! | `check-building-gateway-version-health`          | one canary step's verdict, from analytics |
//! | `monitor-building-by-pnu-serving`                | the hourly synthetic read of the live host |
//!
//! Each command exists for the parcel lane too, with `parcel` in place of `building`
//! (`export-parcel-by-pnu-section-packs`, `check-parcel-gateway-version-health`, ...). Environment
//! variables carry the lane's prefix (`FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_`,
//! `FOUNDATION_PLATFORM_PARCEL_BY_PNU_SERVING_`).

use crate::by_pnu_gateway_contract::ByPnuLane;

mod analytics;
mod bake;
mod equality;
mod gate;
mod health;
mod inspect;
mod latency;
mod load;
mod monitor;
mod publish;
mod read;
pub(crate) mod sections;

#[cfg(test)]
mod latency_tests;
#[cfg(test)]
mod tests;

/// What a section pack command does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PackCommandKind {
    Export,
    VerifyEquality,
    ProbeLatency,
    Publish,
    Inspect,
    CheckVersionHealth,
    Monitor,
}

/// One section pack command of one lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PackCommand {
    pub(crate) lane: ByPnuLane,
    pub(crate) kind: PackCommandKind,
}

impl PackCommand {
    /// The command a name spells, if it is one of these.
    pub(crate) fn parse(name: &str) -> Option<Self> {
        let (lane, kind) = match name {
            "export-building-by-pnu-section-packs" => {
                (ByPnuLane::Building, PackCommandKind::Export)
            }
            "verify-building-by-pnu-section-pack-equality" => {
                (ByPnuLane::Building, PackCommandKind::VerifyEquality)
            }
            "probe-building-by-pnu-section-pack-latency" => {
                (ByPnuLane::Building, PackCommandKind::ProbeLatency)
            }
            "publish-building-by-pnu-section-packs" => {
                (ByPnuLane::Building, PackCommandKind::Publish)
            }
            "inspect-building-by-pnu-section-packs" => {
                (ByPnuLane::Building, PackCommandKind::Inspect)
            }
            "check-building-gateway-version-health" => {
                (ByPnuLane::Building, PackCommandKind::CheckVersionHealth)
            }
            "monitor-building-by-pnu-serving" => (ByPnuLane::Building, PackCommandKind::Monitor),
            "export-parcel-by-pnu-section-packs" => (ByPnuLane::Parcel, PackCommandKind::Export),
            "verify-parcel-by-pnu-section-pack-equality" => {
                (ByPnuLane::Parcel, PackCommandKind::VerifyEquality)
            }
            "probe-parcel-by-pnu-section-pack-latency" => {
                (ByPnuLane::Parcel, PackCommandKind::ProbeLatency)
            }
            "publish-parcel-by-pnu-section-packs" => (ByPnuLane::Parcel, PackCommandKind::Publish),
            "inspect-parcel-by-pnu-section-packs" => (ByPnuLane::Parcel, PackCommandKind::Inspect),
            "check-parcel-gateway-version-health" => {
                (ByPnuLane::Parcel, PackCommandKind::CheckVersionHealth)
            }
            "monitor-parcel-by-pnu-serving" => (ByPnuLane::Parcel, PackCommandKind::Monitor),
            _ => return None,
        };
        Some(Self { lane, kind })
    }

    /// Runs the command.
    ///
    /// # Errors
    /// Whatever the command refuses.
    pub(crate) async fn run(self) -> anyhow::Result<()> {
        let lane = self.lane;
        match self.kind {
            PackCommandKind::Export => bake::run(lane).await,
            PackCommandKind::VerifyEquality => equality::run(lane).await,
            PackCommandKind::ProbeLatency => latency::run(lane).await,
            PackCommandKind::Publish => publish::run(lane).await,
            PackCommandKind::Inspect => inspect::run(lane).await,
            PackCommandKind::CheckVersionHealth => health::run(lane).await,
            PackCommandKind::Monitor => monitor::run(lane).await,
        }
    }
}
