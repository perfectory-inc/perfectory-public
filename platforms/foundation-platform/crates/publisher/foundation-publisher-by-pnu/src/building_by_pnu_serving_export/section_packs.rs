//! A by-PNU lane served from section packs (root ADR-0147, ADR-0151).
//!
//! | command                                          | does                                    |
//! |--------------------------------------------------|-----------------------------------------|
//! | `plan-building-by-pnu-pack-shards`               | a bake's shards, from the manifests     |
//! | `export-building-by-pnu-section-packs`           | Gold → packs, a base or a patch         |
//! | `verify-building-by-pnu-section-pack-equality`   | gate (가): every PNU, packs = objects    |
//! | `probe-building-by-pnu-section-pack-latency`     | gate (나): cold first read, live vs pack |
//! | `publish-building-by-pnu-section-packs`          | the manifest's `section_packs` block     |
//! | `inspect-building-by-pnu-section-packs`          | one PNU's fragments and answer          |
//! | `check-building-gateway-version-health`          | one canary step's verdict, from analytics |
//! | `monitor-building-by-pnu-serving`                | the hourly synthetic read of the live host |
//! | `retire-building-superseded-pack-generations`    | deletes generations replaced a week ago |
//!
//! Each command exists for every lane, with the lane's name in place of `building`
//! (`export-parcel-by-pnu-section-packs`, `check-unit-price-gateway-version-health`, ...): the
//! parcel lane and every lane the contract's `by_pnu_lanes` names (root ADR-0190). Environment
//! variables carry the lane's prefix (`FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_`,
//! `FOUNDATION_PLATFORM_UNIT_PRICE_BY_PNU_SERVING_`).

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
mod retire;
pub mod sections;

#[cfg(test)]
mod latency_tests;
#[cfg(test)]
mod pack_lane_tests;
#[cfg(test)]
mod tests;

/// What a section pack command does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackCommandKind {
    PlanShards,
    Export,
    VerifyEquality,
    ProbeLatency,
    Publish,
    Inspect,
    CheckVersionHealth,
    Monitor,
    /// `show-<lane>-by-pnu-serving-state`, for the lanes `main` does not name itself.
    ShowState,
    Retire,
}

/// One section pack command of one lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackCommand {
    pub lane: ByPnuLane,
    pub kind: PackCommandKind,
}

impl PackCommand {
    /// The command a name spells, if it is one of these, for any lane: the parcel and building
    /// lanes and every lane the contract's `by_pnu_lanes` names (root ADR-0190).
    pub fn parse(name: &str) -> Option<Self> {
        const KINDS: [(&str, &str, PackCommandKind); 10] = [
            ("plan-", "-by-pnu-pack-shards", PackCommandKind::PlanShards),
            ("export-", "-by-pnu-section-packs", PackCommandKind::Export),
            (
                "verify-",
                "-by-pnu-section-pack-equality",
                PackCommandKind::VerifyEquality,
            ),
            (
                "probe-",
                "-by-pnu-section-pack-latency",
                PackCommandKind::ProbeLatency,
            ),
            (
                "publish-",
                "-by-pnu-section-packs",
                PackCommandKind::Publish,
            ),
            (
                "inspect-",
                "-by-pnu-section-packs",
                PackCommandKind::Inspect,
            ),
            (
                "check-",
                "-gateway-version-health",
                PackCommandKind::CheckVersionHealth,
            ),
            ("monitor-", "-by-pnu-serving", PackCommandKind::Monitor),
            ("show-", "-by-pnu-serving-state", PackCommandKind::ShowState),
            (
                "retire-",
                "-superseded-pack-generations",
                PackCommandKind::Retire,
            ),
        ];
        let lanes = ByPnuLane::all().ok()?;
        KINDS.into_iter().find_map(|(verb, object, kind)| {
            let lane = name.strip_prefix(verb)?.strip_suffix(object)?;
            let lane = lanes
                .iter()
                .copied()
                .find(|candidate| candidate.name() == lane)?;
            Some(Self { lane, kind })
        })
    }

    /// Runs the command.
    ///
    /// # Errors
    /// Whatever the command refuses.
    pub async fn run(self) -> anyhow::Result<()> {
        let lane = self.lane;
        match self.kind {
            PackCommandKind::PlanShards => bake::plan_shards(lane).await,
            PackCommandKind::Export => bake::run(lane).await,
            PackCommandKind::VerifyEquality => equality::run(lane).await,
            PackCommandKind::ProbeLatency => latency::run(lane).await,
            PackCommandKind::Publish => publish::run(lane).await,
            PackCommandKind::Inspect => inspect::run(lane).await,
            PackCommandKind::CheckVersionHealth => health::run(lane).await,
            PackCommandKind::Monitor => monitor::run(lane).await,
            PackCommandKind::ShowState => crate::by_pnu_serving_state::run(lane).await,
            PackCommandKind::Retire => retire::run(lane).await,
        }
    }
}
