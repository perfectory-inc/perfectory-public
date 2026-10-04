//! The building lane served from section packs (root ADR-0147).
//!
//! | command                                          | does                                    |
//! |--------------------------------------------------|-----------------------------------------|
//! | `export-building-by-pnu-section-packs`           | Gold → packs, a base or a patch         |
//! | `verify-building-by-pnu-section-pack-equality`   | gate (가): every PNU, packs = objects    |
//! | `probe-building-by-pnu-section-pack-latency`     | gate (나): cold first read, live vs pack |
//! | `publish-building-by-pnu-section-packs`          | the manifest's `section_packs` block     |
//! | `inspect-building-by-pnu-section-packs`          | one PNU's fragments and answer          |
//!
//! Environment variables carry the lane's prefix (`FOUNDATION_PLATFORM_BUILDING_BY_PNU_SERVING_`).

mod bake;
mod equality;
mod gate;
mod inspect;
mod latency;
mod publish;
mod read;
pub(crate) mod sections;

#[cfg(test)]
mod tests;

/// One section pack command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PackCommand {
    Export,
    VerifyEquality,
    ProbeLatency,
    Publish,
    Inspect,
}

impl PackCommand {
    /// The command a name spells, if it is one of these.
    pub(crate) fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "export-building-by-pnu-section-packs" => Self::Export,
            "verify-building-by-pnu-section-pack-equality" => Self::VerifyEquality,
            "probe-building-by-pnu-section-pack-latency" => Self::ProbeLatency,
            "publish-building-by-pnu-section-packs" => Self::Publish,
            "inspect-building-by-pnu-section-packs" => Self::Inspect,
            _ => return None,
        })
    }

    /// Runs the command.
    ///
    /// # Errors
    /// Whatever the command refuses.
    pub(crate) async fn run(self) -> anyhow::Result<()> {
        match self {
            Self::Export => bake::run().await,
            Self::VerifyEquality => equality::run().await,
            Self::ProbeLatency => latency::run().await,
            Self::Publish => publish::run().await,
            Self::Inspect => inspect::run().await,
        }
    }
}
