//! Every HUB family lane's export through the shared streaming engine (root ADR-0551).
//!
//! The lane's source contract and Silver table both come from its family file entry.

use anyhow::Context;
use lakehouse_domain::HubFamilyLane;

use crate::hub_register_silver_export::{self, Layout};

pub(crate) fn layout(lane: &HubFamilyLane) -> anyhow::Result<Layout> {
    Layout::parse(lane.source_document, &lane.table)
        .with_context(|| format!("{}: {} is not a HUB layout", lane.family_file, lane.id))
}

/// The family lane `export` names, by its id.
#[must_use]
pub fn lane_id_of_export(export: &str) -> Option<&'static str> {
    lakehouse_domain::hub_family_lane_by_export(export).map(|lane| lane.id)
}

/// Exports the selected national ZIP of family lane `id` as a complete Silver handoff.
///
/// # Errors
/// Fails on an unknown lane, invalid configuration, source layout/integrity or publication errors.
pub async fn run(id: &str) -> anyhow::Result<()> {
    let lane = lakehouse_domain::hub_family_lane_by_id(id)
        .with_context(|| format!("{id} is not a HUB family lane"))?;
    hub_register_silver_export::run(lane.env_prefix, layout(lane)?).await
}
