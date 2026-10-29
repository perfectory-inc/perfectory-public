//! What the served state records as its document schema, and what a reader holds the documents
//! to (root ADR-0320).
//!
//! 2026-10-28: parcel generation 6 (14:47 UTC) and public-auction generation 2 (21:04 UTC) were
//! whole generations baked under v2 over a state recording v1. The publish kept the replaced
//! generation's record, so the lanes served v2 documents under a v1 record, and from the next hour
//! every serving monitor run of both lanes failed "the documents fragment is not a ...v1 document"
//! (a Slack notice an hour per lane) while their hostnames were faithful.

use anyhow::Context as _;
use serde_json::Value as JsonValue;

use super::{
    empty_row, spark_row, Lane, BUILDING_DOCUMENT_SCHEMA_VERSION, LANE, PNU_A, PNU_B, SNAPSHOT,
};
use crate::building_by_pnu_serving_export::section_packs::{inspect, sections};
use crate::r2_layout::by_pnu;

/// A version the lane served once and the release no longer reads.
const RETIRED: &str = "foundation-platform.building_by_pnu_profile.v1";

impl Lane {
    /// Rewrites the live state's record as the publisher before 2026-10-29 left it.
    fn record_document_schema(&self, schema: &str) -> anyhow::Result<()> {
        let path = self.root.join(by_pnu::manifest_key(LANE)?);
        let mut manifest: JsonValue = serde_json::from_slice(&std::fs::read(&path)?)?;
        manifest["section_packs"]["document_schema_version"] = schema.into();
        std::fs::write(&path, serde_json::to_vec(&manifest)?)?;
        Ok(())
    }
}

#[tokio::test]
async fn a_lagging_record_still_reads_documents_the_lane_may_serve() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("schema-record-read", &rows).await?;
    lane.cut_over(&rows).await?;
    lane.record_document_schema(RETIRED)?;
    // What the monitor reads: the record says RETIRED, the documents are of the release's version.
    let inspected = inspect::inspect(&lane.store, PNU_A, None).await?;
    assert_eq!(inspected["answer"], "document", "{inspected}");
    assert_eq!(
        inspected["document"]["schema_version"],
        BUILDING_DOCUMENT_SCHEMA_VERSION
    );
    Ok(())
}

#[tokio::test]
async fn a_whole_generation_records_the_schema_it_was_baked_under() -> anyhow::Result<()> {
    let rows = vec![spark_row()?, empty_row(PNU_B)];
    let lane = Lane::serving_objects("schema-record-write", &rows).await?;
    lane.cut_over(&rows).await?;
    lane.record_document_schema(RETIRED)?;
    let documents = vec![sections::DOCUMENTS.to_owned()];
    let rebaked = lane
        .bake_sections(&rows, SNAPSHOT, None, &documents, 2)
        .await?;
    let state = lane
        .publish(
            lane.summaries("schema-record", &[&rebaked])?,
            SNAPSHOT,
            None,
            None,
        )
        .await?;
    assert_eq!(
        state.document_schema_version,
        BUILDING_DOCUMENT_SCHEMA_VERSION
    );
    let live = lane
        .live()
        .await?
        .section_packs
        .context("packs are published")?;
    assert_eq!(
        live.served_document_schema_versions()
            .into_iter()
            .collect::<Vec<_>>(),
        vec![BUILDING_DOCUMENT_SCHEMA_VERSION]
    );
    Ok(())
}
