//! Planning helpers for ILIS (industryland.or.kr) industrial-complex Bronze ingestion pages.
//!
//! ILIS serves its list endpoints as `POST` with a JSON body, and it honours a page size large
//! enough to return the whole result set in one response. That is why this lane's request carries a
//! `pageSize` parameter name rather than data.go.kr's `numOfRows`, and no format parameter at all:
//! the recorded lineage has to say what was actually sent, not what a neighbouring lane sends.
//!
//! Every object is content-qualified (`page-000001--sha256-<hex>.json`, root ADR-0152): the lane asks
//! the same national endpoints every day, and ILIS republishes them under the same request with new
//! bytes. A request-only key named one object for every answer, so the second answer that differed
//! from the first was refused by the write-once bucket (2026-11-22: the first two scheduled runs
//! stopped on the 2026-08-18 hand load's object). The checksum makes the key name one answer: a new
//! answer lands beside the earlier ones (append-only), and the same answer again is the same object,
//! so a rerun or a same-day retry finds its own object. A date in the key would not do: the notice
//! list comes back in a different row order from one request to the next (two answers of
//! 2026-09-27 differ in 2,731 row positions), so two answers of one day still differ.

use chrono::NaiveDate;
use collection_domain::build_bronze_content_object_key;
use foundation_shared_kernel::ids::IngestionRunId;
use serde_json::Value as JsonValue;

use crate::{
    plan_public_data_bronze_page, PublicDataBronzePagePlan, PublicDataBronzePagePlanInput,
    PublicDataBronzePageRequest, PublicDataBronzePlanError, PublicDataPageRequest,
    PublicDataPartitionField,
};

/// Request parameters for one ILIS page.
///
/// The partition is carried on the request because this lane serves two shapes: the national bulk
/// endpoints (`scope=national`, one object for the whole country) and the per-complex detail
/// endpoint (`complex=<code>`, one object each). Both go through the same key compile, so neither
/// can invent its own object layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IlisIndustrialComplexPageRequest {
    /// Provider-native operation, for example `danjiList`.
    pub operation: String,
    /// Provider-neutral partition name included in the Bronze object key.
    pub partition_name: String,
    /// Provider-neutral partition value included in the Bronze object key.
    pub partition_value: String,
    /// One-based page number.
    pub page_no: u32,
    /// Requested page size. The bulk lane sets this above the known total row count on purpose.
    pub page_size: u32,
    /// JSON pointer to the logical records array inside the provider response.
    pub logical_items_pointer: String,
    /// Field-path suffixes scored as candidate source keys when non-null.
    pub candidate_key_field_suffixes: Vec<String>,
}

impl IlisIndustrialComplexPageRequest {
    /// Converts the ILIS request into the provider-neutral public-data planner request.
    ///
    /// # Errors
    /// Returns [`PublicDataBronzePlanError`] when fields cannot be represented canonically.
    pub fn to_public_data_request(
        &self,
    ) -> Result<PublicDataBronzePageRequest, PublicDataBronzePlanError> {
        Ok(PublicDataBronzePageRequest {
            operation: self.operation.clone(),
            partition_fields: vec![PublicDataPartitionField {
                name: self.partition_name.clone(),
                value: self.partition_value.clone(),
            }],
            query_params: std::collections::BTreeMap::new(),
            format_query_param: None,
            page_param_name: "pageNo".to_owned(),
            size_param_name: "pageSize".to_owned(),
            page_no: self.page_no,
            num_of_rows: self.page_size,
        })
    }
}

/// Input required to plan one immutable ILIS Bronze page object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IlisIndustrialComplexBronzePagePlanInput<'a> {
    /// Stable lowercase source slug.
    pub source_slug: &'a str,
    /// Ingestion date recorded as metadata; object keys are not partitioned by date (the payload's
    /// checksum, not the day, tells two answers apart).
    pub ingest_date: NaiveDate,
    /// Ingestion run that owns this object.
    pub ingestion_run_id: IngestionRunId,
    /// Provider request parameters.
    pub request: IlisIndustrialComplexPageRequest,
    /// Raw provider response bytes, stored unchanged in Bronze.
    pub raw_payload: Vec<u8>,
    /// Parsed provider response used only for metadata and schema profiling.
    pub payload: JsonValue,
}

/// Planned metadata for one immutable ILIS raw response page.
pub type IlisIndustrialComplexBronzePagePlan = PublicDataBronzePagePlan;

/// Plans object metadata for one immutable ILIS raw response page, under a content-qualified key:
/// the request's canonical key with the payload's SHA-256 (`…/page-000001--sha256-<hex>.json`).
///
/// The bytes are hashed exactly as the provider sent them; nothing is reordered or normalized
/// first, so the key names the raw answer Bronze keeps.
///
/// # Errors
/// Returns [`PublicDataBronzePlanError`] when request parameters cannot be represented in the
/// canonical Bronze object layout.
pub fn plan_ilis_industrial_complex_bronze_page(
    input: IlisIndustrialComplexBronzePagePlanInput<'_>,
) -> Result<IlisIndustrialComplexBronzePagePlan, PublicDataBronzePlanError> {
    let mut plan = plan_public_data_bronze_page(PublicDataBronzePagePlanInput {
        source_slug: input.source_slug,
        ingest_date: input.ingest_date,
        ingestion_run_id: input.ingestion_run_id,
        logical_items_pointer: &input.request.logical_items_pointer,
        candidate_key_field_suffixes: input.request.candidate_key_field_suffixes.clone(),
        request: input.request.to_public_data_request()?,
        raw_payload: input.raw_payload,
        payload: input.payload,
    })?;
    plan.object_key = build_bronze_content_object_key(&plan.object_key, &plan.checksum_sha256)?;
    Ok(plan)
}

impl PublicDataPageRequest for IlisIndustrialComplexPageRequest {
    fn compile_bronze_page_plan(
        &self,
        source_slug: &str,
        ingest_date: NaiveDate,
        ingestion_run_id: IngestionRunId,
        raw_payload: Vec<u8>,
        payload: JsonValue,
    ) -> Result<PublicDataBronzePagePlan, PublicDataBronzePlanError> {
        plan_ilis_industrial_complex_bronze_page(IlisIndustrialComplexBronzePagePlanInput {
            source_slug,
            ingest_date,
            ingestion_run_id,
            request: self.clone(),
            raw_payload,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use chrono::NaiveDate;
    use foundation_shared_kernel::ids::IngestionRunId;
    use serde_json::{json, Value as JsonValue};
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::{
        plan_ilis_industrial_complex_bronze_page, IlisIndustrialComplexBronzePagePlanInput,
        IlisIndustrialComplexPageRequest,
    };
    use crate::{PublicDataBronzePagePlan, PublicDataPageRequest};

    const LIST_SLUG: &str = "industrylandorkr__industrial_complex_list";
    const DETAIL_SLUG: &str = "industrylandorkr__industrial_complex_detail";

    fn request(
        operation: &str,
        partition: (&str, &str),
        page_size: u32,
    ) -> IlisIndustrialComplexPageRequest {
        IlisIndustrialComplexPageRequest {
            operation: operation.to_owned(),
            partition_name: partition.0.to_owned(),
            partition_value: partition.1.to_owned(),
            page_no: 1,
            page_size,
            logical_items_pointer: "/result/dataList".to_owned(),
            candidate_key_field_suffixes: vec!["danji_cd".to_owned()],
        }
    }

    fn national_list() -> IlisIndustrialComplexPageRequest {
        request("danjiList", ("scope", "national"), 2_000)
    }

    /// A synthetic bulk answer: the provider's envelope, its rows in the order given.
    fn answer(codes: &[&str]) -> anyhow::Result<(Vec<u8>, JsonValue)> {
        let rows: Vec<JsonValue> = codes
            .iter()
            .enumerate()
            .map(
                |(index, code)| json!({"rn": index + 1, "danji_cd": code, "addr_cd": "9999900000"}),
            )
            .collect();
        let payload =
            json!({"code": 200, "msg": "ok", "result": {"cnt": rows.len(), "dataList": rows}});
        Ok((serde_json::to_vec(&payload)?, payload))
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        Sha256::digest(bytes)
            .iter()
            .fold(String::new(), |mut hex, byte| {
                let _ = write!(&mut hex, "{byte:02x}");
                hex
            })
    }

    fn plan(
        slug: &str,
        request: IlisIndustrialComplexPageRequest,
        (raw_payload, payload): (Vec<u8>, JsonValue),
        day: u32,
    ) -> anyhow::Result<PublicDataBronzePagePlan> {
        Ok(plan_ilis_industrial_complex_bronze_page(
            IlisIndustrialComplexBronzePagePlanInput {
                source_slug: slug,
                ingest_date: NaiveDate::from_ymd_opt(2099, 1, day)
                    .ok_or_else(|| anyhow::anyhow!("synthetic date"))?,
                ingestion_run_id: IngestionRunId::new(Uuid::new_v4()),
                request,
                raw_payload,
                payload,
            },
        )?)
    }

    /// The key is the request's key plus the checksum of the bytes as sent, nothing else.
    #[test]
    fn the_key_names_the_answer_by_the_checksum_of_its_raw_bytes() -> anyhow::Result<()> {
        let sent = answer(&["99999A", "99999B"])?;
        let checksum = sha256_hex(&sent.0);
        let planned = plan(LIST_SLUG, national_list(), sent, 1)?;
        assert_eq!(planned.checksum_sha256, checksum);
        assert_eq!(
            planned.object_key.as_str(),
            format!(
                "bronze/source={LIST_SLUG}/operation=danjiList/scope=national/page-000001--sha256-{checksum}.json"
            )
        );
        Ok(())
    }

    /// 2026-11-22: the scheduled run's answer differed from the one a hand load had stored under the
    /// request-only key, and the write-once bucket refused it. A different answer is a different
    /// object; the same answer on another day or in a retry is the same object.
    #[test]
    fn a_new_answer_lands_beside_the_old_and_the_same_answer_is_the_same_object(
    ) -> anyhow::Result<()> {
        let first = plan(
            LIST_SLUG,
            national_list(),
            answer(&["99999A", "99999B"])?,
            1,
        )?;
        let again = plan(
            LIST_SLUG,
            national_list(),
            answer(&["99999A", "99999B"])?,
            2,
        )?;
        // The notice list comes back reordered between two requests of one day: other bytes.
        let reordered = plan(
            LIST_SLUG,
            national_list(),
            answer(&["99999B", "99999A"])?,
            1,
        )?;
        assert_eq!(again.object_key, first.object_key);
        assert_eq!(again.dedupe_key, first.dedupe_key);
        assert_ne!(reordered.object_key, first.object_key);
        assert_ne!(reordered.dedupe_key, first.dedupe_key);
        // The coverage identity is the request, whatever the answer.
        assert_eq!(reordered.source_identity_key, first.source_identity_key);
        Ok(())
    }

    /// The committer compiles the key itself from the raw page (ADR 0016); it must reach the same
    /// key the collector planned, or the collector would report one object and write another.
    #[test]
    fn the_committer_compile_reaches_the_key_the_lane_plans() -> anyhow::Result<()> {
        let sent = answer(&["99999A"])?;
        let planned = plan(LIST_SLUG, national_list(), sent.clone(), 1)?;
        let compiled = national_list().compile_bronze_page_plan(
            LIST_SLUG,
            NaiveDate::from_ymd_opt(2099, 1, 1).ok_or_else(|| anyhow::anyhow!("synthetic date"))?,
            IngestionRunId::new(Uuid::new_v4()),
            sent.0,
            sent.1,
        )?;
        assert_eq!(compiled.object_key, planned.object_key);
        assert_eq!(compiled.dedupe_key, planned.dedupe_key);
        Ok(())
    }

    /// A per-complex detail keeps its `complex=<code>` segment: the Silver plan groups by it.
    #[test]
    fn a_detail_object_keeps_its_complex_segment() -> anyhow::Result<()> {
        let mut detail = request("danjiDetInfo", ("complex", "99999A"), 1);
        detail.logical_items_pointer = "/result/data".to_owned();
        let payload = json!({"result": {"data": {"danji_cd": "99999A"}}});
        let raw = serde_json::to_vec(&payload)?;
        let checksum = sha256_hex(&raw);
        let planned = plan(DETAIL_SLUG, detail, (raw, payload), 1)?;
        assert_eq!(
            planned.object_key.as_str(),
            format!(
                "bronze/source={DETAIL_SLUG}/operation=danjiDetInfo/complex=99999A/page-000001--sha256-{checksum}.json"
            )
        );
        Ok(())
    }
}
