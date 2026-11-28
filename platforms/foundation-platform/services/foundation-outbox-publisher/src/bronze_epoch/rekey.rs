//! The Bronze epoch's one rule (root ADR-0626 §4, §8 N2): the key an object has after the epoch,
//! and every ledger field built from it, exactly as today's writer builds them.
//!
//! Two things change a key, both decided in the naming registry and the write boundary, never here:
//! a run stamp in the leaf (G5 refuses it; when a run collected an object is its row's
//! `collected_at`), and a renamed source (`naming.renames.sources`). The new fields come from the
//! same planner every bulk writer uses (`plan_public_data_bulk_file_metadata`), so a writer after
//! the epoch and the epoch agree on every key by construction. A row is moved only when the planner
//! rebuilds its current fields exactly from its own identity: a row of any other shape is refused,
//! never guessed at.

use std::collections::BTreeMap;

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, NaiveDate, Utc};
use collection_application::{
    plan_public_data_bulk_file_metadata, public_data_bulk_file_dedupe_key,
    public_data_bulk_file_source_identity_key, public_data_bulk_file_source_partition_key,
    PublicDataBulkFileIdentity, PublicDataBulkFileMetadataInput,
    PublicDataBulkFileSourcePartitionKeyInput,
};
use collection_domain::{
    bronze_content_object_key_checksum, build_bronze_content_object_key, run_stamp_range,
    validate_bronze_object_key_contract, BRONZE_CONTENT_KEY_SEPARATOR,
};
use foundation_shared_kernel::ObjectKey;
use serde_json::Value as JsonValue;
use uuid::Uuid;

/// One ledger row the epoch may move (`catalog.bronze_object` and its source's slug).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LedgerRow {
    pub(crate) id: Uuid,
    pub(crate) source_catalog_id: Uuid,
    pub(crate) slug: String,
    pub(crate) object_key: String,
    pub(crate) dedupe_key: String,
    pub(crate) source_identity_key: String,
    pub(crate) source_partition_key: Option<String>,
    pub(crate) provider_file_id: Option<String>,
    pub(crate) provider_file_name: Option<String>,
    pub(crate) request_params: JsonValue,
    pub(crate) checksum_sha256: String,
    pub(crate) size_bytes: i64,
    pub(crate) content_type: String,
    pub(crate) collected_at: DateTime<Utc>,
}

/// The fields a moved row holds after the epoch.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Rekeyed {
    pub(crate) slug: String,
    pub(crate) object_key: String,
    pub(crate) dedupe_key: String,
    pub(crate) source_identity_key: String,
    pub(crate) source_partition_key: String,
    pub(crate) provider_file_id: Option<String>,
    pub(crate) provider_file_name: Option<String>,
    pub(crate) request_params: JsonValue,
}

/// `<provider>__<dataset>` → `<dataset>`.
fn dataset_of(slug: &str) -> anyhow::Result<&str> {
    slug.split_once("__")
        .map(|(_, dataset)| dataset)
        .with_context(|| format!("{slug} is not a source id"))
}

/// A provider file id or name as today's writer names it: without the run stamp (and the `-` or
/// `_` that joined it), and with the dataset part a writer derives from the dataset id renamed with
/// it (`auction-item-detail-part0001` → `item-detail-part0001`).
pub(crate) fn rename_file_part(text: &str, old_dataset: &str, new_dataset: &str) -> String {
    let mut text = text.to_owned();
    if let Some(range) = run_stamp_range(&text) {
        let start = if range.start > 0 && matches!(text.as_bytes()[range.start - 1], b'-' | b'_') {
            range.start - 1
        } else {
            range.start
        };
        text.replace_range(start..range.end, "");
    }
    let (old, new) = (old_dataset.replace('_', "-"), new_dataset.replace('_', "-"));
    if old != new {
        if let Some(rest) = text.strip_prefix(&old) {
            if rest.is_empty() || rest.starts_with(['-', '.']) {
                text = format!("{new}{rest}");
            }
        }
    }
    text
}

fn text_param<'a>(params: &'a JsonValue, name: &str, key: &str) -> anyhow::Result<&'a str> {
    params[name]
        .as_str()
        .with_context(|| format!("{key}: request_params holds no {name}"))
}

fn optional_date(params: &JsonValue, name: &str, key: &str) -> anyhow::Result<Option<NaiveDate>> {
    params[name]
        .as_str()
        .map(|text| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .with_context(|| format!("{key}: request_params {name} {text:?} is not a date"))
        })
        .transpose()
}

struct Planned {
    object_key: ObjectKey,
    dedupe_key: String,
    source_identity_key: String,
    source_partition_key: String,
}

fn plan(
    row: &LedgerRow,
    slug: &str,
    identity: PublicDataBulkFileIdentity,
    qualified: bool,
) -> anyhow::Result<Planned> {
    let plan = plan_public_data_bulk_file_metadata(PublicDataBulkFileMetadataInput {
        source_slug: slug,
        ingest_date: row.collected_at.date_naive(),
        ingestion_run_id: foundation_shared_kernel::ids::IngestionRunId::new(Uuid::nil()),
        identity,
        checksum_sha256: row.checksum_sha256.clone(),
        size_bytes: u64::try_from(row.size_bytes).context("a negative size")?,
        content_type: row.content_type.clone(),
    })
    .with_context(|| format!("{}: the bulk planner refuses its identity", row.object_key))?;
    let object_key = if qualified {
        build_bronze_content_object_key(&plan.object_key, &row.checksum_sha256)
            .with_context(|| format!("{}: cannot content-qualify", row.object_key))?
    } else {
        plan.object_key
    };
    Ok(Planned {
        object_key,
        dedupe_key: plan.dedupe_key,
        source_identity_key: plan.source_identity_key,
        source_partition_key: plan.source_partition_key,
    })
}

/// The fields `row` holds after the epoch, or `None` when the epoch leaves it as it is.
///
/// # Errors
/// The row is not of the bulk form the planner rebuilds exactly, or its new key breaks the Bronze
/// key contract (a run stamp still in it, among others).
pub(crate) fn rekey(
    row: &LedgerRow,
    renamed: &BTreeMap<String, String>,
) -> anyhow::Result<Option<Rekeyed>> {
    let key = row.object_key.as_str();
    let leaf = key.rsplit('/').next().unwrap_or(key);
    let stamped = run_stamp_range(leaf).is_some();
    let slug = renamed
        .get(&row.slug)
        .map_or(row.slug.as_str(), String::as_str);
    if !stamped && slug == row.slug {
        return Ok(None);
    }
    let params = &row.request_params;
    let operation = text_param(params, "operation", key)?;
    let file_id = text_param(params, "provider_file_id", key)?;
    // A row the catalog recovery rehydrated from evidence keeps the name in its column only.
    let file_name = match params["provider_file_name"].as_str() {
        Some(name) => name,
        None => row.provider_file_name.as_deref().with_context(|| {
            format!("{key}: neither request_params nor the row holds provider_file_name")
        })?,
    };
    let identity = |operation: &str,
                    file_id: &str,
                    file_name: &str|
     -> anyhow::Result<PublicDataBulkFileIdentity> {
        Ok(PublicDataBulkFileIdentity {
            operation: operation.to_owned(),
            provider_file_period: params["provider_file_period"].as_str().map(str::to_owned),
            provider_snapshot_date: optional_date(params, "provider_snapshot_date", key)?,
            provider_file_id: file_id.to_owned(),
            provider_file_name: file_name.to_owned(),
            provider_updated_at: optional_date(params, "provider_updated_at", key)?,
        })
    };
    // The row is moved only when the bulk planner names what it holds now: its fields from its own
    // identity, and its key the builder's prefix for the source and operation with the provider
    // file id as the leaf. (The builder itself refuses the stamped leaf now, G5.)
    let qualified = bronze_content_object_key_checksum(key).is_some();
    let (old_dataset, new_dataset) = (dataset_of(&row.slug)?, dataset_of(slug)?);
    let probe = plan(
        row,
        &row.slug,
        identity(
            operation,
            &rename_file_part(file_id, old_dataset, old_dataset),
            &rename_file_part(file_name, old_dataset, old_dataset),
        )?,
        false,
    )?;
    let (prefix, probe_leaf) = probe
        .object_key
        .as_str()
        .rsplit_once('/')
        .with_context(|| format!("{key}: the planner built a key without a leaf"))?;
    let extension = probe_leaf
        .rsplit_once('.')
        .map(|(_, extension)| extension)
        .with_context(|| format!("{key}: the planner built a leaf without an extension"))?;
    let leaf = if qualified {
        format!(
            "{file_id}{BRONZE_CONTENT_KEY_SEPARATOR}{}.{extension}",
            row.checksum_sha256
        )
    } else {
        format!("{file_id}.{extension}")
    };
    let identity_key =
        public_data_bulk_file_source_identity_key(&identity(operation, file_id, file_name)?)?;
    let partition_key =
        public_data_bulk_file_source_partition_key(PublicDataBulkFileSourcePartitionKeyInput {
            operation,
            provider_file_id: file_id,
        })?;
    ensure!(
        key == format!("{prefix}/{leaf}")
            && row.source_identity_key == identity_key
            && row.dedupe_key
                == public_data_bulk_file_dedupe_key(&row.slug, &identity_key, &row.checksum_sha256)
            && row.source_partition_key.as_deref() == Some(partition_key.as_str()),
        "{key}: the bulk planner does not name this row's fields from its own identity; the epoch moves only rows it rebuilds exactly"
    );
    // An operation the writer names after the dataset (the V-World datasets) is renamed with it.
    let operation = if operation == old_dataset {
        new_dataset
    } else {
        operation
    };
    let file_id = rename_file_part(file_id, old_dataset, new_dataset);
    let file_name = rename_file_part(file_name, old_dataset, new_dataset);
    // A key without its run stamp names its bytes instead (root ADR-0152).
    let after = plan(
        row,
        slug,
        identity(operation, &file_id, &file_name)?,
        qualified || stamped,
    )?;
    validate_bronze_object_key_contract(after.object_key.as_str()).with_context(|| {
        format!(
            "{key}: the new key {} breaks the Bronze key contract",
            after.object_key.as_str()
        )
    })?;
    ensure!(
        after.object_key.as_str() != key,
        "{key}: the epoch would leave the key as it is"
    );
    let mut request_params = params.clone();
    if let Some(object) = request_params.as_object_mut() {
        object.insert("operation".to_owned(), JsonValue::from(operation));
        object.insert(
            "provider_file_id".to_owned(),
            JsonValue::from(file_id.clone()),
        );
        if object.contains_key("provider_file_name") {
            object.insert(
                "provider_file_name".to_owned(),
                JsonValue::from(file_name.clone()),
            );
        }
    } else {
        bail!("{key}: request_params is not an object");
    }
    Ok(Some(Rekeyed {
        slug: slug.to_owned(),
        object_key: after.object_key.as_str().to_owned(),
        dedupe_key: after.dedupe_key,
        source_identity_key: after.source_identity_key,
        source_partition_key: after.source_partition_key,
        provider_file_id: row.provider_file_id.as_ref().map(|_| file_id),
        provider_file_name: row.provider_file_name.as_ref().map(|_| file_name),
        request_params,
    }))
}

/// Rows as the writers of before the epoch landed them, for this module's and the driver's tests.
#[cfg(test)]
pub(super) mod fixture {
    use chrono::{TimeZone, Utc};
    use collection_application::{
        public_data_bulk_file_dedupe_key, public_data_bulk_file_request_params,
        public_data_bulk_file_source_identity_key, public_data_bulk_file_source_partition_key,
        PublicDataBulkFileIdentity, PublicDataBulkFileSourcePartitionKeyInput,
    };
    use collection_domain::BRONZE_CONTENT_KEY_SEPARATOR;
    use serde_json::json;
    use uuid::Uuid;

    use super::LedgerRow;

    /// The row of `file_id` (stamped or not) of `slug`, its key `<partition><leaf>`, the lane's
    /// own request parameters beside the identity's.
    pub(in crate::bronze_epoch) fn pre_epoch_row(
        slug: &str,
        operation: &str,
        partition: &str,
        file_id: &str,
        extension: &str,
        qualified: bool,
        sha: &str,
    ) -> anyhow::Result<LedgerRow> {
        let identity = PublicDataBulkFileIdentity {
            operation: operation.to_owned(),
            provider_file_period: None,
            provider_snapshot_date: chrono::NaiveDate::from_ymd_opt(2099, 1, 2),
            provider_file_id: file_id.to_owned(),
            provider_file_name: format!("{file_id}.{extension}"),
            provider_updated_at: None,
        };
        let identity_key = public_data_bulk_file_source_identity_key(&identity)?;
        let leaf = if qualified {
            format!("{file_id}{BRONZE_CONTENT_KEY_SEPARATOR}{sha}.{extension}")
        } else {
            format!("{file_id}.{extension}")
        };
        let mut params = public_data_bulk_file_request_params(&identity);
        params["night"] = json!({"asking": {"requests": 1}});
        Ok(LedgerRow {
            id: Uuid::new_v4(),
            source_catalog_id: Uuid::nil(),
            slug: slug.to_owned(),
            object_key: format!("bronze/source={slug}/{partition}{leaf}"),
            dedupe_key: public_data_bulk_file_dedupe_key(slug, &identity_key, sha),
            source_identity_key: identity_key,
            source_partition_key: Some(public_data_bulk_file_source_partition_key(
                PublicDataBulkFileSourcePartitionKeyInput {
                    operation,
                    provider_file_id: file_id,
                },
            )?),
            provider_file_id: None,
            provider_file_name: None,
            request_params: params,
            checksum_sha256: sha.to_owned(),
            size_bytes: 9,
            content_type: "application/gzip".to_owned(),
            collected_at: Utc
                .with_ymd_and_hms(2099, 1, 2, 16, 5, 4)
                .single()
                .unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{rekey, rename_file_part, LedgerRow};

    const SHA: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// A row as a writer of before the epoch landed it: the bulk planner's fields for `slug`,
    /// `operation` and `file_id`, content-qualified when `qualified`.
    /// A row as a writer of before the epoch landed it, composed from the bulk planner's field
    /// functions (its builder refuses the stamped leaf now, G5).
    fn landed(
        slug: &str,
        operation: &str,
        file_id: &str,
        extension: &str,
        qualified: bool,
    ) -> anyhow::Result<LedgerRow> {
        let partition = if operation == slug.split_once("__").map_or("", |(_, d)| d) {
            String::new()
        } else {
            format!("operation={operation}/")
        };
        super::fixture::pre_epoch_row(
            slug, operation, &partition, file_id, extension, qualified, SHA,
        )
    }

    fn renamed() -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "courtauctiongokr__auction_item_detail".to_owned(),
                "courtauctiongokr__item_detail".to_owned(),
            ),
            (
                "vworldkr__sandan_boundary".to_owned(),
                "vworldkr__industrial_complex_boundary".to_owned(),
            ),
        ])
    }

    #[test]
    fn a_file_part_loses_its_run_stamp_and_follows_its_dataset() {
        assert_eq!(
            rename_file_part(
                "auction-item-detail-part0001-20990102t030405z",
                "auction_item_detail",
                "item_detail"
            ),
            "item-detail-part0001"
        );
        assert_eq!(
            rename_file_part(
                "auction-item-detail-part0001-20990102t030405z.gz",
                "auction_item_detail",
                "item_detail"
            ),
            "item-detail-part0001.gz"
        );
        assert_eq!(
            rename_file_part(
                "notice-detail-20990102t030405z-000",
                "notice_detail",
                "notice_detail"
            ),
            "notice-detail-000"
        );
        assert_eq!(
            rename_file_part(
                "regcode-20990102T030405Z.html",
                "legal_dong_code_table",
                "legal_dong_code"
            ),
            "regcode.html"
        );
        assert_eq!(
            rename_file_part(
                "20991231DS99991-1",
                "sandan_boundary",
                "industrial_complex_boundary"
            ),
            "20991231DS99991-1"
        );
        // A dataset prefix is renamed only as a whole part.
        assert_eq!(
            rename_file_part("auction-items-1", "auction_item", "item"),
            "auction-items-1"
        );
    }

    /// The court collector's run-stamped object of a renamed source becomes the key today's
    /// collector writes: the new source, the dataset part renamed, no stamp, the bytes' hash.
    #[test]
    fn a_stamped_object_of_a_renamed_source_takes_the_key_todays_writer_builds(
    ) -> anyhow::Result<()> {
        let row = landed(
            "courtauctiongokr__auction_item_detail",
            "selectAuctnCsSrchRslt",
            "auction-item-detail-part0001-20990102t030405z",
            "gz",
            false,
        )?;
        let moved = rekey(&row, &renamed())?.ok_or_else(|| anyhow::anyhow!("moved"))?;
        let today = landed(
            "courtauctiongokr__item_detail",
            "selectAuctnCsSrchRslt",
            "item-detail-part0001",
            "gz",
            true,
        )?;
        assert_eq!(moved.object_key, today.object_key);
        assert_eq!(moved.object_key, format!("bronze/source=courtauctiongokr__item_detail/operation=selectAuctnCsSrchRslt/item-detail-part0001--sha256-{SHA}.gz"));
        assert_eq!(
            (
                moved.dedupe_key.as_str(),
                moved.source_identity_key.as_str()
            ),
            (
                today.dedupe_key.as_str(),
                today.source_identity_key.as_str()
            )
        );
        assert_eq!(
            Some(moved.source_partition_key.as_str()),
            today.source_partition_key.as_deref()
        );
        assert_eq!(
            moved.request_params["provider_file_id"],
            "item-detail-part0001"
        );
        assert_eq!(
            moved.request_params["night"], row.request_params["night"],
            "what the lane added stays"
        );
        assert!(collection_domain::run_stamp_range(&moved.dedupe_key).is_none());
        Ok(())
    }

    /// A stamped key that was already content-qualified loses only the stamp; a renamed source's
    /// unstamped key keeps its form and follows its dataset, the operation named after it included.
    #[test]
    fn the_key_form_is_kept_but_for_the_stamp_and_the_source() -> anyhow::Result<()> {
        let none = BTreeMap::new();
        let notice = landed(
            "kicoxorkr__industrial_land_sale_notice_detail",
            "detail",
            "industrial-land-sale-notice-detail-20990102t030405z-000",
            "gz",
            true,
        )?;
        let moved = rekey(&notice, &none)?.ok_or_else(|| anyhow::anyhow!("moved"))?;
        assert_eq!(moved.object_key, format!("bronze/source=kicoxorkr__industrial_land_sale_notice_detail/operation=detail/industrial-land-sale-notice-detail-000--sha256-{SHA}.gz"));

        let boundary = landed(
            "vworldkr__sandan_boundary",
            "sandan_boundary",
            "20991231DS99991-1",
            "zip",
            false,
        )?;
        assert_eq!(
            boundary.object_key,
            "bronze/source=vworldkr__sandan_boundary/20991231DS99991-1.zip"
        );
        let moved = rekey(&boundary, &renamed())?.ok_or_else(|| anyhow::anyhow!("moved"))?;
        assert_eq!(
            moved.object_key,
            "bronze/source=vworldkr__industrial_complex_boundary/20991231DS99991-1.zip"
        );
        assert_eq!(
            moved.source_partition_key,
            "operation=industrial_complex_boundary/provider_file_id=20991231DS99991-1"
        );
        assert_eq!(
            moved.request_params["operation"],
            "industrial_complex_boundary"
        );

        // A recovered row (2026-11-28 04:50Z: the 14 sandan rows) has the name in its column only.
        let mut recovered = boundary.clone();
        let name = recovered.request_params["provider_file_name"].clone();
        recovered
            .request_params
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("params"))?
            .remove("provider_file_name");
        recovered.provider_file_name = name.as_str().map(str::to_owned);
        let again = rekey(&recovered, &renamed())?.ok_or_else(|| anyhow::anyhow!("moved"))?;
        assert_eq!(again.object_key, moved.object_key);
        assert!(again.request_params.get("provider_file_name").is_none());
        recovered.provider_file_name = None;
        assert!(rekey(&recovered, &renamed()).is_err());
        Ok(())
    }

    #[test]
    fn an_unstamped_object_of_a_kept_source_is_left_and_a_row_of_another_shape_is_refused(
    ) -> anyhow::Result<()> {
        let none = BTreeMap::new();
        let kept = landed(
            "datagokr__onbid_item_bid_info",
            "getCltrBidInf2",
            "item-bid-info",
            "gz",
            true,
        )?;
        assert_eq!(rekey(&kept, &none)?, None);
        let mut odd = landed(
            "datagokr__onbid_item_bid_info",
            "getCltrBidInf2",
            "item-bid-info-20990102t030405z",
            "gz",
            false,
        )?;
        odd.dedupe_key = "datagokr__onbid_item_bid_info:hand-written".to_owned();
        assert!(
            rekey(&odd, &none).is_err(),
            "a row the planner does not rebuild is never guessed at"
        );
        Ok(())
    }
}
