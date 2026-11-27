//! One API's responses of one run as one Bronze object, written through the one Bronze line
//! writer (root ADR-0280, ADR-0616 §3-1).

use std::path::Path;

use anyhow::bail;
use chrono::{DateTime, Utc};
use collection_application::ports::BronzeIngestUnitOfWork;
use collection_domain::{source_slug, SourceAuthKind, SourceCatalogEntry, SourcePayloadFormat};
use foundation_outbox::ObjectStorageService;
use foundation_shared_kernel::ids::SourceCatalogId;
use serde::Serialize;
use serde_json::Value as JsonValue;
use uuid::Uuid;

use super::{Api, Endpoint, SourceContract, PROVIDER};
use crate::bronze_line_objects::{JsonLinesWriter, LineObject, LineObjects, Target};

/// The provider file id of one API's object: the API's name. The key adds the object's checksum
/// (root ADR-0152), so two runs' answers are two objects and the same answer is one; when a run
/// collected it is its ledger row's `collected_at`, never a part of the key (root ADR-0626 rule 5).
pub(crate) fn file_id(api: Api) -> String {
    api.name().replace('_', "-")
}

/// One landed (or, in a dry run, planned) object, as the manifest names it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct ManifestObject {
    pub(crate) role: String,
    pub(crate) dataset_slug: String,
    pub(crate) object_key: String,
    pub(crate) checksum_sha256: String,
    pub(crate) size_bytes: u64,
    pub(crate) lines: usize,
    /// Where a dry run wrote it, relative to the output directory.
    pub(crate) local_path: Option<String>,
    pub(crate) written: bool,
}

/// Commits one API's lines of one run as one object (one group: never split).
pub(crate) async fn persist<Uow, Storage>(
    contract: &SourceContract,
    output_dir: &Path,
    api: Api,
    lines: &[String],
    at: DateTime<Utc>,
    live: Option<(&Uow, &Storage)>,
) -> anyhow::Result<ManifestObject>
where
    Uow: BronzeIngestUnitOfWork + ?Sized,
    Storage: ObjectStorageService + ?Sized,
{
    let endpoint = contract.endpoint(api);
    let mut objects = JsonLinesWriter::new(
        LineObjects {
            source: source_catalog_entry(contract, endpoint, at)?,
            operation: &endpoint.operation,
            collected_at: at,
            file_id: Box::new(move |_| file_id(api)),
            content_qualified: true,
            params: Box::new(|| JsonValue::Null),
        },
        Target::of(live, output_dir),
    )?;
    objects.push_group(lines).await?;
    let landed = objects.finish().await?;
    let [object] = landed.as_slice() else {
        bail!(
            "{} landed as {} objects: one group is one object",
            api.name(),
            landed.len()
        );
    };
    Ok(manifest_object(api, endpoint, object))
}

fn manifest_object(api: Api, endpoint: &Endpoint, object: &LineObject) -> ManifestObject {
    ManifestObject {
        role: api.name().to_owned(),
        dataset_slug: endpoint.dataset_slug.clone(),
        object_key: object.object_key.clone(),
        checksum_sha256: object.checksum_sha256.clone(),
        size_bytes: object.size_bytes,
        lines: object.lines,
        local_path: object.local_path.clone(),
        written: object.written,
    }
}

fn source_catalog_entry(
    contract: &SourceContract,
    endpoint: &Endpoint,
    now: DateTime<Utc>,
) -> anyhow::Result<SourceCatalogEntry> {
    Ok(SourceCatalogEntry {
        id: SourceCatalogId::new(Uuid::new_v4()),
        slug: source_slug(PROVIDER, &endpoint.dataset_slug)?,
        name: endpoint.dataset_slug.clone(),
        provider: PROVIDER.to_owned(),
        dataset_name: endpoint.dataset_slug.clone(),
        base_url: Some(contract.base_url.clone()),
        auth_kind: SourceAuthKind::ServiceKey,
        payload_format: SourcePayloadFormat::Json,
        license_name: None,
        license_url: None,
        terms_url: None,
        collection_frequency: Some(format!("every {} hours", endpoint.every_hours)),
        is_active: true,
        created_at: now,
        updated_at: now,
        version: 1,
    })
}
