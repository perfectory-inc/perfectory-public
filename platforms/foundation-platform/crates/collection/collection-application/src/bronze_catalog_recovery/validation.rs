use chrono::Datelike as _;
use collection_domain::{validate_bronze_object_key_contract, SnapshotBasis, SnapshotGranularity};

use super::{
    invalid, BronzeCatalogRecoveryCandidate, BronzeCatalogRecoveryError,
    BronzeCatalogRecoveryInput, ExistingBronzeObject,
};

pub(super) fn validate_input(
    input: &BronzeCatalogRecoveryInput,
) -> Result<(), BronzeCatalogRecoveryError> {
    let source_slug = input.source.slug.trim();
    if source_slug.is_empty() {
        return Err(invalid("source slug must not be empty"));
    }
    if input.evidence_manifest_uri.trim().is_empty() {
        return Err(invalid("evidence manifest URI must not be empty"));
    }
    if !is_sha256(&input.evidence_manifest_sha256) {
        return Err(invalid(
            "evidence manifest checksum must be lowercase SHA-256",
        ));
    }
    if input.candidates.is_empty() {
        return Err(invalid("at least one recovery candidate is required"));
    }

    let expected_prefix = format!("bronze/source={source_slug}/");
    for candidate in &input.candidates {
        validate_bronze_object_key_contract(candidate.object_key.as_str())
            .map_err(|error| invalid(format!("{}: {error}", candidate.object_key.as_str())))?;
        if !candidate.object_key.as_str().starts_with(&expected_prefix) {
            return Err(invalid(format!(
                "object key source does not match manifest source {source_slug}: {}",
                candidate.object_key.as_str()
            )));
        }
        if candidate.expected_size_bytes == 0 {
            return Err(invalid(format!(
                "zero-byte Bronze recovery candidate is not allowed: {}",
                candidate.object_key.as_str()
            )));
        }
        if candidate
            .expected_checksum_sha256
            .as_ref()
            .is_some_and(|checksum| !is_sha256(checksum))
        {
            return Err(invalid(format!(
                "candidate checksum must be lowercase SHA-256: {}",
                candidate.object_key.as_str()
            )));
        }
        if candidate.source_identity_key.trim().is_empty() {
            return Err(invalid(format!(
                "source identity key must not be empty: {}",
                candidate.object_key.as_str()
            )));
        }
        if !candidate.request_params.is_object() {
            return Err(invalid(format!(
                "request_params must be a JSON object: {}",
                candidate.object_key.as_str()
            )));
        }
        if candidate.request_params.get("catalog_recovery").is_some() {
            return Err(invalid(format!(
                "request_params uses reserved catalog_recovery field: {}",
                candidate.object_key.as_str()
            )));
        }
        if candidate.content_type.trim().is_empty() {
            return Err(invalid(format!(
                "content type must not be empty: {}",
                candidate.object_key.as_str()
            )));
        }
        if candidate.observed_r2_etag.trim().is_empty()
            || candidate.observed_r2_etag.trim() != candidate.observed_r2_etag
        {
            return Err(invalid(format!(
                "inventory ETag must be non-empty and whitespace-free: {}",
                candidate.object_key.as_str()
            )));
        }
        if !candidate.evidence_kind.is_authoritative() {
            return Err(invalid(format!(
                "object path inference is not authoritative recovery evidence: {}",
                candidate.object_key.as_str()
            )));
        }
        validate_snapshot_metadata(candidate)?;
    }
    Ok(())
}

fn validate_snapshot_metadata(
    candidate: &BronzeCatalogRecoveryCandidate,
) -> Result<(), BronzeCatalogRecoveryError> {
    if candidate.snapshot_granularity == SnapshotGranularity::Month
        && candidate.snapshot_date.day() != 1
    {
        return Err(invalid(format!(
            "month-granularity snapshot date must use day 1: {}",
            candidate.object_key.as_str()
        )));
    }
    match candidate.snapshot_basis {
        SnapshotBasis::ProviderFilePeriod => {
            let period = candidate.snapshot_period.as_deref().unwrap_or_default();
            if !is_year_month(period) {
                return Err(invalid(format!(
                    "provider-file-period snapshot requires YYYY-MM snapshot_period: {}",
                    candidate.object_key.as_str()
                )));
            }
        }
        SnapshotBasis::ProviderUpdatedAt if candidate.provider_updated_at.is_none() => {
            return Err(invalid(format!(
                "provider-updated-at snapshot requires provider_updated_at: {}",
                candidate.object_key.as_str()
            )));
        }
        SnapshotBasis::ProviderSnapshotDate
        | SnapshotBasis::RequestMonth
        | SnapshotBasis::CollectedAtFallback
        | SnapshotBasis::ProviderUpdatedAt => {}
    }
    Ok(())
}

pub(super) fn validate_observed_object(
    candidate: &BronzeCatalogRecoveryCandidate,
    object: &ExistingBronzeObject,
) -> Result<(), BronzeCatalogRecoveryError> {
    if object.observed_r2_etag != candidate.observed_r2_etag
        || object.observed_r2_last_modified.timestamp()
            != candidate.observed_r2_last_modified.timestamp()
    {
        return Err(BronzeCatalogRecoveryError::ObjectVersionMismatch {
            key: candidate.object_key.as_str().to_owned(),
            expected_etag: candidate.observed_r2_etag.clone(),
            observed_etag: object.observed_r2_etag.clone(),
            expected_last_modified: candidate.observed_r2_last_modified,
            observed_last_modified: object.observed_r2_last_modified,
        });
    }
    if object.size_bytes != candidate.expected_size_bytes {
        return Err(BronzeCatalogRecoveryError::SizeMismatch {
            key: candidate.object_key.as_str().to_owned(),
            expected: candidate.expected_size_bytes,
            observed: object.size_bytes,
        });
    }
    if !is_sha256(&object.checksum_sha256) {
        return Err(invalid(format!(
            "storage reader returned a non-canonical checksum: {}",
            candidate.object_key.as_str()
        )));
    }
    if candidate
        .expected_checksum_sha256
        .as_ref()
        .is_some_and(|expected| expected != &object.checksum_sha256)
    {
        return Err(BronzeCatalogRecoveryError::ChecksumMismatch {
            key: candidate.object_key.as_str().to_owned(),
        });
    }
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_year_month(value: &str) -> bool {
    if value.len() != 7 || value.as_bytes().get(4) != Some(&b'-') {
        return false;
    }
    let year = value[0..4].parse::<u16>();
    let month = value[5..7].parse::<u8>();
    year.is_ok() && month.is_ok_and(|month| (1..=12).contains(&month))
}
