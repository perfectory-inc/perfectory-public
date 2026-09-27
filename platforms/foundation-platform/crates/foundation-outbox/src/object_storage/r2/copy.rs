//! Server-side immutable copies; all byte verification uses the existing bounded rehash reader.

use aws_sdk_s3::{
    error::ProvideErrorMetadata as _,
    operation::head_object::HeadObjectOutput,
    types::{CompletedMultipartUpload, CompletedPart},
};

use crate::{
    errors::PublishError,
    object_storage::{
        copy::{copy_plan, CopyPlan},
        CreateOnlyCopyObjectRequest, ObjectWriteMode,
    },
};

use super::{
    is_create_only_already_exists_response, is_r2_not_found, map_r2_put_error,
    validate_relative_r2_object_key, R2ObjectStorage,
};

impl R2ObjectStorage {
    pub(super) async fn copy_immutable_object(
        &self,
        request: CreateOnlyCopyObjectRequest,
    ) -> Result<(), PublishError> {
        let plan = copy_plan(&request)?;
        validate_relative_r2_object_key(&request.source_key, "copy source_key")?;
        validate_relative_r2_object_key(&request.destination_key, "copy destination_key")?;
        self.require_copy_destination_absent(&request.destination_key)
            .await?;
        let source = self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(&request.source_key)
            .send()
            .await
            .map_err(|error| {
                map_r2_put_error(
                    &request.source_key,
                    &error,
                    "head copy source",
                    ObjectWriteMode::OverwriteAllowed,
                )
            })?;
        if source
            .content_length()
            .and_then(|size| u64::try_from(size).ok())
            != Some(request.size_bytes)
        {
            return Err(PublishError::Infrastructure(
                "R2 copy source size differs from source ledger".to_owned(),
            ));
        }
        if source.e_tag().is_none_or(str::is_empty) {
            return Err(PublishError::Infrastructure(
                "R2 copy source omitted ETag required to pin source bytes".to_owned(),
            ));
        }
        match plan {
            CopyPlan::Single => self.copy_single_create_only(&request, &source).await,
            CopyPlan::Multipart {
                part_bytes,
                part_count,
            } => {
                self.copy_multipart_create_only(&request, &source, part_bytes, part_count)
                    .await
            }
        }
    }

    async fn require_copy_destination_absent(&self, key: &str) -> Result<(), PublishError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket_name)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Err(PublishError::ObjectAlreadyExists {
                key: key.to_owned(),
            }),
            Err(error) if is_r2_not_found(&error) => Ok(()),
            Err(error) => Err(map_r2_put_error(
                key,
                &error,
                "head copy destination",
                ObjectWriteMode::OverwriteAllowed,
            )),
        }
    }

    async fn copy_single_create_only(
        &self,
        request: &CreateOnlyCopyObjectRequest,
        source: &HeadObjectOutput,
    ) -> Result<(), PublishError> {
        let builder = self
            .client
            .copy_object()
            .bucket(&self.bucket_name)
            .key(&request.destination_key)
            .copy_source(encoded_copy_source(&self.bucket_name, &request.source_key))
            .set_copy_source_if_match(source.e_tag().map(str::to_owned));
        // S3's standard destination precondition plus R2's documented destination extension.
        // The extension protects R2 versions that ignore the standard header.
        // https://developers.cloudflare.com/r2/api/s3/extensions/#conditional-operations-in-copyobject-for-the-destination-object
        let conditional = builder
            .clone()
            .if_none_match("*")
            .customize()
            .mutate_request(|http| {
                http.headers_mut()
                    .insert("cf-copy-destination-if-none-match", "*");
            })
            .send()
            .await;
        match conditional {
            Ok(_) => Ok(()),
            Err(error)
                if explicitly_rejected_conditional_header(
                    error
                        .raw_response()
                        .map(|response| response.status().as_u16()),
                    error.as_service_error().and_then(|service| service.code()),
                    error
                        .as_service_error()
                        .and_then(|service| service.message()),
                ) =>
            {
                // The source remains pinned by If-Match. The caller owns the single full-byte
                // destination rehash against ledger evidence, including on fallback.
                warn_copy_fallback();
                self.require_copy_destination_absent(&request.destination_key)
                    .await?;
                builder
                    .customize()
                    .config_override(copy_fallback_config())
                    .send()
                    .await
                    .map_err(|error| {
                        map_r2_put_error(
                            &request.destination_key,
                            &error,
                            "copy after conditional header rejection",
                            ObjectWriteMode::OverwriteAllowed,
                        )
                    })?;
                Ok(())
            }
            Err(error) => {
                // CopyObject has TWO preconditions. A 412 alone cannot identify a destination
                // collision. Reconcile only if the pinned source is unchanged and HEAD actually
                // observes the destination; ambiguous/source failures must remain hard errors.
                if is_create_only_already_exists_response(
                    ObjectWriteMode::CreateOnly,
                    error
                        .raw_response()
                        .map(|response| response.status().as_u16()),
                    error.as_service_error().and_then(|service| service.code()),
                ) {
                    let current_source = self
                        .client
                        .head_object()
                        .bucket(&self.bucket_name)
                        .key(&request.source_key)
                        .send()
                        .await
                        .map_err(|error| {
                            map_r2_put_error(
                                &request.source_key,
                                &error,
                                "head copy source after precondition failure",
                                ObjectWriteMode::OverwriteAllowed,
                            )
                        })?;
                    if current_source.e_tag() == source.e_tag() {
                        self.require_copy_destination_absent(&request.destination_key)
                            .await?;
                    }
                }
                Err(map_r2_put_error(
                    &request.destination_key,
                    &error,
                    "copy with pinned source",
                    ObjectWriteMode::OverwriteAllowed,
                ))
            }
        }
    }

    async fn copy_multipart_create_only(
        &self,
        request: &CreateOnlyCopyObjectRequest,
        source: &HeadObjectOutput,
        part_bytes: u64,
        part_count: u64,
    ) -> Result<(), PublishError> {
        let created = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(&request.destination_key)
            .set_content_type(source.content_type().map(str::to_owned))
            .set_cache_control(source.cache_control().map(str::to_owned))
            .set_content_disposition(source.content_disposition().map(str::to_owned))
            .set_content_encoding(source.content_encoding().map(str::to_owned))
            .set_content_language(source.content_language().map(str::to_owned))
            .set_metadata(source.metadata().cloned())
            .send()
            .await
            .map_err(|error| {
                map_r2_put_error(
                    &request.destination_key,
                    &error,
                    "start multipart copy",
                    ObjectWriteMode::CreateOnly,
                )
            })?;
        let upload_id = created.upload_id().ok_or_else(|| {
            PublishError::Infrastructure("R2 multipart copy response omitted upload ID".to_owned())
        })?;
        let result = self
            .copy_parts_and_complete(request, source, upload_id, part_bytes, part_count)
            .await;
        if result.is_err() {
            if let Err(error) = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket_name)
                .key(&request.destination_key)
                .upload_id(upload_id)
                .send()
                .await
            {
                tracing::error!(%error, "failed to abort incomplete R2 multipart copy");
            }
        }
        result
    }

    async fn copy_parts_and_complete(
        &self,
        request: &CreateOnlyCopyObjectRequest,
        source: &HeadObjectOutput,
        upload_id: &str,
        part_bytes: u64,
        part_count: u64,
    ) -> Result<(), PublishError> {
        let mut parts = Vec::new();
        let copy_source = encoded_copy_source(&self.bucket_name, &request.source_key);
        // Sequential requests bound provider pressure and retain only <=10,000 small ETag records.
        for index in 0..part_count {
            let part_number = i32::try_from(index + 1).map_err(|_| {
                PublishError::Infrastructure("copy part number overflow".to_owned())
            })?;
            let start = index * part_bytes;
            let end = (start + part_bytes).min(request.size_bytes) - 1;
            let copied = self
                .client
                .upload_part_copy()
                .bucket(&self.bucket_name)
                .key(&request.destination_key)
                .upload_id(upload_id)
                .copy_source(&copy_source)
                .set_copy_source_if_match(source.e_tag().map(str::to_owned))
                // S3 permits a range only when the source exceeds 5 MiB. A one-part copy needs none.
                .set_copy_source_range((part_count > 1).then(|| format!("bytes={start}-{end}")))
                .part_number(part_number)
                .send()
                .await
                .map_err(|error| {
                    map_r2_put_error(
                        &request.destination_key,
                        &error,
                        "copy multipart source range",
                        ObjectWriteMode::OverwriteAllowed,
                    )
                })?;
            let e_tag = copied
                .copy_part_result()
                .and_then(|part| part.e_tag())
                .ok_or_else(|| {
                    PublishError::Infrastructure("R2 copied part response omitted ETag".to_owned())
                })?;
            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(e_tag)
                    .build(),
            );
        }
        let builder = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(&request.destination_key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            );
        match builder.clone().if_none_match("*").send().await {
            Ok(_) => Ok(()),
            Err(error)
                if explicitly_rejected_conditional_header(
                    error
                        .raw_response()
                        .map(|response| response.status().as_u16()),
                    error.as_service_error().and_then(|service| service.code()),
                    error
                        .as_service_error()
                        .and_then(|service| service.message()),
                ) =>
            {
                // Parts were pinned to the source ETag. Only the caller rehashes the destination.
                warn_copy_fallback();
                self.require_copy_destination_absent(&request.destination_key)
                    .await?;
                builder
                    .customize()
                    .config_override(copy_fallback_config())
                    .send()
                    .await
                    .map_err(|error| {
                        map_r2_put_error(
                            &request.destination_key,
                            &error,
                            "complete copy after conditional header rejection",
                            ObjectWriteMode::CreateOnly,
                        )
                    })?;
                Ok(())
            }
            Err(error) => Err(map_r2_put_error(
                &request.destination_key,
                &error,
                "complete multipart copy",
                ObjectWriteMode::CreateOnly,
            )),
        }
    }
}

/// HEAD+copy needs exclusive destination ownership. Only an explicit unsupported-header
/// response selects this fallback, and the caller must rehash against its source ledger.
fn warn_copy_fallback() {
    tracing::warn!("R2 rejected copy destination precondition; using serialized HEAD-absent copy with caller rehash");
}

fn copy_fallback_config() -> aws_sdk_s3::config::Builder {
    // A retry after a lost response must re-enter through destination HEAD and full reconciliation,
    // never automatically repeat an unconditional mutation after the original absence check.
    aws_sdk_s3::config::Builder::new()
        .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
}

fn explicitly_rejected_conditional_header(
    status: Option<u16>,
    code: Option<&str>,
    message: Option<&str>,
) -> bool {
    let message = message.unwrap_or_default().to_ascii_lowercase();
    matches!(status, Some(400 | 501))
        && matches!(
            code,
            Some(
                "NotImplemented"
                    | "NotSupported"
                    | "UnsupportedHeader"
                    | "InvalidArgument"
                    | "InvalidRequest"
            )
        )
        && (message.contains("if-none-match") || message.contains("conditional header"))
        && (message.contains("not support")
            || message.contains("unsupported")
            || message.contains("not implement"))
}

fn encoded_copy_source(bucket: &str, key: &str) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::new();
    for byte in format!("{bucket}/{key}").bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~/".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

#[cfg(test)]
#[path = "copy_protocol_tests.rs"]
mod protocol_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_requires_explicit_unsupported_conditional_header() {
        let message = Some("If-None-Match header is not supported");
        assert!(explicitly_rejected_conditional_header(
            Some(501),
            Some("NotImplemented"),
            message
        ));
        assert!(explicitly_rejected_conditional_header(
            Some(400),
            Some("InvalidRequest"),
            message
        ));
        for (status, code, message) in [
            (
                Some(501),
                Some("NotImplemented"),
                Some("CopyObject not implemented"),
            ),
            (Some(500), Some("InternalError"), message),
            (Some(403), Some("AccessDenied"), message),
            (Some(412), Some("PreconditionFailed"), message),
            (Some(409), Some("ConditionalRequestConflict"), message),
            (
                Some(400),
                Some("InvalidArgument"),
                Some("invalid If-None-Match value"),
            ),
            (None, None, None),
        ] {
            assert!(!explicitly_rejected_conditional_header(
                status, code, message
            ));
        }
    }

    #[test]
    fn source_header_encodes_reserved_bytes_without_losing_key_segments() {
        assert_eq!(
            encoded_copy_source("bucket", "folder/a +%?#.bin"),
            "bucket/folder/a%20%2B%25%3F%23.bin"
        );
    }
}
