use async_trait::async_trait;
use knowledge_domain::{
    validate_knowledge_source_event, KnowledgeSourceRecord, KnowledgeSourceUpserted,
};

use crate::{KnowledgeProjectionError, KnowledgeProjectionPort, KnowledgeSourceRegistryPort};

pub struct UpsertKnowledgeSource<R> {
    registry: R,
}

impl<R> UpsertKnowledgeSource<R> {
    pub const fn new(registry: R) -> Self {
        Self { registry }
    }
}

impl<R> UpsertKnowledgeSource<R>
where
    R: KnowledgeSourceRegistryPort,
{
    pub async fn execute(
        &self,
        event: KnowledgeSourceUpserted,
    ) -> Result<KnowledgeSourceRecord, KnowledgeProjectionError> {
        validate_knowledge_source_event(&event).map_err(|error| {
            KnowledgeProjectionError::InvalidEvent {
                message: error.to_string(),
            }
        })?;
        self.registry.upsert_source(event).await
    }
}

#[async_trait]
impl<R> KnowledgeProjectionPort for UpsertKnowledgeSource<R>
where
    R: KnowledgeSourceRegistryPort,
{
    async fn record_source_upsert(
        &self,
        event: KnowledgeSourceUpserted,
    ) -> Result<(), KnowledgeProjectionError> {
        self.execute(event).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use knowledge_domain::{KnowledgeSourceRecord, KnowledgeSourceUpserted};

    use super::*;

    struct RecordingRegistry {
        writes: Arc<AtomicUsize>,
    }

    impl Default for RecordingRegistry {
        fn default() -> Self {
            Self {
                writes: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl KnowledgeSourceRegistryPort for RecordingRegistry {
        async fn upsert_source(
            &self,
            event: KnowledgeSourceUpserted,
        ) -> Result<KnowledgeSourceRecord, KnowledgeProjectionError> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            Ok(KnowledgeSourceRecord {
                tenant_id: event.tenant_id,
                product_id: event.product_id,
                source_id: event.source_id,
                source_kind: event.source_kind,
                source_uri: event.source_uri,
                content_uri: event.content_uri,
                content_checksum_sha256: event.content_checksum_sha256,
                last_event_id: event.event_id,
                last_seen_at_millis: event.occurred_at_millis,
                metadata: event.metadata,
                version: 1,
            })
        }

        async fn get_source(
            &self,
            _tenant_id: &str,
            _product_id: &str,
            _source_id: &str,
        ) -> Result<Option<KnowledgeSourceRecord>, KnowledgeProjectionError> {
            Ok(None)
        }
    }

    fn source_event(uri: &str) -> KnowledgeSourceUpserted {
        KnowledgeSourceUpserted {
            event_id: "event-1".to_owned(),
            tenant_id: "tenant-1".to_owned(),
            product_id: "foundation-platform".to_owned(),
            source_id: "building-register-floor".to_owned(),
            source_kind: "silver-table".to_owned(),
            source_uri: uri.to_owned(),
            content_uri: None,
            content_checksum_sha256: None,
            occurred_at_millis: 1,
            metadata: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn invalid_event_never_reaches_registry() {
        let registry = RecordingRegistry::default();
        let writes = Arc::clone(&registry.writes);
        let use_case = UpsertKnowledgeSource::new(registry);
        let result = use_case.execute(source_event("invalid-uri")).await;

        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("invalid source URI must be rejected"),
        };
        assert_eq!(error.safe_message(), "knowledge event is invalid");
        assert_eq!(
            error.to_string(),
            "knowledge event is invalid: source_uri must use s3, http, or https scheme"
        );
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn valid_event_is_written_once() {
        let registry = RecordingRegistry::default();
        let writes = Arc::clone(&registry.writes);
        let use_case = UpsertKnowledgeSource::new(registry);
        let result = use_case
            .execute(source_event("s3://foundation-platform/source"))
            .await;

        assert!(result.is_ok());
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn projection_port_uses_the_upsert_use_case() {
        let registry = RecordingRegistry::default();
        let writes = Arc::clone(&registry.writes);
        let use_case = UpsertKnowledgeSource::new(registry);
        let projection: &dyn KnowledgeProjectionPort = &use_case;

        let result = projection
            .record_source_upsert(source_event("s3://foundation-platform/source"))
            .await;

        assert!(
            result.is_ok(),
            "valid event must be projected through the use case"
        );
        assert_eq!(writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn projection_port_rejects_invalid_events_before_registry_write() {
        let registry = RecordingRegistry::default();
        let writes = Arc::clone(&registry.writes);
        let use_case = UpsertKnowledgeSource::new(registry);
        let projection: &dyn KnowledgeProjectionPort = &use_case;

        let result = projection
            .record_source_upsert(source_event("invalid-uri"))
            .await;

        assert!(matches!(
            result,
            Err(KnowledgeProjectionError::InvalidEvent { .. })
        ));
        assert_eq!(writes.load(Ordering::SeqCst), 0);
    }
}
