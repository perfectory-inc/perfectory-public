use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use knowledge_application::{KnowledgeProjectionError, KnowledgeSourceRegistryPort};
use knowledge_domain::{KnowledgeSourceRecord, KnowledgeSourceUpserted};

type RegistryKey = (String, String, String);
type RegistryMap = BTreeMap<RegistryKey, KnowledgeSourceRecord>;

#[derive(Clone, Default)]
pub struct InMemoryKnowledgeSourceRegistry {
    sources: Arc<Mutex<RegistryMap>>,
}

impl InMemoryKnowledgeSourceRegistry {
    fn lock_sources(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, RegistryMap>, KnowledgeProjectionError> {
        self.sources
            .lock()
            .map_err(|error| KnowledgeProjectionError::StoreUnavailable {
                message: format!("knowledge source registry mutex poisoned: {error}"),
            })
    }
}

#[async_trait]
impl KnowledgeSourceRegistryPort for InMemoryKnowledgeSourceRegistry {
    async fn upsert_source(
        &self,
        event: KnowledgeSourceUpserted,
    ) -> Result<KnowledgeSourceRecord, KnowledgeProjectionError> {
        let key = (
            event.tenant_id.clone(),
            event.product_id.clone(),
            event.source_id.clone(),
        );
        let mut sources = self.lock_sources()?;
        if let Some(existing) = sources
            .get(&key)
            .filter(|record| record.last_event_id == event.event_id)
        {
            return Ok(existing.clone());
        }
        let next_version = sources.get(&key).map_or(1, |record| record.version + 1);
        let record = KnowledgeSourceRecord {
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
            version: next_version,
        };
        sources.insert(key, record.clone());
        Ok(record)
    }

    async fn get_source(
        &self,
        tenant_id: &str,
        product_id: &str,
        source_id: &str,
    ) -> Result<Option<KnowledgeSourceRecord>, KnowledgeProjectionError> {
        let sources = self.lock_sources()?;
        Ok(sources
            .get(&(
                tenant_id.to_string(),
                product_id.to_string(),
                source_id.to_string(),
            ))
            .cloned())
    }
}
