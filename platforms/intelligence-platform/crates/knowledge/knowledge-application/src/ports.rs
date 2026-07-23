use std::fmt;

use async_trait::async_trait;
use knowledge_domain::{KnowledgeSourceRecord, KnowledgeSourceUpserted};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KnowledgeProjectionError {
    InvalidEvent { message: String },
    StoreUnavailable { message: String },
}

impl KnowledgeProjectionError {
    pub const fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidEvent { .. } => "knowledge event is invalid",
            Self::StoreUnavailable { .. } => "knowledge projection durable effect failed",
        }
    }
}

impl fmt::Display for KnowledgeProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEvent { message } => {
                write!(formatter, "knowledge event is invalid: {message}")
            }
            Self::StoreUnavailable { message } => {
                write!(
                    formatter,
                    "knowledge projection durable effect failed: {message}"
                )
            }
        }
    }
}

impl std::error::Error for KnowledgeProjectionError {}

#[async_trait]
pub trait KnowledgeProjectionPort: Send + Sync {
    async fn record_source_upsert(
        &self,
        event: KnowledgeSourceUpserted,
    ) -> Result<(), KnowledgeProjectionError>;
}

#[async_trait]
pub trait KnowledgeSourceRegistryPort: Send + Sync {
    async fn upsert_source(
        &self,
        event: KnowledgeSourceUpserted,
    ) -> Result<KnowledgeSourceRecord, KnowledgeProjectionError>;

    async fn get_source(
        &self,
        tenant_id: &str,
        product_id: &str,
        source_id: &str,
    ) -> Result<Option<KnowledgeSourceRecord>, KnowledgeProjectionError>;
}
