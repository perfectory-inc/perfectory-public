mod memory_registry;
mod postgres_registry;

pub use memory_registry::InMemoryKnowledgeSourceRegistry;
pub use postgres_registry::{
    PostgresKnowledgeSourceRegistry, PostgresKnowledgeSourceRegistryConfig,
    PostgresKnowledgeSourceRegistryError,
};
