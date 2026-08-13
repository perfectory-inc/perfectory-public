pub mod index_corpus;
pub mod index_ports;
pub mod ports;
pub mod upsert_source;

pub use index_corpus::{IndexCorpus, IndexCorpusReport, KnowledgeSourceReaderPort, SourceDocument};
pub use index_ports::{
    IndexedChunk, KnowledgeIndexError, KnowledgeIndexPort, ReleaseRef, SearchHit, TenantScope,
    SCAFFOLD_TENANT_ID,
};
pub use ports::{KnowledgeProjectionError, KnowledgeProjectionPort, KnowledgeSourceRegistryPort};
pub use upsert_source::UpsertKnowledgeSource;
