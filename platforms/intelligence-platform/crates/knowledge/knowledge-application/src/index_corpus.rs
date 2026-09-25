use async_trait::async_trait;

use crate::index_ports::{IndexedChunk, KnowledgeIndexError, KnowledgeIndexPort, ReleaseRef};

/// 원천 문서 하나의 원문과 스냅숏 식별자.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceDocument {
    pub source_id: String,
    pub text: String,
    /// 원본 바이트 전체의 SHA-256.
    pub source_snapshot_id: String,
}

/// 원천에서 문서를 읽어 오는 경계. 지금의 유일한 구현은 로컬 파일이다.
#[async_trait]
pub trait KnowledgeSourceReaderPort: Send + Sync {
    async fn read_all(&self) -> Result<Vec<SourceDocument>, KnowledgeIndexError>;
}

/// 원천을 읽어 청킹하고 한 release로 색인한 뒤 활성화한다.
pub struct IndexCorpus<'a> {
    pub reader: &'a dyn KnowledgeSourceReaderPort,
    pub index: &'a dyn KnowledgeIndexPort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexCorpusReport {
    pub documents: u64,
    pub chunks: u64,
}

impl IndexCorpus<'_> {
    pub async fn execute(
        &self,
        release: &ReleaseRef,
    ) -> Result<IndexCorpusReport, KnowledgeIndexError> {
        let documents = self.reader.read_all().await?;
        let mut indexed = Vec::new();
        for document in &documents {
            let chunks = knowledge_domain::split_markdown(&document.source_id, &document.text)
                .map_err(|error| KnowledgeIndexError::InvalidRequest {
                    message: error.to_string(),
                })?;
            for chunk in chunks {
                indexed.push(IndexedChunk {
                    chunk,
                    source_snapshot_id: document.source_snapshot_id.clone(),
                });
            }
        }

        let chunks = self.index.index_chunks(release, indexed).await?;
        self.index.activate_release(release).await?;

        Ok(IndexCorpusReport {
            documents: documents.len() as u64,
            chunks,
        })
    }
}
