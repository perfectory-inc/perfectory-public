use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use knowledge_application::{
    IndexedChunk, KnowledgeIndexError, KnowledgeIndexPort, ReleaseRef, SearchHit, TenantScope,
};

type ChunkKey = (String, String, String, String, i32);

/// 테스트와 loopback 개발용 색인. 순위는 단어 포함 여부로만 매긴다.
///
/// Postgres 어댑터와 같은 계약을 만족하지만 순위 규칙은 같지 않다. 순위 품질에 관한
/// 단정은 Postgres 스위트에서만 한다.
#[derive(Debug, Default)]
pub struct InMemoryKnowledgeIndex {
    chunks: Mutex<BTreeMap<ChunkKey, IndexedChunk>>,
    active: Mutex<BTreeMap<(String, String), String>>,
}

#[async_trait]
impl KnowledgeIndexPort for InMemoryKnowledgeIndex {
    async fn index_chunks(
        &self,
        release: &ReleaseRef,
        chunks: Vec<IndexedChunk>,
    ) -> Result<u64, KnowledgeIndexError> {
        let mut store = self.chunks.lock().map_err(poisoned)?;
        let mut written = 0_u64;
        for indexed in chunks {
            let key = (
                release.scope.tenant_id.clone(),
                release.scope.product_id.clone(),
                release.release_id.clone(),
                indexed.chunk.source_id.clone(),
                indexed.chunk.chunk_ordinal,
            );
            store.insert(key, indexed);
            written += 1;
        }
        Ok(written)
    }

    async fn activate_release(&self, release: &ReleaseRef) -> Result<(), KnowledgeIndexError> {
        let mut active = self.active.lock().map_err(poisoned)?;
        active.insert(
            (
                release.scope.tenant_id.clone(),
                release.scope.product_id.clone(),
            ),
            release.release_id.clone(),
        );
        Ok(())
    }

    async fn search(
        &self,
        scope: &TenantScope,
        query: &str,
        limit: u32,
    ) -> Result<Vec<SearchHit>, KnowledgeIndexError> {
        if query.trim().is_empty() {
            return Err(KnowledgeIndexError::InvalidRequest {
                message: "query must be non-empty".to_string(),
            });
        }
        let release_id = {
            let active = self.active.lock().map_err(poisoned)?;
            match active.get(&(scope.tenant_id.clone(), scope.product_id.clone())) {
                Some(release_id) => release_id.clone(),
                None => return Ok(Vec::new()),
            }
        };

        let terms: Vec<String> = query
            .split_whitespace()
            .map(|term| term.to_lowercase())
            .collect();
        let store = self.chunks.lock().map_err(poisoned)?;
        let mut scored: Vec<(usize, SearchHit)> = store
            .iter()
            .filter(|((tenant, product, release, _, _), _)| {
                tenant == &scope.tenant_id && product == &scope.product_id && release == &release_id
            })
            .filter_map(|(_, indexed)| {
                let haystack = format!(
                    "{} {}",
                    indexed.chunk.heading_path.to_lowercase(),
                    indexed.chunk.body.to_lowercase()
                );
                let score = terms
                    .iter()
                    .filter(|term| haystack.contains(term.as_str()))
                    .count();
                (score > 0).then(|| {
                    (
                        score,
                        SearchHit {
                            source_id: indexed.chunk.source_id.clone(),
                            chunk_ordinal: indexed.chunk.chunk_ordinal,
                            heading_path: indexed.chunk.heading_path.clone(),
                            body: indexed.chunk.body.clone(),
                            // 인메모리 어댑터는 문맥 복원을 하지 않는다. 테스트와
                            // loopback 개발용이며, 문맥 복원은 Postgres 스위트가 본다.
                            context_before: None,
                            context_after: None,
                            release_id: release_id.clone(),
                        },
                    )
                })
            })
            .collect();

        scored.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.source_id.cmp(&right.1.source_id))
                .then_with(|| left.1.chunk_ordinal.cmp(&right.1.chunk_ordinal))
        });
        Ok(scored
            .into_iter()
            .take(limit as usize)
            .map(|(_, hit)| hit)
            .collect())
    }

    async fn purge_scope(&self, scope: &TenantScope) -> Result<u64, KnowledgeIndexError> {
        if !scope.is_scaffold() {
            return Err(KnowledgeIndexError::ScopeNotPurgeable {
                tenant_id: scope.tenant_id.clone(),
            });
        }
        let removed = {
            let mut store = self.chunks.lock().map_err(poisoned)?;
            let before = store.len();
            store.retain(|(tenant, product, _, _, _), _| {
                tenant != &scope.tenant_id || product != &scope.product_id
            });
            before - store.len()
        };

        let mut active = self.active.lock().map_err(poisoned)?;
        active.remove(&(scope.tenant_id.clone(), scope.product_id.clone()));
        Ok(removed as u64)
    }
}

fn poisoned<T>(_: T) -> KnowledgeIndexError {
    KnowledgeIndexError::StoreUnavailable {
        message: "in-memory index lock was poisoned".to_string(),
    }
}
