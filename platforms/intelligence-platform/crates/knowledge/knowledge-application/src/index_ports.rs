use std::fmt;

use async_trait::async_trait;
use knowledge_domain::KnowledgeChunk;

/// 비계(scaffold) 전용 테넌트.
///
/// 이 테넌트의 자료는 정본이 아니라 배관 시험용이며 통째로 삭제할 수 있다.
/// `purge_scope`는 이 테넌트에서만 허용된다 — 정본 테넌트의 자료는 삭제하지 않고
/// release 비활성화로만 물러난다.
pub const SCAFFOLD_TENANT_ID: &str = "tenant:scaffold";

/// 색인 범위. 한 테넌트의 한 제품.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantScope {
    pub tenant_id: String,
    pub product_id: String,
}

impl TenantScope {
    pub fn new(tenant_id: impl Into<String>, product_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            product_id: product_id.into(),
        }
    }

    pub fn is_scaffold(&self) -> bool {
        self.tenant_id == SCAFFOLD_TENANT_ID
    }
}

/// 색인 1회분. 교체 단위이며 Intelligence ADR-0002의 `release_id`에 해당한다.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseRef {
    pub scope: TenantScope,
    pub release_id: String,
}

impl ReleaseRef {
    pub fn new(scope: TenantScope, release_id: impl Into<String>) -> Self {
        Self {
            scope,
            release_id: release_id.into(),
        }
    }
}

/// 색인에 넣는 한 건. 원천 식별자와 스냅숏 식별자를 청크와 함께 보존한다.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedChunk {
    pub chunk: KnowledgeChunk,
    /// 원본 파일 전체의 SHA-256. ADR-0002의 `source_snapshot_id`에 해당한다.
    pub source_snapshot_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchHit {
    pub source_id: String,
    pub chunk_ordinal: i32,
    pub heading_path: String,
    pub body: String,
    pub release_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KnowledgeIndexError {
    InvalidRequest { message: String },
    ScopeNotPurgeable { tenant_id: String },
    StoreUnavailable { message: String },
}

impl KnowledgeIndexError {
    pub const fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidRequest { .. } => "knowledge index request is invalid",
            Self::ScopeNotPurgeable { .. } => "knowledge index scope is not purgeable",
            Self::StoreUnavailable { .. } => "knowledge index store failed",
        }
    }
}

impl fmt::Display for KnowledgeIndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { message } => {
                write!(formatter, "knowledge index request is invalid: {message}")
            }
            Self::ScopeNotPurgeable { tenant_id } => write!(
                formatter,
                "knowledge index scope is not purgeable: {tenant_id} is not the scaffold tenant"
            ),
            Self::StoreUnavailable { message } => {
                write!(formatter, "knowledge index store failed: {message}")
            }
        }
    }
}

impl std::error::Error for KnowledgeIndexError {}

/// 색인기 경계. 구현을 바꿔도 이 위의 코드는 바뀌지 않는다.
///
/// 지금의 유일한 실구현은 Postgres 전문검색이다. 임베딩·벡터 인덱스 어댑터는
/// Intelligence ADR-0002가 요구하는 별도 ADR이 승인된 뒤에만 추가한다.
#[async_trait]
pub trait KnowledgeIndexPort: Send + Sync {
    /// 청크를 특정 release에 넣는다. 같은 release에 두 번 넣어도 결과가 같아야 한다.
    async fn index_chunks(
        &self,
        release: &ReleaseRef,
        chunks: Vec<IndexedChunk>,
    ) -> Result<u64, KnowledgeIndexError>;

    /// 이 release를 활성으로 만들고 같은 범위의 다른 release를 비활성으로 만든다.
    async fn activate_release(&self, release: &ReleaseRef) -> Result<(), KnowledgeIndexError>;

    /// 활성 release만 검색한다.
    async fn search(
        &self,
        scope: &TenantScope,
        query: &str,
        limit: u32,
    ) -> Result<Vec<SearchHit>, KnowledgeIndexError>;

    /// 비계 테넌트의 자료를 흔적 없이 지운다. 다른 테넌트에서는 실패한다.
    async fn purge_scope(&self, scope: &TenantScope) -> Result<u64, KnowledgeIndexError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_scaffold_tenant_is_scaffold() {
        assert!(TenantScope::new(SCAFFOLD_TENANT_ID, "product-1").is_scaffold());
        assert!(!TenantScope::new("tenant:production", "product-1").is_scaffold());
        assert!(
            !TenantScope::new("tenant:scaffold-2", "product-1").is_scaffold(),
            "접두사가 같다고 비계가 되지 않는다"
        );
    }

    #[test]
    fn scaffold_tenant_id_is_the_documented_literal() {
        // 이 값이 바뀌면 이미 색인된 비계 자료가 고아가 되고 purge가 그것을 못 지운다.
        assert_eq!(SCAFFOLD_TENANT_ID, "tenant:scaffold");
    }
}
