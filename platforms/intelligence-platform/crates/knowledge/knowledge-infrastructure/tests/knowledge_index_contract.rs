// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use knowledge_application::{
    IndexedChunk, KnowledgeIndexError, KnowledgeIndexPort, ReleaseRef, TenantScope,
    SCAFFOLD_TENANT_ID,
};
use knowledge_domain::KnowledgeChunk;
use knowledge_infrastructure::InMemoryKnowledgeIndex;

#[tokio::test]
async fn memory_index_satisfies_the_index_contract() {
    knowledge_index_contract_suite(InMemoryKnowledgeIndex::default(), SCAFFOLD_TENANT_ID).await;
}

pub async fn knowledge_index_contract_suite<I>(index: I, tenant_id: &str)
where
    I: KnowledgeIndexPort,
{
    let scope = TenantScope::new(tenant_id, "product-1");
    let release_1 = ReleaseRef::new(scope.clone(), "release-1");

    let written = index
        .index_chunks(
            &release_1,
            vec![chunk("doc-1", 0, "건폐율", "건폐율 완화 기준")],
        )
        .await
        .expect("index must succeed");
    assert_eq!(written, 1);

    assert!(
        index
            .search(&scope, "건폐율", 5)
            .await
            .expect("search must succeed")
            .is_empty(),
        "활성화되지 않은 release는 검색되지 않는다"
    );

    index
        .activate_release(&release_1)
        .await
        .expect("activate must succeed");

    let hits = index
        .search(&scope, "건폐율", 5)
        .await
        .expect("search must succeed");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].source_id, "doc-1");
    assert_eq!(hits[0].release_id, "release-1");

    let reindexed = index
        .index_chunks(
            &release_1,
            vec![chunk("doc-1", 0, "건폐율", "건폐율 완화 기준")],
        )
        .await
        .expect("re-index must succeed");
    assert_eq!(reindexed, 1, "같은 release 재색인은 중복을 만들지 않는다");
    assert_eq!(
        index
            .search(&scope, "건폐율", 5)
            .await
            .expect("search")
            .len(),
        1
    );

    let release_2 = ReleaseRef::new(scope.clone(), "release-2");
    index
        .index_chunks(&release_2, vec![chunk("doc-2", 0, "용적률", "용적률 산정")])
        .await
        .expect("index must succeed");
    index
        .activate_release(&release_2)
        .await
        .expect("activate must succeed");

    assert!(
        index
            .search(&scope, "건폐율", 5)
            .await
            .expect("search")
            .is_empty(),
        "이전 release는 교체 후 검색되지 않는다"
    );
    assert_eq!(
        index
            .search(&scope, "용적률", 5)
            .await
            .expect("search")
            .len(),
        1
    );

    let other = TenantScope::new("tenant:production", "product-1");
    let denied = index
        .purge_scope(&other)
        .await
        .expect_err("정본 테넌트 삭제는 거부되어야 한다");
    assert!(matches!(
        denied,
        KnowledgeIndexError::ScopeNotPurgeable { .. }
    ));

    if scope.is_scaffold() {
        let purged = index.purge_scope(&scope).await.expect("purge must succeed");
        assert!(purged >= 2, "두 release의 청크가 모두 지워져야 한다");
        assert!(
            index
                .search(&scope, "용적률", 5)
                .await
                .expect("search")
                .is_empty(),
            "삭제 후에는 아무것도 남지 않는다"
        );
    }
}

pub fn chunk(source_id: &str, ordinal: i32, heading: &str, body: &str) -> IndexedChunk {
    IndexedChunk {
        chunk: KnowledgeChunk::new(source_id, ordinal, heading, body)
            .expect("fixture chunk must be valid"),
        source_snapshot_id: "3333333333333333333333333333333333333333333333333333333333333333"
            .to_string(),
    }
}
