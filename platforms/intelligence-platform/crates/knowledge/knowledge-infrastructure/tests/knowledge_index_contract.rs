// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use knowledge_application::{
    IndexedChunk, KnowledgeIndexError, KnowledgeIndexPort, ReleaseRef, TenantScope,
    SCAFFOLD_TENANT_ID,
};
use knowledge_domain::KnowledgeChunk;
use knowledge_infrastructure::{
    InMemoryKnowledgeIndex, PostgresKnowledgeIndex, PostgresKnowledgeIndexConfig,
};

#[tokio::test]
async fn memory_index_satisfies_the_index_contract() {
    knowledge_index_contract_suite(InMemoryKnowledgeIndex::default(), SCAFFOLD_TENANT_ID).await;
}

/// 한국어는 조사가 명사에 붙어 한 토큰이 된다. 검색어 "건폐율"이 본문의 "건폐율을"과
/// 맞아야 검색이 쓸모 있다 — 고시문은 거의 모든 명사가 조사를 달고 나온다.
///
/// 이 테스트가 실패하면 형태소 분석이 없는 것이고, 그 상태로 고시 코퍼스를 넣으면
/// 관은 뚫렸는데 물이 안 흐른다.
#[tokio::test]
#[ignore = "requires the Intelligence Postgres integration lane"]
async fn postgres_index_matches_a_noun_that_carries_a_josa() {
    let index = pg_index().await;
    let scope = TenantScope::new(SCAFFOLD_TENANT_ID, "product-josa");
    index.purge_scope(&scope).await.expect("clean start");

    let release = ReleaseRef::new(scope.clone(), "release-josa");
    index
        .index_chunks(
            &release,
            vec![chunk(
                "notice-1",
                0,
                // 제목은 일부러 질의어를 하나도 담지 않는다. 담으면 본문 토큰화가
                // 실패해도 제목이 대신 맞아 테스트가 통과해 버린다 — 실제로 처음
                // 이렇게 써서 3개 중 2개가 거짓 통과했다.
                "고시 본문",
                "해당 지구단위계획구역에서는 건폐율을 100분의 80까지 완화하여 적용한다.",
            )],
        )
        .await
        .expect("index must succeed");
    index
        .activate_release(&release)
        .await
        .expect("activate must succeed");

    for (query, why) in [
        ("건폐율", "조사 '을'이 붙은 명사를 어간으로 찾아야 한다"),
        ("완화", "'완화하여'의 어간을 찾아야 한다"),
        (
            "지구단위계획구역",
            "조사 '에서는'이 붙은 복합명사를 찾아야 한다",
        ),
    ] {
        let hits = index
            .search(&scope, query, 5)
            .await
            .expect("search must succeed");
        assert_eq!(hits.len(), 1, "질의 '{query}': {why}");
    }

    index.purge_scope(&scope).await.expect("cleanup");
}

#[tokio::test]
#[ignore = "requires the Intelligence Postgres integration lane"]
async fn postgres_index_satisfies_the_index_contract() {
    let index = pg_index().await;
    index
        .purge_scope(&TenantScope::new(SCAFFOLD_TENANT_ID, "product-1"))
        .await
        .expect("clean start");
    knowledge_index_contract_suite(index, SCAFFOLD_TENANT_ID).await;
}

#[tokio::test]
#[ignore = "requires the Intelligence Postgres integration lane"]
async fn postgres_index_ranks_the_denser_chunk_first() {
    let index = pg_index().await;
    let scope = TenantScope::new(SCAFFOLD_TENANT_ID, "product-rank");
    index.purge_scope(&scope).await.expect("clean start");

    let release = ReleaseRef::new(scope.clone(), "release-rank");
    index
        .index_chunks(
            &release,
            vec![
                chunk("doc-sparse", 0, "기타", "건폐율 한 번만 나온다"),
                chunk("doc-dense", 0, "건폐율", "건폐율 건폐율 건폐율 기준"),
            ],
        )
        .await
        .expect("index must succeed");
    index
        .activate_release(&release)
        .await
        .expect("activate must succeed");

    let hits = index
        .search(&scope, "건폐율", 5)
        .await
        .expect("search must succeed");
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].source_id, "doc-dense", "밀도가 높은 청크가 앞선다");

    index.purge_scope(&scope).await.expect("cleanup");
}

/// 융합이 실제로 순위를 바꾸는지 증명한다. **리트리버를 하나로 줄이면 반드시 실패해야 하는**
/// 데이터로 짰다 — 처음 쓴 판본은 한 개짜리로도 통과해서 아무것도 증명하지 못했다.
///
/// | 문서 | 형태소 | 원문 | 제목 |
/// |---|---|---|---|
/// | `josa-heavy` | **1등** (`건폐율`이 3회) | ✗ 원문은 `건폐율을`이라 질의 `건폐율`과 다른 토큰 | ✗ 제목이 `부칙` |
/// | `consensus`  | 2등 (1회) | ✓ | ✓ |
///
/// 형태소 하나만 쓰면 `josa-heavy`가 이긴다. 셋을 합치면 `consensus`가 이긴다.
#[tokio::test]
#[ignore = "requires the Intelligence Postgres integration lane"]
async fn postgres_index_lets_consensus_across_signals_win() {
    let index = pg_index().await;
    let scope = TenantScope::new(SCAFFOLD_TENANT_ID, "product-fusion");
    index.purge_scope(&scope).await.expect("clean start");

    let release = ReleaseRef::new(scope.clone(), "release-fusion");
    index
        .index_chunks(
            &release,
            vec![
                chunk(
                    "josa-heavy",
                    0,
                    "부칙",
                    "건폐율을 건폐율을 건폐율을 준용하여 적용한다.",
                ),
                chunk("consensus", 0, "건폐율", "완화 기준은 별표와 같다."),
            ],
        )
        .await
        .expect("index must succeed");
    index
        .activate_release(&release)
        .await
        .expect("activate must succeed");

    let hits = index
        .search(&scope, "건폐율", 5)
        .await
        .expect("search must succeed");

    assert_eq!(hits.len(), 2, "두 문서 모두 잡혀야 한다: {hits:?}");
    assert_eq!(
        hits[0].source_id, "consensus",
        "세 신호의 합의가 형태소 신호 단독 1등을 이겨야 한다. \
         이 단정이 리트리버 1개로도 통과한다면 융합이 일하지 않는 것이다: {hits:?}"
    );

    index.purge_scope(&scope).await.expect("cleanup");
}

/// 한 원천이 결과를 독식하지 못한다. 상한을 없애면 실패한다.
///
/// `long-notice`는 다섯 절이 모두 질의어를 담고 있어 순위만으로는 상위 5칸을 다 차지한다.
/// `short-notice`는 한 절뿐이라 밀려난다. 사용자는 "건폐율"을 물었는데 같은 고시의 다섯
/// 조각만 보게 된다 — 순위는 조각 하나하나만 보지 결과 묶음 전체를 보지 않기 때문이다.
#[tokio::test]
#[ignore = "requires the Intelligence Postgres integration lane"]
async fn postgres_index_caps_how_much_one_source_can_fill() {
    let index = pg_index().await;
    let scope = TenantScope::new(SCAFFOLD_TENANT_ID, "product-diversity");
    index.purge_scope(&scope).await.expect("clean start");

    let release = ReleaseRef::new(scope.clone(), "release-diversity");
    let mut chunks = Vec::new();
    for ordinal in 0..5 {
        chunks.push(chunk(
            "long-notice",
            ordinal,
            "건폐율",
            "건폐율을 완화하여 적용한다.",
        ));
    }
    chunks.push(chunk("short-notice", 0, "건폐율", "건폐율을 준용한다."));

    index
        .index_chunks(&release, chunks)
        .await
        .expect("index must succeed");
    index
        .activate_release(&release)
        .await
        .expect("activate must succeed");

    let hits = index
        .search(&scope, "건폐율", 5)
        .await
        .expect("search must succeed");

    let from_long = hits
        .iter()
        .filter(|hit| hit.source_id == "long-notice")
        .count();
    assert!(
        from_long <= 3,
        "한 원천이 상한을 넘어 결과를 차지했다: {hits:?}"
    );
    assert!(
        hits.iter().any(|hit| hit.source_id == "short-notice"),
        "상한이 없으면 짧은 고시가 밀려난다. 다른 원천이 보여야 한다: {hits:?}"
    );

    index.purge_scope(&scope).await.expect("cleanup");
}

/// 매치된 조각의 앞뒤 문맥이 함께 온다. 청킹이 잘라 버린 전제와 단서를 되돌리는 자리다.
#[tokio::test]
#[ignore = "requires the Intelligence Postgres integration lane"]
async fn postgres_index_returns_neighbouring_context() {
    let index = pg_index().await;
    let scope = TenantScope::new(SCAFFOLD_TENANT_ID, "product-context");
    index.purge_scope(&scope).await.expect("clean start");

    let release = ReleaseRef::new(scope.clone(), "release-context");
    index
        .index_chunks(
            &release,
            vec![
                chunk("notice", 0, "제1조", "이 고시는 다음 각 호에 적용한다."),
                chunk("notice", 1, "제2조", "건폐율을 완화하여 적용한다."),
                chunk("notice", 2, "제3조", "다만 보전녹지지역은 제외한다."),
            ],
        )
        .await
        .expect("index must succeed");
    index
        .activate_release(&release)
        .await
        .expect("activate must succeed");

    let hits = index
        .search(&scope, "건폐율", 5)
        .await
        .expect("search must succeed");

    let matched = hits
        .iter()
        .find(|hit| hit.chunk_ordinal == 1)
        .expect("가운데 조각이 잡혀야 한다");

    assert_eq!(
        matched.context_before.as_deref(),
        Some("이 고시는 다음 각 호에 적용한다."),
        "앞 조각의 전제가 함께 와야 한다: {matched:?}"
    );
    assert_eq!(
        matched.context_after.as_deref(),
        Some("다만 보전녹지지역은 제외한다."),
        "뒤 조각의 단서가 함께 와야 한다: {matched:?}"
    );
    assert!(
        !matched.body.contains("보전녹지지역"),
        "문맥을 본문에 섞으면 무엇이 맞았는지 알 수 없다: {matched:?}"
    );

    index.purge_scope(&scope).await.expect("cleanup");
}

/// `INTELLIGENCE_TEST_DATABASE_URL`이 없으면 실패한다. 조용한 skip을 만들지 않는다 —
/// `scripts/guard/no-silent-test-skip.sh`가 강제하는 규칙이고, 등록부 계약 테스트가
/// 같은 이유로 이미 이 형태다.
async fn pg_index() -> PostgresKnowledgeIndex {
    let url = std::env::var("INTELLIGENCE_TEST_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())
        .expect("INTELLIGENCE_TEST_DATABASE_URL must be set and non-empty for the Postgres lane");

    let config = PostgresKnowledgeIndexConfig::new(url, 10)
        .expect("INTELLIGENCE_TEST_DATABASE_URL produced an invalid config");
    PostgresKnowledgeIndex::connect(config)
        .await
        .expect("failed to connect to test database")
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
