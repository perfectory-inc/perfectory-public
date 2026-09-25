use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use knowledge_application::{
    IndexedChunk, KnowledgeIndexError, KnowledgeIndexPort, ReleaseRef, SearchHit, TenantScope,
};
use knowledge_domain::{cap_per_group, reciprocal_rank_fusion, RankedList, DEFAULT_RRF_SMOOTHING};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

use crate::korean_tokenizer::{build_query_text, tokenize_korean};

/// 융합에서 청크를 가리키는 식별자. 활성 release는 범위마다 하나뿐이므로
/// (`idx_ip_knowledge_release_single_active`) 이 쌍이 검색 범위 안에서 유일하다.
type ChunkKey = (String, i32);

/// 하나의 검색 신호. 각자 자기 순위 목록을 내고 질의 시점에 합쳐진다.
///
/// 신호를 한 칼럼에 섞으면 서로를 가린다 — 제목이 정확히 맞은 문서가 긴 본문에 희석되고,
/// 원문 일치와 형태소 일치를 구분할 수 없다. 순위를 매기는 자가 하나뿐이면 그 자가 틀렸을 때
/// 대안이 없다. 근거는 `docs/reference/knowledge-search-industry-cases.md`.
struct RetrieverSpec {
    /// 진단용 이름. 융합 결과에 어느 리트리버가 올렸는지가 남는다.
    name: &'static str,
    /// 이 신호가 읽는 tsvector 칼럼. **컴파일 시점 상수이며 사용자 입력이 아니다** —
    /// 아래 `format!`이 안전한 이유가 이것이다.
    column: &'static str,
    /// 질의를 형태소로 쪼개서 넣을지, 원문 그대로 넣을지.
    morpheme_query: bool,
}

/// 지금의 신호 세 가지. 벡터 리트리버는 여기에 **한 줄 더하는 것**이 된다 —
/// 융합·수화(hydrate)·포트는 바뀌지 않는다. 그것이 이 구조의 목적이다.
const RETRIEVERS: &[RetrieverSpec] = &[
    RetrieverSpec {
        name: "morpheme",
        column: "search_vector_morph",
        morpheme_query: true,
    },
    RetrieverSpec {
        name: "raw",
        column: "search_vector_raw",
        morpheme_query: false,
    },
    RetrieverSpec {
        name: "heading",
        column: "search_vector_heading",
        morpheme_query: false,
    },
];

/// 리트리버마다 최종 개수의 몇 배까지 후보를 가져오는가.
///
/// 융합은 순위를 보고 합의를 찾는 것이므로 후보가 얕으면 합의할 거리가 없다. 조사한
/// 사례들도 융합 뒤 상위 20을 만들고 재순위로 10을 남긴다 — 넉넉히 가져와서 줄이는 쪽이다.
const CANDIDATE_DEPTH_FACTOR: u32 = 4;

/// 후보 깊이의 하한. `limit`이 1이어도 합의를 볼 수 있을 만큼은 가져온다.
const MIN_CANDIDATE_DEPTH: u32 = 20;

/// 한 원천 문서가 결과에 낼 수 있는 조각 수의 상한.
///
/// 순위는 조각 하나하나가 얼마나 맞는지만 본다. 결과 묶음 전체가 쓸모 있는지는 보지 않는다.
/// 그래서 긴 고시 하나가 여러 절에서 걸리면 그 문서만으로 결과가 다 찬다.
///
/// 3인 이유: 1이면 실제로 관련 있는 다른 절을 버리게 되고, 크면 상한의 의미가 없다.
/// 3이면 `limit`이 10일 때 서로 다른 문서가 최소 4개는 보인다. **측정 근거는 없다** —
/// 평가 세트가 생기면 그 자료로 정한다.
const MAX_CHUNKS_PER_SOURCE: usize = 3;

#[derive(Debug, thiserror::Error)]
pub enum PostgresKnowledgeIndexError {
    #[error("postgres knowledge index config is invalid")]
    InvalidConfig,
    #[error("postgres knowledge index failed: {message}")]
    StoreFailed { message: String },
}

impl PostgresKnowledgeIndexError {
    pub fn safe_message(&self) -> &'static str {
        match self {
            Self::InvalidConfig => "postgres knowledge index config is invalid",
            Self::StoreFailed { .. } => "postgres knowledge index failed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct PostgresKnowledgeIndexConfig {
    database_url: String,
    timeout_seconds: u64,
    max_connections: u32,
}

impl PostgresKnowledgeIndexConfig {
    pub fn new(
        database_url: impl Into<String>,
        timeout_seconds: u64,
    ) -> Result<Self, PostgresKnowledgeIndexError> {
        let database_url = database_url.into();
        if database_url.trim().is_empty() || timeout_seconds == 0 {
            return Err(PostgresKnowledgeIndexError::InvalidConfig);
        }
        Ok(Self {
            database_url,
            timeout_seconds,
            max_connections: 10,
        })
    }

    pub fn with_max_connections(
        mut self,
        max_connections: u32,
    ) -> Result<Self, PostgresKnowledgeIndexError> {
        if max_connections == 0 {
            return Err(PostgresKnowledgeIndexError::InvalidConfig);
        }
        self.max_connections = max_connections;
        Ok(self)
    }
}

pub struct PostgresKnowledgeIndex {
    pool: PgPool,
}

impl std::fmt::Debug for PostgresKnowledgeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresKnowledgeIndex")
            .field("pool", &"PgPool { .. }")
            .finish()
    }
}

impl PostgresKnowledgeIndex {
    pub async fn connect(
        config: PostgresKnowledgeIndexConfig,
    ) -> Result<Self, PostgresKnowledgeIndexError> {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_secs(config.timeout_seconds))
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await
            .map_err(|error| PostgresKnowledgeIndexError::StoreFailed {
                message: error.to_string(),
            })?;

        sqlx::migrate!("../../../migrations")
            .run(&pool)
            .await
            .map_err(|error| PostgresKnowledgeIndexError::StoreFailed {
                message: error.to_string(),
            })?;

        Ok(Self { pool })
    }

    /// 리트리버 하나를 돌려 순위 목록을 얻는다. 본문은 가져오지 않는다 — 융합이 끝난
    /// 뒤에 살아남은 것만 수화한다.
    async fn retrieve(
        &self,
        scope: &TenantScope,
        retriever: &RetrieverSpec,
        morph_query: &str,
        raw_query: &str,
        depth: u32,
    ) -> Result<Vec<ChunkKey>, KnowledgeIndexError> {
        let text = if retriever.morpheme_query {
            morph_query
        } else {
            raw_query
        };

        // `column`은 위 const 표의 `&'static str`이며 사용자 입력이 경유하지 않는다.
        // 질의어는 전부 바인드 파라미터다.
        let sql = format!(
            r#"
            SELECT c.source_id, c.chunk_ordinal
            FROM ip_knowledge_chunk c
            JOIN ip_knowledge_release r
              ON r.tenant_id = c.tenant_id
             AND r.product_id = c.product_id
             AND r.release_id = c.release_id
            WHERE c.tenant_id = $1
              AND c.product_id = $2
              AND r.is_active
              AND c.{column} @@ websearch_to_tsquery('simple', $3)
            ORDER BY
                ts_rank(c.{column}, websearch_to_tsquery('simple', $3)) DESC,
                c.source_id,
                c.chunk_ordinal
            LIMIT $4
            "#,
            column = retriever.column
        );

        let rows = sqlx::query(&sql)
            .bind(&scope.tenant_id)
            .bind(&scope.product_id)
            .bind(text)
            .bind(i64::from(depth))
            .fetch_all(&self.pool)
            .await
            .map_err(store_failed_from)?;

        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("source_id")
                        .map_err(store_failed_from)?,
                    row.try_get::<i32, _>("chunk_ordinal")
                        .map_err(store_failed_from)?,
                ))
            })
            .collect()
    }

    /// 융합이 고른 키를 본문과 **이웃 문맥**까지 읽고 **융합 순서 그대로** 돌려준다.
    /// SQL이 정한 순서가 아니라 융합이 정한 순서가 답이다.
    ///
    /// 이웃을 함께 읽는 이유: 청킹은 제목 아래 문단을 잘라 놓는다. 매치된 조각만 보여주면
    /// 그 앞 조각에 있던 전제나 정의, 뒤 조각에 있던 단서가 사라진 채 한 문단만 남는다.
    /// 조사한 사례도 같은 일을 한다 —
    /// *"we pull in the two neighboring sections so the heading, preconditions, and caveats
    /// that chunking split apart aren't lost."* (Cerebras)
    async fn hydrate(
        &self,
        scope: &TenantScope,
        keys: &[ChunkKey],
    ) -> Result<Vec<SearchHit>, KnowledgeIndexError> {
        // 매치된 조각과 그 앞뒤를 한 번에 읽는다. 결과당 질의를 따로 내면 N+1이 된다.
        let mut source_ids: Vec<String> = Vec::with_capacity(keys.len() * 3);
        let mut ordinals: Vec<i32> = Vec::with_capacity(keys.len() * 3);
        for (source_id, ordinal) in keys {
            for neighbour in [ordinal.saturating_sub(1), *ordinal, ordinal + 1] {
                source_ids.push(source_id.clone());
                ordinals.push(neighbour);
            }
        }

        let rows = sqlx::query(
            r#"
            SELECT
                c.source_id,
                c.chunk_ordinal,
                c.heading_path,
                c.body,
                c.release_id
            FROM ip_knowledge_chunk c
            JOIN ip_knowledge_release r
              ON r.tenant_id = c.tenant_id
             AND r.product_id = c.product_id
             AND r.release_id = c.release_id
            WHERE c.tenant_id = $1
              AND c.product_id = $2
              AND r.is_active
              AND (c.source_id, c.chunk_ordinal)
                  IN (SELECT * FROM unnest($3::text[], $4::int[]))
            "#,
        )
        .bind(&scope.tenant_id)
        .bind(&scope.product_id)
        .bind(&source_ids)
        .bind(&ordinals)
        .fetch_all(&self.pool)
        .await
        .map_err(store_failed_from)?;

        let mut bodies: BTreeMap<ChunkKey, ChunkRow> = BTreeMap::new();
        for row in &rows {
            let parsed = row_to_chunk(row)?;
            bodies.insert((parsed.source_id.clone(), parsed.chunk_ordinal), parsed);
        }

        Ok(keys
            .iter()
            .filter_map(|key| {
                let matched = bodies.get(key)?;
                Some(SearchHit {
                    source_id: matched.source_id.clone(),
                    chunk_ordinal: matched.chunk_ordinal,
                    heading_path: matched.heading_path.clone(),
                    body: matched.body.clone(),
                    context_before: matched
                        .chunk_ordinal
                        .checked_sub(1)
                        .and_then(|before| bodies.get(&(matched.source_id.clone(), before)))
                        .map(|row| row.body.clone()),
                    context_after: bodies
                        .get(&(matched.source_id.clone(), matched.chunk_ordinal + 1))
                        .map(|row| row.body.clone()),
                    release_id: matched.release_id.clone(),
                })
            })
            .collect())
    }
}

#[async_trait]
impl KnowledgeIndexPort for PostgresKnowledgeIndex {
    async fn index_chunks(
        &self,
        release: &ReleaseRef,
        chunks: Vec<IndexedChunk>,
    ) -> Result<u64, KnowledgeIndexError> {
        let mut tx = self.pool.begin().await.map_err(store_failed_from)?;

        sqlx::query(
            r#"
            INSERT INTO ip_knowledge_release (tenant_id, product_id, release_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (tenant_id, product_id, release_id) DO NOTHING
            "#,
        )
        .bind(&release.scope.tenant_id)
        .bind(&release.scope.product_id)
        .bind(&release.release_id)
        .execute(&mut *tx)
        .await
        .map_err(store_failed_from)?;

        let mut written = 0_u64;
        for indexed in &chunks {
            sqlx::query(
                r#"
                INSERT INTO ip_knowledge_chunk (
                    tenant_id, product_id, release_id, source_id, chunk_ordinal,
                    heading_path, body, search_text_morph, search_text_raw,
                    source_snapshot_id, content_checksum_sha256
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (tenant_id, product_id, release_id, source_id, chunk_ordinal)
                DO UPDATE SET
                    heading_path = EXCLUDED.heading_path,
                    body = EXCLUDED.body,
                    search_text_morph = EXCLUDED.search_text_morph,
                    search_text_raw = EXCLUDED.search_text_raw,
                    source_snapshot_id = EXCLUDED.source_snapshot_id,
                    content_checksum_sha256 = EXCLUDED.content_checksum_sha256
                "#,
            )
            .bind(&release.scope.tenant_id)
            .bind(&release.scope.product_id)
            .bind(&release.release_id)
            .bind(&indexed.chunk.source_id)
            .bind(indexed.chunk.chunk_ordinal)
            .bind(&indexed.chunk.heading_path)
            .bind(&indexed.chunk.body)
            // 두 신호를 **각자의 칼럼에** 넣는다. 한 칸에 섞으면 서로를 가린다.
            .bind(tokenize_korean(&searchable_text(indexed)))
            .bind(searchable_text(indexed))
            .bind(&indexed.source_snapshot_id)
            .bind(&indexed.chunk.content_checksum_sha256)
            .execute(&mut *tx)
            .await
            .map_err(store_failed_from)?;
            written += 1;
        }

        tx.commit().await.map_err(store_failed_from)?;
        Ok(written)
    }

    async fn activate_release(&self, release: &ReleaseRef) -> Result<(), KnowledgeIndexError> {
        let mut tx = self.pool.begin().await.map_err(store_failed_from)?;

        sqlx::query(
            r#"
            UPDATE ip_knowledge_release
            SET is_active = false
            WHERE tenant_id = $1 AND product_id = $2 AND is_active
            "#,
        )
        .bind(&release.scope.tenant_id)
        .bind(&release.scope.product_id)
        .execute(&mut *tx)
        .await
        .map_err(store_failed_from)?;

        let updated = sqlx::query(
            r#"
            UPDATE ip_knowledge_release
            SET is_active = true, activated_at = now()
            WHERE tenant_id = $1 AND product_id = $2 AND release_id = $3
            "#,
        )
        .bind(&release.scope.tenant_id)
        .bind(&release.scope.product_id)
        .bind(&release.release_id)
        .execute(&mut *tx)
        .await
        .map_err(store_failed_from)?;

        if updated.rows_affected() == 0 {
            return Err(KnowledgeIndexError::InvalidRequest {
                message: format!("release {} does not exist", release.release_id),
            });
        }

        tx.commit().await.map_err(store_failed_from)?;
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

        // 질의도 색인과 **같은 함수**를 통과해야 한다. 한쪽만 형태소로 쪼개면
        // 토큰 경계가 어긋나 아무것도 맞지 않는다.
        let morph_query = build_query_text(query);

        let depth = (limit.saturating_mul(CANDIDATE_DEPTH_FACTOR)).max(MIN_CANDIDATE_DEPTH);

        // 리트리버마다 자기 순위 목록을 만든다. 하나의 자로 전부를 재지 않는 것이 요점이다 —
        // 신호를 한 칼럼에 섞으면 서로를 가린다.
        let mut lists = Vec::with_capacity(RETRIEVERS.len());
        for retriever in RETRIEVERS {
            let ids = self
                .retrieve(scope, retriever, &morph_query, query, depth)
                .await?;
            lists.push(RankedList {
                retriever: retriever.name,
                ids,
            });
        }

        let fused = reciprocal_rank_fusion(&lists, DEFAULT_RRF_SMOOTHING);

        // 상한을 **자르기 전에** 건다. 자른 뒤에 걸면 이미 한 출처가 자리를 다 차지한
        // 뒤라 다양성이 회복되지 않는다.
        let diverse = cap_per_group(fused, MAX_CHUNKS_PER_SOURCE, |item| item.id.0.clone());

        let wanted: Vec<ChunkKey> = diverse
            .into_iter()
            .take(limit as usize)
            .map(|item| item.id)
            .collect();
        if wanted.is_empty() {
            return Ok(Vec::new());
        }

        self.hydrate(scope, &wanted).await
    }

    async fn purge_scope(&self, scope: &TenantScope) -> Result<u64, KnowledgeIndexError> {
        if !scope.is_scaffold() {
            return Err(KnowledgeIndexError::ScopeNotPurgeable {
                tenant_id: scope.tenant_id.clone(),
            });
        }
        let mut tx = self.pool.begin().await.map_err(store_failed_from)?;

        let removed =
            sqlx::query("DELETE FROM ip_knowledge_chunk WHERE tenant_id = $1 AND product_id = $2")
                .bind(&scope.tenant_id)
                .bind(&scope.product_id)
                .execute(&mut *tx)
                .await
                .map_err(store_failed_from)?
                .rows_affected();

        sqlx::query("DELETE FROM ip_knowledge_release WHERE tenant_id = $1 AND product_id = $2")
            .bind(&scope.tenant_id)
            .bind(&scope.product_id)
            .execute(&mut *tx)
            .await
            .map_err(store_failed_from)?;

        tx.commit().await.map_err(store_failed_from)?;
        Ok(removed)
    }
}

/// 색인 대상 텍스트. 제목 경로와 본문을 잇는다 — 제목에 담긴 어휘도 본문 신호에 들어가야
/// 하기 때문이다. 제목 **전용** 신호는 별도 리트리버(`heading`)가 따로 본다.
fn searchable_text(indexed: &IndexedChunk) -> String {
    format!("{} {}", indexed.chunk.heading_path, indexed.chunk.body)
}

/// 읽어 온 조각 한 줄. 매치된 것과 이웃을 같은 모양으로 담는다 — 이웃도 결국 조각이다.
struct ChunkRow {
    source_id: String,
    chunk_ordinal: i32,
    heading_path: String,
    body: String,
    release_id: String,
}

fn row_to_chunk(row: &sqlx::postgres::PgRow) -> Result<ChunkRow, KnowledgeIndexError> {
    Ok(ChunkRow {
        source_id: row.try_get("source_id").map_err(store_failed_from)?,
        chunk_ordinal: row.try_get("chunk_ordinal").map_err(store_failed_from)?,
        heading_path: row.try_get("heading_path").map_err(store_failed_from)?,
        body: row.try_get("body").map_err(store_failed_from)?,
        release_id: row.try_get("release_id").map_err(store_failed_from)?,
    })
}

fn store_failed_from(error: sqlx::Error) -> KnowledgeIndexError {
    KnowledgeIndexError::StoreUnavailable {
        message: error.to_string(),
    }
}
