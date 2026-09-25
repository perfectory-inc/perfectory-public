//! 비계 색인을 질의하는 개발용 CLI.
//!
//! HTTP route를 만들지 않는 이유는 Intelligence ADR-0002가 승인 전 production RAG
//! endpoint를 금지하기 때문이다. route가 없으면 실수로 노출될 수 없다.

use knowledge_application::{KnowledgeIndexPort, TenantScope, SCAFFOLD_TENANT_ID};
use knowledge_infrastructure::{PostgresKnowledgeIndex, PostgresKnowledgeIndexConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let query = std::env::args()
        .nth(1)
        .ok_or("usage: knowledge_scaffold_query <질의어>")?;
    let product_id = std::env::var("KNOWLEDGE_SCAFFOLD_PRODUCT_ID")
        .unwrap_or_else(|_| "monorepo-docs".to_string());
    let database_url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set")?;

    let scope = TenantScope::new(SCAFFOLD_TENANT_ID, product_id);
    let index =
        PostgresKnowledgeIndex::connect(PostgresKnowledgeIndexConfig::new(database_url, 10)?)
            .await?;

    let hits = index.search(&scope, &query, 10).await?;
    if hits.is_empty() {
        println!("결과 없음: {query}");
        return Ok(());
    }
    for (rank, hit) in hits.iter().enumerate() {
        println!(
            "{}. {} [{}] #{}",
            rank + 1,
            hit.source_id,
            hit.heading_path,
            hit.chunk_ordinal
        );
    }
    Ok(())
}
