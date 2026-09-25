//! 비계 코퍼스를 색인하는 개발용 CLI.
//!
//! 이 바이너리는 `tenant:scaffold`에만 쓴다. 테넌트를 입력으로 받지 않으므로 정본
//! 범위로는 아예 쓸 수 없다 — 비계 자료가 정본으로 새는 것을 구조적으로 막는 자리다.

use knowledge_application::{IndexCorpus, ReleaseRef, TenantScope, SCAFFOLD_TENANT_ID};
use knowledge_infrastructure::{
    LocalMarkdownSource, PostgresKnowledgeIndex, PostgresKnowledgeIndexConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let root = std::env::var("KNOWLEDGE_SCAFFOLD_ROOT")
        .map_err(|_| "KNOWLEDGE_SCAFFOLD_ROOT must point at the markdown root")?;
    let product_id = std::env::var("KNOWLEDGE_SCAFFOLD_PRODUCT_ID")
        .unwrap_or_else(|_| "monorepo-docs".to_string());
    let release_id = std::env::var("KNOWLEDGE_SCAFFOLD_RELEASE_ID")
        .map_err(|_| "KNOWLEDGE_SCAFFOLD_RELEASE_ID must be set explicitly")?;
    let database_url = std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL must be set")?;

    let scope = TenantScope::new(SCAFFOLD_TENANT_ID, product_id);
    let release = ReleaseRef::new(scope, release_id);

    let reader = LocalMarkdownSource::new(&root)?;
    let index =
        PostgresKnowledgeIndex::connect(PostgresKnowledgeIndexConfig::new(database_url, 10)?)
            .await?;

    let report = IndexCorpus {
        reader: &reader,
        index: &index,
    }
    .execute(&release)
    .await?;

    println!(
        "문서 {}개 읽음 → 조각 {}개 색인 → release {} 활성",
        report.documents, report.chunks, release.release_id
    );
    Ok(())
}
