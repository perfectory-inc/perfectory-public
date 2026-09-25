// test code: panics are failures
#![allow(clippy::unwrap_used, clippy::expect_used)]

use knowledge_application::KnowledgeSourceReaderPort;
use knowledge_infrastructure::LocalMarkdownSource;

#[tokio::test]
async fn reads_markdown_files_recursively_with_stable_source_ids() {
    let root = tempdir();
    std::fs::create_dir_all(root.join("nested")).expect("mkdir");
    std::fs::write(root.join("a.md"), "# A\n\n본문 A\n").expect("write");
    std::fs::write(root.join("nested/b.md"), "# B\n\n본문 B\n").expect("write");
    std::fs::write(root.join("ignore.txt"), "not markdown").expect("write");

    let reader = LocalMarkdownSource::new(&root).expect("reader must build");
    let mut documents = reader.read_all().await.expect("read must succeed");
    documents.sort_by(|left, right| left.source_id.cmp(&right.source_id));

    assert_eq!(documents.len(), 2, ".md 파일만 읽는다");
    assert_eq!(documents[0].source_id, "a.md");
    assert_eq!(
        documents[1].source_id, "nested/b.md",
        "경로 구분자는 항상 /"
    );
    assert_eq!(documents[0].source_snapshot_id.len(), 64);

    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn snapshot_id_changes_when_the_file_changes() {
    let root = tempdir();
    std::fs::write(root.join("a.md"), "# A\n\n첫 내용\n").expect("write");
    let reader = LocalMarkdownSource::new(&root).expect("reader must build");
    let before = reader.read_all().await.expect("read")[0]
        .source_snapshot_id
        .clone();

    std::fs::write(root.join("a.md"), "# A\n\n바뀐 내용\n").expect("write");
    let after = reader.read_all().await.expect("read")[0]
        .source_snapshot_id
        .clone();

    assert_ne!(before, after);
    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn a_missing_root_is_rejected_at_construction() {
    let error = LocalMarkdownSource::new("definitely/not/here").expect_err("must reject");
    assert!(error.to_string().contains("not a directory"));
}

#[tokio::test]
async fn index_corpus_reads_chunks_and_activates_in_one_call() {
    use knowledge_application::{
        IndexCorpus, KnowledgeIndexPort, ReleaseRef, TenantScope, SCAFFOLD_TENANT_ID,
    };
    use knowledge_infrastructure::InMemoryKnowledgeIndex;

    let root = tempdir();
    std::fs::write(root.join("a.md"), "# 건폐율\n\n건폐율 완화 기준\n").expect("write");

    let reader = LocalMarkdownSource::new(&root).expect("reader must build");
    let index = InMemoryKnowledgeIndex::default();
    let scope = TenantScope::new(SCAFFOLD_TENANT_ID, "product-corpus");
    let release = ReleaseRef::new(scope.clone(), "release-1");

    let report = IndexCorpus {
        reader: &reader,
        index: &index,
    }
    .execute(&release)
    .await
    .expect("index corpus must succeed");

    assert_eq!(report.documents, 1);
    assert_eq!(report.chunks, 1);

    let hits = index
        .search(&scope, "건폐율", 5)
        .await
        .expect("search must succeed");
    assert_eq!(hits.len(), 1, "색인 직후 활성화되어 바로 검색된다");

    std::fs::remove_dir_all(&root).ok();
}

fn tempdir() -> std::path::PathBuf {
    let base = std::env::temp_dir().join(format!("ip-knowledge-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&base).expect("mkdir");
    base
}
