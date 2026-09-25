use std::path::{Path, PathBuf};

use async_trait::async_trait;
use knowledge_application::{KnowledgeIndexError, KnowledgeSourceReaderPort, SourceDocument};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum LocalMarkdownSourceError {
    #[error("local markdown source root is not a directory: {path}")]
    NotADirectory { path: String },
}

/// 디렉터리 아래의 `.md` 파일을 읽는 원천 어댑터.
///
/// `source_id`는 root 기준 상대 경로이며 구분자는 항상 `/`다. 그래야 Windows와
/// Linux에서 같은 문서가 같은 식별자를 갖는다.
#[derive(Clone, Debug)]
pub struct LocalMarkdownSource {
    root: PathBuf,
}

impl LocalMarkdownSource {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, LocalMarkdownSourceError> {
        let root = root.as_ref().to_path_buf();
        if !root.is_dir() {
            return Err(LocalMarkdownSourceError::NotADirectory {
                path: root.display().to_string(),
            });
        }
        Ok(Self { root })
    }

    fn collect(&self, dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
        for entry in std::fs::read_dir(dir)? {
            let path = entry?.path();
            if path.is_dir() {
                self.collect(&path, out)?;
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("md") {
                out.push(path);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl KnowledgeSourceReaderPort for LocalMarkdownSource {
    async fn read_all(&self) -> Result<Vec<SourceDocument>, KnowledgeIndexError> {
        let mut paths = Vec::new();
        self.collect(&self.root, &mut paths).map_err(io_failed)?;

        let mut documents = Vec::with_capacity(paths.len());
        for path in paths {
            let bytes = std::fs::read(&path).map_err(io_failed)?;
            let text = String::from_utf8(bytes.clone()).map_err(|error| {
                KnowledgeIndexError::InvalidRequest {
                    message: format!("{} is not utf-8: {error}", path.display()),
                }
            })?;
            let relative = path.strip_prefix(&self.root).map_err(|error| {
                KnowledgeIndexError::InvalidRequest {
                    message: error.to_string(),
                }
            })?;
            let source_id = relative
                .components()
                .map(|component| component.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");

            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            documents.push(SourceDocument {
                source_id,
                text,
                source_snapshot_id: format!("{:x}", hasher.finalize()),
            });
        }
        Ok(documents)
    }
}

fn io_failed(error: std::io::Error) -> KnowledgeIndexError {
    KnowledgeIndexError::StoreUnavailable {
        message: error.to_string(),
    }
}
