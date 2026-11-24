//! A rehearsal of a candidate edition (root ADR-0618 §9).

use super::*;

/// A rehearsal (root ADR-0618 §9): a candidate edition built, checked and staged, and nothing
/// committed: no table created, no tag moved, no commit. Each 시도's built rows, every row of the
/// candidate (not its difference from the head), go to `<dir>/sido=<시도>.parquet`, and the summary
/// names each file with its rows and SHA-256, so the bake's gates read what it would serve from the
/// files alone. Promotion is the ordinary served build. It reads with the reader keys.
pub(super) struct Rehearsal {
    pub(super) edition: String,
    pub(super) dir: std::path::PathBuf,
}

impl Rehearsal {
    pub(super) fn from_env() -> anyhow::Result<Option<Self>> {
        match (
            optional_env_value(REHEARSAL_EDITION_ENV)?,
            optional_env_value(REHEARSAL_DIR_ENV)?,
        ) {
            (None, None) => Ok(None),
            (Some(edition), Some(dir)) => {
                ensure!(
                    !edition.is_empty()
                        && edition
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || b"_.:-".contains(&byte)),
                    "{edition:?} is not a snapshot id"
                );
                let dir = std::path::PathBuf::from(dir);
                ensure!(
                    !dir.exists(),
                    "the rehearsal directory {} already exists and is evidence",
                    dir.display()
                );
                std::fs::create_dir_all(&dir)
                    .with_context(|| format!("cannot create {}", dir.display()))?;
                Ok(Some(Self { edition, dir }))
            }
            _ => bail!("a rehearsal names both {REHEARSAL_EDITION_ENV} and {REHEARSAL_DIR_ENV}"),
        }
    }

    /// Writes the loaded 시도's built rows (`columns`, in PNU order) and returns what the summary
    /// records of the file.
    pub(super) fn write(
        &self,
        connection: &foundation_publisher_silver::duckdb::Connection,
        sido: &str,
        columns: &[&str],
    ) -> anyhow::Result<serde_json::Value> {
        let name = format!("sido={sido}.parquet");
        let path = self.dir.join(&name);
        let text = path
            .to_str()
            .context("the rehearsal directory must be UTF-8")?
            .replace('\\', "/");
        ensure!(
            !text.contains('\''),
            "the rehearsal directory must not contain a quote"
        );
        connection.execute_batch(&format!(
            "COPY (SELECT {} FROM built ORDER BY pnu) TO '{text}' (FORMAT parquet, COMPRESSION zstd);",
            columns.join(", ")
        ))?;
        let rows: i64 = connection.query_row("SELECT count(*) FROM built", [], |row| row.get(0))?;
        let mut digest = <sha2::Sha256 as sha2::Digest>::new();
        let mut file = std::fs::File::open(&path)
            .with_context(|| format!("cannot read {}", path.display()))?;
        let mut buffer = vec![0_u8; 1 << 20];
        loop {
            let read = std::io::Read::read(&mut file, &mut buffer)?;
            if read == 0 {
                break;
            }
            sha2::Digest::update(&mut digest, &buffer[..read]);
        }
        Ok(json!({
            "sido": sido, "path": name, "rows": rows,
            "sha256": format!("{:x}", sha2::Digest::finalize(digest)),
        }))
    }
}
