use super::*;
use crate::building_register_snapshot::validate_object_name;
use lakehouse_application::{
    parse_building_register_unit_source_row_from_hub_bulk_text_line, BuildingRegisterBasisIndex,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Serialize)]
pub(super) struct SourceEvidence {
    role: &'static str,
    bronze_object_key: String,
    byte_size: u64,
    sha256: String,
}

pub(super) struct ParentInputs {
    pub unit_path: PathBuf,
    pub unit_key: String,
    pub titles: BuildingTitleKeyIndex,
    pub basis: BuildingRegisterBasisIndex,
    pub evidence: Vec<SourceEvidence>,
    paths: Vec<PathBuf>,
}

impl ParentInputs {
    pub fn load(config: &UnitExportConfig) -> anyhow::Result<Self> {
        let specs = [
            (
                "unit",
                "mart_djy_09.txt",
                config.source_slug.as_str(),
                config.source_object.as_deref(),
            ),
            (
                "title",
                "mart_djy_03.txt",
                config
                    .title_source_slug
                    .as_deref()
                    .context("title source is required for unit parent validation")?,
                config.title_source_object.as_deref(),
            ),
            (
                "basis",
                "mart_djy_01.txt",
                config.basis_source_slug.as_str(),
                config.basis_source_object.as_deref(),
            ),
        ];
        let mut paths = Vec::new();
        let mut evidence = Vec::new();
        for (role, expected_entry, slug, pin) in specs {
            if slug.is_empty()
                || !slug
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
            {
                bail!("{role} source slug must be a safe source identifier");
            }
            if let Some(name) = pin {
                validate_object_name(name, config.valid_from_utc.date_naive())?;
            }
            let path = locate_zip_object(&config.bronze_local_object_root, slug, pin, role)?;
            validate_zip_entry(&path, expected_entry)?;
            validate_object_name(
                path.file_name()
                    .and_then(|name| name.to_str())
                    .context("invalid Bronze object file name")?,
                config.valid_from_utc.date_naive(),
            )?;
            evidence.push(SourceEvidence {
                role,
                bronze_object_key: bronze_object_key(&config.bronze_local_object_root, &path)?,
                byte_size: fs::metadata(&path)?.len(),
                sha256: hash_file(&path)?,
            });
            paths.push(path);
        }
        let digest = format!("{:x}", Sha256::digest(serde_json::to_vec(&evidence)?));
        let mut titles = BuildingTitleKeyIndex::new();
        decode_zip_lines(&paths[1], None, |line, line_number| {
            let entry = parse_building_title_building_link_from_hub_bulk_text_line(line)
                .with_context(|| format!("invalid building title at line {line_number}"))?;
            titles.insert(entry);
            Ok(())
        })?;
        let mut basis = BuildingRegisterBasisIndex::new(&evidence[2].bronze_object_key, &digest)?;
        decode_zip_lines(&paths[2], None, |line, line_number| {
            basis.insert_hub_line(line, line_number)?;
            Ok(())
        })?;
        // Validate the complete source before any writer can create/truncate output.
        // Reuse the source parser; this pass retains no unit rows in memory.
        decode_zip_lines(&paths[0], None, |line, line_number| {
            parse_building_register_unit_source_row_from_hub_bulk_text_line(
                line,
                &evidence[0].bronze_object_key,
                line_number,
            )?;
            Ok(())
        })?;
        Ok(Self {
            unit_path: paths[0].clone(),
            unit_key: evidence[0].bronze_object_key.clone(),
            titles,
            basis,
            evidence,
            paths,
        })
    }

    pub fn verify_unchanged(&self) -> anyhow::Result<()> {
        for (path, source) in self.paths.iter().zip(&self.evidence) {
            if fs::metadata(path)?.len() != source.byte_size || hash_file(path)? != source.sha256 {
                bail!("{} Bronze input changed during unit export", source.role);
            }
        }
        Ok(())
    }
}

fn hash_file(path: &Path) -> anyhow::Result<String> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    std::io::copy(&mut reader, &mut digest)?;
    Ok(format!("{:x}", digest.finalize()))
}

fn validate_zip_entry(path: &Path, expected: &str) -> anyhow::Result<()> {
    let mut archive = ZipArchive::new(File::open(path)?)?;
    let entry_index = single_file_entry_index(&mut archive)?;
    let entry = archive.by_index_raw(entry_index)?;
    if entry.name() != expected {
        bail!(
            "{} must contain {expected}, found {}",
            path.display(),
            entry.name()
        );
    }
    Ok(())
}
