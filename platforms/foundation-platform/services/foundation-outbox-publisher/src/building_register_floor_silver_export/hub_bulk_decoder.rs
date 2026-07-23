use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::{bail, Context};
use lakehouse_application::{
    parse_building_register_floor_source_row_from_hub_bulk_text_line,
    BuildingRegisterFloorSourceRow,
};
use zip::ZipArchive;

pub(crate) struct HubBuildingRegisterFloorBulkDecoder;

impl HubBuildingRegisterFloorBulkDecoder {
    pub(crate) fn decode_zip_rows(
        object_path: &Path,
        bronze_object_key: &str,
        max_rows: Option<usize>,
        mut on_row: impl FnMut(BuildingRegisterFloorSourceRow) -> anyhow::Result<()>,
    ) -> anyhow::Result<usize> {
        let file = File::open(object_path)
            .with_context(|| format!("failed to open HUB Bronze zip {}", object_path.display()))?;
        let mut archive = ZipArchive::new(file)
            .with_context(|| format!("failed to read HUB Bronze zip {}", object_path.display()))?;
        let entry_index = single_file_entry_index(&mut archive)?;
        let entry = archive.by_index(entry_index).with_context(|| {
            format!(
                "failed to open HUB Bronze zip entry {} in {}",
                entry_index,
                object_path.display()
            )
        })?;
        let reader = BufReader::new(entry);

        let mut decoded_count = 0usize;
        for (line_index, line_result) in reader.lines().enumerate() {
            if matches!(max_rows, Some(limit) if decoded_count >= limit) {
                break;
            }
            let one_based_line_number =
                u64::try_from(line_index + 1).context("HUB Bronze zip line number exceeded u64")?;
            let line = line_result.with_context(|| {
                format!(
                    "failed to read HUB Bronze zip line {} from {}",
                    one_based_line_number,
                    object_path.display()
                )
            })?;
            if line.trim().is_empty() {
                continue;
            }

            let source_row = parse_building_register_floor_source_row_from_hub_bulk_text_line(
                line.as_str(),
                bronze_object_key,
                one_based_line_number,
            )?;
            on_row(source_row)?;
            decoded_count += 1;
        }

        Ok(decoded_count)
    }
}

pub(crate) fn single_file_entry_index<R: std::io::Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
) -> anyhow::Result<usize> {
    let mut file_indexes = Vec::new();
    for index in 0..archive.len() {
        let entry = archive
            .by_index_raw(index)
            .with_context(|| format!("failed to inspect zip entry {index}"))?;
        if !entry.is_dir() {
            file_indexes.push(index);
        }
    }
    match file_indexes.as_slice() {
        [index] => Ok(*index),
        [] => bail!("HUB Bronze zip must contain one TXT file, found no file entries"),
        indexes => bail!(
            "HUB Bronze zip must contain one TXT file, found {} file entries",
            indexes.len()
        ),
    }
}
