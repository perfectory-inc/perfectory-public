//! Loads 표제부 (building-register main) 지상/지하층수 counts into an in-memory map
//! keyed by `mgm_bldrgst_pk`, for use as the building-title witness during
//! floor contradiction resolution.

use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use anyhow::Context;
use foundation_normalization_domain::BuildingFloorCounts;
use lakehouse_application::parse_building_title_floor_counts_from_hub_bulk_text_line;
use zip::ZipArchive;

use super::hub_bulk_decoder::single_file_entry_index;

/// Builds a `mgm_bldrgst_pk -> BuildingFloorCounts` map from 표제부 Bronze zip objects.
///
/// The first row seen for a management key wins. Objects are UTF-8 pipe-delimited
/// hub bulk TXT inside a single-file zip, same as the floor bulk objects.
pub(crate) fn load_building_title_floor_counts(
    object_paths: &[PathBuf],
) -> anyhow::Result<HashMap<String, BuildingFloorCounts>> {
    let mut counts = HashMap::new();
    for object_path in object_paths {
        load_from_zip(object_path, &mut counts)?;
    }
    Ok(counts)
}

fn load_from_zip(
    object_path: &Path,
    counts: &mut HashMap<String, BuildingFloorCounts>,
) -> anyhow::Result<()> {
    let file = File::open(object_path)
        .with_context(|| format!("failed to open 표제부 Bronze zip {}", object_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("failed to read 표제부 Bronze zip {}", object_path.display()))?;
    let entry_index = single_file_entry_index(&mut archive)?;
    let entry = archive.by_index(entry_index).with_context(|| {
        format!(
            "failed to open 표제부 Bronze zip entry {} in {}",
            entry_index,
            object_path.display()
        )
    })?;
    let reader = BufReader::new(entry);
    for line_result in reader.lines() {
        let line = line_result.with_context(|| {
            format!(
                "failed to read 표제부 Bronze zip line from {}",
                object_path.display()
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some((mgm_bldrgst_pk, floor_counts)) =
            parse_building_title_floor_counts_from_hub_bulk_text_line(&line)
        {
            counts.entry(mgm_bldrgst_pk).or_insert(floor_counts);
        }
    }
    Ok(())
}
