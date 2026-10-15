use std::path::Path;

use anyhow::Context;
use lakehouse_application::{
    parse_building_register_floor_source_row_from_hub_bulk_text_line,
    BuildingRegisterFloorSourceRow,
};

pub(super) struct HubBuildingRegisterFloorBulkDecoder;

impl HubBuildingRegisterFloorBulkDecoder {
    pub(crate) fn decode_zip_rows(
        object_path: &Path,
        bronze_object_key: &str,
        max_rows: Option<usize>,
        mut on_row: impl FnMut(BuildingRegisterFloorSourceRow) -> anyhow::Result<()>,
    ) -> anyhow::Result<usize> {
        let mut decoded_count = 0usize;
        crate::building_register_zip_lines::decode_zip_lines(
            object_path,
            max_rows,
            |line, one_based_line_number| {
                let source_row = parse_building_register_floor_source_row_from_hub_bulk_text_line(
                    line,
                    bronze_object_key,
                    one_based_line_number,
                )
                .with_context(|| {
                    format!("failed to parse HUB floor line {one_based_line_number}")
                })?;
                on_row(source_row)?;
                decoded_count = decoded_count
                    .checked_add(1)
                    .context("HUB floor decoded row count overflow")?;
                Ok(())
            },
        )?;
        Ok(decoded_count)
    }
}
