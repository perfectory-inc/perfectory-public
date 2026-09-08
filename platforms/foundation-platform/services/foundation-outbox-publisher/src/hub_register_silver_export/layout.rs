use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub(super) struct Column {
    pub(super) index: usize,
}

#[derive(Clone, Deserialize)]
pub(super) struct SourceObject {
    pub(super) object_key: String,
    pub(super) bytes: u64,
    pub(super) dataset_name: String,
    pub(super) vintage: String,
}

#[derive(Clone, Deserialize)]
pub struct Layout {
    #[serde(skip)]
    expected_columns: BTreeSet<String>,
    schema_version: u32,
    pub(super) source: String,
    pub(super) selected_vintage: String,
    pub(super) inner_file: String,
    csv_delimiter: String,
    encoding: String,
    has_header: bool,
    pub(super) column_count: usize,
    pub(super) columns: BTreeMap<String, Column>,
    pub(super) handoff_suffix: String,
    pub(super) rows_per_part: u64,
    pub(super) max_row_bytes: u64,
    pub(super) objects: Vec<SourceObject>,
}

impl Layout {
    pub(crate) fn parse(
        source: &str,
        table: &lakehouse_domain::LakehouseTableContract,
    ) -> anyhow::Result<Self> {
        let mut layout: Self = serde_json::from_str(source)?;
        layout.expected_columns = table
            .columns
            .iter()
            .filter(|column| !super::GENERATED_COLUMNS.contains(&column.name))
            .map(|column| column.name.to_owned())
            .collect();
        layout.validate()?;
        Ok(layout)
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported HUB source contract version"
        );
        ensure!(
            self.encoding == "utf-8" && self.csv_delimiter == "|" && !self.has_header,
            "unsupported HUB text framing"
        );
        ensure!(
            self.column_count > 0 && self.column_count <= 256,
            "invalid column_count"
        );
        ensure!(
            self.rows_per_part > 0 && self.max_row_bytes > 0 && self.max_row_bytes <= 1024 * 1024,
            "invalid streaming bounds"
        );
        ensure!(
            self.handoff_suffix == ".jsonl.gz",
            "unsupported handoff suffix"
        );
        ensure!(
            self.columns.keys().cloned().collect::<BTreeSet<_>>() == self.expected_columns,
            "consumed column names differ from the Silver table contract"
        );
        ensure!(
            [
                "mgmt_key",
                "sigungu_cd",
                "bjdong_cd",
                "san_gubun",
                "bonbeon",
                "bubeon"
            ]
            .iter()
            .all(|name| self.columns.contains_key(*name))
                && self
                    .columns
                    .keys()
                    .all(|name| !super::GENERATED_COLUMNS.contains(&name.as_str())),
            "HUB columns must contain the PNU inputs and must not shadow generated columns"
        );
        let positions: BTreeSet<_> = self.columns.values().map(|c| c.index).collect();
        ensure!(
            positions.len() == self.columns.len()
                && positions.iter().all(|i| *i < self.column_count),
            "duplicate or out-of-range HUB column index"
        );
        ensure!(!self.objects.is_empty(), "source objects are empty");
        ensure!(
            self.objects.iter().map(|o| &o.vintage).max() == Some(&self.selected_vintage),
            "selected_vintage is not the latest measured vintage"
        );
        let keys: BTreeSet<_> = self.objects.iter().map(|o| &o.object_key).collect();
        let vintages: BTreeSet<_> = self.objects.iter().map(|o| &o.vintage).collect();
        ensure!(
            keys.len() == self.objects.len() && vintages.len() == self.objects.len(),
            "national source objects/vintages must be unique"
        );
        for object in &self.objects {
            ensure!(
                object.bytes > 0
                    && object.dataset_name == self.inner_file
                    && object.object_key.starts_with(&format!("{}/", self.source))
                    && object.object_key.strip_suffix(".zip").is_some()
                    && object.vintage.len() == 6
                    && object.vintage.bytes().all(|b| b.is_ascii_digit()),
                "invalid measured HUB source object"
            );
        }
        Ok(())
    }

    pub(super) fn selected(&self) -> anyhow::Result<&SourceObject> {
        self.objects
            .iter()
            .find(|o| o.vintage == self.selected_vintage)
            .context("selected national object is missing")
    }

    pub(super) fn value<'a>(&self, fields: &[&'a str], name: &str) -> anyhow::Result<&'a str> {
        let index = self
            .columns
            .get(name)
            .context("missing consumed HUB column")?
            .index;
        fields
            .get(index)
            .copied()
            .context("HUB column index outside row")
    }
}
