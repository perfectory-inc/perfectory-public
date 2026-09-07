use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub(super) struct Column {
    pub index: usize,
}

#[derive(Clone, Deserialize)]
pub(super) struct SourceObject {
    pub object_key: String,
    pub bytes: u64,
    pub dataset_name: String,
    pub vintage: String,
}

#[derive(Clone, Deserialize)]
pub(super) struct Layout {
    schema_version: u32,
    pub source: String,
    pub selected_vintage: String,
    pub inner_file: String,
    csv_delimiter: String,
    encoding: String,
    has_header: bool,
    pub column_count: usize,
    pub columns: BTreeMap<String, Column>,
    pub handoff_suffix: String,
    pub rows_per_part: u64,
    pub max_row_bytes: u64,
    pub objects: Vec<SourceObject>,
}

impl Layout {
    pub fn embedded() -> anyhow::Result<Self> {
        let layout: Self = serde_json::from_str(include_str!(
            "../../../../infra/lakehouse/contracts/hub-building-register-apartment-price-source-objects.json"
        ))?;
        layout.validate()?;
        Ok(layout)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
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
        let required: BTreeSet<_> = [
            "mgmt_key",
            "sigungu_cd",
            "bjdong_cd",
            "san_gubun",
            "bonbeon",
            "bubeon",
            "base_date",
            "price_won",
            "notice_date",
        ]
        .into_iter()
        .collect();
        ensure!(
            self.columns
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                == required,
            "consumed column names differ from the HUB adapter"
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

    pub fn selected(&self) -> anyhow::Result<&SourceObject> {
        self.objects
            .iter()
            .find(|o| o.vintage == self.selected_vintage)
            .context("selected national object is missing")
    }

    pub fn value<'a>(&self, fields: &[&'a str], name: &str) -> anyhow::Result<&'a str> {
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
