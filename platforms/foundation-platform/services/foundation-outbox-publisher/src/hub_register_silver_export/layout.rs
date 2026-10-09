use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub(super) struct Column {
    pub(super) index: usize,
}

/// The file layout of one hub national register (root ADR-0092). Which ZIP to read is not here:
/// the Silver refresh picks it from the Bronze ledger and passes it in (root ADR-0169 §2).
#[derive(Clone, Deserialize)]
pub struct Layout {
    #[serde(skip)]
    expected_columns: BTreeSet<String>,
    schema_version: u32,
    pub(super) source: String,
    pub(super) inner_file: String,
    csv_delimiter: String,
    encoding: String,
    has_header: bool,
    pub(super) column_count: usize,
    pub(super) columns: BTreeMap<String, Column>,
    pub(super) handoff_suffix: String,
    pub(super) rows_per_part: u64,
    pub(super) max_row_bytes: u64,
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
        ensure!(
            self.source.starts_with("bronze/source=hubgokr__") && !self.source.ends_with('/'),
            "source must be a hub.go.kr Bronze prefix"
        );
        Ok(())
    }

    /// The `vintage` (`YYYYMM`) of one of this source's monthly ZIPs: the provider month its
    /// `OPN<YYYYMMDD>` name carries, the month the ledger files it under.
    ///
    /// # Errors
    /// Refuses a key outside this source or a name without a valid provider date.
    pub fn vintage_of(&self, object_key: &str) -> anyhow::Result<String> {
        let name = object_key
            .strip_prefix(&format!("{}/", self.source))
            .filter(|name| !name.contains('/'))
            .with_context(|| format!("{object_key} is not an object of {}", self.source))?;
        let day = crate::building_register_snapshot::object_date(name)?;
        Ok(day.format("%Y%m").to_string())
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
