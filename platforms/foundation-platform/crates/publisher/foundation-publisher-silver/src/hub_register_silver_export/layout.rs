use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context};
use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub(super) struct Column {
    pub(super) index: usize,
}

/// The slot of `inner_file` a monthly member name fills with its `YYYYMM`.
const MONTH_SLOT: &str = "{YYYYMM}";

/// Where a row's `mgmt_key` comes from when the provider's key column is empty on every row.
#[derive(Clone, Deserialize, Eq, PartialEq)]
pub(super) enum RowKey {
    /// The lowercase hex SHA-256 of the row's text: the row is its own identity, so a changed row
    /// is a new key and the old one a removal (root ADR-0397).
    #[serde(rename = "sha256_of_row")]
    Sha256OfRow,
    /// These consumed columns' trimmed values joined by `|`, in order: a key the provider spells
    /// across columns (월별 에너지: 사용 월 and 지번, root ADR-0396). The first column is what
    /// the compare buckets by.
    #[serde(rename = "joined_columns")]
    JoinedColumns(Vec<String>),
}

/// The consumed columns one PNU is composed from: the five 지번 parts of a row.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PnuInputs {
    pub(super) sigungu_cd: String,
    pub(super) bjdong_cd: String,
    pub(super) san_gubun: String,
    pub(super) bonbeon: String,
    pub(super) bubeon: String,
}

impl PnuInputs {
    pub(super) fn names(&self) -> [&str; 5] {
        [
            &self.sigungu_cd,
            &self.bjdong_cd,
            &self.san_gubun,
            &self.bonbeon,
            &self.bubeon,
        ]
    }
}

/// How a file without the PNU inputs reaches its parcel: by a building-register key the named
/// register also carries beside its `pnu`. The export stores the key verbatim and composes no PNU;
/// the consumer joins the register's current rows at read, so a re-composed or code-changed title
/// PNU is followed instead of frozen at load (root ADR-0404).
#[derive(Clone, Deserialize)]
pub(super) struct ParcelLink {
    pub(super) column: String,
    register: String,
    register_column: String,
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
    #[serde(default)]
    pub(super) row_key: Option<RowKey>,
    /// A second parcel a row names (부속지번's other 지번, root ADR-0395): each Silver column here is
    /// composed like `pnu`, from its own consumed columns.
    #[serde(default)]
    pub(super) extra_pnus: BTreeMap<String, PnuInputs>,
    #[serde(default)]
    pub(super) parcel_link: Option<ParcelLink>,
    /// Whether the Silver table has a `pnu` column: a linked layout's table must not (root ADR-0404).
    #[serde(skip)]
    table_has_pnu: bool,
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
        // A removal marker's column is written by the release compare, never by the source (root
        // ADR-0318); the export leaves it out and the load reads it as null.
        let removal = table
            .current_row_latest()
            .and_then(|latest| latest.removal_column);
        layout.expected_columns = table
            .columns
            .iter()
            .filter(|column| {
                // Columns the export makes itself rather than reads from a consumed source column.
                let made_here = super::GENERATED_COLUMNS.contains(&column.name)
                    || Some(column.name) == removal
                    || (layout.row_key.is_some() && column.name == "mgmt_key")
                    || layout.extra_pnus.contains_key(column.name);
                !made_here
            })
            .map(|column| column.name.to_owned())
            .collect();
        layout.table_has_pnu = table.columns.iter().any(|column| column.name == "pnu");
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
            self.columns
                .keys()
                .all(|name| !super::GENERATED_COLUMNS.contains(&name.as_str())),
            "HUB columns must not shadow generated columns"
        );
        let pnu_inputs = ["sigungu_cd", "bjdong_cd", "san_gubun", "bonbeon", "bubeon"]
            .iter()
            .all(|name| self.columns.contains_key(*name));
        ensure!(
            pnu_inputs != self.parcel_link.is_some() && self.table_has_pnu == pnu_inputs,
            "a HUB layout reaches its parcel by the PNU inputs into a `pnu` column or by a              parcel_link into a table without one, exactly one of them"
        );
        if let Some(RowKey::JoinedColumns(names)) = &self.row_key {
            ensure!(
                !names.is_empty() && names.iter().all(|name| self.columns.contains_key(name)),
                "a joined row key names consumed columns"
            );
        }
        ensure!(
            self.inner_file.matches(MONTH_SLOT).count() <= 1,
            "inner_file holds at most one {MONTH_SLOT}"
        );
        if let Some(link) = &self.parcel_link {
            let register = lakehouse_domain::industrial_complex_lakehouse_contract_by_table_name(
                &link.register,
            )
            .context("parcel_link names an unknown register table")?;
            ensure!(
                self.columns.contains_key(&link.column)
                    && [link.register_column.as_str(), "pnu"]
                        .iter()
                        .all(|name| register.columns.iter().any(|c| c.name == *name)),
                "parcel_link must name a consumed key column and a register carrying that key and a pnu"
            );
        }
        ensure!(
            self.columns.contains_key("mgmt_key") != self.row_key.is_some(),
            "a HUB layout reads its mgmt_key from a column or makes it from the row, not both"
        );
        ensure!(
            self.extra_pnus.iter().all(|(name, inputs)| {
                !self.columns.contains_key(name)
                    && !super::GENERATED_COLUMNS.contains(&name.as_str())
                    && inputs
                        .names()
                        .iter()
                        .all(|input| self.columns.contains_key(*input))
            }),
            "an extra PNU must be its own column composed from consumed columns"
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

    /// Whether `name` is this layout's ZIP member: `inner_file` exactly, or with its one
    /// `{YYYYMM}` standing for six digits (월별 에너지 names each file by its usage month,
    /// `eais_elcty_202606.txt`, root ADR-0396).
    pub(super) fn accepts_member(&self, name: &str) -> bool {
        match self.inner_file.split_once(MONTH_SLOT) {
            None => name == self.inner_file,
            Some((head, tail)) => name
                .strip_prefix(head)
                .and_then(|rest| rest.strip_suffix(tail))
                .is_some_and(|month| month.len() == 6 && month.bytes().all(|b| b.is_ascii_digit())),
        }
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
        Ok(crate::hub_provider_release::ProviderMonth::of_file(name)?.vintage())
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
