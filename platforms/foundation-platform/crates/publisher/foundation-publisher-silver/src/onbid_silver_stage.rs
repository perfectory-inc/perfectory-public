//! Stages one Bronze object of 차세대 온비드 public sale as Silver rows (root ADR-0280, ADR-0340).
//!
//! In `DuckDB` inside the publisher (root ADR-0559): it was the Spark job `data_go_kr_onbid.py`. The
//! collector lands, per run, one gzip JSON Lines object per API; `plan-silver-load-objects` fetches,
//! per table, the objects its commits do not record yet. For each object, oldest first:
//!
//! - `answers.sql` reads its requests and the items of their 00 answers, and whether an item list
//!   object read every (prptDivCd, pvctTrgtYn) combination to its end;
//! - the role's row SQL ([`OnbidSource::rows_sql`]) builds one row per key from its last line,
//!   every column from the field the inventory (`data-go-kr-onbid.fields.json`) maps to it, read as
//!   the job read it (`values.sql`);
//! - an item list's rows are placed on the served parcels (`place_identity_stage`) and compared with
//!   the table's current rows (`item_changes.sql`: changed keys, and removal markers when the pass
//!   was complete); a compared table's rows are compared by key and content (`changed.sql`); the
//!   bid result and media tables take every row.
//!
//! The caller appends the staged file, each object in its own commit that records the object even
//! when it holds no row (`lakehouse_data_commit::append_at_head`).

use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context};
use chrono::{DateTime, Utc};
use foundation_publisher_core::lakehouse_data_commit::{StagedFile, TableShape};
use lakehouse_domain::LakehouseTableContract;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::duckdb_parquet_stage::{DuckDbParquetStage, PYTHON_VALUES_SQL};
use crate::silver_changes::{self, same, table_key, LOAD_COLUMNS};

/// The plan file `plan-silver-load-objects` writes.
pub const PLAN_SCHEMA: &str = "foundation-platform.silver_load_objects.v1";
/// The item condition every item-level answer carries.
const KEY: [&str; 2] = ["cltr_mng_no", "pbct_cdtn_no"];
/// Which round answered an item-level response: kept, but not content.
const ASKED_THROUGH: [&str; 1] = ["pbct_cdtn_no"];
/// The removal marker's value (root ADR-0275).
const REMOVED: &str = "removed";

const SOURCE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/data-go-kr-onbid.contract.json");
const FIELDS: &str =
    include_str!("../../../../infra/lakehouse/contracts/data-go-kr-onbid.fields.json");
const VALUES_SQL: &str = include_str!("onbid_silver_stage/values.sql");
const ANSWERS_SQL: &str = include_str!("onbid_silver_stage/answers.sql");
const ITEM_CHANGES_SQL: &str = include_str!("onbid_silver_stage/item_changes.sql");

/// What one object loads into: the table, and how its rows are kept (root ADR-0340 §6).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Role {
    /// The item list: the changed conditions, placed, and the removal markers of a complete pass.
    ItemList,
    /// Item bid results: every answer, as given.
    ItemBidResult,
    /// The media of the item detail, by content hash.
    ItemPhoto,
    /// Everything else of the item detail, per item.
    ItemDetail,
    /// Item bid information.
    ItemBidInfo,
    /// Announcements.
    Announcement,
    /// Announcement bid results.
    AnnouncementBidResult,
    /// Announcement details.
    AnnouncementDetail,
}

/// Every role, in the order the Spark job listed them.
pub const ROLES: [Role; 8] = [
    Role::ItemList,
    Role::ItemBidResult,
    Role::ItemPhoto,
    Role::ItemDetail,
    Role::ItemBidInfo,
    Role::Announcement,
    Role::AnnouncementBidResult,
    Role::AnnouncementDetail,
];

impl Role {
    /// The role's name in the summary line.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ItemList => "item_list",
            Self::ItemBidResult => "item_bid_result",
            Self::ItemPhoto => "item_photo",
            Self::ItemDetail => "item_detail",
            Self::ItemBidInfo => "item_bid_info",
            Self::Announcement => "announcement",
            Self::AnnouncementBidResult => "announcement_bid_result",
            Self::AnnouncementDetail => "announcement_detail",
        }
    }

    /// The inventory's API whose fields fill the table.
    const fn api(self) -> &'static str {
        match self {
            Self::ItemList => "item_list",
            Self::ItemBidResult => "item_bid_result",
            Self::ItemPhoto | Self::ItemDetail => "item_detail",
            Self::ItemBidInfo => "item_bid_info",
            Self::Announcement => "announcement_list",
            Self::AnnouncementBidResult => "announcement_bid_result_list",
            Self::AnnouncementDetail => "announcement_detail",
        }
    }

    /// The source contract's endpoint the objects come from.
    const fn endpoint(self) -> &'static str {
        self.api()
    }

    /// The Silver table.
    #[must_use]
    pub const fn table(self) -> &'static str {
        match self {
            Self::ItemList => "silver.onbid_real_estate_item",
            Self::ItemBidResult => "silver.onbid_item_bid_result",
            Self::ItemPhoto => "silver.onbid_real_estate_item_photo",
            Self::ItemDetail => "silver.onbid_real_estate_item_detail",
            Self::ItemBidInfo => "silver.onbid_item_bid_info",
            Self::Announcement => "silver.onbid_announcement",
            Self::AnnouncementBidResult => "silver.onbid_announcement_bid_result",
            Self::AnnouncementDetail => "silver.onbid_announcement_detail",
        }
    }

    /// Whether a key-less item refuses the object (the item tables, whose key every answer carries).
    const fn strict(self) -> bool {
        matches!(self, Self::ItemList | Self::ItemBidResult)
    }

    /// Whether the table appends only the keys whose content changed (root ADR-0311).
    #[must_use]
    pub const fn compared(self) -> bool {
        matches!(
            self,
            Self::ItemDetail
                | Self::ItemBidInfo
                | Self::Announcement
                | Self::AnnouncementBidResult
                | Self::AnnouncementDetail
        )
    }

    /// The role of a table this load writes.
    ///
    /// # Errors
    /// Refuses a table that is not one of them.
    pub fn of_table(table: &str) -> anyhow::Result<Self> {
        ROLES
            .into_iter()
            .find(|role| role.table() == table)
            .with_context(|| format!("{table} is not an Onbid table this load writes"))
    }
}

/// The source contract and the field inventory compiled into the publisher.
pub struct OnbidSource {
    contract: Value,
    inventory: Value,
}

fn plain(name: &str) -> anyhow::Result<&str> {
    ensure!(
        !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "{name:?} is not a plain field name"
    );
    Ok(name)
}

/// A text the SQL holds as a literal: refused when it holds a quote or a backslash.
fn literal(text: &str) -> anyhow::Result<&str> {
    ensure!(
        !text.contains('\'') && !text.contains('\\'),
        "{text:?} must not hold a quote"
    );
    Ok(text)
}

fn sql_list(values: &[String]) -> anyhow::Result<String> {
    let items = values
        .iter()
        .map(|value| literal(value).map(|value| format!("'{value}'")))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("[{}]::VARCHAR[]", items.join(", ")))
}

/// `sqmsList[].clandCont` → `sqmsList`: the response key a field path starts at.
fn top_field(path: &str) -> &str {
    let head = path.split("[]").next().unwrap_or(path);
    head.split('.').next().unwrap_or(head)
}

/// The SQL type a staged column carries for a contract's logical type.
fn sql_type(logical: &str) -> anyhow::Result<&'static str> {
    Ok(match logical {
        "string" => "VARCHAR",
        "int" => "INTEGER",
        "long" => "BIGINT",
        "double" => "DOUBLE",
        "timestamp" => "TIMESTAMPTZ",
        "date" => "DATE",
        other => bail!("an Onbid column of type {other} has no reader"),
    })
}

/// The fields one row reads, extracted from each item in one parse (`onbid_values.v`): an item is
/// about 1.4 KB and `DuckDB` keeps every parse of a chunk until the chunk ends.
#[derive(Default)]
struct Extraction {
    keys: Vec<String>,
}

impl Extraction {
    /// `v[n]`: the value of `key`, added to the extraction when it is new.
    fn value(&mut self, key: &str) -> anyhow::Result<String> {
        let key = plain(key)?;
        let at = if let Some(at) = self.keys.iter().position(|known| known == key) {
            at
        } else {
            self.keys.push(key.to_owned());
            self.keys.len() - 1
        };
        Ok(format!("v[{}]", at + 1))
    }

    fn sql(&self) -> String {
        let paths = self
            .keys
            .iter()
            .map(|key| format!("'$.\"{key}\"'"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE OR REPLACE TEMP TABLE onbid_values AS\nSELECT line_number, ordinal, json_extract(item, [{paths}]) AS v\nFROM onbid_items;\n\n"
        )
    }
}

/// The columns compared for a change: every column but the key, the item condition, the load's own
/// and which round answered (`data_go_kr_onbid.content_columns`).
#[must_use]
pub fn content_columns(contract: &LakehouseTableContract) -> Vec<&'static str> {
    let key = table_key(contract).unwrap_or_else(|_| KEY.to_vec());
    contract
        .columns
        .iter()
        .map(|column| column.name)
        .filter(|name| {
            !KEY.contains(name)
                && !key.contains(name)
                && !LOAD_COLUMNS.contains(name)
                && !ASKED_THROUGH.contains(name)
        })
        .collect()
}

impl OnbidSource {
    /// The contract and inventory compiled into the publisher.
    ///
    /// # Errors
    /// Returns an error when either is not JSON.
    pub fn compiled() -> anyhow::Result<Self> {
        Ok(Self {
            contract: serde_json::from_str(SOURCE_CONTRACT)
                .context("data-go-kr-onbid.contract.json is not JSON")?,
            inventory: serde_json::from_str(FIELDS)
                .context("data-go-kr-onbid.fields.json is not JSON")?,
        })
    }

    fn text(&self, pointer: &str) -> anyhow::Result<&str> {
        self.contract
            .pointer(pointer)
            .and_then(Value::as_str)
            .with_context(|| format!("data-go-kr-onbid.contract.json has no {pointer}"))
    }

    /// The Bronze source slug a role's objects come from (`datagokr__<dataset_slug>`).
    ///
    /// # Errors
    /// Returns an error when the contract names no dataset for the role's endpoint.
    pub fn source_slug(&self, role: Role) -> anyhow::Result<String> {
        Ok(format!(
            "datagokr__{}",
            self.text(&format!("/{}/dataset_slug", role.endpoint()))?
        ))
    }

    /// The run's time budget: an object not started by then is left for the next run.
    ///
    /// # Errors
    /// Returns an error when the contract does not say it.
    pub fn run_time_budget_seconds(&self) -> anyhow::Result<u64> {
        self.contract
            .pointer("/silver_load/run_time_budget_seconds")
            .and_then(Value::as_u64)
            .context("data-go-kr-onbid.contract.json has no silver_load.run_time_budget_seconds")
    }

    fn fields(&self, api: &str) -> anyhow::Result<&Vec<Value>> {
        self.inventory
            .pointer(&format!("/apis/{api}/fields"))
            .and_then(Value::as_array)
            .with_context(|| format!("data-go-kr-onbid.fields.json has no API {api}"))
    }

    /// The media fields of one kind and their `media_kind`, in the order the contract writes them
    /// (the Spark job read them in that order, and the last one naming a hash wins).
    fn media(&self, kind: &str) -> Vec<(String, String)> {
        let section = SOURCE_CONTRACT
            .find("\"media\"")
            .and_then(|at| {
                SOURCE_CONTRACT[at..]
                    .find(&format!("\"{kind}\""))
                    .map(|found| at + found)
            })
            .unwrap_or(0);
        let written_at = |field: &str| {
            SOURCE_CONTRACT[section..]
                .find(&format!("\"{field}\""))
                .unwrap_or(usize::MAX)
        };
        let mut found: Vec<(String, String)> = self
            .contract
            .pointer(&format!("/media/fields/{kind}"))
            .and_then(Value::as_object)
            .map(|fields| {
                fields
                    .iter()
                    .filter(|(field, _)| !field.starts_with('$'))
                    .map(|(field, value)| {
                        let kind = value
                            .as_str()
                            .or_else(|| value.get("media_kind").and_then(Value::as_str))
                            .unwrap_or_default();
                        (field.clone(), kind.to_owned())
                    })
                    .collect()
            })
            .unwrap_or_default();
        found.sort_by_key(|(field, _)| written_at(field));
        found
    }

    /// The keys the inventory marks `pii` under an API's lists (root ADR-0340 §5).
    fn private_keys(&self, api: &str) -> anyhow::Result<Vec<String>> {
        let mut keys = self
            .fields(api)?
            .iter()
            .filter(|field| field.get("pii").and_then(Value::as_bool) == Some(true))
            .filter_map(|field| field.get("path").and_then(Value::as_str))
            .map(|path| path.rsplit('.').next().unwrap_or(path).to_owned())
            .collect::<Vec<_>>();
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    /// Every response key the inventory knows for an API, with the collector's `<field>Media` and
    /// `<field>Original` siblings.
    fn known_fields(&self, api: &str) -> anyhow::Result<Vec<String>> {
        let mut known = self
            .fields(api)?
            .iter()
            .filter_map(|field| field.get("path").and_then(Value::as_str))
            .map(|path| top_field(path).to_owned())
            .collect::<Vec<_>>();
        known.extend(
            self.media("pipe")
                .into_iter()
                .map(|(field, _)| format!("{field}Media")),
        );
        known.extend(
            self.media("originals")
                .into_iter()
                .map(|(field, _)| format!("{field}Original")),
        );
        known.sort();
        known.dedup();
        Ok(known)
    }

    /// (column, response key) for every column of the role's table the API's fields claim, in the
    /// table's column order.
    fn column_sources(
        &self,
        role: Role,
        contract: &LakehouseTableContract,
    ) -> anyhow::Result<Vec<(&'static str, String)>> {
        let mut sources = std::collections::BTreeMap::new();
        for field in self.fields(role.api())? {
            let path = field
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or_default();
            for claim in field
                .get("silver")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                if let Some((table, column)) = claim.rsplit_once('.') {
                    if table == role.table() {
                        sources
                            .entry(column.to_owned())
                            .or_insert_with(|| top_field(path).to_owned());
                    }
                }
            }
        }
        Ok(contract
            .columns
            .iter()
            .filter_map(|column| {
                sources
                    .get(column.name)
                    .map(|source| (column.name, source.clone()))
            })
            .collect())
    }

    /// The macros the rows are read with: the shared Python readers, then the Onbid ones with the
    /// contract's PNU rule.
    ///
    /// # Errors
    /// Returns an error when the contract lacks the PNU rule.
    pub fn values_sql(&self) -> anyhow::Result<String> {
        let rule = self
            .contract
            .get("pnu")
            .context("data-go-kr-onbid.contract.json has no pnu rule")?;
        let dialect = rule
            .get("provider_ledger_kind_digits")
            .and_then(Value::as_object)
            .context("pnu.provider_ledger_kind_digits")?
            .iter()
            .filter(|(digit, _)| !digit.starts_with('$'))
            .map(|(digit, standard)| {
                let standard = standard.as_str().context("a standard digit")?;
                Ok(format!(
                    "WHEN '{}' THEN '{}'",
                    literal(digit)?,
                    literal(standard)?
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(" ");
        let digits = rule
            .get("ledger_kind_digits")
            .and_then(Value::as_array)
            .context("pnu.ledger_kind_digits")?
            .iter()
            .map(|digit| {
                digit
                    .as_str()
                    .map(|digit| format!("'{digit}'"))
                    .context("a ledger digit")
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(", ");
        Ok(format!(
            "{PYTHON_VALUES_SQL}\n{}",
            VALUES_SQL
                .replace("{pnu_dialect}", &dialect)
                .replace("{ledger_kind_digits}", &format!("[{digits}]"))
        ))
    }

    /// `answers.sql` with the contract's result codes and every combination an item list pass asks.
    ///
    /// # Errors
    /// Returns an error when the contract lacks them.
    // `{name}` is a placeholder of the SQL text, not a format argument.
    #[allow(clippy::literal_string_with_formatting_args)]
    pub fn answers_sql(&self) -> anyhow::Result<String> {
        let codes = self
            .contract
            .get("property_division_codes")
            .and_then(Value::as_object)
            .context("property_division_codes")?
            .keys()
            .filter(|code| !code.starts_with('$'))
            .collect::<Vec<_>>();
        let flags = self
            .contract
            .pointer("/item_list/private_contract_flags")
            .and_then(Value::as_array)
            .context("item_list.private_contract_flags")?;
        let mut combos = Vec::new();
        for code in codes {
            for flag in flags {
                let flag = flag.as_str().context("a private contract flag")?;
                combos.push(format!("('{}', '{}')", literal(code)?, literal(flag)?));
            }
        }
        ensure!(!combos.is_empty(), "an item list pass asks no combination");
        Ok(ANSWERS_SQL
            .replace("{ok_code}", literal(self.text("/result_codes/ok")?)?)
            .replace(
                "{no_data_code}",
                literal(self.text("/result_codes/no_data")?)?,
            )
            .replace("{combos}", &combos.join(", ")))
    }

    /// The role's row SQL: `onbid_rows`, one row per key of the object from its last line, every
    /// column of the table in contract order. Placeholders `{object_key}`, `{snapshot_id}`,
    /// `{ingested_at_epoch_seconds}` remain for the object.
    ///
    /// # Errors
    /// Returns an error when a column's type has no reader or a field name is not plain.
    pub fn rows_sql(
        &self,
        role: Role,
        contract: &LakehouseTableContract,
    ) -> anyhow::Result<String> {
        if role == Role::ItemPhoto {
            return self.photo_sql(contract);
        }
        let drop = sql_list(&self.private_keys(role.api())?)?;
        let mut extraction = Extraction::default();
        let mut built = std::collections::BTreeMap::new();
        for (column, key) in self.column_sources(role, contract)? {
            let value = extraction.value(&key)?;
            let logical = contract
                .columns
                .iter()
                .find(|c| c.name == column)
                .map(|c| c.logical_type)
                .unwrap_or_default();
            let expression = if column == "pnu" {
                format!("onbid_pnu({value})")
            } else if column.ends_with("_json") {
                format!("onbid_json_text({value}, {drop})")
            } else {
                match logical {
                    "string" => format!("py_text({value})"),
                    "int" | "long" => format!("onbid_whole({value})"),
                    "double" => format!("onbid_decimal({value})"),
                    "timestamp" => format!("onbid_kst({value})"),
                    "date" => format!("onbid_ymd({value})"),
                    other => bail!("{}.{column} is a {other}: no reader", role.table()),
                }
            };
            built.insert(column, expression);
        }
        let key = if role.strict() {
            KEY.to_vec()
        } else {
            table_key(contract)?
        };
        let select = contract
            .columns
            .iter()
            .map(|column| {
                let expression = built
                    .get(column.name)
                    .cloned()
                    .unwrap_or_else(|| load_column(column.name).unwrap_or("NULL").to_owned());
                Ok(format!(
                    "  CAST({expression} AS {}) AS {}",
                    sql_type(column.logical_type)?,
                    column.name
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(",\n");
        let missing = key
            .iter()
            .map(|column| format!("{column} IS NULL"))
            .collect::<Vec<_>>()
            .join(" OR ");
        let order = key.join(", ");
        let keep = if role.strict() {
            format!(
                "CASE WHEN {missing} THEN error('{{object_key}} line ' || line_number || ': an item without {}') ELSE true END",
                key.join(" and ")
            )
        } else {
            format!("NOT ({missing})")
        };
        Ok(format!(
            "{}CREATE OR REPLACE TEMP TABLE onbid_built AS\nSELECT line_number, ordinal,\n{select}\nFROM onbid_values;\n\n\
             -- One row per key: the object's last line that holds it (a later page is the newer answer).\n\
             CREATE OR REPLACE TEMP TABLE onbid_rows AS\nSELECT * EXCLUDE (line_number, ordinal, onbid_last) FROM (\n\
             \x20 SELECT *, row_number() OVER (PARTITION BY {order} ORDER BY line_number DESC, ordinal DESC) AS onbid_last\n\
             \x20 FROM onbid_built WHERE {keep}\n) WHERE onbid_last = 1;\n",
            extraction.sql()
        ))
    }

    fn photo_sql(&self, contract: &LakehouseTableContract) -> anyhow::Result<String> {
        let mut sources = self.media("list");
        sources.extend(
            self.media("pipe")
                .into_iter()
                .map(|(field, kind)| (format!("{field}Media"), kind)),
        );
        sources.extend(
            self.media("originals")
                .into_iter()
                .map(|(field, kind)| (format!("{field}Original"), kind)),
        );
        let mut extraction = Extraction::default();
        extraction.value("cltrMngNo")?;
        extraction.value("pbctCdtnNo")?;
        let entries = sources
            .iter()
            .enumerate()
            .map(|(index, (field, kind))| {
                Ok(format!(
                    "  SELECT {index} AS source_index, '{}' AS media_kind, {} AS entries",
                    literal(kind)?,
                    extraction.value(field)?
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join("\n  UNION ALL\n");
        let select = contract
            .columns
            .iter()
            .map(|column| {
                let expression = match column.name {
                    "cltr_mng_no" => "cltr",
                    "pbct_cdtn_no" => "pbct",
                    "sha256" => "py_text(json_extract(entry, '$.sha256'))",
                    "media_kind" => "media_kind",
                    "sequence" => "CASE WHEN list_contains(json_keys(entry), 'sequence') THEN onbid_whole(json_extract(entry, '$.sequence')) ELSE position END",
                    "title" => "py_text(json_extract(entry, '$.apslEvlOrgNm'))",
                    "media_format" => "py_text(json_extract(entry, '$.media_format'))",
                    "size_bytes" => "onbid_whole(json_extract(entry, '$.size_bytes'))",
                    other => load_column(other).unwrap_or("NULL"),
                };
                Ok(format!(
                    "  CAST({expression} AS {}) AS {}",
                    sql_type(column.logical_type)?,
                    column.name
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .join(",\n");
        Ok(format!(
            "{}CREATE OR REPLACE TEMP TABLE onbid_items_keyed AS\n\
             SELECT line_number, ordinal, v, coalesce(py_text(v[1]), '') AS cltr,\n\
             \x20      onbid_whole(v[2]) AS pbct\nFROM onbid_values;\n\n\
             CREATE OR REPLACE TEMP TABLE onbid_items_checked AS SELECT CASE WHEN cltr = '' OR pbct IS NULL THEN error('{{object_key}} line ' || line_number || ': an item without cltrMngNo and pbctCdtnNo') END AS refused FROM onbid_items_keyed;\n\n\
             -- One row per stored media file of each item; the last line, field and position that names an\n\
             -- (item, hash) wins.\n\
             CREATE OR REPLACE TEMP TABLE onbid_media AS\n\
             SELECT k.line_number, k.ordinal, k.cltr, k.pbct, s.source_index, s.media_kind, e.position, e.entry\n\
             FROM onbid_items_keyed k,\nLATERAL (\n{entries}\n) s,\n\
             LATERAL (SELECT unnest(range(CAST(json_array_length(s.entries) AS BIGINT))) AS position, unnest(json_extract(s.entries, '$[*]')) AS entry) e\n\
             WHERE json_type(s.entries) = 'ARRAY' AND json_type(e.entry) = 'OBJECT'\n\
             \x20 AND py_text(json_extract(e.entry, '$.sha256')) IS NOT NULL;\n\n\
             CREATE OR REPLACE TEMP TABLE onbid_rows AS\nSELECT\n{select}\nFROM (\n\
             \x20 SELECT *, row_number() OVER (PARTITION BY cltr, pbct, py_text(json_extract(entry, '$.sha256'))\n\
             \x20                              ORDER BY line_number DESC, ordinal DESC, source_index DESC, position DESC) AS onbid_last\n\
             \x20 FROM onbid_media\n) WHERE onbid_last = 1;\n",
            extraction.sql()
        ))
    }
}

/// What a load column is set to; `None` for a column no field fills.
fn load_column(name: &str) -> Option<&'static str> {
    match name {
        "source_record_id" => Some("'{object_key}'"),
        "source_line_number" => Some("line_number"),
        "source_snapshot_id" => Some("'{snapshot_id}'"),
        "ingested_at_utc" => Some("to_timestamp({ingested_at_epoch_seconds})"),
        _ => None,
    }
}

/// One Bronze object a plan fetched, and the table it loads into.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedObject {
    /// Its key in the lakehouse bucket: the batch identity the table records.
    pub object_key: String,
    /// What it loads as.
    pub role: Role,
    /// The run id the collector put in the key (`…-20990102t030405z.gz` → `20990102T030405Z`).
    pub source_snapshot_id: String,
    /// Where the plan put the object.
    pub path: PathBuf,
}

/// The run id in an object key.
///
/// # Errors
/// Refuses a key that carries none.
pub fn snapshot_id(object_key: &str) -> anyhow::Result<String> {
    let run = object_key
        .strip_suffix(".gz")
        .and_then(|stem| stem.rsplit_once('-'))
        .map(|(_, run)| run)
        .with_context(|| format!("{object_key} carries no run id"))?;
    let digits = |text: &str| text.bytes().all(|byte| byte.is_ascii_digit());
    ensure!(
        run.len() == 16
            && digits(&run[..8])
            && run.as_bytes()[8] == b't'
            && digits(&run[9..15])
            && run.as_bytes()[15] == b'z',
        "{object_key} carries no run id"
    );
    Ok(run.to_ascii_uppercase())
}

/// Every object the plans in `plan_dirs` fetched, with its role, oldest run first.
///
/// # Errors
/// Refuses another plan format, a plan of a table this load does not write, and an object of a
/// source that does not load into the plan's table.
pub fn planned_objects(
    plan_dirs: &[PathBuf],
    source: &OnbidSource,
) -> anyhow::Result<Vec<PlannedObject>> {
    let mut objects = Vec::new();
    for plan_dir in plan_dirs {
        let plan: Value = serde_json::from_slice(
            &std::fs::read(plan_dir.join("plan.json"))
                .with_context(|| format!("cannot read the plan in {}", plan_dir.display()))?,
        )?;
        ensure!(
            plan["schema_version"] == PLAN_SCHEMA,
            "{}: not a {PLAN_SCHEMA} plan",
            plan_dir.display()
        );
        let role = Role::of_table(
            plan["table"]
                .as_str()
                .with_context(|| format!("{}: the plan names no table", plan_dir.display()))?,
        )?;
        let slug = source.source_slug(role)?;
        for entry in plan["objects"]
            .as_array()
            .context("the plan lists no objects")?
        {
            let key = entry["object_key"]
                .as_str()
                .context("a planned object has no key")?;
            let found = entry["source_slug"].as_str().unwrap_or_default();
            ensure!(
                found == slug,
                "{key}: {found} does not load into {}",
                role.table()
            );
            let local = entry["local_path"]
                .as_str()
                .context("a planned object has no local path")?;
            ensure!(
                !local.starts_with('/') && !local.split('/').any(|part| part == ".."),
                "{key}: the local path {local:?} leaves the plan directory"
            );
            objects.push(PlannedObject {
                object_key: key.to_owned(),
                role,
                source_snapshot_id: snapshot_id(key)?,
                path: plan_dir.join(local),
            });
        }
    }
    objects.sort_by(|a, b| {
        (&a.source_snapshot_id, &a.object_key, a.role).cmp(&(
            &b.source_snapshot_id,
            &b.object_key,
            b.role,
        ))
    });
    let mut seen = std::collections::BTreeSet::new();
    for object in &objects {
        ensure!(
            seen.insert((object.role, object.object_key.clone())),
            "the plans name {} twice for {}",
            object.object_key,
            object.role.table()
        );
    }
    Ok(objects)
}

/// `answers.sql` split at its `-- step:` lines: run once, once per line, once at the end.
fn answer_steps(sql: &str) -> anyhow::Result<(&str, &str, &str)> {
    let step = |name: &str| format!("\n-- step: {name}\n");
    let (_, rest) = sql
        .split_once(&step("init"))
        .context("answers.sql has no init step")?;
    let (init, rest) = rest
        .split_once(&step("line"))
        .context("answers.sql has no line step")?;
    let (line, pass) = rest
        .split_once(&step("pass"))
        .context("answers.sql has no pass step")?;
    Ok((init, line, pass))
}

/// What one object holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadObject {
    /// Rows it stages (one per key; one per item and hash for the media).
    pub rows: u64,
    /// For an item list: whether the pass read every combination to its end.
    pub complete: bool,
    /// Response keys of its items the inventory does not know (root ADR-0340 §2).
    pub unrepresented_fields: Vec<String>,
    /// Its 200 lines whose body is not JSON: requests the provider did not answer.
    pub unreadable_answers: u64,
}

fn count(stage: &DuckDbParquetStage, table: &str) -> anyhow::Result<u64> {
    let value: i64 =
        stage
            .connection()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
    Ok(u64::try_from(value)?)
}

/// Reads `object` into `onbid_rows` (the table's columns, one row per key).
///
/// `values_sql` ([`OnbidSource::values_sql`]) must have run on the stage.
///
/// # Errors
/// Refuses what the Spark load stopped on: an item without its key in an item table; and a `DuckDB`
/// failure. A 200 whose body is not JSON stopped the Spark load too; here it is counted
/// ([`ReadObject::unreadable_answers`]) and read as a request with no answer, so the object is
/// loaded and recorded.
pub fn read_object(
    stage: &DuckDbParquetStage,
    source: &OnbidSource,
    contract: &LakehouseTableContract,
    object: &PlannedObject,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<ReadObject> {
    let lines = stage.load_lines(&object.path, "onbid_lines")?;
    let connection = stage.connection();
    let answers = source.answers_sql()?;
    let (init, line, pass) = answer_steps(&answers)?;
    let read = |sql: &str| {
        connection
            .execute_batch(sql)
            .with_context(|| format!("DuckDB failed to read {}", object.object_key))
    };
    read(init)?;
    // A line at a time: a line is a page of up to a thousand items, and a chunk of them outgrew 2 GB.
    for number in 1..=lines {
        read(&line.replace("{line_number}", &number.to_string()))?;
    }
    read(pass)?;
    let rows = source
        .rows_sql(object.role, contract)?
        .replace("{object_key}", literal(&object.object_key)?)
        .replace("{snapshot_id}", literal(&object.source_snapshot_id)?)
        .replace(
            "{ingested_at_epoch_seconds}",
            &ingested_at.timestamp().to_string(),
        );
    connection
        .execute_batch(&rows)
        .with_context(|| format!("DuckDB failed to build the rows of {}", object.object_key))?;
    let known = sql_list(&source.known_fields(object.role.api())?)?;
    let mut unknown = connection.prepare(&format!(
        "SELECT DISTINCT k FROM (SELECT unnest(json_keys(item)) AS k FROM onbid_items \
         WHERE json_type(item) = 'OBJECT') WHERE NOT list_contains({known}, k) ORDER BY k"
    ))?;
    let unrepresented_fields = unknown
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    let complete = object.role == Role::ItemList
        && connection.query_row("SELECT complete FROM onbid_pass", [], |row| row.get(0))?;
    let unreadable: i64 = connection.query_row(
        "SELECT count(*) FROM onbid_answers WHERE unreadable",
        [],
        |row| row.get(0),
    )?;
    Ok(ReadObject {
        rows: count(stage, "onbid_rows")?,
        complete,
        unrepresented_fields,
        unreadable_answers: u64::try_from(unreadable)?,
    })
}

fn current(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    columns: &[&str],
) -> anyhow::Result<String> {
    silver_changes::current_rows(stage, contract, head, columns, "onbid_rows")
}

/// What an item list object appends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ItemChanges {
    /// Rows appended: changed conditions and removal markers.
    pub changes: u64,
    /// Of them, removal markers.
    pub removed: u64,
}

/// Builds `onbid_changes` from the placed item rows (`onbid_staged`).
///
/// It holds the rows that are new or changed against the table's current rows at `head`, and, when
/// `complete`, a removal marker per current condition the pass no longer holds.
///
/// # Errors
/// Returns a `DuckDB` failure.
// `{name}` is a placeholder of the SQL text, not a format argument.
#[allow(clippy::literal_string_with_formatting_args)]
pub fn item_changes(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    object: &PlannedObject,
    complete: bool,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<ItemChanges> {
    let columns = contract
        .columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>();
    let key = literal(&object.object_key)?;
    let run = literal(&object.source_snapshot_id)?;
    let markers = columns
        .iter()
        .map(|column| match *column {
            "row_state" => format!("'{REMOVED}' AS row_state"),
            "source_snapshot_id" => format!("'{run}' AS source_snapshot_id"),
            "ingested_at_utc" => format!(
                "to_timestamp({}) AS ingested_at_utc",
                ingested_at.timestamp()
            ),
            "source_record_id" => format!("'{key}' AS source_record_id"),
            other => format!("c.{other}"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = ITEM_CHANGES_SQL
        .replace("{current}", &current(stage, contract, head, &columns)?)
        .replace("{key_matches}", &same(&KEY))
        .replace("{content_matches}", &same(&content_columns(contract)))
        .replace("{complete}", if complete { "true" } else { "false" })
        .replace("{columns}", &columns.join(", "))
        .replace("{marker_columns}", &markers);
    stage.connection().execute_batch(&sql).with_context(|| {
        format!(
            "DuckDB failed to compare the items of {}",
            object.object_key
        )
    })?;
    let removed: i64 = stage.connection().query_row(
        &format!("SELECT count(*) FROM onbid_changes WHERE row_state = '{REMOVED}'"),
        [],
        |row| row.get(0),
    )?;
    Ok(ItemChanges {
        changes: count(stage, "onbid_changes")?,
        removed: u64::try_from(removed)?,
    })
}

/// `onbid_changed`: the rows of a compared table whose key is new or whose content differs from a
/// current row of that key at `head`.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn compared_changes(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
) -> anyhow::Result<u64> {
    let mut compared = table_key(contract)?;
    compared.extend(content_columns(contract));
    silver_changes::changed(
        stage,
        contract,
        head,
        "onbid_rows",
        "onbid_changed",
        &compared,
    )
}

/// The name a staged file of `table` for `object_key` gets: the same object always names the same
/// file, so a run that died leaves at most one object's files behind (root ADR-0560 §2).
#[must_use]
pub fn file_name(table: &str, object_key: &str) -> String {
    let digest = format!("{:x}", Sha256::digest(object_key.as_bytes()));
    format!(
        "{}-{}",
        table.rsplit('.').next().unwrap_or(table).replace('_', "-"),
        &digest[..24]
    )
}

/// Writes the rows of `query` as the object's file of `table`; `None` when there are none.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn write(
    stage: &DuckDbParquetStage,
    shape: &TableShape,
    table: &str,
    query_table: &str,
    object_key: &str,
) -> anyhow::Result<Option<StagedFile>> {
    if count(stage, query_table)? == 0 {
        return Ok(None);
    }
    stage
        .write(
            shape,
            &format!("SELECT * FROM {query_table}"),
            &file_name(table, object_key),
        )
        .map(Some)
}

/// The plan directories under `root`: one per table, as the load script lays them out.
///
/// # Errors
/// Returns an error when `root` cannot be listed.
pub fn plan_dirs(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut dirs = std::fs::read_dir(root)
        .with_context(|| format!("cannot list {}", root.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.join("plan.json").is_file())
        .collect::<Vec<_>>();
    dirs.sort();
    Ok(dirs)
}

#[cfg(test)]
mod tests;
