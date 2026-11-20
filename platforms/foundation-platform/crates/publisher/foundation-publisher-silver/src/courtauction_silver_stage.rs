//! Stages one Bronze object of 법원경매정보 as Silver rows (root ADR-0280, ADR-0540, ADR-0575).
//!
//! In `DuckDB` inside the publisher (root ADR-0559): the roles here were the Spark job
//! `courtauction_go_kr.py`, which still loads the others until they move too. The collector lands,
//! per run and per document, one gzip JSON Lines object; `plan-silver-load-objects` fetches, per
//! table, the objects its commits do not record yet. For each object, oldest first, the role's SQL
//! builds every table it writes (one row per key, the last line naming it winning, each value read
//! as the job read it: `python_values.sql`, then `courtauction_silver_stage/values.sql`), and each
//! table keeps its changed keys (`silver_changes`) or every row (a read record).
//!
//! The caller appends each table's staged file, in the role's table order, each in its own commit
//! that records the object even when it holds no row (`lakehouse_data_commit::append_at_head`).

use std::path::PathBuf;

use anyhow::{ensure, Context};
use chrono::{DateTime, Utc};
use lakehouse_domain::LakehouseTableContract;
use serde_json::Value;

use crate::duckdb_parquet_stage::{DuckDbParquetStage, PYTHON_VALUES_SQL};
use crate::onbid_silver_stage::{snapshot_id, PLAN_SCHEMA};
use crate::silver_changes::{self, content_columns, table_key};

pub mod names;

const SOURCE_CONTRACT: &str =
    include_str!("../../../../infra/lakehouse/contracts/courtauction-go-kr.contract.json");
const VALUES_SQL: &str = include_str!("courtauction_silver_stage/values.sql");
const LINES_SQL: &str = include_str!("courtauction_silver_stage/lines.sql");
const SLUG_PREFIX: &str = "courtauctiongokr__";

/// How a table keeps what an object stages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Keep {
    /// Its changed keys against the current rows (the job's `changed_only`).
    Changed,
    /// Every row: a record of what was asked.
    Every,
}

/// What one object loads as.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Role {
    /// 공통코드 (root ADR-0540 §5).
    CommonCodes,
    /// 감정평가서 PDF reads (root ADR-0540 §1).
    AppraisalReports,
    /// 매각물건명세서 PDF reads (root ADR-0575 §1).
    SaleSpecifications,
    /// 배당요구종기공고 lists and their 물건 lists (root ADR-0575 §6).
    DividendNotices,
}

/// Every role, in the order a run loads an object of each.
pub const ROLES: [Role; 4] = [
    Role::CommonCodes,
    Role::AppraisalReports,
    Role::SaleSpecifications,
    Role::DividendNotices,
];

impl Role {
    /// Its section of the source contract.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CommonCodes => "common_codes",
            Self::AppraisalReports => "appraisal_reports",
            Self::SaleSpecifications => "sale_specifications",
            Self::DividendNotices => "dividend_notices",
        }
    }

    /// The tables it writes, in commit order (a read record last), and how each keeps its rows.
    #[must_use]
    pub const fn tables(self) -> &'static [(&'static str, Keep)] {
        match self {
            Self::CommonCodes => &[("silver.court_auction_code", Keep::Changed)],
            Self::AppraisalReports => &[
                ("silver.court_auction_appraisal_report", Keep::Changed),
                ("silver.court_auction_appraisal_report_read", Keep::Every),
            ],
            Self::SaleSpecifications => {
                &[("silver.court_auction_sale_specification_read", Keep::Every)]
            }
            Self::DividendNotices => &[
                ("silver.court_auction_dividend_notice", Keep::Changed),
                ("silver.court_auction_dividend_object", Keep::Changed),
            ],
        }
    }

    const fn sql(self) -> &'static str {
        match self {
            Self::CommonCodes => include_str!("courtauction_silver_stage/common_codes.sql"),
            Self::AppraisalReports => {
                include_str!("courtauction_silver_stage/appraisal_reports.sql")
            }
            Self::SaleSpecifications => {
                include_str!("courtauction_silver_stage/sale_specifications.sql")
            }
            Self::DividendNotices => include_str!("courtauction_silver_stage/dividend_notices.sql"),
        }
    }

    /// The staging table of rows of `table` (`ca_` and the table's name).
    fn staged(table: &str) -> String {
        format!("ca_{}", table.rsplit('.').next().unwrap_or(table))
    }

    /// The lines a line without its key refuses: the staging table of each line's key, for the
    /// roles whose builder raised on one.
    const fn keyed_lines(self) -> Option<(&'static str, &'static [&'static str])> {
        match self {
            Self::AppraisalReports => Some((
                "ca_report_lines",
                &[
                    "court_office_code",
                    "case_no",
                    "round_seq",
                    "appraisal_no",
                    "written_date",
                ],
            )),
            Self::SaleSpecifications => Some((
                "ca_spec_lines",
                &["court_office_code", "case_no", "item_seq", "document_id"],
            )),
            Self::CommonCodes | Self::DividendNotices => None,
        }
    }

    /// The role whose tables include `table`.
    ///
    /// # Errors
    /// Refuses a table no role here writes.
    pub fn of_table(table: &str) -> anyhow::Result<Self> {
        ROLES
            .into_iter()
            .find(|role| role.tables().iter().any(|(name, _)| *name == table))
            .with_context(|| format!("{table} is not a table the court auction DuckDB load writes"))
    }
}

/// The source contract compiled into the publisher.
#[derive(Clone, Debug)]
pub struct CourtAuctionSource {
    contract: Value,
}

/// A JSON pointer (`/data/sccdLst`) as a `DuckDB` JSON path (`$.data.sccdLst`).
fn json_path(pointer: &str) -> anyhow::Result<String> {
    let parts = pointer
        .strip_prefix('/')
        .with_context(|| format!("{pointer:?} is not a JSON pointer"))?
        .split('/')
        .collect::<Vec<_>>();
    ensure!(
        parts.iter().all(|part| !part.is_empty()
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')),
        "{pointer:?} is not a plain JSON pointer"
    );
    Ok(format!("$.{}", parts.join(".")))
}

/// Text that goes into the SQL between single quotes: refused when it holds one.
fn literal(text: &str) -> anyhow::Result<&str> {
    ensure!(!text.contains('\''), "{text:?} cannot go into the SQL");
    Ok(text)
}

impl CourtAuctionSource {
    /// The contract compiled into the publisher.
    ///
    /// # Errors
    /// Returns an error when it is not JSON.
    pub fn compiled() -> anyhow::Result<Self> {
        Ok(Self {
            contract: serde_json::from_str(SOURCE_CONTRACT)
                .context("courtauction-go-kr.contract.json is not JSON")?,
        })
    }

    fn field(&self, role: Role, name: &str) -> anyhow::Result<&str> {
        self.contract[role.name()][name].as_str().with_context(|| {
            format!(
                "courtauction-go-kr.contract.json has no {}.{name}",
                role.name()
            )
        })
    }

    /// The Bronze source slug of a role's objects (root ADR-0014 `{provider}__{dataset}`).
    ///
    /// # Errors
    /// Returns an error when the contract names no dataset for the role.
    pub fn source_slug(&self, role: Role) -> anyhow::Result<String> {
        Ok(format!(
            "{SLUG_PREFIX}{}",
            self.field(role, "dataset_slug")?
        ))
    }

    /// The contract's `personal_names`, compiled.
    ///
    /// # Errors
    /// Refuses rules that are not the contract's shape.
    pub fn name_mask(&self) -> anyhow::Result<names::NameMask> {
        names::NameMask::new(&self.contract["personal_names"])
    }

    /// The readers every object's SQL calls: run once on the stage.
    #[must_use]
    pub fn values_sql() -> String {
        format!("{PYTHON_VALUES_SQL}\n{VALUES_SQL}")
    }

    /// A role's SQL with the contract's values in it.
    ///
    /// # Errors
    /// Returns an error when the contract lacks a value the SQL takes.
    pub fn role_sql(&self, role: Role) -> anyhow::Result<String> {
        let mut sql = role.sql().to_owned();
        match role {
            Role::CommonCodes => {
                sql = sql.replace(
                    "{result_pointer}",
                    &json_path(self.field(role, "result_pointer")?)?,
                );
            }
            Role::DividendNotices => {
                sql = sql
                    .replace("{list_operation}", literal(self.field(role, "operation")?)?)
                    .replace(
                        "{objects_operation}",
                        literal(self.field(role, "objects_operation")?)?,
                    );
            }
            Role::AppraisalReports | Role::SaleSpecifications => {}
        }
        Ok(sql)
    }
}

/// One Bronze object a plan fetched, and the role it loads as.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedObject {
    /// Its key in the lakehouse bucket: the batch identity each table records.
    pub object_key: String,
    /// What it loads as.
    pub role: Role,
    /// The run id the collector put in the key.
    pub source_snapshot_id: String,
    /// Where the plan put the object.
    pub path: PathBuf,
}

/// Every object the plans in `plan_dirs` fetched, with its role, oldest run first.
///
/// An object two plans of its role name (one per table it writes) comes once.
///
/// # Errors
/// Refuses another plan format, a plan of a table this load does not write, and an object of a
/// source that does not load into the plan's table.
pub fn planned_objects(
    plan_dirs: &[PathBuf],
    source: &CourtAuctionSource,
) -> anyhow::Result<Vec<PlannedObject>> {
    let mut objects = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
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
        let table = plan["table"]
            .as_str()
            .with_context(|| format!("{}: the plan names no table", plan_dir.display()))?;
        let role = Role::of_table(table)?;
        let slug = source.source_slug(role)?;
        for entry in plan["objects"]
            .as_array()
            .context("the plan lists no objects")?
        {
            let key = entry["object_key"]
                .as_str()
                .context("a planned object has no key")?;
            let found = entry["source_slug"].as_str().unwrap_or_default();
            ensure!(found == slug, "{key}: {found} does not load into {table}");
            let local = entry["local_path"]
                .as_str()
                .context("a planned object has no local path")?;
            ensure!(
                !local.starts_with('/') && !local.split('/').any(|part| part == ".."),
                "{key}: the local path {local:?} leaves the plan directory"
            );
            if !seen.insert((role, key.to_owned())) {
                continue;
            }
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
    Ok(objects)
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

/// What one table of an object stages.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedTable {
    /// The Silver table.
    pub table: &'static str,
    /// How it keeps the rows.
    pub keep: Keep,
    /// The staging table that holds the object's rows of it.
    pub staged: String,
    /// How many.
    pub rows: u64,
}

/// Reads `object` into one staging table per table its role writes.
///
/// [`CourtAuctionSource::values_sql`] must have run on the stage.
///
/// # Errors
/// Refuses what the Spark load stopped on: a line that is not JSON, a read without its key (the
/// 감정평가서 and 매각물건명세서 records); and a `DuckDB` failure.
pub fn read_object(
    stage: &DuckDbParquetStage,
    source: &CourtAuctionSource,
    object: &PlannedObject,
    ingested_at: DateTime<Utc>,
) -> anyhow::Result<Vec<StagedTable>> {
    stage.load_lines(&object.path, "ca_lines")?;
    let fill = |sql: &str| -> anyhow::Result<String> {
        Ok(sql
            .replace("{object_key}", literal(&object.object_key)?)
            .replace("{snapshot_id}", literal(&object.source_snapshot_id)?)
            .replace(
                "{ingested_at_epoch_seconds}",
                &ingested_at.timestamp().to_string(),
            ))
    };
    stage
        .connection()
        .execute_batch(LINES_SQL)
        .with_context(|| format!("{} holds a line that is not JSON", object.object_key))?;
    stage
        .connection()
        .execute_batch(&fill(&source.role_sql(object.role)?)?)
        .with_context(|| format!("DuckDB failed to build the rows of {}", object.object_key))?;
    if let Some((lines, key)) = object.role.keyed_lines() {
        let missing = count(
            stage,
            &format!(
                "{lines} WHERE {}",
                key.iter()
                    .map(|column| format!("{column} IS NULL"))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
        )?;
        ensure!(
            missing == 0,
            "{}: {missing} line(s) without their key",
            object.object_key
        );
    }
    object
        .role
        .tables()
        .iter()
        .map(|&(table, keep)| {
            let staged = Role::staged(table);
            Ok(StagedTable {
                table,
                keep,
                rows: count(stage, &staged)?,
                staged,
            })
        })
        .collect()
}

/// The staging table of what `staged` appends to its table: its changed keys against the current
/// rows at `head` (`<staging>_changed`), or every row. Returns its name and how many rows.
///
/// # Errors
/// Returns a `DuckDB` failure.
pub fn appended(
    stage: &DuckDbParquetStage,
    contract: &LakehouseTableContract,
    head: Option<i64>,
    staged: &StagedTable,
) -> anyhow::Result<(String, u64)> {
    match staged.keep {
        Keep::Every => Ok((staged.staged.clone(), staged.rows)),
        Keep::Changed => {
            let out = format!("{}_changed", staged.staged);
            let mut compared = table_key(contract)?;
            compared.extend(content_columns(contract, &[])?);
            let rows =
                silver_changes::changed(stage, contract, head, &staged.staged, &out, &compared)?;
            Ok((out, rows))
        }
    }
}

#[cfg(test)]
mod tests;
