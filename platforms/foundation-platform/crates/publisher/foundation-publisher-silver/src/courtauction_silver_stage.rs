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
const CASE_LINES_SQL: &str = include_str!("courtauction_silver_stage/case_lines.sql");
const SLUG_PREFIX: &str = "courtauctiongokr__";

/// How a table keeps what an object stages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Keep {
    /// Its changed keys against the current rows (the job's `changed_only`).
    Changed,
    /// Every row: a record of what was asked.
    Every,
    /// Each group's whole list when it differs from the group's newest edition (the job's
    /// `changed_editions`).
    Editions,
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
    /// 매각공고 lists and each notice's lists (root ADR-0575 §6).
    SaleNotices,
    /// 사건상세: the case, its related cases, objects and parties (root ADR-0540 §1).
    CaseDetail,
    /// 현황조사: a round's survey and its objects (root ADR-0540 §1).
    CaseSurvey,
    /// 감정평가: a round's appraisal (root ADR-0540 §1).
    CaseAppraisal,
    /// 문건·송달 lists (root ADR-0575 §5).
    CaseFilings,
    /// 현황조사서 임대차 lists (root ADR-0575 §4).
    CaseSurveyReport,
    /// 물건상세: each property's read, its case's photos and its documents (root ADR-0384,
    /// ADR-0540 §4).
    ItemDetail,
}

/// Every role, in the order a run loads an object of each.
pub const ROLES: [Role; 11] = [
    Role::CommonCodes,
    Role::AppraisalReports,
    Role::SaleSpecifications,
    Role::DividendNotices,
    Role::SaleNotices,
    Role::CaseDetail,
    Role::CaseSurvey,
    Role::CaseAppraisal,
    Role::CaseFilings,
    Role::CaseSurveyReport,
    Role::ItemDetail,
];

/// The read record every case document writes, last.
const CASE_READ: (&str, Keep) = ("silver.court_auction_case_document_read", Keep::Every);

impl Role {
    /// Its section of the source contract.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CommonCodes => "common_codes",
            Self::AppraisalReports => "appraisal_reports",
            Self::SaleSpecifications => "sale_specifications",
            Self::DividendNotices => "dividend_notices",
            Self::SaleNotices => "sale_notices",
            Self::CaseDetail => "case_detail",
            Self::CaseSurvey => "case_survey",
            Self::CaseAppraisal => "case_appraisal",
            Self::CaseFilings => "case_filings",
            Self::CaseSurveyReport => "case_survey_report",
            Self::ItemDetail => "item_detail",
        }
    }

    /// A 사건 문서, which the contract lists under `case_documents`.
    const fn is_case_document(self) -> bool {
        matches!(
            self,
            Self::CaseDetail
                | Self::CaseSurvey
                | Self::CaseAppraisal
                | Self::CaseFilings
                | Self::CaseSurveyReport
        )
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
            Self::SaleNotices => &[
                ("silver.court_auction_sale_notice", Keep::Changed),
                ("silver.court_auction_sale_notice_item", Keep::Editions),
            ],
            Self::CaseDetail => &[
                ("silver.court_auction_case", Keep::Changed),
                ("silver.court_auction_case_related", Keep::Editions),
                ("silver.court_auction_case_object", Keep::Editions),
                ("silver.court_auction_case_party", Keep::Editions),
                CASE_READ,
            ],
            Self::CaseSurvey => &[
                ("silver.court_auction_survey", Keep::Changed),
                ("silver.court_auction_survey_object", Keep::Editions),
                CASE_READ,
            ],
            Self::CaseAppraisal => &[("silver.court_auction_appraisal", Keep::Changed), CASE_READ],
            Self::CaseFilings => &[
                ("silver.court_auction_case_filing", Keep::Editions),
                ("silver.court_auction_case_delivery", Keep::Editions),
                CASE_READ,
            ],
            Self::CaseSurveyReport => &[
                ("silver.court_auction_survey_tenant", Keep::Editions),
                CASE_READ,
            ],
            // The job's DETAIL_TABLES: the photos, the documents, then the reads.
            Self::ItemDetail => &[
                ("silver.court_auction_item_photo", Keep::Changed),
                ("silver.court_auction_case", Keep::Changed),
                ("silver.court_auction_item_spec", Keep::Changed),
                ("silver.court_auction_item_schedule", Keep::Editions),
                ("silver.court_auction_item_object", Keep::Editions),
                ("silver.court_auction_item_annex", Keep::Editions),
                ("silver.court_auction_appraisal_point", Keep::Editions),
                ("silver.court_auction_item_detail", Keep::Every),
            ],
        }
    }

    /// The free-text columns of each table that may name a person, masked after the rows are built
    /// (the job's `masked_text`, root ADR-0540 §4).
    #[must_use]
    pub const fn masked(self) -> &'static [(&'static str, &'static [&'static str])] {
        match self {
            Self::CaseSurvey => &[
                (
                    "silver.court_auction_survey",
                    &[
                        "possession_summary",
                        "first_possession",
                        "second_possession",
                        "lease_text",
                    ],
                ),
                (
                    "silver.court_auction_survey_object",
                    &["possession_text", "remarks"],
                ),
            ],
            Self::CaseAppraisal => &[("silver.court_auction_appraisal", &["opinion"])],
            Self::CaseSurveyReport => &[(
                "silver.court_auction_survey_tenant",
                &["leased_part_raw", "possession_text", "remarks"],
            )],
            Self::ItemDetail => &[
                (
                    "silver.court_auction_item_spec",
                    &[
                        "assumed_rights",
                        "superficies",
                        "spec_remarks",
                        "first_rank_setting",
                        "item_remarks",
                    ],
                ),
                ("silver.court_auction_item_annex", &["remarks"]),
                ("silver.court_auction_appraisal_point", &["content"]),
            ],
            Self::CommonCodes
            | Self::AppraisalReports
            | Self::SaleSpecifications
            | Self::DividendNotices
            | Self::SaleNotices
            | Self::CaseDetail
            | Self::CaseFilings => &[],
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
            Self::SaleNotices => include_str!("courtauction_silver_stage/sale_notices.sql"),
            Self::CaseDetail => include_str!("courtauction_silver_stage/case_detail.sql"),
            Self::CaseSurvey => include_str!("courtauction_silver_stage/case_survey.sql"),
            Self::CaseAppraisal => include_str!("courtauction_silver_stage/case_appraisal.sql"),
            Self::CaseFilings => include_str!("courtauction_silver_stage/case_filings.sql"),
            Self::CaseSurveyReport => {
                include_str!("courtauction_silver_stage/case_survey_report.sql")
            }
            Self::ItemDetail => include_str!("courtauction_silver_stage/item_detail.sql"),
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
            Self::SaleNotices => Some((
                "ca_notice_detail_lines",
                &[
                    "court_office_code",
                    "department_code",
                    "sale_date",
                    "bid_kind_code",
                ],
            )),
            Self::CaseDetail
            | Self::CaseSurvey
            | Self::CaseAppraisal
            | Self::CaseFilings
            | Self::CaseSurveyReport => Some(("ca_case_lines", &["court_office_code", "case_no"])),
            Self::ItemDetail => Some((
                "ca_detail_lines",
                &["court_office_code", "case_no", "property_seq"],
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
    mask: names::NameMask,
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

/// A name that goes into a JSON path: letters, digits and `_` only.
fn plain_name(text: &str) -> anyhow::Result<&str> {
    ensure!(
        !text.is_empty()
            && text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "{text:?} is not a plain name"
    );
    Ok(text)
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
        let contract: Value = serde_json::from_str(SOURCE_CONTRACT)
            .context("courtauction-go-kr.contract.json is not JSON")?;
        let mask = names::NameMask::new(&contract["personal_names"])?;
        Ok(Self { contract, mask })
    }

    /// A role's section of the contract: `case_documents.documents[]` for a 사건 문서.
    fn section(&self, role: Role) -> &Value {
        if role.is_case_document() {
            self.contract["case_documents"]["documents"]
                .as_array()
                .and_then(|documents| documents.iter().find(|d| d["role"] == role.name()))
                .unwrap_or(&Value::Null)
        } else {
            &self.contract[role.name()]
        }
    }

    fn field(&self, role: Role, name: &str) -> anyhow::Result<&str> {
        self.section(role)[name].as_str().with_context(|| {
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

    /// `code_of(item, OBJECT_CODE)` as SQL over `item`: the 물건상세's four legal-dong code fields,
    /// each of the list rows' width and all digits, joined; None otherwise.
    fn object_code_sql(&self) -> anyhow::Result<String> {
        let fields = self.contract["collection"]["pnu_assembly"]["legal_dong_code_fields"]
            .as_array()
            .context("courtauction-go-kr.contract.json has no legal_dong_code_fields")?;
        let widths = self.contract["pnu_resolution"]["from_code"]["legal_dong_code_widths"]
            .as_array()
            .context("courtauction-go-kr.contract.json has no legal_dong_code_widths")?;
        ensure!(
            !fields.is_empty() && fields.len() == widths.len(),
            "the legal-dong code fields and widths do not pair"
        );
        let mut parts = Vec::new();
        let mut checks = Vec::new();
        for (field, width) in fields.iter().zip(widths) {
            let field = plain_name(field.as_str().context("a code field is not text")?)?;
            let width = width.as_u64().context("a code width is not a count")?;
            let part = format!("onbid_or_blank(json_extract(item, '$.{field}'))");
            // shortcut: Python's isdigit() also takes a few non-decimal digits (superscripts); none
            // is a code field's value in the site's answers.
            checks.push(format!("regexp_full_match({part}, '\\p{{Nd}}{{{width}}}')"));
            parts.push(part);
        }
        Ok(format!(
            "CASE WHEN {} THEN {} END",
            checks.join(" AND "),
            parts.join(" || ")
        ))
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
        if role.is_case_document() {
            ensure!(
                self.contract["case_documents"]["key_fields"]
                    == serde_json::json!(["cortOfcCd", "csNo"]),
                "case_lines.sql reads the case key from cortOfcCd and csNo"
            );
            let quoted = |name: Option<&str>| {
                name.map_or_else(|| "NULL".to_owned(), |text| format!("'{text}'"))
            };
            let round_field = self.section(role)["round_field"]
                .as_str()
                .map(plain_name)
                .transpose()?;
            let answered = self.section(role)["answered_round_pointer"]
                .as_str()
                .map(|pointer| {
                    json_path(
                        pointer
                            .strip_prefix("/data")
                            .context("an answered round pointer starts at /data")?,
                    )
                })
                .transpose()?;
            sql = format!("{CASE_LINES_SQL}\n{sql}")
                .replace("{document}", plain_name(role.name())?)
                .replace(
                    "{request_object}",
                    plain_name(self.field(role, "request_object")?)?,
                )
                .replace("{round_field}", &quoted(round_field))
                .replace("{answered_round_path}", &quoted(answered.as_deref()))
                .replace("{object_code}", &self.object_code_sql()?);
        }
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
            Role::SaleNotices => {
                sql = sql
                    .replace("{list_operation}", literal(self.field(role, "operation")?)?)
                    .replace(
                        "{detail_operation}",
                        literal(self.field(role, "detail_operation")?)?,
                    )
                    .replace(
                        "{detail_request_object}",
                        plain_name(self.field(role, "detail_request_object")?)?,
                    );
            }
            Role::ItemDetail => {
                let photos = &self.section(role)["photos"];
                let photo = |name: &str| -> anyhow::Result<&str> {
                    plain_name(photos[name].as_str().with_context(|| {
                        format!("courtauction-go-kr.contract.json has no item_detail.photos.{name}")
                    })?)
                };
                sql = sql
                    .replace(
                        "{request_object}",
                        plain_name(self.field(role, "request_object")?)?,
                    )
                    .replace(
                        "{result_path}",
                        &json_path(self.field(role, "result_pointer")?)?,
                    )
                    .replace("{photo_list}", photo("list_field")?)
                    .replace("{photo_kind}", photo("kind_field")?)
                    .replace("{photo_sequence}", photo("sequence_field")?)
                    .replace("{photo_page}", photo("page_field")?)
                    .replace("{photo_title}", photo("title_field")?)
                    .replace("{object_code}", &self.object_code_sql()?);
            }
            Role::AppraisalReports
            | Role::SaleSpecifications
            | Role::CaseDetail
            | Role::CaseSurvey
            | Role::CaseAppraisal
            | Role::CaseFilings
            | Role::CaseSurveyReport => {}
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
        Role::of_table(table)?;
        for entry in plan["objects"]
            .as_array()
            .context("the plan lists no objects")?
        {
            let key = entry["object_key"]
                .as_str()
                .context("a planned object has no key")?;
            let found = entry["source_slug"].as_str().unwrap_or_default();
            // The source names the role: the 사건 문서 share their read record, so a plan of it
            // holds objects of each (the job's `search_roles`).
            let role = ROLES
                .into_iter()
                .find(|role| source.source_slug(*role).is_ok_and(|slug| slug == found))
                .with_context(|| format!("{key}: {found} is not a source this load reads"))?;
            ensure!(
                role.tables().iter().any(|(name, _)| *name == table),
                "{key}: {found} does not load into {table}"
            );
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

/// Masks the names in `columns` of `staged`: each distinct text masked once, in Rust (the patterns
/// look around a word, which `DuckDB`'s RE2 does not), then written back.
fn mask_columns(
    stage: &DuckDbParquetStage,
    mask: &names::NameMask,
    staged: &str,
    columns: &[&str],
) -> anyhow::Result<()> {
    let connection = stage.connection();
    for column in columns {
        let texts = connection
            .prepare(&format!(
                "SELECT DISTINCT {column} FROM {staged} WHERE {column} IS NOT NULL"
            ))?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        connection
            .execute_batch("CREATE OR REPLACE TEMP TABLE ca_masks (raw VARCHAR, masked VARCHAR)")?;
        {
            let mut appender = connection.appender("ca_masks")?;
            for text in &texts {
                appender.append_row(duckdb::params![text, mask.apply(text)?])?;
            }
            appender.flush()?;
        }
        connection
            .execute_batch(&format!(
                "UPDATE {staged} SET {column} = m.masked FROM ca_masks m WHERE {staged}.{column} = m.raw"
            ))
            .with_context(|| format!("DuckDB failed to mask {staged}.{column}"))?;
    }
    Ok(())
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
    for (table, columns) in object.role.masked() {
        mask_columns(stage, &source.mask, &Role::staged(table), columns)?;
    }
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
        Keep::Editions => {
            let out = format!("{}_changed", staged.staged);
            let rows =
                silver_changes::changed_editions(stage, contract, head, &staged.staged, &out)?;
            Ok((out, rows))
        }
    }
}

#[cfg(test)]
mod tests;
