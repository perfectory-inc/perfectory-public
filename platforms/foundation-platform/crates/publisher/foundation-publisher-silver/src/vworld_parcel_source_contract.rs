//! The cadastral parcel editions, read from the Bronze ledger (root ADR-0067, ADR-0148, ADR-0209).
//!
//! `vworld-parcel-source-objects.json` holds the rules only: which member names an edition's
//! shapefile (`member_name`), which granularity loads, how far an edition may differ from the one
//! before it, where each edition's handoffs live, and which edition the map serves. Which objects
//! hold an edition is a fact of the ledger: `catalog.bronze_object` and the ZIP members measured
//! beside it (`catalog.bronze_object_member`, root ADR-0169 §1). Until 2026-10 the contract listed
//! all 272 objects of every edition by hand, one pull request per edition; [`editions`] derives
//! the same entries from the ledger at run time.
//!
//! Every reader goes through here: the catalog projection load and the national PostGIS mirror
//! (the served edition, [`served_edition_view_from_ledger`]), the 시군구 crosswalk (the served
//! edition's 시도, [`served_sido`], compiled in), and Python and the shell loaders through the
//! publisher's `vworld-parcel-editions` command (the JSON of [`editions`]), which
//! `vworld_parcel_editions.py --editions` reads.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, ensure, Context};
use chrono::NaiveDate;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{PgConnection, Row};

/// The contract version this module reads.
pub const SCHEMA_VERSION: u64 = 3;
/// A read bound: the parcel source holds a few hundred objects per edition, a handful of members each.
const MAX_MEMBER_ROWS: i64 = 200_000;
/// The command that measures what the ledger holds and nobody has read yet (root ADR-0169 §1).
const MEASURE_COMMAND: &str = "scripts/ops/bronze-object-members.sh";
/// Keys the contract must never hold again: the per-edition object lists the ledger replaced.
const RETIRED_KEYS: [&str; 2] = ["editions", "objects"];
const GRANULARITIES: [&str; 2] = ["sido", "sigungu"];

#[derive(Debug, Deserialize)]
struct Bounds {
    max_sido_set_difference: usize,
    max_sigungu_count_change_share: f64,
}

#[derive(Debug, Deserialize)]
struct Contract {
    source: String,
    load_granularity: String,
    member_name: String,
    served_edition: String,
    served_edition_sido: Vec<String>,
    handoff_prefix: String,
    handoff_prefix_legacy: BTreeMap<String, String>,
    handoff_suffix: String,
    new_edition_bounds: Bounds,
}

impl Contract {
    fn slug(&self) -> anyhow::Result<&str> {
        self.source
            .strip_prefix("bronze/source=")
            .filter(|slug| !slug.is_empty() && !slug.contains('/'))
            .with_context(|| format!("source {:?} is not bronze/source=<slug>", self.source))
    }

    fn handoff_prefix(&self, edition: &str) -> String {
        self.handoff_prefix_legacy
            .get(edition)
            .cloned()
            .unwrap_or_else(|| format!("{}/edition={edition}", self.handoff_prefix))
    }
}

fn retired_key(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => map.iter().find_map(|(key, inner)| {
            if RETIRED_KEYS.contains(&key.as_str()) {
                Some(key.clone())
            } else {
                retired_key(inner)
            }
        }),
        Value::Array(items) => items.iter().find_map(retired_key),
        _ => None,
    }
}

fn parse(contract_json: &str) -> anyhow::Result<Contract> {
    let raw: Value =
        serde_json::from_str(contract_json).context("parcel source contract is not valid JSON")?;
    if let Some(key) = retired_key(&raw) {
        bail!(
            "parcel source contract holds `{key}`: the objects of an edition are the Bronze \
             ledger's, read at run time (root ADR-0209), not a list in the contract"
        );
    }
    let version = raw.get("schema_version").and_then(Value::as_u64);
    ensure!(
        version == Some(SCHEMA_VERSION),
        "parcel source contract schema_version {version:?} is not the {SCHEMA_VERSION} this command reads"
    );
    let contract: Contract =
        serde_json::from_value(raw).context("parcel source contract is missing a rule")?;
    ensure!(
        GRANULARITIES.contains(&contract.load_granularity.as_str()),
        "load_granularity must be one of {GRANULARITIES:?}"
    );
    ensure!(
        is_edition(&contract.served_edition),
        "served_edition {:?} is not YYYYMM",
        contract.served_edition
    );
    contract.slug()?;
    Ok(contract)
}

fn is_edition(name: &str) -> bool {
    name.len() == 6
        && name.bytes().all(|b| b.is_ascii_digit())
        && matches!(
            name.get(4..),
            Some("01" | "02" | "03" | "04" | "05" | "06" | "07" | "08" | "09" | "10" | "11" | "12")
        )
}

/// The 시도 the served edition carries, as the contract states them for the compiled-in
/// crosswalk; every ledger read refuses a ledger whose served edition covers others.
///
/// # Errors
/// When the contract is unreadable or names no 시도.
pub fn served_sido(contract_json: &str) -> anyhow::Result<BTreeSet<String>> {
    let contract = parse(contract_json)?;
    let sido: BTreeSet<String> = contract.served_edition_sido.iter().cloned().collect();
    ensure!(
        !sido.is_empty() && sido.iter().all(|code| code.len() == 2),
        "served_edition_sido must name two-digit 시도 codes"
    );
    Ok(sido)
}

/// One ZIP member as measured beside the ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Member {
    /// The name in the central directory.
    pub name: String,
    /// The day of its MS-DOS time stamp, when it has one.
    pub modified: Option<NaiveDate>,
}

/// What the settled reading of one ledger object found.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reading {
    /// No settled reading yet (none, or only `failed` ones).
    Unmeasured,
    /// A ZIP, with its members in directory order.
    Zip(Vec<Member>),
    /// Settled as not a ZIP, or unreadable: it holds no edition.
    NotZip,
}

/// One object of the parcel source as the ledger records it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerObject {
    /// The Bronze key.
    pub object_key: String,
    /// The size the ledger recorded.
    pub size_bytes: u64,
    /// The provider's update date, when the collection recorded one.
    pub provider_updated_at: Option<NaiveDate>,
    /// Its settled ZIP member reading.
    pub reading: Reading,
}

struct Found<'a> {
    object: &'a LedgerObject,
    region: String,
    dataset_name: String,
}

/// The editions the ledger holds, as `{"editions": {<YYYYMM>: entry}, "skipped": [..], "refused": [..]}`.
///
/// An entry is what the contract used to list by hand: `provider_base_month`, `extracted_on`
/// (every member's date), `handoff_prefix`, `granularity_counts` and `objects`. An object is an
/// edition's when exactly one of its members matches `member_name`; the region code in that name
/// decides its granularity (two digits 시도, five 시군구). An edition that covers less of the
/// country than the one before it (`new_edition_bounds`), or whose 시도 objects reach a 시도 its
/// 시군구 objects do not, is a partial collection and is skipped by name. Refused outright: an
/// object nobody has measured yet (it may hold the newest edition), two objects of one region and
/// edition the provider update date cannot tell apart, and a served edition the ledger lacks or
/// whose 시도 differ from `served_edition_sido`.
///
/// # Errors
/// As above, and when the contract cannot be read.
pub fn editions(contract_json: &str, ledger: &[LedgerObject]) -> anyhow::Result<Value> {
    let contract = parse(contract_json)?;
    let slug = contract.slug()?;
    let grouped = group(&contract, ledger)?;
    let mut entries = serde_json::Map::new();
    let mut skipped = Vec::new();
    let mut previous: Option<(String, BTreeSet<String>, usize)> = None;
    for (edition, regions) in grouped.by_edition {
        let objects = pick(&edition, regions)?;
        let cover = |len: usize| -> BTreeSet<String> {
            objects
                .iter()
                .filter(|f| f.region.len() == len)
                .filter_map(|f| f.region.get(..2).map(str::to_owned))
                .collect()
        };
        let (sido, sigungu) = (cover(2), cover(5));
        let sigungu_count = objects.iter().filter(|f| f.region.len() == 5).count();
        if let Some(why) = partial(
            &contract.new_edition_bounds,
            &sido,
            &sigungu,
            sigungu_count,
            previous.as_ref(),
        )? {
            ensure!(
                edition != contract.served_edition,
                "the served edition {edition} is partial in the ledger: {why}"
            );
            skipped.push(format!(
                "{edition}: a partial collection, not an edition: {why}"
            ));
            continue;
        }
        let dates = grouped.dates.get(&edition).cloned().unwrap_or_default();
        let (Some(earliest), Some(latest)) = (dates.iter().min(), dates.iter().max()) else {
            bail!("edition {edition} has no member dates, so when it was extracted cannot be told");
        };
        if edition == contract.served_edition {
            let stated: BTreeSet<String> = contract.served_edition_sido.iter().cloned().collect();
            ensure!(
                stated == sigungu,
                "served_edition_sido says {stated:?}; the ledger's edition {edition} covers {sigungu:?}"
            );
        }
        let granularity = |f: &Found<'_>| {
            if f.region.len() == 2 {
                "sido"
            } else {
                "sigungu"
            }
        };
        let counts: BTreeMap<&str, usize> = GRANULARITIES
            .iter()
            .map(|g| (*g, objects.iter().filter(|f| granularity(f) == *g).count()))
            .collect();
        entries.insert(
            edition.clone(),
            json!({
                "provider_base_month": format!("{}-{}", edition.get(..4).unwrap_or_default(), edition.get(4..).unwrap_or_default()),
                "extracted_on": {"earliest": earliest.to_string(), "latest": latest.to_string()},
                "handoff_prefix": contract.handoff_prefix(&edition),
                "granularity_counts": counts,
                "objects": objects.iter().map(|f| json!({
                    "object_key": f.object.object_key,
                    "bytes": f.object.size_bytes,
                    "dataset_name": f.dataset_name,
                    "region_code": f.region,
                    "granularity": granularity(f),
                })).collect::<Vec<_>>(),
            }),
        );
        previous = Some((edition, sigungu, sigungu_count));
    }
    ensure!(
        entries.contains_key(&contract.served_edition),
        "the ledger holds no edition {} (served_edition); it holds {:?}",
        contract.served_edition,
        entries.keys().collect::<Vec<_>>()
    );
    Ok(json!({"source": slug, "editions": entries, "skipped": skipped, "refused": grouped.refused}))
}

struct Grouped<'a> {
    by_edition: BTreeMap<String, BTreeMap<String, Vec<Found<'a>>>>,
    dates: BTreeMap<String, Vec<NaiveDate>>,
    refused: Vec<String>,
}

/// The ledger's objects by edition and region, by the contract's member rule.
fn group<'a>(contract: &Contract, ledger: &'a [LedgerObject]) -> anyhow::Result<Grouped<'a>> {
    let slug = contract.slug()?;
    let rule = Regex::new(&contract.member_name)
        .with_context(|| format!("member_name {:?} is not a regex", contract.member_name))?;
    let groups: BTreeSet<&str> = rule.capture_names().flatten().collect();
    ensure!(
        contract.member_name.starts_with('^')
            && contract.member_name.ends_with('$')
            && groups == BTreeSet::from(["edition", "region"]),
        "member_name must match a whole name and name the groups edition and region"
    );
    let unmeasured: Vec<&str> = ledger
        .iter()
        .filter(|object| object.reading == Reading::Unmeasured)
        .map(|object| object.object_key.as_str())
        .collect();
    ensure!(
        unmeasured.is_empty(),
        "{} Bronze objects of {slug} have no ZIP member reading yet, so its editions cannot be told \
         (first: {}). Measure them, then run again: {MEASURE_COMMAND} {slug} (root ADR-0169 §1)",
        unmeasured.len(),
        unmeasured.iter().take(3).copied().collect::<Vec<_>>().join(", ")
    );
    let mut grouped = Grouped {
        by_edition: BTreeMap::new(),
        dates: BTreeMap::new(),
        refused: Vec::new(),
    };
    for object in ledger {
        let Reading::Zip(members) = &object.reading else {
            continue;
        };
        let matching: Vec<_> = members
            .iter()
            .filter_map(|member| rule.captures(&member.name).map(|found| (member, found)))
            .collect();
        let (member, found) = match matching.as_slice() {
            [] => continue,
            [one] => one,
            many => {
                grouped.refused.push(format!(
                    "{} holds {} edition members",
                    object.object_key,
                    many.len()
                ));
                continue;
            }
        };
        let (Some(edition), Some(region)) = (found.name("edition"), found.name("region")) else {
            continue;
        };
        let (edition, region) = (edition.as_str().to_owned(), region.as_str().to_owned());
        let in_source = object
            .object_key
            .strip_prefix(&format!("{}/", contract.source))
            .is_some_and(|file| {
                !file.contains('/')
                    && std::path::Path::new(file)
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("zip"))
            });
        if !is_edition(&edition) || !matches!(region.len(), 2 | 5) || !in_source {
            grouped
                .refused
                .push(format!("{} holds {}", object.object_key, member.name));
            continue;
        }
        grouped
            .dates
            .entry(edition.clone())
            .or_default()
            .extend(members.iter().filter_map(|m| m.modified));
        let dataset_name = member
            .name
            .rsplit_once('.')
            .map_or(member.name.as_str(), |(stem, _)| stem)
            .to_owned();
        grouped
            .by_edition
            .entry(edition)
            .or_default()
            .entry(region.clone())
            .or_default()
            .push(Found {
                object,
                region,
                dataset_name,
            });
    }
    Ok(grouped)
}

/// One object per region: the provider re-uploads under the same base month, so the later update
/// wins, and two the date cannot tell apart are refused rather than decided by key order.
fn pick<'a>(
    edition: &str,
    regions: BTreeMap<String, Vec<Found<'a>>>,
) -> anyhow::Result<Vec<Found<'a>>> {
    let mut objects = Vec::new();
    for (region, mut held) in regions {
        let latest = held
            .iter()
            .map(|f| f.object.provider_updated_at)
            .max()
            .flatten();
        let winners: Vec<usize> = (0..held.len())
            .filter(|&at| held[at].object.provider_updated_at == latest)
            .collect();
        let [chosen] = winners.as_slice() else {
            bail!(
                "{} objects of edition {edition} region {region} share the provider update date \
                 {latest:?}; refusing to choose: {}",
                winners.len(),
                winners
                    .iter()
                    .map(|&at| held[at].object.object_key.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        };
        objects.push(held.swap_remove(*chosen));
    }
    objects.sort_by_key(|f| (f.region.len() != 2, f.region.clone()));
    Ok(objects)
}

/// Why an edition is a partial collection, if it is: a 시도 object without its 시군구 objects, or
/// less of the country than the edition before it (`new_edition_bounds`, root ADR-0148 §6).
fn partial(
    bounds: &Bounds,
    sido: &BTreeSet<String>,
    sigungu: &BTreeSet<String>,
    sigungu_count: usize,
    previous: Option<&(String, BTreeSet<String>, usize)>,
) -> anyhow::Result<Option<String>> {
    if sigungu.is_empty() || !sido.is_subset(sigungu) {
        return Ok(Some(format!(
            "its 시도 objects cover {sido:?} and its 시군구 objects {sigungu:?}; every province needs its districts"
        )));
    }
    let Some((before, was_sido, was_count)) = previous else {
        return Ok(None);
    };
    let left = was_sido.difference(sigungu).count();
    let appeared = sigungu.difference(was_sido).count();
    if (left > 0 && appeared == 0) || left + appeared > bounds.max_sido_set_difference {
        return Ok(Some(format!(
            "it covers the 시도 {sigungu:?} where {before} covered {was_sido:?}"
        )));
    }
    let allowed = f64::from(u32::try_from(*was_count)?) * bounds.max_sigungu_count_change_share;
    let changed = f64::from(u32::try_from(sigungu_count.abs_diff(*was_count))?);
    Ok((changed > allowed).then(|| {
        format!("it holds {sigungu_count} 시군구 objects where {before} held {was_count}")
    }))
}

/// Every object of the parcel source with its settled reading's members (bounded).
///
/// # Errors
/// When the ledger cannot be read or holds more rows than the bound.
pub async fn read_ledger(
    conn: &mut PgConnection,
    contract_json: &str,
) -> anyhow::Result<Vec<LedgerObject>> {
    let contract = parse(contract_json)?;
    let rows = sqlx::query(
        "SELECT b.id::text AS id, b.object_key, b.provider_updated_at, b.size_bytes, m.outcome,
                member.member_name, member.member_modified::date AS member_modified
         FROM catalog.bronze_object b
         JOIN catalog.source_catalog s ON s.id = b.source_catalog_id
         LEFT JOIN catalog.bronze_object_measurement m
                ON m.bronze_object_id = b.id AND m.outcome <> 'failed'
         LEFT JOIN catalog.bronze_object_member member ON member.measurement_id = m.id
         WHERE s.slug = $1
         ORDER BY b.object_key, b.id, member.member_index
         LIMIT $2",
    )
    .bind(contract.slug()?)
    .bind(MAX_MEMBER_ROWS + 1)
    .fetch_all(conn)
    .await
    .context("cannot read the Bronze ledger and its ZIP members")?;
    ensure!(
        i64::try_from(rows.len())? <= MAX_MEMBER_ROWS,
        "the parcel source holds more than {MAX_MEMBER_ROWS} ledger member rows"
    );
    let mut objects: BTreeMap<String, LedgerObject> = BTreeMap::new();
    for row in &rows {
        let id: String = row.try_get("id")?;
        let name: Option<String> = row.try_get("member_name")?;
        if !objects.contains_key(&id) {
            let reading = match row.try_get::<Option<String>, _>("outcome")?.as_deref() {
                None => Reading::Unmeasured,
                Some("zip") => Reading::Zip(Vec::new()),
                Some("not_zip" | "unreadable") => Reading::NotZip,
                Some(other) => bail!("unknown settled measurement outcome {other:?}"),
            };
            let object = LedgerObject {
                object_key: row.try_get("object_key")?,
                size_bytes: u64::try_from(row.try_get::<i64, _>("size_bytes")?)?,
                provider_updated_at: row.try_get("provider_updated_at")?,
                reading,
            };
            objects.insert(id.clone(), object);
        }
        if let (
            Some(name),
            Some(LedgerObject {
                reading: Reading::Zip(members),
                ..
            }),
        ) = (name, objects.get_mut(&id))
        {
            members.push(Member {
                name,
                modified: row.try_get("member_modified")?,
            });
        }
    }
    Ok(objects.into_values().collect())
}

/// The served edition in the per-edition reader shape (`load_granularity`, `handoff_prefix`,
/// `handoff_suffix`, `granularity_counts`, `objects`), from the ledger's editions.
///
/// # Errors
/// When the contract is unreadable or `editions` lacks the served edition.
pub fn served_edition_view(contract_json: &str, editions: &Value) -> anyhow::Result<Value> {
    let contract = parse(contract_json)?;
    let served = &contract.served_edition;
    let edition = editions
        .get("editions")
        .and_then(|all| all.get(served))
        .with_context(|| format!("the ledger's editions hold no served edition {served}"))?;
    let field = |name: &str| -> anyhow::Result<Value> {
        edition
            .get(name)
            .cloned()
            .with_context(|| format!("edition {served} names no {name}"))
    };
    Ok(json!({
        "schema_version": SCHEMA_VERSION,
        "edition": served,
        "load_granularity": contract.load_granularity,
        "handoff_suffix": contract.handoff_suffix,
        "handoff_prefix": field("handoff_prefix")?,
        "granularity_counts": field("granularity_counts")?,
        "objects": field("objects")?,
    }))
}

/// [`served_edition_view`] over the editions the ledger holds now.
///
/// # Errors
/// As [`read_ledger`], [`editions`] and [`served_edition_view`].
pub async fn served_edition_view_from_ledger(
    conn: &mut PgConnection,
    contract_json: &str,
) -> anyhow::Result<Value> {
    let ledger = read_ledger(conn, contract_json).await?;
    served_edition_view(contract_json, &editions(contract_json, &ledger)?)
}

#[cfg(test)]
#[path = "vworld_parcel_source_contract_tests.rs"]
mod tests;
