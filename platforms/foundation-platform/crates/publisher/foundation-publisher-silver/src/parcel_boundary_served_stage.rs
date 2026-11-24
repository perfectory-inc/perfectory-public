//! The served parcel boundaries in `DuckDB` inside the publisher.
//!
//! Root ADR-0133 §2, ADR-0559; on the shared Gold versions driver (root ADR-0615 §7).
//!
//! One cadastral edition of `silver.parcel_boundaries` with every ledgered `parcels` edit applied
//! ([`ServedBoundaries`]). It was the Spark jobs `parcel_boundary_served_gold.py` and
//! `parcel_boundary_fold.py`; the fold is the shared driver's partial run over the PNUs whose final
//! edit state moved. This module also holds the ledger's checks, each edited PNU's final state, the
//! edit store handoff's reading and the bake handoff's line.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::Instant;

use anyhow::{bail, ensure, Context};
use duckdb::Connection;
use sha2::{Digest, Sha256};

use crate::kind_versions_stage::{
    elapsed, fill, is_plain, spool_by_sido, KindVersionsStage, RangedRead, RowSet,
    CODE_CHANGE_SOURCE, SHARED_SQL,
};
use crate::parcel_nearby_stage::Source;

/// The map edit ledger's unit of the parcels.
pub const UNIT: &str = "parcels";
/// The parcels' one CRS (the Silver and served contracts declare it).
pub const GEOMETRY_SRID: i32 = 4326;
/// What an edit does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Op {
    /// The parcel is served with the edit's geometry.
    Upsert,
    /// The parcel is not served.
    Delete,
}

/// One ledgered `parcels` edit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerEdit {
    /// Its place in the ledger's order.
    pub change_seq: i64,
    /// The PNU.
    pub feature_id: String,
    /// Upsert or delete.
    pub op: Op,
    /// Its CRS.
    pub geometry_srid: i32,
    /// The geometry an upsert puts.
    pub geometry_wkb: Option<Vec<u8>>,
    /// Its digest.
    pub geometry_checksum_sha256: Option<String>,
}

impl LedgerEdit {
    /// The served `source_snapshot_id` of a parcel this edit puts.
    #[must_use]
    pub fn source_snapshot_id(&self) -> String {
        format!("map-edit-{}", self.change_seq)
    }
}

fn is_pnu(value: &str) -> bool {
    value.len() == 19 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// The unit's ledgered edits in change order, refusing any the served table could not hold
/// (`parcel_boundary_served_gold.check_ledger`).
///
/// # Errors
/// A repeated or non-positive `change_seq`, another CRS, a feature id that is not a PNU, or an upsert
/// without its geometry or checksum.
pub fn check_ledger(mut ledger: Vec<LedgerEdit>) -> anyhow::Result<Vec<LedgerEdit>> {
    let mut seen = BTreeSet::new();
    for edit in &ledger {
        let seq = edit.change_seq;
        ensure!(
            seq > 0 && seen.insert(seq),
            "the parcels ledger repeats or lacks change_seq {seq}"
        );
        ensure!(
            edit.geometry_srid == GEOMETRY_SRID,
            "edit {seq} is EPSG:{}, the parcels are EPSG:{GEOMETRY_SRID}",
            edit.geometry_srid
        );
        ensure!(
            is_pnu(&edit.feature_id),
            "edit {seq} names {:?}, which is not a PNU",
            edit.feature_id
        );
        if edit.op == Op::Upsert {
            ensure!(
                edit.geometry_wkb
                    .as_ref()
                    .is_some_and(|wkb| !wkb.is_empty()),
                "edit {seq} upserts a parcel without its geometry"
            );
            ensure!(
                edit.geometry_checksum_sha256
                    .as_deref()
                    .is_some_and(is_sha256_hex),
                "edit {seq} upserts a parcel without its geometry checksum"
            );
        }
    }
    ledger.sort_by_key(|edit| edit.change_seq);
    Ok(ledger)
}

/// What the edits did, as the summary counts them.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct EditCounts {
    /// Upserts.
    pub upserts: u64,
    /// Deletes.
    pub deletes: u64,
    /// Deletes of a parcel not there at that point in the order: counted, never passed over.
    pub deletes_of_absent_features: u64,
}

/// Each edited PNU's final state (its last upsert, or `None` when deleted) and what the edits did
/// (`parcel_boundary_served_gold.fold_edits`). `present` is which edited PNUs the edition holds.
#[must_use]
pub fn fold_edits<'a>(
    ledger: &'a [LedgerEdit],
    present: &BTreeSet<String>,
) -> (BTreeMap<String, Option<&'a LedgerEdit>>, EditCounts) {
    let mut last: BTreeMap<String, Option<&LedgerEdit>> = BTreeMap::new();
    let mut counts = EditCounts::default();
    for edit in ledger {
        let exists = last
            .get(&edit.feature_id)
            .map_or_else(|| present.contains(&edit.feature_id), Option::is_some);
        match edit.op {
            Op::Delete => {
                counts.deletes += 1;
                if !exists {
                    counts.deletes_of_absent_features += 1;
                }
                last.insert(edit.feature_id.clone(), None);
            }
            Op::Upsert => {
                counts.upserts += 1;
                last.insert(edit.feature_id.clone(), Some(edit));
            }
        }
    }
    (last, counts)
}

/// One bake handoff line without its newline, byte for byte as `served_gold_common.bake_handoff_line`
/// writes it for parcels: sorted keys, no spaces, the WKB as lowercase hex, no properties.
#[must_use]
pub fn handoff_line(feature_id: &str, geometry_wkb: &[u8], origin: &str) -> String {
    let mut hex = String::with_capacity(geometry_wkb.len() * 2);
    for byte in geometry_wkb {
        let _ = write!(hex, "{byte:02x}");
    }
    format!(
        "{{\"feature_id\":{},\"geometry_srid\":{GEOMETRY_SRID},\"geometry_wkb_hex\":\"{hex}\",\"origin\":{},\"properties\":{{}}}}",
        serde_json::Value::from(feature_id),
        serde_json::Value::from(origin),
    )
}

/// A served row's content digest: the PNU, the geometry's checksum, the CRS and the origin, not the
/// edition or the stamps, so a new edition appends only the parcels whose geometry moved.
#[must_use]
pub fn row_digest(pnu: &str, geometry_checksum_sha256: &str, origin: &str) -> String {
    let mut digest = Sha256::new();
    for part in [
        pnu,
        geometry_checksum_sha256,
        &GEOMETRY_SRID.to_string(),
        origin,
    ] {
        digest.update(part.as_bytes());
        digest.update([0x1f]);
    }
    format!("{:x}", digest.finalize())
}

/// The `DuckDB` expression of [`row_digest`] over a row's columns.
pub const ROW_DIGEST_SQL: &str = "sha256(pnu || chr(31) || geometry_checksum_sha256 || chr(31) || \
     CAST(geometry_srid AS VARCHAR) || chr(31) || origin || chr(31))";

/// The rules this module builds by; a commit records it, so a change of them rebuilds every 시도.
const RULES_VERSION: &str =
    "parcel-boundary-served v2: edition minus edited PNUs plus final upserts, a 시군구 at a time";

/// The cadastral editions the boundaries are served from.
pub const PARCEL_SOURCE: &str = crate::kind_versions_stage::PARCEL_SOURCE;
/// The map edit ledger, to which every edit is appended once (root ADR-0375).
pub const LEDGER_SOURCE: &str = "silver.map_edit_ledger";
/// The spool, and a 시도's table, of each edited PNU's final state.
const EDITS: &str = "edits";
/// A 시도's open rows of the edition, read by its ranges (never spooled: 39,861,511 geometries).
const EDITION_ROWS: &str = "edition_rows";
/// The newest edit a build applies, `(change_seq)`.
const EDITS_THROUGH: &str = "edits_through";

/// The edition's open rows in the ranges a 시도's load reads ([`RangedRead`]).
const EDITION_ROWS_SQL: &str = "SELECT pnu, geometry_wkb, geometry_checksum_sha256, \
    CAST(source_snapshot_id AS VARCHAR) AS source_snapshot_id FROM @table@ \
    WHERE source_snapshot_id = '@edition@' AND valid_to_utc IS NULL AND (@range@)";

/// Each edited PNU's final state: its last upsert's geometry, or a delete (no geometry).
const EDIT_STATES_TABLE: &str =
    "CREATE OR REPLACE TABLE edit_states@suffix@ (pnu VARCHAR NOT NULL, \
    upsert BOOLEAN NOT NULL, geometry_wkb BLOB, geometry_checksum_sha256 VARCHAR, \
    source_snapshot_id VARCHAR);";

/// `built`: the versions table's content columns.
const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, geometry_wkb BLOB, \
    geometry_srid INTEGER, geometry_checksum_sha256 VARCHAR, origin VARCHAR, \
    source_snapshot_id VARCHAR, silver_iceberg_snapshot_id VARCHAR, \
    edits_through_change_seq BIGINT, published_at_utc TIMESTAMP, row_digest VARCHAR);";

/// The served rows of the loaded part into `built`: the edition's rows but the edited PNUs, and
/// each edited PNU's final upsert (a deleted one has no row, so the diff marks it removed).
const BUILD_SQL: &str = "INSERT INTO built \
    SELECT pnu, geometry_wkb, geometry_srid, geometry_checksum_sha256, origin, source_snapshot_id, \
      '@silver@' AS silver_iceberg_snapshot_id, CAST(@through@ AS BIGINT) AS edits_through_change_seq, \
      CAST('@published@' AS TIMESTAMP) AS published_at_utc, @digest@ AS row_digest \
    FROM (SELECT pnu, geometry_wkb, CAST(@srid@ AS INTEGER) AS geometry_srid, geometry_checksum_sha256, \
            'source' AS origin, source_snapshot_id \
          FROM edition_rows WHERE pnu NOT IN (SELECT pnu FROM edits) \
          UNION ALL \
          SELECT pnu, geometry_wkb, CAST(@srid@ AS INTEGER), geometry_checksum_sha256, 'edit', \
            source_snapshot_id \
          FROM edits WHERE upsert) \
    WHERE @targeted@";

/// The build's code version (`foundation.build-source-sha256`): its rules and SQL, and the shared
/// split it runs by.
#[must_use]
pub fn code_version() -> String {
    let mut hash = Sha256::new();
    for text in [
        RULES_VERSION,
        ROW_DIGEST_SQL,
        EDITION_ROWS_SQL,
        EDIT_STATES_TABLE,
        BUILD_SQL,
    ]
    .into_iter()
    .chain(SHARED_SQL)
    {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

/// The parcels unit's ledgered edits in `ledger` (a table reference `DuckDB` reads), with their
/// fingerprints (what the ledger's own command compares a handoff's edits with).
///
/// # Errors
/// Returns an error when the ledger cannot be read or holds an op neither upsert nor delete.
pub fn read_ledger(
    connection: &Connection,
    ledger: &str,
) -> anyhow::Result<(Vec<LedgerEdit>, BTreeMap<i64, EditFingerprint>)> {
    let mut statement = connection.prepare(&format!(
        "SELECT change_seq, feature_id, op, geometry_srid, geometry_wkb, geometry_checksum_sha256, \
                properties_json, editor, edited_at \
         FROM {ledger} WHERE unit = '{UNIT}'"
    ))?;
    let mut edits = Vec::new();
    let mut fingerprints = BTreeMap::new();
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let change_seq: i64 = row.get(0)?;
        let feature_id: String = row.get(1)?;
        let op: String = row.get(2)?;
        let checksum: Option<String> = row.get(5)?;
        fingerprints.insert(
            change_seq,
            (
                change_seq,
                feature_id.clone(),
                op.clone(),
                checksum.clone().filter(|sum| !sum.is_empty()),
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            ),
        );
        edits.push(LedgerEdit {
            change_seq,
            feature_id,
            op: match op.as_str() {
                "upsert" => Op::Upsert,
                "delete" => Op::Delete,
                other => bail!("edit {change_seq} has op {other}"),
            },
            geometry_srid: row.get(3)?,
            geometry_wkb: row.get(4)?,
            geometry_checksum_sha256: checksum,
        });
    }
    Ok((edits, fingerprints))
}

/// The served parcel boundaries as the shared driver builds them.
///
/// `kind_versions_build` `BOUNDARY_SERVED_BUILD` (root ADR-0615 §7): the edition's rows read a 시도 at a time by its key
/// ranges, the ledger's final states spooled by 시도, and each 시군구 built from both. A run in which
/// only the ledger moved is partial (a fold): it rebuilds the PNUs whose final edit state differs
/// between the two reads of the ledger. A moved edition or parcels pin builds every PNU.
#[derive(Clone, Copy, Debug)]
pub struct ServedBoundaries;

/// The one served boundary lane.
pub const SERVED_BOUNDARIES: ServedBoundaries = ServedBoundaries;

impl KindVersionsStage for ServedBoundaries {
    fn section_column(&self) -> &'static str {
        "geometry_wkb"
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![PARCEL_SOURCE, LEDGER_SOURCE, CODE_CHANGE_SOURCE]
    }

    // The edition is read by its ranges, not compared: any move of it builds every PNU. A moved
    // code change table moves no boundary (the edition's PNUs are the served codes).
    fn whole_table_inputs(&self) -> Vec<&'static str> {
        vec![PARCEL_SOURCE]
    }

    fn code_version(&self) -> String {
        code_version()
    }

    fn row_sets(&self) -> Vec<RowSet> {
        Vec::new()
    }

    fn ranged_reads(&self) -> Vec<RangedRead> {
        vec![RangedRead {
            name: EDITION_ROWS,
            input: PARCEL_SOURCE,
            query: EDITION_ROWS_SQL,
        }]
    }

    // The ledger, once: its edits checked, each edited PNU's final state, spooled by 시도. A final
    // state does not depend on which edited parcels the edition holds (only the count of deletes of
    // absent parcels does, a figure of the ledger's own command).
    fn spool_more(
        &self,
        connection: &Connection,
        source: &Source<'_>,
        _edition: &str,
        suffix: &str,
        dir_text: &str,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        let at = Instant::now();
        let (ledger, _) = read_ledger(connection, &source.of(LEDGER_SOURCE)?)?;
        let ledger = check_ledger(ledger)?;
        let through = ledger.iter().map(|edit| edit.change_seq).max().unwrap_or(0);
        let (states, _) = fold_edits(&ledger, &BTreeSet::new());
        connection.execute_batch(&fill(EDIT_STATES_TABLE, &[("suffix", suffix)]))?;
        {
            let mut appender = connection.appender(&format!("edit_states{suffix}"))?;
            for (pnu, state) in &states {
                match state {
                    Some(edit) => appender.append_row(duckdb::params![
                        pnu,
                        true,
                        edit.geometry_wkb.as_deref(),
                        edit.geometry_checksum_sha256.as_deref(),
                        edit.source_snapshot_id(),
                    ])?,
                    None => appender.append_row(duckdb::params![
                        pnu,
                        false,
                        None::<&[u8]>,
                        None::<&str>,
                        None::<String>,
                    ])?,
                }
            }
            appender.flush()?;
        }
        connection.execute_batch(&format!(
            "CREATE OR REPLACE TABLE {EDITS_THROUGH}{suffix} AS SELECT CAST({through} AS BIGINT) AS change_seq;"
        ))?;
        spool_by_sido(
            connection,
            &format!("SELECT * FROM edit_states{suffix}"),
            dir_text,
            &format!("{EDITS}{suffix}"),
        )?;
        Ok(BTreeMap::from([(LEDGER_SOURCE.to_owned(), elapsed(at))]))
    }

    fn load_more(
        &self,
        connection: &Connection,
        read: &dyn Fn(&str) -> Option<String>,
        suffix: &str,
    ) -> anyhow::Result<u64> {
        let name = format!("{EDITS}{suffix}");
        let body = read(&name).map_or_else(
            || {
                "SELECT CAST(NULL AS VARCHAR) AS pnu, CAST(NULL AS BOOLEAN) AS upsert, \
                 CAST(NULL AS BLOB) AS geometry_wkb, CAST(NULL AS VARCHAR) AS geometry_checksum_sha256, \
                 CAST(NULL AS VARCHAR) AS source_snapshot_id WHERE FALSE"
                    .to_owned()
            },
            |held| {
                format!(
                    "SELECT pnu, upsert, geometry_wkb, geometry_checksum_sha256, source_snapshot_id \
                     FROM {held}"
                )
            },
        );
        connection.execute_batch(&format!("CREATE OR REPLACE TABLE {name} AS {body};"))?;
        let rows: i64 =
            connection.query_row(&format!("SELECT count(*) FROM {name}"), [], |row| {
                row.get(0)
            })?;
        Ok(u64::try_from(rows)?)
    }

    fn pnu_spools(&self) -> Vec<&'static str> {
        vec![EDITS]
    }

    // An upsert serves a parcel the edition may not hold.
    fn more_served_spools(&self) -> Vec<&'static str> {
        vec![EDITS]
    }

    // The Spark job tags no build and records no edition: its source rows carry it.
    fn replaced_edition_sql(&self) -> Option<&'static str> {
        Some(
            "SELECT string_agg(DISTINCT CAST(source_snapshot_id AS VARCHAR), ',') FROM @rows@ \
             WHERE origin = 'source'",
        )
    }

    fn create_built(&self, connection: &Connection) -> anyhow::Result<()> {
        connection.execute_batch(BUILT_TABLE)?;
        Ok(())
    }

    fn build(
        &self,
        connection: &Connection,
        all: bool,
        source_snapshot_id: &str,
        published_at_utc: &str,
    ) -> anyhow::Result<()> {
        ensure!(
            is_plain(source_snapshot_id) && is_plain(published_at_utc),
            "the stamps {source_snapshot_id:?} and {published_at_utc:?} are not plain ids"
        );
        let through: i64 = connection.query_row(
            &format!("SELECT change_seq FROM {EDITS_THROUGH}"),
            [],
            |row| row.get(0),
        )?;
        let targeted = if all {
            "TRUE"
        } else {
            "pnu IN (SELECT pnu FROM targets)"
        };
        connection.execute_batch(&fill(
            BUILD_SQL,
            &[
                ("silver", source_snapshot_id),
                ("through", &through.to_string()),
                ("published", published_at_utc),
                ("digest", ROW_DIGEST_SQL),
                ("srid", &GEOMETRY_SRID.to_string()),
                ("targeted", targeted),
            ],
        ))?;
        Ok(())
    }
}

/// One line of the edit store's handoff for a unit (`export-map-edit-handoff`), as the ledger keeps it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
pub struct HandoffEdit {
    /// The unit.
    pub unit: String,
    /// Its place in the order.
    pub change_seq: i64,
    /// The PNU.
    pub feature_id: String,
    /// `upsert` or `delete`.
    pub op: String,
    /// The geometry as `GeoJSON`, on an upsert.
    pub geometry_geojson: Option<String>,
    /// The geometry as hex WKB, on an upsert.
    pub geometry_wkb_hex: Option<String>,
    /// Its CRS.
    pub geometry_srid: i32,
    /// Its digest, on an upsert.
    pub geometry_checksum_sha256: Option<String>,
    /// The edit's properties, a JSON object.
    pub properties_json: String,
    /// Who edited.
    pub editor: String,
    /// When.
    pub edited_at: String,
}

fn present(value: Option<&String>) -> bool {
    value.is_some_and(|value| !value.is_empty())
}

/// Parses the handoff for the parcels unit, refusing what the ledger contract would refuse
/// (`served_gold_common.read_edit_handoff`), in change order.
///
/// # Errors
/// A line that is not an edit of the parcels unit in EPSG:4326, a repeated or non-positive
/// `change_seq`, an upsert without its geometry or a delete with one, properties that are not an
/// object.
pub fn read_edit_handoff(text: &str) -> anyhow::Result<Vec<HandoffEdit>> {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for (number, line) in text.split('\n').enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let row: HandoffEdit = serde_json::from_str(line)
            .with_context(|| format!("edit handoff line {} is not an edit", number + 1))?;
        ensure!(
            row.unit == UNIT,
            "edit handoff line {} is for unit {}, not {UNIT}",
            number + 1,
            row.unit
        );
        ensure!(
            row.geometry_srid == GEOMETRY_SRID,
            "edit handoff line {} is not in EPSG:{GEOMETRY_SRID}",
            number + 1
        );
        let seq = row.change_seq;
        ensure!(
            seq > 0 && seen.insert(seq),
            "edit handoff line {} repeats or lacks a change_seq",
            number + 1
        );
        ensure!(
            row.op == "upsert" || row.op == "delete",
            "edit {seq} has op {}",
            row.op
        );
        let carries = [
            present(row.geometry_wkb_hex.as_ref()),
            present(row.geometry_geojson.as_ref()),
            present(row.geometry_checksum_sha256.as_ref()),
        ];
        let upsert = row.op == "upsert";
        ensure!(
            if upsert {
                carries.iter().all(|carried| *carried)
            } else {
                !carries.iter().any(|carried| *carried)
            },
            "edit {seq}: an upsert carries its geometry, a delete carries none"
        );
        let properties: serde_json::Value = serde_json::from_str(&row.properties_json)
            .with_context(|| format!("edit {seq} properties are not JSON"))?;
        ensure!(
            properties.is_object(),
            "edit {seq} properties are not an object"
        );
        rows.push(row);
    }
    rows.sort_by_key(|row| row.change_seq);
    Ok(rows)
}

/// What makes two ledger rows the same edit, whichever export carried them
/// (`served_gold_common.edit_fingerprint`).
pub type EditFingerprint = (i64, String, String, Option<String>, String, String, String);

impl HandoffEdit {
    /// Its fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> EditFingerprint {
        (
            self.change_seq,
            self.feature_id.clone(),
            self.op.clone(),
            self.geometry_checksum_sha256
                .clone()
                .filter(|sum| !sum.is_empty()),
            self.properties_json.clone(),
            self.editor.clone(),
            self.edited_at.clone(),
        )
    }

    /// As the ledger's check reads it.
    ///
    /// # Errors
    /// A WKB that is not hex.
    pub fn ledger_edit(&self) -> anyhow::Result<LedgerEdit> {
        let geometry_wkb = match self
            .geometry_wkb_hex
            .as_deref()
            .filter(|hex| !hex.is_empty())
        {
            Some(hex) => Some(
                hex_bytes(hex)
                    .with_context(|| format!("edit {} geometry is not hex WKB", self.change_seq))?,
            ),
            None => None,
        };
        Ok(LedgerEdit {
            change_seq: self.change_seq,
            feature_id: self.feature_id.clone(),
            op: if self.op == "upsert" {
                Op::Upsert
            } else {
                Op::Delete
            },
            geometry_srid: self.geometry_srid,
            geometry_wkb,
            geometry_checksum_sha256: self.geometry_checksum_sha256.clone(),
        })
    }
}

fn hex_bytes(hex: &str) -> anyhow::Result<Vec<u8>> {
    ensure!(hex.len().is_multiple_of(2), "odd length");
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).context("not hex"))
        .collect()
}

/// The handoff's edits the ledger does not hold yet; the same `change_seq` with other content is
/// refused, the ledger being append-only (`served_gold_common.new_ledger_rows`).
///
/// # Errors
/// An edit already ledgered with different content.
pub fn fresh_edits<'a>(
    ledgered: &BTreeMap<i64, EditFingerprint>,
    handoff: &'a [HandoffEdit],
) -> anyhow::Result<Vec<&'a HandoffEdit>> {
    let mut fresh = Vec::new();
    for row in handoff {
        match ledgered.get(&row.change_seq) {
            None => fresh.push(row),
            Some(known) => ensure!(
                *known == row.fingerprint(),
                "edit {} is already ledgered with different content; the ledger is append-only",
                row.change_seq
            ),
        }
    }
    Ok(fresh)
}

#[cfg(test)]
mod tests;
