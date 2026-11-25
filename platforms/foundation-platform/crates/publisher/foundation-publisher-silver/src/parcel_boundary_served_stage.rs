//! The served parcel boundaries in `DuckDB` inside the publisher.
//!
//! Root ADR-0133 §2, ADR-0559; on the shared Gold versions driver (root ADR-0615 §7).
//!
//! One cadastral edition of `silver.parcel_boundaries` as the source holds it ([`ServedBoundaries`]):
//! parcel geometry takes no staff edits (root ADR-0621 §1), so every served row is the edition's.
//! It was the Spark job `parcel_boundary_served_gold.py`. This module also holds the bake handoff's
//! line and the row digest.

use std::fmt::Write as _;

use anyhow::ensure;
use duckdb::Connection;
use sha2::{Digest, Sha256};

use crate::kind_versions_stage::{
    fill, is_plain, KindVersionsStage, RangedRead, RowSet, CODE_CHANGE_SOURCE, SHARED_SQL,
};

/// The unit the tiles serve the parcels as.
pub const UNIT: &str = "parcels";
/// The parcels' one CRS (the Silver and served contracts declare it).
pub const GEOMETRY_SRID: i32 = 4326;

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
    "parcel-boundary-served v3: the edition as the source holds it, a 시군구 at a time";

/// The cadastral editions the boundaries are served from.
pub const PARCEL_SOURCE: &str = crate::kind_versions_stage::PARCEL_SOURCE;
/// A 시도's open rows of the edition, read by its ranges (never spooled: 39,861,511 geometries).
const EDITION_ROWS: &str = "edition_rows";

/// The edition's open rows in the ranges a 시도's load reads ([`RangedRead`]).
const EDITION_ROWS_SQL: &str = "SELECT pnu, geometry_wkb, geometry_checksum_sha256, \
    CAST(source_snapshot_id AS VARCHAR) AS source_snapshot_id FROM @table@ \
    WHERE source_snapshot_id = '@edition@' AND valid_to_utc IS NULL AND (@range@)";

/// `built`: the versions table's content columns.
const BUILT_TABLE: &str = "CREATE OR REPLACE TABLE built (pnu VARCHAR, geometry_wkb BLOB, \
    geometry_srid INTEGER, geometry_checksum_sha256 VARCHAR, origin VARCHAR, \
    source_snapshot_id VARCHAR, silver_iceberg_snapshot_id VARCHAR, \
    edits_through_change_seq BIGINT, published_at_utc TIMESTAMP, row_digest VARCHAR);";

/// The served rows of the loaded part into `built`: the edition's rows, each from the source and no
/// edit through (`edits_through_change_seq` 0).
const BUILD_SQL: &str = "INSERT INTO built \
    SELECT pnu, geometry_wkb, geometry_srid, geometry_checksum_sha256, origin, source_snapshot_id, \
      '@silver@' AS silver_iceberg_snapshot_id, CAST(0 AS BIGINT) AS edits_through_change_seq, \
      CAST('@published@' AS TIMESTAMP) AS published_at_utc, @digest@ AS row_digest \
    FROM (SELECT pnu, geometry_wkb, CAST(@srid@ AS INTEGER) AS geometry_srid, geometry_checksum_sha256, \
            'source' AS origin, source_snapshot_id \
          FROM edition_rows) \
    WHERE @targeted@";

/// The build's code version (`foundation.build-source-sha256`): its rules and SQL, and the shared
/// split it runs by.
#[must_use]
pub fn code_version() -> String {
    let mut hash = Sha256::new();
    for text in [RULES_VERSION, ROW_DIGEST_SQL, EDITION_ROWS_SQL, BUILD_SQL]
        .into_iter()
        .chain(SHARED_SQL)
    {
        hash.update(text.as_bytes());
        hash.update([0]);
    }
    format!("{:x}", hash.finalize())
}

/// The served parcel boundaries as the shared driver builds them.
///
/// `kind_versions_build` `BOUNDARY_SERVED_BUILD` (root ADR-0615 §7): the edition's rows read a 시도 at
/// a time by its key ranges, and each 시군구 built from them. A moved edition or parcels pin builds
/// every PNU.
#[derive(Clone, Copy, Debug)]
pub struct ServedBoundaries;

/// The one served boundary lane.
pub const SERVED_BOUNDARIES: ServedBoundaries = ServedBoundaries;

impl KindVersionsStage for ServedBoundaries {
    fn section_column(&self) -> &'static str {
        "geometry_wkb"
    }

    fn inputs(&self) -> Vec<&'static str> {
        vec![PARCEL_SOURCE, CODE_CHANGE_SOURCE]
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
        let targeted = if all {
            "TRUE"
        } else {
            "pnu IN (SELECT pnu FROM targets)"
        };
        connection.execute_batch(&fill(
            BUILD_SQL,
            &[
                ("silver", source_snapshot_id),
                ("published", published_at_utc),
                ("digest", ROW_DIGEST_SQL),
                ("srid", &GEOMETRY_SRID.to_string()),
                ("targeted", targeted),
            ],
        ))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
