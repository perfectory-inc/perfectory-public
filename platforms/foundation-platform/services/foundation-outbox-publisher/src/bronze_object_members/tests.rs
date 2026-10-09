use std::{
    collections::HashMap,
    io::{Cursor, Read, Write},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use zip::{write::SimpleFileOptions, CompressionMethod, ZipArchive, ZipWriter};

use super::zip_directory::{self, NameEncoding, Tail, TAIL_BYTES};
use super::{measure, measured_by, ObjectRanges, Outcome, SourceSelector};

/// Objects held in memory, read by exact ranges like R2 answers them.
#[derive(Default)]
struct MemoryObjects(HashMap<String, Vec<u8>>);

#[async_trait]
impl ObjectRanges for MemoryObjects {
    async fn read(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Vec<u8>> {
        let object = self
            .0
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("NoSuchKey {key}"))?;
        anyhow::ensure!(end <= object.len() as u64, "InvalidRange {start}..{end}");
        Ok(object[start as usize..end as usize].to_vec())
    }
}

fn single(key: &str, bytes: Vec<u8>) -> MemoryObjects {
    MemoryObjects(HashMap::from([(key.to_owned(), bytes)]))
}

async fn measure_bytes(bytes: Vec<u8>) -> (Outcome, u32) {
    let size = bytes.len() as u64;
    measure(&single("o.zip", bytes), "o.zip", size).await
}

fn at(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(year, month, day)
        .and_then(|date| date.and_hms_opt(hour, minute, second))
        .unwrap()
}

/// An archive written by the `zip` crate.
fn crate_zip(
    entries: &[(&str, Vec<u8>, CompressionMethod)],
    zip64_comment: Option<String>,
) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let time = zip::DateTime::from_date_and_time(2024, 5, 6, 7, 8, 10).unwrap();
    for (name, body, method) in entries {
        let options = SimpleFileOptions::default()
            .compression_method(*method)
            .last_modified_time(time);
        if name.ends_with('/') {
            writer.add_directory(*name, options).unwrap();
        } else {
            writer.start_file(*name, options).unwrap();
            writer.write_all(body).unwrap();
        }
    }
    writer.set_zip64_comment(zip64_comment);
    writer.finish().unwrap().into_inner()
}

/// What the `zip` crate itself reads from the same bytes: (name, size, compressed, modified).
fn crate_reading(bytes: &[u8]) -> Vec<(String, u64, u64, Option<NaiveDateTime>)> {
    let mut archive = ZipArchive::new(Cursor::new(bytes)).unwrap();
    (0..archive.len())
        .map(|index| {
            let file = archive.by_index_raw(index).unwrap();
            let modified = file.last_modified().map(|time| {
                at(
                    i32::from(time.year()),
                    u32::from(time.month()),
                    u32::from(time.day()),
                    u32::from(time.hour()),
                    u32::from(time.minute()),
                    u32::from(time.second()),
                )
            });
            (
                file.name().to_owned(),
                file.size(),
                file.compressed_size(),
                modified,
            )
        })
        .collect()
}

fn our_reading(outcome: &Outcome) -> Vec<(String, u64, u64, Option<NaiveDateTime>)> {
    let Outcome::Zip(members) = outcome else {
        panic!("expected a ZIP, got {outcome:?}");
    };
    members
        .iter()
        .map(|m| {
            (
                m.name.clone(),
                m.uncompressed_size,
                m.compressed_size,
                m.modified,
            )
        })
        .collect()
}

#[tokio::test]
async fn a_small_zip_is_read_in_one_request_and_agrees_with_the_zip_crate() {
    let bytes = crate_zip(
        &[
            ("table/", Vec::new(), CompressionMethod::Stored),
            (
                "table/first.csv",
                b"a,b\n1,2\n".repeat(50),
                CompressionMethod::Deflated,
            ),
            ("시험자료.csv", b"plain".to_vec(), CompressionMethod::Stored),
        ],
        None,
    );
    let (outcome, requests) = measure_bytes(bytes.clone()).await;
    assert_eq!(requests, 1);
    assert_eq!(our_reading(&outcome), crate_reading(&bytes));
    let Outcome::Zip(members) = outcome else {
        unreachable!()
    };
    assert_eq!(members[2].name_encoding, NameEncoding::Utf8Flagged);
    assert_eq!(members[1].name_encoding, NameEncoding::Utf8Unflagged);
    assert_eq!(members[1].modified, Some(at(2024, 5, 6, 7, 8, 10)));
    assert!(members[1].compressed_size < members[1].uncompressed_size);
    assert_eq!(
        members.iter().map(|m| m.index).collect::<Vec<_>>(),
        [0, 1, 2]
    );
}

#[tokio::test]
async fn a_directory_beyond_the_tail_costs_exactly_one_more_request() {
    let entries: Vec<_> = (0..3_000)
        .map(|n| {
            (
                format!("part/{n:05}-{}.txt", "x".repeat(60)),
                Vec::new(),
                CompressionMethod::Stored,
            )
        })
        .collect();
    let named: Vec<_> = entries
        .iter()
        .map(|(n, b, m)| (n.as_str(), b.clone(), *m))
        .collect();
    let bytes = crate_zip(&named, None);
    assert!(
        bytes.len() as u64 > 2 * TAIL_BYTES,
        "the directory must not fit in the tail"
    );
    let (outcome, requests) = measure_bytes(bytes.clone()).await;
    assert_eq!(requests, 2);
    assert_eq!(our_reading(&outcome), crate_reading(&bytes));
}

#[tokio::test]
async fn a_zip64_end_record_far_before_the_tail_is_followed() {
    // A ZIP64 end record with a 200 KB extensible sector puts the record and the directory
    // before the tail: tail, ZIP64 record, directory.
    let bytes = crate_zip(
        &[("only.csv", b"1\n".to_vec(), CompressionMethod::Stored)],
        Some("z".repeat(200_000)),
    );
    let (outcome, requests) = measure_bytes(bytes.clone()).await;
    assert_eq!(our_reading(&outcome), crate_reading(&bytes));
    assert_eq!(requests, 3);
}

/// One central directory entry for a hand-built archive.
struct Entry {
    name: Vec<u8>,
    flags: u16,
    uncompressed: u64,
    compressed: u64,
    extra: Vec<u8>,
    date: u16,
    time: u16,
}

impl Entry {
    fn named(name: &[u8]) -> Self {
        Self {
            name: name.to_vec(),
            flags: 0,
            uncompressed: 10,
            compressed: 7,
            extra: Vec::new(),
            date: (44 << 9) | (5 << 5) | 6,
            time: (7 << 11) | (8 << 5) | 5,
        }
    }
}

fn extra_block(id: u16, data: &[u8]) -> Vec<u8> {
    let mut block = id.to_le_bytes().to_vec();
    block.extend_from_slice(&u16::try_from(data.len()).unwrap().to_le_bytes());
    block.extend_from_slice(data);
    block
}

/// A ZIP whose local headers are replaced by a few bytes: the directory reader never looks at
/// them. With `zip64`, every 32-bit field of the end record is saturated and the counts live in
/// the ZIP64 end record, as a writer of a multi-gigabyte archive leaves them.
fn handmade(entries: &[Entry], zip64: bool) -> Vec<u8> {
    let mut bytes = b"DATA".to_vec();
    let directory_start = bytes.len() as u64;
    for entry in entries {
        let mut extra = entry.extra.clone();
        let mut sizes = Vec::new();
        let saturate = |value: u64, sizes: &mut Vec<u8>| {
            if value >= u64::from(u32::MAX) {
                sizes.extend_from_slice(&value.to_le_bytes());
                u32::MAX
            } else {
                u32::try_from(value).unwrap()
            }
        };
        let uncompressed = saturate(entry.uncompressed, &mut sizes);
        let compressed = saturate(entry.compressed, &mut sizes);
        if !sizes.is_empty() {
            extra.extend(extra_block(0x0001, &sizes));
        }
        bytes.extend_from_slice(b"PK\x01\x02");
        bytes.extend_from_slice(&[45, 3, 45, 0]);
        bytes.extend_from_slice(&entry.flags.to_le_bytes());
        bytes.extend_from_slice(&0_u16.to_le_bytes());
        bytes.extend_from_slice(&entry.time.to_le_bytes());
        bytes.extend_from_slice(&entry.date.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&compressed.to_le_bytes());
        bytes.extend_from_slice(&uncompressed.to_le_bytes());
        bytes.extend_from_slice(&u16::try_from(entry.name.len()).unwrap().to_le_bytes());
        bytes.extend_from_slice(&u16::try_from(extra.len()).unwrap().to_le_bytes());
        // Comment length, disk, internal and external attributes; then the local header offset.
        bytes.extend_from_slice(&[0; 10]);
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&entry.name);
        bytes.extend_from_slice(&extra);
    }
    let directory_size = bytes.len() as u64 - directory_start;
    let count = entries.len() as u64;
    if zip64 {
        let record = bytes.len() as u64;
        bytes.extend_from_slice(b"PK\x06\x06");
        bytes.extend_from_slice(&44_u64.to_le_bytes());
        bytes.extend_from_slice(&[45, 3, 45, 0]);
        bytes.extend_from_slice(&[0; 8]);
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(&directory_size.to_le_bytes());
        bytes.extend_from_slice(&directory_start.to_le_bytes());
        bytes.extend_from_slice(b"PK\x06\x07");
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&record.to_le_bytes());
        bytes.extend_from_slice(&1_u32.to_le_bytes());
    }
    bytes.extend_from_slice(b"PK\x05\x06");
    bytes.extend_from_slice(&[0; 4]);
    let (short_count, size, offset) = if zip64 {
        (u16::MAX, u32::MAX, u32::MAX)
    } else {
        (
            u16::try_from(count).unwrap(),
            u32::try_from(directory_size).unwrap(),
            u32::try_from(directory_start).unwrap(),
        )
    };
    bytes.extend_from_slice(&short_count.to_le_bytes());
    bytes.extend_from_slice(&short_count.to_le_bytes());
    bytes.extend_from_slice(&size.to_le_bytes());
    bytes.extend_from_slice(&offset.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes
}

#[tokio::test]
async fn zip64_member_sizes_come_from_the_extra_field() {
    let mut entry = Entry::named(b"AL_00_D000_20990101.csv");
    entry.uncompressed = 5 * 1024 * 1024 * 1024;
    entry.compressed = 4 * 1024 * 1024 * 1024 + 7;
    let mut small = Entry::named(b"readme.txt");
    small.compressed = 3;
    let (outcome, requests) = measure_bytes(handmade(&[entry, small], true)).await;
    assert_eq!(requests, 1);
    let Outcome::Zip(members) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(members[0].uncompressed_size, 5 * 1024 * 1024 * 1024);
    assert_eq!(members[0].compressed_size, 4 * 1024 * 1024 * 1024 + 7);
    assert_eq!(
        (members[1].uncompressed_size, members[1].compressed_size),
        (10, 3)
    );
    assert_eq!(members[0].modified, Some(at(2024, 5, 6, 7, 8, 10)));
}

#[tokio::test]
async fn a_saturated_size_without_its_zip64_field_is_unreadable() {
    let mut entry = Entry::named(b"big.csv");
    entry.uncompressed = u64::from(u32::MAX);
    let mut bytes = handmade(&[entry], false);
    // Drop the ZIP64 extra block the builder added: name length stays, extra length becomes 0.
    let directory = 4;
    bytes[directory + 30..directory + 32].copy_from_slice(&0_u16.to_le_bytes());
    let (outcome, _) = measure_bytes(bytes).await;
    assert!(matches!(outcome, Outcome::Unreadable(_)), "{outcome:?}");
}

#[tokio::test]
async fn a_korean_name_without_the_flag_is_read_as_cp949() {
    let (raw, _, unmappable) = encoding_rs::EUC_KR.encode("시험자료_합계.csv");
    assert!(!unmappable);
    let (outcome, _) = measure_bytes(handmade(&[Entry::named(&raw)], false)).await;
    let Outcome::Zip(members) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(members[0].name, "시험자료_합계.csv");
    assert_eq!(members[0].name_encoding, NameEncoding::Cp949);
}

#[tokio::test]
async fn a_matching_unicode_path_field_names_the_member() {
    let mut crc = flate2::Crc::new();
    crc.update(b"legacy.csv");
    let mut data = vec![1];
    data.extend_from_slice(&crc.sum().to_le_bytes());
    data.extend_from_slice("새이름.csv".as_bytes());
    let mut matching = Entry::named(b"legacy.csv");
    matching.extra = extra_block(0x7075, &data);
    let mut stale = Entry::named(b"renamed.csv");
    stale.extra = extra_block(0x7075, &data);
    let (outcome, _) = measure_bytes(handmade(&[matching, stale], false)).await;
    let Outcome::Zip(members) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(
        (members[0].name.as_str(), members[0].name_encoding),
        ("새이름.csv", NameEncoding::UnicodePathExtra)
    );
    assert_eq!(
        (members[1].name.as_str(), members[1].name_encoding),
        ("renamed.csv", NameEncoding::Utf8Unflagged)
    );
}

#[tokio::test]
async fn a_zero_dos_date_is_no_date() {
    let mut entry = Entry::named(b"undated.csv");
    entry.date = 0;
    let (outcome, _) = measure_bytes(handmade(&[entry], false)).await;
    let Outcome::Zip(members) = outcome else {
        panic!("{outcome:?}")
    };
    assert_eq!(members[0].modified, None);
}

#[tokio::test]
async fn an_object_that_is_not_a_zip_is_recorded_as_such() {
    let (outcome, requests) = measure_bytes(b"pnu,price\n".repeat(20_000)).await;
    assert!(matches!(outcome, Outcome::NotZip(_)), "{outcome:?}");
    assert_eq!(requests, 1, "only the tail is read, never the whole object");
    let (outcome, requests) = measure_bytes(Vec::new()).await;
    assert!(matches!(outcome, Outcome::NotZip(_)), "{outcome:?}");
    assert_eq!(requests, 0);
    let (outcome, _) = measure_bytes(b"PK\x03\x04".to_vec()).await;
    assert!(matches!(outcome, Outcome::NotZip(_)), "{outcome:?}");
}

#[tokio::test]
async fn a_directory_shorter_than_its_count_is_unreadable() {
    let mut bytes = crate_zip(&[("a.csv", b"1".to_vec(), CompressionMethod::Stored)], None);
    let end = bytes.len() - 22;
    assert_eq!(&bytes[end..end + 4], b"PK\x05\x06");
    for field in [end + 8, end + 10] {
        bytes[field..field + 2].copy_from_slice(&2_u16.to_le_bytes());
    }
    let (outcome, _) = measure_bytes(bytes).await;
    assert!(matches!(outcome, Outcome::Unreadable(_)), "{outcome:?}");
}

#[tokio::test]
async fn an_unreadable_object_fails_rather_than_settles() {
    let bytes = crate_zip(&[("a.csv", b"1".to_vec(), CompressionMethod::Stored)], None);
    let (outcome, _) = measure(&MemoryObjects::default(), "gone.zip", 100).await;
    assert!(matches!(outcome, Outcome::Failed(_)), "{outcome:?}");
    // The ledger says more bytes than the object has.
    let size = bytes.len() as u64 + 10;
    let (outcome, _) = measure(&single("o.zip", bytes), "o.zip", size).await;
    assert!(matches!(outcome, Outcome::Failed(_)), "{outcome:?}");
}

#[test]
fn the_tail_window_holds_every_end_record() {
    assert_eq!(zip_directory::tail_range(10), (0, 10));
    assert_eq!(
        zip_directory::tail_range(TAIL_BYTES * 3),
        (TAIL_BYTES * 2, TAIL_BYTES * 3)
    );
}

#[test]
fn a_zip64_pointer_outside_the_bytes_read_asks_for_the_record() {
    let bytes = handmade(&[Entry::named(b"a.csv")], true);
    let size = bytes.len() as u64;
    // Read only the locator and end record: the ZIP64 record is before them.
    let tail_start = size - 42;
    let verdict = zip_directory::read_tail(&bytes[tail_start as usize..], tail_start, size);
    let Tail::Zip64EndAt { offset } = verdict else {
        panic!("{verdict:?}")
    };
    let record = &bytes[offset as usize..offset as usize + 56];
    assert!(matches!(
        zip_directory::read_zip64_end(record, offset),
        Tail::Directory(directory) if directory.entries == 1 && directory.start == 4
    ));
}

#[test]
fn sources_are_slugs_or_prefixes() {
    let selector =
        SourceSelector::parse("vworldkr__land*, hubgokr__building_register_main").unwrap();
    assert_eq!(selector.prefixes, ["vworldkr__land"]);
    assert_eq!(selector.exact, ["hubgokr__building_register_main"]);
    assert_eq!(SourceSelector::parse("*").unwrap().prefixes, [""]);
    for bad in ["", "a,,b", "Upper", "a*b", "a%", "**"] {
        assert!(
            SourceSelector::parse(bad).is_err(),
            "{bad:?} must be refused"
        );
    }
}

#[test]
fn a_row_names_the_reader_and_the_release() {
    assert_eq!(measured_by(None), "measure-bronze-object-members/v1");
    let release = "0123456789abcdef0123456789abcdef01234567";
    assert_eq!(
        measured_by(Some(release)),
        format!("measure-bronze-object-members/v1@{release}")
    );
}

/// Keeps the in-test archive reader honest: the bytes the crate wrote are a ZIP it can open.
#[test]
fn the_crate_reads_back_its_own_zip64_archive() {
    let bytes = crate_zip(
        &[("only.csv", b"1\n".to_vec(), CompressionMethod::Stored)],
        Some("z".repeat(10)),
    );
    let mut archive = ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut body = String::new();
    archive
        .by_index(0)
        .unwrap()
        .read_to_string(&mut body)
        .unwrap();
    assert_eq!(body, "1\n");
}

mod postgres {
    use super::*;
    use crate::bronze_object_members::{measured_by, record, run_with, Recorded, RunConfig};
    use foundation_disposable_database::{run_in_disposable_database, TestResult};
    use sqlx::PgPool;
    use uuid::Uuid;

    async fn source(pool: &PgPool, slug: &str) -> TestResult<(Uuid, Uuid)> {
        let (source, run) = (Uuid::now_v7(), Uuid::now_v7());
        sqlx::query(
            "INSERT INTO catalog.source_catalog
                (id, slug, name, provider, dataset_name, auth_kind, payload_format)
             VALUES ($1, $2, $2, 'example.test', 'archive', 'none', 'zip')",
        )
        .bind(source)
        .bind(slug)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.ingestion_run (id, source_catalog_id, trigger, status)
             VALUES ($1, $2, 'test', 'succeeded')",
        )
        .bind(run)
        .bind(source)
        .execute(pool)
        .await?;
        Ok((source, run))
    }

    async fn object(
        pool: &PgPool,
        (source, run): (Uuid, Uuid),
        key: &str,
        size: usize,
    ) -> TestResult<Uuid> {
        let id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO catalog.bronze_object
                (id, source_catalog_id, ingestion_run_id, dedupe_key, object_key, checksum_sha256,
                 content_type, size_bytes, source_identity_key, snapshot_date, snapshot_granularity,
                 snapshot_basis)
             VALUES ($1, $2, $3, $4, $4, repeat('0', 64), 'application/zip', $5, $4,
                     DATE '2099-01-01', 'day', 'collected_at_fallback')",
        )
        .bind(id)
        .bind(source)
        .bind(run)
        .bind(key)
        .bind(i64::try_from(size)?)
        .execute(pool)
        .await?;
        Ok(id)
    }

    async fn count(pool: &PgPool, table: &str) -> TestResult<i64> {
        Ok(
            sqlx::query_scalar(&format!("SELECT count(*) FROM catalog.{table}"))
                .fetch_one(pool)
                .await?,
        )
    }

    #[tokio::test]
    #[ignore = "requires an explicitly supplied disposable local PostgreSQL server"]
    async fn measuring_twice_records_once_and_a_failure_is_retried() -> TestResult {
        run_in_disposable_database("bronze_object_members", |pool| async move {
            sqlx::migrate!("../../migrations").run(&pool).await?;
            let selected = source(&pool, "testsrc__archive").await?;
            let other = source(&pool, "othersrc__archive").await?;
            let archive = crate_zip(
                &[
                    ("a.csv", b"1".to_vec(), CompressionMethod::Stored),
                    ("b.csv", b"2".to_vec(), CompressionMethod::Deflated),
                ],
                None,
            );
            let text = b"not a zip".to_vec();
            let zip_id = object(&pool, selected, "bronze/a.zip", archive.len()).await?;
            object(&pool, selected, "bronze/text.zip", text.len()).await?;
            let gone = object(&pool, selected, "bronze/gone.zip", 100).await?;
            object(&pool, selected, "bronze/plain.csv", 5).await?;
            object(&pool, other, "bronze/other.zip", archive.len()).await?;
            let objects = Arc::new(MemoryObjects(HashMap::from([
                ("bronze/a.zip".to_owned(), archive.clone()),
                ("bronze/text.zip".to_owned(), text),
                ("bronze/plain.csv".to_owned(), b"x,y\n1".to_vec()),
                ("bronze/other.zip".to_owned(), archive),
            ])));
            let mut config = RunConfig {
                sources: SourceSelector::parse("testsrc__*")?,
                limit: 100,
                concurrency: 4,
                dry_run: true,
                measured_by: measured_by(None),
            };

            let dry = run_with(&pool, objects.clone(), &config).await?;
            assert_eq!(
                (dry.selected, dry.measured, dry.members, dry.failed),
                (3, 2, 2, 1)
            );
            assert_eq!(count(&pool, "bronze_object_measurement").await?, 0);

            config.dry_run = false;
            let first = run_with(&pool, objects.clone(), &config).await?;
            assert_eq!(
                (first.zip, first.not_zip, first.members, first.failed),
                (1, 1, 2, 1)
            );
            assert_eq!(count(&pool, "bronze_object_member").await?, 2);

            let second = run_with(&pool, objects.clone(), &config).await?;
            assert_eq!((second.selected, second.measured, second.failed), (1, 0, 1));
            assert_eq!(count(&pool, "bronze_object_member").await?, 2);
            let attempts: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM catalog.bronze_object_measurement
                  WHERE bronze_object_id = $1 AND outcome = 'failed'",
            )
            .bind(gone)
            .fetch_one(&pool)
            .await?;
            assert_eq!(attempts, 2, "each failed attempt stays as history");

            let late = record(&pool, zip_id, &Outcome::NotZip("late".to_owned()), "t").await?;
            assert_eq!(late, Recorded::AlreadySettled);

            for statement in [
                "UPDATE catalog.bronze_object_measurement SET detail = 'x'",
                "DELETE FROM catalog.bronze_object_member",
                "TRUNCATE catalog.bronze_object_member",
            ] {
                assert!(
                    sqlx::query(statement).execute(&pool).await.is_err(),
                    "{statement}"
                );
            }
            let short = sqlx::query(
                "INSERT INTO catalog.bronze_object_measurement
                    (id, bronze_object_id, outcome, member_count, measured_by)
                 VALUES ($1, $2, 'zip', 3, 't')",
            )
            .bind(Uuid::now_v7())
            .bind(gone)
            .execute(&pool)
            .await;
            assert!(
                short.is_err(),
                "a ZIP measurement without its members is refused at commit"
            );
            Ok(())
        })
        .await
    }
}
