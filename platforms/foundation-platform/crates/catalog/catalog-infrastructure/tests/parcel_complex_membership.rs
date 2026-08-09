//! Disposable-Postgres proof that membership is a dated fact the database can police (ADR-0019),
//! asserted by a record rather than computed from geometry (ADR-0020).
//!
//! Every constraint this migration installs is asserted by planting a violation and requiring a
//! refusal. A constraint observed only accepting valid rows has not been shown to constrain
//! anything: `parcel_complex_membership_one_complex_excl` is invisible to a test that writes one
//! membership per parcel, because every such row satisfies it.
//!
//! Each refusal is paired with the nearest row the database must still accept — a different parcel,
//! a disjoint interval, every admitted asserter. Without that pair, a constraint that refused
//! everything would pass every assertion here.
//!
//! Most tests run inside a rolled-back transaction against the harness database
//! (`scripts/verify/integration.sh foundation`). The backfill is the exception: it can only be
//! observed on a database that held parcels *before* the migration ran, so it takes a disposable
//! database and applies the migrations in two passes.

#![allow(clippy::expect_used, clippy::too_many_lines, clippy::unwrap_used)]

use std::borrow::Cow;

use catalog_domain::MembershipAssertedBy;
use foundation_disposable_database::{run_in_disposable_database, TestResult};
use sqlx::migrate::{Migration, Migrator};
use sqlx::{Acquire, PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("../../../migrations");

/// Version of the migration under test, spelled once.
const MEMBERSHIP_MIGRATION_VERSION: i64 = 20_260_809_000_001;

/// Connection for this `#[ignore]`d live suite. A missing `DATABASE_URL` aborts the test instead of
/// yielding `None`: these tests run only when a harness asked for them, so an absent database is a
/// provisioning failure — not a reason to report success.
async fn pool() -> Result<PgPool, sqlx::Error> {
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set; run `cargo xtask integration foundation postgres`");
    PgPool::connect(&url).await
}

/// Two parcels, two complexes, and the lineage a membership row needs.
///
/// The second parcel exists so the one-complex exclusion can be shown to key on `parcel_id`: the
/// row it must still accept is the same complex over the same interval for a *different* parcel.
///
/// The snapshot spelling is carried rather than restated at each call site: the migration's
/// `parcel_complex_membership_revision_snapshot_guard` refuses a row whose `source_snapshot_id`
/// differs from its revision's, and a second literal is exactly the drift that guard exists for.
// Every field is named for the column it is bound to, and every one of those columns is an id.
// Dropping the suffix to satisfy `struct_field_names` would put a second spelling between the
// fixture and the row it writes, which is the failure mode this whole file exists to rule out.
#[allow(clippy::struct_field_names)]
struct Fixture {
    parcel_id: Uuid,
    other_parcel_id: Uuid,
    complex_id: Uuid,
    other_complex_id: Uuid,
    revision_id: Uuid,
    source_record_id: Uuid,
    source_snapshot_id: String,
}

impl Fixture {
    async fn insert(tx: &mut Transaction<'_, Postgres>) -> Result<Self, sqlx::Error> {
        let parcel_id = Uuid::new_v4();
        let other_parcel_id = Uuid::new_v4();
        let complex_id = Uuid::new_v4();
        let other_complex_id = Uuid::new_v4();
        let revision_id = Uuid::new_v4();
        let source_record_id = Uuid::new_v4();
        let source_snapshot_id = format!("iceberg:membership-{}", revision_id.simple());

        for (id, label) in [
            (complex_id, "Membership fixture"),
            (other_complex_id, "Membership fixture neighbour"),
        ] {
            sqlx::query(
                "INSERT INTO catalog.industrial_complex
                 (id, official_complex_code, name, kind, primary_bjdong_code, area_m2, version)
                 VALUES ($1, $2, $3, 'general', '9999900101', 1, 1)",
            )
            .bind(id)
            .bind(format!("IC-MEMBERSHIP-{id}"))
            .bind(label)
            .execute(&mut **tx)
            .await?;
        }
        for (id, pnu) in [
            (parcel_id, "9999900101100010001"),
            (other_parcel_id, "9999900101100010002"),
        ] {
            sqlx::query(
                "INSERT INTO catalog.parcel (id, complex_id, pnu, kind, area_m2, version)
                 VALUES ($1, $2, $3, 'factory', 1, 1)",
            )
            .bind(id)
            .bind(complex_id)
            .bind(pnu)
            .execute(&mut **tx)
            .await?;
        }
        sqlx::query(
            "INSERT INTO catalog.source_record (id, source, external_id, checksum_sha256)
             VALUES ($1, 'test', $2, repeat('a', 64))",
        )
        .bind(source_record_id)
        .bind(format!("membership-{source_record_id}"))
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "INSERT INTO catalog.administrative_boundary_revision
             (id, canonical_iceberg_snapshot_id, source_snapshot_id, source_record_id, status,
              validated_at)
             VALUES ($1, '7001', $2, $3, 'validated', now())",
        )
        .bind(revision_id)
        .bind(&source_snapshot_id)
        .bind(source_record_id)
        .execute(&mut **tx)
        .await?;

        Ok(Self {
            parcel_id,
            other_parcel_id,
            complex_id,
            other_complex_id,
            revision_id,
            source_record_id,
            source_snapshot_id,
        })
    }

    /// Inserts one membership and returns the database's verdict instead of unwrapping it.
    ///
    /// `asserted_by` is `&str` on purpose: some of these tests offer the database a spelling the
    /// enum cannot hold, and a typed parameter would make that case unwritable. `parcel_id` is a
    /// parameter rather than always `self.parcel_id` because the one-complex exclusion can only be
    /// shown to key on the parcel by offering it a second one.
    async fn insert_membership(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        asserted_by: &str,
        period: &str,
        parcel_id: Uuid,
        complex_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO catalog.parcel_complex_membership
             (id, parcel_id, complex_id, asserted_by, effective_period,
              data_revision, source_snapshot_id, source_record_id)
             VALUES ($1, $2, $3, $4, $5::daterange, $6, $7, $8)",
        )
        .bind(Uuid::new_v4())
        .bind(parcel_id)
        .bind(complex_id)
        .bind(asserted_by)
        .bind(period)
        .bind(self.revision_id)
        .bind(&self.source_snapshot_id)
        .bind(self.source_record_id)
        .execute(&mut **tx)
        .await
        .map(|_| ())
    }
}

/// A parcel belongs to at most one complex at a time.
///
/// This is the proposition `catalog.parcel.complex_id NOT NULL` was reaching for, and the only part
/// of it that is true — "at most one", not "exactly one". Whether real government data ever
/// violates it is unverified (ADR-0020 남은 부채 2); the constraint is enforced so that if it does,
/// the load stops under this constraint's name instead of accumulating two silent answers.
///
/// Both memberships below say `official_list`, so nothing about the asserter can explain the
/// refusal: the only difference the database can be reacting to is that one parcel would be in two
/// complexes over one instant.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_parcel_belongs_to_at_most_one_complex_at_a_time() -> TestResult {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let fixture = Fixture::insert(&mut tx).await?;

    fixture
        .insert_membership(
            &mut tx,
            MembershipAssertedBy::OfficialList.as_str(),
            "[2020-01-01,)",
            fixture.parcel_id,
            fixture.complex_id,
        )
        .await?;

    let mut overlap_tx = tx.begin().await?;
    let overlapping = fixture
        .insert_membership(
            &mut overlap_tx,
            MembershipAssertedBy::OfficialList.as_str(),
            "[2024-01-01,)",
            fixture.parcel_id,
            fixture.other_complex_id,
        )
        .await;
    assert!(
        overlapping.is_err(),
        "two overlapping memberships put one parcel in two complexes at once"
    );
    overlap_tx.rollback().await?;

    // The nearest row that must still be accepted: same complex, same interval, different parcel.
    // Without it the assertion above would also hold against a constraint keyed on nothing at all,
    // or one that refused every second membership in the table.
    let mut sibling_tx = tx.begin().await?;
    fixture
        .insert_membership(
            &mut sibling_tx,
            MembershipAssertedBy::OfficialList.as_str(),
            "[2020-01-01,)",
            fixture.other_parcel_id,
            fixture.complex_id,
        )
        .await?;
    sibling_tx.rollback().await?;

    tx.rollback().await?;
    Ok(())
}

/// A designation that ends and another that begins is two facts, not a contradiction.
///
/// Holding membership as an interval is worth nothing if a parcel cannot move between complexes, so
/// `_one_complex_excl` must refuse overlap and nothing more. The first interval is closed before
/// the second opens, and the pair must be accepted.
///
/// This is also the shape ADR-0020 §Decision 4 requires of every fact: the earlier membership is
/// still there afterwards. Nothing was overwritten.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_parcel_can_change_complex_across_disjoint_intervals() -> TestResult {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let fixture = Fixture::insert(&mut tx).await?;

    fixture
        .insert_membership(
            &mut tx,
            MembershipAssertedBy::OfficialList.as_str(),
            "[2020-01-01,2024-01-01)",
            fixture.parcel_id,
            fixture.complex_id,
        )
        .await?;
    fixture
        .insert_membership(
            &mut tx,
            MembershipAssertedBy::OfficialList.as_str(),
            "[2024-01-01,)",
            fixture.parcel_id,
            fixture.other_complex_id,
        )
        .await?;

    let memberships = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
           FROM catalog.parcel_complex_membership
          WHERE parcel_id = $1",
    )
    .bind(fixture.parcel_id)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(memberships, 2, "both dated memberships must survive");

    // The bounds are half-open, so the changeover date belongs to exactly one of them. A closed
    // upper bound would put the parcel in both complexes on 2024-01-01 and the EXCLUDE above would
    // have refused the second insert — which is what makes this an assertion and not decoration.
    let complex_on_changeover = sqlx::query_scalar::<_, Uuid>(
        "SELECT complex_id
           FROM catalog.parcel_complex_membership
          WHERE parcel_id = $1
            AND effective_period @> DATE '2024-01-01'",
    )
    .bind(fixture.parcel_id)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(complex_on_changeover, fixture.other_complex_id);

    // And the day before still answers with the complex it left, which is the whole point of
    // accumulating rather than overwriting.
    let complex_before = sqlx::query_scalar::<_, Uuid>(
        "SELECT complex_id
           FROM catalog.parcel_complex_membership
          WHERE parcel_id = $1
            AND effective_period @> DATE '2023-12-31'",
    )
    .bind(fixture.parcel_id)
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(complex_before, fixture.complex_id);

    tx.rollback().await?;
    Ok(())
}

/// A value outside the vocabulary is refused by the database, not merely absent from an enum.
///
/// `a_database_vocabulary_is_spelled_the_same_way_in_both_languages` proves the two lists agree. It
/// cannot prove the database enforces its list — a CHECK that failed to attach would leave both
/// lists identical and the column unconstrained. Every admitted value is offered too, so a
/// constraint refusing everything could not pass this test either.
///
/// `geometry_overlay` is among the refused spellings on purpose. It was admitted by this column
/// until ADR-0020, and a vocabulary that quietly still accepts it would leave the rule in prose
/// only.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_membership_outside_the_vocabulary_is_refused() -> TestResult {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let fixture = Fixture::insert(&mut tx).await?;

    for asserted_by in [
        "geometry_overlay",
        "guessed",
        "OFFICIAL_LIST",
        "",
        "official_list ",
    ] {
        let mut probe_tx = tx.begin().await?;
        let rejected = fixture
            .insert_membership(
                &mut probe_tx,
                asserted_by,
                "[2020-01-01,)",
                fixture.parcel_id,
                fixture.complex_id,
            )
            .await;
        assert!(
            rejected.is_err(),
            "asserted_by {asserted_by:?} is outside the vocabulary and must be refused"
        );
        probe_tx.rollback().await?;
    }

    for asserted_by in MembershipAssertedBy::ALL {
        let mut probe_tx = tx.begin().await?;
        fixture
            .insert_membership(
                &mut probe_tx,
                asserted_by.as_str(),
                "[2020-01-01,)",
                fixture.parcel_id,
                fixture.complex_id,
            )
            .await?;
        probe_tx.rollback().await?;
    }

    tx.rollback().await?;
    Ok(())
}

/// Membership history is append-only outside the Foundation publisher.
///
/// The same capability boundary `20260727000001` put on the other temporal fact tables. Asserted
/// because a trigger function that exists but was never attached to this table is silent.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn membership_history_is_append_only() -> TestResult {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let fixture = Fixture::insert(&mut tx).await?;
    fixture
        .insert_membership(
            &mut tx,
            MembershipAssertedBy::OfficialList.as_str(),
            "[2020-01-01,)",
            fixture.parcel_id,
            fixture.complex_id,
        )
        .await?;

    let mut update_tx = tx.begin().await?;
    let tampered = sqlx::query(
        "UPDATE catalog.parcel_complex_membership
            SET asserted_by = 'manual_review'
          WHERE parcel_id = $1",
    )
    .bind(fixture.parcel_id)
    .execute(&mut *update_tx)
    .await;
    assert!(
        tampered.is_err(),
        "membership history must be append-only for API sessions"
    );
    update_tx.rollback().await?;

    let mut delete_tx = tx.begin().await?;
    let deleted = sqlx::query("DELETE FROM catalog.parcel_complex_membership WHERE parcel_id = $1")
        .bind(fixture.parcel_id)
        .execute(&mut *delete_tx)
        .await;
    assert!(
        deleted.is_err(),
        "membership history must not be erasable for API sessions"
    );
    delete_tx.rollback().await?;

    tx.rollback().await?;
    Ok(())
}

/// A membership names a registered revision, and cites that revision's snapshot.
///
/// `parcel_complex_membership_revision_snapshot_guard` is the template's trigger; without it a row
/// could name one revision and cite another's snapshot, and the lineage would read as evidence
/// while pointing at two different things.
#[tokio::test]
#[ignore = "requires disposable Postgres with Foundation migrations"]
async fn a_membership_snapshot_must_match_its_revision() -> TestResult {
    let pool = pool().await?;
    let mut tx = pool.begin().await?;
    let fixture = Fixture::insert(&mut tx).await?;

    let mut mismatch_tx = tx.begin().await?;
    let mismatched = sqlx::query(
        "INSERT INTO catalog.parcel_complex_membership
         (id, parcel_id, complex_id, asserted_by, effective_period,
          data_revision, source_snapshot_id, source_record_id)
         VALUES ($1, $2, $3, 'official_list', '[2020-01-01,)'::daterange, $4,
                 'iceberg:some-other-snapshot', $5)",
    )
    .bind(Uuid::new_v4())
    .bind(fixture.parcel_id)
    .bind(fixture.complex_id)
    .bind(fixture.revision_id)
    .bind(fixture.source_record_id)
    .execute(&mut *mismatch_tx)
    .await;
    assert!(
        mismatched.is_err(),
        "a membership citing another revision's snapshot must be refused"
    );
    mismatch_tx.rollback().await?;

    let mut unknown_tx = tx.begin().await?;
    let unknown_revision = sqlx::query(
        "INSERT INTO catalog.parcel_complex_membership
         (id, parcel_id, complex_id, asserted_by, effective_period,
          data_revision, source_snapshot_id, source_record_id)
         VALUES ($1, $2, $3, 'official_list', '[2020-01-01,)'::daterange, $4, $5, $6)",
    )
    .bind(Uuid::new_v4())
    .bind(fixture.parcel_id)
    .bind(fixture.complex_id)
    .bind(Uuid::new_v4())
    .bind(&fixture.source_snapshot_id)
    .bind(fixture.source_record_id)
    .execute(&mut *unknown_tx)
    .await;
    assert!(
        unknown_revision.is_err(),
        "a membership on an unregistered revision must be refused"
    );
    unknown_tx.rollback().await?;

    tx.rollback().await?;
    Ok(())
}

/// The migrations that precede the membership migration.
///
/// A backfill is only observable on a database that held rows before it ran. Applying every
/// migration and inserting afterwards leaves the backfill nothing to find, and such a test passes
/// unchanged against a migration whose `INSERT` was deleted — which is the whole reason this
/// two-pass split exists rather than a single `MIGRATOR.run`.
fn migrations_before_membership() -> Migrator {
    let migrations: Vec<Migration> = MIGRATOR
        .iter()
        .filter(|migration| migration.version < MEMBERSHIP_MIGRATION_VERSION)
        .cloned()
        .collect();
    assert_eq!(
        migrations.len() + 1,
        MIGRATOR.iter().count(),
        "the membership migration must be the last one; a later migration would be skipped by \
         this pass and then applied by the second, in an order the harness never runs"
    );
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: MIGRATOR.ignore_missing,
        locking: MIGRATOR.locking,
        no_tx: MIGRATOR.no_tx,
    }
}

/// A parcel that existed before the migration comes out of it carrying its membership.
///
/// The asserted row is the whole one — kind, method, interval, complex and lineage — because the
/// backfill's failure mode is not an absent row but a plausible one: a `now()` lower bound or a
/// source record invented for the occasion would each leave a row that a mere existence check
/// accepts.
#[tokio::test]
#[ignore = "requires PostgreSQL 17 with permission to create disposable databases"]
async fn a_pre_existing_parcel_is_backfilled_with_its_membership() -> TestResult {
    run_in_disposable_database("parcel_complex_backfill", |pool| async move {
        migrations_before_membership().run(&pool).await?;

        let complex_id = Uuid::new_v4();
        let parcel_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO catalog.industrial_complex
             (id, official_complex_code, name, kind, primary_bjdong_code, area_m2, version)
             VALUES ($1, $2, 'Backfill fixture', 'general', '9999900101', 1, 1)",
        )
        .bind(complex_id)
        .bind(format!("IC-BACKFILL-{complex_id}"))
        .execute(&pool)
        .await?;
        // An explicit `created_at`, because the interval's lower bound is asserted against it and a
        // defaulted `now()` would make that assertion true by construction on both sides.
        sqlx::query(
            "INSERT INTO catalog.parcel (id, complex_id, pnu, kind, area_m2, created_at, version)
             VALUES ($1, $2, '9999900101100010001', 'factory', 1, TIMESTAMPTZ '2021-03-04 05:06:07Z', 1)",
        )
        .bind(parcel_id)
        .bind(complex_id)
        .execute(&pool)
        .await?;

        MIGRATOR.run(&pool).await?;

        let row = sqlx::query(
            "SELECT m.complex_id,
                    m.asserted_by,
                    lower(m.effective_period) AS effective_from,
                    upper_inf(m.effective_period) AS open_ended,
                    sr.source AS lineage_source,
                    sr.external_id AS lineage_external_id
               FROM catalog.parcel_complex_membership AS m
               JOIN catalog.source_record AS sr ON sr.id = m.source_record_id
              WHERE m.parcel_id = $1",
        )
        .bind(parcel_id)
        .fetch_one(&pool)
        .await?;

        assert_eq!(row.try_get::<Uuid, _>("complex_id")?, complex_id);
        assert_eq!(
            row.try_get::<String, _>("asserted_by")?,
            "official_list",
            "the column these rows come from was written from official parcel lists, and no \
             overlay produced them"
        );
        assert_eq!(
            row.try_get::<chrono::NaiveDate, _>("effective_from")?,
            chrono::NaiveDate::from_ymd_opt(2021, 3, 4).expect("date"),
            "the interval starts on the earliest date this repository can prove"
        );
        assert!(row.try_get::<bool, _>("open_ended")?);
        assert_eq!(
            row.try_get::<String, _>("lineage_source")?,
            "foundation.migration"
        );
        assert_eq!(
            row.try_get::<String, _>("lineage_external_id")?,
            format!("legacy:catalog.parcel:{parcel_id}"),
            "the backfill cites the legacy bridge 20260727000001 mints, not an invented source"
        );

        let memberships = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM catalog.parcel_complex_membership WHERE parcel_id = $1",
        )
        .bind(parcel_id)
        .fetch_one(&pool)
        .await?;
        assert_eq!(memberships, 1, "the backfill writes one row per parcel");

        Ok(())
    })
    .await
}
