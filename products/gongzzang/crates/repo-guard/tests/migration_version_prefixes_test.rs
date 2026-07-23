use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use repo_guard::guards::migration_version_prefixes::check_migration_version_prefixes;

struct TestRoot {
    path: PathBuf,
}

impl TestRoot {
    fn create(name: &str) -> Result<Self, Box<dyn Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path =
            std::env::temp_dir().join(format!("repo-guard-{name}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn write_migration(&self, name: &str) -> Result<(), Box<dyn Error>> {
        let migrations = self.path.join("migrations");
        fs::create_dir_all(&migrations)?;
        fs::write(migrations.join(name), "-- test migration\n")?;
        Ok(())
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn format_error(file_name: &str) -> String {
    format!(
        "migration filename must match YYYYMMDDHHMMSS_<snake_case>.sql (ADR-0001 §7): migrations/{file_name}"
    )
}

#[test]
fn valid_fourteen_digit_set_passes() -> Result<(), Box<dyn Error>> {
    let root = TestRoot::create("valid")?;
    root.write_migration("20260719000101_enable_postgis.sql")?;
    root.write_migration("20260719000102_core_tables.sql")?;

    let report = check_migration_version_prefixes(root.path()).map_err(std::io::Error::other)?;

    assert_eq!(2, report.files);
    Ok(())
}

#[test]
fn five_digit_prefix_fails() -> Result<(), Box<dyn Error>> {
    let root = TestRoot::create("five-digit")?;
    root.write_migration("00001_create_user.sql")?;

    let error = check_migration_version_prefixes(root.path()).err();

    assert_eq!(Some(format_error("00001_create_user.sql")), error);
    Ok(())
}

#[test]
fn twelve_digit_prefix_fails() -> Result<(), Box<dyn Error>> {
    let root = TestRoot::create("twelve-digit")?;
    root.write_migration("202607190001_core_tables.sql")?;

    let error = check_migration_version_prefixes(root.path()).err();

    assert_eq!(Some(format_error("202607190001_core_tables.sql")), error);
    Ok(())
}

#[test]
fn uppercase_slug_fails() -> Result<(), Box<dyn Error>> {
    let root = TestRoot::create("uppercase-slug")?;
    root.write_migration("20260719000101_Core_Tables.sql")?;

    let error = check_migration_version_prefixes(root.path()).err();

    assert_eq!(Some(format_error("20260719000101_Core_Tables.sql")), error);
    Ok(())
}

#[test]
fn non_sql_files_are_ignored() -> Result<(), Box<dyn Error>> {
    let root = TestRoot::create("non-sql")?;
    root.write_migration("20260719000101_enable_postgis.sql")?;
    root.write_migration("README.md")?;

    let report = check_migration_version_prefixes(root.path()).map_err(std::io::Error::other)?;

    assert_eq!(1, report.files);
    Ok(())
}

#[test]
fn duplicate_prefix_fails() -> Result<(), Box<dyn Error>> {
    let root = TestRoot::create("duplicate")?;
    root.write_migration("20260719000101_create_user.sql")?;
    root.write_migration("20260719000101_add_listing.sql")?;

    let error = check_migration_version_prefixes(root.path()).err();

    assert_eq!(
        Some(
            "duplicate migration version prefix '20260719000101': migrations/20260719000101_add_listing.sql, migrations/20260719000101_create_user.sql"
                .to_string()
        ),
        error
    );
    Ok(())
}
