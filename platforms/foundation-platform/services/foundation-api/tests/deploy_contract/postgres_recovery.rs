//! The Postgres recovery contract: compose, pgBackRest repository, restore drill, identity backup,
//! backup scheduler and lock boundary. Moved out of `deploy_contract.rs` unchanged.

use super::*;

#[test]
fn postgres_recovery_contract_is_encrypted_bounded_and_rehearsable() -> TestResult {
    assert_postgres_recovery_compose_contract()?;
    assert_pgbackrest_repository_contract()?;
    assert_postgres_restore_drill_contract()?;
    assert_postgres_backup_scheduler_contract()?;
    assert_postgres_recovery_lock_boundary()?;
    Ok(())
}

fn assert_postgres_recovery_compose_contract() -> TestResult {
    let compose = read_area_file("compose.recovery.yml")?;
    for required in [
        "foundation-postgres-recovery:local",
        "dockerfile: infra/postgres/Dockerfile.recovery",
        "pgbackrest --stanza=foundation archive-push %p",
        "archive_mode=on",
        "archive_timeout=60s",
        "foundation-backup:",
        "foundation-restore-drill:",
        "pgdata:/var/lib/postgresql/data:ro",
        "recovery_data:/var/lib/postgresql/data",
        "FOUNDATION_PLATFORM_R2_POSTGRES_RECOVERY_BUCKET",
        "FOUNDATION_RECOVERY_REPOSITORY_CIPHER_PASS",
    ] {
        assert!(
            compose.contains(required),
            "recovery Compose is missing {required}"
        );
    }
    assert!(!compose.contains("${R2_SECRET_ACCESS_KEY"));
    assert!(!compose.contains("${FOUNDATION_RECOVERY_R2_"));
    assert!(compose.contains("FOUNDATION_PLATFORM_POSTGRES_RECOVERY_WRITER_ACCESS_KEY_ID"));
    assert!(compose.contains("FOUNDATION_PLATFORM_POSTGRES_RECOVERY_WRITER_SECRET_ACCESS_KEY"));
    assert!(compose.contains("FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_ACCESS_KEY_ID"));
    assert!(compose.contains("FOUNDATION_PLATFORM_POSTGRES_RECOVERY_READER_SECRET_ACCESS_KEY"));
    assert!(compose.contains("*foundation-recovery-writer-environment"));
    assert!(compose.contains("*foundation-recovery-reader-environment"));
    assert!(!compose.contains("latest"));

    let dockerfile = read_area_file("infra/postgres/Dockerfile.recovery")?;
    assert!(dockerfile.contains("pgbackrest=2.58.0-r0"));
    assert!(dockerfile.contains(
        "COPY --chmod=0755 infra/postgres/recovery-entrypoint.sh /usr/local/bin/foundation-postgres-recovery-entrypoint"
    ));
    assert!(dockerfile
        .lines()
        .filter(|line| line.starts_with("FROM "))
        .all(|line| line.contains("@sha256:")));
    Ok(())
}

fn assert_pgbackrest_repository_contract() -> TestResult {
    let config = read_area_file("infra/postgres/pgbackrest.conf")?;
    for required in [
        "repo1-type=s3",
        "pg1-user=foundation_admin",
        "repo1-s3-uri-style=path",
        "repo1-cipher-type=aes-256-cbc",
        "repo1-retention-full=35",
        "repo1-retention-full-type=time",
        "repo1-bundle=y",
        "repo1-block=y",
        "start-fast=y",
    ] {
        assert!(
            config.contains(required),
            "pgBackRest config is missing {required}"
        );
    }
    assert!(!config.contains("repo1-s3-key="));
    assert!(!config.contains("repo1-cipher-pass="));
    Ok(())
}

fn assert_postgres_restore_drill_contract() -> TestResult {
    let drill = read_area_file("scripts/recovery/postgres-restore-drill.sh")?;
    for required in [
        "set -Eeuo pipefail",
        "trap cleanup EXIT",
        "local exit_code=$?",
        "logs --no-color",
        "failure-compose.log",
        "pgbackrest-backup check",
        "pgbackrest-backup backup --type=full",
        "pgbackrest-restore restore --type=name",
        "pg_create_restore_point",
        "recovery_probe",
        "pitr_smoke",
        "migration_state",
        "read_smoke",
        "evidence.json",
    ] {
        assert!(
            drill.contains(required),
            "restore drill is missing {required}"
        );
    }
    assert!(drill.contains("NOT IN ('pg_catalog', 'information_schema')"));
    assert!(drill.contains("recovery_drill.recovery_probe"));
    assert!(!drill.contains("catalog.recovery_probe"));
    assert!(!drill.contains("--type=time"));
    assert!(drill.contains("up -d --wait postgres"));
    for service in [
        "foundation-bootstrap",
        "foundation-migrate",
        "foundation-runtime-grants",
        "foundation-finalize",
        "foundation-backup",
    ] {
        let invocation = format!("run --rm -T --interactive=false --no-deps {service}");
        assert!(
            drill.contains(&invocation),
            "restore drill must isolate {service}"
        );
    }
    assert!(drill.contains("recovery run id must be 14 UTC digits"));
    assert!(drill.contains("-c \"INSERT INTO recovery_drill.recovery_probe"));
    assert!(!drill.contains("| \"${source_psql[@]}\""));
    assert!(!drill.contains("set -x"));
    Ok(())
}

// The databases login needs (Zitadel's and the identity platform's) had no backup at all until
// 2026-10-12. They are dumped nightly by their own unit, their age is checked apart from that unit
// (a unit that never starts reports nothing), and a weekly drill restores them and logs in to the
// restored Zitadel with its masterkey: the public keys alone come back under any masterkey.
fn assert_identity_backup_contract() -> TestResult {
    let lib = read_area_file("scripts/recovery/identity-backup-lib.sh")?;
    assert!(lib.contains("\"zitadel-platform zitadel-db zitadel\""));
    assert!(lib.contains("\"identity-platform-runtime identity-db identity\""));
    // gpg authenticates what it decrypts; openssl enc's CBC let a cut-off dump decrypt to garbage.
    assert!(lib.contains("--symmetric --cipher-algo AES256"));
    assert!(lib.contains("--passphrase-fd 3"));
    assert!(!lib.contains("openssl enc"));

    let backup = read_area_file("scripts/recovery/run-identity-backup.sh")?;
    let freshness = read_area_file("scripts/recovery/check-identity-backup-freshness.sh")?;
    let drill = read_area_file("scripts/recovery/identity-restore-drill.sh")?;
    for script in [&backup, &freshness, &drill] {
        assert!(script.contains("set -Eeuo pipefail"));
        assert!(script.contains("source \"${script_dir}/identity-backup-lib.sh\""));
        assert!(!script.contains("set -x"));
    }
    assert!(backup.contains("RECOVERY_WRITER_ACCESS_KEY_ID"));
    for reader in [&freshness, &drill] {
        assert!(reader.contains("RECOVERY_READER_ACCESS_KEY_ID"));
        assert!(!reader.contains("RECOVERY_WRITER_"));
    }
    // A cut-off upload never sits under a whole dump's name.
    assert!(backup.contains("partial=\"${key}.partial\""));
    assert!(backup.contains("trap remove_partial ERR"));
    assert!(backup.contains(
        "aws s3 mv --only-show-errors \"s3://${bucket}/${partial}\" \"s3://${bucket}/${key}\""
    ));
    assert!(
        backup.contains("head-object"),
        "an upload is checked, not assumed"
    );
    assert!(freshness.contains("max_age_hours=26 "));
    assert!(drill.contains("--no-owner --no-privileges --exit-on-error"));
    assert!(drill.contains("trap cleanup EXIT"));
    assert!(drill.contains("docker network create --internal"));
    assert!(drill.contains("start --masterkeyFromEnv"));
    assert!(
        drill.contains("/auth/v1/users/me"),
        "the drill must make Zitadel decrypt a live token"
    );
    // Dumps are kept as long as the foundation repository keeps its full backups.
    let config = read_area_file("infra/postgres/pgbackrest.conf")?;
    let retention = config
        .lines()
        .find_map(|line| line.strip_prefix("repo1-retention-full="))
        .ok_or("pgbackrest.conf has no repo1-retention-full")?;
    assert!(
        backup.contains(&format!("retention_days={retention} ")),
        "the identity dumps must expire with the foundation's full backups ({retention} days)"
    );

    let release = read_area_file("scripts/deploy/foundation-release.sh")?;
    for (unit, script) in [
        ("foundation-identity-backup", "run-identity-backup.sh"),
        (
            "foundation-identity-backup-freshness",
            "check-identity-backup-freshness.sh",
        ),
        (
            "foundation-identity-restore-drill",
            "identity-restore-drill.sh",
        ),
    ] {
        let service = read_area_file(&format!("infra/systemd/{unit}.service"))?;
        assert!(service.contains(&format!(
            "ExecStart=/opt/foundation-platform/current/scripts/recovery/{script}"
        )));
        assert!(service.contains("EnvironmentFile=/etc/foundation-platform/recovery.env"));
        assert!(
            service.contains("OnFailure=foundation-unit-failed@%n.service"),
            "{unit} must report its own failure"
        );
        let timer = read_area_file(&format!("infra/systemd/{unit}.timer"))?;
        assert!(timer.contains(&format!("Unit={unit}.service")));
        assert!(timer.contains("Persistent=true"));
        assert!(
            release.contains(&format!("{unit}.timer")),
            "the deploy must enable {unit}.timer"
        );
    }
    let drill_unit = read_area_file("infra/systemd/foundation-identity-restore-drill.service")?;
    assert!(drill_unit.contains("LoadCredential=zitadel-token:"));
    // Its own unit: a failed foundation backup must not skip it, nor it the foundation backup.
    let foundation = read_area_file("infra/systemd/foundation-postgres-backup.service")?;
    assert!(!foundation.contains("identity"));
    Ok(())
}

fn assert_postgres_backup_scheduler_contract() -> TestResult {
    let backup = read_area_file("scripts/recovery/run-postgres-backup.sh")?;
    for required in [
        "stanza-create",
        "pgbackrest-backup check",
        "backup_type=full",
        "backup_type=diff",
        "pgbackrest-backup expire",
        "backup-info.json",
    ] {
        assert!(
            backup.contains(required),
            "backup runner is missing {required}"
        );
    }
    assert_eq!(
        backup
            .matches("run --rm -T --interactive=false --no-deps foundation-backup")
            .count(),
        7,
        "every scheduled backup operation must attach to the deployed database without starting dependencies"
    );
    assert!(!backup.contains("set -x"));

    let timer = read_area_file("infra/systemd/foundation-postgres-backup.timer")?;
    assert!(timer.contains("OnCalendar=*-*-* 02:15:00"));
    assert!(timer.contains("Persistent=true"));
    assert!(timer.contains("RandomizedDelaySec=15m"));
    let service = read_area_file("infra/systemd/foundation-postgres-backup.service")?;
    assert!(service.contains("EnvironmentFile=/etc/foundation-platform/recovery.env"));
    assert!(service.contains("TimeoutStartSec=6h"));
    assert!(service.contains("WorkingDirectory=/opt/foundation-platform/current"));
    assert!(service.contains(
        "ExecStart=/opt/foundation-platform/current/scripts/recovery/run-postgres-backup.sh"
    ));
    assert!(service.contains("ReadWritePaths=/var/lib/foundation-platform/recovery"));
    assert_identity_backup_contract()?;
    // A failed backup reports itself; the script's own exit code reached nobody for four weeks.
    assert!(service.contains("OnFailure=foundation-unit-failed@%n.service"));
    read_area_file("infra/systemd/foundation-unit-failed@.service")?;

    // pgBackRest follows pg_tblspc, so the backup must see every tablespace the database mounts.
    let database = read_area_file("docker-compose.yml")?;
    let recovery = read_area_file("compose.recovery.yml")?;
    let tablespace_mounts: Vec<&str> = database
        .lines()
        .map(str::trim)
        .filter(|line| line.ends_with(":/tablespaces"))
        .collect();
    assert!(
        !tablespace_mounts.is_empty(),
        "the database mounts no tablespace root"
    );
    for mount in tablespace_mounts {
        assert!(
            recovery.contains(&format!("{mount}:ro")),
            "the backup does not mount the database's tablespace root `{mount}`"
        );
    }

    let release = read_area_file("scripts/deploy/foundation-release.sh")?;
    for required in [
        "FOUNDATION_PLATFORM_RELEASE_ROOT",
        "FOUNDATION_PLATFORM_STATE_ROOT",
        "releases",
        "current",
        "previous",
        "install",
        "activate",
        "rollback",
        ".foundation-release-id",
        ".foundation-release-archive-sha256",
    ] {
        assert!(
            release.contains(required),
            "atomic release script is missing {required}"
        );
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let release_test = Command::new("bash")
        .arg("scripts/deploy/foundation-release-test.sh")
        .current_dir(root)
        .output()?;
    assert!(
        release_test.status.success(),
        "release test failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&release_test.stdout),
        String::from_utf8_lossy(&release_test.stderr)
    );
    assert!(String::from_utf8_lossy(&release_test.stdout).contains("foundation-release-test=pass"));
    Ok(())
}

fn assert_postgres_recovery_lock_boundary() -> TestResult {
    assert_single_bucket_lock_rule(
        "infra/cloudflare/foundation-platform-lakehouse-prod.bucket-lock.json",
        "bronze-raw-30-days",
        "bronze/",
        2_592_000,
    )?;
    let cloudflare_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("infra/cloudflare");
    for forbidden in [
        "foundation-platform-r2-locks.json",
        "foundation-platform-postgres-recovery-prod.bucket-lock.json",
    ] {
        assert!(
            !cloudflare_dir.join(forbidden).exists(),
            "{forbidden} must not lock pgBackRest's mutable repository metadata"
        );
    }

    Ok(())
}
