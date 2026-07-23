use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;

#[derive(Debug)]
pub struct CapabilityLayoutReport {
    pub business_crates: usize,
}

pub fn check_capability_layout(repo_root: &Path) -> Result<CapabilityLayoutReport, String> {
    let forbidden = ["crates/domain", "crates/operations"];
    let present = forbidden
        .iter()
        .filter(|relative| repo_root.join(relative).exists())
        .copied()
        .collect::<Vec<_>>();

    if !present.is_empty() {
        return Err(format!(
            "business crates must use crates/<capability>-<layer>; forbidden grouping directories: {}",
            present.join(", ")
        ));
    }

    let crates_root = repo_root.join("crates");
    let entries = fs::read_dir(&crates_root)
        .map_err(|error| format!("failed to list {}: {error}", crates_root.display()))?;
    let mut business_crates = 0;
    let mut nested_manifests = Vec::new();
    for entry_result in entries {
        let entry = entry_result
            .map_err(|error| format!("failed to read {}: {error}", crates_root.display()))?;
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to inspect {}: {error}", entry.path().display()))?;
        if !file_type.is_dir() {
            continue;
        }

        let name = entry.file_name();
        let name = name.to_string_lossy();
        if entry.path().join("Cargo.toml").is_file()
            && (name.ends_with("-domain") || name == "shared-kernel")
        {
            business_crates += 1;
        }
        collect_nested_manifests(&entry.path(), 0, &mut nested_manifests)?;
    }

    if let Some(path) = nested_manifests.first() {
        let relative = path.strip_prefix(repo_root).unwrap_or(path);
        return Err(format!(
            "workspace crates must be direct children of crates: {}",
            relative.display().to_string().replace('\\', "/")
        ));
    }

    check_product_ownership_contract(repo_root, &crates_root)?;

    Ok(CapabilityLayoutReport { business_crates })
}

fn check_product_ownership_contract(repo_root: &Path, crates_root: &Path) -> Result<(), String> {
    let contract_path = repo_root.join("docs/architecture/foundation-platform-boundary.v1.json");
    let contract_text = fs::read_to_string(&contract_path)
        .map_err(|error| format!("failed to read {}: {error}", contract_path.display()))?;
    let contract: Value = serde_json::from_str(&contract_text)
        .map_err(|error| format!("failed to parse {}: {error}", contract_path.display()))?;
    let ownership = contract
        .get("path_ownership")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{} is missing path_ownership", contract_path.display()))?;

    let mut declared_domains = BTreeSet::new();
    for entry in ownership {
        if entry.get("owner").and_then(Value::as_str) != Some("gongzzang")
            || entry.get("classification").and_then(Value::as_str) != Some("product_domain")
        {
            continue;
        }
        let path = entry
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| "Gongzzang product_domain entry is missing path".to_string())?;
        if path.starts_with("crates/") && !declared_domains.insert(path.to_owned()) {
            return Err(format!(
                "Gongzzang product-domain ownership is duplicated: {path}"
            ));
        }
    }

    let mut actual_domains = BTreeSet::new();
    let entries = fs::read_dir(crates_root)
        .map_err(|error| format!("failed to list {}: {error}", crates_root.display()))?;
    for entry_result in entries {
        let entry = entry_result
            .map_err(|error| format!("failed to read {}: {error}", crates_root.display()))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.path().join("Cargo.toml").is_file() && name.ends_with("-domain") {
            actual_domains.insert(format!("crates/{name}"));
        }
    }

    let missing = actual_domains
        .difference(&declared_domains)
        .cloned()
        .collect::<Vec<_>>();
    let stale = declared_domains
        .difference(&actual_domains)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() || !stale.is_empty() {
        return Err(format!(
            "Gongzzang product-domain ownership drift: missing=[{}] stale=[{}]",
            missing.join(", "),
            stale.join(", ")
        ));
    }

    let shared_kernel_is_declared = ownership.iter().any(|entry| {
        entry.get("path").and_then(Value::as_str) == Some("crates/shared-kernel")
            && entry.get("owner").and_then(Value::as_str) == Some("gongzzang")
            && entry.get("classification").and_then(Value::as_str) == Some("product_shared_kernel")
    });
    if crates_root.join("shared-kernel/Cargo.toml").is_file() && !shared_kernel_is_declared {
        return Err(
            "Gongzzang shared-kernel ownership drift: crates/shared-kernel must be declared as product_shared_kernel"
                .to_string(),
        );
    }

    Ok(())
}

fn collect_nested_manifests(
    directory: &Path,
    depth: usize,
    manifests: &mut Vec<PathBuf>,
) -> Result<(), String> {
    let entries = fs::read_dir(directory)
        .map_err(|error| format!("failed to list {}: {error}", directory.display()))?;
    for entry_result in entries {
        let entry = entry_result
            .map_err(|error| format!("failed to read {}: {error}", directory.display()))?;
        let file_type = entry
            .file_type()
            .map_err(|error| format!("failed to inspect {}: {error}", entry.path().display()))?;
        if file_type.is_dir() {
            collect_nested_manifests(&entry.path(), depth + 1, manifests)?;
        } else if depth > 0 && entry.file_name() == "Cargo.toml" {
            manifests.push(entry.path());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::check_capability_layout;

    struct TestRepo {
        root: PathBuf,
    }

    impl TestRepo {
        fn new(name: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock must be after Unix epoch")
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "gongzzang-capability-layout-{name}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&root).expect("test repository must be created");
            Self { root }
        }

        fn create_dir(&self, relative: impl AsRef<Path>) {
            fs::create_dir_all(self.root.join(relative)).expect("test directory must be created");
        }

        fn write_manifest(&self, relative: impl AsRef<Path>) {
            let path = self.root.join(relative);
            fs::create_dir_all(path.parent().expect("manifest must have a parent"))
                .expect("manifest parent must be created");
            fs::write(path, "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\n")
                .expect("test manifest must be written");
        }

        fn write_boundary_contract(&self, entries: &[(&str, &str)]) {
            let path = self
                .root
                .join("docs/architecture/foundation-platform-boundary.v1.json");
            fs::create_dir_all(path.parent().expect("contract must have a parent"))
                .expect("contract parent must be created");
            let path_ownership = entries
                .iter()
                .map(|(path, classification)| {
                    serde_json::json!({
                        "path": path,
                        "owner": "gongzzang",
                        "classification": classification,
                    })
                })
                .collect::<Vec<_>>();
            fs::write(
                path,
                serde_json::to_vec(&serde_json::json!({ "path_ownership": path_ownership }))
                    .expect("contract must serialize"),
            )
            .expect("contract must be written");
        }
    }

    impl Drop for TestRepo {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn rejects_catch_all_domain_layout() {
        let repo = TestRepo::new("domain");
        repo.create_dir("crates/domain/core/listing");

        let error = check_capability_layout(&repo.root)
            .expect_err("catch-all domain directory must be rejected");

        assert!(error.contains("crates/domain"));
    }

    #[test]
    fn rejects_generic_operations_layout() {
        let repo = TestRepo::new("operations");
        repo.create_dir("crates/operations/listing-review");

        let error = check_capability_layout(&repo.root)
            .expect_err("generic operations directory must be rejected");

        assert!(error.contains("crates/operations"));
    }

    #[test]
    fn rejects_arbitrary_nested_business_crate() {
        let repo = TestRepo::new("nested");
        repo.write_manifest("crates/market/listing-domain/Cargo.toml");

        let error = check_capability_layout(&repo.root)
            .expect_err("all crates must be direct children of crates");

        assert!(error.contains("crates/market/listing-domain/Cargo.toml"));
    }

    #[test]
    fn accepts_flat_capability_first_layout() {
        let repo = TestRepo::new("flat");
        repo.create_dir("crates/listing-domain");
        repo.create_dir("crates/listing-review-domain");
        repo.create_dir("crates/shared-kernel");
        repo.write_manifest("crates/listing-domain/Cargo.toml");
        repo.write_manifest("crates/listing-review-domain/Cargo.toml");
        repo.write_manifest("crates/shared-kernel/Cargo.toml");
        repo.write_boundary_contract(&[
            ("crates/listing-domain", "product_domain"),
            ("crates/listing-review-domain", "product_domain"),
            ("crates/shared-kernel", "product_shared_kernel"),
        ]);

        let report =
            check_capability_layout(&repo.root).expect("capability-first layout must be accepted");

        assert_eq!(report.business_crates, 3);
    }

    #[test]
    fn rejects_product_domain_missing_from_ownership_contract() {
        let repo = TestRepo::new("missing-ownership");
        repo.write_manifest("crates/listing-domain/Cargo.toml");
        repo.write_boundary_contract(&[]);

        let error = check_capability_layout(&repo.root)
            .expect_err("every product domain must be in the ownership contract");

        assert!(error.contains("missing=[crates/listing-domain]"));
    }

    #[test]
    fn rejects_stale_product_domain_in_ownership_contract() {
        let repo = TestRepo::new("stale-ownership");
        repo.create_dir("crates");
        repo.write_boundary_contract(&[("crates/removed-domain", "product_domain")]);

        let error = check_capability_layout(&repo.root)
            .expect_err("removed product domains must leave the ownership contract");

        assert!(error.contains("stale=[crates/removed-domain]"));
    }
}
