//! The schema registry (root ADR-0336): `contracts/schema-registry/registry.json`, read as the
//! connection contract is, from the bytes compiled in.
//!
//! A registered subject's document names its subject and major in `schema_version`
//! (`<subject>.v<major>`) and its revision in `schema_revision`; a revision served before the
//! registry keeps its old string as a frozen alias of its major. Every reader here (the bake's
//! patch decision, the serving monitor, the manifest record, the gateway ask before a new major)
//! takes the subject and the major from this file and nothing else, so a revision of the same major
//! changes no reader. `scripts/schema_registry.py check` holds the file to its rules (FULL
//! compatibility within a major, append-only revisions); this module trusts what that check passed.

use std::sync::OnceLock;

use serde::Deserialize;

const REGISTRY: &str = include_str!("../../../../contracts/schema-registry/registry.json");

#[derive(Debug, Deserialize)]
struct Registry {
    schema_version: u32,
    subjects: Vec<Subject>,
}

/// One registered subject.
#[derive(Debug, Deserialize)]
pub struct Subject {
    /// `foundation-platform.<name>`.
    pub subject: String,
    /// The major the producer writes now.
    pub major: u32,
    owner: Owner,
    revisions: Vec<Revision>,
    #[serde(skip)]
    current: Revision,
}

#[derive(Debug, Deserialize)]
struct Owner {
    lane: String,
}

/// One revision of a subject.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Revision {
    pub major: u32,
    pub revision: u32,
    /// What its documents carry in `schema_version`.
    pub schema_version: String,
    /// Served before the registry under its own string, which stays an alias of its major.
    #[serde(default)]
    pub legacy: bool,
}

impl Subject {
    /// The revision the producer writes: the newest of the subject's major (found when the
    /// registry is read, which refuses a major without one).
    #[must_use]
    pub const fn current(&self) -> &Revision {
        &self.current
    }

    /// Every `schema_version` a document of `major` may carry: `<subject>.v<major>`, then the
    /// legacy strings its revisions were served under.
    #[must_use]
    pub fn wire_versions(&self, major: u32) -> Vec<String> {
        let mut found = vec![format!("{}.v{major}", self.subject)];
        for revision in self.revisions.iter().filter(|r| r.major == major) {
            if !found.contains(&revision.schema_version) {
                found.push(revision.schema_version.clone());
            }
        }
        found
    }

    /// Every major the registry lists: what a reader of the subject reads.
    ///
    /// shortcut: one live major per subject until a breaking change; a second major then lists both
    /// until the old one's documents are gone, and the registry gains a way to retire one.
    #[must_use]
    pub fn majors(&self) -> Vec<u32> {
        let mut majors = self.revisions.iter().map(|r| r.major).collect::<Vec<_>>();
        majors.dedup();
        majors
    }

    /// The major a `schema_version` names, when it is one of this subject's.
    #[must_use]
    pub fn major_of(&self, schema_version: &str) -> Option<u32> {
        self.majors().into_iter().find(|major| {
            self.wire_versions(*major)
                .iter()
                .any(|v| v == schema_version)
        })
    }

    /// What the producer writes in `schema_revision`: the current revision, once it is one the
    /// registry made (a legacy revision's documents never carried the field).
    #[must_use]
    pub fn written_revision(&self) -> Option<u32> {
        let current = self.current();
        (!current.legacy).then_some(current.revision)
    }
}

fn registry() -> anyhow::Result<&'static Registry> {
    static PARSED: OnceLock<Result<Registry, String>> = OnceLock::new();
    PARSED
        .get_or_init(|| {
            let mut registry: Registry = serde_json::from_str(REGISTRY)
                .map_err(|error| format!("invalid schema registry: {error}"))?;
            for subject in &mut registry.subjects {
                subject.current = subject
                    .revisions
                    .iter()
                    .rev()
                    .find(|revision| revision.major == subject.major)
                    .cloned()
                    .ok_or_else(|| {
                        format!(
                            "{}: major {} has no revision",
                            subject.subject, subject.major
                        )
                    })?;
            }
            if registry.schema_version != 1 {
                return Err(format!(
                    "the schema registry's schema must be 1, got {}",
                    registry.schema_version
                ));
            }
            Ok(registry)
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!("{error}"))
}

/// The subject a by-PNU lane owns, when the registry lists one.
///
/// # Errors
/// Returns an error when the registry cannot be read.
pub fn subject_of_lane(lane: &str) -> anyhow::Result<Option<&'static Subject>> {
    Ok(registry()?
        .subjects
        .iter()
        .find(|subject| subject.owner.lane == lane))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_public_auction_lane_owns_major_one_with_its_legacy_strings() -> anyhow::Result<()> {
        let subject = subject_of_lane("parcel-public-auction")?
            .ok_or_else(|| anyhow::anyhow!("the registry lists the public auction subject"))?;
        let name = "foundation-platform.parcel_public_auction_by_pnu";
        assert_eq!(subject.subject, name);
        assert_eq!(subject.majors(), [1]);
        assert_eq!(
            subject.wire_versions(1),
            [format!("{name}.v1"), format!("{name}.v2")]
        );
        // 2026-10-29: v2 was an optional field added to v1, so it is major 1, revision 2.
        assert_eq!(subject.major_of(&format!("{name}.v2")), Some(1));
        assert_eq!(subject.major_of(&format!("{name}.v1")), Some(1));
        assert_eq!(subject.major_of(&format!("{name}.v3")), None);
        assert_eq!(subject.current().revision, 2);
        assert_eq!(subject.current().schema_version, format!("{name}.v2"));
        // Revision 2 was served before the registry, so its documents carry no schema_revision.
        assert_eq!(subject.written_revision(), None);
        assert!(subject_of_lane("parcel-zoning")?.is_none());
        Ok(())
    }
}
