from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

import workspace_build_cache as cache

OLD_NS = 1_600_000_000 * 1_000_000_000


def git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), "-c", "user.name=ci", "-c", "user.email=ci@example.invalid", *args],
        check=True,
        capture_output=True,
    )


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def touch_all_now(repo: Path) -> None:
    """What a fresh checkout does to every tracked file."""
    for path in repo.rglob("*"):
        if ".git" not in path.parts:
            os.utime(path)


class WorkspaceBuildCacheTest(unittest.TestCase):
    def setUp(self) -> None:
        self.scratch = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.scratch, ignore_errors=True)

    def test_unchanged_sources_get_back_the_mtime_their_artifacts_saw(self) -> None:
        repo = self.scratch / "repo"
        write(repo / "app/src/lib.rs", "pub fn a() {}\n")
        write(repo / "app/src/changed.rs", "pub fn b() {}\n")
        git(repo, "init", "-q")
        git(repo, "add", ".")
        git(repo, "commit", "-qm", "base")
        for path in ("app/src/lib.rs", "app/src/changed.rs"):
            os.utime(repo / path, ns=(OLD_NS, OLD_NS))

        write(repo / "app/target/debug/deps/libdependency-1.rlib", "dependency")
        snapshot_file = self.scratch / "snapshot.txt"
        cache.snapshot(repo, ["app/target"], snapshot_file)
        write(repo / "app/target/debug/deps/libapp-2.rlib", "workspace")
        write(repo / "app/target/debug/.fingerprint/app-2/lib-app", "fingerprint")
        write(repo / "app/target/debug/app", "uplifted")

        stash_dir = self.scratch / "stash"
        self.assertEqual(cache.stash(repo, ["app/target"], snapshot_file, stash_dir), 0)
        self.assertFalse((repo / "app/target/debug/deps/libapp-2.rlib").exists())
        self.assertTrue((repo / "app/target/debug/deps/libdependency-1.rlib").exists())
        self.assertFalse((stash_dir / cache.ARTIFACTS / "app/target/debug/deps/libdependency-1.rlib").exists())
        self.assertFalse((stash_dir / cache.ARTIFACTS / "app/target/debug/app").exists())

        # The next run: a new checkout with one file edited and one added.
        shutil.rmtree(repo / "app/target")
        write(repo / "app/src/changed.rs", "pub fn b() { let _ = 1; }\n")
        write(repo / "app/src/added.rs", "pub fn c() {}\n")
        git(repo, "add", ".")
        touch_all_now(repo)

        stats = cache.restore(repo, stash_dir)

        self.assertEqual(stats, {"artifacts": 2, "unchanged": 1, "changed": 2})
        self.assertTrue((repo / "app/target/debug/deps/libapp-2.rlib").exists())
        self.assertEqual((repo / "app/src/lib.rs").stat().st_mtime_ns, OLD_NS)
        self.assertGreater((repo / "app/src/changed.rs").stat().st_mtime_ns, OLD_NS)
        self.assertGreater((repo / "app/src/added.rs").stat().st_mtime_ns, OLD_NS)

    def test_a_file_the_job_edited_is_not_recorded_as_unchanged(self) -> None:
        repo = self.scratch / "repo"
        write(repo / "src/lib.rs", "pub fn a() {}\n")
        git(repo, "init", "-q")
        git(repo, "add", ".")
        git(repo, "commit", "-qm", "base")
        write(repo / "src/lib.rs", "pub fn a() { edited }\n")
        self.assertNotIn("src/lib.rs", cache.tracked_blobs(repo))

    def test_restore_without_a_stash_changes_nothing(self) -> None:
        self.assertEqual(
            cache.restore(self.scratch, self.scratch / "missing"),
            {"artifacts": 0, "unchanged": 0, "changed": 0},
        )

    def test_cache_pressure_names_usage_our_builds_and_the_next_eviction(self) -> None:
        def fetch(url: str) -> dict:
            if url.endswith("/cache/usage"):
                return {"active_caches_size_in_bytes": 9_500_000_000, "active_caches_count": 30}
            if "sort=last_accessed_at" in url:
                return {"actions_caches": [{"key": "idle-key", "last_accessed_at": "2026-10-01T00:00:00Z"}]}
            self.assertIn("key=prefix-", url)
            return {"actions_caches": [{"size_in_bytes": 300_000_000}, {"size_in_bytes": 200_000_000}]}

        lines = cache.cache_pressure(fetch, "owner/repo", "prefix-")
        self.assertIn("9500 MB of 10000 MB (95%) in 30 entries; 2 stashed workspace builds", lines[0])
        self.assertIn("hold 500 MB", lines[0])
        self.assertIn("`idle-key` (last used 2026-10-01T00:00)", lines[1])
        self.assertTrue(lines[2].startswith("::warning::"))

        def calm(url: str) -> dict:
            if url.endswith("/cache/usage"):
                return {"active_caches_size_in_bytes": 1_000_000_000, "active_caches_count": 3}
            return {"actions_caches": []}

        self.assertEqual(len(cache.cache_pressure(calm, "owner/repo", "prefix-")), 2)

    def cargo_after_change(self, change) -> str:
        """Build a workspace, stash it, check it out anew with `change`, rebuild."""
        repo = self.scratch / "repo"
        members = ("kept", "edited", "migrated")
        write(repo / "Cargo.toml", '[workspace]\nmembers = ["kept", "edited", "migrated"]\nresolver = "2"\n')
        for name in members:
            write(
                repo / name / "Cargo.toml",
                f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2021"\n',
            )
            write(repo / name / "src/lib.rs", "pub fn f() -> u8 { 1 }\n")
        # The shape of every package that embeds the migration set.
        write(repo / "migrated/build.rs", 'fn main() { println!("cargo:rerun-if-changed=../migrations"); }\n')
        write(repo / "migrations/1_a.sql", "select 1;\n")
        write(repo / "migrations/2_b.sql", "select 2;\n")
        write(repo / ".gitignore", "/target\n")
        git(repo, "init", "-q")
        git(repo, "add", ".")
        git(repo, "commit", "-qm", "base")
        environment = {
            **os.environ,
            "CARGO_INCREMENTAL": "0",
            "CARGO_TARGET_DIR": str(repo / "target"),
            "CARGO_TERM_COLOR": "never",
        }

        def build() -> str:
            result = subprocess.run(
                ["cargo", "build", "--offline", "-v"],
                cwd=repo,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )
            return result.stderr

        snapshot_file = self.scratch / "snapshot.txt"
        cache.snapshot(repo, ["target"], snapshot_file)
        build()
        stash_dir = self.scratch / "stash"
        self.assertEqual(cache.stash(repo, ["target"], snapshot_file, stash_dir), 0)

        # A later run on another runner: a new checkout, after the stash was built.
        time.sleep(1.1)
        shutil.rmtree(repo / "target")
        change(repo)
        git(repo, "add", "-A")
        touch_all_now(repo)
        cache.restore(repo, stash_dir)
        return build()

    @unittest.skipUnless(shutil.which("cargo") or os.environ.get("CI"), "cargo is not on PATH")
    def test_cargo_rebuilds_only_the_crate_whose_source_changed(self) -> None:
        """The premise itself: cargo trusts restored mtimes and still sees an edit."""
        log = self.cargo_after_change(lambda repo: write(repo / "edited/src/lib.rs", "pub fn f() -> u8 { 2 }\n"))
        self.assertIn("Fresh kept", log)
        self.assertIn("Fresh migrated", log)
        self.assertIn("Compiling edited", log)

    @unittest.skipUnless(shutil.which("cargo") or os.environ.get("CI"), "cargo is not on PATH")
    def test_an_added_migration_rebuilds_the_package_that_embeds_the_set(self) -> None:
        log = self.cargo_after_change(lambda repo: write(repo / "migrations/3_c.sql", "select 3;\n"))
        self.assertIn("Compiling migrated", log)
        self.assertIn("Fresh kept", log)

    @unittest.skipUnless(shutil.which("cargo") or os.environ.get("CI"), "cargo is not on PATH")
    def test_a_deleted_migration_rebuilds_the_package_that_embeds_the_set(self) -> None:
        log = self.cargo_after_change(lambda repo: (repo / "migrations/2_b.sql").unlink())
        self.assertIn("Compiling migrated", log)
        self.assertIn("Fresh kept", log)


if __name__ == "__main__":
    unittest.main()
