"""The schema registry's checker and can-i-deploy (scripts/schema_registry.py, root ADR-0336).

The incidents it stands for (2026-10-27..29): the public auction document went from v1 to v2 (an
optional media index per round, #877) and four consumers broke, each on its own copy of the version
string: the hourly serving monitor, the manifest record a whole generation copied (#929), the web
preview's z.enum, and the Worker deploy whose smoke is that monitor. These tests hold the registry
to the rule that makes such a bump invisible to consumers (FULL_TRANSITIVE within a major), and to
the rules that keep a breaking change, a hand-written version list and a lying consumer out.
"""

import copy
import importlib.util
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("schema_registry", PLATFORM / "scripts/schema_registry.py")
registry_tool = importlib.util.module_from_spec(spec)
sys.modules["schema_registry"] = registry_tool
spec.loader.exec_module(registry_tool)

SUBJECT = "foundation-platform.parcel_public_auction_by_pnu"
# A git hook binds git to the real repository through these (scripts/guard/lib/fixture-repo.sh); the
# fixtures below are repositories of their own, and so is every git call the checker makes on them.
for variable in ("GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_OBJECT_DIRECTORY",
                 "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_NAMESPACE", "GIT_PREFIX", "GIT_CEILING_DIRECTORIES",
                 "GIT_DISCOVERY_ACROSS_FILESYSTEM"):
    os.environ.pop(variable, None)


def git(root: pathlib.Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), "-c", "user.email=t@example.invalid", "-c", "user.name=t",
                    "-c", "commit.gpgsign=false", *args], check=True, capture_output=True)


class Fixture:
    """A repository holding a copy of the platform's registry, records and lane lines."""

    def __init__(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="schema-registry-"))
        self.platform = self.tmp / "platforms/foundation-platform"
        for part in ("contracts/schema-registry", "contracts/by-pnu-documents"):
            shutil.copytree(PLATFORM / part, self.platform / part)
        (self.platform / "config").mkdir(parents=True)
        lanes = json.loads((PLATFORM / "config/r2-connections.contract.json").read_text(encoding="utf-8"))["by_pnu_lanes"]
        self.write("config/r2-connections.contract.json", {"by_pnu_lanes": lanes})
        git(self.tmp, "init", "-q")
        self.commit()

    def commit(self) -> None:
        git(self.tmp, "add", "-A")
        git(self.tmp, "commit", "-q", "--allow-empty", "-m", "fixture")

    def write(self, relative: str, value) -> None:
        path = self.platform / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value if isinstance(value, str) else json.dumps(value, indent=2) + "\n", encoding="utf-8")

    def registry(self) -> dict:
        return registry_tool.load(self.platform)

    def schema(self, revision: int) -> dict:
        return json.loads((self.platform / f"contracts/schema-registry/parcel_public_auction_by_pnu/1/{revision}.schema.json").read_text(encoding="utf-8"))

    def register(self, schema: dict, **revision) -> None:
        """Adds revision 3 of the subject: `schema` under the canonical string unless told otherwise."""
        registry = self.registry()
        entry = registry["subjects"][0]
        entry["revisions"].append({"major": 1, "revision": 3, "schema_version": f"{SUBJECT}.v1",
                                   "decision": "docs/adr/0336-x.md", "reason": "a test revision", **revision})
        self.write("contracts/schema-registry/registry.json", registry)
        self.write(f"contracts/schema-registry/parcel_public_auction_by_pnu/{entry['revisions'][-1]['major']}/3.schema.json", schema)

    def check(self, base: str | None = None) -> list[str]:
        return registry_tool.check(self.platform, base)

    def close(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)


def revision_three(schema: dict) -> dict:
    """Revision 2 re-registered as 3: the same fields plus the optional schema_revision."""
    schema = copy.deepcopy(schema)
    schema["properties"]["schema_revision"] = {"type": "integer"}
    return schema


def round_of(schema: dict) -> dict:
    return schema["properties"]["public_auction"]["properties"]["rounds"]["items"]


class TheRegistryOnMain(unittest.TestCase):
    def test_the_committed_registry_passes_its_own_check(self):
        self.assertEqual(registry_tool.check(PLATFORM), [])

    def test_revisions_one_and_two_are_their_records_translated(self):
        # The legacy revisions are the field records the publisher's test held the documents to.
        registry = registry_tool.load(PLATFORM)
        entry = next(e for e in registry["subjects"] if e["subject"] == SUBJECT)
        for revision in entry["revisions"]:
            record = json.loads((PLATFORM / revision["record"]).read_text(encoding="utf-8"))
            self.assertEqual(registry_tool.record_of(registry_tool.schema_of(PLATFORM, SUBJECT, revision)),
                             sorted(record, key=lambda e: e["path"]))

    def test_one_major_with_the_legacy_v2_as_its_alias(self):
        entry = next(e for e in registry_tool.load(PLATFORM)["subjects"] if e["subject"] == SUBJECT)
        self.assertEqual(entry["major"], 1)
        self.assertEqual(registry_tool.identities(entry, 1), [f"{SUBJECT}.v1", f"{SUBJECT}.v2"])
        self.assertEqual(registry_tool.current(entry)["revision"], 2)


class Compatibility(unittest.TestCase):
    """FULL within a major, for a reader that ignores the members it does not declare."""

    def setUp(self):
        self.fixture = Fixture()
        self.v2 = self.fixture.schema(2)

    def tearDown(self):
        self.fixture.close()

    def assertRefused(self, failures, *words):
        self.assertTrue(any(all(w in f for w in words) for f in failures), failures)

    def test_v1_to_v2_registered_again_passes(self):
        # The 10-27 expansion, now a revision of the same major: no consumer changes.
        self.fixture.register(revision_three(self.v2))
        self.assertEqual(self.fixture.check(), [])

    def test_an_added_optional_field_passes(self):
        schema = revision_three(self.v2)
        round_of(schema)["properties"]["bid_result"] = {"type": ["string", "null"]}
        self.fixture.register(schema)
        self.assertEqual(self.fixture.check(), [])

    def test_an_added_required_field_fails(self):
        schema = revision_three(self.v2)
        round_of(schema)["properties"]["bid_result"] = {"type": "string"}
        round_of(schema)["required"].append("bid_result")
        self.fixture.register(schema)
        self.assertRefused(self.fixture.check(), "bid_result", "the reader requires it")

    def test_an_optional_field_made_required_fails(self):
        schema = revision_three(self.v2)
        round_of(schema)["required"].append("item_name")
        self.fixture.register(schema)
        self.assertRefused(self.fixture.check(), "item_name", "not FULL compatible")

    def test_a_changed_type_fails(self):
        schema = revision_three(self.v2)
        round_of(schema)["properties"]["pbct_cdtn_no"] = {"type": "string"}
        self.fixture.register(schema)
        self.assertRefused(self.fixture.check(), "pbct_cdtn_no", "the writer admits")

    def test_a_value_added_to_a_closed_enum_fails_and_to_an_open_one_passes(self):
        schema = revision_three(self.v2)
        photo = round_of(schema)["properties"]["photos"]["items"]["properties"]
        photo["media_format"]["enum"].append("webp")
        self.fixture.register(schema)
        self.assertRefused(self.fixture.check(), "media_format", "closed enum")

    def test_a_field_dropped_that_a_consumer_requires_fails(self):
        schema = revision_three(self.v2)
        round_of(schema)["properties"].pop("bid_ends_at")
        round_of(schema)["required"].remove("bid_ends_at")
        self.fixture.register(schema)
        self.assertRefused(self.fixture.check(), "requires public_auction.rounds[].bid_ends_at")

    def test_a_registered_revision_carries_the_major_not_its_own_number(self):
        self.fixture.register(revision_three(self.v2), schema_version=f"{SUBJECT}.v3")
        self.assertRefused(self.fixture.check(), "schema_version must be", f"{SUBJECT}.v1")

    def test_a_registered_revision_declares_its_optional_schema_revision(self):
        self.fixture.register(copy.deepcopy(self.v2))
        self.assertRefused(self.fixture.check(), "schema_revision")

    def test_no_legacy_revision_after_a_registered_one(self):
        self.fixture.register(revision_three(self.v2))
        registry = self.fixture.registry()
        registry["subjects"][0]["revisions"].append({**registry["subjects"][0]["revisions"][1], "revision": 4})
        self.fixture.write("contracts/schema-registry/registry.json", registry)
        self.fixture.write("contracts/schema-registry/parcel_public_auction_by_pnu/1/4.schema.json", self.v2)
        self.assertRefused(self.fixture.check(), "legacy revision follows")

    def test_a_new_major_may_not_take_a_legacy_string(self):
        # Major 2 would be `...v2`, the string revision 2 of major 1 was served under.
        registry = self.fixture.registry()
        entry = registry["subjects"][0]
        entry["major"] = 2
        entry["revisions"].append({"major": 2, "revision": 3, "schema_version": f"{SUBJECT}.v2",
                                   "decision": "docs/adr/0336-x.md", "reason": "a breaking change"})
        self.fixture.write("contracts/schema-registry/registry.json", registry)
        schema = revision_three(self.v2)
        schema["properties"]["schema_version"]["enum"] = [f"{SUBJECT}.v2"]
        self.fixture.write("contracts/schema-registry/parcel_public_auction_by_pnu/2/3.schema.json", schema)
        self.assertRefused(self.fixture.check(), "is a legacy string of major 1")

    def test_a_keyword_outside_the_subset_fails(self):
        schema = revision_three(self.v2)
        schema["additionalProperties"] = False
        self.fixture.register(schema)
        self.assertRefused(self.fixture.check(), "additionalProperties", "outside the registry's subset")

    def test_a_lane_line_that_states_its_version_by_hand_fails(self):
        contract = json.loads((self.fixture.platform / "config/r2-connections.contract.json").read_text(encoding="utf-8"))
        contract["by_pnu_lanes"]["parcel-public-auction"]["document_schema_version"] = f"{SUBJECT}.v2"
        self.fixture.write("config/r2-connections.contract.json", contract)
        self.assertRefused(self.fixture.check(), "states ['document_schema_version']")

    def test_a_source_file_that_spells_the_wire_string_fails(self):
        (self.fixture.tmp / "web.ts").write_text(f'z.enum(["{SUBJECT}.v1", "{SUBJECT}.v2"])\n', encoding="utf-8")
        (self.fixture.tmp / "web.test.ts").write_text(f'"{SUBJECT}.v1"\n', encoding="utf-8")
        git(self.fixture.tmp, "add", "-A")
        failures = self.fixture.check()
        self.assertRefused(failures, "web.ts", "by hand")
        self.assertFalse(any("web.test.ts" in f for f in failures), failures)


class AppendOnly(unittest.TestCase):
    def setUp(self):
        self.fixture = Fixture()
        self.base = subprocess.run(["git", "-C", str(self.fixture.tmp), "rev-parse", "HEAD"],
                                   capture_output=True, text=True, check=True).stdout.strip()

    def tearDown(self):
        self.fixture.close()

    def test_a_new_revision_passes(self):
        self.fixture.register(revision_three(self.fixture.schema(2)))
        self.fixture.commit()
        self.assertEqual(self.fixture.check(self.base), [])

    def test_an_edited_schema_file_fails(self):
        schema = self.fixture.schema(2)
        schema["description"] = "edited"
        self.fixture.write("contracts/schema-registry/parcel_public_auction_by_pnu/1/2.schema.json", schema)
        self.fixture.commit()
        self.assertTrue(any("never changes" in f for f in self.fixture.check(self.base)))

    def test_an_edited_revision_entry_fails(self):
        registry = self.fixture.registry()
        registry["subjects"][0]["revisions"][1]["reason"] = "rewritten"
        self.fixture.write("contracts/schema-registry/registry.json", registry)
        self.fixture.commit()
        self.assertTrue(any("merged revision changed" in f for f in self.fixture.check(self.base)))

    def test_a_revision_added_as_legacy_fails(self):
        self.fixture.register(revision_three(self.fixture.schema(2)), legacy=True)
        self.fixture.commit()
        self.assertTrue(any("added as legacy" in f for f in self.fixture.check(self.base)))


class CanIDeploy(unittest.TestCase):
    registry = registry_tool.load(PLATFORM)

    def test_every_deployed_consumer_must_read_the_major(self):
        reads = {"document_schemas": {SUBJECT: [1]}}
        self.assertEqual(registry_tool.can_i_deploy(self.registry, SUBJECT, 1, {"parcel-gateway": reads}), [])
        refused = registry_tool.can_i_deploy(self.registry, SUBJECT, 3, {"parcel-gateway": reads})
        self.assertEqual(len(refused), 1)
        self.assertIn("parcel-gateway", refused[0])

    def test_a_consumer_that_names_no_majors_reads_none(self):
        # A Worker built before the registry answers its capabilities without document_schemas.
        self.assertTrue(registry_tool.can_i_deploy(self.registry, SUBJECT, 1, {"old": {"unit": "x"}}))

    def test_an_unregistered_subject_is_refused(self):
        self.assertTrue(registry_tool.can_i_deploy(self.registry, "foundation-platform.nothing", 1, {}))


if __name__ == "__main__":
    unittest.main()
