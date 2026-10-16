"""One contract for the host environment files every unit and operator run loads (root ADR-0153,
scripts/deploy/runtime_secrets.py, config/runtime-secrets.contract.json).

The repository half runs on a copy of this tree with one defect planted at a time: each is a way
the files, the scripts' needs and the runbooks drifted apart before, and each must be refused. The
host half runs on a fake host root: names only, never values.
"""

import grp
import json
import os
import pathlib
import pwd
import shutil
import sys
import tempfile
import unittest

ORCHESTRATION = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ORCHESTRATION / "dags"))

import job_specs  # noqa: E402

AREA = job_specs.PLATFORM_ROOT
sys.path.insert(0, str(AREA / "scripts/deploy"))
import runtime_secrets  # noqa: E402

CONTRACT = AREA / "config/runtime-secrets.contract.json"
NUMBER_CHANGE = "foundation-parcel-number-change.service"
READER = "FOUNDATION_PLATFORM_LAKEHOUSE_READER_ACCESS_KEY_ID"
READER_SECRET = "FOUNDATION_PLATFORM_LAKEHOUSE_READER_SECRET_ACCESS_KEY"
CATALOG_WRITER = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN"
# Names these credentials went by before root ADR-0247. Spelled in pieces: the credential-names guard
# refuses a retired name written whole anywhere in the tree.
OLD = {
    READER: "FOUNDATION_PLATFORM_" + "R2_LAKEHOUSE_READER_ACCESS_KEY_ID",
    READER_SECRET: "FOUNDATION_PLATFORM_" + "R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY",
    CATALOG_WRITER: "FOUNDATION_PLATFORM_LAKEHOUSE_" + "CATALOG_TOKEN",
}


def plant_renames(contract):
    """A contract whose tokens list the old names above as renamed_from (the rename mechanism's fixture;
    the real contract lists none once a rename is complete)."""
    for token in contract["cloudflare_tokens"]["tokens"]:
        if token["name"] == "foundation-platform-lakehouse-reader-prod":
            token["renamed_from"] = [OLD[READER], OLD[READER_SECRET]]
        if token["name"] == "foundation-platform-lakehouse-catalog-writer-prod":
            token["renamed_from"] = [OLD[CATALOG_WRITER]]
    return contract


class TheRepository(unittest.TestCase):
    def test_the_units_scripts_and_runbooks_agree_with_the_contract(self):
        self.assertEqual(runtime_secrets.check(AREA), [])

    def test_every_unit_is_in_the_contract(self):
        units = {path.name for path in (AREA / "infra/systemd").glob("*.service")}
        declared = {c.key for c in runtime_secrets.load(AREA).consumers if c.kind == "unit"}
        self.assertEqual(units, declared)

    def test_a_unit_that_reads_bronze_back_gets_the_reader_pair_alone(self):
        contract = runtime_secrets.load(AREA)
        reader = contract.groups["lakehouse-reader"]
        self.assertEqual(reader.holds, {READER, READER_SECRET})
        self.assertEqual(contract.tokens["foundation-platform-lakehouse-reader-prod"], [READER, READER_SECRET])
        self.assertEqual(reader.mode, 0o600)
        lines = runtime_secrets.rendered_lines(contract, contract.consumer(NUMBER_CHANGE))
        self.assertEqual(lines[-1], reader.path)


class CloudflareTokens(unittest.TestCase):
    """Root ADR-0247: a credential's variable names are derived from its token's name."""

    def test_the_names_are_derived_from_the_token_name(self):
        self.assertEqual(runtime_secrets.derived_names("foundation-platform-lakehouse-catalog-writer-prod", "api_token"),
                         ["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN"])
        self.assertEqual(runtime_secrets.derived_names("foundation-platform-lakehouse-writer-prod", "r2_access_key"),
                         ["FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID",
                          "FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY"])

    def test_a_token_not_named_by_the_rule_is_refused(self):
        for name in ("foundation-platform-lakehouse-catalog-prod", "foundation-platform-lakehouse-writer",
                     "lakehouse-writer-prod", "foundation-platform-Lakehouse-writer-prod"):
            with self.subTest(name=name), self.assertRaises(runtime_secrets.ContractError):
                runtime_secrets.derived_names(name, "api_token")

    def test_renamed_from_maps_each_old_name_to_the_derived_name_in_order(self):
        with tempfile.TemporaryDirectory() as temp:
            area = pathlib.Path(temp)
            shutil.copytree(AREA / "config", area / "config")
            shutil.copytree(AREA / "infra/systemd", area / "infra/systemd")
            path = area / "config/runtime-secrets.contract.json"
            path.write_text(json.dumps(plant_renames(json.loads(path.read_text(encoding="utf-8")))), encoding="utf-8")
            renamed = runtime_secrets.load(area).renamed
        self.assertEqual(renamed, {OLD[READER]: READER, OLD[READER_SECRET]: READER_SECRET,
                                   OLD[CATALOG_WRITER]: CATALOG_WRITER})



class PlantedDefects(unittest.TestCase):
    """A copy of the tree, one planted defect per test."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="runtime-secrets-")
        self.addCleanup(self.temp.cleanup)
        self.area = pathlib.Path(self.temp.name)
        for relative in ("config", "infra/systemd", "scripts/ops", "scripts/recovery", "scripts/deploy", "docs/runbooks"):
            shutil.copytree(AREA / relative, self.area / relative)
        shutil.copy(AREA / "infra/systemd/building-register-floor.env.example", self.area / "infra/systemd")
        self.assertEqual(runtime_secrets.check(self.area), [], "the copy starts clean")

    def contract(self):
        return json.loads((self.area / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))

    def write_contract(self, contract):
        (self.area / "config/runtime-secrets.contract.json").write_text(json.dumps(contract, indent=2), encoding="utf-8")

    def consumer(self, contract, key):
        return next(c for c in contract["consumers"] if c.get("unit", c.get("run")) == key)

    def findings(self):
        return "\n".join(runtime_secrets.check(self.area))

    def test_a_unit_whose_script_needs_a_name_its_files_do_not_hold(self):
        # 2026-10-05: the collection script required the reader key; the unit's files did not hold it.
        contract = self.contract()
        needs = self.consumer(contract, NUMBER_CHANGE)["needs"]
        for name in [n for n, g in needs.items() if g == "lakehouse-reader"]:
            del needs[name]
        self.write_contract(contract)
        runtime_secrets.render(self.area)
        self.assertIn(f"{NUMBER_CHANGE}: its ExecStart requires {READER}", self.findings())

    def test_a_run_whose_script_needs_a_name_the_contract_does_not_give_it(self):
        # 2026-10-04: the measurement ran without the Gold read's key pair.
        contract = self.contract()
        self.consumer(contract, "measure-building-section-packs")["needs"] = {}
        self.write_contract(contract)
        self.assertIn("measure-building-section-packs: FOUNDATION_PLATFORM_LAKEHOUSE_WRITER_ACCESS_KEY_ID is required by its script",
                      self.findings())

    def test_a_hand_edited_environment_file_line(self):
        unit = self.area / "infra/systemd" / NUMBER_CHANGE
        text = unit.read_text(encoding="utf-8")
        unit.write_text(text.replace("EnvironmentFile=/etc/foundation-platform/recovery.env\n",
                                     "EnvironmentFile=/etc/foundation-platform/recovery.env\n"
                                     "EnvironmentFile=/etc/foundation-platform/parcel-publication.env\n"), encoding="utf-8")
        self.assertIn(f"{NUMBER_CHANGE}: EnvironmentFile lines", self.findings())
        self.assertEqual(runtime_secrets.render(self.area), [NUMBER_CHANGE])
        self.assertEqual(self.findings(), "", "render writes the contract's lines back")
        self.assertEqual(unit.read_text(encoding="utf-8"), text)

    def test_a_runbook_that_hand_writes_an_environment_file(self):
        runbook = self.area / "docs/runbooks/planted.md"
        runbook.write_text("```bash\nsudo systemd-run -p EnvironmentFile=/etc/foundation-platform/recovery.env x\n```\n",
                           encoding="utf-8")
        self.assertIn("docs/runbooks/planted.md:2: hand-written EnvironmentFile", self.findings())

    def test_a_need_from_a_group_that_does_not_hold_it(self):
        contract = self.contract()
        self.consumer(contract, NUMBER_CHANGE)["needs"][READER] = "source-sweep"
        self.write_contract(contract)
        self.assertIn(f"needs {READER} from source-sweep, which does not hold it", self.findings())

    def test_a_need_that_a_later_file_overrides(self):
        contract = self.contract()
        self.consumer(contract, "foundation-by-pnu-serving-bake.service")["needs"] = {
            "FOUNDATION_PLATFORM_RUNTIME_ENV": "source-sweep"}
        self.write_contract(contract)
        self.assertIn("needs FOUNDATION_PLATFORM_RUNTIME_ENV from source-sweep, but map-edit-fold is loaded later and wins",
                      self.findings())

    def test_a_unit_the_contract_does_not_name(self):
        shutil.copy(self.area / "infra/systemd" / NUMBER_CHANGE, self.area / "infra/systemd/foundation-planted.service")
        self.assertIn("foundation-planted.service: the unit is not in", self.findings())

    def test_a_group_loaded_without_a_reason(self):
        contract = self.contract()
        self.consumer(contract, "foundation-gold-panel-rebuild.service")["unenumerated"] = {"map-edit-fold": " "}
        self.write_contract(contract)
        with self.assertRaisesRegex(runtime_secrets.ContractError, "needs its reason"):
            runtime_secrets.check(self.area)

    def test_a_group_that_holds_an_old_name_without_its_derived_name(self):
        contract = plant_renames(self.contract())
        group = next(g for g in contract["groups"] if g["name"] == "lakehouse-reader")
        group["holds"] = [OLD[READER], OLD[READER_SECRET]]
        self.write_contract(contract)
        self.assertIn(f"group lakehouse-reader: holds {OLD[READER]} but not {READER}", self.findings())

    def test_a_runbook_that_names_a_host_file_the_contract_does_not_declare(self):
        runbook = self.area / "docs/runbooks/vworld-parcel-editions.md"
        runbook.write_text(runbook.read_text(encoding="utf-8") + "\n/etc/foundation-platform/planted.env\n", encoding="utf-8")
        self.assertIn("/etc/foundation-platform/planted.env is not a group in", self.findings())

    def test_the_parcel_publication_file_lives_in_the_contract(self):
        """The manual parcel load's file was the one host file outside the contract (root ADR-0247)."""
        contract = self.contract()
        contract["groups"] = [g for g in contract["groups"] if g["name"] != "parcel-publication"]
        self.write_contract(contract)
        self.assertIn("/etc/foundation-platform/parcel-publication.env is not a group in", self.findings())

    def test_a_token_recorded_twice(self):
        contract = self.contract()
        contract["cloudflare_tokens"]["tokens"].append(dict(contract["cloudflare_tokens"]["tokens"][0]))
        self.write_contract(contract)
        with self.assertRaisesRegex(runtime_secrets.ContractError, "recorded twice"):
            runtime_secrets.check(self.area)


class TheHost(unittest.TestCase):
    """`host` on a fake root: the group files the units load, owned by whoever runs the test."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="runtime-secrets-host-")
        self.addCleanup(self.temp.cleanup)
        base = pathlib.Path(self.temp.name)
        self.area = base / "area"
        self.root = base / "root"
        for relative in ("config", "infra/systemd"):
            shutil.copytree(AREA / relative, self.area / relative)
        contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
        self.user = pwd.getpwuid(os.getuid()).pw_name
        self.group = grp.getgrgid(os.getgid()).gr_name
        for group in contract["groups"]:
            if "owner" in group:
                group["owner"], group["group"] = self.user, self.group
        plant_renames(contract)
        (self.area / "config/runtime-secrets.contract.json").write_text(json.dumps(contract), encoding="utf-8")
        self.contract = runtime_secrets.load(self.area)
        used = {g.name for c in self.contract.consumers if c.kind == "unit" for g in self.contract.loaded(c)}
        for group in self.contract.groups.values():
            if group.release or group.optional or group.name not in used:
                continue
            path = self.root / group.path.lstrip("/")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("".join(f"{name}=planted-secret-value\n" for name in sorted(group.holds)), encoding="utf-8")
            path.chmod(group.mode)
        self.reader = self.root / "etc/foundation-platform/lakehouse-reader.env"

    def findings(self):
        return runtime_secrets.host(self.area, self.root)

    def test_a_host_that_holds_every_declared_name_passes(self):
        self.assertEqual(self.findings(), [])

    def test_a_missing_group_file_is_refused(self):
        self.reader.unlink()
        self.assertEqual(self.findings(), ["/etc/foundation-platform/lakehouse-reader.env: missing"])

    def test_a_file_without_a_declared_name_is_refused_by_name_only(self):
        held = self.contract.groups["lakehouse-reader"].holds - {READER}
        self.reader.write_text("".join(f"{name}=planted-secret-value\n" for name in sorted(held)), encoding="utf-8")
        self.reader.chmod(0o600)
        findings = self.findings()
        self.assertEqual(findings, [f"/etc/foundation-platform/lakehouse-reader.env: does not hold {READER}"])
        self.assertNotIn("planted-secret-value", "\n".join(findings))

    def test_a_file_more_open_than_its_mode_is_refused(self):
        self.reader.chmod(0o644)
        self.assertEqual(self.findings(), ["/etc/foundation-platform/lakehouse-reader.env: mode 0644 is more open than 0600"])

    def test_a_file_owned_by_another_account_is_refused(self):
        contract = json.loads((self.area / "config/runtime-secrets.contract.json").read_text(encoding="utf-8"))
        next(g for g in contract["groups"] if g["name"] == "lakehouse-reader")["owner"] = "planted-owner"
        (self.area / "config/runtime-secrets.contract.json").write_text(json.dumps(contract), encoding="utf-8")
        self.assertEqual(self.findings(), [f"/etc/foundation-platform/lakehouse-reader.env: owner {self.user}, not planted-owner"])

    def test_rename_gives_an_old_name_its_derived_name_with_the_same_value_and_mode(self):
        derived = self.contract.tokens["foundation-platform-lakehouse-reader-prod"]
        self.reader.write_text(f"{OLD[READER]}=planted-id\n{OLD[READER_SECRET]}=planted-secret-value", encoding="utf-8")
        self.reader.chmod(0o600)
        self.assertEqual(self.findings(), [f"/etc/foundation-platform/lakehouse-reader.env: does not hold {name}"
                                           for name in derived])
        changed = runtime_secrets.rename(self.area, self.root)
        self.assertEqual(changed, [f"/etc/foundation-platform/lakehouse-reader.env: {OLD[READER]} -> {derived[0]}",
                                   f"/etc/foundation-platform/lakehouse-reader.env: {OLD[READER_SECRET]} -> {derived[1]}",
                                   f"/etc/foundation-platform/lakehouse-reader.env: {OLD[READER]} removed",
                                   f"/etc/foundation-platform/lakehouse-reader.env: {OLD[READER_SECRET]} removed"])
        self.assertNotIn("planted", "\n".join(changed))
        lines = self.reader.read_text(encoding="utf-8").splitlines()
        self.assertEqual(lines, [f"{derived[0]}=planted-id", f"{derived[1]}=planted-secret-value"])
        self.assertEqual(self.reader.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.findings(), [])
        self.assertEqual(runtime_secrets.rename(self.area, self.root), [], "a second run changes nothing")

    def test_an_old_line_goes_when_its_value_is_under_another_held_name(self):
        """The map's file held the reader token under the writer's old name (root ADR-0247 phase three)."""
        operations_map = self.root / "etc/foundation-platform/operations-map.env"
        old_catalog = OLD[CATALOG_WRITER]
        operations_map.write_text(f"FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=planted-uri\n{old_catalog}=planted-reader\n"
                                  "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN=planted-reader\n", encoding="utf-8")
        operations_map.chmod(0o640)
        self.assertEqual(runtime_secrets.rename(self.area, self.root),
                         [f"/etc/foundation-platform/operations-map.env: {old_catalog} removed"])
        self.assertEqual(operations_map.read_text(encoding="utf-8"),
                         "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI=planted-uri\n"
                         "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN=planted-reader\n")

    def test_an_old_line_whose_value_is_nowhere_else_is_kept_and_named(self):
        held = "".join(f"{name}=planted-secret-value\n" for name in sorted(self.contract.groups["lakehouse-reader"].holds))
        self.reader.write_text(held + f"{OLD[READER]}=planted-different\n", encoding="utf-8")
        before = self.reader.read_text(encoding="utf-8")
        changed = runtime_secrets.rename(self.area, self.root)
        self.assertEqual(changed, [f"/etc/foundation-platform/lakehouse-reader.env: {OLD[READER]} kept, "
                                   "its value is under no name the group holds"])
        self.assertNotIn("planted", "\n".join(changed))
        self.assertEqual(self.reader.read_text(encoding="utf-8"), before)

    def test_rename_leaves_a_file_that_already_holds_the_derived_name(self):
        before = self.reader.read_text(encoding="utf-8")
        self.assertEqual(runtime_secrets.rename(self.area, self.root), [])
        self.assertEqual(self.reader.read_text(encoding="utf-8"), before)

    def source(self, text):
        path = pathlib.Path(self.temp.name) / "source.env"
        path.write_text(text, encoding="utf-8")
        return path

    def test_set_writes_a_held_name_with_the_contracts_mode_and_prints_no_value(self):
        operations_map = self.root / "etc/foundation-platform/operations-map.env"
        self.assertFalse(operations_map.exists())
        names = runtime_secrets.set_names(self.area, "operations-map",
                                          self.source("FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN=planted-reader\n"),
                                          self.root)
        self.assertEqual(names, ["FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN"])
        self.assertEqual(operations_map.read_text(encoding="utf-8"),
                         "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_READER_TOKEN=planted-reader\n")
        self.assertEqual(operations_map.stat().st_mode & 0o777, 0o640)

    def test_set_replaces_that_names_lines_and_keeps_the_others(self):
        before = self.reader.read_text(encoding="utf-8").splitlines()
        runtime_secrets.set_names(self.area, "lakehouse-reader", self.source(f"{READER}=planted-new\n"), self.root)
        after = self.reader.read_text(encoding="utf-8").splitlines()
        self.assertEqual(after, [line for line in before if not line.startswith(f"{READER}=")] + [f"{READER}=planted-new"])
        self.assertEqual(self.reader.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.findings(), [])

    def test_set_refuses_a_name_the_group_does_not_hold(self):
        """The operations map never gets a write credential (root ADR-0247)."""
        writer = "FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_WRITER_TOKEN"
        with self.assertRaisesRegex(runtime_secrets.ContractError, writer):
            runtime_secrets.set_names(self.area, "operations-map", self.source(f"{writer}=planted-writer\n"), self.root)
        self.assertFalse((self.root / "etc/foundation-platform/operations-map.env").exists())

    def test_set_refuses_a_source_that_is_not_name_value_lines(self):
        for text in ("export X\n", f"{READER}=a\n{READER}=b\n", "\n"):
            with self.subTest(text=text), self.assertRaises(runtime_secrets.ContractError):
                runtime_secrets.set_names(self.area, "lakehouse-reader", self.source(text), self.root)

    def test_an_optional_file_may_be_absent(self):
        self.assertFalse((self.root / "etc/foundation-platform/by-pnu-bake.env").exists())
        self.assertEqual(self.findings(), [])


class TheCommandLine(unittest.TestCase):
    def test_every_command_runs_wherever_the_release_lives(self):
        """The deploy's rename and host must not depend on how deep the release directory is: a
        default computed from the script's own path once crashed every command at a shallow one."""
        from unittest import mock  # noqa: PLC0415

        with mock.patch.object(runtime_secrets, "AREA", pathlib.Path("/area")):
            self.assertEqual(runtime_secrets.main(["--area", str(AREA), "names", NUMBER_CHANGE]), 0)

    def test_a_run_gets_its_systemd_run_properties_from_the_contract(self):
        contract = runtime_secrets.load(AREA)
        expected = " ".join(f"-p EnvironmentFile={line}"
                            for line in runtime_secrets.rendered_lines(contract, contract.consumer("measure-building-section-packs")))
        self.assertTrue(expected)
        import contextlib  # noqa: PLC0415
        import io  # noqa: PLC0415

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(runtime_secrets.main(["properties", "measure-building-section-packs"]), 0)
        self.assertEqual(out.getvalue().strip(), expected)


if __name__ == "__main__":
    unittest.main()
