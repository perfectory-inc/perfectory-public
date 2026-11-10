#!/usr/bin/env python3
"""CI judges pull requests against production's published shape, checked by hash (root ADR-0476)."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("production_shape_fetch", ROOT / "scripts/ci/production-shape-fetch.py")
fetch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fetch)
check = fetch.check_module(ROOT)

LAKEHOUSE = json.dumps({"schema_version": "x", "captured_at": "2026-11-11T12:30:00Z", "tables": {"gold.t": {}}})
POSTGRES = json.dumps({"schema_version": "y", "captured_at": "2026-11-11", "tables_with_rows": ["catalog.t"]})


def head(lakehouse=LAKEHOUSE, postgres=POSTGRES, checked_at="2026-11-11T12:30:00Z"):
    ledgers = {}
    for name, text in (("lakehouse-production-shape.json", lakehouse), ("postgres-production-shape.json", postgres)):
        digest = check.shape_digest(text)
        ledgers[name] = {"key": check.object_key(name, digest), "shape_sha256": digest}
    return {"schema_version": check.HEAD_SCHEMA, "checked_at": checked_at, "release": "r", "ledgers": ledgers}


class Place(unittest.TestCase):
    def tree(self) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        for relative in (check.LAKEHOUSE, check.POSTGRES):
            (root / fetch.AREA / relative).parent.mkdir(parents=True, exist_ok=True)
        return root

    def test_the_published_pair_replaces_the_in_tree_ledgers(self):
        root = self.tree()
        lines = fetch.place(root, head(), {"lakehouse-production-shape.json": LAKEHOUSE.encode(),
                                           "postgres-production-shape.json": POSTGRES.encode()}, check)
        self.assertEqual((root / fetch.AREA / check.LAKEHOUSE).read_text(encoding="utf-8"), LAKEHOUSE)
        self.assertEqual((root / fetch.AREA / check.POSTGRES).read_text(encoding="utf-8"), POSTGRES)
        self.assertEqual(len(lines), 2)

    def test_an_object_that_does_not_hash_to_the_head_is_refused(self):
        tampered = LAKEHOUSE.replace("gold.t", "gold.u")
        with self.assertRaisesRegex(ValueError, "does not hash"):
            fetch.place(self.tree(), head(), {"lakehouse-production-shape.json": tampered.encode(),
                                              "postgres-production-shape.json": POSTGRES.encode()}, check)

    def test_a_head_naming_other_ledgers_is_refused(self):
        wrong = head()
        wrong["ledgers"]["other.json"] = wrong["ledgers"].pop("postgres-production-shape.json")
        with self.assertRaisesRegex(ValueError, "the head names"):
            fetch.place(self.tree(), wrong, {}, check)


class Main(unittest.TestCase):
    def run_main(self, answers):
        out = io.StringIO()
        with mock.patch.object(fetch, "fetch", side_effect=lambda url: answers(url)), \
                mock.patch.object(fetch, "place", return_value=["- placed"]) as placed, \
                mock.patch.dict(fetch.os.environ, {}, clear=True), contextlib.redirect_stdout(out):
            status = fetch.main(["--root", str(ROOT)])
        return status, out.getvalue(), placed

    def test_before_the_first_head_the_committed_ledgers_stand(self):
        status, out, placed = self.run_main(lambda url: None)
        self.assertEqual(status, 0)
        self.assertIn("no head published yet", out)
        placed.assert_not_called()

    def test_an_unreachable_domain_is_said_and_the_committed_ledgers_stand(self):
        def down(url):
            raise OSError("name not resolved")

        status, out, placed = self.run_main(down)
        self.assertEqual(status, 0)
        self.assertIn("unreachable", out)

    def test_a_head_is_read_from_the_contract_domain_and_its_objects_placed(self):
        asked = []
        published = head()

        def answer(url):
            asked.append(url)
            return json.dumps(published).encode() if url.split("?")[0].endswith(check.HEAD) else b"{}"

        status, out, placed = self.run_main(answer)
        self.assertEqual(status, 0, out)
        self.assertTrue(asked[0].startswith(fetch.base_url(ROOT).rstrip("/") + "/shape-ledger/latest.json?run="))
        self.assertEqual(len(asked), 3)
        placed.assert_called_once()
        self.assertIn("Judged against the head production confirmed at 2026-11-11T12:30:00Z", out)

    def test_a_head_naming_a_missing_object_fails(self):
        published = head()
        status, out, _ = self.run_main(
            lambda url: json.dumps(published).encode() if "latest.json" in url else None)
        self.assertEqual(status, 1)
        self.assertIn("does not exist", out)

    def test_an_old_head_warns(self):
        published = head(checked_at="2026-01-01T00:00:00Z")
        status, out, _ = self.run_main(
            lambda url: json.dumps(published).encode() if "latest.json" in url else b"{}")
        self.assertEqual(status, 0)
        self.assertIn("::warning::production shape: the head is", out)


if __name__ == "__main__":
    unittest.main()
