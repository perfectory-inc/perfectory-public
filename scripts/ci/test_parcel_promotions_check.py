#!/usr/bin/env python3
"""CI judges the parcel source contract against production's promotions ledger (root ADR-0490 §5)."""

from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import io
import json
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("parcel_promotions_check", ROOT / "scripts/ci/parcel-promotions-check.py")
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)
editions = check.editions_module(ROOT)
CONTRACT = json.loads(editions.CONTRACT_PATH.read_text(encoding="utf-8"))
SERVED = CONTRACT["promotions"][-1]["edition"]
NEWER = min((name for name in CONTRACT["editions"] if name > SERVED), default=None)


def promotion(**fields) -> dict:
    entry = {"edition": NEWER, "from_edition": SERVED, "promoted_at_utc": "2099-01-01T00:00:00Z",
             "by": "edition-controller", "reason": "synthetic",
             "readiness": {"checked_at_utc": "2099-01-01T00:00:00Z", "contract_sha256": "0" * 64,
                           "stages": {stage: "synthetic" for stage in ("source", "silver", "rehearsal")}}}
    entry.update(fields)
    return entry


def published(*entries: dict) -> tuple[dict, bytes]:
    body = "".join(json.dumps(entry) + "\n" for entry in entries).encode("utf-8")
    digest = hashlib.sha256(body).hexdigest()
    return {"schema_version": check.HEAD_SCHEMA, "key": f"parcel-editions/objects/{digest}.jsonl", "sha256": digest,
            "lines": len(entries), "checked_at": "2099-01-02T00:00:00Z", "release": "r"}, body


@unittest.skipIf(NEWER is None, "the contract holds no edition newer than its served one to promote")
class Judge(unittest.TestCase):
    def test_an_empty_ledger_leaves_the_contracts_served_edition(self):
        self.assertEqual(check.judge(ROOT, *published()), CONTRACT["served_edition"])

    def test_a_host_promotion_that_continues_the_contract_is_served(self):
        entry = promotion()
        self.assertEqual(check.judge(ROOT, *published(entry)), entry["edition"])

    def test_a_ledger_the_contract_does_not_continue_fails_the_pull_request(self):
        # What every reader on the host would refuse after the deploy: here it fails before the merge.
        with self.assertRaisesRegex(ValueError, "does not continue this contract"):
            check.judge(ROOT, *published(promotion(from_edition=promotion()["edition"])))
        with self.assertRaisesRegex(ValueError, "does not continue this contract"):
            check.judge(ROOT, *published(promotion(readiness=None)))

    def test_a_ledger_that_does_not_hash_to_the_head_is_refused(self):
        head, body = published(promotion())
        with self.assertRaisesRegex(ValueError, "does not hash"):
            check.judge(ROOT, head, body + b"\n")


@unittest.skipIf(NEWER is None, "the contract holds no edition newer than its served one to promote")
class Main(unittest.TestCase):
    def run_main(self, answers: dict) -> tuple[int, str]:
        out = io.StringIO()

        def fetch(url: str):
            return next((body for suffix, body in answers.items() if url.endswith(suffix)), None)

        with mock.patch.object(check, "fetch", fetch), contextlib.redirect_stdout(out):
            code = check.main(["--root", str(ROOT)])
        return code, out.getvalue()

    def test_no_head_yet_passes_and_says_so(self):
        code, said = self.run_main({})
        self.assertEqual(code, 0)
        self.assertIn("no head published yet", said)

    def test_the_published_ledger_is_judged(self):
        head, body = published(promotion())
        code, said = self.run_main({check.HEAD: json.dumps(head).encode(), head["key"]: body})
        self.assertEqual(code, 0, said)
        self.assertIn("continues production's ledger", said)
        head, body = published(promotion(readiness=None))
        code, said = self.run_main({check.HEAD: json.dumps(head).encode(), head["key"]: body})
        self.assertEqual(code, 1)
        self.assertIn("::error::", said)

    def test_a_head_naming_no_object_fails(self):
        head, _ = published(promotion())
        code, said = self.run_main({check.HEAD: json.dumps(head).encode()})
        self.assertEqual(code, 1)
        self.assertIn("does not exist", said)


if __name__ == "__main__":
    unittest.main()
