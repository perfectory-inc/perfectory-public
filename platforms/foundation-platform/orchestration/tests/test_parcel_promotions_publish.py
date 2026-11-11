"""The host promotions ledger is copied to the ledger bucket, where CI reads it (root ADR-0490 §5).

The ledger lines are synthetic (2099 editions); no bucket is reached: `put` is recorded.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import pathlib
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("parcel_promotions_publish", PLATFORM / "scripts/ops/parcel-promotions-publish.py")
publish = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publish)
check_spec = importlib.util.spec_from_file_location("parcel_promotions_check",
                                                    PLATFORM.parents[1] / "scripts/ci/parcel-promotions-check.py")
check = importlib.util.module_from_spec(check_spec)
check_spec.loader.exec_module(check)

LEDGER = b'{"edition": "209909", "from_edition": "209906"}\n'
NOW = datetime(2099, 10, 1, 8, 10, tzinfo=timezone.utc)


class Publish(unittest.TestCase):
    def put_log(self, held: set[str] | None = None):
        written, held = [], set(held or ())

        def put(name, body, only_new):
            if only_new and name in held:
                return False
            held.add(name)
            written.append((name, body, only_new))
            return True
        return written, put

    def test_a_new_ledger_is_written_once_with_its_head_and_its_history(self):
        written, put = self.put_log()
        head, new = publish.publish(LEDGER, NOW, {}, put)
        self.assertTrue(new)
        names = [name for name, _, _ in written]
        self.assertEqual(names, [head["key"], f"{publish.HISTORY}2099-10-01T08:10:00Z.json", publish.HEAD])
        self.assertEqual(written[0][1], LEDGER)
        self.assertEqual([only_new for _, _, only_new in written], [True, True, False])
        self.assertEqual((head["lines"], head["schema_version"]), (1, check.HEAD_SCHEMA))
        # What CI reads is what the host wrote: the head names it by its hash, under CI's head key.
        self.assertEqual(publish.HEAD, check.HEAD)
        self.assertEqual(json.loads(written[2][1])["sha256"], publish.ledger_digest(LEDGER))

    def test_a_run_that_changed_nothing_writes_only_the_head(self):
        first, put = self.put_log()
        head, _ = publish.publish(LEDGER, NOW, {}, put)
        again, put = self.put_log({head["key"]})
        _, new = publish.publish(LEDGER, NOW, {}, put)
        self.assertFalse(new)
        self.assertEqual([name for name, _, _ in again], [publish.HEAD])

    def test_without_a_key_for_its_prefix_nothing_is_sent_and_the_run_goes_on(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = pathlib.Path(tmp) / "served-ledger.jsonl"
            ledger.write_bytes(LEDGER)
            said = io.StringIO()
            with mock.patch.dict(os.environ, {"RUNTIME_DIRECTORY": tmp}), contextlib.redirect_stderr(said):
                self.assertEqual(publish.main([str(ledger)]), 0)
                # A key the control checkout minted for the shape ledger cannot write here.
                (pathlib.Path(tmp) / publish.WRITER_KEY).write_text(json.dumps({"prefix": "shape-ledger/"}))
                self.assertEqual(publish.main([str(ledger)]), 0)
            self.assertEqual(said.getvalue().count("not published"), 2)

    def test_no_ledger_yet_is_the_empty_ledger(self):
        head = publish.head(b"", NOW, "r")
        self.assertEqual((head["lines"], head["sha256"]), (0, publish.ledger_digest(b"")))


if __name__ == "__main__":
    unittest.main()
