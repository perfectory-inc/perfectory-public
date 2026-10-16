"""Every script that lands VWorld dataset files for real keys them by content (root ADR-0152).

The parcel edition collector set FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1 without the key
form on 2026-10-14: five files met the objects an earlier edition left under the same provider file
number (R2 502) and others were read as already held. The collector itself now refuses that pair at
run time; this test makes CI refuse the script before it ships.
"""

import pathlib
import re
import unittest

OPS = pathlib.Path(__file__).resolve().parents[1].parent / "scripts" / "ops"
LIVE = re.compile(r"^\s*export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1\s*$", re.MULTILINE)
CONTENT_KEY = re.compile(r"^\s*export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY=content_addressed\s*$", re.MULTILINE)
SPOOL = re.compile(r"^\s*export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR=\S+", re.MULTILINE)


def unkeyed(text: str) -> bool:
    return bool(LIVE.search(text)) and not (CONTENT_KEY.search(text) and SPOOL.search(text))


class LiveVWorldIngestKeysByContent(unittest.TestCase):
    def test_every_live_ingest_script_names_the_content_key_and_its_spool(self):
        live = [path for path in sorted(OPS.glob("*.sh")) if LIVE.search(path.read_text(encoding="utf-8"))]
        self.assertTrue(live, "no script lands VWorld dataset files; the pattern no longer matches")
        for path in live:
            with self.subTest(script=path.name):
                self.assertFalse(unkeyed(path.read_text(encoding="utf-8")))

    def test_a_live_ingest_without_the_content_key_is_caught(self):
        self.assertTrue(unkeyed("export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1\n"))
        self.assertTrue(unkeyed(
            "export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1\n"
            "export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY=content_addressed\n"))
        self.assertFalse(unkeyed(
            "export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_LIVE_WRITE=1\n"
            "export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_BRONZE_KEY=content_addressed\n"
            'export FOUNDATION_PLATFORM_VWORLD_DATASET_FILE_SPOOL_DIR="${work}/spool"\n'))


if __name__ == "__main__":
    unittest.main()
