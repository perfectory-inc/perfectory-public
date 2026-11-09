"""A shared cache keeps a mutable answer no longer than its Worker does (root ADR-0470).

The Seoul front door (root ADR-0461) stores what the Workers answer for as long as their Cache-Control
lets a shared cache. A URL whose answer changes in place (a by-PNU document, a patched tile) said
`public, max-age=3600`, which Cloudflare's own edge never stored, so the hour was the browser's
alone; behind CloudFront it became the edge's too: the catalog door was held on 2026-11-09 because
an hour-old document there outlives a publish the hourly monitor and every visitor would miss. Every
such Cache-Control therefore names its shared-cache lifetime, s-maxage, equal to the seconds the same
Worker itself holds the manifest that decides the answer, so a door adds no more staleness than the
Worker already has.
"""

import json
import pathlib
import re
import unittest

AREA = pathlib.Path(__file__).resolve().parents[2]
R2 = AREA / "config/r2-connections.contract.json"
MANIFEST_SECONDS = ("manifest_edge_cache_seconds", "manifest_cache_seconds")


def directive(value: str, name: str) -> int | None:
    found = re.search(rf"(?:^|,)\s*{name}=(\d+)\s*(?:,|$)", value)
    return int(found.group(1)) if found else None


def mutable_cache_controls(node, path=()):
    """Each cacheable, non-immutable *cache_control with the manifest seconds of its own block."""

    if not isinstance(node, dict):
        return
    seconds = next((node[key] for key in MANIFEST_SECONDS if key in node), None)
    for key, value in node.items():
        if key.endswith("cache_control") and isinstance(value, str):
            max_age = directive(value, "max-age")
            if max_age and "immutable" not in value:
                yield "/".join((*path, key)), value, seconds
        yield from mutable_cache_controls(value, (*path, key))


class SharedCacheLifetime(unittest.TestCase):
    def test_every_mutable_answer_names_its_worker_manifest_seconds_as_s_maxage(self):
        found = list(mutable_cache_controls(json.loads(R2.read_text(encoding="utf-8"))))
        self.assertGreaterEqual(len(found), 4, found)
        for path, value, seconds in found:
            with self.subTest(path):
                self.assertIsNotNone(seconds, f"{path}: its block names no manifest cache seconds")
                self.assertEqual(directive(value, "s-maxage"), seconds, f"{path} = {value!r}")

    def test_the_directive_reader_refuses_a_lookalike(self):
        self.assertEqual(directive("public, max-age=3600, s-maxage=60", "max-age"), 3600)
        self.assertEqual(directive("public, max-age=3600, s-maxage=60", "s-maxage"), 60)
        self.assertIsNone(directive("public, max-age=3600", "s-maxage"))
        self.assertEqual(list(mutable_cache_controls({"cache_control": "public, max-age=31536000, immutable"})), [])
        self.assertEqual(list(mutable_cache_controls({"cache_control": "no-cache, max-age=0"})), [])


if __name__ == "__main__":
    unittest.main()
