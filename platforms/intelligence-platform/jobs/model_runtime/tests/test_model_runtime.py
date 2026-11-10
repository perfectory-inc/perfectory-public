"""The weight fetcher keeps weights off the HDD and keeps only verified bytes; the evaluation scores
citations and values as written (root ADR-0478).

What real incident does failing prevent? 2026-09-19 a 23 GB model was pulled to /data, the HDD
(/data/ollama-models, ITP-ADR-0006), where every load reads at HDD speed; and gemma2:9b put a 2019
date in place of the changed one, which only a check against the document shows.
"""

import hashlib
import json
import os
import pathlib
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE))

import eval_extraction  # noqa: E402
import fetch_weights  # noqa: E402

CONTRACT = json.loads((HERE.parents[1] / "config/model-runtime.contract.json").read_text(encoding="utf-8"))


def fake_sys(root: pathlib.Path, path: pathlib.Path, name: str, rotational: str) -> pathlib.Path:
    """A /sys where the filesystem holding `path` is one partition of the disk `name`."""
    device = os.stat(path).st_dev
    disk = root / "devices" / name
    (disk / "queue").mkdir(parents=True)
    (disk / "queue/rotational").write_text(rotational)
    part = disk / f"{name}p1"
    part.mkdir()
    (part / "partition").write_text("1")
    (root / "dev/block").mkdir(parents=True)
    (root / "dev/block" / f"{os.major(device)}:{os.minor(device)}").symlink_to(part)
    return root


@unittest.skipIf(sys.platform == "win32", "device numbers and /sys are Linux")
class WeightsGoOnlyToTheDeclaredDisk(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = pathlib.Path(self.temp.name)
        self.models = self.base / "models"
        self.models.mkdir()
        self.payload = b"synthetic weights 99999"
        self.contract = json.loads(json.dumps(CONTRACT))
        self.contract["model_root"]["free_bytes_margin"] = 0
        self.contract["runtimes"]["strata_qwen38_flash_next"]["weights"]["files"] = [
            {"path": "Q2_0/a.gguf", "size": len(self.payload), "sha256": hashlib.sha256(self.payload).hexdigest()}
        ]

    def tearDown(self):
        self.temp.cleanup()

    def fetch(self, url, target, size):
        target.write_bytes(self.payload)

    def test_an_nvme_disk_takes_the_weights_and_keeps_only_verified_bytes(self):
        sys_root = fake_sys(self.base / "sys", self.models, "nvme0n1", "0")
        done = fetch_weights.run("strata_qwen38_flash_next", self.models, False, self.contract, sys_root, self.fetch)
        self.assertEqual(done, [str(self.models / "strata_qwen38_flash_next/Q2_0/a.gguf")])

    def test_an_hdd_or_a_sata_ssd_is_refused_before_anything_is_fetched(self):
        for name, rotational, found in (("sda", "1", "hdd"), ("sdb", "0", "ssd")):
            with self.subTest(name=name):
                sys_root = fake_sys(self.base / f"sys-{name}", self.models, name, rotational)
                with self.assertRaisesRegex(fetch_weights.Refused, f"on a {found} disk"):
                    fetch_weights.run("strata_qwen38_flash_next", self.models, False, self.contract, sys_root,
                                      lambda *_: self.fail("fetched"))

    def test_bytes_that_do_not_match_the_pinned_digest_are_refused(self):
        sys_root = fake_sys(self.base / "sys", self.models, "nvme0n1", "0")
        self.payload, wrong = b"other bytes 99999!!!!!!", self.payload
        self.assertEqual(len(self.payload), len(wrong))
        with self.assertRaisesRegex(fetch_weights.Refused, "SHA-256"):
            fetch_weights.run("strata_qwen38_flash_next", self.models, False, self.contract, sys_root, self.fetch)


class TheEvaluationScoresWhatTheDocumentSays(unittest.TestCase):
    TEXT = "가상시 고시 제2099-99호\n가상99999일반산업단지 계획 변경 승인\n면적 : 1,234,500㎡\n2099. 9. 9."
    GOLD = {"notice_number": "가상시 고시 제2099-99호", "complex_name": "가상99999일반산업단지"}

    def test_a_right_value_with_a_verbatim_quote_counts_and_a_made_up_quote_does_not(self):
        facts = [
            {"field": "notice_number", "value": "가상시 고시 제 2099-99호", "quote": "가상시 고시 제2099-99호"},
            {"field": "complex_name", "value": "가상99999일반산업단지", "quote": "가상99999 일반산업단지 계획을 승인"},
        ]
        result = eval_extraction.score(self.TEXT, self.GOLD, facts)
        self.assertEqual((result["correct"], result["valid_citations"], result["facts"]), (2, 1, 2))

    def test_a_wrong_value_and_a_missing_field_are_named(self):
        facts = [{"field": "notice_number", "value": "가상시 고시 제2019-99호", "quote": "가상시 고시 제2099-99호"}]
        result = eval_extraction.score(self.TEXT, self.GOLD, facts)
        self.assertEqual((result["wrong"], result["missing"]), (["notice_number"], ["complex_name"]))

    def test_a_dash_or_brackets_spelled_differently_are_the_same_value(self):
        self.assertTrue(eval_extraction.matches("notice_number", "가상시 고시 제2099 – 99호", "가상시 고시 제2099-99호"))
        self.assertTrue(eval_extraction.matches("complex_name", "가상 99999 일반산업단지", "가상(99999)일반산업단지"))

    def test_dates_and_areas_compare_by_their_digits(self):
        self.assertEqual(eval_extraction.normalize("notice_date", "2099. 9. 9."), eval_extraction.normalize("notice_date", "2099.9.9"))
        self.assertEqual(eval_extraction.normalize("area_m2", "1,234,500㎡"), eval_extraction.normalize("area_m2", "1234500"))

    def test_a_short_complex_name_matches_the_full_one_the_notice_writes(self):
        self.assertTrue(eval_extraction.matches("complex_name", "가상99999 일반산업단지", "가상99999"))
        self.assertFalse(eval_extraction.matches("complex_name", "다른99999농공단지", "가상99999"))
        self.assertFalse(eval_extraction.matches("complex_name", "", "가상99999"))

    def test_a_nul_in_the_text_layer_does_not_cut_the_document_short(self):
        with tempfile.TemporaryDirectory() as directory:
            sample = pathlib.Path(directory)
            (sample / "99999.txt").write_text("가상시\x00고시\x00제2099-99호\n" + "본문 " * 50, encoding="utf-8")
            seen = []
            original = eval_extraction.ask
            eval_extraction.ask = lambda base, path, model, text, *rest: (seen.append(text), ({"choices": []}, 1.0))[1]
            try:
                eval_extraction.evaluate(sample, "http://127.0.0.1:1", "/v1/chat/completions", "m", 1000, 0.1, 16, 5)
            finally:
                eval_extraction.ask = original
            self.assertNotIn("\x00", seen[0])
            self.assertIn("가상시 고시 제2099-99호", seen[0])

    def test_only_a_json_answer_with_known_fields_is_read(self):
        content = '생각 끝.\n{"facts": [{"field": "notice_number", "value": "x", "quote": "x"}, {"field": "other", "value": "y"}]}'
        self.assertEqual([fact["field"] for fact in eval_extraction.parse_facts(content)], ["notice_number"])
        self.assertEqual(eval_extraction.parse_facts("no json"), [])


if __name__ == "__main__":
    unittest.main()
