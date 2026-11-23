"""법정동 코드 변경이 닿는 PNU 접두어(root ADR-0329 §3, ADR-0416)를 합성 코드로 본다(시도 98·99, 지어낸 것)."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

import code_change_follow as follow  # noqa: E402


class PrefixTest(unittest.TestCase):
    def test_only_the_codes_that_changed_are_followed(self) -> None:
        before = [{"kind": "pair", "old_code": "9110110100", "new_code": "9999910100", "level": "eupmyeondong"}]
        after = [*before, {"kind": "pair", "old_code": "9899920100", "new_code": "9999920100", "level": "eupmyeondong"}]
        import legal_dong_code_change_views as views  # noqa: PLC0415

        self.assertEqual(follow.prefixes_from_changes(before, after),
                         {views.code_prefix("9899920100"), views.code_prefix("9999920100")})
        self.assertEqual(follow.prefixes_from_changes(before, before), set())


if __name__ == "__main__":
    unittest.main()
