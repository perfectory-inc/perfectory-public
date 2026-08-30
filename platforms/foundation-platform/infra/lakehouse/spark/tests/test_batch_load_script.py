"""적재 스크립트가 지켜야 할 것을 고정한다.

이 스크립트는 2026-08-30 까지 저장소 밖에 있었다. 3,986만 행을 넣은 그 스크립트가 임시
폴더에만 있었고, 그 안에 오늘 배운 것 전부가 들어 있었다 — 묶음 크기, 표 이름, 실패 시
중단, 마커 파일을 쓰지 않는 이유.

세 가지를 검사한다. 셋 다 어겼을 때 실제로 무슨 일이 있었는지가 근거다.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

SCRIPT = (
    Path(__file__).resolve().parents[4] / "scripts" / "load" / "lakehouse-batch-load.sh"
)


class BatchLoadScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.source = SCRIPT.read_text(encoding="utf-8")

    def test_the_script_exists_in_the_repository(self) -> None:
        """적재기가 저장소 밖에 있으면 그것을 아는 사람이 떠나면 사라진다."""
        self.assertTrue(SCRIPT.is_file(), f"{SCRIPT} 가 없다")

    def test_it_keeps_no_record_of_what_it_loaded(self) -> None:
        """이미 넣었는지는 표가 안다 (root ADR-0062).

        스크립트 옆의 마커 파일은 표와 다른 매체에 따로 커밋된 두 번째 사실이라, 실행이 그
        사이에서 죽으면 둘이 어긋난다. 어긋나는 방향은 둘이고 둘 다 나쁘다 — 다시 붙이거나,
        안 넣은 것을 넣었다고 보거나.

        산문이 아니라 실행되는 줄만 본다. 주석에 "marker file 은 왜 나쁜가"를 적었다고
        검사가 걸리면, 다음 사람은 설명을 지워서 검사를 통과시킨다.
        """
        code = "\n".join(
            line
            for line in self.source.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )
        for forbidden in (".done", "touch "):
            self.assertNotIn(
                forbidden,
                code,
                f"{forbidden!r} 로 적재 여부를 기억하지 마라 — 표에 물어야 한다",
            )
        self.assertNotRegex(
            code,
            r"\[\s+-f\s+.*(marker|done)",
            "파일이 있는지로 건너뛰기를 결정하지 마라 — 표의 스냅숏 요약이 정본이다",
        )

    def test_maintenance_runs_after_the_load_not_per_batch(self) -> None:
        """합치기는 묶음마다가 아니라 적재가 끝난 뒤 한 번이다.

        묶음마다 돌리면 16번 적재에 16번 전량 재작성이 붙는다. 합치기는 읽은 바이트를 전부
        다시 쓰므로 그것은 열여섯 배의 비용이다.
        """
        self.assertIn("lakehouse_maintenance.py", self.source, "적재 뒤 유지보수가 있어야 한다")

        loop_end = self.source.index("적재 끝:")
        maintenance = self.source.index("lakehouse_maintenance.py")
        self.assertGreater(
            maintenance, loop_end, "유지보수는 묶음 반복이 끝난 뒤에 와야 한다"
        )

    def test_maintenance_is_skipped_when_the_load_failed(self) -> None:
        """반쯤 들어간 표를 자동으로 다시 쓰면 사람이 볼 기회를 없앤다."""
        tail = self.source[self.source.index("적재 끝:"):]
        self.assertRegex(
            tail,
            r'fail"?\s*-eq\s*0',
            "실패했으면 유지보수를 돌리지 않아야 한다",
        )

    def test_the_iceberg_version_comes_from_the_contract(self) -> None:
        """판 번호를 여기 적으면 잡과 다른 Iceberg 가 실린다 (root ADR-0065)."""
        self.assertIn("lakehouse-engine.contract.json", self.source)
        self.assertIsNone(
            re.search(r"iceberg-spark-runtime-[\d.]+_[\d.]+:\d", self.source),
            "판 번호는 계약에서 읽어야 한다",
        )


if __name__ == "__main__":
    unittest.main()
