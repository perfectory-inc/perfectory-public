"""변환 실행 스크립트가 지켜야 할 것.

이 단계는 2026-08-31 까지 실행기가 없었다. 명령은 있었고 아무도 부르지 않았으며, 이미 표에
들어간 3,986만 필지는 사람이 손으로 만든 것이다. 다시 돌릴 수 없는 단계는 아무도 확인할 수
없는 단계다.

`python3 -m unittest discover` 로 수집되므로 `unittest.TestCase` 여야 한다.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "scripts" / "load" / "vworld-parcel-handoff-export.sh"
CONTRACT = ROOT / "infra" / "lakehouse" / "contracts" / "vworld-parcel-source-objects.json"


class SourceObjectContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = json.loads(CONTRACT.read_text(encoding="utf-8"))

    def test_the_country_is_covered_twice_and_only_one_covering_is_loaded(self) -> None:
        """원천에 전국이 두 벌 있고, 한 벌만 실어야 한다.

        둘 다 실으면 모든 필지가 두 번 들어간다. 재실행 안전장치는 **객체** 단위라 이것을
        막지 못한다 — 두 벌은 서로 다른 객체이고, 같은 땅을 담고 있을 뿐이다.
        """
        counts = self.contract["granularity_counts"]

        self.assertEqual(counts["sido"], 17)
        self.assertEqual(counts["sigungu"], 255)
        self.assertEqual(self.contract["load_granularity"], "sigungu")

    def test_the_two_coverings_are_of_the_same_country(self) -> None:
        """시군구 코드의 앞 두 자리 집합이 시도 코드 집합과 같아야 한다.

        이것이 "두 벌"의 근거다. 다르면 두 데이터셋이지 두 벌이 아니며, 그때는 하나만
        싣는 선택이 데이터를 버리는 것이 된다.
        """
        sido = {o["region_code"] for o in self.contract["objects"] if o["granularity"] == "sido"}
        sigungu = {
            o["region_code"][:2]
            for o in self.contract["objects"]
            if o["granularity"] == "sigungu"
        }

        self.assertEqual(sido, sigungu)

    def test_every_object_is_classified(self) -> None:
        """알갱이를 모르는 객체가 있으면 그것은 조용히 빠진다."""
        unknown = [o for o in self.contract["objects"] if o["granularity"] not in ("sido", "sigungu")]

        self.assertEqual(unknown, [], "분류되지 않은 원천 객체가 있다")

    def test_each_object_key_appears_once(self) -> None:
        """같은 객체가 두 줄이면 한 번은 변환되고 한 번은 건너뛴 것처럼 보인다."""
        keys = [o["object_key"] for o in self.contract["objects"]]

        self.assertEqual(len(keys), len(set(keys)))


class ExportScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.source = SCRIPT.read_text(encoding="utf-8")

    def test_the_script_exists_in_the_repository(self) -> None:
        """실행기가 저장소 밖에 있으면 그것을 아는 사람이 떠나면 사라진다."""
        self.assertTrue(SCRIPT.is_file(), f"{SCRIPT} 가 없다")

    def test_the_granularity_comes_from_the_contract(self) -> None:
        """어떤 벌을 싣는지를 여기 적으면 목록 파일과 갈라진다."""
        code = self._code()

        self.assertIn("load_granularity", code, "실을 알갱이는 목록 파일이 정한다")
        self.assertNotIn(
            'granularity"] == "sigungu"',
            code,
            "알갱이 이름을 스크립트가 직접 고르면 목록 파일이 정본이 아니게 된다",
        )

    def test_it_keeps_no_record_of_what_it_converted(self) -> None:
        """이미 변환했는지는 R2 가 안다 (root ADR-0062).

        스크립트 옆의 기록은 실물과 다른 매체에 있는 두 번째 사실이고, 그 사이에서 죽으면
        둘이 어긋난다. 어긋나는 방향은 둘이고 둘 다 나쁘다 — 다시 변환하거나, 안 한 것을
        했다고 보거나.
        """
        code = self._code()

        for forbidden in (".done", "touch "):
            self.assertNotIn(forbidden, code, f"{forbidden!r} 로 변환 여부를 기억하지 마라")

    def test_it_refuses_to_invent_lineage(self) -> None:
        """계보 값을 지어내면 발행이 세 겹으로 막히고, 그때는 이 실행이 이미 사라진 뒤다."""
        code = self._code()

        self.assertIn("SOURCE_SNAPSHOT_ID", code)
        self.assertIn("VALID_FROM_UTC", code)
        self.assertRegex(
            code,
            r'-z "\$SOURCE_SNAPSHOT_ID"',
            "원천 스냅숏 id 가 비면 멈춰야 한다",
        )

    def test_the_source_record_id_is_the_object_it_read(self) -> None:
        """계보 칸은 원천 객체 이름이어야 한다.

        적재기의 재실행 안전장치가 이 값으로 "이 객체를 이미 넣었나"를 판단한다. 다른 것을
        넣으면 안전장치가 엉뚱한 것을 세게 된다.
        """
        self.assertIn('SOURCE_RECORD_ID="$key"', self._code())

    def _code(self) -> str:
        return "\n".join(
            line
            for line in self.source.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )


if __name__ == "__main__":
    unittest.main()
