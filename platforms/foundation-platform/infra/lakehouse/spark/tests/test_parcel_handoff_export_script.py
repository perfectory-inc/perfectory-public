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

    def test_the_handoff_suffix_lives_here_and_says_it_is_compressed(self) -> None:
        """변환기와 적재기가 같은 이름을 만들어야 한다.

        이름을 두 스크립트가 각자 적으면 갈라지고, 그때 적재기는 없는 객체를 찾는다.
        `.gz` 는 한 문자열로 두 가지를 정한다 — 변환기에게는 "압축해라", Spark 에게는
        "풀어라". 둘을 따로 두면 언젠가 어긋나고, 그 결과는 읽는 쪽이 못 여는 파일이다.
        """
        suffix = self.contract["handoff_suffix"]

        self.assertTrue(suffix.startswith(".jsonl"), suffix)
        self.assertTrue(suffix.endswith(".gz"), "압축하지 않으면 전송량이 그대로다")
        self.assertTrue(self.contract["handoff_suffix_reason"].strip())

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

    def test_neither_script_spells_the_handoff_suffix_itself(self) -> None:
        """접미사는 목록 파일이 정본이다.

        스크립트가 직접 적으면 그것이 두 번째 정본이 되고, 압축을 켜거나 끌 때 한쪽만
        고치게 된다. 그때 적재기는 있지도 않은 이름의 객체를 찾는다.
        """
        loader = (ROOT / "scripts" / "load" / "lakehouse-batch-load.sh").read_text(encoding="utf-8")
        for name, source in (("변환기", self.source), ("적재기", loader)):
            code = "\n".join(
                line
                for line in source.splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            )
            with self.subTest(script=name):
                self.assertIn("handoff_suffix", code, f"{name} 는 접미사를 목록 파일에서 읽어야 한다")
                self.assertNotIn(
                    "'.jsonl.gz'",
                    code,
                    f"{name} 가 접미사를 직접 적으면 정본이 둘이 된다",
                )

    def test_the_tally_is_read_from_summaries_and_not_from_the_log(self) -> None:
        """무엇을 했는지는 요약 파일이 말한다.

        로그 문장을 세던 때는 `RUST_LOG` 하나로 답이 바뀌었다. 두 결과가 모두 info
        수준이라, warn 으로 돌린 전국 실행은 255개를 전부 건너뛰고도 전부 변환했다고
        셌다. 세는 방식이 소음 설정에 달려 있으면, 그 설정을 바꾼 사람은 자기가 집계를
        바꾼 줄 모른다.
        """
        code = self._code()

        self.assertIn("SUMMARY_PATH", code, "각 변환은 자기가 한 일을 요약에 남겨야 한다")
        self.assertIn("outcome", code, "집계는 요약의 결과 칸을 읽어야 한다")
        self.assertNotIn(
            'grep -aq "already exists"',
            code,
            "로그 문장으로 건너뜀을 세면 로그 수준이 집계를 바꾼다",
        )

    def test_each_run_writes_its_evidence_somewhere_of_its_own(self) -> None:
        """앞 실행의 요약을 덮어쓰면 두 실행의 증거가 하나만 남는다.

        남는 쪽은 나중에 쓴 것이고, 다시 돌린 실행은 대개 아무것도 안 한 실행이다.
        그러면 무엇을 실제로 만들었는지 아는 파일이 사라진다.
        """
        code = self._code()

        self.assertIn("RUN_ID", code)
        self.assertRegex(code, r'RUN_DIR="\$STATE/\$RUN_ID"')

    def test_it_converts_several_at_once(self) -> None:
        """순차로는 아무도 다시 안 돌린다.

        20코어 서버의 부하가 0.27 이었다 — 시간은 계산이 아니라 R2 와 주고받는 데
        쓰인다. 겹치지 않으면 그 기다림이 하나씩 더해진다.
        """
        code = self._code()

        self.assertIn("JOBS", code)
        self.assertIn("xargs -P", code)

    def test_the_summaries_are_counted_but_never_consulted_for_the_decision(self) -> None:
        """건너뛸지 말지는 여전히 R2 가 정한다.

        요약을 보고 건너뛰면 그것이 실물과 어긋날 수 있는 세 번째 기록이 된다 —
        스크립트 옆의 표시 파일과 똑같은 결함이고, 이름만 요약으로 바뀐 것이다.
        """
        code = self._code()

        for forbidden in (".done", "touch "):
            self.assertNotIn(forbidden, code)
        # 요약 **경로를 넘기는 것**은 쓰기지 읽기가 아니다. 결정이 되는 것은 변환 직전에
        # 그 파일이 있는지 **묻는** 것이고, 그 물음이 있으면 안 된다.
        worker = code.split("convert_one() {", 1)[1].split("\n}", 1)[0]
        for asking in ("-f \"$RUN_DIR", "-e \"$RUN_DIR", "-s \"$RUN_DIR"):
            self.assertNotIn(asking, worker, "요약이 있는지 물어 건너뛰면 세 번째 기록이 된다")

    def test_an_unreadable_summary_does_not_count_as_an_explanation(self) -> None:
        """총계 대조에는 **아는 것만** 넣어야 한다.

        못 읽은 요약을 세면 개수가 맞아떨어져 실패가 성공으로 보인다. 세는 자리에
        "무슨 일이 있었는지 모른다"를 넣으면 그 모름이 답에서 사라진다.
        """
        code = self._code()

        self.assertIn("accounted = converted + skipped", code)
        self.assertNotIn(
            "accounted = sum(counts.values())",
            code,
            "전부 더하면 못 읽은 요약이 성공을 메운다",
        )

    def test_an_object_without_a_summary_fails_the_run(self) -> None:
        """요약이 없는 객체는 실패했거나 시작도 못 한 것이다.

        0 으로 끝나면 아무도 다시 안 본다. 전국 실행에서 조용히 빠진 시군구 하나는
        그 지역 필지 전부가 표에 없다는 뜻이고, 표는 그것을 말해 주지 않는다.
        """
        code = self._code()

        self.assertIn("accounted != total", code)

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
