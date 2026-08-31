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

    def _code(self) -> str:
        """실행되는 줄만. 주석에 금지된 형태를 설명하면 검사가 그것을 실물로 읽는다."""
        return "\n".join(
            line
            for line in self.source.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )

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

    def test_the_jar_cache_outlives_the_container(self) -> None:
        """받은 jar 는 컨테이너보다 오래 살아야 한다.

        적재기는 묶음마다 컨테이너를 새로 띄우고 `--rm` 으로 버린다. 캐시가 컨테이너 안에
        있으면 `--packages` 가 같은 jar 를 묶음 수만큼 다시 내려받는다. 전국 필지 한 번에
        열여섯 번이다.

        **이 결함은 실패하지 않는다. 조용히 느려질 뿐이다.** 그래서 검사가 필요하다.
        """
        self.assertNotIn(
            "spark.jars.ivy=/tmp/",
            self.source,
            "컨테이너 안 임시 폴더는 --rm 과 함께 사라진다",
        )
        self.assertIn(
            "spark.jars.ivy=/home/spark/.ivy2",
            self.source,
            "캐시는 compose 가 호스트에 붙여 주는 경로를 가리켜야 한다",
        )

    def test_an_object_batch_is_a_list_of_keys_not_a_glob(self) -> None:
        """객체 묶음은 폴더가 아니라 키 목록이어야 한다.

        하드링크가 없으니 묶음을 폴더로 만들 수 없고, 글롭은 접두사 아래 **전부**를 가리킨다.
        묶음마다 전부를 읽으면 같은 데이터를 묶음 수만큼 읽는다 — 그리고 재실행 안전장치가
        그것을 "이미 넣었다"로 받아 두 번째 묶음부터 아무것도 안 하게 된다.
        """
        code = self._code()

        self.assertIn('input="${input:+$input,}${all[$j]}"', code, "키를 이어 붙여야 한다")
        self.assertNotIn(
            's3a://$FOUNDATION_PLATFORM_R2_LAKEHOUSE_BUCKET/$HANDOFF_PREFIX/*',
            code,
            "객체 입력에 글롭을 쓰면 묶음이 의미를 잃는다",
        )

    def test_the_object_keys_come_from_the_contract_not_from_a_listing(self) -> None:
        """키는 R2 를 훑어서 얻지 않는다.

        훑으면 반쯤 변환된 상태가 "이만큼이 전부"로 보인다. 변환기와 적재기가 같은 목록
        파일에서 같은 규칙으로 이름을 만들면, 아직 변환되지 않은 것을 읽으려다 그 자리에서
        드러난다.
        """
        code = self._code()

        self.assertIn("load_granularity", code, "실을 알갱이는 목록 파일이 정한다")
        self.assertNotIn("list-objects", code)
        self.assertNotIn("inventory-r2", code, "적재기가 버킷을 훑으면 안 된다")

    def test_r2_credentials_reach_the_container(self) -> None:
        """자격증명이 컨테이너 밖에 있으면 잡은 자기 입력을 못 연다.

        `docker compose run` 은 호스트 환경을 물려주지 않는다. `-e` 로 넘기지 않으면 잡은
        s3a 설정을 만들다가 없는 변수로 멈춘다 — 원인이 자격증명이라는 말은 어디에도 없이.
        """
        code = self._code()

        for name in (
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_ENDPOINT",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID",
            "FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_SECRET_ACCESS_KEY",
        ):
            self.assertIn(f"-e {name}", code, f"{name} 을 컨테이너에 넘겨야 한다")

    def test_the_reader_credentials_are_the_ones_passed(self) -> None:
        """적재는 읽기만 한다. 쓰기 키를 넘기면 쓰지 않을 권한을 주는 것이다.

        그 권한이 미치는 버킷에는 모든 표가 들어 있다.
        """
        code = self._code()

        self.assertNotIn("R2_LAKEHOUSE_WRITER_ACCESS_KEY_ID", code)
        self.assertNotIn("R2_LAKEHOUSE_WRITER_SECRET_ACCESS_KEY", code)

    def test_the_cache_directory_exists_before_the_container_wants_it(self) -> None:
        """붙일 폴더가 없으면 도커가 root 소유로 만들고, 컨테이너 사용자는 못 쓴다.

        컨테이너는 uid 185 로 돌고 이미지에 `/home/spark` 가 없다. 없는 경로에 볼륨을 붙이면
        쓰기가 막히는데, 그때도 적재는 성공한다 — jar 를 매번 다시 받으면서.
        """
        code = "\n".join(
            line
            for line in self.source.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )
        make = code.index("mkdir -p")
        submit = code.index("spark.jars.ivy")
        self.assertLess(make, submit, "폴더를 먼저 만들고 나서 컨테이너를 띄워야 한다")
        self.assertIn("IVY_CACHE", code, "캐시 위치를 이름 있는 값으로 둬야 한다")


if __name__ == "__main__":
    unittest.main()
