"""카탈로그에 닿는 방법은 한 곳에만 적혀 있어야 한다.

2026-09-01 실측: 같은 사실이 여덟 곳에 있었고 두 곳이 나머지와 달랐다. 갈라진 것을 알려주는
감시자를 붙이는 대신 복사본을 없앴고, 이 검사는 아홉 번째가 생기지 않게 한다.

가드가 아니라 시험인 이유: 검사 대상이 이 디렉터리의 파일이고, 이 레인이 이미 그것을 읽는다.
`python3 -m unittest discover` 로 수집되므로 `unittest.TestCase` 여야 한다.
"""

from __future__ import annotations

import io
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

from lakehouse_engine import required_catalog_env  # noqa: E402

OWNER = "lakehouse_engine.py"

# 이 문자열이 job 안에 있으면 그 job 은 설정을 스스로 적고 있는 것이다.
SPELLS_THE_SETTINGS = (
    "org.apache.iceberg.spark.SparkCatalog",
    "IcebergSparkSessionExtensions",
    "X-Iceberg-Access-Delegation",
    "s3.remote-signing-enabled",
)

# 이 이름들을 job 이 직접 부르면, 그 job 은 카탈로그 변수를 스스로 읽고 있는 것이다.
READS_THE_VARIABLES = tuple(required_catalog_env("lakehouse"))


def job_files() -> list[Path]:
    return sorted(p for p in JOBS_DIR.glob("*.py") if p.name != OWNER)


class OnePlaceReachesTheCatalogTest(unittest.TestCase):
    def test_no_job_spells_the_catalog_settings(self) -> None:
        offenders: list[str] = []
        for path in job_files():
            text = io.open(path, encoding="utf-8").read()
            found = [needle for needle in SPELLS_THE_SETTINGS if needle in text]
            if found:
                offenders.append(f"{path.name}: {found}")

        self.assertEqual(
            offenders,
            [],
            f"카탈로그 설정은 {OWNER} 에만 적는다. 여기 적으면 복사본이 하나 늘고, "
            "늘어난 복사본은 언젠가 갈라진다",
        )

    def test_no_job_reads_the_catalog_variables_itself(self) -> None:
        offenders: list[str] = []
        for path in job_files():
            text = io.open(path, encoding="utf-8").read()
            found = [name for name in READS_THE_VARIABLES if name in text]
            if found:
                offenders.append(f"{path.name}: {found}")

        self.assertEqual(
            offenders,
            [],
            f"카탈로그 변수는 {OWNER} 가 읽는다. job 이 이름을 직접 적으면 "
            "사전 점검 목록이 설정과 따로 늙는다",
        )

    def test_the_variable_list_comes_from_the_settings(self) -> None:
        """이 검사가 무엇을 찾을지는 손으로 적지 않는다.

        찾을 이름을 여기 적으면, 이 시험이 곧 아홉 번째 복사본이 된다.
        """
        source = (JOBS_DIR / OWNER).read_text(encoding="utf-8")
        self.assertGreaterEqual(len(READS_THE_VARIABLES), 3)
        for name in READS_THE_VARIABLES:
            with self.subTest(name=name):
                self.assertIn(name, source)


if __name__ == "__main__":
    unittest.main()
