"""계보 값 이관이 지켜야 할 것.

3,986만 행의 계보 칸을 고쳐 쓰고 기록을 남기는 조작이다. 잘못 돌면 표는 그대로인데 기록만
바뀌거나, 그 반대가 된다. 둘 다 안전장치가 틀린 것을 믿게 만든다.

`python3 -m unittest discover` 로 수집되므로 `unittest.TestCase` 여야 한다.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SPARK_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SPARK_DIR / "jobs"))

from lakehouse_lineage_migration import (  # noqa: E402
    canonical_by_bare_name,
    chunked,
    plan_migration,
    refuse_values_sql_cannot_carry,
)
import vworld_parcel_editions  # noqa: E402

# The ledger's editions must hold the contract's served edition; the rest is synthetic.
SERVED = vworld_parcel_editions.served(vworld_parcel_editions.load())


def ledger_editions(*names: str) -> dict:
    """Synthetic editions as the publisher's `vworld-parcel-editions` writes them (root ADR-0224)."""

    return {"editions": {name: {
        "provider_base_month": f"{name[:4]}-{name[4:]}",
        "extracted_on": {"earliest": f"{name[:4]}-{name[4:]}-10", "latest": f"{name[:4]}-{name[4:]}-10"},
        "handoff_prefix": f"silver-handoff/synthetic/edition={name}",
        "granularity_counts": {"sido": 1, "sigungu": 1},
        "objects": [{"object_key": f"bronze/source=vworldkr__parcel/{name}-{code}.zip", "bytes": 1,
                     "dataset_name": f"LSMD_CONT_LDREG_{code}_{name}", "region_code": code,
                     "granularity": "sido" if len(code) == 2 else "sigungu"} for code in ("97", "97110")],
    } for name in names}}


class MappingTest(unittest.TestCase):
    def mapping(self, document: dict | None) -> dict[str, str]:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "editions.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            env = {k: v for k, v in os.environ.items() if k != "VWORLD_PARCEL_EDITIONS"}
            if document is not None:
                env["VWORLD_PARCEL_EDITIONS"] = str(path)
            with mock.patch.dict(os.environ, env, clear=True):
                return canonical_by_bare_name()

    def test_the_mapping_comes_from_the_ledgers_objects(self) -> None:
        """접두사를 붙이는 게 아니라 실제 객체 목록(장부의 판, root ADR-0224)에서 찾는다.

        접두사를 지어 붙이면 그 접두사가 틀린 날 아무도 모른다. 두 표기가 생긴 것도 각자
        값을 만든 탓이다.
        """
        mapping = self.mapping(ledger_editions(SERVED, "209909"))

        self.assertEqual(len(mapping), 4)
        for bare, full in mapping.items():
            with self.subTest(bare=bare):
                self.assertTrue(full.endswith("/" + bare))
                self.assertTrue(full.startswith("bronze/source="))
        # 심은 위반: 장부 없이 돌면 목록을 지어내지 않고 거부한다.
        with self.assertRaisesRegex(ValueError, "Bronze ledger"):
            self.mapping(None)

    def test_every_bare_name_resolves_to_exactly_one_object(self) -> None:
        """한 이름이 두 객체를 가리키면 그것은 사상이 아니다.

        2026-09-01 실측: 272개 객체 전부에서 파일 이름이 겹치는 경우가 없다. 이 검사는
        그것이 계속 참이게 한다 — 겹치는 순간 이관이 엉뚱한 객체를 가리키게 된다.
        """
        mapping = self.mapping(ledger_editions(SERVED))
        self.assertEqual(len(set(mapping.values())), len(mapping))
        planted = ledger_editions(SERVED, "209909")
        planted["editions"]["209909"]["objects"][1]["object_key"] = f"bronze/source=other/{SERVED}-97110.zip"
        with self.assertRaisesRegex(ValueError, "named by editions"):
            self.mapping(planted)


class PlanTest(unittest.TestCase):
    MAPPING = {
        "a.zip": "bronze/source=d/a.zip",
        "b.zip": "bronze/source=d/b.zip",
    }

    def test_only_the_bare_values_are_rewritten(self) -> None:
        """이미 정본인 값을 다시 고치면 접두사가 두 번 붙는다."""
        plan = plan_migration(["a.zip", "bronze/source=d/b.zip"], self.MAPPING)

        self.assertEqual(plan, {"a.zip": "bronze/source=d/a.zip"})

    def test_a_value_nobody_can_explain_stops_the_run(self) -> None:
        """설명 못 하는 값을 그냥 두면, 이관 뒤에도 두 형태가 남는다."""
        with self.assertRaisesRegex(ValueError, "neither canonical"):
            plan_migration(["a.zip", "who-knows.zip"], self.MAPPING)

    def test_a_value_sql_cannot_carry_stops_the_run(self) -> None:
        """고치는 문장을 글자로 만든다. 따옴표가 든 값은 그 문장을 일찍 끝낸다.

        수집된 객체 이름에는 없다 — 그래서 벗어나는 값은 이 이관이 본 적 없는 값이고,
        빠져나갈 방법을 짐작하기보다 멈추는 것이 맞다.
        """
        with self.assertRaisesRegex(ValueError, "quote or backslash"):
            refuse_values_sql_cannot_carry({"it's.zip": "bronze/source=d/it's.zip"})

        # 정상적인 이름은 통과해야 한다. 안 그러면 이관 자체가 못 돈다.
        refuse_values_sql_cannot_carry({"a.zip": "bronze/source=d/a.zip"})


class ChunkTest(unittest.TestCase):
    def test_the_record_is_split_to_fit_one_commit(self) -> None:
        """한 기록이 이름할 수 있는 객체 수에 상한이 있다. 필지는 255개다.

        상한을 넘겨 자르면 기록은 적재보다 적은 객체를 말하게 되고, 빠진 객체는 다음
        실행에서 다시 들어간다.
        """
        chunks = chunked([f"{n}.zip" for n in range(255)], 64)

        self.assertEqual([len(c) for c in chunks], [64, 64, 64, 63])
        self.assertEqual(sum(len(c) for c in chunks), 255)
        # 나뉘어도 하나도 빠지지 않아야 한다.
        self.assertEqual(len({name for chunk in chunks for name in chunk}), 255)


if __name__ == "__main__":
    unittest.main()
