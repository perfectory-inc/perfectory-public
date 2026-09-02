"""잡이 부르는 이름이 전부 실제로 존재하는지, 실행하지 않고 확인한다.

이 디렉터리의 잡들은 pyspark 를 함수 안에서 지연 import 한다. CI 레인에 pyspark 가 없기
때문이고, 그건 옳다 — 모듈 최상단에서 import 하면 여기 검사들이 전부 조용히 건너뛰어진다.

**그런데 그 대가로, 없는 이름을 부르는 결함이 Spark 를 띄우기 전까지 드러나지 않는다.**
`python3 -m py_compile` 은 문법만 본다. 지운 함수를 계속 부르거나, 공용 모듈에 없는
속성을 쓰거나, 오타 난 이름은 전부 문법상 멀쩡하다. 2026-09-02 에 두 핸드오프 잡의
공통부를 `spatial_silver_handoff` 로 옮기면서 함수 스무 개가 자리를 바꿨는데, 그때
잘못 옮긴 이름 하나를 말해 줄 검사가 하나도 없었다.

그래서 이 검사는 AST 로 같은 것을 본다: 함수 안에서 **읽는** 모든 전역 이름이 모듈에
있거나, 내장이거나, `load_pyspark` 이 묶는 다섯 개 중 하나여야 한다. 그리고 `shared.X` 는
공용 모듈에 X 가 실제로 있어야 한다.

자기 시험은 `test_the_check_catches_a_name_that_does_not_exist` 다 — 위반을 심어 잡히는
것까지 본다. 통과만 본 검사는 이 저장소에서 여러 번 배신했다.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import sys
import unittest
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parents[1] / "jobs"
sys.path.insert(0, str(JOBS_DIR))

# `load_pyspark` 이 모듈 전역에 묶는 이름들. 이 검사가 실행 없이 보는 것이므로, 지연 바인딩은
# 여기서 알려 준다. 이름이 늘면 이 목록도 늘어야 하고, 늘지 않으면 검사가 그렇다고 말한다.
PYSPARK_BOUND = frozenset({"DataFrame", "SparkSession", "F", "T", "StorageLevel"})

SHARED_MODULE = "spatial_silver_handoff"

# 확인 대상. 지연 import 를 쓰는 잡이 늘면 여기 더한다.
MODULES = (
    SHARED_MODULE,
    "vworld_parcel_boundaries_handoff_to_silver",
    "industrial_complex_boundaries_handoff_to_silver",
)


def bound_in(node: ast.AST) -> set[str]:
    """이 함수 안에서 대입·인자·for·with·except·import 로 묶이는 이름."""
    names: set[str] = set()
    for child in ast.walk(node):
        if isinstance(child, ast.arg):
            names.add(child.arg)
        elif isinstance(child, ast.Name) and isinstance(child.ctx, (ast.Store, ast.Del)):
            names.add(child.id)
        elif isinstance(child, ast.ExceptHandler) and child.name:
            names.add(child.name)
        elif isinstance(child, (ast.Import, ast.ImportFrom)):
            for alias in child.names:
                names.add(alias.asname or alias.name.split(".")[0])
    return names


def unresolved(module_name: str, source: str | None = None) -> list[str]:
    """해석되지 않는 참조를 전부 돌려준다. 빈 목록이면 통과."""
    module = importlib.import_module(module_name)
    if source is None:
        source = (JOBS_DIR / f"{module_name}.py").read_text(encoding="utf-8")
    shared = sys.modules.get(SHARED_MODULE)
    known = set(vars(module)) | set(dir(builtins)) | PYSPARK_BOUND

    problems: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        visible = bound_in(node) | known
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load):
                if child.id not in visible:
                    problems.append(f"{module_name}.{node.name}:{child.lineno} -> {child.id}")
            elif (
                isinstance(child, ast.Attribute)
                and isinstance(child.value, ast.Name)
                and child.value.id == "shared"
                and shared is not None
                and not hasattr(shared, child.attr)
            ):
                problems.append(
                    f"{module_name}.{node.name}:{child.lineno} -> shared.{child.attr}"
                )
    return problems


class DeferredPysparkNamesResolveTest(unittest.TestCase):
    def test_every_referenced_name_exists(self) -> None:
        for module_name in MODULES:
            with self.subTest(module=module_name):
                problems = unresolved(module_name)
                self.assertEqual(
                    problems,
                    [],
                    "실행해야만 NameError 로 드러날 참조가 있다:\n  " + "\n  ".join(problems),
                )

    def test_the_check_catches_a_name_that_does_not_exist(self) -> None:
        """위반을 심어 잡히는 것까지 본다.

        통과만 본 검사는 거부할 수 있다는 증거가 없다.
        """
        planted = (
            "def a_planted_function(argument):\n"
            "    return a_name_that_does_not_exist + argument\n"
        )

        problems = unresolved(SHARED_MODULE, planted)

        self.assertEqual(len(problems), 1, f"없는 이름을 잡지 못했다: {problems}")
        self.assertIn("a_name_that_does_not_exist", problems[0])

    def test_the_check_catches_a_shared_attribute_that_does_not_exist(self) -> None:
        planted = (
            "def a_planted_function(frame):\n"
            "    return shared.a_function_the_shared_module_does_not_have(frame)\n"
        )

        problems = unresolved("vworld_parcel_boundaries_handoff_to_silver", planted)

        self.assertEqual(len(problems), 1, f"없는 공용 속성을 잡지 못했다: {problems}")
        self.assertIn("a_function_the_shared_module_does_not_have", problems[0])

    def test_the_check_does_not_flag_the_deferred_pyspark_names(self) -> None:
        """`F` 를 결함으로 읽으면 이 검사는 매번 빨갛고 아무도 안 본다."""
        planted = "def a_planted_function(column):\n    return F.col(column)\n"

        self.assertEqual(unresolved(SHARED_MODULE, planted), [])


if __name__ == "__main__":
    unittest.main()
