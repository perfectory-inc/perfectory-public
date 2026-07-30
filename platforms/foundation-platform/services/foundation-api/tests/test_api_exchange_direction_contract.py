import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "docs/architecture/api-exchange-direction-contract.md"
TRAFFIC_AUTH_REGISTRY = ROOT / "docs/architecture/traffic-auth-policy-registry.v1.json"


class ApiExchangeDirectionContractTest(unittest.TestCase):
    def test_contract_covers_the_current_push_and_pull_surfaces(self) -> None:
        contract = CONTRACT.read_text(encoding="utf-8")
        registry = json.loads(TRAFFIC_AUTH_REGISTRY.read_text(encoding="utf-8"))

        service_surfaces = {
            f"{surface['method']} {surface['path']}"
            for policy in registry["service_identity_policies"]
            for surface in policy["allowed_service_surfaces"]
        }

        self.assertIn("GET /catalog/v1/parcels/by-pnu/:pnu", service_surfaces)
        self.assertIn("POST /internal/lakehouse/artifacts", service_surfaces)
        self.assertIn("POST /internal/normalization/proposals", service_surfaces)

        for surface in service_surfaces:
            self.assertIn(surface, contract)

        # The direction rules are section headings, which the Korean-first migration (558c5beb)
        # translated. Headings are the durable anchor: a reworded paragraph keeps them, a deleted
        # rule does not.
        self.assertIn("### 외부 제공기관 수집은 가져오기(Pull)", contract)
        self.assertIn("### 제안 접수는 밀어넣기(Push)", contract)
        self.assertIn("### Outbox 전달은 밀어넣기(Push)", contract)
        self.assertIn("### dbt/Trino 모델링은 가져오기/조회다", contract)
        self.assertIn("서비스 간 데이터베이스 직접 접근은 금지한다", contract)


if __name__ == "__main__":
    unittest.main()
