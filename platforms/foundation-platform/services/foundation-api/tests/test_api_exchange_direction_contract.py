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

        self.assertIn("External provider acquisition is pull", contract)
        self.assertIn("Proposal intake is push", contract)
        self.assertIn("Outbox fan-out is push", contract)
        self.assertIn("dbt/Trino modeling is pull/query", contract)
        self.assertIn("cross-service direct database access is forbidden", contract)


if __name__ == "__main__":
    unittest.main()
