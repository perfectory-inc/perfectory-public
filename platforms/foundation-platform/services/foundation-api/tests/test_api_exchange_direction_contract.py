import json
import re
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "docs/architecture/api-exchange-direction-contract.md"
TRAFFIC_AUTH_REGISTRY = ROOT / "docs/architecture/traffic-auth-policy-registry.v1.json"

CONTRACT_MARKER = re.compile(r"^<!--\s*contract:\s*([a-z0-9]+(?:-[a-z0-9]+)*)\s*-->\s*$", re.M)

# `scripts/guard/document-contract-markers.py` reads this collection by name and fails if any id
# here is declared by no document. That is the half this test cannot cover: an assertion against an
# id that stopped existing would otherwise just be deleted along with it.
CONTRACT_IDS = {
    "external-acquisition-pull",
    "catalog-lookup-pull",
    "proposal-intake-push",
    "artifact-registration-push",
    "staff-command-push",
    "outbox-fanout-push",
    "dbt-modeling-pull",
    "no-cross-service-db-access",
}


def declared_contracts(document: Path) -> set[str]:
    """Contract identifiers a document declares, from its `<!-- contract: id -->` markers.

    This test used to assert the prose of each direction rule. ADR-0009 requires human-readable
    narrative to be Korean, so those assertions failed on the edit the policy demanded rather than
    on a missing rule — and would equally have passed had a rule been deleted with its sentence
    left behind. A marker is not prose: nothing translates it, it sits on the section it names, and
    it disappears exactly when that section does. See ADR-0012 rule 4.
    """
    return set(CONTRACT_MARKER.findall(document.read_text(encoding="utf-8")))


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

        # Route literals are identifiers, so these stay direct content assertions.
        for surface in service_surfaces:
            self.assertIn(surface, contract)

    def test_every_direction_rule_is_declared(self) -> None:
        # Equality, not containment: a missing rule fails, and so does a marker added without a
        # decision to add a direction rule.
        self.assertEqual(declared_contracts(CONTRACT), CONTRACT_IDS)


if __name__ == "__main__":
    unittest.main()
