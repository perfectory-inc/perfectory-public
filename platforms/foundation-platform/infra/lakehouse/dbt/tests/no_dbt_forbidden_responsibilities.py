from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[4]
DBT_ROOT = ROOT / "infra" / "lakehouse" / "dbt"


class FoundationDbtBoundaryTest(unittest.TestCase):
    def test_dbt_project_exists(self) -> None:
        self.assertTrue((DBT_ROOT / "dbt_project.yml").exists())
        self.assertTrue((DBT_ROOT / "profiles.example.yml").exists())

    def test_dbt_does_not_claim_forbidden_responsibilities(self) -> None:
        forbidden = [
            "R2 write-once commit",
            "publish command",
            "rollback command",
            "human review workflow",
            "AI model call",
            "RAON acquisition",
        ]
        text = "\n".join(
            path.read_text(encoding="utf-8")
            for path in DBT_ROOT.rglob("*")
            if path.is_file() and path.suffix in {".md", ".sql", ".yml", ".yaml"}
        )
        for token in forbidden:
            self.assertNotIn(token, text)

    def test_profiles_example_has_no_secret_values(self) -> None:
        profiles_path = DBT_ROOT / "profiles.example.yml"
        self.assertTrue(profiles_path.exists())
        profiles = profiles_path.read_text(encoding="utf-8")
        forbidden = ["password:", "token:", "secret:", "access_key:", "secret_key:"]
        for token in forbidden:
            self.assertNotIn(token, profiles.lower())


if __name__ == "__main__":
    unittest.main()
