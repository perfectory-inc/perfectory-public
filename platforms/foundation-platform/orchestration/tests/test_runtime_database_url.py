"""Host jobs consume the existing Compose connection and only change its transport address."""

import copy
import json
import pathlib
import subprocess
import sys
import unittest


ADAPTER = pathlib.Path(__file__).resolve().parents[2] / "scripts/ops/runtime-database-url.py"


class RuntimeDatabaseUrl(unittest.TestCase):
    def setUp(self):
        self.model = {"services": {
            "foundation-api": {"environment": {
                "DATABASE_URL": "postgresql://read_role:p%40ss%3Aword@postgres:5433/ledger?sslmode=disable",
            }},
            "postgres": {"ports": [
                {"host_ip": "127.0.0.1", "published": "15439", "target": 5433, "protocol": "tcp"},
            ]},
        }}

    def run_adapter(self, model):
        return subprocess.run([sys.executable, str(ADAPTER)], input=json.dumps(model),
                              text=True, capture_output=True)

    def test_preserves_existing_role_database_encoded_password_and_query(self):
        result = self.run_adapter(self.model)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(),
                         "postgresql://read_role:p%40ss%3Aword@127.0.0.1:15439/ledger?sslmode=disable")
        self.assertEqual(result.stderr, "")

    def test_refuses_missing_ambiguous_and_non_loopback_port_without_disclosing_url(self):
        ports = self.model["services"]["postgres"]["ports"]
        for changed in [[], ports * 2, [{**ports[0], "host_ip": "0.0.0.0"}],
                        [{**ports[0], "published": "1-2"}], [{**ports[0], "target": 5432}]]:
            with self.subTest(ports=changed):
                model = copy.deepcopy(self.model)
                model["services"]["postgres"]["ports"] = changed
                result = self.run_adapter(model)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("p%40ss", result.stderr)
                self.assertNotIn("read_role", result.stderr)

    def test_refuses_malformed_or_query_overridden_destination(self):
        for url in ["", "http://read_role:secret@postgres:5433/ledger", "postgres://secret@other:5433/ledger",
                    "postgres://read_role:secret@postgres:5433/ledger?host=remote",
                    "postgres://read_role:secret@postgres:5433/ledger?port=1111",
                    "postgres://read_role:secret@postgres:5433/ledger?service=other"]:
            with self.subTest(url=url):
                self.model["services"]["foundation-api"]["environment"]["DATABASE_URL"] = url
                result = self.run_adapter(self.model)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertNotIn("secret", result.stderr)


if __name__ == "__main__":
    unittest.main()
