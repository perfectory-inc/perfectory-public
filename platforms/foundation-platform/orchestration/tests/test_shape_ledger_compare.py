"""The scheduled comparison of production's captured shape ledgers with main's (root ADR-0279)."""

import contextlib
import importlib.util
import io
import json
import pathlib
import unittest
import unittest.mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("shape_ledger_compare", PLATFORM / "scripts/ops/shape-ledger-compare.py")
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


def committed(relative: str) -> dict:
    return json.loads((PLATFORM / relative).read_text(encoding="utf-8"))


class Compare(unittest.TestCase):
    LAKEHOUSE = "infra/lakehouse/contracts/lakehouse-production-shape.json"
    POSTGRES = "config/postgres-production-shape.json"

    def run_main(self, environment):
        out = io.StringIO()
        with unittest.mock.patch.dict(compare.os.environ, environment, clear=True), contextlib.redirect_stdout(out):
            self.assertEqual(compare.main(), 0, "it gates nothing")
        return out.getvalue()

    def test_without_the_environment_secrets_it_says_so_and_skips(self):
        said = self.run_main({})
        self.assertIn("Skipped", said)
        for name in compare.READ_ENV:
            self.assertIn(name, said)

    def test_main_s_own_ledgers_match(self):
        captured = {pathlib.Path(p).name: json.dumps(committed(p)).encode() for p in (self.LAKEHOUSE, self.POSTGRES)}
        said = "\n".join(compare.report(captured))
        self.assertEqual(said.count("matches main"), 2)
        self.assertIn("Nothing to refresh.", said)

    def test_a_new_table_and_a_table_that_now_holds_rows_are_named(self):
        lakehouse = committed(self.LAKEHOUSE)
        lakehouse["tables"]["silver.synthetic_new"] = {"has_rows": True, "fields": [], "columns": [], "earlier": []}
        postgres = committed(self.POSTGRES)
        postgres["tables_with_rows"] = sorted(set(postgres["tables_with_rows"]) | {"catalog.synthetic_rows"})
        said = "\n".join(compare.report({"lakehouse-production-shape.json": json.dumps(lakehouse).encode(),
                                         "postgres-production-shape.json": json.dumps(postgres).encode()}))
        self.assertIn("silver.synthetic_new: new in production", said)
        self.assertIn("catalog.synthetic_rows: now holds rows", said)
        self.assertIn("A refresh pull request is due", said)

    def test_a_ledger_with_no_capture_yet_is_said(self):
        self.assertIn("no capture in the ledger bucket yet", "\n".join(compare.report({})))

    def test_the_bucket_is_the_one_the_r2_contract_pins(self):
        self.assertEqual(compare.bucket(), "foundation-platform-shape-ledger-prod")

    def test_with_the_secrets_it_reads_both_captures_from_the_bucket(self):
        r2 = unittest.mock.Mock()
        r2.get.return_value = None
        secrets = dict(zip(compare.READ_ENV, ("account.example.invalid", "key-id", "key-secret")))
        real_module = compare.module
        with unittest.mock.patch.object(compare, "module",
                                        side_effect=lambda name, file: r2 if name == "r2_object" else real_module(name, file)):
            said = self.run_main(secrets)
        self.assertEqual([c.args[:3] for c in r2.get.call_args_list],
                         [("https://account.example.invalid", "foundation-platform-shape-ledger-prod",
                           "shape-ledger/lakehouse-production-shape.json"),
                          ("https://account.example.invalid", "foundation-platform-shape-ledger-prod",
                           "shape-ledger/postgres-production-shape.json")])
        self.assertIn("no capture in the ledger bucket yet", said)


if __name__ == "__main__":
    unittest.main()
