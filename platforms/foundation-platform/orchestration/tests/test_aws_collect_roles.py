"""Only the credential vendor reads the minting token, a lane's run reaches only its own lane, and a source key is
read only by the lanes that name it (ADR-0623 §2, contract collect.source_secrets).

infra/aws/collect.tf as the parser reads the stack (aws_stack.py), and the contract's lanes: only the vendor's
blocks name the token and its slot refuses every other reader; neither a lane's run role nor Dagster's roles can
read it; a lane role may invoke its own alias, read the database role's secret and its own source keys, nothing
else, and sits under the contract's run path, the only one Dagster may pass; no block names a lane (a lane is
data); and each lane's scope is its own: Bronze and media prefixes no other lane writes, its state prefix
generated from its id, no table (the table half waits for root ADR-0624).
"""

import json
import pathlib
import re
import unittest

import aws_stack
from aws_stack import body, naming, references, statements

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
TOKEN = "aws_secretsmanager_secret.r2_mint_token"
VENDOR = ["aws_lambda_function.r2_vend", "aws_secretsmanager_secret_policy.r2_mint_token",
          "data.aws_iam_policy_document.r2_vend"]
BRONZE = re.compile(r"^bronze/source=[a-z0-9]+__[a-z0-9_]+/$")
MEDIA = ("media/display/", "media/received/", "media/rendition/")


def lanes():
    return aws_stack.contract()["collect"]["lanes"]


class OnlyTheVendorReadsTheToken(unittest.TestCase):
    def test_the_vendor_reads_the_minting_token_only(self) -> None:
        reads = [statement for statement in statements("data.aws_iam_policy_document.r2_vend").values()
                 if "secretsmanager:GetSecretValue" in statement["actions"]]
        self.assertEqual([statement["resources"] for statement in reads], [[f"${{{TOKEN}.arn}}"]])

    def test_only_the_vendor_s_blocks_name_the_token(self) -> None:
        self.assertEqual(sorted(naming(TOKEN)), VENDOR)

    def test_the_token_s_slot_refuses_every_reader_but_the_vendor(self) -> None:
        deny = statements("data.aws_iam_policy_document.r2_mint_token")["OnlyTheVendorReads"]
        self.assertEqual((deny["effect"], deny["actions"]), ("Deny", ["secretsmanager:GetSecretValue"]))
        self.assertEqual(deny["condition"][0]["values"], ["${aws_iam_role.r2_vend.arn}"])
        self.assertEqual(deny["condition"][0]["test"], "ArnNotEquals")

    def test_neither_a_lane_s_run_role_nor_dagster_s_roles_can_read_the_token(self) -> None:
        for role in ("lane", "dagster", "dagster_execution", "run_execution"):
            named = references(body(f"data.aws_iam_policy_document.{role}"))
            self.assertFalse([name for name in named if name.startswith(TOKEN)], f"the {role} role reads {TOKEN}")

    def test_no_catalog_token_the_table_half_is_not_here(self) -> None:
        self.assertNotIn("r2_catalog_reader_token", json.dumps(aws_stack.stack()))
        self.assertFalse((PLATFORM / "infra" / "aws" / "r2-vend" / "iceberg_catalog.py").exists())


class ALaneRoleReachesOnlyItsOwn(unittest.TestCase):
    def test_a_lane_role_asks_only_its_own_alias_and_reads_the_database_role_and_its_source_keys(self) -> None:
        lane = statements("data.aws_iam_policy_document.lane")
        self.assertEqual({sid: (statement["actions"], statement["resources"]) for sid, statement in lane.items()}, {
            "AskTheVendorForThisLane": (["lambda:InvokeFunction"], ["${aws_lambda_alias.lane[each.key].arn}"]),
            "ReadTheDatabaseRole": (["secretsmanager:GetSecretValue"], ["${aws_secretsmanager_secret.foundation_runtime.arn}"]),
        })
        dynamic = body("data.aws_iam_policy_document.lane")["dynamic"]["statement"][0]
        self.assertIn("each.value.secrets", references(dynamic["for_each"]))
        content = dynamic["content"][0]
        self.assertEqual((content["sid"], content["actions"]), ("ReadItsSourceKeys", ["secretsmanager:GetSecretValue"]))
        self.assertIn("aws_secretsmanager_secret.source", references(content["resources"]))
        self.assertIn("statement.value", references(content["resources"]))
        self.assertNotIn("aws_lambda_function.r2_vend.arn", references(body("data.aws_iam_policy_document.lane")),
                         "the unqualified function would serve any lane")

    def test_a_source_key_s_slot_refuses_every_reader_but_the_lanes_that_name_it(self) -> None:
        deny = body("data.aws_iam_policy_document.source_secret")["statement"][0]
        self.assertEqual((deny["sid"], deny["effect"]), ("OnlyItsLanesRead", "Deny"))
        self.assertIn("aws_iam_role.lane", references(deny["condition"][0]["values"]))
        self.assertIn("each.value.lanes", references(deny["condition"][0]["values"]))
        self.assertEqual(body("aws_secretsmanager_secret_policy.source")["block_public_policy"], True)

    def test_lane_roles_sit_under_the_contract_s_run_path(self) -> None:
        self.assertEqual(body("aws_iam_role.lane")["path"], "${local.contract.ecs.run_role_path}")
        self.assertEqual(body("aws_iam_role.lane")["for_each"], "${local.collect.lanes}")

    def test_a_lane_is_data_no_block_names_one(self) -> None:
        stack = json.dumps(aws_stack.stack())
        for lane in lanes():
            self.assertNotIn(lane, stack)

    def test_the_state_prefix_is_generated_from_the_lane_s_id(self) -> None:
        vendor = body("local.vendor_lanes")
        self.assertIn("local.collect.state_prefix", references(vendor))
        for name, lane in lanes().items():
            for prefix in lane["write_prefixes"]:
                self.assertFalse(prefix.startswith(aws_stack.contract()["collect"]["state_prefix"]),
                                 f"{name} declares a state prefix: it is generated")


class EachLaneHasItsOwnScope(unittest.TestCase):
    def test_every_lane_is_a_collector_or_a_load_of_the_job_list(self) -> None:
        jobs = {job["id"]: job for job in json.loads((PLATFORM / "orchestration" / "jobs.v1.json").read_text(encoding="utf-8"))["jobs"]}
        for name, lane in lanes().items():
            if lane["handover"] == "none":
                # A source with no host copy (the RTMS lane): no job on the host list.
                self.assertNotIn(name, jobs)
                continue
            # A load (`started_by: inputs`, W109e) is a Silver or Gold job; every other lane collects.
            group = ("silver", "gold") if lane.get("started_by") == "inputs" else ("collect",)
            self.assertIn(jobs[name]["dagster_group"], group, name)

    def test_a_lane_writes_bronze_media_and_its_index_delta_only_and_names_no_table(self) -> None:
        # A lane reaches a table only through the Lakekeeper catalog, with the grants generated from the graph
        # (config/lakekeeper-declaration.v1.json); its object scope never names one. A load of a media index on that
        # catalog also writes the index's key deltas for the host's media reconciler (coordinator 2026-11-28).
        declaration = json.loads((PLATFORM / "config" / "lakekeeper-declaration.v1.json").read_text(encoding="utf-8"))
        for name, lane in lanes().items():
            self.assertTrue(lane["write_prefixes"], f"{name} has no write prefix")
            deltas = {f"control/media_index/{table.replace('.', '/')}/"
                      for table in declaration["lanes"].get(name, {}).get("write", [])}
            for prefix in lane["write_prefixes"]:
                self.assertTrue(BRONZE.match(prefix) or prefix in MEDIA or prefix in deltas, f"{name}: {prefix}")
            self.assertNotIn("read_tables", lane, f"{name}: a lane's tables are its catalog grants")
            for prefix in lane.get("read_prefixes", []):
                self.assertTrue(BRONZE.match(prefix), f"{name} reads {prefix}")

    def test_no_two_lanes_write_the_same_bronze_prefix(self) -> None:
        seen = {}
        for name, lane in lanes().items():
            for prefix in lane["write_prefixes"]:
                if prefix in MEDIA:
                    continue
                self.assertNotIn(prefix, seen, f"{name} and {seen.get(prefix)}")
                seen[prefix] = name

    def test_no_lane_reaches_court_auction(self) -> None:
        for name, lane in lanes().items():
            for scope in (*lane["write_prefixes"], *lane.get("read_prefixes", [])):
                self.assertNotIn("courtauction", scope.replace("_", ""), name)

    def test_a_source_key_is_declared_once_and_named_by_a_lane(self) -> None:
        declared = aws_stack.contract()["collect"]["source_secrets"]
        named = {name for lane in lanes().values() for name in lane["secrets"]}
        self.assertEqual(named, set(declared), "a key no lane reads, or a lane reading an undeclared key")


if __name__ == "__main__":
    unittest.main()
