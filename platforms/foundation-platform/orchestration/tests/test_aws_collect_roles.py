"""Only the credential vendor reads the Cloudflare tokens, and a lane's run reaches only its own lane (ADR-0623 §2).

infra/aws/collect.tf read the way test_aws_dagster_secrets.py reads the stack: no role but the vendor's names a
token; neither a lane's run role nor Dagster's roles can read one; a lane role may invoke its own alias and read
the database role's secret, nothing else, and sits under the run path the launcher passes; no block names a lane
(a lane is data); and each lane's scope in the contract is the one its source contract says.
"""

import json
import pathlib
import re
import unittest

from orchestration.tests.test_aws_dagster_secrets import CONTRACT, STACK, blocks

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
TOKENS = ["aws_secretsmanager_secret.r2_mint_token", "aws_secretsmanager_secret.r2_catalog_reader_token"]


class OnlyTheVendorReadsTheTokens(unittest.TestCase):
    def setUp(self) -> None:
        self.blocks = [block for path in sorted(STACK.glob("*.tf")) for block in blocks(path.read_text(encoding="utf-8"))]
        self.by_header = dict(self.blocks)
        self.contract = json.loads(CONTRACT.read_text(encoding="utf-8"))

    def test_the_vendor_reads_the_two_tokens(self) -> None:
        vendor = self.by_header["data aws_iam_policy_document r2_vend"]
        self.assertEqual(re.findall(r"(aws_secretsmanager_secret\.\w+)\.arn", vendor), TOKENS)

    def test_no_other_block_names_a_token_s_arn(self) -> None:
        for token in TOKENS:
            naming = [header for header, body in self.blocks if f"{token}.arn" in body]
            self.assertEqual(naming, ["data aws_iam_policy_document r2_vend"], token)

    def test_neither_a_lane_s_run_role_nor_dagster_s_roles_can_read_a_token(self) -> None:
        for role in ("lane", "dagster", "dagster_execution"):
            policy = self.by_header[f"data aws_iam_policy_document {role}"]
            for token in TOKENS:
                self.assertNotIn(token, policy, f"the {role} role reads {token}")

    def test_a_lane_role_asks_only_its_own_alias_and_reads_only_the_database_role(self) -> None:
        lane = self.by_header["data aws_iam_policy_document lane"]
        self.assertEqual(re.findall(r'actions\s*=\s*\[([^\]]*)\]', lane),
                         ['"lambda:InvokeFunction"', '"secretsmanager:GetSecretValue"'])
        self.assertIn("resources = [aws_lambda_alias.lane[each.key].arn]", lane)
        self.assertEqual(re.findall(r"(aws_secretsmanager_secret\.\w+)\.arn", lane),
                         ["aws_secretsmanager_secret.foundation_runtime"])
        self.assertNotIn("aws_lambda_function.r2_vend.arn", lane, "the unqualified function would serve any lane")

    def test_lane_roles_sit_under_the_run_path(self) -> None:
        self.assertIn("path               = local.run_role_path", self.by_header["resource aws_iam_role lane"])
        shared = next(body for header, body in self.blocks if header == "locals" and "run_role_path" in body)
        self.assertIn('run_role_path    = "/${local.project}/run/"', shared)

    def test_a_lane_is_data_no_block_names_one(self) -> None:
        stack = (STACK / "collect.tf").read_text(encoding="utf-8")
        for lane in self.contract["collect"]["lanes"]:
            self.assertNotIn(lane, stack)


def derived_scope(lane):
    """(write prefixes, read tables) as the lane's source contract says them: its Bronze sources (the provider
    id is the approved domain without its dots, collection-domain source_slug.rs), the media store it puts
    attachments in, and the table it reads what it owes from."""
    contracts = PLATFORM / "infra" / "lakehouse" / "contracts"
    source = json.loads((contracts / lane["source_contract"]).read_text(encoding="utf-8"))
    section = source["sources"][lane["source"]]
    provider = section["provider"].replace(".", "")
    prefixes = [f"bronze/source={provider}__{section['list']['dataset_slug']}/",
                f"bronze/source={provider}__{section['detail_dataset_slug']}/"]
    pointer_file, pointer = source["attachments"]["object_prefix_from"].split("#", 1)
    media = json.loads((contracts / pointer_file).read_text(encoding="utf-8"))
    for part in pointer.strip("/").split("/"):
        media = media[part]
    return [*prefixes, media.rstrip("/") + "/"], [source["read_from"]]


class EachLaneHasTheScopeItsSourceSays(unittest.TestCase):
    def test_the_scope_the_vendor_serves_is_the_source_contract_s(self) -> None:
        lanes = json.loads(CONTRACT.read_text(encoding="utf-8"))["collect"]["lanes"]
        self.assertTrue(lanes)
        for name, lane in lanes.items():
            self.assertEqual(derived_scope(lane), (lane["write_prefixes"], lane["read_tables"]), name)
            self.assertTrue(lane["write_prefixes"], f"{name} has no write prefix")

    def test_no_lane_but_court_auction_s_reaches_court_auction(self) -> None:
        for name, lane in json.loads(CONTRACT.read_text(encoding="utf-8"))["collect"]["lanes"].items():
            if "court_auction" in name:
                continue
            for scope in (*lane["write_prefixes"], *lane["read_tables"]):
                self.assertNotIn("courtauction", scope.replace("_", ""), name)


if __name__ == "__main__":
    unittest.main()
