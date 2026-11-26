"""Only the credential vendor reads the Cloudflare tokens, and a lane's run reaches only its own lane (ADR-0623 §2).

infra/aws/collect.tf as the parser reads the stack (aws_stack.py): only the vendor's blocks name a token; neither a
lane's run role nor Dagster's roles can read one; a lane role may invoke its own alias and read the database role's
secret, nothing else, and sits under the contract's run path, the only one Dagster may pass; no block names a lane
(a lane is data); and each lane's scope in the contract is the one its source contract says.
"""

import json
import pathlib
import unittest

import aws_stack
from aws_stack import body, naming, references, statements

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
TOKENS = ["aws_secretsmanager_secret.r2_mint_token", "aws_secretsmanager_secret.r2_catalog_reader_token"]
VENDOR = ["aws_lambda_function.r2_vend", "data.aws_iam_policy_document.r2_vend"]


class OnlyTheVendorReadsTheTokens(unittest.TestCase):
    def test_the_vendor_reads_the_two_tokens(self) -> None:
        reads = [statement for statement in statements("data.aws_iam_policy_document.r2_vend").values()
                 if "secretsmanager:GetSecretValue" in statement["actions"]]
        self.assertEqual([statement["resources"] for statement in reads], [[f"${{{token}.arn}}" for token in TOKENS]])

    def test_only_the_vendor_s_blocks_name_a_token(self) -> None:
        for token in TOKENS:
            self.assertEqual(sorted(naming(token)), VENDOR, token)

    def test_neither_a_lane_s_run_role_nor_dagster_s_roles_can_read_a_token(self) -> None:
        for role in ("lane", "dagster", "dagster_execution", "run_execution"):
            named = references(body(f"data.aws_iam_policy_document.{role}"))
            for token in TOKENS:
                self.assertFalse([name for name in named if name.startswith(token)], f"the {role} role reads {token}")

    def test_a_lane_role_asks_only_its_own_alias_and_reads_only_the_database_role(self) -> None:
        lane = statements("data.aws_iam_policy_document.lane")
        self.assertEqual({sid: (statement["actions"], statement["resources"]) for sid, statement in lane.items()}, {
            "AskTheVendorForThisLane": (["lambda:InvokeFunction"], ["${aws_lambda_alias.lane[each.key].arn}"]),
            "ReadTheDatabaseRole": (["secretsmanager:GetSecretValue"], ["${aws_secretsmanager_secret.foundation_runtime.arn}"]),
        })
        self.assertNotIn("aws_lambda_function.r2_vend.arn", references(body("data.aws_iam_policy_document.lane")),
                         "the unqualified function would serve any lane")

    def test_lane_roles_sit_under_the_contract_s_run_path(self) -> None:
        self.assertEqual(body("aws_iam_role.lane")["path"], "${local.contract.ecs.run_role_path}")
        self.assertEqual(body("aws_iam_role.lane")["for_each"], "${local.collect.lanes}")

    def test_a_lane_is_data_no_block_names_one(self) -> None:
        stack = json.dumps(aws_stack.stack())
        for lane in aws_stack.contract()["collect"]["lanes"]:
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
        lanes = aws_stack.contract()["collect"]["lanes"]
        self.assertTrue(lanes)
        for name, lane in lanes.items():
            self.assertEqual(derived_scope(lane), (lane["write_prefixes"], lane["read_tables"]), name)
            self.assertTrue(lane["write_prefixes"], f"{name} has no write prefix")

    def test_no_lane_but_court_auction_s_reaches_court_auction(self) -> None:
        for name, lane in aws_stack.contract()["collect"]["lanes"].items():
            if "court_auction" in name:
                continue
            for scope in (*lane["write_prefixes"], *lane["read_tables"]):
                self.assertNotIn("courtauction", scope.replace("_", ""), name)


if __name__ == "__main__":
    unittest.main()
