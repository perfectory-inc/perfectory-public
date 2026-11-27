"""The credential vendor serves a lane its own objects and nothing else (root ADR-0623 §2, object half).

infra/aws/r2-vend (vend.py, r2_credentials.py) with Cloudflare and Secrets Manager replaced: the lane is the
alias the call came through; `credentials` are read-write on the lane's write prefixes and read-only on its read
prefixes only, with an expiration from the one platform lifetime; an empty scope is refused, never widened; a lane
naming tables and the `pin` call are refused (the table half waits for root ADR-0624); before the owner places the
token it is inert; every call logs the lane, the run and the expiry, never a value.
"""

import contextlib
import datetime
import io
import json
import os
import pathlib
import sys
import types
import unittest
import unittest.mock

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PLATFORM / "infra" / "aws" / "r2-vend"))
import r2_credentials  # noqa: E402
import vend  # noqa: E402

BUCKET = "foundation-platform-lakehouse-prod"
ACCOUNT = "synthetic-account"
LANES = {"writes": {"write_prefixes": ["bronze/source=a__list/", "onbid/media/", "collector-state/writes/"],
                    "read_prefixes": []},
         "reads_too": {"write_prefixes": ["bronze/source=b__x/"], "read_prefixes": ["bronze/source=c__profile/"]},
         "no_scope": {"write_prefixes": [], "read_prefixes": []},
         "tables": {"write_prefixes": ["bronze/source=d__x/"], "read_tables": ["silver.t"]}}
MINT = f"{ACCOUNT}:mint-token"
NOW = datetime.datetime(2026, 11, 27, 8, 0, tzinfo=datetime.timezone.utc)
FUNCTION = "arn:aws:lambda:ap-northeast-2:091109217099:function:perfectory-platform-r2-vend"


class Cloudflare:
    def __init__(self, refuse=False):
        self.calls, self.refuse = [], refuse

    def __call__(self, token, method, path, body=None):
        self.calls.append((token, path, body))
        if path == "/user/tokens/verify":
            return 401, {"success": False, "errors": [{"code": 1000}]}
        if path.endswith("/tokens/verify"):
            # An account-owned token (perfectory-r2-vend-prod) verifies on the account's endpoint.
            return 200, {"success": True, "result": {"id": "token-id"}}
        if self.refuse:
            return 403, {"success": False, "errors": [{"code": 10042}]}
        n = len(self.calls)
        return 200, {"success": True, "result": {"accessKeyId": f"key-{n}", "secretAccessKey": f"secret-{n}",
                                                  "sessionToken": f"session-{n}"}}

    def minted(self):
        return [body for _, path, body in self.calls if path.endswith("/r2/temp-access-credentials")]


def ask(lane, call_name="credentials", cloudflare=None, mint=MINT, ttl=3600):
    return vend.vend(lane, call_name, LANES, BUCKET, ttl, mint, NOW, cloudflare or Cloudflare())


class TheLaneIsTheAlias(unittest.TestCase):
    def test_the_alias_names_the_lane(self):
        self.assertEqual(vend.lane_of(f"{FUNCTION}:writes", LANES), "writes")

    def test_no_alias_latest_an_unknown_alias_or_a_body_that_disagrees_is_refused(self):
        for arn, asked in ((FUNCTION, None), (f"{FUNCTION}:$LATEST", None), (f"{FUNCTION}:court_auction", None),
                           (f"{FUNCTION}:writes", "reads_too")):
            with self.assertRaises(vend.Refused, msg=arn):
                vend.lane_of(arn, LANES, asked)


class CredentialsOnTheLanesObjects(unittest.TestCase):
    def test_read_write_on_its_prefixes_with_one_lifetime_and_no_reader_when_it_reads_nothing(self):
        cloudflare = Cloudflare()
        answer = ask("writes", cloudflare=cloudflare)
        (writer,) = cloudflare.minted()
        self.assertEqual(writer, {"bucket": BUCKET, "parentAccessKeyId": "token-id", "permission": "object-read-write",
                                  "ttlSeconds": 3600, "prefixes": LANES["writes"]["write_prefixes"]})
        self.assertEqual(answer["expiration"], "2026-11-27T09:00:00Z")
        self.assertEqual(answer["writer"]["expiration"], answer["expiration"])
        self.assertIsNone(answer["reader"])
        self.assertNotIn("tables", answer)

    def test_read_only_on_its_read_prefixes(self):
        cloudflare = Cloudflare()
        answer = ask("reads_too", cloudflare=cloudflare)
        writer, reader = cloudflare.minted()
        self.assertEqual((writer["permission"], writer["prefixes"]), ("object-read-write", ["bronze/source=b__x/"]))
        self.assertEqual((reader["permission"], reader["prefixes"]), ("object-read-only", ["bronze/source=c__profile/"]))
        self.assertEqual(answer["reader"]["expiration"], answer["expiration"])


class TheVendorRefuses(unittest.TestCase):
    def test_an_empty_scope_is_refused_never_widened_to_the_bucket(self):
        with self.assertRaises(vend.Refused):
            ask("no_scope")
        for prefixes in ([], [""]):
            with self.assertRaises(r2_credentials.MintError):
                r2_credentials.mint("t", ACCOUNT, BUCKET, "object-read-only", prefixes, 60, Cloudflare(), parent="p")

    def test_the_table_half_is_not_served(self):
        for lane, call in (("writes", "pin"), ("tables", "credentials"), ("writes", "revoke")):
            cloudflare = Cloudflare()
            with self.assertRaises(vend.Refused, msg=(lane, call)):
                ask(lane, call, cloudflare)
            self.assertEqual(cloudflare.calls, [], "nothing is asked for a call it refuses")

    def test_a_refusal_names_codes_never_a_value(self):
        with self.assertRaises(r2_credentials.MintError) as refused:
            ask("writes", cloudflare=Cloudflare(refuse=True))
        self.assertIn("10042", str(refused.exception))
        self.assertNotIn("mint-token", str(refused.exception))

    def test_inert_before_the_owner_places_the_token(self):
        for mint in (None, "", "no-colon", ":token", "account:"):
            cloudflare = Cloudflare()
            self.assertIn("inert", ask("writes", cloudflare=cloudflare, mint=mint))
            self.assertEqual(cloudflare.calls, [], "nothing is asked before it is placed")

    def test_only_object_permissions_and_a_bounded_lifetime(self):
        for permission, ttl in (("admin-read-write", 60), ("object-read-only", 0),
                                ("object-read-only", r2_credentials.MAX_TTL_SECONDS + 1)):
            with self.assertRaises(r2_credentials.MintError):
                r2_credentials.mint("t", ACCOUNT, "b", permission, ["p/"], ttl, Cloudflare(), parent="p")


class EveryCallIsLoggedWithoutAValue(unittest.TestCase):
    def handle(self, event, arn):
        client = types.SimpleNamespace(
            get_secret_value=lambda SecretId: {"SecretString": {"mint": MINT}[SecretId]},  # noqa: N803
            exceptions=types.SimpleNamespace(ResourceNotFoundException=KeyError))
        boto3 = types.SimpleNamespace(client=lambda name: client)
        env = {"LANES": json.dumps(LANES), "BUCKET": BUCKET, "CREDENTIAL_TTL_SECONDS": "3600", "MINT_SECRET": "mint"}
        out = io.StringIO()
        with unittest.mock.patch.dict(os.environ, env), unittest.mock.patch.dict(sys.modules, {"boto3": boto3}), \
                unittest.mock.patch.object(r2_credentials, "cloudflare", Cloudflare()), contextlib.redirect_stdout(out):
            answer = vend.handler(event, types.SimpleNamespace(invoked_function_arn=arn))
        return answer, out.getvalue()

    def test_the_log_line_is_the_lane_the_run_the_call_and_the_expiry(self):
        answer, logged = self.handle({"call": "credentials", "run_id": "run-1"}, f"{FUNCTION}:reads_too")
        record = json.loads(logged)["vend"]
        self.assertEqual((record["lane"], record["run_id"], record["call"]), ("reads_too", "run-1", "credentials"))
        self.assertEqual(record["expiration"], answer["expiration"])
        for value in ("mint-token", answer["writer"]["secret_access_key"], answer["writer"]["session_token"],
                      answer["reader"]["secret_access_key"]):
            self.assertNotIn(value, logged)

    def test_a_call_without_a_run_id_or_through_no_alias_is_refused_and_logged(self):
        for event, arn in (({"call": "credentials"}, f"{FUNCTION}:writes"),
                           ({"call": "credentials", "run_id": "r"}, FUNCTION),
                           ({"call": "pin", "run_id": "r"}, f"{FUNCTION}:writes")):
            answer, logged = self.handle(event, arn)
            self.assertIn("refused", answer)
            self.assertIn("refused", json.loads(logged)["vend"])


class TheStackFeedsTheVendorTheContractsLanes(unittest.TestCase):
    def test_the_function_gets_no_catalog_secret_and_each_lane_its_state_prefix(self):
        import aws_stack

        variables = aws_stack.body("aws_lambda_function.r2_vend")["environment"][0]["variables"]
        self.assertEqual(sorted(variables), ["BUCKET", "CREDENTIAL_TTL_SECONDS", "LANES", "MINT_SECRET"])
        self.assertIn("local.vendor_lanes", aws_stack.references(variables["LANES"]))
        lanes = aws_stack.body("local.vendor_lanes")
        self.assertIn("local.collect.state_prefix", aws_stack.references(lanes))
        self.assertNotIn("read_tables", json.dumps(lanes))


if __name__ == "__main__":
    unittest.main()
