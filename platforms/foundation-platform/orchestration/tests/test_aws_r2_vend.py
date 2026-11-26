"""The credential vendor serves a lane its own scope and nothing else (root ADR-0623 §2).

infra/aws/r2-vend (vend.py, iceberg_catalog.py, r2_credentials.py) with Cloudflare, the catalog and Secrets
Manager replaced: the lane is the alias the call came through; `pin` returns the snapshots once and
`credentials` never moves them; credentials are read-write on the lane's prefixes and read-only on its tables'
locations only, with an expiration from the one platform lifetime; an empty scope is refused, never widened;
before the owner places the tokens it is inert; every call logs the lane, the run and the expiry, never a value.
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
import iceberg_catalog  # noqa: E402
import r2_credentials  # noqa: E402
import vend  # noqa: E402

BUCKET = "foundation-platform-lakehouse-prod"
ACCOUNT = "synthetic-account"
TABLE = "silver.industrial_land_sale_notice_read"
LOCATION = f"s3://{BUCKET}/__r2_data_catalog/w/n/t-uuid"
LANES = {"ilis": {"write_prefixes": ["bronze/source=a__list/", "onbid/media/"], "read_tables": [TABLE]},
         "writes_only": {"write_prefixes": ["bronze/source=b__x/"], "read_tables": []},
         "no_scope": {"write_prefixes": [], "read_tables": [TABLE]}}
SECRETS = (f"{ACCOUNT}:mint-token", "catalog-token")
NOW = datetime.datetime(2026, 11, 26, 8, 0, tzinfo=datetime.timezone.utc)
FUNCTION = "arn:aws:lambda:ap-northeast-2:091109217099:function:perfectory-platform-r2-vend"


class Cloudflare:
    def __init__(self, refuse=False):
        self.calls, self.refuse = [], refuse

    def __call__(self, token, method, path, body=None):
        self.calls.append((token, path, body))
        if path == "/user/tokens/verify":
            return 200, {"success": True, "result": {"id": "token-id"}}
        if self.refuse:
            return 403, {"success": False, "errors": [{"code": 10042}]}
        n = len(self.calls)
        return 200, {"success": True, "result": {"accessKeyId": f"key-{n}", "secretAccessKey": f"secret-{n}",
                                                  "sessionToken": f"session-{n}"}}

    def minted(self):
        return [body for _, path, body in self.calls if path.endswith("/r2/temp-access-credentials")]


class Catalog:
    """R2 Data Catalog's config and load-table answers; `snapshot` is the table's current one."""

    def __init__(self, snapshot=42, location=LOCATION, status=200):
        self.snapshot, self.location, self.status, self.urls = snapshot, location, status, []

    def __call__(self, token, url):
        self.urls.append((token, url))
        if "/v1/config?" in url:
            return 200, {"overrides": {"prefix": "the-prefix"}}
        if self.status != 200:
            return self.status, {}
        snapshots = [{"snapshot-id": s, "timestamp-ms": 1764115200000 + s, "manifest-list": f"{self.location}/metadata/snap-{s}.avro"}
                     for s in (41, 42, 43)]
        current = -1 if self.snapshot is None else self.snapshot
        return 200, {"metadata-location": f"{self.location}/metadata/{current}.metadata.json",
                     "metadata": {"location": self.location, "current-snapshot-id": current, "snapshots": snapshots}}


def ask(lane, call_name, cloudflare=None, catalog=None, secrets=SECRETS, ttl=3600):
    return vend.vend(lane, call_name, LANES, BUCKET, ttl, secrets, NOW, cloudflare or Cloudflare(), catalog or Catalog())


class TheLaneIsTheAlias(unittest.TestCase):
    def test_the_alias_names_the_lane(self):
        self.assertEqual(vend.lane_of(f"{FUNCTION}:ilis", LANES), "ilis")

    def test_no_alias_latest_an_unknown_alias_or_a_body_that_disagrees_is_refused(self):
        for arn, asked in ((FUNCTION, None), (f"{FUNCTION}:$LATEST", None), (f"{FUNCTION}:court_auction", None),
                           (f"{FUNCTION}:ilis", "writes_only")):
            with self.assertRaises(vend.Refused, msg=arn):
                vend.lane_of(arn, LANES, asked)


class PinOnceCredentialsOften(unittest.TestCase):
    def test_the_pin_is_each_table_s_current_snapshot_with_its_metadata_location(self):
        self.assertEqual(ask("ilis", "pin")["tables"], {TABLE: {
            "snapshot_id": 42, "snapshot_timestamp_ms": 1764115200042,
            "manifest_list_location": f"{LOCATION}/metadata/snap-42.avro",
            "metadata_location": f"{LOCATION}/metadata/42.metadata.json"}})

    def test_credentials_carry_no_snapshot_so_fetching_them_again_never_moves_the_pin(self):
        pin = ask("ilis", "pin", catalog=Catalog(snapshot=42))["tables"]
        later = ask("ilis", "credentials", catalog=Catalog(snapshot=43))
        self.assertNotIn("tables", later)
        self.assertEqual(pin[TABLE]["snapshot_id"], 42)

    def test_read_write_on_its_prefixes_read_only_on_its_tables_only_with_one_lifetime(self):
        cloudflare = Cloudflare()
        answer = ask("ilis", "credentials", cloudflare, ttl=3600)
        writer, reader = cloudflare.minted()
        self.assertEqual(writer, {"bucket": BUCKET, "parentAccessKeyId": "token-id", "permission": "object-read-write",
                                  "ttlSeconds": 3600, "prefixes": LANES["ilis"]["write_prefixes"]})
        self.assertEqual(reader, {"bucket": BUCKET, "parentAccessKeyId": "token-id", "permission": "object-read-only",
                                  "ttlSeconds": 3600, "prefixes": ["__r2_data_catalog/w/n/t-uuid/"]})
        self.assertEqual(answer["expiration"], "2026-11-26T09:00:00Z")
        self.assertEqual((answer["writer"]["expiration"], answer["reader"]["expiration"]), (answer["expiration"],) * 2)

    def test_a_table_without_a_snapshot_is_null_and_a_lane_reading_nothing_gets_no_reader(self):
        self.assertEqual(ask("ilis", "pin", catalog=Catalog(snapshot=None))["tables"], {TABLE: None})
        missing = ask("ilis", "credentials", catalog=Catalog(status=404))
        self.assertIsNone(missing["reader"])
        cloudflare = Cloudflare()
        self.assertIsNone(ask("writes_only", "credentials", cloudflare)["reader"])
        self.assertEqual([body["permission"] for body in cloudflare.minted()], ["object-read-write"])


class TheVendorRefuses(unittest.TestCase):
    def test_an_empty_scope_is_refused_never_widened_to_the_bucket(self):
        with self.assertRaises(vend.Refused):
            ask("no_scope", "credentials")
        for prefixes in ([], [""]):
            with self.assertRaises(r2_credentials.MintError):
                r2_credentials.mint("t", ACCOUNT, BUCKET, "object-read-only", prefixes, 60, Cloudflare(), parent="p")
        cloudflare = Cloudflare()
        r2_credentials.mint("t", ACCOUNT, BUCKET, "object-read-only", [], 60, cloudflare, parent="p", whole_bucket=True)
        self.assertNotIn("prefixes", cloudflare.minted()[0])

    def test_a_table_outside_the_bucket_and_an_unknown_call(self):
        with self.assertRaises(vend.Refused):
            ask("ilis", "credentials", catalog=Catalog(location="s3://another-bucket/t"))
        with self.assertRaises(vend.Refused):
            ask("ilis", "revoke")

    def test_a_refusal_names_codes_never_a_value(self):
        with self.assertRaises(r2_credentials.MintError) as refused:
            ask("ilis", "credentials", Cloudflare(refuse=True))
        self.assertIn("10042", str(refused.exception))
        self.assertNotIn("mint-token", str(refused.exception))
        with self.assertRaises(iceberg_catalog.CatalogError) as catalog:
            ask("ilis", "pin", catalog=Catalog(status=500))
        self.assertNotIn("catalog-token", str(catalog.exception))

    def test_inert_before_the_owner_places_the_tokens(self):
        for secrets in ((None, "catalog-token"), (SECRETS[0], None), ("no-colon", "catalog-token")):
            cloudflare = Cloudflare()
            self.assertIn("inert", ask("ilis", "credentials", cloudflare, secrets=secrets))
            self.assertEqual(cloudflare.calls, [], "nothing is asked before both are placed")

    def test_only_object_permissions_and_a_bounded_lifetime(self):
        for permission, ttl in (("admin-read-write", 60), ("object-read-only", 0),
                                ("object-read-only", r2_credentials.MAX_TTL_SECONDS + 1)):
            with self.assertRaises(r2_credentials.MintError):
                r2_credentials.mint("t", ACCOUNT, "b", permission, ["p/"], ttl, Cloudflare(), parent="p")


class EveryCallIsLoggedWithoutAValue(unittest.TestCase):
    def handle(self, event, arn):
        client = types.SimpleNamespace(
            get_secret_value=lambda SecretId: {"SecretString": {"mint": SECRETS[0], "catalog": SECRETS[1]}[SecretId]},  # noqa: N803
            exceptions=types.SimpleNamespace(ResourceNotFoundException=KeyError))
        boto3 = types.SimpleNamespace(client=lambda name: client)
        env = {"LANES": json.dumps(LANES), "BUCKET": BUCKET, "CREDENTIAL_TTL_SECONDS": "3600",
               "MINT_SECRET": "mint", "CATALOG_SECRET": "catalog"}
        out = io.StringIO()
        with unittest.mock.patch.dict(os.environ, env), unittest.mock.patch.dict(sys.modules, {"boto3": boto3}), \
                unittest.mock.patch.object(r2_credentials, "cloudflare", Cloudflare()), \
                unittest.mock.patch.object(iceberg_catalog, "http", Catalog()), contextlib.redirect_stdout(out):
            answer = vend.handler(event, types.SimpleNamespace(invoked_function_arn=arn))
        return answer, out.getvalue()

    def test_the_log_line_is_the_lane_the_run_the_call_and_the_expiry(self):
        answer, logged = self.handle({"call": "credentials", "run_id": "run-1"}, f"{FUNCTION}:ilis")
        record = json.loads(logged)["vend"]
        self.assertEqual((record["lane"], record["run_id"], record["call"]), ("ilis", "run-1", "credentials"))
        self.assertEqual(record["expiration"], answer["expiration"])
        for value in ("mint-token", "catalog-token", answer["writer"]["secret_access_key"],
                      answer["writer"]["session_token"], answer["reader"]["secret_access_key"]):
            self.assertNotIn(value, logged)

    def test_a_call_without_a_run_id_or_through_no_alias_is_refused_and_logged(self):
        for event, arn in (({"call": "pin"}, f"{FUNCTION}:ilis"), ({"call": "pin", "run_id": "r"}, FUNCTION)):
            answer, logged = self.handle(event, arn)
            self.assertIn("refused", answer)
            self.assertIn("refused", json.loads(logged)["vend"])


if __name__ == "__main__":
    unittest.main()
