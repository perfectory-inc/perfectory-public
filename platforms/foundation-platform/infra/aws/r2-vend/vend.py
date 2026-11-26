"""The credential vendor (root ADR-0623 §2): the one way an AWS job reaches the lakehouse, the way Lake Formation
or Unity Catalog vend scoped credentials.

This function alone reads the two Cloudflare tokens (its own execution role, infra/aws/collect.tf). A lane's run
invokes it through that lane's alias, which its task role alone may invoke: the lane is the alias, never the
request body (§2-2). Two calls, both with the run's id:

- `pin`, once per run: for each table the lane reads, its current snapshot (`snapshot_id`,
  `snapshot_timestamp_ms`, `manifest_list_location`, `metadata_location`; null for a table with none). The run
  reads those snapshots to its end (the publisher's `vended` catalog provider; a DuckDB stage's `iceberg_scan` on
  the metadata location). Fetching credentials again never moves it (§2-4).
- `credentials`, at the start and, from step two, whenever they near their `expiration`: R2 temporary
  credentials, object read-write on the lane's write prefixes and object read-only on its tables' locations
  only, for the platform's one lifetime (CREDENTIAL_TTL_SECONDS).

A scope that is empty is refused, never widened to the bucket (§2-3). Before the owner has placed a token the
answer is `{"inert": "<secret>"}`. Each call writes one log line, the lane, the run, the call and the expiry,
never a value (§2-7).
"""

import datetime
import json
import os

import iceberg_catalog
import r2_credentials

CALLS = ("pin", "credentials")


class Refused(Exception):
    """An invocation this function does not serve."""


def secret(client, name):
    """The secret's value, or None while it has none."""
    try:
        return client.get_secret_value(SecretId=name)["SecretString"].strip()
    except client.exceptions.ResourceNotFoundException:
        return None


def lane_of(invoked_function_arn, lanes, asked=None):
    """The lane an invocation is for: the alias it came through (`arn:...:function:<name>:<alias>`)."""
    parts = (invoked_function_arn or "").split(":")
    alias = parts[7] if len(parts) == 8 else None
    if not alias or alias == "$LATEST" or alias not in lanes:
        raise Refused("invoked without a lane's alias")
    if asked not in (None, alias):
        raise Refused(f"the alias is {alias}, the request names {asked}")
    return alias


def location_prefix(location, bucket):
    """The key prefix of a table's storage location, which must be in the lakehouse bucket."""
    scheme_bucket = f"s3://{bucket}/"
    if not location.startswith(scheme_bucket) or len(location) <= len(scheme_bucket):
        raise Refused(f"a table location outside {bucket}")
    return location[len(scheme_bucket):].rstrip("/") + "/"


def minting(secrets):
    """(account id, minting token, catalog token), or None while one is not placed; and what is missing."""
    mint_value, catalog_token = secrets
    if not mint_value:
        return None, "r2-mint-token"
    if not catalog_token:
        return None, "r2-catalog-reader-token"
    account, separator, token = mint_value.partition(":")
    if not separator or not account or not token:
        return None, "r2-mint-token (not <account id>:<token>)"
    return (account, token, catalog_token), None


def resolve(lane, account, bucket, catalog_token, get):
    """{table: {location, snapshot}} for the tables the lane reads."""
    if not lane["read_tables"]:
        return {}
    uri, warehouse = iceberg_catalog.catalog_uri(account, bucket)
    catalog_prefix = iceberg_catalog.prefix(catalog_token, uri, warehouse, get)
    return {table: iceberg_catalog.current(catalog_token, account, bucket, table, get, catalog_prefix)
            for table in lane["read_tables"]}


def vend(lane_name, call_name, lanes, bucket, ttl_seconds, secrets, now=None, call=None, get=None):
    """The answer for one call of lane `lane_name`."""
    if call_name not in CALLS:
        raise Refused(f"no call {call_name!r}")
    lane = lanes[lane_name]
    if not lane.get("write_prefixes"):
        raise Refused(f"{lane_name} has no write prefix")
    held, missing = minting(secrets)
    if held is None:
        return {"inert": missing}
    account, token, catalog_token = held
    call = call or r2_credentials.cloudflare
    get = get or iceberg_catalog.http
    tables = resolve(lane, account, bucket, catalog_token, get)
    if call_name == "pin":
        return {"account_id": account, "bucket": bucket,
                "tables": {table: resolved["snapshot"] for table, resolved in tables.items()}}
    read_prefixes = [location_prefix(resolved["location"], bucket) for resolved in tables.values() if resolved["location"]]
    parent = r2_credentials.token_id(token, account, call)
    issued = now or datetime.datetime.now(datetime.timezone.utc)
    expiration = (issued + datetime.timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")
    writer = r2_credentials.mint(token, account, bucket, "object-read-write", lane["write_prefixes"], ttl_seconds,
                                 call, parent)
    reader = (r2_credentials.mint(token, account, bucket, "object-read-only", read_prefixes, ttl_seconds, call, parent)
              if read_prefixes else None)
    return {"account_id": account, "bucket": bucket, "expiration": expiration,
            "writer": {**writer, "expiration": expiration},
            "reader": None if reader is None else {**reader, "expiration": expiration}}


def handler(event, context):
    import boto3

    lanes = json.loads(os.environ["LANES"])
    call_name, run_id = event.get("call"), event.get("run_id")
    record = {"call": call_name, "run_id": run_id}
    try:
        lane = lane_of(getattr(context, "invoked_function_arn", ""), lanes, event.get("lane"))
        record["lane"] = lane
        if not isinstance(run_id, str) or not run_id:
            raise Refused("no run id")
        client = boto3.client("secretsmanager")
        answer = vend(lane, call_name, lanes, os.environ["BUCKET"], int(os.environ["CREDENTIAL_TTL_SECONDS"]),
                      (secret(client, os.environ["MINT_SECRET"]), secret(client, os.environ["CATALOG_SECRET"])))
        record.update({key: answer[key] for key in ("inert", "expiration") if key in answer})
        if "tables" in answer:
            record["tables"] = len(answer["tables"])
        return answer
    except (Refused, r2_credentials.MintError, iceberg_catalog.CatalogError) as error:
        # The message names the lane, the HTTP status and Cloudflare's codes, never a value.
        record["refused"] = str(error)
        return {"refused": str(error)}
    finally:
        print(json.dumps({"vend": record}, sort_keys=True))
