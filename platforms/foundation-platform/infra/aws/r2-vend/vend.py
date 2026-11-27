"""The credential vendor (root ADR-0623 §2): the one way an AWS job reaches the lakehouse, the way Lake Formation
or Unity Catalog vend scoped credentials.

This function alone reads the minting token (its own execution role, infra/aws/collect.tf; the token's slot
refuses every other reader). A lane's run invokes it through that lane's alias, which its task role alone may
invoke: the lane is the alias, never the request body (§2-2). One call, `credentials`, with the run's id: R2
temporary credentials, object read-write on the lane's write prefixes (its Bronze and media prefixes and its own
state prefix, infra/aws/collect.tf) and, for a lane that reads another source's objects, object read-only on its
read prefixes, for the platform's one lifetime (CREDENTIAL_TTL_SECONDS).

Only the object half is here (coordinator 2026-11-27): no lane reads a table, so there is no `pin` and no catalog
token. Which tables a lane may read, and at which snapshot, waits for the catalog decision (root ADR-0624).

A scope that is empty is refused, never widened to the bucket (§2-3). Before the owner has placed the token the
answer is `{"inert": "<secret>"}`. Each call writes one log line, the lane, the run, the call and the expiry,
never a value (§2-7).
"""

import datetime
import json
import os

import r2_credentials

CALLS = ("credentials",)


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


def minting(mint_value):
    """(account id, minting token), or None while it is not placed; and what is missing."""
    if not mint_value:
        return None, "r2-mint-token"
    account, separator, token = mint_value.partition(":")
    if not separator or not account or not token:
        return None, "r2-mint-token (not <account id>:<token>)"
    return (account, token), None


def vend(lane_name, call_name, lanes, bucket, ttl_seconds, mint_value, now=None, call=None):
    """The answer for one call of lane `lane_name`."""
    if call_name not in CALLS:
        raise Refused(f"no call {call_name!r}: this vendor serves objects only (root ADR-0624 decides tables)")
    lane = lanes[lane_name]
    if not lane.get("write_prefixes"):
        raise Refused(f"{lane_name} has no write prefix")
    if lane.get("read_tables"):
        raise Refused(f"{lane_name} names tables: the table half waits for the catalog decision (root ADR-0624)")
    held, missing = minting(mint_value)
    if held is None:
        return {"inert": missing}
    account, token = held
    call = call or r2_credentials.cloudflare
    parent = r2_credentials.token_id(token, account, call)
    issued = now or datetime.datetime.now(datetime.timezone.utc)
    expiration = (issued + datetime.timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")
    writer = r2_credentials.mint(token, account, bucket, "object-read-write", lane["write_prefixes"], ttl_seconds,
                                 call, parent)
    read_prefixes = lane.get("read_prefixes") or []
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
                      secret(client, os.environ["MINT_SECRET"]))
        record.update({key: answer[key] for key in ("inert", "expiration") if key in answer})
        return answer
    except (Refused, r2_credentials.MintError) as error:
        # The message names the lane, the HTTP status and Cloudflare's codes, never a value.
        record["refused"] = str(error)
        return {"refused": str(error)}
    finally:
        print(json.dumps({"vend": record}, sort_keys=True))
