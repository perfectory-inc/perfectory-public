#!/usr/bin/env python3
"""Pages what a provider wants from its account's owner (root ADR-0363).

2026-10-30 13:07Z VWorld answered the collection account's login `useChk` (new terms to accept). The
2026-10-30 deploy stopped at the staging smoke and every VWorld collection failed with
"failed to acquire VWorld dataset login session" — an error no release could clear, saying nothing
about who could. The publisher now ends such a run with exit 77 and one line,

    foundation-provider-outcome {"outcome": "provider_account_action_required", "provider": "vworld.kr", "reason": "useChk", ...}

and a login that succeeds prints {"outcome": "ok", "provider": ...}. This script reads those lines
from a run log and holds one Alertmanager alert per provider (the path notify-unit-failure.sh posts
on): firing while the provider refuses, with the owner action config/provider-accounts.contract.json
gives for its reason; resolved by the next login that succeeds. Every job that logs in calls it, so
the owner is paged once per provider, not once per job.

    provider_alert.py accounts --source <job> <run log>...
        Prints "<provider>: <owner action>" for each provider that refuses (joined by " / "), the
        reason a job ends on. Exit 1 when Alertmanager did not take an alert: the caller then fails
        loudly instead of waiting quietly.
"""

import argparse
import datetime
import json
import os
import pathlib
import sys
import urllib.request

PREFIX = "foundation-provider-outcome "
CONTRACT = pathlib.Path(__file__).resolve().parents[2] / "config" / "provider-accounts.contract.json"
RUNBOOK = "docs/runbooks/provider-outage-and-quota.md#계정-조치-root-adr-0363"


def outcomes(paths):
    """The last outcome per provider, in the order the run logs wrote them."""
    last = {}
    for path in paths:
        try:
            lines = pathlib.Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.startswith(PREFIX):
                continue
            try:
                outcome = json.loads(line[len(PREFIX):])
            except ValueError:
                continue
            if isinstance(outcome, dict) and outcome.get("provider"):
                last[outcome["provider"]] = outcome
    return last


def owner_action(contract, provider, reason):
    actions = contract["providers"].get(provider, {}).get("owner_actions", {})
    return actions.get(reason) or actions.get("*") or (
        f"{provider} 에 수집 계정으로 로그인해 사이트가 요구하는 조치를 마치세요 — 이 제공자는 "
        "config/provider-accounts.contract.json 에 없어요")


def post(alerts):
    port = os.environ.get("FOUNDATION_ALERTMANAGER_PORT", "19093")
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/v2/alerts", data=json.dumps(alerts, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return 200 <= response.status < 300
    except OSError as error:
        print(f"provider-alert: Alertmanager did not take the alert: {error}", file=sys.stderr)
        return False


def alert(labels, summary, firing, hold_hours):
    now = datetime.datetime.now(datetime.timezone.utc)
    ends = now + datetime.timedelta(hours=hold_hours) if firing else now
    return {"labels": {"environment": "prelaunch", **labels},
            "annotations": {"summary": summary, "runbook": RUNBOOK},
            "endsAt": ends.strftime("%Y-%m-%dT%H:%M:%S.000Z")}


def accounts(args):
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    settings = contract["alert"]
    alerts, actions = [], []
    for provider, outcome in outcomes(args.run_logs).items():
        labels = {"alertname": settings["alertname"], "service": settings["service"], "provider": provider}
        if outcome.get("outcome") == "ok":
            alerts.append(alert(labels, f"{provider} 로그인이 다시 돼요", False, 0))
        elif outcome.get("outcome") == "provider_account_action_required":
            reason = str(outcome.get("reason") or "unknown")
            action = owner_action(contract, provider, reason)
            actions.append(f"{provider}: {action}")
            jobs = ", ".join(contract["providers"].get(provider, {}).get("jobs", [])) or "그 제공자의 수집"
            alerts.append(alert(labels, f"🔑 {provider} 가 수집 계정을 거부해요 (result={reason}, {args.source} 에서 봄) — "
                                f"{action}. 그때까지 {jobs} 가 이 제공자에서 받지 못해요",
                                True, settings["hold_hours"]))
    print(" / ".join(actions))
    return 0 if not alerts or post(alerts) else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    account = commands.add_parser("accounts")
    account.add_argument("--source", required=True)
    account.add_argument("run_logs", nargs="+")
    account.set_defaults(handler=accounts)
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
