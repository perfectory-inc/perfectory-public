#!/usr/bin/env python3
"""Check the fail-closed prepare/publish/resume ordering of the public publisher."""

from __future__ import annotations

import re
import sys
from pathlib import Path


def fail(message: str) -> None:
    print(f"FAIL public-root-publisher-policy: {message}", file=sys.stderr)
    raise SystemExit(1)


def position(source: str, needle: str, *, after: int = 0) -> int:
    found = source.find(needle, after)
    if found < 0:
        fail(f"missing invariant: {needle}")
    return found


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <publisher-script>", file=sys.stderr)
        return 2
    source = Path(sys.argv[1]).read_text(encoding="utf-8")
    if 'target="perfectory-inc/perfectory-public"' not in source \
            or 'remote_url="https://github.com/${target}.git"' not in source:
        fail("publisher target is not the canonical public repository")
    if "git remote add" in source:
        fail("publisher must never add a remote to the private-history source")
    if "perfectory.publicAudit." in source:
        fail("forgeable local Git config must not authorize publication")
    if not re.search(r'^if \[ "\$#" -ne 2 \]; then$', source, re.MULTILINE):
        fail("publisher must accept only source repository plus source commit")

    for invariant in (
        'prepare="$root/scripts/github/prepare-public-root.sh"',
        'publication_authority="$root/scripts/github/check-publication-authority.sh"',
        'snapshot="$publication_root/public-root.git"',
        'verification_clone="$publication_root/verification-clone"',
        'control_root="$("$git_transport" --repository "$root" rev-parse --show-toplevel',
        'if [ "$source_root" != "$control_root" ] || [ "$control_root" != "$root" ]; then',
        'status --porcelain=v1 --untracked-files=all',
        '"$source_commit" != "$source_head"',
        '"$snapshot_tree" != "$source_tree"',
        'PERFECTORY_EXPECTED_PUBLIC_ROOT="$snapshot_commit"',
        'safe-git-transport.sh',
        "expected_remote_state=\"$(printf 'ref: refs/heads/main\\tHEAD\\n%s\\tHEAD\\n%s\\trefs/heads/main'",
        '"$snapshot_commit" "$snapshot_commit")"',
        'elif [ "$remote_state" = "$expected_remote_state" ]; then',
        'publication_mode=resume',
        'FAIL public-root-publisher: remote is neither empty nor the exact expected root',
        '[ "$main_locked" -eq 0 ]',
        '[ "$exit_code" -ne 0 ]',
    ):
        if invariant not in source:
            fail(f"missing invariant: {invariant}")

    retry_block = (
        'if [ "$publication_started" -eq 1 ] \\\n'
        '    && [ "$main_locked" -eq 0 ] \\\n'
        '    && [ "$exit_code" -ne 0 ]; then'
    )
    if retry_block not in source:
        fail("ambiguous fresh publication must retry the immediate main lock")

    control_validation = position(
        source,
        'if [ "$source_root" != "$control_root" ] || [ "$control_root" != "$root" ]; then',
    )
    prepare = position(source, '"$prepare" "$source_root" "$2" "$snapshot"')
    source_validation = position(
        source,
        '"$snapshot_tree" != "$source_tree"',
        after=prepare,
    )
    remote_query = position(
        source,
        '"$git_transport" --no-repository ls-remote --symref "$remote_url"',
        after=source_validation,
    )
    fresh_decision = position(source, 'if [ -z "$remote_state" ]; then', after=remote_query)
    exact_resume = position(
        source,
        'elif [ "$remote_state" = "$expected_remote_state" ]; then',
        after=fresh_decision,
    )
    rejection = position(
        source,
        'FAIL public-root-publisher: remote is neither empty nor the exact expected root',
        after=exact_resume,
    )
    rejection_exit = position(source, "exit 1", after=rejection)
    fresh_branch = position(
        source,
        'if [ "$publication_mode" = fresh ]; then',
        after=rejection_exit,
    )
    prepublish = position(source, '"$configurator" prepublish', after=fresh_branch)
    authority = position(source, '"$publication_authority"', after=prepublish)
    armed = position(source, "publication_started=1", after=authority)
    push = position(
        source,
        '"$git_transport" --repository "$snapshot" push "$remote_url"',
        after=armed,
    )
    lock = position(source, '"$configurator" lock', after=push)
    lock_confirmed = position(source, "main_locked=1", after=lock)
    fresh_end = position(source, "fi", after=lock_confirmed)
    clone = position(
        source,
        'clone --quiet "$remote_url" "$verification_clone"',
        after=fresh_end,
    )
    clone_rejected = position(
        source,
        "FAIL public-root-publisher: independent clone invariant failed",
        after=clone,
    )
    activate = position(source, '"$configurator" activate', after=clone_rejected)
    disarmed = position(source, "publication_started=0", after=activate)
    if not (
        control_validation
        < prepare
        < source_validation
        < remote_query
        < fresh_decision
        < exact_resume
        < rejection
        < rejection_exit
        < fresh_branch
        < prepublish
        < authority
        < armed
        < push
        < lock
        < lock_confirmed
        < fresh_end
        < clone
        < clone_rejected
        < activate
        < disarmed
    ):
        fail(
            "publisher must prepare, decide exact remote state, then fresh-authorize/"
            "push/lock or resume before clone-verification and activation"
        )

    decision_region = source[remote_query:fresh_branch]
    if "grep" in decision_region or "== *" in decision_region:
        fail("remote resume decision must use exact whole-output equality")

    print("OK public-root-publisher-policy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
