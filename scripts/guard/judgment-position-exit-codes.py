#!/usr/bin/env python3
"""Keep a pipeline out of the position that decides whether a command succeeded.

[ADR-0012](../../docs/adr/0012-verification-results-must-mean-what-they-say.md) rule 2 forbids
judging a command through a filter: a pipeline's status is the *last* stage's, so the thing being
judged can fail while the judgment reads success. The ADR states it and nothing enforced it, which is
how this shape came back during the 2026-08-05 session:

    docker run … cargo check … | tail -40      # reported exit 0
                                               # the run never happened — the path was rejected

Two shapes are unambiguously fail-open, and this guard refuses both:

* reading `$?` after a pipeline, because that reads the filter's status, never the command's;
* `pipeline || rc=1` (and `|| exit`, `|| return`), the ADR's own forbidden example, where the `||`
  cannot fire for a failure the last stage swallowed.

Both are at zero across every tracked shell script and workflow, so this is a rule rather than a
ratchet: it costs no cleanup and prevents the regression.

Deliberately NOT enforced: `if cmd | grep -q …`. Thirty of those exist, and they are fail-*closed* —
a broken command produces no output, the filter finds nothing, and the assertion goes red. The
message misleads but the result does not. Widening this guard to cover them would trade a real
defect class for thirty rewrites and a worse signal-to-noise ratio, and a guard that cries wolf gets
switched off. That distinction is the guard's whole scope, so it is stated here rather than left for
someone to rediscover.

An audited exception carries `# verification-judgment: reviewed-pipeline` on the same line.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path, PurePosixPath

REVIEWED = "verification-judgment: reviewed-pipeline"
WORKFLOWS = PurePosixPath(".github/workflows")
# The right-hand side of a `||` that is *consuming* the status rather than discarding it. `|| true`
# and `|| :` are the discard idiom and stay allowed; an assignment or a control transfer is a
# judgment.
CONSUMING = re.compile(r"\|\|\s*(?:[A-Za-z_][A-Za-z0-9_]*=|exit\b|return\b)")
READS_STATUS = re.compile(r"\$\?")
CASE_IN = re.compile(r"\bcase\b.*\bin\s*$")
ESAC = re.compile(r"^\s*esac\b")
HEREDOC = re.compile(r"<<-?\s*[\"']?(?P<tag>[A-Za-z_][A-Za-z0-9_]*)[\"']?\s*$")


def strip_noise(line: str, quote: str | None = None) -> tuple[str, str | None]:
    """Blank out quoted spans, `[[ … ]]` tests and comments, and report the quote left open.

    Shell strings cross line boundaries, and a line-oriented scanner that resets its quote state
    every line reads the *interior* of a multi-line string as code. This guard's own self-test
    proved it: a `case` arm written inside a fixture string was flagged as a live pipeline. Heredoc
    bodies and multi-line SQL would fail the same way, so the caller threads the state through.
    """
    out: list[str] = []
    index = 0
    while index < len(line):
        character = line[index]
        if quote:
            out.append(character if character == quote else " ")
            if character == quote and (quote == "'" or line[index - 1] != "\\"):
                quote = None
            index += 1
            continue
        if character in "'\"":
            quote = character
            out.append(character)
            index += 1
            continue
        if character == "#" and (not out or out[-1].isspace()):
            break
        out.append(character)
        index += 1
    text = "".join(out)
    # `[[ … ]]` holds regex alternation, which is not a pipeline.
    text = re.sub(r"\[\[.*?\]\]", lambda match: " " * len(match.group()), text)
    text = re.sub(r"\[\[.*$", lambda match: " " * len(match.group()), text)
    return text, quote


def strip_for_status(line: str) -> str:
    """Blank comments and single-quoted spans only.

    `$?` keeps expanding inside double quotes — `echo "EXIT=$?"` is the shape the ADR names — so the
    pipeline detector's stripping is wrong for this question. Reusing it here made the guard's own
    self-test accept the exact defect the guard was written for.
    """
    out: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(line):
        character = line[index]
        if quote == "'":
            out.append(character if character == "'" else " ")
            if character == "'":
                quote = None
            index += 1
            continue
        if quote == '"':
            out.append(character)
            if character == '"' and line[index - 1] != "\\":
                quote = None
            index += 1
            continue
        if character in "'\"":
            quote = character
            out.append(character)
            index += 1
            continue
        if character == "#" and (not out or out[-1].isspace()):
            break
        out.append(character)
        index += 1
    return "".join(out)


def has_pipeline(text: str) -> bool:
    index = 0
    while index < len(text):
        if text[index] == "|":
            if index + 1 < len(text) and text[index + 1] in "|&":
                index += 2
                continue
            if index > 0 and text[index - 1] == "|":
                index += 1
                continue
            return True
        index += 1
    return False


def is_case_arm(text: str, depth: int) -> bool:
    return depth > 0 and ";;" in text and ")" in text


def scan(relative: PurePosixPath, body: str) -> list[str]:
    findings: list[str] = []
    lines = body.splitlines()
    depth = 0
    previous_pipeline: int | None = None
    quote: str | None = None
    heredoc: str | None = None
    for number, raw in enumerate(lines, start=1):
        # A heredoc body is data handed to another program — shell, SQL, YAML — and reading it as
        # shell is how a `||` in SQL string concatenation would become a false positive.
        if heredoc is not None:
            if raw.strip() == heredoc:
                heredoc = None
            previous_pipeline = None
            continue
        if quote is None:
            opener = HEREDOC.search(strip_noise(raw)[0])
            if opener:
                heredoc = opener.group("tag")

        if REVIEWED in raw:
            previous_pipeline = None
            continue
        inside_string = quote is not None
        text, quote = strip_noise(raw, quote)
        if inside_string:
            # Interior of a multi-line string: data, not a judgment.
            previous_pipeline = None
            continue
        if CASE_IN.search(text):
            depth += 1
        elif ESAC.match(text) and depth:
            depth -= 1

        if previous_pipeline is not None and READS_STATUS.search(strip_for_status(raw)):
            findings.append(
                f"{relative}:{number}: reads $? after the pipeline on line {previous_pipeline}; "
                "that is the filter's status, not the command's — capture output to a file and "
                "judge the command directly (ADR-0012 rule 2)"
            )

        if not text.strip():
            continue
        pipeline = has_pipeline(text) and not is_case_arm(text, depth)
        if pipeline and CONSUMING.search(text):
            findings.append(
                f"{relative}:{number}: a pipeline decides this `||`; the last stage's success hides "
                "a failure in the command being judged (ADR-0012 rule 2)"
            )
        previous_pipeline = number if pipeline else None
    return findings


def tracked(root: Path) -> list[PurePosixPath]:
    listed = subprocess.run(
        ["git", "ls-files", "*.sh", ".github/workflows/*.yml", ".github/workflows/*.yaml"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    return [PurePosixPath(line) for line in listed.stdout.split() if line]


def main(argv: list[str]) -> int:
    root = Path(argv[1]).resolve() if len(argv) > 1 else Path(__file__).resolve().parents[2]
    paths = tracked(root)
    if not paths:
        print(
            "FAIL judgment-position-exit-codes: no shell scripts or workflows were listed; "
            "a scan that reads nothing cannot report a pass",
            file=sys.stderr,
        )
        return 1

    findings: list[str] = []
    for relative in paths:
        try:
            body = (root / relative).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        findings.extend(scan(relative, body))

    if findings:
        for finding in findings:
            print(f"FAIL judgment-position-exit-codes: {finding}", file=sys.stderr)
        return 1

    print(f"OK judgment-position-exit-codes (scanned={len(paths)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
