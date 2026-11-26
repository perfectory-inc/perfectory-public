"""The AWS stack (infra/aws/*.tf) and the AWS Dagster instance as their parsers read them, for the tests
that hold their rules.

Parsed by the pinned conftest (tools/conftest.env, fetched as scripts/ci/conftest.sh does, root ADR-0197),
whose hcl2 parser is hcl2json:
a block is the parser's block and an attribute its value. A value that is an expression is the parser's
source text of that expression; `references` reads from it the objects it names, by HCL's traversal
grammar (`aws_iam_role.dagster.arn`, `local.dagster_password`, `var.dagster_image`). No test reads the
files' layout: alignment, line breaks and block order do not change what they see.
"""

from __future__ import annotations

import functools
import json
import pathlib
import re
import subprocess
import sys
import tempfile

PLATFORM = pathlib.Path(__file__).resolve().parents[2]
ROOT = PLATFORM.parents[1]
STACK = PLATFORM / "infra" / "aws"
CONTRACT = PLATFORM / "config" / "aws-platform.contract.json"
INSTANCE = PLATFORM / "orchestration" / "dagster" / "aws" / "dagster.yaml"
# A traversal: a root name, then `.name` or `[index]` steps (hclsyntax.ScopeTraversalExpr).
TRAVERSAL = re.compile(r"(?<![\w.])((?:data\.)?[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*|\[[0-9]+\])+)")


@functools.cache
def _conftest() -> str:
    """The pinned conftest for this machine, from the one pinned fetcher (what scripts/ci/conftest.sh runs)."""
    member = "conftest.exe" if sys.platform == "win32" else "conftest"
    output = pathlib.Path(tempfile.mkdtemp(prefix="aws-stack-")) / member
    subprocess.run([sys.executable, "-I", "-B", str(ROOT / "scripts" / "ci" / "fetch_pinned.py"), "tool", "conftest",
                    "--member", member, "--output", str(output)], cwd=ROOT, check=True, capture_output=True)
    return str(output)


def _parse(parser: str, paths: list[pathlib.Path]) -> list[dict]:
    result = subprocess.run(
        [_conftest(), "parse", "--parser", parser, "--combine", *[path.relative_to(ROOT).as_posix() for path in paths]],
        cwd=ROOT, check=True, capture_output=True, text=True, encoding="utf-8")
    return json.loads(result.stdout)


@functools.cache
def stack() -> dict:
    """Every block of the stack's files by kind: resource, data and ephemeral as {type: {name: body}},
    variable and output as {name: body}, locals as {name: value}."""
    merged: dict = {"resource": {}, "data": {}, "ephemeral": {}, "variable": {}, "output": {}, "locals": {}}
    for entry in _parse("hcl2", sorted(STACK.glob("*.tf"))):
        contents = entry["contents"]
        for kind in ("resource", "data", "ephemeral"):
            for type_, named in contents.get(kind, {}).items():
                for name, bodies in named.items():
                    [body] = bodies
                    merged[kind].setdefault(type_, {})[name] = body
        for kind in ("variable", "output"):
            for name, bodies in contents.get(kind, {}).items():
                [body] = bodies
                merged[kind][name] = body
        for block in contents.get("locals", []):
            merged["locals"].update(block)
    return merged


@functools.cache
def instance() -> dict:
    """orchestration/dagster/aws/dagster.yaml as YAML."""
    [entry] = _parse("yaml", [INSTANCE])
    return entry["contents"]


def contract() -> dict:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def blocks() -> list[tuple[str, dict]]:
    """(address, body) of every resource, data and ephemeral block and every local, e.g.
    ("aws_iam_role.dagster", {...}), ("data.aws_iam_policy_document.dagster", {...}), ("local.dagster_secrets", [...])."""
    found = []
    for kind, prefix in (("resource", ""), ("data", "data."), ("ephemeral", "ephemeral.")):
        for type_, named in stack()[kind].items():
            found.extend((f"{prefix}{type_}.{name}", body) for name, body in named.items())
    found.extend((f"local.{name}", value) for name, value in stack()["locals"].items())
    return found


def body(address: str):
    return dict(blocks())[address]


def expressions(value) -> list[str]:
    """The source text of every expression in a value, in the order the parser gives them."""
    if isinstance(value, dict):
        return [text for item in value.values() for text in expressions(item)]
    if isinstance(value, list):
        return [text for item in value for text in expressions(item)]
    if isinstance(value, str) and "${" in value:
        return [value]
    return []


def references(value) -> list[str]:
    """Every traversal the expressions of a value name, in order: `aws_iam_role.dagster.arn`."""
    return [match for text in expressions(value) for match in TRAVERSAL.findall(_without_strings(text))]


def _without_strings(text: str) -> str:
    """The expression text with its quoted literals blanked, so a word inside a string is no traversal;
    an interpolation inside a literal (`"${x.y}"`) stays."""
    out, index, depth = [], 0, 0
    inside = False
    while index < len(text):
        character = text[index]
        if inside and text.startswith("${", index):
            depth += 1
            out.append(" ")
            index += 2
            inside = False
            continue
        if not inside and depth and character == "}":
            depth -= 1
            inside = True
            out.append(" ")
            index += 1
            continue
        if character == '"' and (index == 0 or text[index - 1] != "\\"):
            inside = not inside
            out.append(" ")
        else:
            out.append(" " if inside else character)
        index += 1
    return "".join(out)


def statements(policy_document: str) -> dict[str, dict]:
    """The statements of a policy document data block, by sid."""
    return {statement.get("sid", ""): statement for statement in body(policy_document)["statement"]}


def naming(prefix: str) -> list[str]:
    """The addresses of every block whose expressions name a traversal that starts with `prefix`."""
    return [address for address, value in blocks()
            if any(reference == prefix or reference.startswith(prefix + ".") or reference.startswith(prefix + "[")
                   for reference in references(value))]
