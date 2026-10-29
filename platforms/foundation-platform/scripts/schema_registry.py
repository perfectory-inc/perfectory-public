#!/usr/bin/env python3
"""The schema registry's checker and its can-i-deploy (root ADR-0336).

`contracts/schema-registry/registry.json` lists each published subject, its revisions (each a JSON
Schema 2020-12 file under `<subject>/<major>/<revision>.schema.json`) and its consumers' pacts.
`check` holds the registry to its rules; `can_i_deploy` answers whether a deployed consumer reads a
subject's major.

Compatibility. A subject's mode is FULL_TRANSITIVE: a revision is readable by a reader of every
earlier revision of its major, and reads every document they wrote. "Readable" is the reader of a
JSON document that ignores members it does not declare, as every consumer here does (zod strips,
serde and the Worker ignore): a writer may add an optional field, never a required one, never change
a type, narrow an enum a reader holds closed (`x-open-enum`), or drop a field a reader requires.
Karapace's checker (the CI's ghcr.io/aiven-open/karapace:6.2.0, run offline) was tried first and
does not fit: it models a schema without `additionalProperties` as accepting any value under any
undeclared name, so adding an optional property is incompatible in one direction under an open
content model and in the other under a closed one, and FULL refuses the one change the ADR exists
to allow (revision 1 to 2 of the public auction subject, measured 2026-10-29); it also reports a
widened closed enum as forward compatible, which a z.enum reader refuses. So the rule is this small
checker over the keyword subset the registry admits (`ALLOWED_KEYWORDS`); any other keyword is
refused rather than ignored.

Run `python3 scripts/schema_registry.py check [--base <ref>]` from the platform directory or name it
with `--platform`; with a base, merged revisions and their files must be unchanged
(append-only) and no new revision may be legacy.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import subprocess
import sys
from typing import Any

PLATFORM = pathlib.Path(__file__).resolve().parents[1]
REGISTRY = pathlib.Path("contracts/schema-registry")
COMPATIBILITY_MODES = ("FULL_TRANSITIVE",)
ALLOWED_KEYWORDS = frozenset({
    "$schema", "$comment", "title", "description",
    "type", "properties", "required", "items", "enum", "pattern", "x-open-enum",
})
TYPES = frozenset({"object", "array", "string", "integer", "number", "boolean", "null"})
PATH = re.compile(r"^[a-z_][a-z0-9_]*(\[\])?(\.[a-z_][a-z0-9_]*(\[\])?)*$")
# Files that may spell a registered subject's wire string: the registry, the records it was
# translated from, generated code, tests, documents. Anywhere else it is a hand-written version list.
WIRE_STRING_ALLOWED = (
    ":(exclude)platforms/foundation-platform/contracts/schema-registry/**",
    ":(exclude)platforms/foundation-platform/contracts/by-pnu-documents/**",
    ":(exclude)*.generated.ts",
    ":(exclude)*test*",
    ":(exclude)*.md",
)


def load(platform: pathlib.Path = PLATFORM) -> dict[str, Any]:
    return json.loads((platform / REGISTRY / "registry.json").read_text(encoding="utf-8"))


def short(subject: str) -> str:
    return subject.removeprefix("foundation-platform.")


def schema_file(subject: str, revision: dict[str, Any]) -> pathlib.Path:
    return REGISTRY / short(subject) / str(revision["major"]) / f"{revision['revision']}.schema.json"


def schema_of(platform: pathlib.Path, subject: str, revision: dict[str, Any]) -> dict[str, Any]:
    return json.loads((platform / schema_file(subject, revision)).read_text(encoding="utf-8"))


def canonical(subject: str, major: int) -> str:
    """The string a document of `major` names in `schema_version` (`<subject>.v<major>`)."""
    return f"{subject}.v{major}"


def identities(entry: dict[str, Any], major: int) -> list[str]:
    """Every `schema_version` a document of `major` may carry: the canonical one, then the legacy
    strings its revisions were served under before the registry."""
    found = [canonical(entry["subject"], major)]
    for revision in entry["revisions"]:
        if revision["major"] == major and revision["schema_version"] not in found:
            found.append(revision["schema_version"])
    return found


def current(entry: dict[str, Any]) -> dict[str, Any]:
    """The revision the producer writes: the newest of the subject's major."""
    return [r for r in entry["revisions"] if r["major"] == entry["major"]][-1]


def subject_of_lane(registry: dict[str, Any], lane: str) -> dict[str, Any] | None:
    return next((entry for entry in registry["subjects"] if entry["owner"]["lane"] == lane), None)


# --- the schema subset -------------------------------------------------------------------------


def types(node: dict[str, Any]) -> set[str] | None:
    value = node.get("type")
    if value is None:
        return None
    return {value} if isinstance(value, str) else set(value)


def keyword_violations(node: Any, at: str) -> list[str]:
    if not isinstance(node, dict):
        return [f"{at or '<root>'}: a schema is an object"]
    found = [f"{at or '<root>'}: keyword {key!r} is outside the registry's subset"
             for key in sorted(set(node) - ALLOWED_KEYWORDS)]
    if (kinds := types(node)) is not None and not kinds <= TYPES:
        found.append(f"{at or '<root>'}: unknown type {sorted(kinds - TYPES)}")
    for name, inner in (node.get("properties") or {}).items():
        found += keyword_violations(inner, f"{at}.{name}" if at else name)
    missing = set(node.get("required", [])) - set(node.get("properties") or {})
    if missing:
        found.append(f"{at or '<root>'}: requires undeclared {sorted(missing)}")
    if "items" in node:
        found += keyword_violations(node["items"], f"{at}[]")
    return found


def record_of(schema: dict[str, Any]) -> list[dict[str, Any]]:
    """The schema as a field record (`contracts/by-pnu-documents` grammar): every property path, an
    array's items under `<field>[]`, and whether it is required."""
    out: list[dict[str, Any]] = []

    def walk(node: dict[str, Any], at: str) -> None:
        if "items" in node:
            walk(node["items"], f"{at}[]")
        required = set(node.get("required", []))
        for name, inner in (node.get("properties") or {}).items():
            path = f"{at}.{name}" if at else name
            out.append({"path": path, "required": name in required})
            walk(inner, path)

    walk(schema, "")
    return sorted(out, key=lambda entry: entry["path"])


def node_at(schema: dict[str, Any], path: str) -> tuple[dict[str, Any], bool] | None:
    """The schema of `path` (record grammar) and whether it is required where it is declared."""
    node, required = schema, True
    for part in path.split("."):
        items = part.endswith("[]")
        name = part.removesuffix("[]")
        properties = node.get("properties") or {}
        if name not in properties:
            return None
        required = name in node.get("required", [])
        node = properties[name]
        if items:
            if "items" not in node:
                return None
            node = node["items"]
    return node, required


def readable(reader: dict[str, Any], writer: dict[str, Any], at: str = "") -> list[str]:
    """Why a reader of `reader` cannot read every document `writer` admits; empty when it can."""
    where = at or "<root>"
    found = []
    want, have = types(reader), types(writer)
    if want is not None:
        if have is None:
            found.append(f"{where}: the writer admits any type, the reader {sorted(want)}")
        else:
            widened = {t for t in have if t not in want and not (t == "integer" and "number" in want)}
            if widened:
                found.append(f"{where}: the writer admits {sorted(widened)}, the reader only {sorted(want)}")
    if "enum" in reader and not reader.get("x-open-enum"):
        extra = set(writer["enum"]) - set(reader["enum"]) if "enum" in writer else {"<any>"}
        if extra:
            found.append(f"{where}: the writer admits {sorted(map(str, extra))} outside the reader's closed enum")
    if "pattern" in reader and writer.get("pattern") != reader["pattern"]:
        found.append(f"{where}: the writer's pattern {writer.get('pattern')!r} is not the reader's {reader['pattern']!r}")
    written = writer.get("properties") or {}
    writer_required = set(writer.get("required", []))
    for name, inner in (reader.get("properties") or {}).items():
        path = f"{at}.{name}" if at else name
        if name in reader.get("required", []) and name not in writer_required:
            found.append(f"{path}: the reader requires it and the writer may omit it")
        if name in written:
            found += readable(inner, written[name], path)
    if "items" in reader and "items" in writer:
        found += readable(reader["items"], writer["items"], f"{at}[]")
    elif "items" in reader:
        found.append(f"{where}[]: the writer's items admit any value")
    return found


def full(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    """FULL: the new revision reads the old documents and the old reader reads the new ones."""
    return ([f"new reads old: {why}" for why in readable(new, old)]
            + [f"old reads new: {why}" for why in readable(old, new)])


# --- the registry's rules ----------------------------------------------------------------------


def check_subject(platform: pathlib.Path, entry: dict[str, Any], lanes: dict[str, Any]) -> list[str]:
    subject = entry.get("subject", "")
    out = []

    def fail(message: str) -> None:
        out.append(f"{subject}: {message}")

    if not re.fullmatch(r"foundation-platform\.[a-z][a-z0-9_]*", subject):
        return [f"{subject!r}: a subject is foundation-platform.<snake_case name>"]
    if entry.get("format") != "JSON":
        fail("format must be JSON (root ADR-0336 §2; Avro events join later)")
    if entry.get("compatibility") not in COMPATIBILITY_MODES:
        fail(f"compatibility must be one of {COMPATIBILITY_MODES}")
    revisions = entry.get("revisions") or []
    if not revisions:
        return out + [f"{subject}: names no revision"]
    keys = [(r.get("major"), r.get("revision")) for r in revisions]
    if not all(isinstance(m, int) and isinstance(n, int) and m >= 1 and n >= 1 for m, n in keys):
        return out + [f"{subject}: each revision names a major and a revision number, both from 1"]
    if keys != sorted(keys) or len({n for _, n in keys}) != len(keys) or [n for _, n in keys] != sorted(n for _, n in keys):
        fail("revisions are listed in order, each revision number once and increasing")
    majors = sorted({m for m, _ in keys})
    if entry.get("major") != majors[-1]:
        fail(f"major {entry.get('major')} is not its newest major {majors[-1]}")
    legacy = {r["schema_version"]: r["major"] for r in revisions if r.get("legacy")}
    seen_canonical = False
    for revision in revisions:
        number, major = revision["revision"], revision["major"]
        label = f"revision {number}"
        if revision.get("legacy"):
            if seen_canonical:
                fail(f"{label}: a legacy revision follows a registered one; only the strings served before the registry are legacy")
        else:
            seen_canonical = True
            if revision["schema_version"] != canonical(subject, major):
                fail(f"{label}: schema_version must be {canonical(subject, major)}; the revision goes in schema_revision")
        if not (revision.get("decision") or "").startswith("docs/adr/") or not (revision.get("reason") or "").strip():
            fail(f"{label}: names no decision (docs/adr/...) and reason")
        path = platform / schema_file(subject, revision)
        if not path.is_file():
            fail(f"{label}: no schema file {schema_file(subject, revision)}")
            continue
        schema = json.loads(path.read_text(encoding="utf-8"))
        out += [f"{subject} {label}: {why}" for why in keyword_violations(schema, "")]
        if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema" or types(schema) != {"object"}:
            fail(f"{label}: a document schema is a JSON Schema 2020-12 object")
        version = (schema.get("properties") or {}).get("schema_version", {})
        if "schema_version" not in schema.get("required", []) or sorted(version.get("enum", [])) != sorted(identities(entry, major)):
            fail(f"{label}: schema_version is required and admits exactly the major's strings {identities(entry, major)}")
        if not revision.get("legacy"):
            declared = (schema.get("properties") or {}).get("schema_revision")
            if declared is None or types(declared) != {"integer"} or "schema_revision" in schema.get("required", []):
                fail(f"{label}: declares schema_revision, an optional integer (old documents have none)")
        if revision.get("record"):
            record = json.loads((platform / revision["record"]).read_text(encoding="utf-8"))
            if record_of(schema) != sorted(record, key=lambda e: e["path"]):
                fail(f"{label}: translates to other fields than its record {revision['record']}")
        for earlier in revisions:
            if earlier["major"] == major and earlier["revision"] < number:
                old = schema_of(platform, subject, earlier)
                for why in full(old, schema):
                    fail(f"{label} is not FULL compatible with revision {earlier['revision']}: {why}")
    for major in majors:
        for string, other in legacy.items():
            if other != major and string == canonical(subject, major):
                fail(f"major {major}'s string {string} is a legacy string of major {other}; take a major above every legacy number")
    out += check_consumers(platform, entry)
    lane = (entry.get("owner") or {}).get("lane")
    line = lanes.get(lane)
    if line is None:
        fail(f"owner.lane {lane!r} is no by_pnu_lanes line of config/r2-connections.contract.json")
    elif "host" not in line:
        fail(f"by_pnu_lanes.{lane} is written whole; the registry takes over kind lines (a host and a kind) only so far")
    else:
        stated = sorted(f for f in ("document_schema_version", "document_schema_version_reason", "document_schema_expands") if f in line)
        if stated:
            fail(f"by_pnu_lanes.{lane} states {stated}; a registered subject's versions live in the registry only")
    return out


def check_consumers(platform: pathlib.Path, entry: dict[str, Any]) -> list[str]:
    """The pacts (Pact's consumer-driven contracts): every revision of the subject's major
    guarantees each path a consumer requires, and the current revision declares every path a
    consumer reads at all."""
    subject = entry["subject"]
    out = []
    live = [r for r in entry["revisions"] if r["major"] == entry["major"]]
    schemas = {r["revision"]: schema_of(platform, subject, r) for r in live if (platform / schema_file(subject, r)).is_file()}
    newest = schemas.get(current(entry)["revision"])
    names = set()
    for consumer in entry.get("consumers") or []:
        name = consumer.get("name", "")
        if not name or name in names or not (consumer.get("reason") or "").strip() or not consumer.get("at"):
            out.append(f"{subject}: consumer {name!r} needs a unique name, where it reads (at) and a reason")
        names.add(name)
        optional = {read.get("path") for read in consumer.get("reads", []) if read.get("required") is False}
        for read in consumer.get("reads", []):
            path, required = read.get("path", ""), read.get("required")
            if not PATH.match(path) or not isinstance(required, bool):
                out.append(f"{subject}: consumer {name} reads {read!r}, not a path and whether it is required")
                continue
            if newest is not None and node_at(newest, path) is None:
                out.append(f"{subject}: consumer {name} reads {path}, which revision {current(entry)['revision']} does not declare")
            if required:
                for number, schema in schemas.items():
                    if not guaranteed(schema, path, optional):
                        out.append(f"{subject}: consumer {name} requires {path}, which revision {number} does not guarantee")
    return out


def guaranteed(schema: dict[str, Any], path: str, optional: set[str]) -> bool:
    """Whether a document of `schema` holds `path` whenever the consumer looks for it: required where
    it is declared, or under a field this revision lacks that the consumer reads as optional (a
    round's photos, which revision 1 has none of: the photo's hash is required of every photo)."""
    parts = path.split(".")
    for end in range(1, len(parts) + 1):
        prefix = ".".join(parts[:end])
        if node_at(schema, prefix) is None:
            return end < len(parts) and prefix.removesuffix("[]") in optional
    found = node_at(schema, path)
    return found is not None and found[1]


def wire_strings_by_hand(root: pathlib.Path, registry: dict[str, Any]) -> list[str]:
    """Source files outside the registry that spell a registered subject's versioned string: a
    hand-written allow-list a consumer keeps beside the registry (2026-10-29: the web's z.enum and
    the monitor's manifest record each refused the public auction's v2)."""
    out = []
    for entry in registry["subjects"]:
        needle = f"{entry['subject']}.v"
        result = subprocess.run(["git", "-C", str(root), "grep", "-l", "-F", needle, "--", ".", *WIRE_STRING_ALLOWED],
                                capture_output=True, text=True, check=False)
        if result.returncode not in (0, 1):
            out.append(f"cannot search the tree for {needle}: {result.stderr.strip()}")
        out += [f"{path}: spells {needle}<n> by hand; read the subject's major from the registry (root ADR-0336)"
                for path in result.stdout.split()]
    return out


def unchanged_since(platform: pathlib.Path, base: str) -> list[str]:
    """Append-only against `base`: its revisions and their files are unchanged, none is removed, and
    a revision added to a subject it already had is not legacy."""
    root = pathlib.Path(subprocess.run(["git", "-C", str(platform), "rev-parse", "--show-toplevel"],
                                       capture_output=True, text=True, check=True).stdout.strip())
    prefix = platform.resolve().relative_to(root.resolve()).as_posix()
    merge_base = subprocess.run(["git", "-C", str(root), "merge-base", base, "HEAD"], capture_output=True, text=True, check=False)
    if merge_base.returncode != 0:
        return [f"cannot find the merge base of {base} and HEAD"]
    at = merge_base.stdout.strip()

    def show(path: pathlib.Path) -> str | None:
        shown = subprocess.run(["git", "-C", str(root), "show", f"{at}:{prefix}/{path.as_posix()}"],
                               capture_output=True, text=True, check=False, encoding="utf-8")
        return shown.stdout if shown.returncode == 0 else None

    before_text = show(REGISTRY / "registry.json")
    if before_text is None:
        return []
    before = {entry["subject"]: entry for entry in json.loads(before_text)["subjects"]}
    now = {entry["subject"]: entry for entry in load(platform)["subjects"]}
    out = []
    for subject, entry in before.items():
        if subject not in now:
            out.append(f"{subject}: removed from the registry; a subject is retired, never deleted")
            continue
        kept = now[subject]["revisions"][: len(entry["revisions"])]
        if kept != entry["revisions"]:
            out.append(f"{subject}: a merged revision changed; add a new revision instead")
        for revision in now[subject]["revisions"][len(entry["revisions"]):]:
            if revision.get("legacy"):
                out.append(f"{subject}: revision {revision['revision']} is added as legacy; only strings served before the registry are")
        for revision in entry["revisions"]:
            path = schema_file(subject, revision)
            if show(path) != ((platform / path).read_text(encoding="utf-8") if (platform / path).is_file() else None):
                out.append(f"{subject}: {path} changed; a merged revision's file never changes")
    return out


def check(platform: pathlib.Path = PLATFORM, base: str | None = None) -> list[str]:
    registry = load(platform)
    out = []
    if registry.get("schema_version") != 1 or not isinstance(registry.get("subjects"), list):
        return ["registry.json: schema_version 1 with a list of subjects"]
    lanes = json.loads((platform / "config/r2-connections.contract.json").read_text(encoding="utf-8"))["by_pnu_lanes"]
    subjects = [entry.get("subject") for entry in registry["subjects"]]
    if len(set(subjects)) != len(subjects):
        out.append("registry.json: a subject is listed twice")
    owners = [(entry.get("owner") or {}).get("lane") for entry in registry["subjects"]]
    if len(set(owners)) != len(owners):
        out.append("registry.json: two subjects name the same owner lane")
    for entry in registry["subjects"]:
        out += check_subject(platform, entry, lanes)
    listed = {schema_file(e["subject"], r).as_posix() for e in registry["subjects"] for r in e.get("revisions") or []
              if isinstance(r.get("major"), int) and isinstance(r.get("revision"), int)}
    for path in sorted((platform / REGISTRY).rglob("*.schema.json")):
        if path.relative_to(platform).as_posix() not in listed:
            out.append(f"{path.relative_to(platform).as_posix()}: a schema file no revision names")
    out += wire_strings_by_hand(platform.parents[1], registry)
    if base:
        out += unchanged_since(platform, base)
    return out


def can_i_deploy(registry: dict[str, Any], subject: str, major: int, deployed: dict[str, Any]) -> list[str]:
    """Pact's can-i-deploy over the consumers that say what they read (`deployed`: consumer name to
    its capabilities answer): why a document of `subject` major `major` cannot be served to one of
    them; empty when every one reads it. A consumer whose answer names no majors of the subject
    reads none of them."""
    if all(entry["subject"] != subject for entry in registry["subjects"]):
        return [f"{subject} is not a registered subject"]
    out = []
    for name, answer in deployed.items():
        majors = ((answer or {}).get("document_schemas") or {}).get(subject) or []
        if major not in majors:
            out.append(f"{name} reads {subject} majors {majors}, not {major}")
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=("check",))
    parser.add_argument("--platform", type=pathlib.Path, default=PLATFORM)
    parser.add_argument("--base", help="the pull request base: merged revisions must be unchanged")
    args = parser.parse_args(argv)
    failures = check(args.platform, args.base)
    for failure in failures:
        print(f"FAIL schema-registry: {failure}", file=sys.stderr)
    if failures:
        return 1
    print("OK schema-registry")
    return 0


if __name__ == "__main__":
    sys.exit(main())
