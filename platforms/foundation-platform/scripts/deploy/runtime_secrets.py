#!/usr/bin/env python3
"""Runtime secrets: which host environment files exist, what names each holds, and which of them
every systemd unit and operator run loads (root ADR-0153). Names only; no value is ever read
into this program's output.

The one source is `config/runtime-secrets.contract.json`. Everything else is derived from it or
checked against it:

    check        repository: each unit's `EnvironmentFile=` lines are exactly what the contract
                 renders; every variable a unit's or run's script requires (`${NAME:?}`, a
                 `required_env=(...)` list, Python `os.environ["NAME"]`, in the script and what it
                 sources) is supplied by a group the contract says it loads and which holds it;
                 no runbook or script hand-writes an `EnvironmentFile=/etc/foundation-platform/...`.
    render       rewrites the units' `EnvironmentFile=` lines from the contract.
    set          on the data host, as root (foundation-release.sh env-set): writes the NAME=value
                 lines of a file into a group's file, refusing a name the group does not hold, with
                 the contract's owner, group and mode (root ADR-0247). Prints names only.
    credentials  repository: every credential-shaped FOUNDATION_PLATFORM_ name in a tracked file is
                 derived from a recorded Cloudflare token or named in not_cloudflare (root ADR-0247).
    rename       on the data host, as root, before host: a group file holding a token's old name
                 (`renamed_from`) and not its derived one gains the derived name with the same value
                 (root ADR-0247). Prints the files and names it changed, never a value.
    host         on the data host, as root: every group file a unit loads exists, is owned and
                 no more open than the contract says, and holds every name it declares. Prints
                 names, never values.
    properties   `-p EnvironmentFile=...` arguments for an operator run (`systemd-run`).
    names        the names a consumer needs, one per line.

Why one contract: on 2026-10-05 the 필지고유번호변동연혁 unit's script required the lakehouse reader
key and none of the unit's files held it; the same day a measurement script dropped a key the
read it made needed. Each unit's files, each script's needs and each runbook's `systemd-run`
were three hand-kept lists.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

AREA = Path(__file__).resolve().parents[2]
CONTRACT = Path("config/runtime-secrets.contract.json")
UNIT_DIR = Path("infra/systemd")
NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# What a script says it cannot run without.
REQUIRED_EXPANSION = re.compile(r"\$\$?\{([A-Za-z_][A-Za-z0-9_]*):\?")
REQUIRED_LIST = re.compile(r"^\s*required_env=\(([^)]*)\)", re.MULTILINE)
PYTHON_REQUIRED = re.compile(r"""os\.environ\[\s*["']([A-Za-z_][A-Za-z0-9_]*)["']\s*\]""")
SOURCED = re.compile(r"""^\s*(?:source|\.)\s+"\$\(dirname "\$\{BASH_SOURCE\[0\]\}"\)/([^"]+)\"""", re.MULTILINE)
EXEC_SCRIPT = re.compile(r"scripts/[A-Za-z0-9_./-]+\.(?:sh|py)")
HAND_WRITTEN = re.compile(r"EnvironmentFile=-?/etc/foundation-platform/")
HOST_ENV_FILE = re.compile(r"/etc/foundation-platform/[A-Za-z0-9_.-]+?\.env(?![A-Za-z0-9_.-])")
# A Cloudflare token's name, and the values each kind of token gives (root ADR-0247).
TOKEN_NAME = re.compile(r"^foundation-platform-[a-z0-9]+(?:-[a-z0-9]+)*-(?:reader|writer)-prod$")
TOKEN_VALUES = {"api_token": ("TOKEN",), "r2_access_key": ("ACCESS_KEY_ID", "SECRET_ACCESS_KEY")}
# A name in our namespace shaped like a credential; `credentials` requires it to be derived.
CREDENTIAL = re.compile(r"(?<![A-Za-z0-9_])FOUNDATION_PLATFORM_[A-Z0-9_]*?_(?:TOKEN|ACCESS_KEY_ID|SECRET_ACCESS_KEY)(?![A-Za-z0-9_])")
# Accepted ADRs are immutable (root ADR-0002), so they keep the names of their day.
HISTORY = ("docs/adr/",)


class ContractError(ValueError):
    """The contract, a unit or a script disagrees with the contract."""


@dataclass
class Group:
    name: str
    path: str
    holds: frozenset[str]
    owner: str | None = None
    group: str | None = None
    mode: int | None = None
    optional: bool = False
    release: bool = False  # sourced by the unit's own ExecStart from the release, not an EnvironmentFile


@dataclass
class Consumer:
    key: str  # unit file name or run name
    kind: str  # "unit" | "run"
    needs: dict[str, str]
    unenumerated: dict[str, str]
    optional_groups: list[str]
    scripts: list[str] = field(default_factory=list)


@dataclass
class Contract:
    groups: dict[str, Group]  # in load order
    systemd_supplied: frozenset[str]
    consumers: list[Consumer]
    tokens: dict[str, list[str]] = field(default_factory=dict)  # Cloudflare token name -> its derived names
    renamed: dict[str, str] = field(default_factory=dict)  # old name -> the derived name that replaces it
    not_cloudflare: dict[str, str] = field(default_factory=dict)  # credential name -> why no token derives it

    def consumer(self, key: str) -> Consumer:
        for consumer in self.consumers:
            if consumer.key == key:
                return consumer
        raise ContractError(f"the contract names no unit or run {key!r}")

    def loaded(self, consumer: Consumer) -> list[Group]:
        """The groups a consumer loads, in the contract's order (a later file overrides an earlier one)."""

        wanted = set(consumer.needs.values()) | set(consumer.unenumerated) | set(consumer.optional_groups)
        return [group for name, group in self.groups.items() if name in wanted]


def derived_names(token: str, gives: str) -> list[str]:
    """The variable names a Cloudflare token's values go by (root ADR-0247): the token's name
    without `-prod`, upper case, `-` as `_`, then the kind of value."""

    if not TOKEN_NAME.match(token):
        raise ContractError(f"cloudflare token {token!r} is not named foundation-platform-<store>-<reader|writer>-prod")
    if gives not in TOKEN_VALUES:
        raise ContractError(f"cloudflare token {token!r}: gives must be one of {sorted(TOKEN_VALUES)}")
    stem = token.removesuffix("-prod").upper().replace("-", "_")
    return [f"{stem}_{suffix}" for suffix in TOKEN_VALUES[gives]]


def _resolve_holds(area: Path, entries: Sequence[str], tokens: Mapping[str, list[str]]) -> frozenset[str]:
    """Names a group holds. An entry `@<file>` reads the names of an env example file; an entry
    `@<file.json>#/<pointer>` reads a name or a list of names from a contract the code reads, so
    a name lives where its reader takes it from, not here as well. An entry `@token:<name>` is
    the names a recorded Cloudflare token's values go by."""

    names: set[str] = set()
    for entry in entries:
        if not entry.startswith("@"):
            names.add(entry)
            continue
        if entry.startswith("@token:"):
            if entry[7:] not in tokens:
                raise ContractError(f"holds reference {entry!r}: cloudflare_tokens records no such token")
            names.update(tokens[entry[7:]])
            continue
        reference, _, pointer = entry[1:].partition("#")
        path = area / reference
        if not path.is_file():
            raise ContractError(f"holds reference {entry!r}: {reference} does not exist")
        if not pointer:
            names.update(re.findall(r"^([A-Za-z_][A-Za-z0-9_]*)=", path.read_text(encoding="utf-8"), re.MULTILINE))
            continue
        value: Any = json.loads(path.read_text(encoding="utf-8"))
        for part in pointer.strip("/").split("/"):
            if not isinstance(value, dict) or part not in value:
                raise ContractError(f"holds reference {entry!r}: {pointer} is not in {reference}")
            value = value[part]
        names.update([value] if isinstance(value, str) else value)
    bad = sorted(name for name in names if not isinstance(name, str) or not NAME.match(name))
    if bad:
        raise ContractError(f"holds names that are not variable names: {bad}")
    return frozenset(names)


def load(area: Path = AREA) -> Contract:
    raw = json.loads((area / CONTRACT).read_text(encoding="utf-8"))
    if raw.get("schema_version") != 1:
        raise ContractError("runtime-secrets contract: schema_version must be 1")
    tokens: dict[str, list[str]] = {}
    renamed: dict[str, str] = {}
    for entry in raw.get("cloudflare_tokens", {}).get("tokens", []):
        if entry["name"] in tokens:
            raise ContractError(f"cloudflare token {entry['name']!r} is recorded twice")
        derived = derived_names(entry["name"], entry["gives"])
        tokens[entry["name"]] = derived
        old_names = entry.get("renamed_from", [])
        if old_names and len(old_names) != len(derived):
            raise ContractError(f"cloudflare token {entry['name']!r}: renamed_from names one old name per value {derived}")
        for old, new in zip(old_names, derived):
            if not NAME.match(old) or old == new or old in renamed:
                raise ContractError(f"cloudflare token {entry['name']!r}: {old!r} is not a distinct old name")
            renamed[old] = new
    groups: dict[str, Group] = {}
    paths: set[str] = set()
    for entry in raw["groups"]:
        name = entry["name"]
        if name in groups or entry["path"] in paths:
            raise ContractError(f"group {name!r} or its path is declared twice")
        paths.add(entry["path"])
        release = entry.get("source") == "release"
        if not release and not entry["path"].startswith("/etc/foundation-platform/"):
            raise ContractError(f"group {name!r}: a host group lives under /etc/foundation-platform/")
        mode = entry.get("mode")
        if not release and not entry.get("optional") and not (entry.get("owner") and entry.get("group") and mode):
            raise ContractError(f"group {name!r}: owner, group and mode are required")
        groups[name] = Group(
            name=name, path=entry["path"], holds=_resolve_holds(area, entry.get("holds", []), tokens),
            owner=entry.get("owner"), group=entry.get("group"), mode=int(mode, 8) if mode else None,
            optional=bool(entry.get("optional")), release=release,
        )
    consumers = []
    for entry in raw["consumers"]:
        kind = "unit" if "unit" in entry else "run"
        consumer = Consumer(
            key=entry.get("unit") or entry["run"], kind=kind, needs=dict(entry.get("needs", {})),
            unenumerated=dict(entry.get("unenumerated", {})), optional_groups=list(entry.get("optional_groups", [])),
            scripts=list(entry.get("scripts", [])),
        )
        for group_name in [*consumer.needs.values(), *consumer.unenumerated, *consumer.optional_groups]:
            if group_name not in groups:
                raise ContractError(f"{consumer.key}: no group {group_name!r}")
        for group_name in consumer.optional_groups:
            if not groups[group_name].optional:
                raise ContractError(f"{consumer.key}: {group_name!r} is loaded as optional but the group is not")
        for group_name, reason in consumer.unenumerated.items():
            if not str(reason).strip():
                raise ContractError(f"{consumer.key}: an unenumerated group needs its reason ({group_name})")
        consumers.append(consumer)
    keys = [consumer.key for consumer in consumers]
    if len(keys) != len(set(keys)):
        raise ContractError("a unit or run is declared twice")
    return Contract(groups=groups, systemd_supplied=frozenset(raw.get("systemd_supplied", [])), consumers=consumers,
                    tokens=tokens, renamed=renamed,
                    not_cloudflare={name: reason for name, reason in raw.get("cloudflare_tokens", {}).get("not_cloudflare", {}).items()
                                    if not name.startswith("$")})


# --- what scripts require --------------------------------------------------------------------


def script_requirements(path: Path, seen: set[Path] | None = None) -> set[str]:
    """Every name `path` (and what it sources) cannot run without."""

    seen = set() if seen is None else seen
    path = path.resolve()
    if path in seen:
        return set()
    seen.add(path)
    if not path.is_file():
        raise ContractError(f"{path} does not exist")
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".py":
        return set(PYTHON_REQUIRED.findall(text))
    names = set(REQUIRED_EXPANSION.findall(text))
    for block in REQUIRED_LIST.findall(text):
        names.update(block.split())
    for sourced in SOURCED.findall(text):
        names |= script_requirements(path.parent / sourced, seen)
    return names


@dataclass
class Unit:
    text: str
    exec_lines: list[str]
    environment: set[str]
    environment_files: list[str]


def read_unit(path: Path) -> Unit:
    text = path.read_text(encoding="utf-8")
    exec_lines = re.findall(r"^Exec[A-Za-z]*=(.*)$", text, re.MULTILINE)
    environment = set()
    for line in re.findall(r"^Environment=(.*)$", text, re.MULTILINE):
        environment.update(re.findall(r"(?:^|\s)\"?([A-Za-z_][A-Za-z0-9_]*)=", line))
    return Unit(text=text, exec_lines=exec_lines, environment=environment,
                environment_files=re.findall(r"^EnvironmentFile=(.*)$", text, re.MULTILINE))


def unit_requirements(area: Path, unit: Unit) -> set[str]:
    names: set[str] = set()
    for line in unit.exec_lines:
        names.update(REQUIRED_EXPANSION.findall(line))
        for script in EXEC_SCRIPT.findall(line):
            names |= script_requirements(area / script)
    return names


# --- rendering ---------------------------------------------------------------------------------


def rendered_lines(contract: Contract, consumer: Consumer) -> list[str]:
    return [("-" if group.optional else "") + group.path
            for group in contract.loaded(consumer) if not group.release]


def render_unit_text(text: str, lines: Sequence[str]) -> str:
    """The unit with its `EnvironmentFile=` lines replaced by `lines`, where the first one was (or,
    without one, after `WorkingDirectory=`)."""

    block = "".join(f"EnvironmentFile={line}\n" for line in lines)
    kept = re.sub(r"^EnvironmentFile=.*\n", "\x00", text, flags=re.MULTILINE)
    if "\x00" in kept:
        head, _, tail = kept.partition("\x00")
        return head + block + tail.replace("\x00", "")
    anchor = re.search(r"^WorkingDirectory=.*\n", kept, re.MULTILINE) or re.search(r"^\[Service\]\n", kept, re.MULTILINE)
    if anchor is None:
        if lines:
            raise ContractError("a unit without [Service] cannot load environment files")
        return kept
    return kept[:anchor.end()] + block + kept[anchor.end():]


# --- checks ------------------------------------------------------------------------------------


def _effective_holder(contract: Contract, consumer: Consumer, name: str) -> Group | None:
    """The group whose value the process sees: EnvironmentFiles in order, then a release file the
    ExecStart sources (which comes last)."""

    loaded = contract.loaded(consumer)
    ordered = [g for g in loaded if not g.release] + [g for g in loaded if g.release]
    holders = [g for g in ordered if name in g.holds]
    return holders[-1] if holders else None


def check(area: Path = AREA) -> list[str]:
    contract = load(area)
    findings: list[str] = []
    units = {path.name: path for path in sorted((area / UNIT_DIR).glob("*.service"))}
    declared_units = {c.key for c in contract.consumers if c.kind == "unit"}
    for missing in sorted(set(units) - declared_units):
        findings.append(f"{missing}: the unit is not in {CONTRACT}")
    for stale in sorted(declared_units - set(units)):
        findings.append(f"{stale}: the contract names a unit that does not exist")
    allowed = {name for names in contract.tokens.values() for name in names} | set(contract.not_cloudflare)
    for name, reason in sorted(contract.not_cloudflare.items()):
        if name in allowed - set(contract.not_cloudflare) or not str(reason).strip():
            findings.append(f"not_cloudflare: {name} needs a reason and must not be a derived name")
    for group in contract.groups.values():
        for name in sorted(n for n in group.holds - set(contract.renamed) if re.search(r"_(TOKEN|ACCESS_KEY_ID|SECRET_ACCESS_KEY)$", n)):
            if name not in allowed:
                findings.append(f"group {group.name}: holds {name}, which no recorded Cloudflare token derives and not_cloudflare "
                                f"does not name (root ADR-0247)")
        for old in sorted(group.holds & set(contract.renamed)):
            if contract.renamed[old] not in group.holds:
                findings.append(f"group {group.name}: holds {old} but not {contract.renamed[old]}, the name it is renamed to "
                                f"(root ADR-0247)")
    for consumer in contract.consumers:
        for name, group_name in sorted(consumer.needs.items()):
            group = contract.groups[group_name]
            if name not in group.holds:
                findings.append(f"{consumer.key}: needs {name} from {group_name}, which does not hold it")
                continue
            effective = _effective_holder(contract, consumer, name)
            if effective is not None and effective.name != group_name:
                findings.append(f"{consumer.key}: needs {name} from {group_name}, but {effective.name} is loaded later and wins")
        if consumer.kind == "unit" and consumer.key in units:
            unit = read_unit(units[consumer.key])
            required = unit_requirements(area, unit)
            supplied = set(consumer.needs) | contract.systemd_supplied | unit.environment
            for name in sorted(required - supplied):
                findings.append(f"{consumer.key}: its ExecStart requires {name}, which no group the contract loads for it supplies")
            expected = rendered_lines(contract, consumer)
            if unit.environment_files != expected:
                findings.append(f"{consumer.key}: EnvironmentFile lines {unit.environment_files} are not the contract's {expected} "
                                f"(scripts/deploy/runtime_secrets.py render)")
        else:
            required: set[str] = set()
            for script in consumer.scripts:
                required |= script_requirements(area / script)
            for name in sorted(required - set(consumer.needs)):
                findings.append(f"{consumer.key}: {name} is required by its script but not in its needs")
    declared = {group.path for group in contract.groups.values()}
    named = sorted([*(area / "docs").rglob("*.md"), *(area / "scripts").rglob("*.sh"), *(area / "scripts").rglob("*.py"),
                    *(area / UNIT_DIR).glob("*.service")])
    for path in named:
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            where = f"{path.relative_to(area).as_posix()}:{line_no}"
            if path.suffix in (".md", ".sh") and HAND_WRITTEN.search(line):
                findings.append(f"{where}: hand-written EnvironmentFile; use `scripts/deploy/runtime_secrets.py properties <run>`")
            for host_file in HOST_ENV_FILE.findall(line):
                if host_file not in declared:
                    findings.append(f"{where}: {host_file} is not a group in {CONTRACT}; a host environment file "
                                    f"lives in the contract or not at all (root ADR-0247)")
    return findings


def credentials(repo: Path, area: Path = AREA) -> list[str]:
    """Every credential-shaped name in the repository's tracked files that a recorded Cloudflare token
    does not derive and `cloudflare_tokens.not_cloudflare` does not name (root ADR-0247). Accepted
    ADRs keep the names of their day (they are immutable); during a rename, the contract's own
    `renamed_from` lists the retired names until no host file holds them."""

    contract = load(area)
    allowed = {name for names in contract.tokens.values() for name in names} | set(contract.not_cloudflare)
    own = (area / CONTRACT).resolve()
    listed = subprocess.run(["git", "-C", str(repo), "ls-files", "-z"], capture_output=True, check=False)
    if listed.returncode != 0:
        raise ContractError(f"credentials: git cannot list {repo}: {listed.stderr.decode(errors='replace').strip()}")
    tracked = listed.stdout
    findings = []
    for relative in sorted(filter(None, tracked.decode("utf-8").split("\0"))):
        path = repo / relative
        if relative.startswith(HISTORY) or not path.is_file() or path.resolve() == own:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for line_no, line in enumerate(text.splitlines(), start=1):
            for name in CREDENTIAL.findall(line):
                if name not in allowed:
                    findings.append(f"{relative}:{line_no}: {name} is not derived from a token in cloudflare_tokens "
                                    f"(drop -prod, upper case, - as _, then the value kind) nor named in not_cloudflare")
    return findings


def render(area: Path = AREA) -> list[str]:
    contract = load(area)
    changed = []
    for consumer in contract.consumers:
        if consumer.kind != "unit":
            continue
        path = area / UNIT_DIR / consumer.key
        text = path.read_text(encoding="utf-8")
        new = render_unit_text(text, rendered_lines(contract, consumer))
        if new != text:
            path.write_bytes(new.encode("utf-8"))
            changed.append(consumer.key)
    return changed


# --- the host ----------------------------------------------------------------------------------


def _names_in(path: Path) -> set[str]:
    names = set()
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = re.match(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=", line)
        if match:
            names.add(match.group(1))
    return names


def _account(uid: int, kind: str) -> str:
    try:
        if kind == "user":
            import pwd  # noqa: PLC0415 (Unix only)

            return pwd.getpwuid(uid).pw_name
        import grp  # noqa: PLC0415 (Unix only)

        return grp.getgrgid(uid).gr_name
    except (ImportError, KeyError):
        return str(uid)


def _line_name(line: str) -> str | None:
    match = re.match(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=", line)
    return match.group(1) if match else None


def _values_in(text: str) -> dict[str, str]:
    """Each name's value as an environment file loader keeps it: the last line wins."""
    return {match.group(1): match.group(2)
            for match in re.finditer(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$", text, re.MULTILINE)}


def rename(area: Path = AREA, root: Path = Path("/")) -> list[str]:
    """Gives each group file the new name of every `renamed` variable it holds under the old name
    only (root ADR-0247): `NEW=<the old line's value>` is appended to a copy with the file's owner
    and mode, which then replaces it, so a reader never sees half a file. Then removes an old name's
    lines once the same value is in the file under a name the group holds; an old line whose value
    is nowhere else is kept and reported. Names only."""

    contract = load(area)
    changed = []
    for group in contract.groups.values():
        path = root / group.path.lstrip("/")
        if group.release or not path.is_file() or path.is_symlink():
            continue
        text = path.read_text(encoding="utf-8")
        present = _names_in(path)
        added = []
        for old, new in contract.renamed.items():
            if new not in group.holds or new in present or old not in present:
                continue
            # The last line is the one an environment file loader keeps.
            value = re.findall(rf"^\s*(?:export\s+)?{old}=(.*)$", text, re.MULTILINE)[-1]
            text += ("" if text.endswith("\n") or not text else "\n") + f"{new}={value}\n"
            added.append(f"{old} -> {new}")
        # Phase three: an old line goes once its value is in the file under a name the group holds,
        # so nothing is lost; one whose value is nowhere else is kept and named.
        held = {name: value for name, value in _values_in(text).items() if name in group.holds}
        for old in sorted(set(contract.renamed) & set(_values_in(text)) - group.holds):
            if _values_in(text)[old] in held.values():
                text = "".join(line for line in text.splitlines(keepends=True) if _line_name(line) != old)
                added.append(f"{old} removed")
            else:
                changed.append(f"{group.path}: {old} kept, its value is under no name the group holds")
        if not added:
            continue
        info = path.stat()
        _replace_atomically(path, text, info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode))
        changed.extend(f"{group.path}: {entry}" for entry in added)
    return changed


def _replace_atomically(path: Path, text: str, uid: int, gid: int, mode: int) -> None:
    staged = path.with_name(f".{path.name}.staged")
    staged.unlink(missing_ok=True)  # left by a run that died before its replace
    descriptor = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        if hasattr(os, "chown"):
            os.chown(staged, uid, gid)
        os.chmod(staged, mode)
        os.replace(staged, path)
    finally:
        staged.unlink(missing_ok=True)


def set_names(area: Path, group_name: str, source: Path, root: Path = Path("/")) -> list[str]:
    """Writes each `NAME=value` line of `source` into the group's file (root ADR-0247): that name's
    lines are replaced, every other line is kept, and the file ends up with the contract's owner,
    group and mode. A name the group does not hold is refused. Returns the names; never a value."""

    contract = load(area)
    group = contract.groups.get(group_name)
    if group is None or group.release:
        raise ContractError(f"set: the contract has no host group {group_name!r}")
    if group.owner is None or group.group is None or group.mode is None:
        raise ContractError(f"set: group {group_name!r} declares no owner, group and mode to write it with")
    if source.is_symlink() or not source.is_file():
        raise ContractError(f"set: {source} is not a regular file")
    values: dict[str, str] = {}
    for line in source.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = re.fullmatch(r"([A-Za-z_][A-Za-z0-9_]*)=(.*)", line)
        if not match:
            raise ContractError(f"set: {source} has a line that is not NAME=value")
        if match.group(1) in values:
            raise ContractError(f"set: {source} names {match.group(1)} twice")
        values[match.group(1)] = match.group(2)
    if not values:
        raise ContractError(f"set: {source} names nothing")
    foreign = sorted(set(values) - group.holds)
    if foreign:
        raise ContractError(f"set: group {group_name!r} does not hold {foreign}")
    import grp  # noqa: PLC0415 (Unix only)
    import pwd  # noqa: PLC0415 (Unix only)

    path = root / group.path.lstrip("/")
    if path.is_symlink():
        raise ContractError(f"set: {group.path} is a symlink")
    kept = []
    if path.exists():
        kept = [line for line in path.read_text(encoding="utf-8").splitlines()
                if not ((match := re.match(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=", line)) and match.group(1) in values)]
    text = "".join(f"{line}\n" for line in kept) + "".join(f"{name}={value}\n" for name, value in values.items())
    _replace_atomically(path, text, pwd.getpwnam(group.owner).pw_uid, grp.getgrnam(group.group).gr_gid, group.mode)
    return list(values)


def host(area: Path = AREA, root: Path = Path("/")) -> list[str]:
    """Findings for every group a unit loads. Names only."""

    contract = load(area)
    findings = []
    used = {g.name for c in contract.consumers if c.kind == "unit" for g in contract.loaded(c)}
    for group in contract.groups.values():
        if group.release or group.name not in used:
            continue
        path = root / group.path.lstrip("/")
        if not path.exists():
            if not group.optional:
                findings.append(f"{group.path}: missing")
            continue
        info = path.stat()
        if not stat.S_ISREG(info.st_mode):
            findings.append(f"{group.path}: not a regular file")
            continue
        mode = stat.S_IMODE(info.st_mode)
        if group.mode is not None and mode & ~group.mode:
            findings.append(f"{group.path}: mode {mode:04o} is more open than {group.mode:04o}")
        owner = _account(info.st_uid, "user")
        if group.owner and owner != group.owner:
            findings.append(f"{group.path}: owner {owner}, not {group.owner}")
        group_name = _account(info.st_gid, "group")
        if group.group and group.mode is not None and group.mode & 0o070 and group_name != group.group:
            findings.append(f"{group.path}: group {group_name}, not {group.group}")
        try:
            present = _names_in(path)
        except OSError as error:
            findings.append(f"{group.path}: cannot be read ({error.strerror})")
            continue
        for name in sorted(group.holds - present):
            findings.append(f"{group.path}: does not hold {name}")
    return findings


# --- command line ------------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--area", type=Path, default=AREA, help="the foundation-platform tree (default: this release)")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check")
    sub.add_parser("render")
    parsers = {name: sub.add_parser(name) for name in ("host", "rename", "set")}
    for parser_ in parsers.values():
        parser_.add_argument("--root", type=Path, default=Path("/"), help="filesystem root the group paths are under")
    parsers["set"].add_argument("group", help="the contract group whose file is written")
    parsers["set"].add_argument("source", type=Path, help="a file of NAME=value lines the group holds")
    sub.add_parser("credentials").add_argument("--repo", type=Path, default=None,
                                               help="the repository whose tracked files are read (default: the one holding --area)")
    for name in ("properties", "names"):
        sub.add_parser(name).add_argument("consumer")
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            findings = check(args.area)
            for finding in findings:
                print(f"FAIL runtime-secrets: {finding}", file=sys.stderr)
            if not findings:
                print(f"OK runtime-secrets ({len(load(args.area).consumers)} consumers)")
            return 1 if findings else 0
        if args.command == "render":
            for unit in render(args.area):
                print(f"rendered {unit}")
            return 0
        if args.command == "credentials":
            findings = credentials(args.repo or args.area.resolve().parents[1], args.area)
            for finding in findings:
                print(f"FAIL credential-names: {finding}", file=sys.stderr)
            if not findings:
                print("OK credential-names (every credential name is derived from its Cloudflare token, root ADR-0247)")
            return 1 if findings else 0
        if args.command == "set":
            for name in set_names(args.area, args.group, args.source, args.root):
                print(f"set {name} in {args.group}")
            return 0
        if args.command == "rename":
            for entry in rename(args.area, args.root):
                print(f"renamed {entry}")
            return 0
        if args.command == "host":
            findings = host(args.area, args.root)
            for finding in findings:
                print(f"FAIL runtime-secrets host: {finding}", file=sys.stderr)
            if not findings:
                print("OK runtime-secrets host")
            return 1 if findings else 0
        contract = load(args.area)
        consumer = contract.consumer(args.consumer)
        if args.command == "properties":
            print(" ".join(f"-p EnvironmentFile={line}" for line in rendered_lines(contract, consumer)))
        else:
            print("\n".join(sorted(consumer.needs)))
        return 0
    except ContractError as error:
        print(f"FAIL runtime-secrets: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
