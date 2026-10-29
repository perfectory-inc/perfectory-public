#!/usr/bin/env python3
"""Every compile-time file read a container image compiles must be inside that image.

Prevents: 2026-10-29. #939 made `foundation-publisher-core` compile in
`contracts/schema-registry/registry.json` (`include_str!`, root ADR-0336), and
`services/foundation-outbox-publisher/Dockerfile.lakehouse-control` never copied `contracts/`.
Every release build on main failed from then on, the host deployed nothing for about 11 hours and
about 25 merged pull requests (security fixes among them) never reached production. Pull-request CI
compiled the crate from the checkout, where the file exists, so nothing failed before main
(root ADR-0345).

build-coupling-baseline.sh counts these reads; this is the same inventory, followed into the images.
For every tracked Dockerfile that runs `cargo build` or `cargo chef cook`:

- its build context is the one a tracked compose file builds it with (`build: context/dockerfile`);
- the crates compiled are the `-p` packages and every workspace path dependency they reach through
  normal and build dependencies, with Cargo's feature unification (a dev-dependency's feature,
  such as `test-fixtures`, is not on in a release build);
- the sources compiled are each crate's module tree from its targets and build script, leaving out
  `mod` items whose `#[cfg(...)]` is false in a release build (`test`, a feature nothing enables);
- every `include_str!` / `include_bytes!` / `sqlx::migrate!` path in those sources, resolved as
  rustc resolves it, must be a file the image holds at the same place: a COPY of the build stage
  (or a stage it is built FROM) carries it to `<WORKDIR>/<path>`, and `.dockerignore` (whose
  `**/tests` drops every tests directory) does not exclude it.

It reads Rust textually, as the count does: a read it cannot resolve to a literal path fails rather
than passing unseen.

    build-coupling-baseline.py <repository root>
"""

from __future__ import annotations

import functools
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path, PurePosixPath

READ_MACRO = re.compile(r"\b(?:include_str!|include_bytes!|sqlx::migrate!)")
INCLUDE = re.compile(
    r"\binclude_(?:str|bytes)!\s*\(\s*(?:\"([^\"]+)\"|concat!\s*\(\s*env!\s*\(\s*\"CARGO_MANIFEST_DIR\"\s*\)\s*,"
    r"\s*\"([^\"]+)\"\s*\))\s*,?\s*\)"
)
MIGRATE = re.compile(r"\bsqlx::migrate!\s*\(\s*(?:\"([^\"]*)\")?\s*\)")
MOD = re.compile(r"((?:#\[[^\]]*\]\s*)*)(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+(\w+)\s*([;{])")
CFG = re.compile(r"#\[cfg\((.*)\)\]\s*$")
PATH_ATTR = re.compile(r"#\[path\s*=\s*\"([^\"]+)\"\]")
# What a string, char or comment hides from brace counting; replaced by spaces, so offsets hold.
OPAQUE = re.compile(
    r"b?r(#*)\"(?:.|\n)*?\"\1|b?\"(?:\\.|[^\"\\])*\"|b?'(?:\\.|[^'\\])'|//[^\n]*|/\*(?:.|\n)*?\*/"
)
# `cargo chef cook` is left out: it compiles the third-party dependencies only, never the sources of a
# workspace crate.
CARGO_RUN = re.compile(r"\bcargo\s+(?:\+\S+\s+)?build\b([^&;|]*)")


def git_files(root: Path) -> list[str]:
    out = subprocess.run(["git", "-C", str(root), "ls-files", "-z"], check=True, capture_output=True).stdout
    return [path for path in out.decode("utf-8").split("\0") if path]


def norm(path: PurePosixPath | str) -> str | None:
    """A normalized relative path, or None when it climbs out of its base."""
    parts: list[str] = []
    for part in PurePosixPath(path).parts:
        if part == "..":
            if not parts:
                return None
            parts.pop()
        elif part not in (".", "", "/"):
            parts.append(part)
    return "/".join(parts)


# --- cfg ----------------------------------------------------------------------------------------


def cfg_true(expression: str, features: set[str]) -> bool:
    """A cfg predicate in a release build: `test` is false, a feature is on when enabled, any other
    predicate (target, unix, …) counts as true, so a read behind it is held to the image."""
    tokens = re.findall(r'\w+|"[^"]*"|[(),=]', expression)
    position = 0

    def take() -> str:
        nonlocal position
        position += 1
        return tokens[position - 1]

    def predicate() -> bool:
        word = take()
        if word in ("any", "all", "not") and position < len(tokens) and tokens[position] == "(":
            take()
            values = []
            while tokens[position] != ")":
                values.append(predicate())
                if tokens[position] == ",":
                    take()
            take()
            if word == "not":
                return not values[0]
            return any(values) if word == "any" else all(values)
        if position < len(tokens) and tokens[position] == "=":
            take()
            value = take().strip('"')
            return value in features if word == "feature" else True
        return word != "test"

    try:
        return predicate()
    except IndexError:
        return True


# --- the crates an image compiles ---------------------------------------------------------------


class Workspace:
    def __init__(self, root: Path, directory: str, tracked: set[str]):
        self.root, self.directory = root, directory
        manifest = tomllib.loads((root / directory / "Cargo.toml").read_text(encoding="utf-8"))
        workspace = manifest.get("workspace")
        if workspace is None:
            raise ValueError(f"{directory}/Cargo.toml is not a workspace root")
        inherited = workspace.get("dependencies", {})
        self.crates: dict[str, dict] = {}
        for member_glob in workspace.get("members", []):
            pattern = re.compile(re.escape(norm(f"{directory}/{member_glob}")).replace(r"\*", "[^/]*") + r"/Cargo\.toml\Z")
            for path in sorted(p for p in tracked if pattern.match(p)):
                self._add(path.removesuffix("/Cargo.toml"), inherited)

    def _add(self, crate_dir: str, inherited: dict) -> None:
        manifest = tomllib.loads((self.root / crate_dir / "Cargo.toml").read_text(encoding="utf-8"))
        name = manifest["package"]["name"]
        deps = []
        tables = [manifest.get("dependencies", {}), manifest.get("build-dependencies", {})]
        for target in manifest.get("target", {}).values():
            tables += [target.get("dependencies", {}), target.get("build-dependencies", {})]
        for table in tables:
            for key, spec in table.items():
                spec = spec if isinstance(spec, dict) else {"version": spec}
                base = inherited.get(key, {}) if spec.get("workspace") else {}
                base = base if isinstance(base, dict) else {"version": base}
                path = spec.get("path") or base.get("path")
                if path is None:
                    continue
                path_root = crate_dir if spec.get("path") else self.directory
                deps.append({
                    "key": key,
                    "dir": norm(f"{path_root}/{path}"),
                    "optional": bool(spec.get("optional")),
                    "features": set(base.get("features", [])) | set(spec.get("features", [])),
                    "default": spec.get("default-features", base.get("default-features", True)) is not False,
                })
        self.crates[name] = {"dir": crate_dir, "manifest": manifest, "deps": deps}

    def by_dir(self, crate_dir: str) -> str | None:
        return next((name for name, crate in self.crates.items() if crate["dir"] == crate_dir), None)

    def compiled(self, packages: list[str], features: set[str], default: bool) -> dict[str, set[str]]:
        """Crate -> its enabled features, for a build of `packages` (Cargo's unification, resolver 2)."""
        enabled: dict[str, set[str]] = {}
        active: dict[str, set[str]] = {}  # crate -> the optional dependency keys switched on

        def enable(name: str, wanted: set[str], with_default: bool) -> bool:
            before = (len(enabled.get(name, ())), name in enabled)
            table = self.crates[name]["manifest"].get("features", {})
            have = enabled.setdefault(name, set())
            have |= wanted | ({"default"} if with_default and "default" in table else set())
            return (len(have), True) != before

        for package in packages:
            if package not in self.crates:
                raise ValueError(f"package {package} is not a member of {self.directory}")
            enable(package, set(features), default)
        changed = True
        while changed:
            changed = False
            for name in list(enabled):
                crate = self.crates[name]
                table = crate["manifest"].get("features", {})
                deps = {dep["key"]: dep for dep in crate["deps"]}
                on = active.setdefault(name, set())
                pending, dep_features = list(enabled[name]), {}
                while pending:
                    feature = pending.pop()
                    if feature in deps and deps[feature]["optional"] and feature not in table:
                        on.add(feature)
                    for item in table.get(feature, []):
                        if item.startswith("dep:"):
                            on.add(item[4:])
                        elif "/" in item:
                            key, sub = item.split("/", 1)
                            weak = key.endswith("?")
                            key = key.rstrip("?")
                            if not weak:
                                on.add(key)
                            dep_features.setdefault(key, set()).add(sub)
                        elif item not in enabled[name]:
                            enabled[name].add(item)
                            pending.append(item)
                for dep in crate["deps"]:
                    if dep["optional"] and dep["key"] not in on:
                        continue
                    target = self.by_dir(dep["dir"])
                    if target is None:
                        continue  # a path dependency outside this workspace builds from its own tree
                    if enable(target, dep["features"] | dep_features.get(dep["key"], set()), dep["default"]):
                        changed = True
        return enabled


def blank(match: re.Match[str]) -> str:
    text = match.group(0)
    return re.sub(r"[^\n]", " ", text) if "\n" in text else " " * len(text)


@functools.lru_cache(maxsize=None)
def source(root: Path, path: str) -> tuple[str, str]:
    """A source's text, and the same text with its strings, chars and comments blanked (same offsets)."""
    text = (root / path).read_text(encoding="utf-8")
    return text, OPAQUE.sub(blank, text)


def crate_targets(root: Path, crate: dict, tracked: set[str]) -> list[str]:
    directory, manifest = crate["dir"], crate["manifest"]
    targets = [f"{directory}/{manifest.get('lib', {}).get('path', 'src/lib.rs')}", f"{directory}/src/main.rs"]
    targets += [f"{directory}/{entry['path']}" for entry in manifest.get("bin", []) if "path" in entry]
    bins = f"{directory}/src/bin/"
    targets += [p for p in tracked if p.startswith(bins) and re.fullmatch(r"[^/]+\.rs|[^/]+/main\.rs", p[len(bins):])]
    build = manifest["package"].get("build", "build.rs")
    if build:
        targets.append(f"{directory}/{build}")
    return sorted({norm(t) for t in targets if norm(t) in tracked})


def compiled_sources(root: Path, crate: dict, features: set[str], tracked: set[str]) -> dict[str, list[tuple[int, int]]]:
    """Source file -> the spans (start, end) of it a release build leaves out, for one crate."""
    sources: dict[str, list[tuple[int, int]]] = {}
    roots = crate_targets(root, crate, tracked)
    pending = [(path, True) for path in roots]
    while pending:
        path, is_root = pending.pop()
        if path in sources:
            continue
        text, masked = source(root, path)
        excluded: list[tuple[int, int]] = []
        sources[path] = excluded
        file = PurePosixPath(path)
        own_dir = file.parent if is_root or file.name == "mod.rs" else file.parent / file.stem
        for match in MOD.finditer(masked):
            if any(start <= match.start() < end for start, end in excluded):
                continue
            attributes = text[match.start(1):match.end(1)]
            gated = [CFG.match(attr.strip()) for attr in re.findall(r"#\[[^\]]*\]", attributes)]
            compiled = all(cfg_true(gate.group(1), features) for gate in gated if gate)
            if match.group(3) == "{":
                if not compiled:
                    depth, end = 0, match.end(3) - 1
                    for index in range(match.end(3) - 1, len(masked)):
                        depth += {"{": 1, "}": -1}.get(masked[index], 0)
                        if depth == 0:
                            end = index + 1
                            break
                    excluded.append((match.start(), end if depth == 0 else len(masked)))
                continue
            if not compiled:
                continue
            explicit = PATH_ATTR.search(attributes)
            name = match.group(2)
            candidates = ([norm(file.parent / explicit.group(1))] if explicit
                          else [norm(own_dir / f"{name}.rs"), norm(own_dir / name / "mod.rs")])
            found = next((c for c in candidates if c in tracked), None)
            if found is None:
                raise ValueError(f"{path}: `mod {name};` names none of {candidates} (an inline module's file child is not followed)")
            pending.append((found, False))
    return sources


def compile_time_reads(root: Path, crate: dict, sources: dict[str, list[tuple[int, int]]]) -> list[tuple[str, int, str | None]]:
    """(source, line, repository path read or None when unresolvable) for every read a build compiles."""
    reads = []
    for path, excluded in sorted(sources.items()):
        # Positions come from the text with strings and comments blanked, so a macro a comment or a
        # string names is no read; the paths come from the text itself.
        text, code = source(root, path)
        found = {}
        for match in INCLUDE.finditer(text):
            literal, manifest_relative = match.groups()
            base = crate["dir"] if manifest_relative else str(PurePosixPath(path).parent)
            found[match.start()] = norm(f"{base}/{(literal or manifest_relative).lstrip('/')}")
        for match in MIGRATE.finditer(text):
            found[match.start()] = norm(f"{crate['dir']}/{match.group(1) if match.group(1) is not None else 'migrations'}")
        for match in READ_MACRO.finditer(code):
            if any(start <= match.start() < end for start, end in excluded):
                continue
            line = code.count("\n", 0, match.start()) + 1
            reads.append((path, line, found.get(match.start(), "\0unresolved")))
    return reads


# --- what an image holds ------------------------------------------------------------------------


def instructions(text: str) -> list[tuple[str, str]]:
    joined = re.sub(r"\\\r?\n", " ", "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#")))
    out = []
    for line in joined.splitlines():
        parts = line.strip().split(None, 1)
        if parts:
            out.append((parts[0].upper(), parts[1] if len(parts) > 1 else ""))
    return out


def copy_arguments(argument: str) -> tuple[list[str], str, dict[str, str]]:
    flags = {}
    while argument.startswith("--"):
        flag, _, argument = argument.partition(" ")
        key, _, value = flag[2:].partition("=")
        flags[key] = value
        argument = argument.lstrip()
    if argument.startswith("["):
        words = json.loads(argument)
    else:
        words = argument.split()
    return words[:-1], words[-1], flags


def builds(dockerfile_text: str) -> list[dict]:
    """Each `cargo build` / `cargo chef cook` run: its packages, features and the COPYs it sees."""
    stages: dict[str, dict] = {}
    stage: dict = {}
    found = []
    for keyword, argument in instructions(dockerfile_text):
        if keyword == "FROM":
            words = [w for w in argument.split() if not w.startswith("--")]
            base = stages.get(words[0].lower())
            stage = {"copies": list(base["copies"]) if base else [], "workdir": base["workdir"] if base else "/"}
            if len(words) >= 3 and words[1].lower() == "as":
                stages[words[2].lower()] = stage
        elif keyword == "WORKDIR":
            stage["workdir"] = norm(PurePosixPath(stage["workdir"]) / argument.strip()) or ""
            stage["workdir"] = "/" + stage["workdir"].lstrip("/")
        elif keyword in ("COPY", "ADD"):
            sources, destination, flags = copy_arguments(argument)
            if "from" in flags:
                continue
            if not destination.startswith("/"):
                destination = f"{stage['workdir'].rstrip('/')}/{destination}"
            stage["copies"].append((sources, destination))
        elif keyword == "RUN":
            for run in CARGO_RUN.finditer(argument):
                words = run.group(1).split()
                packages = [words[i + 1] for i, w in enumerate(words[:-1]) if w in ("-p", "--package")]
                packages += [w.split("=", 1)[1] for w in words if w.startswith("--package=")]
                features = {f for i, w in enumerate(words[:-1]) if w in ("-F", "--features") for f in re.split(r"[ ,]", words[i + 1]) if f}
                found.append({"packages": packages, "features": features, "default": "--no-default-features" not in words,
                              "all": "--workspace" in words or "--all" in words,
                              "copies": list(stage["copies"]), "workdir": stage["workdir"]})
    return found


def ignore_patterns(text: str) -> list[tuple[re.Pattern[str], bool, str]]:
    patterns = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        exclude = not line.startswith("!")
        line = norm(line.lstrip("!").strip().lstrip("/")) or ""
        out, i = [], 0
        while i < len(line):
            if line.startswith("**/", i):
                out.append("(?:.*/)?")
                i += 3
            elif line.startswith("**", i):
                out.append(".*")
                i += 2
            elif line[i] == "*":
                out.append("[^/]*")
                i += 1
            elif line[i] == "?":
                out.append("[^/]")
                i += 1
            else:
                out.append(re.escape(line[i]))
                i += 1
        patterns.append((re.compile("".join(out) + r"\Z"), exclude, raw.strip()))
    return patterns


def ignored_by(path: str, patterns: list[tuple[re.Pattern[str], bool, str]]) -> str | None:
    """The .dockerignore pattern that keeps `path` out of the context (Docker: last match wins, and a
    pattern matching a parent directory matches everything below it), or None."""
    parts = path.split("/")
    prefixes = ["/".join(parts[: i + 1]) for i in range(len(parts))]
    verdict = None
    for pattern, exclude, raw in patterns:
        if any(pattern.match(prefix) for prefix in prefixes):
            verdict = raw if exclude else None
    return verdict


def image_location(context_path: str, copies: list[tuple[list[str], str]], tracked_dirs: set[str]) -> set[str]:
    """Where the COPYs put one file of the context in the image."""
    places = set()
    for sources, destination in copies:
        into_dir = destination.endswith("/") or len(sources) > 1
        for source in sources:
            source = norm(source) or ""
            if any(ch in source for ch in "*?["):
                pattern = re.compile(source.replace(".", r"\.").replace("*", "[^/]*").replace("?", "[^/]"))
                parts = context_path.split("/")
                for depth in range(1, len(parts) + 1):
                    head = "/".join(parts[:depth])
                    if pattern.fullmatch(head):
                        rest = "/".join(parts[depth:])
                        base = f"{destination.rstrip('/')}/{parts[depth - 1]}"
                        places.add(f"{base}/{rest}" if rest else base)
                continue
            if source == "" or source in tracked_dirs:
                if source == "" or context_path.startswith(source + "/"):
                    rest = context_path if source == "" else context_path[len(source) + 1:]
                    places.add(f"{destination.rstrip('/')}/{rest}")
            elif context_path == source:
                places.add(f"{destination.rstrip('/')}/{PurePosixPath(source).name}" if into_dir else destination)
    return {"/" + (norm(place) or "") for place in places}


def compose_contexts(root: Path, tracked: list[str]) -> dict[str, set[str]]:
    """Dockerfile -> the contexts a tracked compose file builds it with (both repository paths)."""
    found: dict[str, set[str]] = {}
    for compose in tracked:
        name = PurePosixPath(compose).name
        if "compose" not in name or not name.endswith((".yml", ".yaml")):
            continue
        lines = (root / compose).read_text(encoding="utf-8").splitlines()
        for index, line in enumerate(lines):
            match = re.match(r"^(\s*)build:\s*(\S.*)?$", line)
            if not match:
                continue
            values = {"context": (match.group(2) or "").strip().strip("'\"") or ".", "dockerfile": "Dockerfile"}
            for following in lines[index + 1:]:
                if following.strip() and len(following) - len(following.lstrip()) <= len(match.group(1)):
                    break
                field = re.match(r"^\s*(context|dockerfile):\s*(\S+)", following)
                if field:
                    values[field.group(1)] = field.group(2).strip("'\"")
            context = norm(PurePosixPath(compose).parent / values["context"])
            dockerfile = norm(PurePosixPath(context) / values["dockerfile"])
            if context is not None and dockerfile is not None:
                found.setdefault(dockerfile, set()).add(context)
    return found


def check(root: Path) -> list[str]:
    tracked_list = git_files(root)
    tracked = set(tracked_list)
    tracked_dirs = {str(parent) for path in tracked_list for parent in PurePosixPath(path).parents if str(parent) != "."}
    contexts = compose_contexts(root, tracked_list)
    walked: dict[tuple[str, frozenset[str]], dict] = {}
    errors = []
    for dockerfile in sorted(p for p in tracked_list if PurePosixPath(p).name.startswith("Dockerfile")):
        runs = builds((root / dockerfile).read_text(encoding="utf-8"))
        if not runs:
            continue
        if dockerfile not in contexts:
            errors.append(f"{dockerfile} compiles Rust, but no tracked compose file builds it, so its context is unknown")
            continue
        for context in sorted(contexts[dockerfile]):
            ignore_file = f"{dockerfile}.dockerignore" if f"{dockerfile}.dockerignore" in tracked else f"{context}/.dockerignore".lstrip("/")
            patterns = ignore_patterns((root / ignore_file).read_text(encoding="utf-8")) if ignore_file in tracked else []
            try:
                workspace = Workspace(root, context, tracked)
            except (OSError, ValueError, KeyError) as error:
                errors.append(f"{dockerfile} (context {context}): {error}")
                continue
            seen: set[tuple[str, int]] = set()
            for run in runs:
                packages = list(workspace.crates) if run["all"] or not run["packages"] else run["packages"]
                try:
                    compiled = workspace.compiled(packages, run["features"], run["default"])
                except ValueError as error:
                    errors.append(f"{dockerfile}: {error}")
                    continue
                for name, features in sorted(compiled.items()):
                    crate = workspace.crates[name]
                    try:
                        key = (crate["dir"], frozenset(features))
                        sources = walked[key] if key in walked else walked.setdefault(
                            key, compiled_sources(root, crate, features, tracked))
                    except (OSError, ValueError) as error:
                        errors.append(f"{dockerfile}: {error}")
                        continue
                    for source, line, read in compile_time_reads(root, crate, sources):
                        if (source, line) in seen:
                            continue
                        seen.add((source, line))
                        where = f"{dockerfile} compiles {name}, whose {source}:{line}"
                        if read == "\0unresolved":
                            errors.append(f"{where} reads a path this guard cannot resolve; write it as a string literal")
                            continue
                        prefix = f"{context}/" if context else ""
                        if read is None or (prefix and not read.startswith(prefix)):
                            errors.append(f"{where} compiles in `{read}`, outside the build context `{context or '.'}`")
                            continue
                        inside = read[len(prefix):]
                        files = [inside] if read in tracked else [p[len(prefix):] for p in tracked_list if p.startswith(read + "/")]
                        if not files:
                            errors.append(f"{where} compiles in `{read}`, which is not a tracked file")
                            continue
                        expected = "/" + (norm(f"{run['workdir']}/{inside}") or "")
                        for file in files:
                            pattern = ignored_by(file, patterns)
                            target = "/" + (norm(f"{run['workdir']}/{file}") or "")
                            if pattern is not None:
                                errors.append(f"{where} compiles in `{prefix}{file}`, which {ignore_file} excludes (`{pattern}`)")
                                break
                            if target not in image_location(file, run["copies"], {d[len(prefix):] for d in tracked_dirs if d.startswith(prefix)} if prefix else tracked_dirs):
                                errors.append(f"{where} compiles in `{prefix}{file}`, which no COPY of the build stage puts at {expected}")
                                break
    return errors


def main(argv: list[str]) -> int:
    root = Path(argv[1] if len(argv) > 1 else Path(__file__).resolve().parents[2])
    errors = check(root)
    for error in errors:
        print(f"FAIL build-coupling-baseline: {error}", file=sys.stderr)
    if errors:
        print("    Copy the file into the image (Dockerfile COPY, not excluded by .dockerignore) or stop compiling",
              file=sys.stderr)
        print("    it in. 2026-10-29: a missing COPY of contracts/ failed every release build for 11 hours.",
              file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
