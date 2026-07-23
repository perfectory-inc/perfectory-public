#!/usr/bin/env bash
# Checks tracked container-runtime declarations without requiring Docker or a
# YAML dependency. This is deliberately not a general YAML parser: security-
# relevant Compose keys use a narrow syntax, and ambiguous forms fail closed.
set -euo pipefail

root="${1:-.}"
if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL container-runtime-policy: python3 is required" >&2
  exit 1
fi

python3 - "$root" <<'PY'
from __future__ import annotations

import ast
import pathlib
import posixpath
import re
import shlex
import subprocess
import sys


ROOT = pathlib.Path(sys.argv[1]).resolve()
HEX64 = re.compile(r"[0-9a-f]{64}")
IMAGE_VARIABLE = re.compile(r"\$(?:\{)?([A-Z][A-Z0-9_]*IMAGE[A-Z0-9_]*)(?:\})?$")
IMAGE_WITH_DEFAULT = re.compile(
    r"\$\{([A-Z][A-Z0-9_]*IMAGE[A-Z0-9_]*):-([^}]+)\}$"
)
SAFE_BIND = re.compile(
    r"^(?:127\.0\.0\.1|\$\{[A-Z_][A-Z0-9_]*:-127\.0\.0\.1\}):"
    r"(?:[0-9]+|\$\{[A-Z_][A-Z0-9_]*:-[0-9]+\}):[0-9]+(?:/(?:tcp|udp))?$"
)
SECRET_KEY = re.compile(r"(?:^|_)(?:PASSWORD|MASTERKEY|SECRET|TOKEN|KEY)(?:$|_)")
REQUIRED_ENV = re.compile(r"\$\{[A-Z][A-Z0-9_]*:\?[^}]+\}")


def fail(path: str, line: int, message: str) -> None:
    location = f"{path}:{line}" if line else path
    errors.append(f"{location}: {message}")


def tracked_files() -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "-z"],
        check=True,
        stdout=subprocess.PIPE,
    )
    return [item.decode("utf-8") for item in result.stdout.split(b"\0") if item]


def read_text(relative: str) -> str | None:
    try:
        return (ROOT / relative).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None


def unquote_yaml_scalar(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return re.split(r"\s+#", value, maxsplit=1)[0].strip()


def image_is_immutable(reference: str) -> bool:
    reference = unquote_yaml_scalar(reference)
    if not reference or any(char.isspace() for char in reference):
        return False
    if reference.endswith(":local") and "@" not in reference:
        return True
    if reference.count("@sha256:") != 1:
        return False
    named, digest = reference.rsplit("@sha256:", 1)
    if not HEX64.fullmatch(digest):
        return False
    final_component = named.rsplit("/", 1)[-1]
    if ":" not in final_component:
        return False
    _, tag = final_component.rsplit(":", 1)
    return bool(tag) and not any(token in named for token in ("$", "{", "}"))


def is_compose_path(relative: str) -> bool:
    name = pathlib.PurePosixPath(relative).name.lower()
    return bool(
        re.fullmatch(r"(?:docker-)?compose(?:[.-][a-z0-9_.-]+)?\.ya?ml", name)
        or re.fullmatch(r"[a-z0-9_.-]+\.compose\.ya?ml", name)
    )


def looks_like_compose(text: str) -> bool:
    return bool(
        re.search(r"^(?:services|include):", text, flags=re.MULTILINE)
        or re.search(r"^[\"'](?:services|include)[\"']\s*:", text, flags=re.MULTILINE)
    )


def check_port(path: str, line: int, value: object) -> None:
    if not isinstance(value, str) or not SAFE_BIND.fullmatch(value):
        fail(
            path,
            line,
            "published Compose ports must use short syntax with a 127.0.0.1 "
            "literal or ${NAME:-127.0.0.1} host default",
        )


def indentation(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def parent_mapping_key(lines: list[str], child_index: int) -> str | None:
    child_indent = indentation(lines[child_index])
    for previous in range(child_index - 1, -1, -1):
        raw = lines[previous]
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        if indentation(raw) >= child_indent:
            continue
        match = re.match(r"^\s*([A-Za-z0-9_-]+):(?:\s.*)?$", raw)
        return match.group(1) if match else None
    return None


def local_build_context(value: str) -> bool:
    value = unquote_yaml_scalar(value)
    if value == ".":
        return True
    if not value.startswith("./") or any(token in value for token in ("$", "{", "}", "\\")):
        return False
    return ".." not in pathlib.PurePosixPath(value).parts


def check_build(path: str, lines: list[str], index: int, match: re.Match[str]) -> None:
    base_indent = len(match.group(1))
    inline = re.split(r"\s+#", match.group(2), maxsplit=1)[0].strip()
    if inline:
        if not local_build_context(inline):
            fail(path, index + 1, "Compose build context must be a literal repository-relative path")
        return

    child = index + 1
    context_found = False
    while child < len(lines):
        raw = lines[child]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            child += 1
            continue
        if indentation(raw) <= base_indent:
            break
        key_match = re.match(r"^\s*([A-Za-z0-9_-]+):\s*(.*?)\s*$", raw)
        if key_match:
            key, value = key_match.groups()
            if key in {"additional_contexts", "dockerfile_inline"}:
                fail(path, child + 1, f"unsupported security-relevant Compose build key: {key}")
            if key == "context":
                context_found = True
                if not local_build_context(value):
                    fail(path, child + 1, "Compose build context must be a literal repository-relative path")
        child += 1
    if not context_found:
        fail(path, index + 1, "long-form Compose build requires an explicit local context")


def check_include(path: str, lines: list[str], index: int, match: re.Match[str]) -> None:
    base_indent = len(match.group(1))
    if match.group(2).strip():
        fail(path, index + 1, "Compose include must use an auditable local path list")
        return
    child = index + 1
    found = False
    while child < len(lines):
        raw = lines[child]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            child += 1
            continue
        if indentation(raw) <= base_indent:
            break
        found = True
        item = re.match(r"^\s*-\s+path:\s+([^\s#]+)\s*(?:#.*)?$", raw)
        if not item:
            fail(path, child + 1, "Compose include entries must be literal '- path: compose*.yml' values")
            child += 1
            continue
        include_path = unquote_yaml_scalar(item.group(1))
        candidate = posixpath.normpath(
            posixpath.join(str(pathlib.PurePosixPath(path).parent), include_path)
        )
        if (
            include_path.startswith("/")
            or ".." in pathlib.PurePosixPath(include_path).parts
            or any(token in include_path for token in ("$", "{", "}", "\\", "://"))
            or not is_compose_path(candidate)
            or candidate not in tracked_set
        ):
            fail(path, child + 1, "Compose include path must name a tracked local Compose file")
        child += 1
    if not found:
        fail(path, index + 1, "Compose include list is empty or malformed")


def check_compose(path: str, text: str) -> None:
    lines = text.splitlines()
    for index, raw in enumerate(lines):
        content = re.split(r"\s+#", raw, maxsplit=1)[0]
        if "\t" in raw:
            fail(path, index + 1, "tabs are unsupported in Compose policy inputs")
        if re.match(r"^\s*[\"'][^\"']+[\"']\s*:", content):
            fail(path, index + 1, "quoted Compose mapping keys are unsupported")
        if re.match(r"^\s*[A-Za-z0-9_-]+\s+:", content):
            fail(path, index + 1, "whitespace before Compose mapping colons is unsupported")
        if re.match(r"^\s*[?:](?:\s|$)", content):
            fail(path, index + 1, "explicit YAML mapping keys are unsupported")
        if re.search(r":\s*\{", content):
            fail(path, index + 1, "flow-style Compose mappings are unsupported")
        if re.search(r":\s*!", content):
            fail(path, index + 1, "custom YAML tags are unsupported in Compose policy inputs")
        if re.match(r"^\s*extends:", content):
            fail(path, index + 1, "Compose extends indirection is unsupported")
        include_match = re.match(r"^(\s*)include:\s*(.*?)\s*$", content)
        if include_match:
            check_include(path, lines, index, include_match)

        anchors = re.findall(r":\s*&([A-Za-z_][A-Za-z0-9_-]*)", content)
        if anchors and not re.match(
            r"^\s*(?:environment|x-[A-Za-z0-9_-]*environment):\s*&[A-Za-z_][A-Za-z0-9_-]*\s*$",
            content,
        ):
            fail(path, index + 1, "YAML anchors are allowed only for environment mappings")
        aliases = re.findall(r"\*([A-Za-z_][A-Za-z0-9_-]*)", content)
        if aliases:
            merge = re.match(r"^\s*<<:\s*\*[A-Za-z_][A-Za-z0-9_-]*\s*$", content)
            if not merge or parent_mapping_key(lines, index) != "environment":
                fail(path, index + 1, "YAML aliases are allowed only as environment mapping merges")

        namespace_match = re.match(
            r"^\s*(?:network_mode|ipc|pid|uts|userns_mode):\s*(.+?)\s*$", content
        )
        if namespace_match:
            namespace = unquote_yaml_scalar(namespace_match.group(1))
            if "$" in namespace:
                fail(path, index + 1, "namespace sharing modes must be literal")
            elif namespace == "host":
                fail(path, index + 1, "host namespace sharing is forbidden")

        privileged_match = re.match(r"^\s*privileged:\s*(.+?)\s*$", content)
        if privileged_match and unquote_yaml_scalar(privileged_match.group(1)) != "false":
            fail(path, index + 1, "privileged Compose services are forbidden")
        if re.search(r"(?:^|/)(?:var/)?run/docker\.sock(?:$|[:/])|//\./pipe/docker_engine", content):
            fail(path, index + 1, "mounting a container-engine control socket is forbidden")

        build_match = re.match(r"^(\s*)build:\s*(.*?)\s*$", content)
        if build_match:
            check_build(path, lines, index, build_match)

        environment_match = re.match(r"^\s*([A-Z][A-Z0-9_]*):\s*(.+?)\s*$", raw)
        if environment_match and SECRET_KEY.search(environment_match.group(1)):
            value = unquote_yaml_scalar(environment_match.group(2))
            if not REQUIRED_ENV.fullmatch(value):
                fail(path, index + 1, "secret-like Compose environment values require ${NAME:?message}")

        image_match = re.match(r"^\s*image:\s*(.+?)\s*$", raw)
        if image_match and not image_is_immutable(image_match.group(1)):
            fail(path, index + 1, "external Compose images require tag@sha256; only *:local may omit a digest")

        ports_match = re.match(r"^(\s*)ports:\s*(.*?)\s*$", raw)
        if not ports_match:
            continue
        base_indent = len(ports_match.group(1))
        inline = re.split(r"\s+#", ports_match.group(2), maxsplit=1)[0].strip()
        if inline:
            try:
                values = ast.literal_eval(inline)
            except (SyntaxError, ValueError):
                fail(path, index + 1, "ports must be an inline string list or an indented short-syntax list")
                continue
            if not isinstance(values, list):
                fail(path, index + 1, "ports must be a list")
                continue
            for value in values:
                check_port(path, index + 1, value)
            continue

        child = index + 1
        found = False
        while child < len(lines):
            candidate = lines[child]
            stripped = candidate.strip()
            if not stripped or stripped.startswith("#"):
                child += 1
                continue
            indent = len(candidate) - len(candidate.lstrip(" "))
            if indent <= base_indent:
                break
            found = True
            item = re.match(r"^\s*-\s*([\"'])(.*?)\1\s*(?:#.*)?$", candidate)
            if not item:
                fail(path, child + 1, "ports entries must use quoted short syntax")
            else:
                check_port(path, child + 1, item.group(2))
            child += 1
        if not found:
            fail(path, index + 1, "ports list is empty or malformed")


def is_dockerfile_path(relative: str) -> bool:
    name = pathlib.PurePosixPath(relative).name.lower()
    return name.startswith("dockerfile") or name.startswith("containerfile") or name.endswith(".dockerfile")


def shell_logical_lines(text: str) -> list[tuple[int, str]]:
    output: list[tuple[int, str]] = []
    buffer: list[str] = []
    start = 1
    for line_number, raw in enumerate(text.splitlines(), start=1):
        if not buffer:
            start = line_number
        continued = raw.rstrip().endswith("\\")
        buffer.append(raw.rstrip()[:-1] if continued else raw)
        if continued:
            continue
        candidate = "\n".join(buffer)
        if "docker" in candidate:
            try:
                shlex.split(candidate, comments=True, posix=True)
            except ValueError as error:
                if "No closing quotation" in str(error):
                    continue
        output.append((start, candidate))
        buffer = []
    if buffer:
        output.append((start, "\n".join(buffer)))
    return output


def check_dockerfile(path: str, text: str) -> None:
    aliases: set[str] = set()
    for line, logical in shell_logical_lines(text):
        syntax = re.match(r"^\s*#\s*syntax=(\S+)", logical, flags=re.IGNORECASE)
        if syntax and not image_is_immutable(syntax.group(1)):
            fail(path, line, "Dockerfile syntax frontends require tag@sha256")

        match = re.match(
            r"^\s*FROM\s+(?:--platform=\S+\s+)?(\S+)(?:\s+AS\s+(\S+))?",
            logical,
            flags=re.IGNORECASE,
        )
        if not match:
            external_stages: list[str] = []
            if re.match(r"^\s*COPY\b", logical, flags=re.IGNORECASE):
                external_stages.extend(
                    unquote_yaml_scalar(value)
                    for value in re.findall(r"(?:^|\s)--from=(\"[^\"]+\"|'[^']+'|[^\s]+)", logical)
                )
            if re.match(r"^\s*RUN\b", logical, flags=re.IGNORECASE):
                for mount in re.findall(r"(?:^|\s)--mount=([^\s]+)", logical):
                    external_stages.extend(
                        option.split("=", 1)[1]
                        for option in mount.split(",")
                        if option.startswith("from=") and len(option.split("=", 1)) == 2
                    )
            for source in external_stages:
                if (
                    not source.isdigit()
                    and source.lower() not in aliases
                    and not image_is_immutable(source)
                ):
                    fail(path, line, "external Dockerfile stage images require tag@sha256")
            continue
        reference, alias = match.groups()
        if (
            reference.lower() != "scratch"
            and reference.lower() not in aliases
            and not image_is_immutable(reference)
        ):
            fail(path, line, "external Dockerfile FROM images require tag@sha256; only *:local may omit a digest")
        if alias:
            aliases.add(alias.lower())


def assignment_values(text: str, name: str) -> list[str]:
    assignment = re.compile(
        rf"(?:^|[;\s])(?:(?:export|readonly)\s+|declare\s+-[A-Za-z]+\s+)?"
        rf"{re.escape(name)}=(\"[^\"]*\"|'[^']*'|[^\s;]+)",
        flags=re.MULTILINE,
    )
    return [unquote_yaml_scalar(match.group(1)) for match in assignment.finditer(text)]


def resolve_image_variable(path: str, text: str, token: str) -> str | None:
    with_default = IMAGE_WITH_DEFAULT.fullmatch(token)
    if with_default:
        return with_default.group(2)
    variable = IMAGE_VARIABLE.fullmatch(token)
    if not variable:
        return None
    name = variable.group(1)
    local = assignment_values(text, name)
    if local:
        return local[0] if len(local) == 1 else None
    if "tools/container-images.env" in text:
        env_text = read_text("tools/container-images.env") or ""
        values = assignment_values(env_text, name)
        return values[0] if len(values) == 1 else None
    return None


RUN_OPTIONS_WITH_VALUE = {
    "--add-host", "--cap-add", "--cap-drop", "--cpus", "--device", "--dns",
    "--entrypoint", "--env", "--env-file", "--hostname", "--label", "--memory",
    "--mount", "--name", "--network", "--pids-limit", "--platform", "--publish", "--security-opt",
    "--tmpfs", "--user", "--volume", "--workdir", "-e", "-h", "-l", "-m", "-p",
    "-u", "-v", "-w",
}
PULL_OPTIONS_WITH_VALUE = {"--platform"}
BUILD_OPTIONS_WITH_VALUE = {
    "--add-host", "--build-arg", "--cache-from", "--cache-to", "--file", "--label",
    "--network", "--output", "--platform", "--secret", "--ssh", "--tag", "--target",
    "-f", "-t",
}


def command_image(tokens: list[str], command: str, index: int) -> str | None:
    options_with_value = RUN_OPTIONS_WITH_VALUE if command == "run" else PULL_OPTIONS_WITH_VALUE
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            index += 1
            break
        if token.startswith("--"):
            option = token.split("=", 1)[0]
            index += 1
            if "=" not in token and option in options_with_value:
                index += 1
            continue
        if token.startswith("-") and token != "-":
            index += 1
            if token in options_with_value:
                index += 1
            continue
        break
    return tokens[index] if index < len(tokens) else None


def check_docker_build(path: str, line: int, fragment: str) -> None:
    try:
        tokens = shlex.split(fragment, comments=True, posix=True)
    except ValueError:
        fail(path, line, "cannot parse docker build command")
        return
    if len(tokens) < 3 or tokens[0] != "docker":
        fail(path, line, "cannot parse docker build command prefix")
        return
    index = 3 if tokens[1:3] == ["buildx", "build"] else 2
    contexts: list[str] = []
    while index < len(tokens):
        token = tokens[index]
        if token == "--":
            contexts.extend(tokens[index + 1:])
            break
        if token.startswith("--"):
            option = token.split("=", 1)[0]
            if option == "--build-context":
                fail(path, line, "docker build additional contexts are unsupported")
            index += 1
            if "=" not in token and option in BUILD_OPTIONS_WITH_VALUE:
                index += 1
            continue
        if token.startswith("-") and token != "-":
            index += 1
            if token in BUILD_OPTIONS_WITH_VALUE:
                index += 1
            continue
        contexts.append(token)
        index += 1
    if len(contexts) != 1 or not local_build_context(contexts[0]):
        fail(path, line, "docker build requires one literal repository-relative context")


def check_shell(path: str, text: str) -> None:
    if path in {
        "scripts/guard/check-container-runtime-policy.sh",
        "scripts/guard/container-runtime-policy-self-test.sh",
    }:
        return
    for line, logical in shell_logical_lines(text):
        build_matches = list(re.finditer(r"\bdocker\s+(?:(?:buildx\s+)?build)\b", logical))
        for position, match in enumerate(build_matches):
            end = build_matches[position + 1].start() if position + 1 < len(build_matches) else len(logical)
            check_docker_build(path, line, logical[match.start():end])

        matches = list(
            re.finditer(
                r"\bdocker\s+(?:(?:image\s+pull)|(?:container\s+run)|pull|run)\b",
                logical,
            )
        )
        for position, match in enumerate(matches):
            end = matches[position + 1].start() if position + 1 < len(matches) else len(logical)
            fragment = logical[match.start():end]
            try:
                tokens = shlex.split(fragment, comments=True, posix=True)
            except ValueError:
                fail(path, line, "cannot parse docker run/pull command")
                continue
            if len(tokens) < 2 or tokens[0] != "docker":
                fail(path, line, "cannot parse docker run/pull command prefix")
                continue
            if tokens[1] in {"image", "container"}:
                if len(tokens) < 3:
                    fail(path, line, "cannot parse docker run/pull command")
                    continue
                command = tokens[2]
                image_index = 3
            else:
                command = tokens[1]
                image_index = 2
            reference = command_image(tokens, command, image_index)
            if reference is None:
                fail(path, line, f"cannot identify docker {command} image")
                continue
            resolved = resolve_image_variable(path, text, reference) if reference.startswith("$") else reference
            if resolved is None or not image_is_immutable(resolved):
                fail(path, line, f"docker {command} image must resolve to tag@sha256 (or *:local)")


errors: list[str] = []
try:
    files = tracked_files()
except (OSError, subprocess.CalledProcessError) as error:
    print(f"FAIL container-runtime-policy: cannot enumerate tracked files: {error}", file=sys.stderr)
    raise SystemExit(1)
tracked_set = set(files)

for relative in files:
    text = read_text(relative)
    if text is None:
        continue
    if is_compose_path(relative) or (relative.endswith((".yml", ".yaml")) and looks_like_compose(text)):
        check_compose(relative, text)
    if is_dockerfile_path(relative):
        check_dockerfile(relative, text)
    if relative.endswith((".sh", ".bash")):
        check_shell(relative, text)

if errors:
    for error in errors:
        print(f"FAIL container-runtime-policy: {error}", file=sys.stderr)
    raise SystemExit(1)
print("OK container-runtime-policy")
PY
