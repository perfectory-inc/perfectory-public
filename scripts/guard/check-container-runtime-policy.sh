#!/usr/bin/env bash
# Checks tracked container-runtime declarations. A Compose file is normalized by the pinned
# `docker compose config` (tools/docker-compose.env) and judged by the Conftest policies in tools/conftest/compose: the
# engine resolves anchors, `extends`, `include` and `!reset`, so no rule needs a YAML subset. A
# Dockerfile is read by Conftest's Dockerfile parser (BuildKit's) and judged by the same directory's
# `dockerfile` policy. The shell's docker build/run/pull lines are read here.
set -euo pipefail

root="${1:-.}"
if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL container-runtime-policy: python3 is required" >&2
  exit 1
fi

guard_root="$(cd "$(dirname "$0")/../.." && { pwd -W 2>/dev/null || pwd -P; })"
python3 - "$root" "$guard_root" <<'PY'
from __future__ import annotations

import concurrent.futures
import json
import pathlib
import re
import shlex
import subprocess
import sys


ROOT = pathlib.Path(sys.argv[1]).resolve()
GUARD_ROOT = pathlib.Path(sys.argv[2]).resolve()
COMPOSE_POLICY = GUARD_ROOT / "tools" / "conftest" / "compose"
sys.path.insert(0, str(GUARD_ROOT / "scripts" / "ci"))
import compose_model  # noqa: E402  (the guard's own repository, also for a fixture root)
HEX64 = re.compile(r"[0-9a-f]{64}")
IMAGE_VARIABLE = re.compile(r"\$(?:\{)?([A-Z][A-Z0-9_]*IMAGE[A-Z0-9_]*)(?:\})?$")
IMAGE_WITH_DEFAULT = re.compile(
    r"\$\{([A-Z][A-Z0-9_]*IMAGE[A-Z0-9_]*):-([^}]+)\}$"
)
STATIC_RELEASE_CONTRACT = pathlib.PurePosixPath(
    "platforms/foundation-platform/config/static-release-toolchain.contract.json"
)
STATIC_RELEASE_PROJECTOR = pathlib.PurePosixPath(
    "scripts/tiles/static_release_toolchain_contract.py"
)
STATIC_RELEASE_PROJECTION = re.compile(
    r'toolchain_environment="\$\(\s*python3\s+"\$REPO_ROOT/'
    + re.escape(STATIC_RELEASE_PROJECTOR.as_posix())
    + r'"\s+image-env\s+--shell\s*\)"[^\n]*\n\s*'
    r'eval\s+"\$toolchain_environment"',
    flags=re.MULTILINE,
)
CONTRACT_COMPOSE_OWNER = re.compile(
    r"^x-perfectory-contract-image-owner:\s*([^\s#]+)\s*(?:#.*)?$",
    flags=re.MULTILINE,
)


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


def load_contract_images() -> dict[str, str]:
    contract_present = STATIC_RELEASE_CONTRACT.as_posix() in tracked_set
    projector_present = STATIC_RELEASE_PROJECTOR.as_posix() in tracked_set
    if not contract_present and not projector_present:
        return {}
    if not contract_present or not projector_present:
        raise RuntimeError(
            "static-release contract projection requires both its tracked contract and projector"
        )
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / STATIC_RELEASE_PROJECTOR),
            "--contract",
            str(ROOT / STATIC_RELEASE_CONTRACT),
            "image-env",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    value = json.loads(completed.stdout)
    if not isinstance(value, dict) or not value:
        raise RuntimeError("static-release projector returned no image environment")
    images: dict[str, str] = {}
    for raw_name, raw_reference in value.items():
        if (
            not isinstance(raw_name, str)
            or not IMAGE_VARIABLE.fullmatch(f"${raw_name}")
            or not isinstance(raw_reference, str)
            or not image_is_immutable(raw_reference)
        ):
            raise RuntimeError("static-release projector returned an invalid image environment")
        images[raw_name] = raw_reference
    return images


def line_number(text: str, position: int) -> int:
    return text.count("\n", 0, position) + 1


def contract_projection_precedes(text: str, use_line: int) -> bool:
    matches = list(STATIC_RELEASE_PROJECTION.finditer(text))
    return len(matches) == 1 and line_number(text, matches[0].end()) <= use_line


def contract_image_has_no_assignment(text: str, name: str) -> bool:
    return not assignment_values(text, name)


def contract_compose_owner_is_valid(path: str, text: str) -> bool:
    owners = list(CONTRACT_COMPOSE_OWNER.finditer(text))
    if len(owners) != 1:
        return False
    owner = pathlib.PurePosixPath(owners[0].group(1))
    owner_name = owner.as_posix()
    if (
        owner.is_absolute()
        or ".." in owner.parts
        or any(token in owner_name for token in ("$", "{", "}", "\\", "://"))
        or not owner_name.endswith((".sh", ".bash"))
        or owner_name not in tracked_set
    ):
        return False
    owner_text = read_text(owner_name)
    if owner_text is None:
        return False
    relative_assignment = re.compile(
        r'^\s*COMPOSE_FILE_RELATIVE=(["\'])'
        + re.escape(path)
        + r'\1\s*$',
        flags=re.MULTILINE,
    )
    compose_command = re.search(
        r'docker\s+compose\b[\s\S]{0,800}?--file\s+'
        r'"\$\(host_path\s+"\$REPO_ROOT/\$COMPOSE_FILE_RELATIVE"\)"',
        owner_text,
    )
    if relative_assignment.search(owner_text) is None or compose_command is None:
        return False
    if not contract_projection_precedes(
        owner_text, line_number(owner_text, compose_command.start())
    ):
        return False
    return all(
        contract_image_has_no_assignment(owner_text, name) for name in contract_images
    )


def judge(path: str, namespace: str, target: pathlib.Path, data: dict | None) -> None:
    try:
        for message in compose_model.judge(COMPOSE_POLICY, namespace, target, data):
            fail(path, 0, message)
    except compose_model.ComposeError as error:
        fail(path, 0, str(error))


def check_compose(path: str, text: str) -> None:
    source = ROOT / path
    judge(path, "compose_source", source, None)
    try:
        model = compose_model.write_model(path, compose_model.model([source]))
    except compose_model.ComposeError as error:
        fail(path, 0, str(error))
        return
    owned = bool(CONTRACT_COMPOSE_OWNER.search(text))
    judge(path, "compose", model, {"context": {
        "contract_images": sorted(contract_images),
        "contract_owner_valid": owned and contract_compose_owner_is_valid(path, text),
    }})


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
    # BuildKit returns no parser directives; they are the leading `# key=value` comment lines.
    syntax = None
    for raw in text.splitlines():
        directive = re.match(r"^\s*#\s*([A-Za-z]+)\s*=\s*(\S+)\s*$", raw)
        if not directive:
            break
        if directive.group(1).lower() == "syntax":
            syntax = directive.group(2)
    try:
        for message in compose_model.judge(COMPOSE_POLICY, "dockerfile", ROOT / path,
                                           {"context": {"syntax": syntax}}, parser="dockerfile"):
            fail(path, 0, message)
    except compose_model.ComposeError as error:
        fail(path, 0, str(error))


def assignment_values(text: str, name: str) -> list[str]:
    assignment = re.compile(
        rf"(?:^|[;\s])(?:(?:export|readonly)\s+|declare\s+-[A-Za-z]+\s+)?"
        rf"{re.escape(name)}=(\"[^\"]*\"|'[^']*'|[^\s;]+)",
        flags=re.MULTILINE,
    )
    return [unquote_yaml_scalar(match.group(1)) for match in assignment.finditer(text)]


def resolve_image_variable(path: str, text: str, token: str, use_line: int) -> str | None:
    with_default = IMAGE_WITH_DEFAULT.fullmatch(token)
    if with_default:
        return with_default.group(2)
    variable = IMAGE_VARIABLE.fullmatch(token)
    if not variable:
        return None
    name = variable.group(1)
    if name in contract_images:
        if (
            contract_projection_precedes(text, use_line)
            and contract_image_has_no_assignment(text, name)
        ):
            return contract_images[name]
        return None
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


def local_build_context(value: str) -> bool:
    value = unquote_yaml_scalar(value)
    if value == ".":
        return True
    if not value.startswith("./") or any(token in value for token in ("$", "{", "}", "\\")):
        return False
    return ".." not in pathlib.PurePosixPath(value).parts


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
            resolved = (
                resolve_image_variable(path, text, reference, line)
                if reference.startswith("$")
                else reference
            )
            if resolved is None or not image_is_immutable(resolved):
                fail(path, line, f"docker {command} image must resolve to tag@sha256 (or *:local)")


errors: list[str] = []
try:
    files = tracked_files()
except (OSError, subprocess.CalledProcessError) as error:
    print(f"FAIL container-runtime-policy: cannot enumerate tracked files: {error}", file=sys.stderr)
    raise SystemExit(1)
tracked_set = set(files)
try:
    contract_images = load_contract_images()
except (
    OSError,
    RuntimeError,
    json.JSONDecodeError,
    subprocess.CalledProcessError,
    subprocess.TimeoutExpired,
) as error:
    print(f"FAIL container-runtime-policy: cannot load contract images: {error}", file=sys.stderr)
    raise SystemExit(1)

compose_files: list[tuple[str, str]] = []
for relative in files:
    text = read_text(relative)
    if text is None:
        continue
    if is_compose_path(relative) or (relative.endswith((".yml", ".yaml")) and looks_like_compose(text)):
        compose_files.append((relative, text))
    if is_dockerfile_path(relative):
        check_dockerfile(relative, text)
    if relative.endswith((".sh", ".bash")):
        check_shell(relative, text)

# Each Compose file is normalized and judged on its own; `docker compose config` dominates the run.
if compose_files:
    compose_model.conftest_binary()
    compose_model.compose_binary()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda entry: check_compose(*entry), compose_files))
if errors:
    for error in errors:
        print(f"FAIL container-runtime-policy: {error}", file=sys.stderr)
    raise SystemExit(1)
print("OK container-runtime-policy")
PY
