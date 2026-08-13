#!/usr/bin/env bash
# Threat model (ADR-0027): honest-mistake detection.
# Prevents: honest reintroduction of the two direct React class error-boundary
# lifecycle identifiers after the repository adopted @suspensive/react.
# Does not prevent: deliberate evasion or semantically equivalent dynamic code;
# this is neither a security boundary nor a complete semantic analysis.
#
# Scope: tracked JavaScript/TypeScript source with direct identifier spelling.
# Known limits: computed member names, Object.defineProperty/prototype mutation,
# generated or untracked source, and runtime construction can pass. The
# self-test deliberately records a computed-member pass so this limit cannot be
# mistaken for coverage. Comments, literals, regular expressions, and JSX text
# are excluded to keep the blocking guard's known false-positive count at zero.
# There is intentionally no inline suppression. A justified direct use changes
# this guard and its threat model in one reviewed commit.
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "FAIL hand-rolled-error-boundary: usage: $0 [repository-root]" >&2
  exit 2
fi

if [ "$#" -eq 1 ]; then
  root="$1"
else
  root="$(cd "$(dirname "$0")/../.." && pwd -P)"
fi

for command_name in git python3; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL hand-rolled-error-boundary: missing command '$command_name'" >&2
    exit 1
  }
done

python3 - "$root" <<'PY'
import os
import subprocess
import sys

root = sys.argv[1]
source_pathspecs = ("*.js", "*.jsx", "*.ts", "*.tsx", "*.mjs", "*.cjs", "*.mts", "*.cts")
listed = subprocess.run(
    ["git", "-C", root, "ls-files", "-z", "--", *source_pathspecs],
    check=False,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)
if listed.returncode != 0:
    sys.stderr.buffer.write(listed.stderr)
    print(
        "FAIL hand-rolled-error-boundary: could not enumerate tracked source files",
        file=sys.stderr,
    )
    raise SystemExit(1)

files = [path for path in listed.stdout.decode("utf-8").split("\0") if path]
forbidden = {"getDerivedStateFromError", "componentDidCatch"}
regex_prefix_keywords = {
    "await",
    "case",
    "delete",
    "do",
    "else",
    "in",
    "instanceof",
    "new",
    "of",
    "return",
    "throw",
    "typeof",
    "void",
    "yield",
}


def find_forbidden_identifiers(source):
    hits = []
    index = 0
    line = 1

    def advance():
        nonlocal index, line
        if source[index] == "\n":
            line += 1
        index += 1

    def unicode_escape_at(position):
        if not source.startswith("\\u", position):
            return None
        if position + 2 < len(source) and source[position + 2] == "{":
            end = source.find("}", position + 3)
            if end == -1:
                return None
            digits = source[position + 3 : end]
            consumed = end - position + 1
        else:
            end = position + 6
            if end > len(source):
                return None
            digits = source[position + 2 : end]
            consumed = 6
        if not digits or any(character not in "0123456789abcdefABCDEF" for character in digits):
            return None
        try:
            return chr(int(digits, 16)), consumed
        except (ValueError, OverflowError):
            return None

    def is_identifier_start(position):
        character = source[position]
        if character.isascii() and (character.isalpha() or character in "_$"):
            return True
        escaped = unicode_escape_at(position)
        return escaped is not None and escaped[0].isascii() and (
            escaped[0].isalpha() or escaped[0] in "_$"
        )

    def read_identifier():
        normalized = []
        while index < len(source):
            character = source[index]
            if character.isascii() and (character.isalnum() or character in "_$"):
                normalized.append(character)
                advance()
                continue
            escaped = unicode_escape_at(index)
            if escaped is None:
                break
            decoded, consumed = escaped
            if not decoded.isascii() or not (decoded.isalnum() or decoded in "_$"):
                break
            normalized.append(decoded)
            for _ in range(consumed):
                advance()
        return "".join(normalized)

    def is_jsx_text(position):
        """Recognize raw JSX children without pretending to parse TSX.

        This only excludes text between a tag-shaped closing angle bracket and
        the next JSX delimiter. Expressions in braces remain ordinary code.
        The guard's threat model treats this as false-positive control, not as
        added semantic coverage.
        """
        tag_end = position - 1
        while tag_end >= 0 and source[tag_end] not in ">;{}":
            tag_end -= 1
        if tag_end < 0 or source[tag_end] != ">":
            return False

        statement_start = max(
            source.rfind(";", 0, tag_end),
            source.rfind("{", 0, tag_end),
            source.rfind("}", 0, tag_end),
        )
        tag_start = source.rfind("<", statement_start + 1, tag_end)
        if tag_start == -1:
            return False
        tag_body = source[tag_start + 1 : tag_end]
        if tag_body.startswith("/"):
            tag_body = tag_body[1:]
        if tag_body and (tag_body[0].isspace() or not (tag_body[0].isalnum() or tag_body[0] in "_$")):
            return False

        text_end = position
        while text_end < len(source) and source[text_end] not in "<;{}":
            text_end += 1
        return text_end < len(source) and source[text_end] in "<{"

    def skip_quoted(quote):
        advance()
        while index < len(source):
            if source[index] == "\\":
                advance()
                if index < len(source):
                    advance()
            elif source[index] == quote:
                advance()
                return
            else:
                advance()

    def skip_line_comment():
        advance()
        advance()
        while index < len(source) and source[index] != "\n":
            advance()

    def skip_block_comment():
        advance()
        advance()
        while index < len(source):
            if source[index] == "*" and index + 1 < len(source) and source[index + 1] == "/":
                advance()
                advance()
                return
            advance()

    def skip_regex():
        advance()
        in_character_class = False
        while index < len(source):
            character = source[index]
            if character == "\n" or character == "\r":
                return
            if character == "\\":
                advance()
                if index < len(source):
                    advance()
                continue
            if character == "[":
                in_character_class = True
                advance()
                continue
            if character == "]" and in_character_class:
                in_character_class = False
                advance()
                continue
            if character == "/" and not in_character_class:
                advance()
                while index < len(source) and source[index].isascii() and source[index].isalpha():
                    advance()
                return
            advance()

    def scan_code(stop_at_template_brace=False):
        nonlocal index
        brace_depth = 0
        regex_allowed = True
        while index < len(source):
            character = source[index]
            next_character = source[index + 1] if index + 1 < len(source) else ""

            if character.isspace():
                advance()
                continue
            if character == "/" and next_character == "/":
                skip_line_comment()
                continue
            if character == "/" and next_character == "*":
                skip_block_comment()
                continue
            if character == "/" and regex_allowed:
                skip_regex()
                regex_allowed = False
                continue
            if character in ("'", '"'):
                skip_quoted(character)
                regex_allowed = False
                continue
            if character == "`":
                skip_template()
                regex_allowed = False
                continue

            if stop_at_template_brace:
                if character == "{":
                    brace_depth += 1
                    advance()
                    regex_allowed = True
                    continue
                if character == "}":
                    if brace_depth == 0:
                        advance()
                        return
                    brace_depth -= 1
                    advance()
                    regex_allowed = False
                    continue

            if is_identifier_start(index):
                identifier_line = line
                identifier_start = index
                identifier = read_identifier()
                if identifier in forbidden and not is_jsx_text(identifier_start):
                    hits.append((identifier, identifier_line))
                regex_allowed = identifier in regex_prefix_keywords
                continue

            if character.isascii() and character.isdigit():
                advance()
                while index < len(source) and source[index].isascii() and (
                    source[index].isalnum() or source[index] in "._"
                ):
                    advance()
                regex_allowed = False
                continue

            if character in ")]}":
                advance()
                regex_allowed = False
                continue
            if character == ".":
                advance()
                regex_allowed = False
                continue
            if character in "+-" and next_character == character:
                advance()
                advance()
                regex_allowed = False
                continue

            advance()
            regex_allowed = True

    def skip_template():
        advance()
        while index < len(source):
            if source[index] == "\\":
                advance()
                if index < len(source):
                    advance()
            elif source[index] == "`":
                advance()
                return
            elif source[index] == "$" and index + 1 < len(source) and source[index + 1] == "{":
                advance()
                advance()
                scan_code(True)
            else:
                advance()

    scan_code()
    return hits


failed = False
for path in files:
    with open(os.path.join(root, path), encoding="utf-8") as source_file:
        source = source_file.read()
    for identifier, line in find_forbidden_identifiers(source):
        print(
            f"FAIL hand-rolled-error-boundary: {path}:{line} uses {identifier}",
            file=sys.stderr,
        )
        failed = True

if failed:
    print(
        "    Use @suspensive/react, the canonical choice in docs/technology-stack.md section 1.1.",
        file=sys.stderr,
    )
    raise SystemExit(1)

print(f"OK hand-rolled-error-boundary ({len(files)} tracked source files)")
PY
