"""Every shell script the platform runs by its path is executable in git.

A script run by its path (a runbook's command, a systemd unit's Exec line, another script) needs
its executable bit, or the host answers `Permission denied` (systemd: 203/EXEC). The bit lives in
git (mode 100755), so this reads git, not the checkout: a Windows checkout cannot see it.
`measure-building-section-packs.sh` shipped as 100644 in #336, and the runbook's `systemd-run`
of it could never have started.

A reference runs the script by its path unless an interpreter is named before it (`bash x.sh`,
`sh x.sh`, `source x.sh`, `. x.sh`). Markdown is read inside fenced code blocks, and in inline code
that starts with the script and passes it arguments (`` `scripts/x.sh up -d` ``); shell comments
are skipped. A sentence or table cell that only names a script does not run it.
"""

import pathlib
import re
import subprocess
import unittest

PLATFORM_ROOT = pathlib.Path(__file__).resolve().parents[2]
# Where the platform's scripts are run from: runbooks and docs, systemd units, scripts.
SOURCES = ("docs/**/*.md", "*.md", "infra/systemd/*", "scripts/**/*.sh", "scripts/**/*.md")
SCRIPT = re.compile(r"(?<![\w./-])(?:[^\s\"'`=]*/)?(scripts/[\w./-]+\.sh)\b")
INTERPRETER = re.compile(r"(?:^|[\s;&|(`\"'])(?:bash|sh|source|\.)\s+[\"']?\S*$")
INLINE_COMMAND = re.compile(r"`((?:[^\s`]*/)?scripts/[\w./-]+\.sh)\s+[^`]+`")


def commands(path, text):
    """The parts of `text` that are commands."""
    if path.suffix != ".md":
        return [line for line in text.splitlines() if not line.lstrip().startswith("#")]
    lines, inside = [], False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            inside = not inside
        elif inside:
            if not line.lstrip().startswith("#"):
                lines.append(line)
        else:
            lines.extend(match.group(1) for match in INLINE_COMMAND.finditer(line))
    return lines


def run_by_path(path, text, tracked):
    """The tracked scripts `text` runs by their path."""
    found = set()
    for line in commands(path, text):
        for match in SCRIPT.finditer(line):
            name = match.group(1)
            if name in tracked and not INTERPRETER.search(line[: match.start()]):
                found.add(name)
    return found


def tracked_modes(root=PLATFORM_ROOT):
    """Every tracked `scripts/**/*.sh` with its git mode."""
    listed = subprocess.run(["git", "ls-files", "-s", "--", "scripts"], cwd=root,
                            capture_output=True, text=True, check=True).stdout
    modes = {}
    for line in listed.splitlines():
        meta, name = line.split("\t", 1)
        if name.endswith(".sh"):
            modes[name] = meta.split()[0]
    return modes


def not_executable(modes, sources):
    """`(script, source)` for every script a source runs by its path that git does not mark
    executable. `sources` is `[(path, text)]`."""
    refused = []
    for path, text in sources:
        for name in sorted(run_by_path(path, text, modes)):
            if modes[name] != "100755":
                refused.append((name, path.as_posix()))
    return refused


def real_sources(root=PLATFORM_ROOT):
    paths = sorted({path for pattern in SOURCES for path in root.glob(pattern) if path.is_file()})
    return [(path.relative_to(root), path.read_text(encoding="utf-8", errors="replace")) for path in paths]


class InvokedScriptsAreExecutable(unittest.TestCase):
    def test_every_script_run_by_its_path_is_executable_in_git(self):
        self.assertEqual(not_executable(tracked_modes(), real_sources()), [])

    def test_the_check_sees_the_ways_a_script_is_run(self):
        # Planted: the same non-executable script, named every way the platform names one.
        modes = {"scripts/ops/x.sh": "100644", "scripts/ops/y.sh": "100755"}
        run = [
            ("runbook.md", "```bash\nsudo systemd-run --wait \\\n  /opt/foundation-platform/current/scripts/ops/x.sh /data/out\n```\n"),
            ("runbook.md", "2. `scripts/ops/x.sh init-secrets` — 비밀값 파일을 만든다.\n"),
            ("foundation-x.service", "[Service]\nExecStart=/opt/foundation-platform/current/scripts/ops/x.sh all\n"),
            ("scripts/ops/z.sh", 'exec "${RELEASE_ROOT}/scripts/ops/x.sh" building\n'),
        ]
        for name, text in run:
            with self.subTest(name=name):
                self.assertEqual(not_executable(modes, [(pathlib.Path(name), text)]),
                                 [("scripts/ops/x.sh", name)])
        named = [
            ("runbook.md", "`scripts/ops/x.sh` 가 측정한다. 실행 스크립트 | `scripts/ops/x.sh` |\n"),
            ("runbook.md", "```bash\nbash scripts/ops/x.sh all\n. scripts/ops/x.sh\nsh ./scripts/ops/x.sh plan\n```\n"),
            ("scripts/ops/z.sh", "# scripts/ops/x.sh does the rest\nsource scripts/ops/x.sh --current\n"),
            ("runbook.md", "```bash\n/opt/foundation-platform/current/scripts/ops/y.sh\n```\n"),
        ]
        for name, text in named:
            with self.subTest(name=name, text=text):
                self.assertEqual(not_executable(modes, [(pathlib.Path(name), text)]), [])


if __name__ == "__main__":
    unittest.main()
