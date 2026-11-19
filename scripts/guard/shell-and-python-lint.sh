#!/usr/bin/env bash
# ShellCheck and ruff over the shell and Python files a change touches, against the shrink-only
# baseline tools/ci/lint-baseline.json (scripts/ci/lint_changed.py). Prevents an exit code hidden
# in a command substitution or an unchecked subprocess.run, which AGENTS.md policed in prose and
# review missed (2026-10-29: a check reported success from piped output).
set -euo pipefail
cd "$(dirname "$0")/../.."
python3 scripts/ci/lint_changed.py check
