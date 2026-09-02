#!/usr/bin/env bash
# The deploy must ask compose for an end state, not walk the chain compose already declares.
#
# What real incident does failing this prevent? On 2026-09-02 the runtime database held all 33
# migrations and `GET /catalog/v1/complexes` answered 500 with `permission denied for schema
# catalog`. `foundation-release.sh migrate` had run `up --no-deps postgres` and then
# `run foundation-migrate` — the compose chain written out a second time and stopped two links
# early, so `foundation-runtime-grants` and `foundation-finalize` never ran. `/health` does not
# touch the database, so the stack looked up for a day.
#
# The compose file owns the order:
#   postgres → foundation-bootstrap → foundation-migrate → foundation-runtime-grants
#            → foundation-finalize → foundation-api
# Each link is a `service_completed_successfully`. Requiring the last one requires all of them.
#
# The property: the deploy may not name an intermediate service of that chain, and may not use
# `--no-deps`, which is the flag that says "ignore what compose knows".
set -uo pipefail

root="${1:-$(git rev-parse --show-toplevel)}"
release="${root}/platforms/foundation-platform/scripts/deploy/foundation-release.sh"
compose="${root}/platforms/foundation-platform/docker-compose.yml"
name="the-deploy-does-not-restate-the-compose-chain"
failed=0

for file in "${release}" "${compose}"; do
  [[ -f "${file}" ]] || {
    printf 'FAIL %s: missing %s\n' "${name}" "${file}" >&2
    exit 1
  }
done

# The chain is read from the compose file, never listed here. A link added there must be covered
# by this guard without anyone editing it — a hand-kept copy is the defect being guarded against.
terminal="foundation-api"
# `tr -d` because python3 on Windows ends its lines with CRLF: without it every name arrives as
# `foundation-migrate<CR>`, every comparison below misses, and the guard reports OK on a tree
# that violates it. That is what it did until a `bash -x` trace showed the trailing byte.
chain=()
while read -r line; do
  [ -n "${line}" ] && chain+=("${line}")
done < <(
  python3 - "${compose}" "${terminal}" <<'PY'
import re, sys

text = open(sys.argv[1], encoding="utf-8").read()
current, deps = None, {}
for line in text.split("\n"):
    service = re.match(r"^  ([a-z][a-z0-9-]*):\s*$", line)
    if service:
        current = service.group(1)
        deps.setdefault(current, [])
    dependency = re.match(r"^      ([a-z][a-z0-9-]*):\s*$", line)
    if dependency and current:
        deps[current].append(dependency.group(1))

# Everything the terminal service transitively waits for.
seen, stack = set(), [sys.argv[2]]
while stack:
    node = stack.pop()
    for parent in deps.get(node, []):
        if parent in deps and parent not in seen and parent != sys.argv[2]:
            seen.add(parent)
            stack.append(parent)
# `sys.stdout.write`, not `print`: python3 on Windows translates "\n" to CRLF, every name would
# arrive as `foundation-migrate<CR>`, and every comparison below would miss. The guard reported OK
# on a tree that violates it until a `bash -x` trace showed the trailing byte.
sys.stdout.reconfigure(newline="\n")
sys.stdout.write("\n".join(sorted(seen)) + "\n")
PY
)

[[ "${#chain[@]}" -ge 3 ]] || {
  printf 'FAIL %s: read only %s links out of the compose chain, which cannot be right\n' \
    "${name}" "${#chain[@]}" >&2
  exit 1
}

# Only the deploy's own commands, not the prose explaining why they are what they are.
commands="$(grep -vE '^\s*#' "${release}")"

if grep -q -- '--no-deps' <<<"${commands}"; then
  printf 'FAIL %s: the deploy passes --no-deps, which tells compose to ignore the order it owns\n' \
    "${name}" >&2
  failed=1
fi

for link in "${chain[@]}"; do
  # A Windows python3 writes CRLF, so each name arrives with a trailing carriage return and
  # every comparison below silently misses. The guard reported OK on a tree that violated
  # it until a `bash -x` trace showed the byte.
  # postgres is the chain's root and a deploy may legitimately address it on its own — a database
  # can be started without running the migration chain. The intermediate steps cannot.
  [[ "${link}" == "postgres" ]] && continue
  # A bare mention is enough. The comments are already stripped, so the name of an
  # intermediate link appearing in a command at all means the deploy is walking the
  # chain by hand — the invocation shape does not matter and trying to match it was
  # how the first two versions of this check let the violation through.
  if printf %s "${commands}" | grep -qF -- "${link}"; then
    printf 'FAIL %s: the deploy runs %s directly instead of requiring %s\n' \
      "${name}" "${link}" "${terminal}" >&2
    printf '  compose already orders it; naming it here is that order written twice\n' >&2
    failed=1
  fi
done

grep -qE "up[^|;&]*--wait[^|;&]*${terminal}|up[^|;&]*${terminal}[^|;&]*--wait" <<<"${commands}" || {
  printf 'FAIL %s: the deploy never waits for %s to be up\n' "${name}" "${terminal}" >&2
  printf '  without --wait the command returns before the chain has finished\n' >&2
  failed=1
}

if [[ "${failed}" -eq 0 ]]; then
  printf 'OK %s (chain=%s)\n' "${name}" "${#chain[@]}"
fi
exit "${failed}"
