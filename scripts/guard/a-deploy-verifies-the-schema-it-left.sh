#!/usr/bin/env bash
# Every install path must end by checking the running schema against the release it just activated.
#
# What real incident does failing this prevent? On 2026-09-01 the deployment host had four
# migrations applied while the release shipped thirty-three. The gap was six weeks old, every
# `catalog.*` table was empty, and `GET /catalog/v1/complexes` answered `[]`. Three deploys ran
# that day and all three reported success, because the release script installs a source tree and
# moves a symlink while the migrations are compiled into a container image — deploying source
# could never move them, and nothing compared the two.
#
# This guard is a property, not a spelling: the install branch must reach the runtime schema
# check, and the check must be shipped in the tree so the release carries it.
set -uo pipefail

root="${1:-$(git rev-parse --show-toplevel)}"
release_script="${root}/platforms/foundation-platform/scripts/deploy/foundation-release.sh"
checker="${root}/platforms/foundation-platform/scripts/deploy/assert-runtime-migrations.sh"
name="a-deploy-verifies-the-schema-it-left"
failed=0

report() {
  printf 'FAIL %s: %s\n' "${name}" "$1" >&2
  failed=1
}

[[ -f "${release_script}" ]] || { report "release script is missing: ${release_script}"; exit 1; }

if [[ ! -f "${checker}" ]]; then
  report "the runtime schema check is missing: ${checker}"
else
  # The check must fail when it cannot look. A check that treats an unreachable database as a
  # pass reports the drift it exists to find as absence of drift.
  #
  # Held as a property, not as a list of sentences. An earlier version of this guard quoted two
  # of the checker's messages verbatim; rewording either would have left the guard passing while
  # checking for text nobody writes any more. What matters is the shape: every refusal sits
  # before the comparison, so no unanswered question can reach it.
  comparison_line="$(grep -n 'comm -23' "${checker}" | head -1 | cut -d: -f1)"
  if [[ -z "${comparison_line}" ]]; then
    report "the check never compares what is shipped against what is applied"
  else
    before="$(awk -v stop="${comparison_line}" 'NR < stop' "${checker}")"

    # Every guard on the way to the comparison must refuse, and none may succeed. A `||` or a
    # `[[ ]] ||` that ends in `exit 0` is the exact shape of "could not look, reported fine".
    # Six, measured 2026-09-02: no migrations directory, no compose wrapper, no shipped
    # migrations, the wrapper refusing to run, no container, an empty ledger. A threshold below
    # the real count lets one be deleted unnoticed — which `-ge 4` did: deleting the
    # missing-container refusal left the guard reporting OK. Adding a refusal means raising this.
    refusals="$(grep -c 'fail "' <<<"${before}")"
    [[ "${refusals}" -ge 6 ]] \
      || report "only ${refusals} refusal(s) precede the comparison, and there were 6; one of the ways this check can fail to look is no longer covered"

    successes="$(grep -cE '\|\|[[:space:]]*exit 0|^[[:space:]]*exit 0' <<<"${before}")"
    [[ "${successes}" -eq 0 ]] \
      || report "${successes} path(s) exit successfully before the comparison; an unanswered question must never be a pass"

    after="$(awk -v start="${comparison_line}" 'NR > start' "${checker}" | grep -c 'fail "')"
    [[ "${after}" -eq 0 ]] \
      || report "${after} refusal(s) sit after the comparison; a question that goes unanswered must stop the check before it decides"
  fi
fi

# The install branch, not the file as a whole: a `verify` subcommand nobody calls is a check
# that never runs.
install_branch="$(awk '/^  install\)/{found=1} found{print} found && /;;/{exit}' "${release_script}")"
if [[ -z "${install_branch}" ]]; then
  report "the release script has no install branch to check"
elif ! grep -Fq 'verify_runtime_schema' <<<"${install_branch}"; then
  report "install does not verify the running schema against the release it activated"
fi

grep -Fq 'assert-runtime-migrations.sh' "${release_script}" \
  || report "the release script never reaches the runtime schema check"

# Applying is separate from checking, and the release has to be able to do both — otherwise the
# only answer to a red check is a manual repair nobody wrote down.
grep -Eq '^\s*migrate\)' "${release_script}" \
  || report "the release script offers no way to apply what the check found missing"

if [[ "${failed}" -eq 0 ]]; then
  printf 'OK %s\n' "${name}"
fi
exit "${failed}"
