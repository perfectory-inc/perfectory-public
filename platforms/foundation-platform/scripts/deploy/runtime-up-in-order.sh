#!/usr/bin/env bash
# Sourced by foundation-release.sh `migrate` (root ADR-0245); a test sources it against a real
# compose project.
#
#   runtime_chain_before <compose-wrapper> <serving service>
#
# Runs, to completion, the one-shots the serving service waits for (its `depends_on` entries with
# `condition: service_completed_successfully`), and fails if any of them did not exit 0 — while the
# serving service's running container is left alone. The caller replaces the serving service only
# after this returned 0.
#
# Why: asking compose for the API alone runs the chain too (postgres, bootstrap, migrate, grants,
# finalize, api), but compose replaces the API's container as soon as the release's image is new,
# before the chain runs. On 2026-10-14 the migration refused (an approval the operator had not
# recorded yet) and the new API container stayed Created for 30 minutes; the old one was already
# gone. Asking for the chain's last one-shots first runs every link in compose's order and touches
# nothing that depends on them.
#
# The one-shots are read from compose's own resolved configuration, never named here or by the
# caller: the order lives in the compose file (scripts/guard/the-deploy-does-not-restate-the-
# compose-chain.sh).
runtime_chain_before() {
  local runtime="$1" serving="$2" config oneshots code=0
  config="$("${runtime}" config --format json)"
  oneshots="$(python3 -c '
import json, sys
service = json.load(sys.stdin)["services"][sys.argv[1]]
print(" ".join(sorted(name for name, need in (service.get("depends_on") or {}).items()
                      if need.get("condition") == "service_completed_successfully")))
' "${serving}" <<<"${config}")"
  if [[ -z "${oneshots}" ]]; then
    printf 'runtime: %s waits for no one-shot; nothing to run before it\n' "${serving}" >&2
    return 0
  fi
  # shellcheck disable=SC2086 # one name per word, from the compose file
  "${runtime}" up -d ${oneshots} || code=$?
  if ((code == 0)); then
    # `docker wait` on the container, not `compose wait` on the service: compose answers "no
    # containers" for a one-shot that already exited (a fast finalize does, before the call), while
    # docker returns the exit code of a container that has exited as well as of one still running.
    local oneshot container status
    for oneshot in ${oneshots}; do
      container="$("${runtime}" ps -a -q "${oneshot}" | sed -n 1p)"
      [[ -n "${container}" ]] || { code=70; break; }
      status="$(docker wait "${container}")" || { code=70; break; }
      ((status == 0)) || { code="${status}"; break; }
    done
  fi
  if ((code != 0)); then
    printf 'runtime: the chain before %s failed (exit %s); %s was not replaced and keeps serving\n' \
      "${serving}" "${code}" "${serving}" >&2
    return 1
  fi
}
