#!/usr/bin/env bash
# Part of foundation-release-test.sh (sourced by it, split out for its line limit): the generate
# order rehearsal. Needs the rehearsal's test root (with its built source-a) and repo root.
# Root ADR-0590: a release run on a host whose files lack a `generated` secret writes it before the
# first compose command. 2026-11-19 (ed0226b3) the generate step ran only inside verify_runtime_schema,
# at the end of `migrate`; compose interpolates the whole file on its first call, so the deploy stopped
# at "required variable ... is missing a value" before anything started. This rehearsal missed it
# because its fake host holds every name and its compose stand-in never interpolates. Here the real
# `migrate_runtime` and `generate_host_secrets` run in order against a host without the secret, with a
# compose stand-in that refuses a missing variable the way compose does; only what lies outside the
# order (admission, the console, the schema checks, the lakehouse) is stubbed.
rehearse_generate_order() {
  local test_root="$1" repo_root="$2"
  local order_root order_host order_user order_group path name
  order_root="${test_root}/generate-order"
  order_host="${order_root}/host"
  mkdir -p "${order_root}/opt" "${order_host}"
  cp -R "${test_root}/source-a" "${order_root}/opt/release"
  ln -s release "${order_root}/opt/current"
  # The contract's own groups, owned here by the rehearsal's user (the host's are root's).
  order_user="$(id -un)"
  order_group="$(id -gn)"
  python3 -I -B - "${order_root}/opt/release" "${order_host}" "${order_user}" "${order_group}" <<'PY'
import json, pathlib, sys
area, root, user, group_name = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3], sys.argv[4]
path = area / "config/runtime-secrets.contract.json"
raw = json.loads(path.read_text(encoding="utf-8"))
for entry in raw["groups"].values() if isinstance(raw["groups"], dict) else raw["groups"]:
    if entry.get("owner"):
        entry["owner"], entry["group"] = user, group_name
path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
sys.path.insert(0, str(area / "scripts/deploy"))
import runtime_secrets
contract = runtime_secrets.load(area)
generated = []
for group in contract.groups.values():
    if group.release or not group.owner:
        continue
    target = root / group.path.lstrip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("".join(f"{name}=rehearsal\n" for name in sorted(group.holds - group.generated)),
                      encoding="utf-8")
    target.chmod(group.mode)
    generated += [f"{group.path} {name}" for name in sorted(group.generated)]
if not generated:
    sys.exit("the contract generates no secret; this rehearsal has nothing to show")
(root / "generated.txt").write_text("".join(f"{line}\n" for line in generated), encoding="utf-8")
PY
  # compose: every call interpolates first, and a required name the host file lacks stops it.
  cat >"${order_root}/opt/release/scripts/deploy/foundation-runtime.sh" <<'RUNTIME'
#!/usr/bin/env bash
set -euo pipefail
while read -r path name; do
  grep -q "^${name}=." "${FOUNDATION_PLATFORM_SECRETS_ROOT}${path}" || {
    printf 'error while interpolating: required variable %s is missing a value\n' "${name}" >&2
    exit 1
  }
done <"${FOUNDATION_PLATFORM_SECRETS_ROOT}/generated.txt"
printf '%s\n' "$*" >>"${FOUNDATION_PLATFORM_SECRETS_ROOT}/compose-calls.txt"
if [[ "${1:-}" == config ]]; then printf '{"services":{"foundation-api":{}}}\n'; fi
RUNTIME
  chmod +x "${order_root}/opt/release/scripts/deploy/foundation-runtime.sh"
  cp "${repo_root}/scripts/deploy/runtime-up-in-order.sh" "${order_root}/opt/release/scripts/deploy/"
  # The one-off move of the data directory (root ADR-0598) lies outside the order: stubbed.
  printf 'move_postgres_data() { :; }\n' >"${order_root}/opt/release/scripts/deploy/postgres-data-move.sh"
  printf '#!/usr/bin/env bash\nexit 0\n' >"${order_root}/admission"
  chmod +x "${order_root}/admission"
  # Its own bash with its own errexit, as the deploy runs it: a function called under `if` runs
  # with errexit off, and a failing compose call inside it would then not stop it.
  cat >"${order_root}/run.sh" <<'RUN'
set -Eeuo pipefail
release_root="$1/opt"
admission="$1/admission"
export FOUNDATION_PLATFORM_SECRETS_ROOT="$2"
assert_current_release() { :; }
start_console() { :; }
verify_runtime_schema() { :; }
migrate_lakehouse() { :; }
# shellcheck disable=SC1090
source "$3"
migrate_runtime skip-unchanged
RUN
  order_rehearsal() {
    bash "${order_root}/run.sh" "${order_root}" "${order_host}" "$1"
  }
  sed -n -e '/^generate_host_secrets() {/,/^}/p' -e '/^migrate_runtime() {/,/^}/p' \
    "${repo_root}/scripts/deploy/foundation-release.sh" >"${order_root}/functions.sh"
  grep -q '^migrate_runtime() {' "${order_root}/functions.sh" \
    || { printf 'read no migrate_runtime out of foundation-release.sh\n' >&2; exit 1; }
  if ! order_rehearsal "${order_root}/functions.sh" >"${order_root}/run.log" 2>&1; then
    printf 'migrate on a host without its generated secret failed:\n' >&2
    cat "${order_root}/run.log" >&2
    exit 1
  fi
  while read -r path name; do
    grep -q "^${name}=[0-9a-f]\{64\}$" "${order_host}${path}" \
      || { printf 'migrate did not generate %s in %s\n' "${name}" "${path}" >&2; exit 1; }
  done <"${order_host}/generated.txt"
  [[ -s "${order_host}/compose-calls.txt" ]] || { printf 'migrate made no compose call\n' >&2; exit 1; }
  printf 'migrate-generates-host-secrets-before-compose=pass\n'
  # The order as it was (generate only after the first compose call): the same host must stop.
  python3 -I -B - "${order_host}" <<'PY'
import pathlib, sys
root = pathlib.Path(sys.argv[1])
for line in (root / "generated.txt").read_text(encoding="utf-8").splitlines():
    path, name = line.split()
    target = root / path.lstrip("/")
    target.write_text("".join(l + "\n" for l in target.read_text(encoding="utf-8").splitlines()
                              if not l.startswith(f"{name}=")), encoding="utf-8")
PY
  sed '/^migrate_runtime() {/,/^}/{/^  generate_host_secrets$/d}' "${order_root}/functions.sh" >"${order_root}/late.sh"
  if cmp -s "${order_root}/functions.sh" "${order_root}/late.sh"; then
    printf 'mutation late-generate changed nothing; migrate_runtime no longer calls generate_host_secrets on its own line\n' >&2
    exit 1
  fi
  if order_rehearsal "${order_root}/late.sh" >"${order_root}/late.log" 2>&1; then
    printf 'migrate passed with generate after the first compose call\n' >&2
    exit 1
  fi
  grep -q 'is missing a value' "${order_root}/late.log" \
    || { printf 'the late order failed for another reason:\n' >&2; cat "${order_root}/late.log" >&2; exit 1; }
  printf 'migrate-with-late-generate=refused-at-interpolation\n'
}
