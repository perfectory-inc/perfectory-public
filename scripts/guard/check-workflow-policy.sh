#!/usr/bin/env bash
# Fail-closed parser for the deliberately small GitHub Actions YAML dialect used
# by this repository. Ambiguous YAML shapes are rejected instead of guessed.
set -euo pipefail

workflow_dir="${1:-.github/workflows}"
ruleset="${2:-tools/github/main-ruleset.json}"
selected_actions="${3:-tools/github/selected-actions.json}"
self_test_mode="${4:-}"

for command_name in awk diff grep mktemp sed sort; do
  command -v "$command_name" >/dev/null || {
    echo "FAIL workflow-policy: missing command '$command_name'" >&2
    exit 1
  }
done
if [ ! -d "$workflow_dir" ] || [ ! -f "$ruleset" ] || [ ! -f "$selected_actions" ]; then
  echo "FAIL workflow-policy: workflow directory or policy file is missing" >&2
  exit 1
fi

# Use the purpose-built upstream parser for YAML/schema correctness; the
# canonical dialect below adds repository-specific least-privilege semantics.
# The negative-fixture harness parses its one valid fixture once, then exercises
# this semantic layer without launching actionlint dozens of times. That mode is
# admitted only for fixture/policy paths sharing one external temporary root.
if [ -n "$self_test_mode" ]; then
  [ "$self_test_mode" = --semantic-self-test ] || {
    echo "FAIL workflow-policy: unknown fourth argument" >&2
    exit 2
  }
  fixture_root="$(dirname "$(realpath "$ruleset")")"
  workflow_real="$(realpath "$workflow_dir")"
  actions_root="$(dirname "$(realpath "$selected_actions")")"
  repository_root="$(cd "$(dirname "$0")/../.." && pwd)"
  case "$workflow_real" in "$fixture_root"/*) ;; *) exit 2 ;; esac
  if [ "$actions_root" != "$fixture_root" ] || [ "$fixture_root" = "$repository_root" ]; then
    echo "FAIL workflow-policy: semantic self-test paths are not isolated fixtures" >&2
    exit 2
  fi
else
  bash "$(dirname "$0")/../ci/actionlint.sh" "$workflow_dir" >/dev/null
fi

shopt -s nullglob
workflows=("$workflow_dir"/*.yml "$workflow_dir"/*.yaml)
if [ "${#workflows[@]}" -eq 0 ]; then
  echo "FAIL workflow-policy: no workflow files found in $workflow_dir" >&2
  exit 1
fi

expected_contexts="$(mktemp)"
actual_contexts="$(mktemp)"
expected_actions="$(mktemp)"
actual_actions="$(mktemp)"
all_uses="$(mktemp)"
cleanup() {
  rm -f -- "$expected_contexts" "$actual_contexts" "$expected_actions" \
    "$actual_actions" "$all_uses"
}
trap cleanup EXIT

grep -oE '"context"[[:space:]]*:[[:space:]]*"required/[^"]+"' "$ruleset" \
  | sed -E 's/.*"(required\/[^"]+)"/\1/' \
  | sort >"$expected_contexts"

for workflow in "${workflows[@]}"; do
  if grep -n $'\t' "$workflow" >/dev/null; then
    echo "FAIL workflow-policy: tabs are forbidden in workflow YAML: $workflow" >&2
    exit 1
  fi

  # Flow maps, explicit/quoted keys, anchors/aliases, and job containers can
  # hide security-sensitive keys from a line-oriented canonical parser.
  noncanonical_yaml="$(grep -En \
    '^[[:space:]]*[?:]([[:space:]]|$)|^[[:space:]]*-[[:space:]]*[\[{]|^[[:space:]]*-[[:space:]]*if:|^[[:space:]]*["'\''][^"'\'']+["'\'']:[[:space:]]*|^[[:space:]]*[A-Za-z_][A-Za-z0-9_-]*:[[:space:]]*\{|^[[:space:]]*(on|jobs|permissions|steps|runs-on|uses|with|container|services):[[:space:]]*[\[{]|(^|[[:space:]])[&*][A-Za-z_][A-Za-z0-9_-]*([[:space:]]|$)|^[[:space:]]*<<:|^    container:' \
    "$workflow" || true)"
  if [ -n "$noncanonical_yaml" ]; then
    echo "FAIL workflow-policy: noncanonical security-relevant YAML in $workflow:" >&2
    printf '%s\n' "$noncanonical_yaml" >&2
    exit 1
  fi

  spaced_security_key="$(grep -En \
    '^[ ]{0,10}(-[ ]+)?[A-Za-z_][A-Za-z0-9_-]*[ ]+:' \
    "$workflow" || true)"
  if [ -n "$spaced_security_key" ]; then
    echo "FAIL workflow-policy: workflow mapping keys require canonical key: spacing in $workflow:" >&2
    printf '%s\n' "$spaced_security_key" >&2
    exit 1
  fi
  tagged_security_value="$(grep -En \
    '^[[:space:]]*(-[[:space:]]+)?(permissions|actions|attestations|checks|contents|deployments|discussions|id-token|issues|models|packages|pages|pull-requests|security-events|statuses|continue-on-error|if|uses|container|services|image):[[:space:]]*!' \
    "$workflow" || true)"
  if [ -n "$tagged_security_value" ]; then
    echo "FAIL workflow-policy: explicit YAML tags are forbidden on security values in $workflow:" >&2
    printf '%s\n' "$tagged_security_value" >&2
    exit 1
  fi

  # Permission maps are intentionally tiny: one top-level contents: read map,
  # plus optional job-level copies of that same least-privilege map.
  awk -v file="$workflow" '
    function indentation(line, copy) {
      copy=line; sub(/[^ ].*$/, "", copy); return length(copy)
    }
    function fail(message) {
      print "FAIL workflow-policy: " file ": " message > "/dev/stderr"; failed=1
    }
    function finish_permissions() {
      if (child_count != 1 || !has_contents_read) {
        fail("permissions must contain exactly contents: read")
      }
      in_permissions=0; base=-1; child_count=0; has_contents_read=0
    }
    {
      current=indentation($0)
      if (in_permissions && $0 !~ /^[[:space:]]*($|#)/ && current <= base) {
        finish_permissions()
      }
      if (in_permissions && $0 !~ /^[[:space:]]*($|#)/) {
        if (current != base + 2) {
          fail("permissions contains unsupported nesting")
        } else {
          child_count++
          if ($0 ~ /^[[:space:]]*contents:[[:space:]]*read[[:space:]]*$/) {
            if (has_contents_read++) fail("duplicate contents permission")
          } else {
            fail("permissions may grant only literal contents: read")
          }
        }
        next
      }
      if ($0 ~ /^permissions:[[:space:]]*$/ || $0 ~ /^    permissions:[[:space:]]*$/) {
        if ($0 ~ /^permissions:/ && top_permissions++) fail("must define one top-level permissions map")
        in_permissions=1; base=current; next
      }
      if ($0 ~ /^[[:space:]]*permissions[[:space:]]*:/) {
        fail("permissions uses unsupported indentation or inline value")
      }
    }
    END {
      if (in_permissions) finish_permissions()
      if (top_permissions != 1) fail("must define exactly one top-level permissions map")
      exit failed ? 1 : 0
    }
  ' "$workflow"

  # Defaults are useful for this monorepo's area-scoped workflows, but only the
  # literal defaults.run.working-directory form is admitted. Shell defaults,
  # expressions, extra children, and alternative indentation fail closed.
  awk -v file="$workflow" '
    function indentation(line, copy) {
      copy=line; sub(/[^ ].*$/, "", copy); return length(copy)
    }
    function fail(message) {
      print "FAIL workflow-policy: " file ": " message > "/dev/stderr"; failed=1
    }
    function finish_defaults() {
      if (state != 3) fail("defaults must contain exactly run.working-directory")
      state=0; base=-1
    }
    function allowed_directory(value) {
      return value == "products/gongzzang" \
        || value == "platforms/foundation-platform" \
        || value == "platforms/identity-platform" \
        || value == "platforms/intelligence-platform"
    }
    {
      current=indentation($0)
      if (state && $0 !~ /^[[:space:]]*($|#)/ && current <= base) finish_defaults()
      if (state == 1 && current == base + 2 && $0 ~ /^[[:space:]]*run:[[:space:]]*$/) {
        state=2; next
      }
      if (state == 2 && current == base + 4 && $0 ~ /^[[:space:]]*working-directory:[[:space:]]*/) {
        value=$0; sub(/^[[:space:]]*working-directory:[[:space:]]*/, "", value)
        sub(/[[:space:]]+$/, "", value)
        if (!allowed_directory(value)) fail("defaults working-directory is not an admitted literal area path")
        state=3; next
      }
      if (state && $0 !~ /^[[:space:]]*($|#)/) {
        if (current > base) { fail("unsupported defaults child"); next }
      }
      if ($0 ~ /^defaults:[[:space:]]*$/ || $0 ~ /^    defaults:[[:space:]]*$/) {
        if ($0 ~ /^defaults:/ && top_defaults++) fail("duplicate top-level defaults")
        state=1; base=current; next
      }
      if ($0 ~ /^[[:space:]]+defaults:/ && $0 !~ /^    defaults:[[:space:]]*$/) {
        fail("defaults uses unsupported indentation or inline value")
      }
    }
    END { if (state) finish_defaults(); exit failed ? 1 : 0 }
  ' "$workflow"

  if grep -En '^[[:space:]]*permissions:[[:space:]]*write-all([[:space:]]|$)|^[[:space:]]+(actions|attestations|checks|contents|deployments|discussions|id-token|issues|models|packages|pages|pull-requests|security-events|statuses):[[:space:]]*write([[:space:]]|$)' \
    "$workflow" >/dev/null; then
    echo "FAIL workflow-policy: public workflows must not grant write permissions: $workflow" >&2
    exit 1
  fi
  if grep -En '^[[:space:]]*(-[[:space:]]+)?continue-on-error:' "$workflow" >/dev/null; then
    echo "FAIL workflow-policy: required workflows must not continue on error: $workflow" >&2
    exit 1
  fi

  private_context_or_oidc_refs="$(awk '
    /\$\{\{/ { in_expression=1 }
    in_expression && /(^|[^[:alnum:]_])secrets([^[:alnum:]_]|$)/ { print NR ":" $0 }
    in_expression && /(^|[^[:alnum:]_])vars([^[:alnum:]_]|$)/ { print NR ":" $0 }
    /\}\}/ { in_expression=0 }
    /ACTIONS_ID_TOKEN/ { print NR ":" $0 }
  ' "$workflow")"
  if [ -n "$private_context_or_oidc_refs" ]; then
    echo "FAIL workflow-policy: public workflows must not reference secrets, variables, or OIDC token channels: $workflow" >&2
    exit 1
  fi

  # Every required workflow uses the same PR trigger shape. Push-only path
  # filters remain allowed for cost, but PR filters/types cannot suppress checks.
  awk -v file="$workflow" '
    function fail(message) {
      print "FAIL workflow-policy: " file ": " message > "/dev/stderr"
      failed=1
    }
    /^on:[[:space:]]*$/ { on_count++; next }
    /^  pull_request:[[:space:]]*$/ { pull_count++; in_pull=1; next }
    in_pull && /^  [A-Za-z0-9_-]+:/ { in_pull=0 }
    in_pull && /^    branches:[[:space:]]*\[main\][[:space:]]*$/ { branch_count++; next }
    in_pull && /^    [A-Za-z0-9_-]+:/ { fail("pull_request allows only branches: [main]") }
    END {
      if (on_count != 1) fail("must use one canonical block-form on key")
      if (pull_count != 1 || branch_count != 1) {
        fail("must run on every pull request targeting main")
      }
      exit failed ? 1 : 0
    }
  ' "$workflow"

  # Conditional steps are restricted to reviewed diagnostic/cleanup steps. A
  # gate step cannot be silently changed to `if: false` and still report green.
  awk -v file="$workflow" '
    function flush_step() {
      if (step_if == "") return
      allowed=(step_name == "Upload supply-chain artifacts" && step_if == "always()") \
        || (step_name == "Clean Compose resources" && step_if == "always()") \
        || (step_name == "Dump API log on failure" && step_if == "failure()") \
        || (step_name == "Upload Playwright report (on failure)" && step_if == "failure()")
      if (!allowed) {
        print "FAIL workflow-policy: " file ": conditional step is not allowlisted: " step_name " / " step_if > "/dev/stderr"
        failed=1
      }
      step_if=""
      step_name=""
    }
    /^    steps:[[:space:]]*$/ { in_steps=1; next }
    in_steps && /^    [A-Za-z0-9_-]+:/ { flush_step(); in_steps=0 }
    in_steps && /^      -([[:space:]]|$)/ { flush_step(); in_step=1 }
    in_steps && in_step && /^      - name:[[:space:]]*/ {
      step_name=$0; sub(/^      - name:[[:space:]]*/, "", step_name)
    }
    in_steps && in_step && /^        name:[[:space:]]*/ {
      step_name=$0; sub(/^        name:[[:space:]]*/, "", step_name)
    }
    in_steps && in_step && /^        if:[[:space:]]*/ {
      step_if=$0; sub(/^        if:[[:space:]]*/, "", step_if)
    }
    END { flush_step(); exit failed ? 1 : 0 }
  ' "$workflow"

  # Job names, dependencies, and the terminal result gate are parsed only from
  # canonical indentation. Multi-job workflows have one exact terminal step.
  awk -v file="$workflow" '
    function trim(value) {
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+$/, "", value)
      return value
    }
    function fail(message) {
      print "FAIL workflow-policy: " file ": " message > "/dev/stderr"
      failed=1
    }
    /^jobs:[[:space:]]*$/ { in_jobs=1; next }
    in_jobs && /^[^[:space:]]/ { in_jobs=0; in_steps=0; in_step=0 }
    in_jobs && /^  [A-Za-z0-9_-]+:[[:space:]]*$/ {
      job=$0
      sub(/^  /, "", job)
      sub(/:[[:space:]]*$/, "", job)
      if (seen_job[job]++) fail("duplicate job id " job)
      job_count++
      job_ids[job_count]=job
      in_steps=0
      in_step=0
      next
    }
    in_jobs && job != "" && /^    name:[[:space:]]*/ {
      value=$0; sub(/^    name:[[:space:]]*/, "", value); job_names[job]=trim(value); next
    }
    in_jobs && job != "" && /^    if:[[:space:]]*/ {
      value=$0; sub(/^    if:[[:space:]]*/, "", value); job_ifs[job]=trim(value); next
    }
    in_jobs && job != "" && /^    needs:[[:space:]]*/ {
      value=$0; sub(/^    needs:[[:space:]]*/, "", value); job_needs[job]=trim(value); next
    }
    in_jobs && job != "" && /^    runs-on:[[:space:]]*/ {
      value=$0; sub(/^    runs-on:[[:space:]]*/, "", value); job_runners[job]=trim(value); next
    }
    in_jobs && job != "" && /^    strategy:[[:space:]]*/ { job_strategy[job]=1 }
    in_jobs && job != "" && /^    env:[[:space:]]*$/ { job_env[job]=1 }
    in_jobs && job != "" && /^    steps:[[:space:]]*$/ { in_steps=1; in_step=0; next }
    in_steps && /^    [A-Za-z0-9_-]+:/ { in_steps=0; in_step=0 }
    in_steps && /^      -([[:space:]]|$)/ {
      in_step=1
      step_count[job]++
      if ($0 ~ /^      - [A-Za-z0-9_-]+:[[:space:]]*/) {
        key=$0; sub(/^      - /, "", key); sub(/:.*/, "", key)
        step_key_count[job SUBSEP key]++
        if (key == "if") step_if_count[job]++
        if (key == "uses") {
          value=$0; sub(/^      - uses:[[:space:]]*/, "", value)
          if (value ~ /^actions\/checkout@[0-9a-f]+([[:space:]]|$)/) checkout_uses[job]++
        }
        if (key == "with") checkout_with[job]++
      }
      if ($0 ~ /^      - run:[[:space:]]*/) {
        run_count[job]++
        value=$0; sub(/^      - run:[[:space:]]*/, "", value)
        if (value == "bash scripts/ci/require-successful-needs.sh") exact_terminal_run[job]++
      }
    }
    in_steps && in_step && /^        [A-Za-z0-9_-]+:[[:space:]]*/ {
      key=$0; sub(/^        /, "", key); sub(/:.*/, "", key)
      step_key_count[job SUBSEP key]++
      if (key == "if") step_if_count[job]++
      if (key == "with") checkout_with[job]++
      if (key == "working-directory") {
        value=$0; sub(/^        working-directory:[[:space:]]*/, "", value)
        terminal_working_directory[job]=trim(value)
      }
      if (key == "run") {
        run_count[job]++
        value=$0; sub(/^        run:[[:space:]]*/, "", value)
        if (value == "bash scripts/ci/require-successful-needs.sh") exact_terminal_run[job]++
      }
    }
    in_steps && in_step && /^          [A-Z][A-Z0-9_]*:[[:space:]]*/ {
      env_child_count[job]++
      mapping=$0
      if (mapping ~ /^          REQUIRED_RESULT_[A-Z0-9_]+:[[:space:]]*\$\{\{[[:space:]]*needs\.[A-Za-z0-9_-]+\.result[[:space:]]*\}\}[[:space:]]*$/) {
        env_name=mapping; sub(/^          /, "", env_name); sub(/:.*/, "", env_name)
        need_id=mapping
        sub(/^[^:]+:[[:space:]]*\$\{\{[[:space:]]*needs\./, "", need_id)
        sub(/\.result[[:space:]]*\}\}[[:space:]]*$/, "", need_id)
        if (result_env[job SUBSEP need_id] != "") fail("duplicate result mapping for " need_id)
        result_env[job SUBSEP need_id]=env_name
        result_mapping_count[job]++
      }
    }
    END {
      if (job_count == 0) fail("has no jobs")
      for (i=1; i<=job_count; i++) {
        candidate=job_ids[i]
        if (job_runners[candidate] != "ubuntu-24.04") {
          fail("job " candidate " must use literal runner ubuntu-24.04")
        }
        if (job_names[candidate] ~ /^required\//) {
          required_count++
          required_job=candidate
        }
      }
       if (required_count != 1) {
         fail("must define exactly one job-level required/* context")
       } else if (job_count == 1) {
         if (job_ifs[required_job] != "") fail("single required job must not be conditional")
       } else {
        if (job_ifs[required_job] != "${{ always() }}") fail("terminal job must use exact if: ${{ always() }}")
        if (job_strategy[required_job]) fail("terminal job must not use a matrix strategy")
        if (job_env[required_job]) fail("terminal job must not define job-level env")
        if (step_count[required_job] != 2 || run_count[required_job] != 1 \
          || exact_terminal_run[required_job] != 1 || checkout_uses[required_job] != 1 \
          || checkout_with[required_job] != 1) {
          fail("terminal job must have one checkout and one canonical result-check step")
        }
        if (step_if_count[required_job] != 0) fail("terminal result step must not be conditional")
        if (step_key_count[required_job SUBSEP "name"] != 1 \
          || step_key_count[required_job SUBSEP "env"] != 1 \
          || step_key_count[required_job SUBSEP "working-directory"] != 1 \
          || step_key_count[required_job SUBSEP "run"] != 1) {
          fail("terminal step must contain exactly name, env, working-directory, and run keys")
        }
        if (terminal_working_directory[required_job] != ".") {
          fail("terminal step must run from repository root")
        }
        for (composite in step_key_count) {
          split(composite, parts, SUBSEP)
          if (parts[1] == required_job \
            && parts[2] != "name" && parts[2] != "env" && parts[2] != "run" \
            && parts[2] != "working-directory" \
            && parts[2] != "uses" && parts[2] != "with") {
            fail("terminal step contains forbidden key " parts[2])
          }
        }

        needs=job_needs[required_job]
        if (needs !~ /^\[[A-Za-z0-9_, -]+\]$/) {
          fail("terminal needs must use the canonical inline list")
        } else {
          sub(/^\[/, "", needs); sub(/\]$/, "", needs)
          need_count=split(needs, raw_need, ",")
          for (i=1; i<=need_count; i++) {
            dependency=trim(raw_need[i])
            if (dependency == "" || dependency == required_job || need_seen[dependency]++) {
              fail("terminal needs contains an empty, self, or duplicate dependency")
            }
          }
          for (i=1; i<=job_count; i++) {
            dependency=job_ids[i]
            if (dependency == required_job) continue
            if (!need_seen[dependency]) fail("terminal job does not need " dependency)
            expected_env=toupper(dependency); gsub(/-/, "_", expected_env)
            expected_env="REQUIRED_RESULT_" expected_env
            if (result_env[required_job SUBSEP dependency] != expected_env) {
              fail("terminal result mapping drift for " dependency)
            }
          }
          for (dependency in need_seen) {
            if (!seen_job[dependency]) fail("terminal needs unknown job " dependency)
          }
          if (need_count != job_count - 1 \
            || result_mapping_count[required_job] != job_count - 1 \
            || env_child_count[required_job] != job_count - 1) {
            fail("terminal result mappings must cover every non-terminal job exactly once")
          }
        }
      }
      exit failed ? 1 : 0
    }
  ' "$workflow"

  # Only canonical job/step uses keys are accepted; named steps are included.
  unusual_uses="$(grep -En '^[[:space:]]*(-[[:space:]]*)?uses:' "$workflow" \
    | grep -Ev '^[0-9]+:(    uses:|      - uses:|        uses:)' || true)"
  if [ -n "$unusual_uses" ]; then
    echo "FAIL workflow-policy: noncanonical uses key in $workflow:" >&2
    printf '%s\n' "$unusual_uses" >&2
    exit 1
  fi
  awk -v file="$workflow" '
    /^(    uses:|      - uses:|        uses:)/ {
      action=$0
      sub(/^[[:space:]]*(-[[:space:]]*)?uses:[[:space:]]*/, "", action)
      sub(/[[:space:]]+#.*$/, "", action)
      sub(/[[:space:]]+$/, "", action)
      print action "\t" file ":" NR
    }
  ' "$workflow" >>"$all_uses"

  # Checkout credentials are disabled only when the value is under the same
  # step's exact `with:` block; an env key with the same spelling is rejected.
  unsafe_checkout="$(awk -v file="$workflow" '
    function flush_step() {
      if (checkout_line && (with_count != 1 || persist_count != 1)) {
        print file ":" checkout_line ": checkout requires one with.persist-credentials=false"
      }
      checkout_line=0; with_count=0; persist_count=0; in_with=0
    }
    /^    steps:[[:space:]]*$/ { in_steps=1; next }
    in_steps && /^    [A-Za-z0-9_-]+:/ { flush_step(); in_steps=0 }
    in_steps && /^      -([[:space:]]|$)/ { flush_step(); in_step=1 }
    in_steps && in_step && /^(      - uses:|        uses:)[[:space:]]*actions\/checkout@/ { checkout_line=NR }
    in_steps && in_step && /^        with:[[:space:]]*$/ { with_count++; in_with=1; next }
    in_steps && in_step && /^        [A-Za-z0-9_-]+:/ { in_with=0 }
    in_steps && in_step && in_with && /^          persist-credentials:[[:space:]]*false[[:space:]]*$/ { persist_count++ }
    END { flush_step() }
  ' "$workflow")"
  if [ -n "$unsafe_checkout" ]; then
    printf 'FAIL workflow-policy: %s\n' "$unsafe_checkout" >&2
    exit 1
  fi

  while IFS= read -r image_line; do
    [ -n "$image_line" ] || continue
    image_ref="$(printf '%s\n' "$image_line" | sed -E 's/^[^:]+:[[:space:]]*//; s/[[:space:]#].*$//')"
    case "$image_ref" in
      *:local) ;;
      *)
        if ! printf '%s\n' "$image_ref" | grep -Eq '^[a-z0-9][a-z0-9./_-]*:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$'; then
          echo "FAIL workflow-policy: mutable workflow service image: $workflow: $image_ref" >&2
          exit 1
        fi
        ;;
    esac
  done < <(grep -E '^[[:space:]]+image:[[:space:]]*' "$workflow" || true)
done

sort -o "$actual_contexts" "$actual_contexts"
# Derive contexts strictly from four-space job names, never from step names.
for workflow in "${workflows[@]}"; do
  awk '
    /^jobs:[[:space:]]*$/ { in_jobs=1; next }
    in_jobs && /^[^[:space:]]/ { in_jobs=0 }
    in_jobs && /^  [A-Za-z0-9_-]+:[[:space:]]*$/ { in_job=1; next }
    in_jobs && in_job && /^    name:[[:space:]]*required\// {
      value=$0; sub(/^    name:[[:space:]]*/, "", value); sub(/[[:space:]]+$/, "", value); print value
    }
  ' "$workflow"
done | sort >"$actual_contexts"

if ! diff -u "$expected_contexts" "$actual_contexts"; then
  echo "FAIL workflow-policy: ruleset contexts and job-level workflow contexts drifted" >&2
  exit 1
fi
if [ ! -s "$expected_contexts" ] \
  || [ "$(uniq -d "$expected_contexts" | wc -l | tr -d ' ')" -ne 0 ]; then
  echo "FAIL workflow-policy: required contexts must be non-empty and unique" >&2
  exit 1
fi

grep -oE '"[[:alnum:]_.-]+/[[:alnum:]_.-]+@[0-9a-f]{40}"' "$selected_actions" \
  | tr -d '"' | sort -u >"$expected_actions"

while IFS=$'\t' read -r action location; do
  [ -n "$action" ] || {
    echo "FAIL workflow-policy: empty uses value at $location" >&2
    exit 1
  }
  case "$action" in
    ./*)
      echo "FAIL workflow-policy: local Actions are not admitted by the audited allowlist: $location" >&2
      exit 1
      ;;
    docker://*)
      if ! printf '%s\n' "$action" | grep -Eq '^docker://[^[:space:]@]+(:[^[:space:]@]+)?@sha256:[0-9a-f]{64}$'; then
        echo "FAIL workflow-policy: mutable Docker action at $location: $action" >&2
        exit 1
      fi
      continue
      ;;
  esac
  if ! printf '%s\n' "$action" | grep -Eq '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.\/-]+@[0-9a-f]{40}$'; then
    echo "FAIL workflow-policy: external Action must use a full commit SHA at $location: $action" >&2
    exit 1
  fi
  case "$action" in
    actions/*|github/*) ;;
    *) printf '%s\n' "$action" >>"$actual_actions" ;;
  esac
done <"$all_uses"

sort -u -o "$actual_actions" "$actual_actions"
if ! diff -u "$expected_actions" "$actual_actions"; then
  echo "FAIL workflow-policy: selected third-party Action policy and workflow references drifted" >&2
  exit 1
fi

echo "OK workflow-policy"
