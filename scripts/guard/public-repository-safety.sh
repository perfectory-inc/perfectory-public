#!/usr/bin/env bash
# Prevents a public repository from silently regaining private evidence, ambiguous
# licensing, mutable CI dependencies, or a path from untrusted PR code to a
# self-hosted runner. This checks the tracked tree; gitleaks owns content/history.
set -euo pipefail
cd "$(dirname "$0")/../.."

rc=0
fail() {
  echo "FAIL public-repository-safety: $*" >&2
  rc=1
}

tracked="$(git ls-files)"

forbidden_paths="$(printf '%s\n' "$tracked" | grep -E \
  '^\.github/CODEOWNERS$|(^|/)docs/(archive|review|superpowers|plans|specs|research|migration)(/|$)|(^|/)docs/([^/]+/)*handoff(/|$)|^products/gongzzang/(MEMORY\.md|memory/)|^products/gongzzang/docs/(KNOWN-ISSUES|followups)\.md$|^products/gongzzang/docs/sp9(/|$)|(^|/)(target|node_modules|\.pnpm-store|__pycache__|\.next|dist|coverage|\.turbo|\.cache|\.playwright-mcp)(/|$)|(^|/)platforms/foundation-platform/docs/runbooks/public-data-bronze-current-status\.md$|^platforms/foundation-platform/docs/catalog/building-register-unit-normalization-second-pass-evidence\.md$|^platforms/foundation-platform/docs/catalog/vworld/(2d-data-api|bulk-download|display-only|national-priority-catalog|ned-operations|wfs-layers)\.md$|^platforms/foundation-platform/docs/canonical-property-data-platform-pipeline-guide\.md$' \
  || true)"
if [ -n "$forbidden_paths" ]; then
  fail "private evidence or generated-output paths are tracked:\n$forbidden_paths"
fi

forbidden_files="$(printf '%s\n' "$tracked" | grep -Ei \
  '(^|/)([^/]+\.)?(pem|key|p12|pfx|jks|keystore|sqlite|db|dump|bak|tar|tgz|zip|7z|exe|dll|dylib|so|o|obj|a|lib|rlib|rmeta|py[cod]|class|wasm)$' \
  || true)"
if [ -n "$forbidden_files" ]; then
  fail "secret, dump, archive, or native build artifacts are tracked:\n$forbidden_files"
fi

while IFS= read -r path; do
  case "$path" in
    *.env.example|*.env.local.example) ;;
    .env|*/.env|.env.*|*/.env.*) fail "real environment file is tracked: $path" ;;
  esac
done <<< "$tracked"

unsafe_links="$(git ls-files --stage \
  | awk '$1 == "120000" || $1 == "160000" { print $1 " " $4 }')"
if [ -n "$unsafe_links" ]; then
  fail "tracked symlinks or gitlinks/submodules are not allowed in the public source snapshot:\n$unsafe_links"
fi

# Windows worktrees commonly use core.filemode=false, so test -x cannot prove
# what a Linux public clone will receive. Inspect staged blob modes directly and
# keep every repository script with a shebang executable in the Git tree.
non_executable_shebang_scripts="$(
  while IFS= read -r -d '' index_entry; do
    metadata="${index_entry%%$'\t'*}"
    script_path="${index_entry#*$'\t'}"
    mode="${metadata%% *}"
    case "$mode" in
      100644|100755) ;;
      *) continue ;;
    esac
    first_line="$(git show ":$script_path" | sed -n '1p')"
    if [[ "$first_line" == '#!'* ]] && [ "$mode" != 100755 ]; then
      printf '%s %s\n' "$mode" "$script_path"
    fi
  done < <(git ls-files --stage -z -- scripts)
)"
if [ -n "$non_executable_shebang_scripts" ]; then
  fail "tracked shebang scripts must be executable in the Git index:\n$non_executable_shebang_scripts"
fi

bash scripts/guard/check-tracked-blob-sizes.sh || rc=1

account_bindings="$(git grep -n -I -i -E \
  'R2_ACCOUNT_ID[[:space:]]*=[[:space:]]*[0-9a-f]{32}|https://[0-9a-f]{32}\.r2\.cloudflarestorage\.com|https://pub-[0-9a-f]+\.r2\.dev' \
  -- . 2>/dev/null || true)"
if [ -n "$account_bindings" ]; then
  fail "account-specific Cloudflare/R2 bindings are present:\n$account_bindings"
fi

workstation_bindings="$(git grep -n -I -i -E \
  '[A-Za-z]:[\\/]+Users[\\/]|file:///[A-Za-z]:/Users/' \
  -- . ':!scripts/guard/public-repository-safety.sh' 2>/dev/null || true)"
if [ -n "$workstation_bindings" ]; then
  fail "personal workstation or private sibling-repository references are present:\n$workstation_bindings"
fi

# Repository-local agent auto-approval settings turn untrusted public content
# into workstation authority. Durable skills/instructions may be versioned;
# command permissions belong only in each maintainer's private user config.
agent_permission_settings="$(printf '%s\n' "$tracked" | grep -E \
  '(^|/)\.(claude|codex|agents)/(settings[^/]*\.(json|toml|ya?ml)|config\.(json|toml|ya?ml))$' || true)"
if [ -n "$agent_permission_settings" ]; then
  fail "tracked agent command-permission settings are forbidden; move them to private user-level config:\n$agent_permission_settings"
fi

external_sibling_bindings="$(git grep -n -I -E \
  '(^|[^.[:alnum:]_/-])(\.\./)+[[:alnum:]_][[:alnum:]_.-]*/(AGENTS\.md|Cargo\.toml|package\.json|docs/|crates/|services/|apps/)' \
  -- . ':!scripts/guard/public-repository-safety.sh' \
       ':!scripts/guard/no-stale-sibling-paths.sh' 2>/dev/null || true)"
if [ -n "$external_sibling_bindings" ]; then
  fail "references to a pre-merge sibling repository are present; use a monorepo-root path:\n$external_sibling_bindings"
fi

private_network_bindings="$(git grep -n -I -E \
  '(^|[^0-9])(10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|192\.168\.[0-9]{1,3}\.[0-9]{1,3}|172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3})([^0-9]|$)' \
  -- . 2>/dev/null || true)"
if [ -n "$private_network_bindings" ]; then
  fail "private-network host bindings are present; use RFC 5737 examples or runtime configuration:\n$private_network_bindings"
fi

private_history_refs="$(git grep -n -I -E \
  '(commit|commits|Commit|커밋)[^[:cntrl:]]{0,80}[0-9a-f]{7,40}' \
  -- '*.md' 2>/dev/null || true)"
if [ -n "$private_history_refs" ]; then
  fail "private-history commit identifiers are present in public documentation:\n$private_history_refs"
fi

# Provider captures are private operational evidence. Public Rust/Python fixtures
# may retain the provider's wire shape only inside the reserved synthetic range.
provider_fixture_ids="$(git grep -n -I -E '[0-9]{8}DS[0-9]{5}' \
  -- '*.rs' '*.py' 2>/dev/null | grep -Ev '20991231DS9999[0-9]' || true)"
if [ -n "$provider_fixture_ids" ]; then
  fail "live-looking provider dataset IDs are present in source/test fixtures; use the 20991231DS9999x synthetic range:\n$provider_fixture_ids"
fi

provider_fixture_paths="$(git grep -n -I -E \
  "AddUploadedFile\\([^[:cntrl:]]*'/[^']+'" \
  -- '*.rs' '*.py' 2>/dev/null \
  | grep -i 'vworld' \
  | grep -Fv 'synthetic-fixture' || true)"
if [ -n "$provider_fixture_paths" ]; then
  fail "captured provider storage paths are present in source/test fixtures; use a synthetic-fixture path:\n$provider_fixture_paths"
fi

provider_fixture_sizes="$(git grep -n -I -i -E \
  '(size(_bytes|_kib)?|content[_-]?length|bytes)[^[:cntrl:]]{0,40}([0-9]{7,}|[0-9]{1,3}(_[0-9]{3}){2,})' \
  -- '*.rs' '*.py' 2>/dev/null | grep -i 'vworld' || true)"
if [ -n "$provider_fixture_sizes" ]; then
  fail "captured-scale provider object sizes are present in source/test fixtures; use a small synthetic size:\n$provider_fixture_sizes"
fi

provider_fixture_names="$(git grep -n -I -E \
  '[A-Z][A-Z0-9]*(_[A-Z0-9]+){2,}\.zip' \
  -- '*.rs' '*.py' 2>/dev/null \
  | grep -i 'vworld' \
  | grep -Fv 'SYNTHETIC' || true)"
if [ -n "$provider_fixture_names" ]; then
  fail "captured provider filenames are present in source/test fixtures; use an explicitly SYNTHETIC filename:\n$provider_fixture_names"
fi

provider_composite_ids="$(
  {
    git grep -n -I -E \
      '([0-9]{8}DS[0-9]{5}-[0-9]+|provider_file_id[=:][^[:space:]\"]*[0-9]{5}-[0-9]+|source=vworldkr__[A-Za-z0-9_/-]+/[0-9]{5}-[0-9]+\.zip)' \
      -- '*.rs' '*.py' 2>/dev/null || true
    git grep -n -I -E '[0-9]{5}-[0-9]+' \
      -- '*vworld_dataset*.rs' '*vworld_dataset*.py' 2>/dev/null || true
  } | sort -u | grep -Ev '20991231DS9999[0-9]-[0-9]+' || true
)"
if [ -n "$provider_composite_ids" ]; then
  fail "provider file composite IDs in VWorld fixtures must use the reserved synthetic DS range:\n$provider_composite_ids"
fi

opn_fixture_ids="$(git grep -n -I -o -E 'OPN20[0-9A-Za-z_-]*' \
  -- . ':!scripts/guard/public-repository-safety.sh' 2>/dev/null \
  | grep -Ev ':OPN2099[0-9A-Za-z_-]*$' || true)"
if [ -n "$opn_fixture_ids" ]; then
  fail "live-looking provider file IDs are present; use the reserved OPN2099 synthetic range:\n$opn_fixture_ids"
fi

raw_floor_building_ids="$(git grep -n -I -E '\"[0-9]{5,}-[0-9]{5,}\"' -- \
  'platforms/foundation-platform/crates/lakehouse/lakehouse-application/tests/building_register_floor_silver_rows.rs' \
  'platforms/foundation-platform/services/foundation-outbox-publisher/src/building_register_floor_silver_export.rs' \
  2>/dev/null | grep -Fv 'OPN2099' || true)"
if [ -n "$raw_floor_building_ids" ]; then
  fail "numeric building identities are present in public floor fixtures; use an explicit SYNTHETIC identity:\n$raw_floor_building_ids"
fi

proposal_fixture_ids="$(git grep -n -I -E \
  'building_mgm_bldrgst_pk[^[:cntrl:]]*:[[:space:]]*\"[0-9]+|line-[1-9][0-9]{3,}' \
  -- 'platforms/foundation-platform/services/foundation-api/src/routes/tests.rs' \
  2>/dev/null || true)"
if [ -n "$proposal_fixture_ids" ]; then
  fail "live-looking proposal identities are present in public API fixtures; use explicit synthetic values:\n$proposal_fixture_ids"
fi

production_fixture_defaults="$(git grep -n -I -F 'const DEFAULT_VALID_FROM_UTC' \
  -- '*.rs' 2>/dev/null || true)"
if [ -n "$production_fixture_defaults" ]; then
  fail "a production fixture-time default is compiled into Rust; require an explicit runtime value:\n$production_fixture_defaults"
fi

preview_fixture='products/gongzzang/apps/web/app/(public)/preview-floors/preview-data.ts'
if [ -f "$preview_fixture" ]; then
  grep -q 'GONGZZANG_PREVIEW_FLOORS_PARCELS_JSON' "$preview_fixture" \
    || fail "$preview_fixture must load parcel fixtures through the environment-owned JSON contract"
  preview_numeric_ids="$(grep -En '(^|[^0-9])[0-9]{19}([^0-9]|$)' "$preview_fixture" || true)"
  if [ -n "$preview_numeric_ids" ]; then
    fail "$preview_fixture contains a live-looking 19-digit parcel identifier:\n$preview_numeric_ids"
  fi
fi

bash scripts/guard/public-fixture-safety.sh || rc=1
bash scripts/guard/public-doc-boundary.sh || rc=1

operational_doc_evidence="$(git grep -n -I -E \
  '현재 구현 상태|현행화 노트|State at the [0-9]{4}|Implementation Status|live[[:space:]]+(검증|evidence|result)|실제[^[:cntrl:]]{0,40}(smoke|검증)|\|[^|]*(inventory|job)[^|]*\|[[:space:]]*[0-9][0-9,]*[[:space:]]*\||[0-9][0-9,]*(\.[0-9]+)?[[:space:]]*(GB|GiB|MB|MiB)[^[:cntrl:]]{0,40}(confirmed|verified|확인|성공)|[0-9]+[[:space:]]*(건|개|files?)[^[:cntrl:]]{0,40}(변환|확인|passed|failed)' \
  -- 'platforms/foundation-platform/docs/catalog/vworld/*.md' \
     'platforms/foundation-platform/docs/canonical-property-data-platform-northstar.md' \
     'platforms/foundation-platform/docs/adr/0025-bronze-catalog-recovery-evidence-sealing.md' \
     'platforms/foundation-platform/docs/catalog/building-register-floor-normalization-rules.v1.md' \
     'platforms/foundation-platform/docs/architecture/ai-driven-maintenance-model.md' \
     'platforms/intelligence-platform/docs/architecture.md' \
     'products/gongzzang/docs/adr/0022-bronze-scraping-isolated-python-service.md' \
     'products/gongzzang/docs/adr/0044-bazel-transition-reconciliation.md' \
     'platforms/foundation-platform/docs/runbooks/building-hub-bulk-bronze-ingest.md' \
  2>/dev/null || true)"
if [ -n "$operational_doc_evidence" ]; then
  fail "private operational measurements or status snapshots are present in public architecture/catalog docs:\n$operational_doc_evidence"
fi

personal_contacts="$(git grep -n -I -i -E \
  '@(gmail\.com|naver\.com|daum\.net|hanmail\.net|hotmail\.com|outlook\.com)|[0-9]+\+[A-Za-z0-9-]+@users\.noreply\.github\.com' \
  -- . 2>/dev/null || true)"
if [ -n "$personal_contacts" ]; then
  fail "consumer email addresses are present; use an approved role address or an .invalid placeholder:\n$personal_contacts"
fi

owned_storage_hosts="$(git grep -n -I -i -E \
  '([a-z0-9-]+\.)*(r2|tiles)\.gongzzang\.(com|dev|net|kr)' \
  -- . 2>/dev/null || true)"
if [ -n "$owned_storage_hosts" ]; then
  fail "account-bound storage/CDN hostnames are present; use a placeholder and bind them privately at deploy time:\n$owned_storage_hosts"
fi

for required in \
  .gitattributes \
  LICENSE \
  LICENSES/LicenseRef-Proprietary.txt \
  LICENSES/OFL-1.1.txt \
  REUSE.toml \
  THIRD_PARTY_NOTICES.md \
  tools/github/third-party-artifact-policy.json \
  CONTRIBUTING.md \
  tools/container-images.env \
  products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt \
  products/gongzzang/apps/web/public/fonts/pretendard-v1.3.9.sha256; do
  [ -f "$required" ] || fail "required licensing file is missing: $required"
done

attribute_test_root="$(mktemp -d)"
cp -- .gitattributes "$attribute_test_root/.gitattributes"
git init -q --initial-branch=main "$attribute_test_root"
for exact_hashed_text in \
  .gitattributes \
  THIRD_PARTY_NOTICES.md \
  LICENSES/OFL-1.1.txt \
  products/gongzzang/apps/web/public/fonts/LICENSE-PRETENDARD.txt \
  products/gongzzang/apps/web/public/fonts/pretendardvariable-dynamic-subset.css \
  products/gongzzang/apps/web/public/fonts/pretendard-v1.3.9.sha256; do
  attributes="$(git -C "$attribute_test_root" check-attr text eol -- "$exact_hashed_text")"
  if ! printf '%s\n' "$attributes" \
    | grep -Fq "$exact_hashed_text: text: auto" \
    || ! printf '%s\n' "$attributes" \
      | grep -Fq "$exact_hashed_text: eol: lf"; then
    fail "exact-hashed legal artifact lacks LF checkout attributes: $exact_hashed_text"
  fi
done
for windows_script in \
  fixture.cmd FIXTURE.CMD fixture.bat FIXTURE.BAT \
  products/gongzzang/fixture.cmd products/gongzzang/FIXTURE.CMD \
  products/gongzzang/fixture.bat products/gongzzang/FIXTURE.BAT; do
  attributes="$(git -C "$attribute_test_root" check-attr text eol -- "$windows_script")"
  if ! printf '%s\n' "$attributes" \
    | grep -Fq "$windows_script: text: set" \
    || ! printf '%s\n' "$attributes" \
      | grep -Fq "$windows_script: eol: crlf"; then
    fail "Windows script checkout attributes are not CRLF: $windows_script"
  fi
done
woff2_probe="products/gongzzang/apps/web/public/fonts/attribute-probe.woff2"
woff2_attributes="$(git -C "$attribute_test_root" check-attr text -- "$woff2_probe")"
if ! printf '%s\n' "$woff2_attributes" \
  | grep -Fq "$woff2_probe: text: unset"; then
  fail "WOFF2 checkout attributes are not binary"
fi
rm -rf -- "$attribute_test_root"

while IFS= read -r image_ref; do
  [ -n "$image_ref" ] || continue
  case "$image_ref" in \#*) continue ;; esac
  if ! printf '%s\n' "$image_ref" | grep -Eq \
    '^[A-Z][A-Z0-9_]*=[a-z0-9./_-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$'; then
    fail "tools/container-images.env contains a mutable or malformed image reference: ${image_ref%%=*}"
  fi
done < tools/container-images.env

for required_image in RUST_TOOLCHAIN_IMAGE NODE_VERIFY_IMAGE LYCHEE_IMAGE REUSE_IMAGE; do
  image_definition_count="$(grep -Ec "^${required_image}=" tools/container-images.env || true)"
  if [ "$image_definition_count" -ne 1 ]; then
    fail "tools/container-images.env must define ${required_image} exactly once"
  fi
done

python3 scripts/guard/check-package-publication-policy.py . || rc=1

workflow_files="$(printf '%s\n' "$tracked" | grep -E '^\.github/workflows/[^/]+\.ya?ml$' || true)"

# A backticked workflow path in maintained documentation is presented as an
# executable repository contract. Keep those references mechanically tied to
# the tracked tree. A deliberately prospective reference must opt out on the
# same line with the explicit marker below so review can distinguish a future
# design from stale instructions.
while IFS= read -r workflow_reference_line; do
  [ -n "$workflow_reference_line" ] || continue
  if printf '%s\n' "$workflow_reference_line" \
    | grep -Fq '<!-- public-repository-safety: allow-future-workflow -->'; then
    continue
  fi

  while IFS= read -r workflow_reference; do
    [ -n "$workflow_reference" ] || continue
    workflow_reference_found=0
    while IFS= read -r workflow_candidate; do
      [ -n "$workflow_candidate" ] || continue
      case "$workflow_candidate" in
        $workflow_reference)
          workflow_reference_found=1
          break
          ;;
      esac
    done <<< "$workflow_files"
    [ "$workflow_reference_found" -eq 1 ] \
      || fail "documentation references no tracked workflow: $workflow_reference_line"
  done < <(
    printf '%s\n' "$workflow_reference_line" \
      | grep -oE '`\.github/workflows/[^`[:space:]]+\.ya?ml`' \
      | sed -E 's/^`//; s/`$//'
  )
done < <(
  git grep -n -I -E '`\.github/workflows/[^`[:space:]]+\.ya?ml`' \
    -- '*.md' 2>/dev/null || true
)

bash scripts/guard/check-workflow-policy.sh \
  .github/workflows tools/github/main-ruleset.json tools/github/selected-actions.json \
  || rc=1
forbidden_tool_actions="$(while IFS= read -r workflow; do
  [ -n "$workflow" ] || continue
  grep -niE "uses:[[:space:]]*['\"]?(lycheeverse/lychee-action|fsfe/reuse-action)@" "$workflow" \
    | sed "s#^#${workflow}:#" || true
done <<< "$workflow_files")"
if [ -n "$forbidden_tool_actions" ]; then
  fail "verification tools must run through repository-owned scripts and digest-pinned container-image SSOT, not wrapper Actions:\n$forbidden_tool_actions"
fi

while IFS= read -r workflow; do
  [ -n "$workflow" ] || continue
  grep -q '^permissions:$' "$workflow" \
    || fail "$workflow has no explicit top-level permissions"
  grep -Eq '^  contents:[[:space:]]*read([[:space:]]|$)' "$workflow" \
    || fail "$workflow must default GITHUB_TOKEN contents permission to read"
  grep -q '^concurrency:$' "$workflow" \
    || fail "$workflow must cancel superseded runs through top-level concurrency"
  grep -Eq '^  group:[[:space:]]*[^[:space:]]' "$workflow" \
    || fail "$workflow concurrency group is missing"
  grep -Eq '^  cancel-in-progress:[[:space:]]*true([[:space:]]|$)' "$workflow" \
    || fail "$workflow must set concurrency.cancel-in-progress=true"

  missing_timeouts="$(awk -v file="$workflow" '
    function flush_job() {
      if (job != "" && !has_timeout) print file ": job " job " has no timeout-minutes"
    }
    /^jobs:[[:space:]]*$/ { in_jobs=1; next }
    in_jobs && /^[^[:space:]]/ { flush_job(); in_jobs=0; job="" }
    in_jobs && /^  [A-Za-z0-9_-]+:[[:space:]]*$/ {
      flush_job()
      job=$1
      sub(/:$/, "", job)
      has_timeout=0
      next
    }
    in_jobs && job != "" && /^    timeout-minutes:[[:space:]]*[0-9]+/ { has_timeout=1 }
    END { if (in_jobs) flush_job() }
  ' "$workflow")"
  if [ -n "$missing_timeouts" ]; then
    fail "$missing_timeouts"
  fi

  expression_in_run="$(awk -v file="$workflow" '
    function indentation(line, copy) {
      copy=line
      sub(/[^ ].*$/, "", copy)
      return length(copy)
    }
    {
      current=indentation($0)
      if (in_run && $0 !~ /^[[:space:]]*$/ && current <= run_indent) in_run=0
      if (in_run && /\$\{\{/) print file ":" NR ":" $0
      if ($0 ~ /^[[:space:]]+run:[[:space:]]*/) {
        run_indent=current
        in_run=1
        value=$0
        sub(/^[[:space:]]+run:[[:space:]]*/, "", value)
        if (value ~ /\$\{\{/) print file ":" NR ":" $0
      }
    }
  ' "$workflow")"
  if [ -n "$expression_in_run" ]; then
    fail "$workflow interpolates a GitHub expression directly into shell code; pass it through env instead:\n$expression_in_run"
  fi

  while IFS= read -r image_line; do
    [ -n "$image_line" ] || continue
    image_ref="$(printf '%s' "$image_line" | sed -E 's/^[^:]+:[[:space:]]*//; s/[[:space:]#].*$//')"
    digest="${image_ref##*@sha256:}"
    if [ "$digest" = "$image_ref" ] || [ "${#digest}" -ne 64 ] || printf '%s' "$digest" | grep -Eq '[^0-9a-f]'; then
      fail "$workflow has a service image without an immutable sha256 digest: $image_ref"
    fi
  done < <(grep -E '^[[:space:]]+image:[[:space:]]*' "$workflow" || true)

  if grep -Eq 'pull_request_target:|workflow_run:|issue_comment:' "$workflow"; then
    fail "$workflow uses a privileged/untrusted event trigger"
  fi
  if grep -Eq 'runs-on:.*(self-hosted|\$\{\{|vars\.)' "$workflow"; then
    fail "$workflow uses a self-hosted or variable runner; public code must use a literal GitHub-hosted runner"
  fi
  if grep -q 'secrets\.' "$workflow"; then
    fail "$workflow consumes repository secrets; live operations belong in the private ops boundary"
  fi
done <<< "$workflow_files"

font_dir="products/gongzzang/apps/web/public/fonts"
font_manifest="$font_dir/pretendard-v1.3.9.sha256"
if [ -f "$font_manifest" ]; then
  if ! (cd "$font_dir" && sha256sum --check --strict --quiet "$(basename "$font_manifest")"); then
    fail "Pretendard font hashes do not match the reviewed v1.3.9 asset manifest"
  fi
  tracked_fonts="$(printf '%s\n' "$tracked" | grep -E '^products/gongzzang/apps/web/public/fonts/.+\.woff2$' | sed "s#^$font_dir/##" | sort || true)"
  manifest_fonts="$(awk '{ sub(/^\*/, "", $2); print $2 }' "$font_manifest" | sort)"
  if [ "$tracked_fonts" != "$manifest_fonts" ]; then
    fail "tracked Pretendard fonts and the reviewed hash manifest differ"
  fi
fi

if [ "$rc" -ne 0 ]; then
  exit "$rc"
fi
echo "OK public-repository-safety"
