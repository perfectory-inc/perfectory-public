#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

checker="scripts/guard/roadmap-owns-recorded-debt.sh"
test_root="$(mktemp -d)"
cleanup() {
  case "${test_root:-}" in
    /tmp/*|/var/tmp/*|[A-Za-z]:/*) rm -rf -- "$test_root" ;;
    *) echo "FAIL roadmap-owns-recorded-debt-self-test: unsafe temp path" >&2 ;;
  esac
}
trap cleanup EXIT

# Builds a synthetic repository: one roadmap body and one ADR body, both written verbatim so a
# fixture can hold a heading that only looks like a debt section.
fixture() {
  local root="$1"
  local roadmap_body="$2"
  local adr_body="$3"
  local adr_path="${4:-docs/adr/0001-example.md}"
  mkdir -p "$root/docs/roadmap" "$root/$(dirname "$adr_path")"
  {
    printf -- '---\nstatus: current\n---\n\n# 운영 준비 작업 목록\n\n'
    printf '%s\n' "$roadmap_body"
  } >"$root/docs/roadmap/production-readiness.md"
  {
    printf -- '---\nstatus: current\n---\n\n# 예시 결정\n\n'
    printf '%s\n' "$adr_body"
  } >"$root/$adr_path"
}
expect_allowed() {
  bash "$checker" "$1" >/dev/null || {
    echo "FAIL roadmap-owns-recorded-debt-self-test: rejected allowed fixture $1" >&2
    exit 1
  }
}
expect_rejected() {
  if bash "$checker" "$1" >/dev/null 2>&1; then
    echo "FAIL roadmap-owns-recorded-debt-self-test: accepted forbidden fixture $1" >&2
    exit 1
  fi
}

linked="$test_root/linked"
fixture "$linked" \
'- [예시 결정](../adr/0001-example.md)의 남은 항목을 우선순위 1에서 처리한다.' \
'## 남은 부채

1. **아직 writer가 없다.**'
expect_allowed "$linked"

# The defect this guard exists for: recorded debt the single task list never mentions.
unlinked="$test_root/unlinked"
fixture "$unlinked" \
'- 다른 작업만 적혀 있다.' \
'## 남은 부채

1. **아직 writer가 없다.**'
expect_rejected "$unlinked"

# An ADR without a debt section is not a participant and needs no link.
no_debt="$test_root/no-debt"
fixture "$no_debt" \
'- 다른 작업만 적혀 있다.' \
'## Consequences

- 게이트가 거부한다.'
expect_allowed "$no_debt"

# A link carrying a section anchor still resolves to the document.
anchored="$test_root/anchored"
fixture "$anchored" \
'- [예시 결정](../adr/0001-example.md#남은-부채)' \
'## 남은 부채

1. **아직 writer가 없다.**'
expect_allowed "$anchored"

# ADR-0010 qualifies its heading in parentheses. The first version of this guard anchored on the
# end of the line and missed it, so this fixture pins the qualified form as a debt section.
qualified="$test_root/qualified"
fixture "$qualified" \
'- 다른 작업만 적혀 있다.' \
'### 남은 부채 (이 ADR로 닫히지 **않는** 것)

1. **아직 writer가 없다.**'
expect_rejected "$qualified"

# A separator is still required, so a sentence-shaped heading is not a debt section.
sentence_heading="$test_root/sentence-heading"
fixture "$sentence_heading" \
'- 다른 작업만 적혀 있다.' \
'## 남은 부채를 로드맵이 소유하는 이유

- 산문 규칙은 지켜지지 않았다.'
expect_allowed "$sentence_heading"

# The phrase in prose is not a declaration. Keying on the heading keeps an ADR that discusses
# another ADR's debt from being forced into the roadmap for a sentence it merely quotes.
prose_only="$test_root/prose-only"
fixture "$prose_only" \
'- 다른 작업만 적혀 있다.' \
'ADR-0016의 남은 부채 3번이 이 결정의 입력이었다.'
expect_allowed "$prose_only"

# A platform ADR has no platform-level roadmap to land in, so the global list owns it too.
platform="$test_root/platform"
fixture "$platform" \
'- 다른 작업만 적혀 있다.' \
'## 남은 부채

1. **아직 writer가 없다.**' \
'platforms/example-platform/docs/adr/0001-example.md'
expect_rejected "$platform"

# Without the roadmap there is no single list, and passing would assert nothing.
no_roadmap="$test_root/no-roadmap"
fixture "$no_roadmap" 'x' '## 남은 부채

1. **아직 writer가 없다.**'
rm -f "$no_roadmap/docs/roadmap/production-readiness.md"
expect_rejected "$no_roadmap"

echo "OK roadmap-owns-recorded-debt-self-test"
