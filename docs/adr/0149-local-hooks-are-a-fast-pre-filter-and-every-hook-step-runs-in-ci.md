# ADR 0149: 로컬 훅은 빠른 사전 필터이고, 훅의 모든 단계는 CI에서도 돈다

- Status: Accepted
- Date: 2026-10-05
- Amends: [ADR-0098](./0098-the-pre-push-hook-keeps-only-fast-checks.md) (예산을 숫자로 적고 가드로 강제한다)

## Context

[ADR-0005](./0005-hooks-advisory-ci-authoritative.md)는 "훅은 조언, CI는 권위"를,
[ADR-0098](./0098-the-pre-push-hook-keeps-only-fast-checks.md)은 pre-push에서 전체 가드 스위트와
lychee를 뺐다(실측 1,533초 → 1분 미만 목표). 두 결정 모두 문장으로만 있었고, 지키는 장치는 없었다.
2026-10-05 16 GB 노트북 실측과 전수 대조에서 드러난 것:

1. **예산이 다시 새고 있었다.** `lefthook run pre-push --all-files` 벽시계 180초. 원인은 Foundation
   소유 경계 검사 하나 — Rust 파일마다 perl 프로세스를 하나씩 띄워 단독 65초, 병렬 경합 시 180초.
   같은 검사는 `gongzzang-ci.yml`의 `Repo guardrails`(required/gongzzang-core)가 이미 돌린다.
2. **훅에만 있는 검사는 아무도 지키지 않는다.** pre-commit의 SP10 패널 가드 3종과 markdownlint는
   어떤 CI 워크플로에도 없었다. 패널 가드는 `git diff --cached --name-only`가 저장소 루트 기준
   경로(`products/gongzzang/apps/web/...`)를 내는데 `^apps/web/`로 걸러, 모노레포에서는 **한 번도
   일치할 수 없었다**. 훅의 `glob`도 같은 결함이었다 — Lefthook은 `root:`가 있어도 glob을 저장소 루트
   경로에 맞추므로 `apps/web/...` glob은 해당 파일이 스테이지돼도 명령을 건너뛰었다. 그 사이 `lib/panel/panel-renderer.tsx`가 `components/panels/**`를 import하는
   위반(#96)이 main에 들어와 그대로 있었다.

대형 조직의 관행도 같은 방향이다: Google은 presubmit에 빠르고 결정적인 검사만 두고 실제 테스트는
서버(TAP)에서 돌리며, Meta·rust-lang도 로컬 훅을 선택형 편의로 둔다(ADR-0005 근거 참조).

## Decision

1. **병합 관문은 CI다.** 브랜치 규칙의 required 컨텍스트 11개가 판정한다. 로컬 훅 초록은 검증
   주장이 아니다.
2. **로컬 훅 예산.** 단계마다 수 초, 훅 전체가 16 GB 노트북에서 2분 미만. 예산 등급을 넘는 것 —
   Rust 빌드/테스트(`cargo fmt` 외 Cargo 하위명령), 컨테이너 런타임, xtask 검증, 전체 가드 스위트,
   링크 크롤러, JS 빌드/테스트 러너, Python 테스트 러너 — 는 훅에서도, 훅이 직접 부르는 스크립트
   안에서도 금지한다.
3. **CI 먼저.** 훅의 모든 단계는 required CI 잡에서도 돌아야 한다. 검사를 추가하는 순서는 CI에
   넣고, 원하면 훅에 거울로 둔다. 훅에만 있는 검사는 허용하지 않는다.
4. **이 PR에서 옮긴 것.**
   - Foundation 소유 경계 검사는 pre-push에서 빼고 CI 전용으로 둔다(이미 required/gongzzang-core).
   - 패널 가드 3종에 `--all` 전체 트리 모드를 주고 `--relative`로 경로 결함을 고쳐 `Repo guardrails`
     에 넣었다. 훅의 죽은 glob은 지웠다(스크립트가 경로를 스스로 거른다, 각 1초 안쪽). `panel-guards.tests.sh`가 가드마다 위반을 심어 staged·`--all` 두 모드의 거부를
     요구한다(고치기 전 스크립트는 6건 모두 통과시켰다).
   - markdownlint는 `frontend` 잡(required/gongzzang-frontend)에서 전체 트리로 돈다.
   - main에 있던 패널 위반은 등록(import)을 `components/panels/panel-renderer.tsx` 조립점으로 옮겨
     해소한다. 프레임워크(`lib/panel`)는 kind에 의존하지 않는다.
5. **강제.** `scripts/guard/lefthook-time-budget.sh`(monorepo-guard, required/repository)가
   `lefthook.yml`의 각 명령에 대해 (a) 예산 등급 위반과 (b) CI 쌍둥이 부재를 거부한다. 쌍둥이 판정은
   명령이 부르는 스크립트 이름 또는 구체 도구 이름이 워크플로, 가드 러너, xtask 소스에 나오는지로
   한다. `lefthook-time-budget-self-test.sh`가 Cargo test/build, docker, 가드 스위트, turbo, 스크립트
   안에 숨은 docker, CI 없는 훅 전용 검사를 심어 거부를 확인한다.

이 가드가 막는 실제 사고: 푸시마다 20분 넘게 걸리다 메모리 압박으로 죽는 훅, 그리고 훅에만 있어
조용히 죽은 검사 뒤로 위반이 main에 들어오는 일.

## Consequences

- pre-push 최악(`--all-files`) 벽시계: 180초 → 46초(같은 노트북, 2026-10-05). 영역 파일이 없는
  일반 푸시는 약 20초. pre-commit은 6초.
- CI는 약해지지 않았다. 검사 4개(패널 3, markdownlint)가 CI에 새로 들어갔고 빠진 것은 없다.
- 로컬 훅의 매달림·실패를 `--no-verify`로 넘기는 것은 정상 절차가 아니다. 훅이 느리거나 막히면 이
  ADR의 예산 위반이므로 훅을 고친다.
- 가드가 스크립트를 한 단계만 따라가므로, 그 안에서 또 다른 스크립트를 부르는 무거운 경로는 보지
  못한다. 그런 사례가 생기면 따라가는 깊이를 늘린다.
