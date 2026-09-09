# ADR 0098: pre-push 훅은 빠른 검사만 남기고 판정은 CI가 한다

- Status: Accepted
- Date: 2026-09-09

## Context

ADR-0005는 이미 "git hook은 조언용(advisory)이고 CI가 유일한 판정자"라고 결정했다. 그런데
pre-push 훅은 그 결정과 반대로 자랐다: `monorepo-guard.sh` 전체 스위트와 Docker 기반 lychee
링크 검사가 들어 있어, 한 번의 push가 개발 워크스테이션에서 20–25분을 차지했고 훅이 도는
동안 작업 트리 전체가 잠겼다.

2026-09-09 실측: push 7회 × 훅 20~25분 ≈ 그날 대기의 최대 단일 항목. 같은 검증을 CI가
매 PR에서 다시 돌렸으므로 로컬 실행은 판정을 하나도 추가하지 않았다. 반면 그날 PR을 세 번
빨갛게 만든 가드들(technology-version, build-coupling, pipeline-graph coverage,
parcel-current-selector)과 생성 문서 신선도 검사는 전부 **각 5초 안쪽**이었다 — 느린 훅이
정작 빠른 예방은 못 하고 있었다. 진단 자체는 이미 기록돼 있었다(개발 루프 병목: "로컬은
fmt+check까지, 판정은 CI").

## Decision

1. **pre-push에서 `monorepo-guard.sh` 전체 실행과 lychee 링크 검사를 제거한다.** 두 검사의
   판정은 CI 워크플로가 계속 소유한다(변경 없음). 이 결정은 ADR-0005의 집행이지 완화가
   아니다 — 훅이 실패해도 CI가 잡고, 훅이 없어도 CI가 잡는다.
2. **pre-push에는 수 초짜리 검사만 남는다**: 기존 영역 grep 3종(no-fake-pass,
   forbidden-markers, ownership-boundary)에 더해, 2026-09-09에 PR을 물었던 빠른 래칫 묶음
   (technology-version, build-coupling, pipeline-graph coverage, parcel-current-selector)과
   생성 문서 신선도 `--check` 4종(document-catalog, document-audit, foundation-baseline,
   pipeline-map)을 추가한다. 목표 총 소요는 1분 미만이다.
3. **이 묶음은 편의이지 검증 주장이 아니다.** 가드를 골라 돌린 로컬 초록은 "CI 초록"을
   의미하지 않는다는 기존 교훈은 그대로다. 완료 주장은 CI 결과로만 한다.

이 훅이 막는 실제 사고: 5초짜리 래칫 위반을 CI에 가서야 발견해 push-훅-CI 왕복(당일 기준
회당 약 40분)을 반복하는 일.

## Consequences

- push 1회의 로컬 비용이 20–25분에서 1분 미만으로 내려가고, 훅이 트리를 잠그는 시간도 같이
  사라진다.
- monorepo-guard 전체를 로컬에서 돌리고 싶을 때는 언제든 수동으로 돌릴 수 있다
  (`bash scripts/guard/monorepo-guard.sh`). Windows에서 Docker가 죽어 있으면 lychee 단계가
  매달린다는 기존 기록도 그대로 유효하다.
- gongzzang `docs/conventions/enforcement-flow.md`의 5단계 표는 pre-push 내용이 바뀐 만큼
  이 ADR을 가리키도록 갱신한다.
