# ADR 0418: 선언 정의가 생성하는 산출물은 커밋하지 않고, 모든 체크아웃과 릴리스가 생성한다 — ADR-0415 2단계 전환

- Status: Accepted (2026-11-05, 소유자 지시 "근본 구조전환까지 전부 알아서 빠르게 진행해")
- Date: 2026-11-05
- Builds on: [ADR-0415](./0415-every-dataset-group-is-one-declared-definition-and-every-copy-is-generated-from-it.md)(정의 하나, 1단계 등가 증명 #1062),
  [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md)·[ADR-0184](./0184-one-answer-for-running-jobs-and-a-release-the-kernel-keeps-immutable.md)(릴리스는 정본 main 의 바이트),
  [ADR-0363](./0363-a-deploy-judges-the-release-not-the-provider-or-the-archive-bytes.md)(같은 커밋은 같은 바이트),
  [ADR-0207](./0207-a-pull-request-runs-only-the-guards-whose-inputs-it-changes.md)(가드 선택기), [ADR-0149](./0149-local-hooks-are-a-fast-pre-filter-and-every-hook-step-runs-in-ci.md)(로컬 훅)
- 관련 사건: 2026-10-24 own-producer 종류 하나를 레인 사본 일부에만 적고 호스트 열거에서 빠뜨렸다(#814).
  2026-11 열린 레인 PR 들이 `jobs.v1.json`·`pipeline-graph.v1.json` 에서 서로 충돌했다(#1051 인벤토리).

## Context

ADR-0415 1단계는 정의 형식과 생성기를 두고, PR 마다 "커밋된 사본 → 정의 → 사본"이 같다는 것을 증명했다(#1062, 2026-11-05 병합).
2단계는 정의를 정본으로 커밋하고 사본을 지운다. 지운 파일 8개를 읽는 곳(2026-11-05 main 에서 잼):

| 읽는 곳 | 수 | 생성물이 있어야 하는 때 |
|---|---:|---|
| 저장소 파일(코드·시험·가드·스크립트, md 제외) | 약 200 | 모든 체크아웃 |
| Rust `include_str!` | 6 | `cargo` 빌드 전 |
| CI 작업의 체크아웃 | 53(희소 체크아웃 1개 제외) | 체크아웃 직후 |
| 가드·작업 선택기(`change_scope.py`·`guard_scope.py`·`foundation_ci_scope.py`) | 3 | 바뀐 파일을 셀 때 |
| 역사 읽기(`schema_registry.py` 의 병합 기준 비교) | 1 | 과거 커밋의 사본 |
| 호스트 배포(`foundation-deploy.sh` 의 git archive 4곳, 릴리스 승인 `foundation-release-admission.py`) | 5 | 릴리스 설치·검증·매 작업 시작 |

## Decision

1. **정본은 `platforms/foundation-platform/definitions/` 다.** 묶음 정의 137개와 `platform.definition.json` 이 커밋된다.
   생성물 8개(`declared_definitions.GENERATED`)는 `.gitignore` 에 있다. 대상은 작업 목록, 파이프라인 그래프와 그 예제, by-PNU 레인 줄이 든 `r2-connections`,
   호스트 메모리 계약, 스키마 레지스트리, 작업 의존 파일, 데이터 지도다.
2. **생성물은 늘 같은 경로에 생긴다.** 읽는 곳 약 200곳은 바꾸지 않는다. `scripts/catalog/declared_definitions.py generate` 가 쓴다. 쓰는 때는 셋이다.
   - CI 의 모든 전체 체크아웃 직후(`scripts/ci/generate-declared.sh`, 약 2초).
   - 로컬의 checkout·merge·rewrite 훅(lefthook, 파이썬이 없으면 건너뜀. ADR-0149: 같은 단계가 CI 에 먼저 있다).
   - 호스트 릴리스(아래 4).
3. **새 레인은 정의 파일 하나를 고친다.** `platform.definition.json` 의 `order` 는 전환 전 항목의 자리만 가진다. 그 뒤 항목은 id 순으로 뒤에 붙는다.
   항목을 지우면 그 id 도 `order` 에서 지운다(가드가 거부). 그래서 `order` 는 줄기만 한다.
4. **호스트 릴리스의 생성물은 서명된 CI 빌드 산출물로 온다**([ADR-0419](./0419-a-release-carries-its-generated-files-signed-by-the-ci-build-and-the-host-renders-nothing.md), #1078).
   호스트는 아무것도 렌더하지 않는다. 생성물은 CI 릴리스 빌드의 `generated` 이미지로 서명되어 온다. 승인 `prepare` 가 그것을 커밋 바이트 옆에 쓰고,
   `seal`·`verify-current` 는 다이제스트 표시 파일로 오프라인 확인한다. 배포의 사전 점검과 1단계 제어 체크아웃은 `generated` 명령으로 받는다.
   ADR-0419 §5 에 따라 이 전환은 #1078 이 배포된 **다음** 배포에서만 들어간다.
5. **정의가 바뀌면 생성물 전부가 바뀐 것이다.** CI 선택기 셋은 바뀐 파일에 정의 디렉터리나 생성기가 있으면 생성물 8개를 더한다
   (`declared_definitions.with_generated`). 그 파일들을 입력으로 적은 가드·작업은 손대지 않아도 선택된다. 범위 계약 검사는 생성물을 체크아웃의 파일로 센다.
6. **역사는 생성해서 읽는다.** `declared_definitions.py show --rev <커밋> --path <생성물>` 은 그 커밋의 정의로 생성한다. 정의가 없던 커밋은 그 커밋의 파일을 준다.
   `schema_registry.py` 의 추가만 하기 검사가 이것으로 병합 기준의 레지스트리를 읽는다.
7. **가드.** `declared-definitions-reproduce-artifacts` 는 2단계 뜻으로 바뀐다. 통과 조건은 셋이다: 정의가 생성된다, git 이 생성물을 추적하지 않는다, 트리의 사본이 지금 정의의 생성과 같다.
   자기 시험은 다섯 경우를 거부한다: 추적된 생성물, 손 편집, 엣지 id 직접 쓰기, 두 정의의 같은 id, 없는 `order` 항목.
8. **이 ADR 다음 PR 들(같은 전환 창, 관심사 하나씩).**
   - (a) 생성된 레인 색인(인벤토리 B-F02): 37개 파일의 레인 목록 재유도를 색인 읽기로 바꾼다.
   - (b) 레인 유닛 12개를 템플릿 유닛 2개로(B-F01).
   - (c) Rust 레인 상수와 손 개수 시험을 색인에서 읽게 한다.
   - (d) 사본 대조 가드 24개를 은퇴한다(인벤토리 E).

## Consequences

- 레인 하나 추가의 손 편집: 이 ADR 이 다루는 사본은 7 파일에서 정의 1 개로 준다(`declared_definitions.py measure`, 잰 값). 남은 사본(8a–d)은 다음 PR 들이 줄인다.
- 정의 디렉터리를 고치는 PR 끼리는 같은 파일을 고칠 때만 충돌한다. 레인마다 파일이 다르다.
- 리뷰어는 생성 결과를 PR 에서 diff 로 보지 못한다. `declared_definitions.py generate` 를 로컬에서 돌리거나, CI 산출물을 본다.
- 전환 PR 은 #1078(ADR-0419)이 병합되고 **배포된 뒤** 병합한다. 먼저 병합되면 실행 중인 제어 체크아웃의 승인에 `generated` 명령이 없어, 배포가 사전 점검에서 모든 작업을 붙잡고 1단계에서 멈춘다. 안전하지만 배포가 멈춘다.
- 되돌리기: 전환 PR 을 되돌리면 사본이 다시 커밋되고, ADR-0415 1단계 가드가 그 사본에서 다시 돈다.

## 기각한 대안

- **생성물을 커밋하고 `--check`**: ADR-0410·0415 가 기각했다. PR 들이 같은 파일에서 충돌한다.
- **읽는 곳 200곳을 정의를 직접 읽게 고치기**: 같은 해석을 언어마다 다시 쓴다(R4 위반). 생성된 파일 하나를 모두가 읽는 것이 하나의 해석이다.
- **Rust `build.rs` 로 생성**: 파이썬·셸·TS 의 읽는 곳이 cargo 를 거치지 않는다.
