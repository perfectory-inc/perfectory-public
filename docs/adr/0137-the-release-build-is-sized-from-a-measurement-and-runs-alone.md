# ADR 0137: 릴리스 빌드의 상한은 측정에서 정하고, 빌드는 혼자 돈다

- Status: Accepted
- Date: 2026-10-03
- Amends: [ADR-0134](./0134-production-installs-only-canonical-main-and-keeps-artifacts-outside-the-release.md) §3 (빌드 상한 "2 CPU·`4g`")

## Context

ADR-0134는 운영 호스트에서 릴리스를 빌드할 때 publisher 빌더에 2 CPU·`4g`, swap 없음을 주었다. 이 값은
측정하지 않은 값이었다.

2026-10-03 ai-server에서 첫 전환(릴리스 451ee512)을 실행했다. `prepare`의 publisher 빌드는 한 시간 동안 끝나지 않았다.

- 빌더 cgroup은 상한에 284만 번 닿았다. OOM은 0번이었다.
- 컴파일러 프로세스는 한 시간 중 CPU를 1~2분만 썼다.
- 결론: 진행하지 못하고 메모리 회수만 반복했다. 상한이 그대로면 끝나지 않는다.

같은 이미지·CPU 할당·작업 수로, 메모리 상한만 24g로 열어 scratch에서 다시 쟀다(쓰기 없음, `--output type=cacheonly`).

| 지표 | 값 |
|---|---:|
| 익명 메모리 최대 (페이지 캐시 제외) | 17,914,359,808 B (16.7GiB) |
| 페이지 캐시 포함 최대 | 21,316,435,968 B (19.9GiB) |
| 걸린 시간 (2 CPU) | 2,590초 |
| 결과 | 성공 |

대부분은 번들된 DuckDB C++ 빌드(`libduckdb-sys`)가 썼다.

ai-server의 메모리 예산(`tools/host-memory-budget.contract.json`)은 다음 합이 물리 메모리 62g 안에 들어야 한다.

- 항상 떠 있는 컨테이너 상한의 합: 33.94g
- 가장 큰 일회성 작업
- 예약분: 5g

릴리스 빌드는 이 예산에 들어가 있지 않았다. 그리고 Spark 일회성 작업과 동시에 돌 수 있었다.

## Decision

1. **상한의 정본은 `tools/release-build.contract.json` 하나다.**
   - 인증 스크립트는 빌더·의존성 해석기의 CPU·메모리·cargo 작업 수를 제어 체크아웃의 이 파일에서 읽는다.
   - 코드에 숫자를 두지 않는다.
   - 값이 형식에 맞지 않으면 빌드를 거부한다.
2. **publisher 빌더는 `22g`, 2 CPU, cargo 작업 2개다.**
   - 22g는 측정한 익명 메모리 최대치의 1.3배다.
   - 의존성 해석기는 `4g`, 2 CPU로 그대로 둔다(Ivy 해석만 한다).
   - 시험은 계약이 인용한 측정값보다 상한이 큰지 확인한다.
3. **빌드는 호스트의 일회성 작업으로 예산에 들어간다.**
   - `host-memory-budget.contract.json`의 `one_shot_contracts`가 이 계약을 가리킨다.
   - 그래서 예산 가드가 빌더 상한을 센다: 33.94 + 22 + 5 = 60.94g ≤ 62g.
   - 24g는 가드가 거부한다(62.94g).
4. **빌드는 혼자 돈다.**
   - `prepare`는 빌드 전에 `orchestration/jobs.v1.json`에 등록된 모든 작업의 systemd 유닛을 확인한다.
   - 하나라도 active면, 그 이름을 대며 거부한다.
   - 운영자는 DAG를 멈추고, 실행 중인 작업이 끝난 뒤 다시 실행한다.
   - 그래서 "가장 큰 일회성 작업 하나"라는 예산의 가정이 지켜진다.
5. **측정을 다시 하는 조건.** 빌드 의존성(특히 DuckDB 판)이 바뀌어 빌드가 상한에 닿으면 같은 방법으로 다시 재고,
   계약의 값과 근거를 함께 고친다.

## Consequences

- 전환과 이후 배포가 끝날 수 있다. 대신 운영 호스트에서 한 번에 약 43분, 최대 약 18GiB를 쓴다.
- 배포하는 동안 등록 작업은 멈춰 있어야 한다. 전환 스크립트와 런북은 이미 DAG를 먼저 멈춘다.
- 운영 호스트에서 빌드하는 비용이 측정으로 드러났다. CI에서 한 번 빌드하고 서명한 이미지를 호스트가 검증만 하는
  방식은 별도 결정으로 다룬다. 그 결정이 나오면 이 계약의 publisher 항목은 사라진다.

## 개정 기록

- 2026-10-03: §4의 "active면 거부"는 구현 결함이었다. 등록 작업은 `oneshot` 이라 실행 내내 `activating` 이고
  `active` 가 되지 않으므로, `is-active` 검사는 돌고 있는 FLOOR 를 통과시켰다(외부 검토가 찾음). 이제 `ActiveState` 가
  `inactive`·`failed` 가 아니면 실행 중으로 본다. 또 검사 직후 작업이 시작되는 틈을 막으려고, 빌드는 검사 전에
  `/run/foundation-platform-release-build.lock` 을 배타로 잡고, 모든 등록 작업의 `ExecStartPre`(`verify-current`)는 그
  잠금이 잡혀 있으면 거부한다(Airflow 가 재시도한다). 위 결정 본문은 고치지 않았다.
