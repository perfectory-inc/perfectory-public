---
status: current
owner: gongzzang-제품
doc_type: documentation
last_reviewed: 2026-07-29
---

# 부하 테스트

## 공개·비공개 경계

이 저장소는 재사용 가능한 k6 시나리오 코드·시나리오 registry schema·안전 검증을 소유한다.
배포별 target host·credential·측정 capacity 상한·실행 증거·출시 결정은 private operations가 소유한다.

따라서 커밋된 registry는 라우팅되지 않는 `.invalid` target과 의도적으로 낮은 synthetic 안전 상한을
사용한다. 이는 측정 capacity가 아니며 출시 추정치로 제시하면 안 된다. private load runner가 실행
시점에 승인 target과 검토된 capacity profile을 주입하고 두 값을 이 저장소에 다시 쓰지 않는다.

## 안전 규칙

- production 사용자 traffic 경로에 stress·spike·soak 테스트를 실행하지 않는다.
- Gongzzang 부하 시나리오에서 VWorld·OpenDataPortal quota를 사용하지 않는다.
- production PII를 사용하거나 인증 자료를 증거에 쓰지 않는다.
- local·CI smoke 결과는 시나리오 검증으로만 취급한다.
- target allowlist·측정 상한·원시 결과·출시 증거는 private operations 경계에 둔다.

운영자가 승인 target을 주지 않으면 runner는 fail-closed해야 한다. k6 시작 전에 URL credential·path·query·
fragment·production host를 거부한다. 인증 시나리오는 private runner 환경에서만 token을 받는다.

## 실행 유형

- `smoke`: 공개 synthetic 상한에서 시나리오·target wiring·credential·증거 writer를 검증한다.
- `baseline`: 승인된 private 환경에서 대표 read workload를 측정한다.
- `stress`: 승인된 non-production 환경에서 상한을 탐색한다.
- `spike`: 승인된 non-production 환경에서 burst 동작을 검증한다.
- `soak`: 승인된 non-production 환경에서 장시간 안정성을 검증한다.

커밋된 기본값은 `smoke`뿐이다. 나머지 실행 유형은 private에서 검토된 capacity profile이 필요하다.

## 시나리오 표

| 시나리오 | 목적 |
| --- | --- |
| `api-read-mix` | 혼합 API read 경로 검증 |
| `map-marker-mix` | 매물 marker base·delta·tombstone·cache 경로 검증 |
| `capacity-stress` | private non-production capacity 탐색 |
| `foundation-platform-events` | Foundation Platform event consumer 검증 |

`map-marker-mix` must exercise the runtime composition:

```text
visible markers = base tile + delta overlay - tombstone overlay - unauthorized records
```

삭제·private marker가 노출되거나, 성공한 tile에서 대상 marker가 빠지거나, private 승인 profile에서
포화가 발생하면 출시 증거로 승격할 수 없다.

## 증거

`LOAD_EVIDENCE_DIR`를 운영자가 소유한 private 위치로 설정하고 `k6 run --summary-export`에 전달한다.
증거에는 시나리오·profile·target·시각·k6 summary·threshold 출력·비교·결과 분류를 보존한다. 원시 결과와
private 증거 위치는 커밋하지 않는다.

## 결과 분류

- `pass`: private profile threshold와 증거 요구사항을 통과한다.
- `warn`: 실행은 끝났지만 운영자 검토가 필요한 우려가 있다.
- `fail`: threshold 실패, 증거 누락, 안전 규칙 위반이다.
