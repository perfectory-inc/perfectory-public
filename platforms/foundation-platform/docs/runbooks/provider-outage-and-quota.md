---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# 공급자 장애·쿼터 런북

## 목적

V-World·data.go.kr 또는 다른 공공데이터 공급자가 unavailable·rate-limited 상태이거나,
잘못된 envelope를 반환하거나, 쿼터 소진에 가까울 때 이 런북을 사용한다.

## 범위

이 런북은 공급자 대상 수집 명령만 다룬다. 새 배치 크기·공공 API 재배포·쿼터에 영향을 주는
실시간 작업을 승인하지 않는다. 그런 작업은 여전히 사용자 승인이 필요하다.

## 탐지

신호는 다음과 같다.

- V-World·data.go.kr client의 일시적 HTTP 오류 반복
- 원본 응답의 공급자 도메인 오류 envelope
- request timeout 급증
- quota header 또는 공급자 포털의 잔여 쿼터 부족
- 수집 실행 실패율이 평상시 기준보다 높음

## 즉시 완화

1. 영향받은 공급자의 선택 수집 작업을 중지한다.
2. 읽기 API는 최신 정본 데이터를 계속 제공하게 한다.
3. 장애가 열린 동안 Bronze 원본 응답을 삭제하지 않는다.
4. 공급자 재요청보다 보관된 Bronze 재생을 우선한다.
5. 공급자·endpoint·시간 범위·요청 수·샘플 request ID를 기록한다.

## Client Circuit Breaker

공급자 HTTP client는 재시도 가능한 일시 오류에 bounded retry·시도별 timeout·프로세스 내부
circuit breaker를 사용한다. 재시도 횟수를 다 쓰면 회로가 열린 동안 다음 요청은 공급자를
즉시 다시 호출하지 않고 빠르게 실패한다.

현재 적용 범위는 다음과 같다.

- `DataGoKrServiceApiClient`
- `VWorldDataApiClient`
- `VWorldNedAttributeClient`

circuit breaker는 프로세스 로컬이다. 영속 DLQ/quarantine 테이블·pod 간 공급자 장애 상태·
운영자용 장애 기록을 대체하지 않는다.

## 쿼터 보호

재개하기 전에 다음을 확인한다.

- 공급자 레인·초기/최대 속도·쿼터 신호·재시도 정책·defer-without-drop 동작의 SSOT는
  `docs/catalog/provider-rate-policy.v1.json`으로 취급한다.
- 국가 Bronze 수집 재개에는 기본 `ProviderLaneMode=provider_policy`인
  `foundation-outbox-publisher resume-national-data-collection-ledger`를 사용한다. 이 명령은
  chunk를 레인 단위로 만들고 레인을 분리 스케줄링하며 레인에서 계산한
  `ProviderMinPageIntervalMs` 상한을 ledger executor에 전달한다.
- fixture 테스트 또는 명시적으로 기록된 증명 실행 외에는 국가 수집에
  `ProviderLaneMode=off`를 사용하지 않는다.
- 공급자 속도·신호 규칙을 바꾼 뒤에는 `foundation-outbox-publisher provider-rate-controller`로
  변경 정책을 실행한다(변경된 레인마다 initialize 모드). 이 명령은
  `docs/catalog/provider-rate-policy.v1.json` 파싱 실패나 알 수 없는 레인 ID에서 빠르게
  실패한다. (기존 독립 `check-provider-rate-policy`/`check-provider-rate-controller` 게이트는
  2026-06-22 자체 검증 evidence-gate 정리 때 삭제됐다.)
- page 범위 또는 bounding box 범위를 줄인다.
- 재시도 제한을 bounded 상태로 유지한다.
- 실시간 공급자 호출보다 읽기 전용 smoke 확인을 우선한다.
- data.go.kr·V-World 실시간 요청 전에 공급자 대상 서브커맨드가 명시적 쿼터 영향 확인을
  요구하는지 확인한다.
- 승인된 실시간 smoke에서 명령의 `*_QUOTA_METRICS_PATH` 환경변수를 설정해 Prometheus 호환
  쿼터/의존성 산출물을 쓴다. 산출물에는 `foundation_platform_public_api_quota_request_total`,
  `foundation_platform_public_api_dependency_request_duration_seconds`,
  `foundation_platform_public_api_dependency_error_total`이 포함된다.
- 같은 공급자에 대해 병렬 수집을 실행하는 운영자가 없는지 확인한다.

## Event Fabric 경계

Kafka/MSK 또는 Redpanda는 outbox fanout·검색·알림·AI·downstream consumer를 위한 향후
event-fabric 어댑터다. 공공 공급자 rate control의 권위가 아니다. 공공 공급자 수집 속도는
공급자 rate policy·레인 scheduler·페이지별 요청 간격·쿼터 증거가 결정한다. 향후 Kafka publisher도
기존 event publisher 계약 뒤에 두며 수집 코드가 Kafka client에 직접 의존하게 만들지 않는다.

## 장애 전환

장애 전환 선택지는 원천 권위에 따라 제한된다.

- V-World cadastral geometry: use cached Bronze/Silver outputs until provider recovers.
- data.go.kr building register: use the latest archived Bronze and mark freshness degraded.
- R2/Iceberg read path: keep serving the latest known-good snapshot.

권위가 없는 데이터셋을 정본 데이터로 조용히 대체하지 않는다. 사용자에게 임시 파생 화면이
필요하면 stale로 표시하고 마지막 source snapshot ID까지 추적한다.

## 복구

1. 공급자에 대해 좁은 범위의 읽기 전용 smoke를 실행한다.
2. 승인된 최소 범위로 수집을 재개한다.
3. 이전 성공 실행과 행 수·스키마 프로필 변화를 비교한다.
4. 장애 ID와 복구 실행 ID를 기록한다.
5. freshness SLO를 놓쳤으면 소비자에게 알린다.
