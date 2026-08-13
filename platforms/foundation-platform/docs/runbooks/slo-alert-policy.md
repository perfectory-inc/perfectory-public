---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# SLO 알림 정책

## 목적

Foundation Platform lakehouse·API SLO를 정의하거나 검토할 때 이 런북을 사용한다. 이 정책은
SLO 목표를 알림 규칙·대시보드 소유권·on-call 대응 기준에 연결한다.

## 필수 신호

SLO를 운영 준비 완료로 판정하기 전에 다음 신호를 추적한다.

- API liveness·readiness 상태
- Catalog 읽기 지연·오류율
- 수집 실행 시간·행 수·검증 실패 수
- source snapshot부터 Gold pointer 발행까지의 freshness 지연
- outbox 발행 지연·재시도 수
- R2 요청 수·저장소 오류 수

## 초기 SLO 목표

- API readiness: 5분 창의 99.9%가 ready를 반환
- API 5xx 비율: 5분 동안 요청의 1%를 초과하면 page
- API request timeout: 5분 동안 timeout이 하나라도 관찰되면 운영 티켓 생성
- API overload 거부: concurrency-limit 거부가 5분 동안 발생하면 운영 티켓 생성
- DB pool 고갈: idle connection이 없고 설정된 최대치인 상태가 5분이면 운영 티켓 생성
- Catalog 읽기 가용성: 월 99.9%
- Gold freshness: 핵심 카탈로그 테이블이 승인된 최대 staleness 창 안에 발행
- Outbox fan-out: 발행 가능한 이벤트의 99%가 10분 안에 전달 또는 격리

계약·소스별 기준 정책의 원본은 `docs/observability/slo-policy.v1.example.json`이다. 대시보드와
알림 규칙이 조용히 바꾸면 안 되는 초기 freshness·duration·outbox pending-age 임계값을 고정한다.

## 대시보드

대시보드는 다음을 보여야 한다.

- 현재 liveness·readiness 상태
- 활성 장애
- 최근 성공 source snapshot ID와 Gold pointer
- 계약별 최근 성공 lakehouse batch 생성 시각·기록 시각·행 수
- 소스별 최근 Bronze 수집 종료 시각·실행 시간·확인 레코드·작성 객체·원본 응답 바이트
- 공공 API 쿼터 영향 요청 수·의존성 시간·의존성 오류 수
- R2 smoke 요청 수·검증한 smoke 바이트·inventory 크기·예상 list 요청 비용·billing 요청 수·
  billing 바이트·billing 비용
- 작업별 검증 실패 수
- 알림 상태와 담당 on-call rotation

기준 대시보드 원본은 `infra/observability/grafana/foundation-api-dashboard.json`이다. API scrape
계약·lakehouse freshness·Bronze 수집 원본 응답 바이트·공공 API 쿼터/의존성 산출물·R2 smoke·
R2 inventory·R2 billing 지표를 포함한다. 대시보드는 선택적 표시 계층이며 Prometheus 알림
평가와 Alertmanager 라우팅은 Grafana 배포 여부에 의존하지 않는다.

## 알림 정책

- SEV1 소비자 데이터 정확성 문제 또는 readiness 손실은 on-call에 page한다.
- freshness 지연·공급자 실패 반복·재시도 backlog는 업무 시간 알림으로 보낸다.
- 쿼터 소진 속도·비용 이상·비핵심 대시보드 drift는 티켓을 만든다.
- 모든 알림에는 service·environment·correlation ID 또는 run ID·첫 번째 런북 링크를 넣는다.

## 규칙 원본

기본 Prometheus alert rule은
`infra/observability/prometheus/foundation-api.rules.yml`에 있다. `compose.observability.yml`은
Prometheus와 Alertmanager를 배포하고 이 rule file을 읽은 뒤 private Compose network에서
`foundation-api:8080/metrics`를 scrape한다. `GET /metrics`가 내보내는 API scrape contract를
다루며 API process·database readiness·API 5xx rate·request timeout count·app-level overload
rejection count·lakehouse batch staleness·DB pool pressure·API p95 latency·ingestion staleness·
ingestion failure·ingestion duration·outbox pending age·outbox retry backlog gauge를 포함한다.
출시 전 Alertmanager receiver는 `prelaunch-audit`이며 외부 paging provider로 보내지 않고 운영자
rehearsal용 routed alert를 보존·노출한다. 공개 launch 전에는 소유한 staff notification route와
secret을 만들고 전달을 테스트해야 한다. 같은 controlled outage alert가 Prometheus와 Alertmanager
양쪽에서 활성화된 뒤 API 복구 후 해제되어야 pre-launch rehearsal을 통과한다.
같은 scrape endpoint는 freshness dashboard용으로 최신 성공 lakehouse batch의 생성 시각·기록
시각·행 수를 contract별로 내보낸다. 최신 Bronze ingestion 종료 시각·duration·확인 record 수·
작성 object 수·source별 raw response bytes와 status, Catalog outbox pending/retry/oldest-age
metric도 제공한다. `foundation_api_http_requests_total`은 method·canonical route·status별로,
timeout response는 `foundation_api_http_request_timeout_total`로 canonical route별 집계한다.
app-level traffic budget 거부는 reason별 `foundation_api_http_overload_rejected_total`로 내보낸다.
PostgreSQL pool pressure는 `foundation_api_db_pool_size`,
`foundation_api_db_pool_idle_connections`, `foundation_api_db_pool_max_connections`로,
request latency는 method·canonical route·status·histogram bucket별
`foundation_api_http_request_duration_seconds_bucket`으로 내보낸다.

초기 staleness 임계값은 24시간, 초기 느린 수집 임계값은 3600초, 초기 outbox pending-age
임계값은 600초, 초기 API p95 지연 임계값은 1초다. 이는 기준 운영 tripwire이며 최종 비즈니스
SLO가 아니다.

## 검토

모든 SEV1·SEV2 장애 뒤와 새 운영 스케줄을 켜기 전에 SLO를 검토한다. 대시보드와 알림 이력이
현재 목표를 일관되게 충족한다는 증거가 생기기 전에는 목표를 높이지 않는다.
