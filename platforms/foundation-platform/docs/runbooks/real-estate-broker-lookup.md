---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-12
---

# 중개사 가입 확인 명부 (`foundation-real-estate-broker-lookup-load.service`)

결정: 루트 ADR-0550. 공짱 가입 때 중개사 한 사람이 명부에 있는지만 답하는 Postgres 명부와 그 적재다.

## 무엇이 도는가

| 조각 | 위치 |
|---|---|
| 명부 | `catalog.real_estate_broker_office`, `catalog.real_estate_broker_member` (덧붙이기 전용), 원장 `catalog.real_estate_broker_lookup_load` |
| 적재 | `foundation-outbox-publisher load-real-estate-broker-lookup` — 두 Silver 표(`silver.real_estate_broker_office`·`_member`)의 원장 워터마크 이후 바뀐 사무소만 한 트랜잭션에 |
| 작업 | `real_estate_broker_lookup_load` (`definitions/real-estate-broker-lookup.definition.json`): 두 레인이 바꾸면 시작, 08:30 대체 |
| 확인 경로 | foundation-api `POST /internal/real-estate-brokers/verify` (서비스 정책 `foundation.real_estate_broker:verify`) |
| 실시간 대체 | 명부에 없으면 VWorld `getEBBrokerInfo`. 키는 `/etc/foundation-platform/vworld-live.env`(키·도메인 둘) |

## 확인

- 마지막 적재: `select * from catalog.real_estate_broker_lookup_load order by loaded_at desc limit 3;`
- 작업 결과 줄: `journalctl -u foundation-real-estate-broker-lookup-load.service -n 20` 의 `foundation-job-outcome`
- 실시간 키가 없으면 확인 경로의 답 `checked` 에 `vworld_live` 가 없다(명부만으로 답함).

## 실패했을 때

- 적재는 한 트랜잭션이라 실패한 run 은 아무것도 남기지 않고, 다음 run 이 같은 워터마크에서 다시 읽는다.
- 실시간 키를 놓거나 바꾸기: `foundation-release.sh env-set vworld-live <NAME=value 파일>` (이름은 `config/environment-variable-naming.contract.json` 의 VWorld 정본). API 컨테이너가 다시 만들어질 때 읽는다.
