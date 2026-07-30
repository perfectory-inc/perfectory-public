---
status: current
owner: foundation-platform
doc_type: architecture
last_reviewed: 2026-07-29
---

# API 교환 방향 계약

Status: Accepted

Owner: foundation-platform

Date: 2026-07-09

## 목적

이 계약은 새 연동에서 수집, 명령 제출, 이벤트 전달, 분석 조회를 섞지 않도록 API 교환 방향을 고정한다.

핵심 규칙은 다음과 같다.

```text
일정·쿼터·멱등성·정본을 소유한 쪽이 방향을 결정한다.
```

## 방향 규칙

### 외부 제공기관 수집은 가져오기(Pull)

Foundation Platform이 data.go.kr, V-World, hub.go.kr, 국토부 실거래 내보내기와 제공기관 다운로드
경로에서 공공 데이터를 가져온다.

제공기관이 Foundation Platform으로 원자료를 밀어 넣지 않는다.

Foundation이 소유하는 수집 제어 항목:

- schedule
- provider quota and backoff
- Bronze object commit
- checksum truth
- lineage
- retry and resume

### 제품 카탈로그 조회는 가져오기(Pull)

제품 서비스는 조회 API로 공개된 Foundation 계약을 가져온다. Foundation 데이터베이스나 객체 레이크
내부를 직접 읽지 않는다.

현재 관리되는 서비스 경로:

- `GET /catalog/v1/parcels/by-pnu/:pnu`
- `GET /catalog/v1/parcels/by-pnu/:pnu/buildings`

공개 조회 계약이 추가될 수 있지만, 이 역시 공개 계약이지 저장소 직접 접근이 아니다.

### 제안 접수는 밀어넣기(Push)

`intelligence-platform`이 AI 정규화 제안을 만들고 Foundation Platform에 전달한다. Foundation은 이를
내구성 있게 접수하고 검토 게이트를 적용한다.

Current governed service surface:

- `POST /internal/normalization/proposals`

이 전달은 정본 쓰기 권한을 주지 않는다. Foundation 제안함에 접수증만 만들며, 검토·적용·롤백은
Foundation 직원/관리자 명령으로만 수행한다.

### 레이크하우스 산출물 등록은 밀어넣기(Push)

제품 소유 worker는 자신이 만든 산출물을 소유하고 Foundation이 교차 서비스 레지스트리 기록을
소유하는 경우 관리되는 산출물 등록 요청을 전달할 수 있다.

Current governed service surface:

- `POST /internal/lakehouse/artifacts`

전달 요청은 메타데이터만 등록한다. 호출자에게 Foundation 데이터베이스 직접 접근 권한을 주지 않는다.

### 관리자 명령은 명령 전달이다

직원/관리자 경로는 Foundation Platform으로 명령을 전달한다. 제공기관 수집이나 이벤트 전달이 아니며,
인증·인가·감사를 거치고 Foundation 애플리케이션 명령으로 라우팅해야 한다.

Examples:

- approve a normalization proposal
- reject a normalization proposal
- apply an approved proposal
- rollback an applied proposal
- promote or rollback a governed manifest

### Outbox 전달은 밀어넣기(Push)

Foundation은 `catalog.outbox_event`와 outbox publisher를 통해 커밋된 이벤트를 발행한다. 현재 전송은
웹훅이다. 나중에 Kafka를 추가할 수 있지만 방향은 Foundation에서 구독자로 가는 Push로 유지한다.

Outbox 이벤트는 커밋된 사실이나 내구성 있는 플랫폼 이벤트를 위한 것이다. 요청/응답 조회나 원천 수집이
아니다.

### dbt/Trino 모델링은 가져오기/조회다

dbt 모델은 Trino를 통해 레이크하우스 관계를 조회한다. dbt는 원천 데이터를 전달하거나 AI 모델을
호출하거나 제안을 승인하거나 정본 상태를 단독으로 공개하지 않는다.

dbt owns SQL modeling and SQL tests only.

## 경계 규칙

- 서비스 간 데이터베이스 직접 접근은 금지한다.
- 명시적으로 공개된 변경 불가 산출물이 아닌 한 서비스 간 객체 레이크 내부 접근은 금지한다.
- Pull API는 멱등 조회이거나 Foundation 소유 수집 worker여야 한다.
- Push API는 내구성 있는 접수증이나 수락된 명령을 반환해야 한다.
- 전달은 임의의 동기 콜백이 아니라 outbox 전송을 사용해야 한다.
- AI는 제안만 전달할 수 있고 정본을 직접 쓸 수 없다.
- 제품 서비스는 공개 계약만 가져올 수 있고 Foundation 내부를 읽을 수 없다.

## 현재 방향 표

| Flow | Direction | Owner of Truth | Current Mechanism |
|---|---|---|---|
| Public data collection | Pull | Foundation Platform | provider clients, BronzeCommitter |
| Gongzzang/Dawneer catalog lookup | Pull | Foundation Platform | service read APIs |
| AI normalization proposal submit | Push | Foundation Platform | `POST /internal/normalization/proposals` |
| Product artifact registration | Push | Foundation Platform registry | `POST /internal/lakehouse/artifacts` |
| Staff review/apply/rollback | Command push | Foundation Platform | staff/admin APIs |
| Event fan-out | Push | Foundation Platform | `catalog.outbox_event` -> webhook, future Kafka |
| SQL modeling | Pull/query | Foundation Platform | dbt -> Trino |

## 금지 사항

- 제품 서비스가 Foundation 내부 PostgreSQL 테이블을 폴링하는 것.
- 제품 서비스가 Foundation 정본 테이블에 쓰는 것.
- `intelligence-platform`이 정규화 제안을 직접 적용하는 것.
- dbt가 관리자 명령을 내리거나 Gold 포인터를 공개하는 것.
- Gongzzang이 공공 카탈로그 원천을 소유한 것처럼 제공기관 데이터를 Foundation으로 전달하는 것.
- 내구성 있는 outbox 이벤트 대신 동기 콜백 체인을 사용하는 것.
