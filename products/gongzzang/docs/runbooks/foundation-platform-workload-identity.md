---
status: current
owner: gongzzang-제품
doc_type: runbook
last_reviewed: 2026-07-29
---

# Foundation Platform 워크로드 Identity 런북

## 범위

Gongzzang service는 `FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE`이 가리키는 파일에서 읽은
짧은 수명의 Zitadel workload bearer로 `foundation-api`를 호출한다. 정적 service-token 대체는 금지한다.

The active callers are:

- `gongzzang-api`: 공개된 Catalog 읽기
- `gongzzang-outbox-publisher`: Lakehouse Registry 계약

## 런타임 계약

- Identity Platform이 workload credential을 발급·갱신한다.
- 배포는 호출자만 읽을 수 있는 파일로 credential을 mount한다.
- Gongzzang은 각 Foundation 요청 전에 파일을 읽으므로 파일 교체에 process 설정 변경이 필요 없다.
- Foundation Platform은 bearer를 검증하고 호출자·요청 resource에 default-deny 정책을 적용한다.
- Gongzzang은 custom header로 authorization scope나 정책 결정을 보내지 않는다. identity·authorization
  소유자가 bearer에서 이를 계산한다.

## 교체

1. 같은 service identity에 대한 교체 workload credential을 발급한다.
2. mount된 credential file을 원자적으로 교체한다.
3. caller의 허용된 Foundation Platform request 하나가 성공하는지 확인한다.
4. caller의 허용 contract 밖 request 하나가 거부되는지 확인한다.
5. 두 확인이 통과한 뒤 이전 credential을 revoke한다.

## 실패 처리

- token 파일이 없거나 비었거나 읽을 수 없으면 Foundation 호출 전에 시작 또는 요청을 실패시킨다.
- bearer가 거부되면 인증 retry를 중단하고 workload credential을 교체·수리한다.
- Foundation Platform이 unavailable이면 호출자 timeout·circuit breaker를 사용하며 static token으로
  전환하지 않는다.

## 증거

배포 revision, 호출 service identity, credential 교체 시각, 허용 호출 결과, 거부 호출 결과, correlation ID를
기록한다. bearer 값이나 token 파일 내용은 절대 기록하지 않는다.
