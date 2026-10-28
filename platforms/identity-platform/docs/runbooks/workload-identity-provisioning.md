---
status: current
owner: identity-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# 워크로드 Identity 프로비저닝

## 목적

Identity Platform만 워크로드 주체 ID·권한 부여와 환경별 서명된 ZITADEL subject를 주체에 매핑하는
책임을 가진다. 소비자 서비스는 Identity 데이터베이스에 쓰지 않고 배포 설정에 권한 정책을 복제하지 않는다.

## 정본

| 사실 | 정본 | 배포 형태 |
|---|---|---|
| Principal ID, display name, exact capabilities | `config/workload-principal-policy.v1.json` | Compiled into the reviewed provisioner binary |
| ZITADEL subject for one environment | Secret-managed binding document | Read-only file mounted into the one-shot job |
| Provisioned state | `identity.service_principal` and the append-only `identity.service_capability_grant_record` (current grants = view `identity.service_capability_grant`) | Written in one PostgreSQL transaction with one outbox event per grant change (root ADR-0212) |
| Contract parser and reconciliation behavior | `tools/identity-service-provisioner` | Versioned Rust code and tests |

예시 바인딩 문서는 의도적으로 잘못된 자리표시자를 포함하므로 환경을 프로비저닝할 수 없다. 실제
ZITADEL subject를 커밋하거나 바인딩 파일에 권한을 복제하지 않는다.

## 필요한 배포 입력

- `IDENTITY_PROVISIONER_PASSWORD`: 전용 `identity_provisioner` DB role의 secret
- `IDENTITY_WORKLOAD_PRINCIPAL_BINDINGS_FILE`: 환경 secret manager가 전달한 엄격한
  `identity.workload-principal-bindings.v1` 문서의 host 경로
- 검토된 Identity runtime image. image에는 policy artifact와 provisioner가 포함된다.

프로비저너 역할의 데이터베이스 권한은 다음뿐이다.

- `SELECT`, `INSERT`, and `UPDATE` on `identity.service_principal`;
- `SELECT` on the view `identity.service_capability_grant`;
- `SELECT` and `INSERT` on `identity.service_capability_grant_record` (no `UPDATE`/`DELETE`; a trigger also refuses them);
- `INSERT` on `identity.outbox_event` (`identity.service_principal.capability_granted.v1`/`capability_revoked.v1`);
- no staff, session, schema-create, database-create, or role-management access.

## 배포 순서

배포 컨트롤러는 다음 작업을 순서대로 실행하며 첫 실패에서 중단한다.

1. `identity-bootstrap`이 강화된 role과 DB 연결을 만든다.
2. `identity-database-migrator`가 검토된 schema를 적용한다.
3. `identity-runtime-grants`가 runtime·provisioner에 정확한 권한만 준다.
4. `identity-workload-provisioner`가 policy와 환경 binding을 검증·해석하고 한 트랜잭션에서 모든
   principal을 reconcile한다.
5. `identity-finalize`가 임시 DB 생성 권한을 회수하고 role 강화를 확인한다.
6. chain이 성공한 뒤에만 Identity API와 policy worker를 시작한다.

로컬 Compose 계약에서는 `.env.example`의 다섯 비밀번호 시크릿
(`IDENTITY_{ADMIN,MIGRATOR,API,POLICY_WORKER,PROVISIONER}_PASSWORD`), point
`IDENTITY_WORKLOAD_PRINCIPAL_BINDINGS_FILE` at a real local binding file, then run:

```bash
scripts/compose-smoke.sh -- start-all
```

명령은 상태와 행 수만 포함한 시크릿 안전 JSON 보고서를 반환한다. 바인딩 경로·데이터베이스 URL·
ZITADEL subject·토큰은 출력하지 않는다.

## 실패·재시도 규칙

- 알 수 없는 field, 지원하지 않는 version, placeholder, 중복 subject, 누락 서비스, capability drift는
  DB 연결 전에 실패한다.
- 모든 principal·capability 변경은 하나의 PostgreSQL 트랜잭션을 공유한다. subject 충돌이나 SQL 실패가
  발생하면 해당 실행의 모든 principal을 rollback한다.
- 동일한 policy와 binding 재실행은 멱등적이다.
- 등록된 principal의 capability를 정확히 동기화하고 제거된 capability는 회수한다.
- policy에서 빠진 principal을 조용히 삭제하지 않는다. retirement는 별도 검토한 명시적 revoke 작업이다.

## 변경·롤백 절차

1. Identity Platform의 policy artifact만 바꾸고 capability delta를 검토한다.
2. unit, strict Clippy, 폐기 가능한 PostgreSQL, Compose 계약 테스트를 실행한다.
3. 불변 runtime image 하나를 만들고 digest를 기록한다.
4. 환경 binding을 secret manager에서 versioning하되 커밋하지 않는다.
5. consumer를 시작하기 전에 one-shot 배포 chain을 실행한다.
6. principal/grant 수와 소비 서비스의 signed-token authorization smoke를 확인한다.

롤백은 기록해 둔 이전 이미지 digest와 시크릿 매니저 바인딩 버전을 사용하고 동일한 멱등 프로비저너를
다시 실행한다. 임의 SQL이나 소비자 서비스의 DB 접근으로 권한을 수리하지 않는다.

## 소비 서비스 쪽 계약 — 토큰 파일과 교체

발급된 자격을 쓰는 서비스(예: `gongzzang-api` 의 공개 Catalog 읽기, `gongzzang-outbox-publisher` 의
Lakehouse Registry 계약)는 짧은 수명의 Zitadel 워크로드 bearer 를 마운트된 파일에서 읽어 `foundation-api` 를
호출한다. 공짱은 그 파일 경로를 `FOUNDATION_PLATFORM_WORKLOAD_IDENTITY_TOKEN_FILE` 로 받는다
(`products/gongzzang/crates/foundation-platform-client/src/lib.rs`). 정적 service-token 대체는 금지한다.

- 배포는 호출자만 읽을 수 있는 파일로 자격을 마운트한다. 소비 서비스는 요청마다 파일을 다시 읽으므로 파일
  교체에 프로세스 설정 변경이 필요 없다.
- Foundation Platform 은 bearer 를 검증하고 호출자·요청 자원에 default-deny 정책을 적용한다. 소비 서비스는
  권한 범위나 정책 결정을 custom header 로 보내지 않는다 — bearer 에서 소유자가 계산한다.

교체 순서:

1. 같은 service identity 의 교체 자격을 발급한다.
2. 마운트된 자격 파일을 원자적으로 교체한다.
3. 호출자의 허용된 Foundation 요청 하나가 성공하는지 확인한다.
4. 호출자의 허용 계약 밖 요청 하나가 거부되는지 확인한다.
5. 두 확인이 통과한 뒤 이전 자격을 revoke 한다.

실패 처리:

- 토큰 파일이 없거나 비었거나 읽을 수 없으면 Foundation 호출 전에 시작 또는 요청을 실패시킨다.
- bearer 가 거부되면 인증 재시도를 멈추고 워크로드 자격을 교체·수리한다.
- Foundation Platform 이 응답하지 않으면 호출자 timeout·circuit breaker 를 쓰며 정적 토큰으로 전환하지 않는다.

기록: 배포 revision, 호출 service identity, 교체 시각, 허용·거부 호출 결과, correlation ID. bearer 값이나 토큰
파일 내용은 기록하지 않는다.

## 검증 증거

- `manifest_contract.rs`는 엄격한 versioning, 정확한 binding 범위, placeholder 거부, 커밋된
  최소권한 policy를 증명한다.
- `live_provisioning.rs`는 폐기 가능한 PostgreSQL에서 멱등성, 정확한 grant 제거, 빈 grant 회수,
  고유 subject 충돌 시 전체 rollback을 증명한다.
- `scripts/compose-smoke.sh`는 배포 순서, 전용 role ACL, principal/grant row, helper non-root UID,
  재실행, runtime credential 격리를 증명한다.
