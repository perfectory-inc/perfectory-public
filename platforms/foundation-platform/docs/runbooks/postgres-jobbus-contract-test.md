---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# Postgres JobBus 계약 테스트

`foundation_outbox::PostgresJobBus`는 내구성 있는 수집 디스패치 어댑터다. 벌크 우선 정책에 따라
기존 data.go.kr 국가 비동기 명령은 비활성화되어 있으므로 해당 JobBus 설정으로 운영 수집을
실행하면 안 된다. 현재 `hub.go.kr` 벌크 수집 명령은 벌크 스트리밍 경로를 사용하고,
실시간 쓰기 모드에서는 `PostgresJobBus`로 claim/ack한다. 아래 증명은 자격증명 없는 단위
테스트가 아니라 보호된 통합 테스트다. 일회용 PostgreSQL을 시작하고 저장소 마이그레이션을
적용한 뒤 lease fencing, 재시도·dead-letter 동작, 트랜잭션 `collection.raw_written` outbox
삽입을 검증한다.

기존 Postgres 모드는 여전히 `FOUNDATION_PLATFORM_NATIONAL_ASYNC_PAGE_QUEUE=1`과 호환되지
않는다. 명령은 공급자 요청을 만들기 전에 전체 레거시 API executor를 거부한다.

저장소 루트에서 Docker Desktop을 실행한 상태로 진행한다.

반복 가능한 일회용 테스트를 위해 Foundation CI 데이터베이스와 같은 고정 이미지
`postgis/postgis:17-3.5-alpine`을 사용 가능한 로컬 포트로 시작하고, 마이그레이션을 적용한다.
`DATABASE_URL`은 테스트 프로세스 환경에만 설정하고 finally/정리 단계에서 컨테이너를 삭제한다.
그 다음 실행한다.

```text
cargo test --locked -p foundation-outbox --test postgres_jobbus -- --ignored --nocapture
```

일반 `cargo test -p foundation-outbox` 실행에서는 이 보호 테스트가 의도적으로 `ignored`로
표시되며, 일반 실행은 자격증명 없이 유지해야 한다. 계약 테스트 성공은 실제 PostgreSQL에
대한 어댑터 동작만 증명하며, 운영 데이터베이스 가용성이나 실제 공급자 수집 성공까지
증명하지는 않는다.
