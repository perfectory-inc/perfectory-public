---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# 운영 Orchestrator 전환

## 목적

Foundation Platform이 lakehouse 수집·projection 작업을 수동 CLI 실행에서 운영 orchestrator로
옮길 때 이 런북을 사용한다. orchestrator는 스케줄·재시도·의존성 순서·운영자 가시성을 맡지만
카탈로그 데이터의 정본이 되지는 않는다.

## 승인 게이트

구현 선택을 승인하기 전에는 운영 orchestrator로 전환하지 않는다. Temporal·Dagster·Airflow 또는
다른 runtime을 도입하면 패키지·인프라·운영 상태가 추가될 수 있다. 운영 스케줄을 켜기 전에
결정을 ADR에 기록한다.

## 사전 조건

- 정본 입력·출력 계약이 `crates/lakehouse/lakehouse-domain/src/lakehouse.rs`에 문서화되어 있다.
- lakehouse smoke 흐름이 통과한다(Docker Spark 프로필에서 `infra/lakehouse/spark/jobs/`의
  Spark 작업을 로컬 실행하는 cargo 테스트 포함).
- `infra/lakehouse/spark/jobs/`의 lakehouse 작업 정의가 갱신·검토되었다(의존성 순서: Bronze→
  Silver, Silver→Gold, gold-pointer publish).
- 선택한 orchestrator의 소유자·배포 대상·롤백 경로·감사 로그가 문서화되어 있다.
- 모든 스케줄 작업에 멱등 run ID·source snapshot ID·대상 테이블·예상 행 수가 있다.

> 2026-06-21 note: the former local pre-runtime manifest runner, the
> `infra/orchestration/foundation-platform-lakehouse.jobs.yml` manifest, the GitHub `workflow_dispatch`
> cutover-evidence path, and the dispatch/fetch helper scripts were all removed as ceremony. The
> production orchestrator runtime itself is still unimplemented; when it is adopted, drive it from
> the Spark jobs under `infra/lakehouse/spark/jobs/` and the `foundation-outbox-publisher`
> publish subcommands, and record the runtime and rollback decisions in an ADR.

## 전환 계획

1. 기존 수동 명령을 실행하고 batch 감사 행을 기록한다.
2. 같은 작업을 스케줄 비활성 상태로 orchestrator에 등록한다.
3. smoke 또는 staging 대상을 대상으로 임시 orchestrated 실행을 한 번 수행한다.
4. 재시도 정책·timeout·취소·운영자 로그를 확인한다.
5. orchestrated 출력이 수동 출력과 일치한 뒤에만 스케줄을 켠다.
6. 수동 명령을 롤백 경로로 문서화해 유지한다.

## 재시도·Backoff

- 공급자·저장소·데이터베이스의 일시적 실패만 재시도한다.
- 운영자 조치 없이 결정적 검증 실패를 재시도하지 않는다.
- 명시적 최대 시도 횟수의 bounded retry를 사용한다.
- 모든 실패 시도를 실행 요약 또는 감사 로그에 보존한다.

## 롤백

orchestrated 실행이 수동 실행과 달라지면:

1. orchestrator 스케줄을 끈다.
2. 소비자는 이전의 검증된 포인터를 계속 사용하게 한다.
3. 입력 쿼터·쓰기 승인 게이트가 허용할 때만 수동 명령을 실행한다.
4. 장애 기록에 orchestrator run ID·source snapshot ID·실패 검증 출력을 첨부한다.
