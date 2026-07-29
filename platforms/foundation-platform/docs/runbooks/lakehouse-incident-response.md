---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-07-29
---

# Lakehouse 장애 대응

## 목적

lakehouse 쓰기·읽기 전용 스모크·R2 객체 배치·Gold 포인터·소비자 무효화 이벤트가
잘못 동작할 때 이 런북을 사용한다.

## 심각도

- SEV1: 정본 포인터 또는 API가 소비자에게 잘못된 데이터를 반환한다.
- SEV2: 정본 쓰기 또는 승격이 막혔지만 소비자는 검증된 스냅샷을 계속 제공한다.
- SEV3: 소비자 영향 없이 스모크·검증·비운영 테이블만 실패한다.

## 초기 분류

다음을 수집한다.

- correlation ID 또는 request ID
- 영향을 받은 계약과 테이블
- source snapshot ID와 Iceberg snapshot ID
- 캐시 무효화가 관련되면 outbox event ID
- 운영자 명령과 환경 프로필

안전한 확인부터 실행한다(lakehouse·R2 서브커맨드 계약은 cargo workspace 테스트가,
Spark 작업은 `py_compile`이 담당한다).

```bash
cargo test --workspace --all-features
python -m py_compile infra/lakehouse/spark/jobs/*.py
```

## 완화

1. 영향을 받은 테이블의 신규 승격을 중지한다.
2. 소비자는 이전의 검증된 포인터를 계속 사용하게 한다.
3. 잘못된 payload의 이벤트가 발행됐다면 과거 payload를 수정하지 말고 교정된 버전 이벤트를
   발행한다.
4. 원본 응답과 실행 요약을 보존한다.
5. R2 네임스페이스 오염이 의심되면 `r2-namespace-contamination-recovery.md`로 전환한다.

## 보고

모든 SEV1·SEV2 장애에 대해 다음을 포함한 장애 기록을 작성한다.

- 심각도
- 시작·탐지 시각
- 현재 완화 조치
- 영향받은 소비자
- 최신 검증 스냅샷 또는 포인터
- 다음 업데이트 시각

## 종료 조건

다음 조건을 모두 충족할 때만 장애를 종료한다.

- 영향을 받은 읽기 경로가 검증된 스냅샷을 제공한다.
- outbox 재시도가 소진되었거나 명시적으로 격리되었다.
- 실행 요약과 감사 기록이 일치한다.
- 장애 기록에서 롤백 또는 정방향 수정 증거를 링크한다.
