---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-29
---

# Iceberg 스냅샷 롤백

## 목적

승격한 Iceberg 스냅샷의 행 수·스키마·계보·소비자 동작이 잘못되어 이전 스냅샷으로
돌려야 할 때 이 런북을 사용한다.

## 사전 조건

- 현재 Iceberg 스냅샷 ID와 되돌릴 대상 이전 스냅샷 ID를 확인한다.
- 대상 스냅샷이 사전 감사와 읽기 전용 스모크를 통과했는지 확인한다.
- 롤백 중 대상 테이블에 대한 추가 쓰기를 동결한다.
- Postgres·PostGIS·검색·캐시 계층을 정본으로 취급하지 않는지 확인한다.

## 롤백 절차

1. 운영자·사유·현재 스냅샷 ID·대상 스냅샷 ID를 롤백 요청 기록에 남긴다.
2. Iceberg 카탈로그 도구 또는 승인된 Spark/Trino 명령으로 테이블 포인터를 대상 스냅샷으로
   바꾼다.
3. 테이블에 대해 읽기 전용 검증을 실행한다.
4. 복구한 스냅샷에서 파생 서빙 계층을 다시 만든다.
5. 버전이 있는 포인터 또는 이벤트를 발행해 소비자가 오래된 읽기 모델을 무효화하게 한다.
6. 배포의 레이크하우스 마이그레이션 기록을 무효로 만든다(root ADR-0308). 배포는 입력·카탈로그 설정·jar 가 그대로면 마이그레이션을
   건너뛴다. 복원된 스냅샷의 스키마는 그 기록이 본 카탈로그와 다를 수 있다. 둘 중 하나를 한다.
   - 지금 다시 맞춘다(권장): `sudo /opt/foundation-platform/current/scripts/deploy/foundation-release.sh migrate`.
     손으로 돌리는 `migrate` 는 항상 돈다. 성공하면 새 기록을 남긴다.
   - 다음 배포가 돌게 한다: `sudo python3 -I -B /opt/foundation-platform/current/scripts/deploy/deploy_steps.py forget /opt/foundation-platform/current lakehouse-migrate`.
   - 둘 다 잊어도 `shape_ledger_check` 가 다음 캡처(매일)에서 계약 열이 빠진 표를 찾으면 다음 배포가 마이그레이션을 다시 돌린다.
     그때까지 기다리지 않는다.

## 감사 기록 필수 항목

감사 기록에는 다음을 포함한다.

- 테이블 계약
- 이전 스냅샷 ID
- 문제가 된 스냅샷 ID
- 복구한 스냅샷 ID
- 롤백 전·후 행 수
- 검증 명령
- 운영자와 요청 ID

## 검증

필수 검증은 다음과 같다.

- 재빌드가 있으면 Spark 실행 요약 계약 검증
- 복구한 테이블에 대한 읽기 전용 lakehouse 스모크
- 소비자 경로의 API 또는 포인터 재조회
- 캐시 무효화 이벤트를 발행했다면 outbox sender 확인

## 실패 처리

롤백이 실패하면:

1. 테이블 동결을 유지한다.
2. 파생 캐시를 수동으로 편집하지 않는다.
3. 소비자가 잘못된 데이터를 읽을 수 있으면 SEV1으로 올린다.
4. 추적되지 않는 수동 수정보다 새로 검증한 스냅샷을 만드는 정방향 수정을 우선한다.
