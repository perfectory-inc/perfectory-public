---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-12
---

# ADR 0026: 필지 증거 봉인자가 유일한 append 경계다

- Status: Accepted
- Date: 2026-08-12
- Implements: [ADR-0025](./0025-parcel-publication-names-one-sealed-iceberg-evidence.md)

## 맥락

ADR-0025는 `catalog.parcel_publication_source_evidence` 하나가 Iceberg table/snapshot,
Catalog provenance, 실행 증거 bytes, mirror source bytes를 함께 묶는다고 결정했다. 그러나 INSERT
권한이 일반 런타임 역할에 남아 있고 terminal run/load/target을 나중에 바꿀 수 있으면 그 관계는
검증 결과가 아니라 쓰기 순서에 따른 주장에 불과하다. 실행 증거의 schema 문자열만 publisher가
판정하는 것도 table UUID, snapshot 목록, 실제 R2 bytes를 검증하지 못한다.

반드시 유지할 불변식은 다음과 같다.

1. evidence INSERT는 실제 R2 bytes와 실제 Iceberg metadata를 읽은 봉인 transaction에서만 일어난다.
2. run은 `planned`로만 태어나고 상태 기계를 거친 terminal tuple은 바뀌지 않는다.
3. terminal projection load tuple과 그 load가 소유한 target row는 바뀌지 않는다.
4. 같은 run의 재시도는 모든 봉인 필드가 같은 경우에만 같은 evidence id를 재사용한다.

## 결정

`foundation-outbox-publisher seal-parcel-publication-evidence`를 ADR-0025의 유일한 봉인 명령으로
둔다. 명령은 `.env.local`의 표준 R2/Iceberg/Postgres 설정을 읽고 다음 세 경계를 순서대로
검증한다.

- R2 execution JSON: strict v1 구조, succeeded 상태, run/source pair, complete national scope,
  object/row/shard limit 부재, Iceberg commit, production cutover, national rollout을 검증하고 읽은
  bytes 자체의 SHA-256을 계산한다.
- Iceberg REST catalog: `silver.parcel_boundaries` table UUID와 보존된 snapshot 목록을 직접 읽어
  JSON의 table UUID/snapshot이 실제 metadata에 존재하는지 검증한다.
- Postgres: terminal run/source tuple과 complete quality report를 잠그고, run-scoped PNU/EWKB를
  다시 읽어 row count와 `parcel-projection-content-sha256-v1` digest를 계산한다.

모든 검증 뒤 같은 transaction에서만
`set_config('foundation.parcel_publication_evidence_sealer', 'on', true)`를 켜고 INSERT한다. 세 번째
인자 `true`는 capability가 transaction 밖이나 connection pool의 다음 사용자에게 남지 않게 한다.
trigger는 capability 없는 INSERT를 거부한다. `foundation_api`에는 evidence, mirror run/row,
projection load, parcel publication DML 권한을 주지 않는다.

봉인 명령은 새 role을 만들지 않고 기존 `foundation_migrator` 연결만 append transaction에 쓴다.
일반 runtime `DATABASE_URL`로 fallback하지 않으며 `.env.local`의
`FOUNDATION_MIGRATOR_DATABASE_URL`이 없으면 시작 전에 실패한다. 이 기존 강한 credential의 소유자는
schema와 capability를 모두 바꿀 수 있으므로 trusted operator 경계다. 별도 최소권한 role을 두지
않기로 한 현재 규모의 선택은 이 신뢰 경계를 없애지 않는다.

재시도 정책은 **exact idempotency**다. 같은 run을 같은 object key/bytes SHA, table UUID/snapshot,
source pair, row count/content digest, schema로 다시 봉인하면 기존 id를 반환한다. 어느 한 필드라도
다르면 이미 봉인된 사실을 덮지 않고 거부한다.

Publisher는 이미 봉인된 row에서 table UUID, execution object key, execution bytes SHA를 포함한
tuple을 읽는다. schema 문자열 하나를 production 판정으로 다시 해석하지 않는다. eligibility의
SSOT는 봉인자와 append-only evidence row다.

## 스키마 근거

append-only migration은 evidence 전용 INSERT capability, planned-only run INSERT, terminal run/load
tuple, terminal load target row를 제약한다. 기존 migration은 바꾸지 않는다. `DROP TRIGGER`는 같은
이름의 run guard를 INSERT까지 확장하기 위한 함수 gate 교체이며 데이터 제약을 제거하지 않는다.

## 신뢰 사슬의 현재 끝

이 저장소에서 `foundation-platform.parcel_publication_execution_evidence.v1` 객체를 production에
생산하는 구현은 아직 없다. 확인된 `silver_gold_national_promotion_execution.v1` writer는 Iceberg
write를 하지 않았고 production/national을 false로 기록하는 bounded negative evidence 생산자일
뿐이다.

`.env.local.example`에는 lakehouse 전체에 쓰는 shared writer credential 이름만 있다. parcel
execution evidence 전용 writer credential, 별도 bucket, IAM으로 고정한 prefix, create-only write,
R2 object lock/immutability 정책은 저장소에서 확인되지 않았다. `control/evidence/...` prefix는 이름
규약이지 credential 경계가 아니다. 따라서 현재 신뢰 사슬은 **shared R2 writer credential의
소유자**와 **foundation_migrator credential 소유자**에서 끝난다. R2 역할이 임의 JSON을 덮어쓸 수
있으면 봉인 직전 원하는 주장을 만들 수 있고, migrator는 DB guard 자체를 바꿀 수
있다. 이 ADR은 그 미해결 운영 경계를 닫혔다고 주장하지 않는다.

위조가 더 어려운 축은 Iceberg catalog에서 직접 읽는 table UUID와 snapshot set이다. R2 JSON에서
오는 축은 run/source ids, scope/limit, commit/cutover/rollout 주장이고, 봉인자는 그중 run/source를
Postgres terminal tuple과, table/snapshot을 catalog metadata와 교차 확인한다. status/scope/limit/
cutover/rollout 의미는 JSON 생산자의 신뢰에 남는다.

선례인 [FP-ADR-0025](../../platforms/foundation-platform/docs/adr/0025-bronze-catalog-recovery-evidence-sealing.md)는
create-only content-addressed key와 stored SHA metadata 재사용으로 이 경계를 닫는다. parcel producer가
생길 때도 전용 최소권한 writer와 create-only/content-addressed object를 생산 경로의 필수 조건으로
추가해야 한다.

## 기각한 대안

- 별도 sealer 서비스와 DB role(B): 현재 한 명령과 한 transaction으로 닫히는 경계에 배포 단위와
  credential 운영을 더하는 것은 현재 규모에 과하다. 일반 API role의 DML revoke와 전용 local
  capability가 필요한 보장을 더 작게 제공한다.
- publisher가 발행할 때마다 외부 증거를 재검증(C): 봉인 SSOT를 없애고 R2/catalog의 외부 TOCTOU를
  매 발행마다 되풀이하며 중복 호출을 만든다. publisher는 봉인된 row와 source/target bytes만
  재검증한다.

## 결과

- 정상 DB 권한과 제약을 모두 통과하던 직접 evidence minting 경로가 capability와 role revoke에서
  막힌다.
- terminal source/load/target을 검증 뒤 바꾸는 경로가 trigger로 막힌다.
- table UUID와 execution object identity가 publisher가 읽는 봉인 tuple에 남는다.
- production evidence producer와 R2 write immutability는 후속 운영 부채이며 이 구현의 완료 조건으로
  거짓 보고하지 않는다.
