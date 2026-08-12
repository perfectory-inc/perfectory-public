---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-11
---

# ADR 0025: 필지 발행은 봉인된 Iceberg 증거 하나를 지명한다

- Status: Accepted
- Date: 2026-08-11
- 관련: [ADR-0006 기준 데이터는 객체 저장소 우선](./0006-object-storage-first-serving.md), [ADR-0016 PostGIS 적재는 신원을 가진 하나의 사실이다](./0016-a-postgis-projection-load-is-a-fact-with-an-identity.md), [ADR-0020 도형은 사실의 근거가 아니다](./0020-geometry-is-not-evidence-for-a-fact.md), [ADR-0024 서빙 투영은 타일 계약이 지명한 것만 싣는다](./0024-the-serving-projection-carries-only-what-the-tile-contract-names.md)
- 발단: 필지 PostGIS 발행 커맨드 `8b42e44480280ab5e9063d6cb53dea24d1426a53` 적대적 리뷰

## Context

필지 PostGIS 발행 커맨드는 미러 재구성 실행 id, 발행 리비전 id, 정본 Iceberg 스냅샷 id,
source record id를 서로 독립된 운영자 입력으로 받는다. 지명한 실행에서는 `status`와
`loaded_row_count`만 읽는다. 따라서 미러 실행 A를 복사하면서 스냅샷 B와 source record C의
투영이라고 기록해도 각 행이 따로 존재하기만 하면 통과한다.

이것은 입력 검사가 적어서 생긴 문제가 아니다. **Postgres 안의 실행 사실과 R2/Iceberg 정본 사실을
한 행으로 결박한 원장이 없어서 운영자가 그 관계를 새로 주장하게 된 것**이 근본 원인이다.
[ADR-0006](./0006-object-storage-first-serving.md)이 정한 방향은 반대다. R2의 Catalog-selected Silver
Iceberg snapshot이 정본이고 PostGIS는 그 스냅샷에서 재구성 가능한 투영이어야 한다.

현행 표와 생산자는 다음과 같이 서로 다른 말을 한다.

- `serving_postgis.parcel_boundary_mirror_rebuild_run.source_snapshot_id`는
  `iceberg:` 이름공간을 가진 문자열이고 `source_record_id`와 `source_file_asset_id`는 nullable이다.
- `catalog.publication_revision.canonical_iceberg_snapshot_id`는 선행 0 없는 양의 십진 문자열이며,
  `(publication_unit_id, canonical_iceberg_snapshot_id)`가 UNIQUE다.
- 전국 미러 재구성 커맨드는 bounded QA 한도를 강제하고 source record/file asset을 기록하지 않는다.
  그 입력인 `silver_gold_national_promotion_execution.v1`은 실제 Iceberg table을 쓰지 않았고 Gold를
  승격하지 않았으며 production cutover와 national rollout을 허용하지 않았다고 기록한다.
- 미러 실행의 `quality_report` 제약은 JSON object라는 것만 보장한다. 현재 성공 픽스처의 빈 object도
  통과한다.
- 미러의 `geometry_checksum_sha256`은 EPSG:4326 source WKB의 checksum을 EPSG:5179 투영 옆에
  복사한 값이다. 그 문자열을 target에 복사해 비교해도 target geometry가 같다는 증거가 되지 않는다.

따라서 지켜야 할 불변식은 여섯 가지다.

1. 정본 스냅샷의 신원은 Iceberg metadata가 말하며 운영자가 번역하지 않는다.
2. 발행 커맨드는 출처 증거 하나만 지명하고 나머지 값을 원장에서 읽는다.
3. 출처를 알 수 없는 과거 실행을 새 provenance로 꾸며 backfill하지 않는다.
4. QA 실행은 값 몇 개가 우연히 맞아도 운영 발행 자격을 얻지 않는다.
5. 품질 보고가 없거나 불완전하면 성공이 아니라 미확인이다.
6. source와 target의 내용 동일성은 행 수가 아니라 정해진 byte digest로 판정한다.

Apache Iceberg는 snapshot을 table metadata가 보유한 `long` id와 그 snapshot의 manifest 집합으로
정의한다. 별도 snapshot 이름 체계를 발명하지 않고 이 값을 재사용한다. Geometry byte identity에는
이미 사용하는 PostGIS의 SRID 포함 EWKB와 PostgreSQL의 SHA-256을 쓴다. 새 data plane이나 외부
provenance 제품은 추가하지 않고, 기존 R2/Iceberg/Postgres 위에 작은 append-only control ledger만
둔다.

## Decision

### 1. 정본 표기는 Iceberg의 양의 십진 snapshot id 하나다

발행 계약의 정본 표기는 `catalog.publication_revision.canonical_iceberg_snapshot_id`가 이미 쓰는
**선행 0 없는 양의 십진 문자열**로 유지한다. `silver.parcel_boundaries`의 publishable 미러 실행에서
namespaced 표기는 정확히 그 값 앞에 `iceberg:`를 붙인 파생 표기여야 한다. 예를 들어 정본 값이
`123`이면 파생 표기는 `iceberg:123` 하나뿐이다.

문자열을 자르는 것만으로 동일성을 주장하지 않는다. 아래 §2의 evidence sealer가 실제 Iceberg
catalog metadata에서 table UUID, logical table `silver.parcel_boundaries`, `snapshot-id`의 존재를 읽고,
실행의 namespaced 값과 숫자 부분이 그 metadata와 같을 때만 봉인한다. 임의 토큰, 선행 0, 다른 table의
같은 숫자는 publishable evidence가 될 수 없다.

기존 숫자 제약과 `(publication_unit_id, canonical_iceberg_snapshot_id)` UNIQUE는 바꾸지 않는다.
기존 publication revision도 다시 쓰지 않는다. 같은 snapshot을 다시 materialise하면 새 revision이
아니라 [ADR-0016](./0016-a-postgis-projection-load-is-a-fact-with-an-identity.md)의 새 projection load가
된다. 기존의 숫자가 아닌 미러 snapshot label은 과거 QA 실행 기록으로 남지만 봉인할 수 없다.

검사는 `parcel_publication_source_evidence_rejects_noncanonical_snapshot_alias`가 namespaced 값의
오타, 선행 0, 다른 table UUID, 없는 snapshot을 각각 거부하는 것으로 증명한다.

### 2. 발행의 유일한 출처 입구는 봉인된 evidence id다

`catalog.parcel_publication_source_evidence`를 필지 발행 출처의 append-only SSOT로 둔다. 한 행은
최소한 다음 관계를 하나로 봉인한다.

| 값 | 의미 |
|---|---|
| `id` | 발행 커맨드가 받는 유일한 출처 id |
| `mirror_rebuild_run_id` | 고정된 source row set의 실행 신원 |
| Iceberg table UUID + logical table + positive snapshot id | 실제 R2/Iceberg 정본 신원 |
| `source_record_id` + `source_file_asset_id` | Catalog provenance와 불변 R2 evidence |
| execution evidence object key + SHA-256 | §4의 발행 적격성을 만든 입력 bytes |
| source row count + projection content SHA-256 | §5와 §6을 통과한 source set |
| quality schema version + sealed time | 판정 방언과 봉인 시점 |

이 행은 성공한 미러 실행과 upstream Catalog 행을 FK로 가리키고, insert 뒤 update/delete할 수 없다.
terminal 상태가 된 미러 실행 tuple도 봉인 뒤 바꿀 수 없다. 한 미러 실행에는 evidence가 최대 하나이고,
여러 실행이 같은 Iceberg snapshot을 다시 materialise하는 것은 허용한다.

`spatial_projection_load`에서 이 evidence까지 FK 경로가 이어지고, 그 load의 publication revision은
evidence에서 읽은 snapshot과 source record를 사용한다. target row는 기존 load/revision FK 경로로
같은 evidence에 도달한다. 환경변수 값의 동시 입력은 이 관계를 대신할 수 없다.

현재 필수 환경변수는 출처 네 개를 포함해 여섯 개다.

| 경계 | 현재 | 결정 후 |
|---|---:|---:|
| 출처·리비전 입력 | mirror run, data revision, snapshot, source record = 4 | source evidence id = 1 |
| 연결·명시 승인 | `DATABASE_URL`, confirm = 2 | `DATABASE_URL`, confirm = 2 |
| 합계 | 6 | 3 |

publisher는 data revision과 projection load id를 생성한다. 같은 snapshot의 revision이 이미 있으면
저장된 revision 전체를 되읽어 evidence의 unit, snapshot, source record와 모두 같은 경우에만
재사용한다. 운영자가 서로 독립된 provenance 값을 조립하는 경로는 두지 않는다.

검사는 `parcel_publication_accepts_one_sealed_source_evidence`와
`parcel_publication_cannot_mix_two_evidence_rows`가 맡는다. 이 검사가 막는 실제 사고는 미러 A를
snapshot B/source record C로 발행해 Postgres를 새 정본으로 만드는 것이다.

### 3. source record는 publishable 실행에는 필수지만 과거 nullable 행을 고쳐 쓰지 않는다

`parcel_boundary_mirror_rebuild_run.source_record_id`를 모든 역사 행에 대해 곧바로 NOT NULL로
바꾸지 않는다. 그 표에는 source record를 만들지 않은 bounded QA 실행이 이미 있고, 임의 UUID를
backfill하면 결측 provenance가 거짓 provenance로 바뀐다.

대신 다음을 동시에 강제한다.

1. 새 production-capable 미러 writer는 run과 그 run의 모든 source row에 같은 non-null
   `source_record_id`와 `source_file_asset_id`를 기록한다.
2. `parcel_publication_source_evidence`의 두 값은 non-null이고, 지명한 run 및 Iceberg execution
   evidence가 가리키는 값과 같아야 한다.
3. 값이 null인 기존 run은 그대로 보존하며 evidence sealer가 거부한다. 수동 보정이나 추정 backfill은
   없다.
4. 과거 run을 재사용해야 한다면 같은 Iceberg snapshot에서 새 run을 append하고 그 새 run을 봉인한다.

따라서 답은 **물리 컬럼 전체에는 nullable 유지, 발행 가능성에는 필수**다. nullable은 legacy/QA의
정직한 결측 표현이고 운영 발행의 우회로가 아니다. 마이그레이션은 기존 행을 UPDATE하지 않고 새
evidence 및 새 run만 쌓는다.

검사는 `legacy_mirror_run_without_source_record_cannot_be_sealed`가 맡는다.

### 4. 운영 발행 적격성은 봉인 성공 자체이며 boolean 하나가 아니다

publisher는 QA/production을 해석하지 않는다. upstream evidence sealer가 다음 조건을 모두 실제
데이터로 검증한 뒤에만 `parcel_publication_source_evidence`를 append한다.

1. Iceberg catalog에서 `silver.parcel_boundaries`의 table UUID와 positive snapshot id가 실제로
   존재하고, 그 snapshot의 metadata/manifest evidence가 R2의 create-only object와 SHA-256으로
   봉인돼 있다.
2. 실행 evidence가 전체 production scope를 처리했고 partial object/row limit을 쓰지 않았으며,
   실제 Iceberg commit과 선택된 source record/file asset을 지명한다.
3. 실행 evidence가 production cutover와 national rollout을 허용하고, 운영자 승인 기록이 있더라도
   그것만으로 1·2를 대신하지 않는다.
4. 미러 source rows는 run id를 키에 포함한 append-only row set이다. 현행 bounded QA 커맨드의
   전량 교체 방식으로 만든 현재 표는 과거 run의 row set을 보존하지 않으므로 production evidence로
   봉인하지 않는다.
5. §5의 quality와 §6의 count/digest를 통과한다.

현재 `silver_gold_national_promotion_execution.v1`은 이 판단의 **negative evidence**로 연결한다.
그 schema가 기록하는 production/national 금지와 Iceberg/Gold 미작성 limitation 중 하나라도 있으면
봉인을 거부한다. 이 파일의 false를 true로 바꾸는 수동 override는 없다. 실제 Iceberg commit을
기록하는 후속 schema가 위 조건을 충족해야 하며, 기존 v1을 운영 증거로 재해석하지 않는다.

generic runtime promote gate도 parcels load가 지명한 evidence가 존재하고 봉인된 것인지 확인한다.
따라서 succeeded load만 보는 현재 gate 뒤로 QA projection이 빠져나갈 수 없다. 현재 생산자는 이
조건을 만족하지 않으므로 **이 ADR 직후 발행 가능한 기존 run은 0개**다.

검사는 `bounded_qa_execution_cannot_be_sealed_for_publication`과
`runtime_promote_rejects_parcel_load_without_sealed_evidence`가 맡는다.

### 5. 품질 보고는 versioned complete object이고 모든 결함 count가 0이어야 한다

publishable run의 `quality_report`는 빈 object나 best-effort 부가정보가 아니다. 정확한 schema version과
다음 필드를 모두 가진 typed object여야 한다.

| 필드 | 통과 조건 |
|---|---|
| `object_count` | 양수 |
| `expected_row_count` | 양수이며 run/source 실제 행 수와 같음 |
| `loaded_row_count` | `expected_row_count`와 같음 |
| `invalid_srid_count` | 0 |
| `invalid_geometry_count` | 0 |
| `empty_geometry_count` | 0 |
| `nonpositive_area_count` | 0 |
| `source_srid` | `EPSG:4326` |
| `target_srid` | `EPSG:5179` |
| `geometry_repair_strategy` | versioned allow-list에 등록된 정확한 값 |

run의 `status`는 `succeeded`, `finished_at`은 non-null, `loaded_row_count`는 양수,
`rejected_row_count`는 0이어야 한다. JSON 숫자는 정수·nonnegative여야 하고 required field가 없거나
schema version을 모르면 거부한다. 빈 object는 “결함 0”이 아니라 **측정하지 않음**이므로 거부한다.

보고서의 주장을 그대로 신뢰하지 않는다. sealer와 publisher가 실제 run-scoped source row count,
SRID, validity, empty, area, §6 digest를 다시 계산해 report/evidence와 대조한다. 현재 성공 픽스처의
빈 object는 이 결정에 따라 complete report로 바뀌어야 한다.

검사는 `empty_quality_report_is_not_publication_evidence`와 각 required field를 하나씩 삭제하거나
nonzero로 만드는 거부 표가 맡는다.

### 6. 내용 동일성은 EPSG:5179 EWKB의 ordered PNU digest로 증명한다

필지 서빙 content의 canonical 비교값은 `parcel-projection-content-sha256-v1`이다. 행마다 다음 bytes를
만든 뒤 PNU byte order로 정렬하고, version domain prefix와 전체 record stream에 SHA-256을 적용한다.

```text
record = ASCII(PNU 19 bytes) || 0x00 || SHA256(ST_AsEWKB(geom, 'NDR')) 32 bytes
set_digest = SHA256("perfectory.parcel-projection-content.v1\0" || record...)
```

`geom`은 실제 저장된 2D MultiPolygon EPSG:5179다. `ST_AsEWKB`는 SRID를 bytes에 포함하고 NDR을
명시해 host endian 차이를 없앤다. 도형의 공간적 포함관계나 “모양이 비슷함”은 어떤 판정에도 쓰지
않는다. source mirror와 target publication이 같은 projection bytes인지 판단하는 digest다.

기존 `geometry_checksum_sha256`은 EPSG:4326 source WKB lineage로 유지하고 이 digest에 사용하지 않는다.
그 값을 EPSG:5179 checksum이라고 재해석하면 과거 행의 의미를 덮어쓰게 되고, 문자열만 복사한 target
geometry의 동일성을 증명하지 못한다.

evidence sealer는 run-scoped source count와 set digest를 봉인한다. publisher는 materialisation
transaction에서 지명한 source row set의 count/digest를 다시 계산해 evidence와 비교하고, insert 뒤
새 load의 target count/digest를 같은 dialect로 계산해 둘 다 같을 때만 succeeded로 닫는다. row의
revision/snapshot/source record 및 source object key 일치는 digest와 별도의 postcondition으로 검사한다.
행 수가 같아도 geometry 한 개가 달라지면 실패한다.

digest dialect는 PostgreSQL `string_agg`의 메모리 사용이나 한 SQL 구현에 결박하지 않는다. 위 byte
stream을 순서대로 읽어 application에서 streaming SHA-256을 계산해도 같은 값이어야 한다. dialect
test vector 하나를 Rust와 PostgreSQL 양쪽이 공유해 두 번째 구현을 막는다.

검사는 `same_count_with_changed_5179_geometry_has_different_content_digest`와
`source_evidence_and_projection_load_have_the_same_content_digest`가 맡는다.

## 기각한 대안

### `publication_revision`의 숫자 제약을 namespaced 문자열 전체로 넓힌다

마이그레이션은 작아 보이지만 numeric snapshot ordering을 쓰는 manifest/promotion 계약을 모두
바꿔야 한다. `123`과 `iceberg:123`이 UNIQUE에서 다른 값이 되어 같은 사실을 두 번 기록할 수도 있고,
현재 미러 제약은 Iceberg snapshot이 아닌 임의 토큰도 허용한다. 표기를 넓히는 것은 결박을 만들지
않고 애매함만 하류로 보낸다.

### `iceberg:` 뒤 숫자만 잘라 publisher가 사용한다

비용이 가장 작고 두 문자열은 맞출 수 있다. 그러나 operator가 만든 label이 실제 Iceberg table의
snapshot인지, 어느 table UUID의 것인지, R2 metadata가 존재하는지는 증명하지 못한다. 숫자 추출은
§1 evidence sealer 내부의 normalization 단계로만 사용하고 독립된 provenance 판정으로는 기각한다.

### 범용 snapshot alias registry를 먼저 만든다

여러 catalog/table의 임의 alias를 하나의 canonical id에 매핑할 수 있고 table 재생성도 명시적으로
표현한다. 반면 현재 발행 대상은 고정된 `silver.parcel_boundaries` 하나이고, alias가 필요한 요구도
없다. mapping row를 운영자가 넣는다면 지금의 operator assertion을 한 표 옮길 뿐이다. Iceberg
metadata를 직접 검증하는 parcel evidence ledger로 필요한 보장만 얻고, 두 번째 table이 실제로 같은
문제를 가질 때 일반화한다.

### 기존 mirror rebuild run에 eligibility boolean과 모든 필드를 추가한다

표 하나를 아낀다. 하지만 run은 실행 중 상태 전이를 기록하는 operational ledger이고, 이미 nullable
legacy/QA 행과 mutable quality JSON을 가진다. 여기에 publication authority까지 넣으면 “실행됐다”와
“운영 발행할 수 있다”가 다시 같은 표의 느슨한 상태가 된다. terminal run을 검증해 별도 immutable
evidence를 append하는 경계가 두 책임을 분리한다.

### publisher가 R2 handoff 또는 Iceberg를 다시 읽어 provenance를 만든다

Postgres mirror를 거치지 않아 출처는 가까워진다. 대신 기존 mirror writer의 object fetch, row
contract, geometry repair, EPSG:5179 변환을 두 번째로 구현한다. 같은 source snapshot에 두 변환 답이
생기므로 SSOT 위반이다. production mirror writer가 exact Iceberg snapshot을 읽고 append-only row set과
evidence를 만드는 한 경로를 공유한다.

### count 또는 기존 per-row checksum 문자열만 비교한다

count는 한 geometry를 다른 geometry로 바꿔도 같다. 기존 checksum은 EPSG:4326 bytes의 lineage이고
EPSG:5179 target geometry에서 다시 계산한 값이 아니다. 둘 다 review에서 지적된 same-N stale copy를
막지 못하므로 §6의 projection-byte digest를 사용한다.

### 운영자 allow-list 또는 승인 문구만으로 QA를 production으로 올린다

승인 주체의 기록은 필요할 수 있지만 실제 Iceberg commit, full scope, 품질, content identity를 만들지
않는다. 사람의 주장은 기계 evidence의 필요조건을 대체할 수 없다.

## Consequences

- 현재 bounded QA mirror와 `silver_gold_national_promotion_execution.v1`만으로는 필지 발행이
  불가능하다. 이는 회귀가 아니라 기존 증거가 말하던 한계를 publisher도 그대로 읽게 된 결과다.
- 후속 schema 변경은 append-only evidence ledger, terminal run immutability, run-keyed source rows,
  load→evidence FK/gate를 한 수직 경로로 먼저 구현해야 한다. 기존 run/publication row를 고쳐 쓰지 않는다.
- publisher는 여섯 환경변수에서 세 환경변수로 줄고 provenance 선택은 네 값에서 evidence id 하나로
  줄어든다.
- 빈 quality fixture는 더 이상 성공 fixture가 아니다. complete quality와 same-count/different-content
  거부 test가 필수다.
- 기존 `(publication_unit_id, canonical_iceberg_snapshot_id)` UNIQUE와 numeric manifest ordering은
  유지된다. 같은 snapshot의 재materialisation은 revision 복제가 아니라 새 load로 쌓인다.
- digest는 서빙 projection의 byte identity이며 R2/Iceberg를 새 정본으로 대체하지 않는다. evidence의
  table UUID/snapshot/source asset 연결이 정본 관계를 소유한다.
- 이 ADR은 [ADR-0006](./0006-object-storage-first-serving.md)이나
  [ADR-0016](./0016-a-postgis-projection-load-is-a-fact-with-an-identity.md)을 supersede하지 않고,
  parcels 발행에서 두 결정을 함께 만족시키는 출처 경계를 구체화한다.

## 확신이 낮은 구현 선택

- [ADR-0026](./0026-parcel-evidence-sealer-is-the-only-append-boundary.md)은 production execution
  evidence를 별도 `parcel_publication_execution_evidence.v1` strict schema로 구현했다. 현행
  `silver_gold_national_promotion_execution.v1`의 뜻은 바꾸지 않는다.
- 전국 digest는 application streaming으로 구현했다. PNU/EWKB ordered stream을 일정 메모리로
  읽으며 §6의 bytes를 그대로 계산한다.
- execution evidence production writer의 전용 credential/create-only object 경계는 아직 구현되지
  않았다. 이 신뢰 부채의 현재 끝은 ADR-0026에 기록한다.

## References

- [Apache Iceberg table specification — snapshots and metadata](https://iceberg.apache.org/spec/)
- [PostGIS `ST_AsEWKB` — SRID를 포함한 endian 지정 binary](https://postgis.net/docs/ST_AsEWKB.html)
- [PostgreSQL aggregate ordering](https://www.postgresql.org/docs/current/functions-aggregate.html)
- [PostgreSQL `pgcrypto` digest](https://www.postgresql.org/docs/current/pgcrypto.html)
- [FP-ADR-0025 Bronze Catalog 복구 증거 봉인](../../platforms/foundation-platform/docs/adr/0025-bronze-catalog-recovery-evidence-sealing.md) — create-only R2 evidence + SHA-256 선례
