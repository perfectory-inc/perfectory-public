# ADR 0046: 수집된 파일에서 온 발행은 그 수집 기록을 이름한다

- Status: Accepted
- Date: 2026-08-22
- 관련: [ADR-0040 아무도 채우지 않는 컬럼은 필수일 수 없다](./0040-a-column-no-producer-fills-cannot-be-required.md),
  [ADR-0043 정본 id 는 다시 계산하지 않고 읽는다](./0043-a-canonical-id-is-read-not-recomputed.md),
  [ADR-0044 사실의 이름을 단 컬럼은 그 사실을 담아야 한다](./0044-a-column-named-for-a-fact-must-hold-that-fact.md),
  [ADR-0045 서빙 투영의 행은 리비전이 아니라 적재를 이름한다](./0045-a-serving-projection-row-names-its-load-not-its-revision.md),
  FP-ADR-0016 (Bronze 커밋 프로토콜), FP-ADR-0017 (Bronze 수집 프로토콜)

## Context

산업단지 경계 1,343행을 PostGIS 로 발행하려 하자 `publish-industrial-complex-boundary-postgis`
가 거부했다.

```
Error: the industrial complex boundary source_record does not exist
```

2026-08-21 로컬 실서비스 DB 실측:

```
catalog.bronze_object        10,867행
  source_record_id 채워짐          0행
catalog.source_record             0행
```

**수집한 10,867개 전부가 `catalog.source_record` 와 끊겨 있다.** 산업단지만의 문제가 아니다.

끊긴 것이 아니라 애초에 이어진 적이 없다. 저장소 전체에서 `catalog.source_record` 에 INSERT 하는
생산 코드는 셋뿐이고, 셋 다 **수집기가 아니다.**

| 자리 | 무엇을 적는가 |
|---|---|
| `catalog-infrastructure/src/unit_of_work.rs` `insert_vector_tile_source_record_tx` | v1 타일 매니페스트 승격이 운영자에게 받은 출처 서술 |
| `lakehouse-infrastructure/src/gold_publication/transaction.rs` `insert_source_record` | Gold 포인터 발행. `raw_object_key` 와 `checksum_sha256` 은 둘 다 `NULL` 을 바인딩한다 |
| `20260727000001` · `20260809000001` | `source = 'foundation.migration'`, `checksum = repeat('0', 64)` 인 레거시 다리 |

반면 FP-ADR-0016 은 `BronzeCommitter` 가 **항상** `catalog.bronze_object` 행을 기록한다고
못박는다("async writes `bronze_object` (option a). The committer ALWAYS records the DB row").
FP-ADR-0016 과 FP-ADR-0017 은 `catalog.source_record` 를 한 번도 언급하지 않는다.
`catalog.bronze_object.source_record_id` 는 nullable 이고 **쓰는 코드가 저장소에 없다** — 외래키만
있다.

그리고 레이크하우스는 이미 답을 정해 두었다. `industrial_complex_boundary_silver_export` 는
Silver 의 `source_record_id` 를 **Bronze 객체 키**로 채운다(`bronze_object_key.as_str()`). 즉
내보낸 1,343행 전부가 `catalog.bronze_object` 에 있고 `catalog.source_record` 는 들어 본 적 없는
객체를 이름하고 있다.

```
object_key       bronze/source=vworldkr__sandan_boundary/30137-1.zip
checksum_sha256  f86332c2...                      ← catalog.bronze_object 에 실재
source_record_id NULL
```

세 번째 사실: 이 커맨드가 요구하던 `SOURCE_RECORD_ID` 는 **운영자가 지어내는 UUID** 였다.
2026-08-19~22 에 이 발행이 세 번 막혔고 그중 한 번이 정확히 그것이었다 — 없는 계보를 만들어
넣으려다 검사에 걸린 것이다. 검사는 옳았다.

## Decision

1. **수집된 파일에서 온 발행의 계보 앵커는 `catalog.bronze_object` 다.**
   마이그레이션 `20260821163536_anchor_a_publication_to_the_object_that_was_collected.sql`.
   `catalog.publication_revision` 은 `source_record_id` 를 nullable 로 내리고
   `bronze_object_id uuid REFERENCES catalog.bronze_object(id)` 를 얻으며,
   `publication_revision_one_provenance_anchor_check` 가
   `num_nonnulls(source_record_id, bronze_object_id) = 1` 을 요구한다. 둘 다 없는 행도, 둘 다 있는
   행도 거부된다 — 후자는 한 판본의 데이터에 대한 두 개의 출처 주장이고, 스키마가 어느 쪽을
   믿으라고 말할 수 없다.

2. **`serving_postgis.industrial_complex_boundary_publication.source_record_id` 를
   `bronze_object_id` 로 이름을 바꾸고 외래키를 옮긴다.** 옆에 새 칸을 만들지 않는다. 그 칸이
   담던 사실은 하나였고("이 폴리곤이 어느 객체에서 왔는가"), ADR-0044 가 컬럼 이름은 그 사실을
   담아야 한다고 정했다. `source_object_key` 는 남는다 — 내보낸 모든 행이 인용하고
   `validate_row` 가 대조하는 값이며, 발행기는 둘이 일치함을 확인한 뒤에야 어느 쪽도 쓴다.

3. **앵커 id 는 운영자가 이름하지 않고 발행기가 읽는다** (ADR-0043 의 규칙을 계보에 적용).
   `FOUNDATION_PLATFORM_INDUSTRIAL_COMPLEX_BOUNDARY_POSTGIS_PUBLISH_SOURCE_RECORD_ID` 는 없어지고,
   발행기는 이미 존재하던 `..._SOURCE_OBJECT_KEY` 로 `catalog.bronze_object` 를 조회한다.
   손이 틀릴 수 있는 별도 식별자가 사라진다.

4. **키는 "어느 객체인가"를, 체크섬은 "같은 객체인가"를 지킨다.** 새 환경변수
   `..._SOURCE_OBJECT_CHECKSUM_SHA256` 이 필수이고, `catalog.bronze_object.checksum_sha256` 과
   같아야 한다. 발행기는 zip 을 열지 않으므로, 산출물을 만든 로컬 파일과 수집기가 커밋한 객체가
   바이트 단위로 다른 경우를 잡을 수 있는 자리는 여기뿐이다. 값은 운영자가 지어내는 것이 아니라
   `sha256sum` 으로 **측정**한다.
   해당 키에 `catalog.bronze_object` 행이 **정확히 하나**여야 한다. 유니크 키는
   `(source_catalog_id, object_key)` 이므로 여러 개가 표현 가능하고, 그중 하나를 고르는 것은
   아무 데서도 잡히지 않는 조용한 선택이 된다. 0개와 똑같이 거부한다.

5. **행정경계 발행기와 필지 발행기는 옮기지 않는다.** 근거는 취향이 아니라 실물이다.
   - 행정경계의 앵커는 `catalog.administrative_boundary_revision.source_record_id` 이고, 그 값은
     `20260727000001` 이 시드한 `source = 'foundation.migration'` 다리다. `raw_object_key` 가
     `NULL` 이고 **뒤에 수집 객체가 없다.** 옮기려면 없는 `bronze_object` 행을 지어내야 하며,
     그것이 이 작업이 금지한 바로 그 행위다. 게다가 같은 `source_record_id` 는 다섯 개
     행정 사실 표의 `NOT NULL` 외래키다.
   - 필지의 앵커는 `catalog.parcel_publication_source_evidence.source_record_id` 이고
     [ADR-0026](./0026-parcel-evidence-sealer-is-the-only-append-boundary.md)·
     [ADR-0029](./0029-parcel-publication-evidence-is-written-from-the-terminal-run.md)·
     [ADR-0030](./0030-parcel-publication-evidence-requires-two-distinct-approvals.md) 이 봉인한
     경로다. `20260811000001` 의 트리거가 `revision.source_record_id` 와 대조한다.

   두 발행기가 서로 다른 규칙을 갖는 이유가 이것이고, 결정 1의 배타적 선택지가 그것을 스키마에
   적어 둔 형태다.

6. **승격은 같은 객체를 대조하되, release 의 계보 기록은 그대로 둔다.**
   `promote-industrial-complex-boundary-runtime` 은 새 환경변수 `..._BRONZE_OBJECT_ID` 로
   revision 의 앵커를 대조한다(`RevisionLedger::PublicationRevisionOnBronzeObject`).
   `..._SOURCE_RECORD_ID` 는 남는다 — `catalog.vector_tile_release.source_record_id` 는
   `NOT NULL` 이고, 그 값은 런타임 매니페스트 응답
   (`VectorTileRuntimeLineageResponse.source_record_id`)의 공개 계약이다. 두 값은 서로 다른
   사실이다: 하나는 폴리곤이 나온 파일, 다른 하나는 승격이 만드는 release 를 서술하는 기록이다.

### 기각한 대안

- **(B) 수집기가 `catalog.source_record` 도 만들게 한다.** `source`·`raw_object_key`·
  `checksum_sha256`·`captured_at` 은 `catalog.bronze_object` 가 이미 담는 값이므로, 이는 같은
  사실의 두 번째 사본을 영구히 만드는 것이다. 루트 AGENTS.md 의 두 번째 원칙("같은 지식이 두
  곳에 복제되면 그 자체가 결함이다")에 정면으로 어긋나고, 10,867개 소급은 수집 시점에 없던
  사실을 오늘 지어내는 일이다.
- **`catalog.publication_revision.source_record_id` 를 통째로 `bronze_object` 로 재지정.**
  결정 5의 두 경로가 즉시 깨진다. 되살리려면 레거시 다리와 봉인된 필지 증거에 대해 수집
  객체를 합성해야 한다.
- **`catalog.vector_tile_release` 까지 같은 배타적 선택지로 확장.**
  `RuntimeTileLineage.source_record_id` → `VectorTileRuntimeLineageResponse.source_record_id` 가
  공개 응답 계약이므로 `Option` 화는 앱이 읽는 계약을 바꾼다. 이 결정의 범위 밖이며,
  Consequences 에 남은 일로 적는다.
- **발행기가 `bronze_object` 를 조회한 결과를 `source_record_id` 컬럼에 넣기.** ADR-0044 위반.
  이름이 `source_record_id` 인 칸에 bronze 객체 id 가 들어가면, 그 칸을 읽는 모든 조인이 조용히
  틀린다.

## Consequences

- `publish-industrial-complex-boundary-postgis` 를 돌리기 위해 손으로 만들어야 할 행이 **없다.**
  수집이 이미 남긴 행을 가리키면 된다.
- 운영자 인자가 하나 줄고(`SOURCE_RECORD_ID`) 하나 는다(`SOURCE_OBJECT_CHECKSUM_SHA256`).
  줄어든 쪽은 지어내던 값이고 늘어난 쪽은 측정하는 값이다.
- `scripts/tiles/industrial-complex-boundary-fixture.sql` 은 이제 수집 쪽을 통째로 시드한다 —
  `source_catalog` → `ingestion_run` → `bronze_object`. `bronze_object` 는 그 둘 없이는 존재할 수
  없으므로, 그것들 없이 시드한 행은 수집의 모양만 흉내낸 행이 된다.
- **남은 일 (1):** `catalog.vector_tile_release.source_record_id` 는 외래키가 없는 `NOT NULL`
  uuid 이고, 승격기가 `catalog.publication_revision` 의 앵커와 같아야 한다고 강제하던 값이다 —
  즉 사본이었다. `complex` 단위에서는 이제 둘이 다른 종류의 사실이 되었으므로 그 대조가 없어졌다.
  이 칸을 revision 을 지나 조인으로 도달하게 만드는 것이 다음 결정이고, 런타임 매니페스트 응답
  계약을 함께 바꾸어야 한다.
- **남은 일 (2):** `dynamic_postgis` release 는 `catalog.file_asset` 을 요구하지만 그런 release 에는
  산출 파일이 없다. ADR-0040 이 이름한 부류의 결함이며 이 결정과 별개다.
- **남은 일 (3):** `catalog.bronze_object.source_record_id` 는 쓰는 코드가 없다. 그 칸이 남아 있는
  한 다음 사람이 이 간극을 (B) 로 "고치려" 한다. 제거는 별도 결정으로 남긴다.
