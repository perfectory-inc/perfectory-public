# ADR 0163: 큰 법정동 묶음은 한 번에 읽히는 크기의 조각으로 나눈다

- Status: Accepted
- Date: 2026-10-09
- Builds on: [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md)(법정동 묶음),
  [ADR-0151](./0151-building-packs-serve-each-document-whole.md)(문서 통째),
  [ADR-0154](./0154-building-pack-read-path-is-placed-beside-the-bucket.md)(읽기 경로 측정)
- Supersedes, when generation 2 of both lanes is published: the latency waiver of
  [ADR-0162](./0162-a-refused-latency-gate-opens-only-by-a-pinned-owner-waiver.md) (parcel g1)

## Context

Worker 는 묶음이 `read_path.whole_pack_max_bytes`(256 KiB) 이하면 GET 한 번으로 통째 읽는다. 그보다 크면 머리(목차)를
읽고 문서 범위를 한 번 더 읽는다. 처음 읽기가 R2 왕복 두 번이 된다.

- 필지 g1(2026-10-08): 묶음 18,823개, 중앙값 1.67 MB, 256 KiB 이하 3%. 관문 (나)에서 처음 읽기가 객체보다 p50 +214 ms,
  p95 +206 ms 였다. 기준은 +50 이다. 사장 결정으로 면제하고 발행했다(ADR-0162).
- 건물 g1: 중앙값 111 KB, 81%가 256 KiB 이하. 큰 아파트 동은 14 MB 까지 간다. 그 동네의 처음 읽기는 같은 이유로
  느리다.
- 법정동마다 크기가 수백 배 다르다. 그래서 법정동이 아닌 다른 고정 단위(PNU 앞자리)로 나눠도 크기가 고르지 않다.
- 묶음 위치를 Worker 가 알아야 한다. 요청마다 다시 읽는 manifest 에 법정동 1.9만 개의 조각 수를 넣으면, 요청마다 수백
  KB 를 해석하게 된다(`resolvePlan` 은 요청마다 manifest JSON 을 파싱한다).

## Decision

1. **조각.** 굽기는 법정동 하나의 문서 크기 합을 보고 조각 수 K 를 정한다:
   `K = max(1, ceil(합 / part_target_bytes))`. PNU 는 `fnv1a32(PNU 의 ASCII 19자리) mod K` 번째 조각에 들어간다.
   K = 1 인 법정동은 지금과 같은 단위(`{법정동}`)다. K > 1 이면 조각마다 단위 `{법정동}-{조각}` 의 묶음 하나를 쓴다.
   묶음 형식(머리·목차·gzip 멤버)은 바뀌지 않는다.
2. **상한은 거부한다.** 어떤 조각이든 `whole_pack_max_bytes` 를 넘으면 굽기가 그 법정동을 쓰지 않고 실패한다. 해시가
   치우쳐 넘는 일은 `part_target_bytes` 를 상한의 절반으로 두어 막는다.
3. **조각 수의 정본은 세대마다 하나다.** K > 1 인 법정동의 조각 수는 굽기 요약에 남고, 모든 샤드를 처음 모으는 관문 (가) 명령이 섹션 세대마다 조각 목록 객체
   (`{root}/{section}/g{n}/parts.json`, create-only, 바뀌지 않음) 하나에 쓴다. 관문 (나)의 미리보기가 발행 전에
   그 세대를 읽으므로 그 전에 있어야 한다. 발행은 같은 바이트를 다시 써서 확인한다. manifest 는 섹션마다 그 객체의 키와
   sha256 만 갖는다. Worker 는 그 sha256 으로 목록을 한 번 읽어 인스턴스 메모리에 둔다.
4. **패치도 같은 조각을 쓴다.** 패치는 그 섹션 세대의 조각 목록을 따른다. 바뀐 PNU 가 든 조각만 패치 묶음으로 쓴다.
   조각 수를 바꾸는 일은 새 세대(전체 굽기)다.
5. **해시는 한 정의다.** `fnv1a32` 는 Rust(`r2_layout::by_pnu_packs`)와 Worker(`packs.ts`)에 각각 한 번 구현한다. 둘 다
   계약의 시험 벡터(`by_pnu_section_packs.parts.hash_test_vectors`)로 시험한다.
6. **계약 값.** `by_pnu_section_packs.parts` 에 `part_target_bytes`(131072, 상한 256 KiB 의 절반), 해시 이름, 시험
   벡터, 조각 목록 객체 이름을 둔다. manifest 의 `section_packs` schema_version 은 4 다. Worker 는 4 를 읽을 수
   있다고 `_capabilities` 에서 먼저 밝힌다. 발행은 그 전에 거부한다(지금 v2 와 같은 순서 규칙).

## Formats

The values below are the contract's (`by_pnu_section_packs.parts`, `manifest_section_packs_parted_schema_version`);
this section names their shapes.

- **Unit:** `{dong}` (10 digits) for a dong of one part, `{dong}-{part}` (part `0..K-1`, decimal, no padding) otherwise.
  Every unit matches `parts.unit_pattern`. The pack key stays `{root}/{section}/g{n}[/p{patch}]/{unit}{suffix}`.
- **Part of a PNU:** `fnv1a32(pnu) mod K`, K = the dong's count in the generation's parts index, 1 when the index does not
  name the dong.
- **Parts index** (`{root}/{section}/g{n}/parts.json`, create-only):
  ```json
  {"schema_version": "foundation-platform.by_pnu_pack_parts.v1", "unit": "parcel-by-pnu", "section": "documents",
   "generation": 2, "hash": "fnv1a32", "parts": {"<dong>": <K>, ...}}
  ```
  `parts` holds only dongs with K > 1, keys sorted.
- **Manifest** (`section_packs`, schema version 4 when any section names an index): each section entry may carry
  `"parts": {"key": "<index key>", "sha256": "<hex of the index bytes>", "parted_units": <dongs in it>}`. A section
  without `parts` is unparted. A reader refuses an index whose bytes do not hash to `sha256`.
- **Patch** `units`: the units (parted or not) the patch wrote, sorted.

## Consequences

- 모든 처음 읽기가 GET 한 번(256 KiB 이하)이 된다. 필지와 건물을 2세대로 다시 굽고 관문 (가)·(나)를 다시 통과해야
  한다. 통과하면 ADR-0162 의 필지 g1 면제는 쓰이지 않는다.
- 같은 법정동의 이웃 PNU 가 서로 다른 조각에 흩어진다. 그래서 "같은 동 두 번째 읽기는 메모리에서" 이득이 줄어든다.
  대신 처음 읽기가 GET 한 번이다. 관문 (나)가 처음·다시 읽기를 모두 잰다.
- 객체 수: 필지 약 36.8 GB / 128 KB ≈ 29만 + 작은 법정동. 다시 굽기 R2 쓰기는 수천 원 수준이다. 실행 전 금액을
  승인받는다.
- 다시 굽기 시간은 Gold 를 PNU 순서로 저장하는 일(개선 2)이 크게 줄인다. 그래서 2세대 굽기는 그 뒤에 한다.
