# ADR 0148: 연속지적도 판은 나란히 쌓고, 지번 근거는 변경마다 제 판 쌍으로 읽는다

- Status: Accepted
- Date: 2026-10-05
- Amends: [ADR-0067](./0067-the-parcel-source-covers-the-country-twice.md) (원천 계약에 판을 둔다),
  [ADR-0144](./0144-region-code-changes-are-derived-from-downloaded-data-only.md) §3.2·§4 (지번 단계의 판 쌍을 정하는 법),
  [ADR-0131](./0131-parcel-lineage-binds-every-input-and-derivation-identity.md) (같은 판의 두 철자가 다시 생기지 않게 한다)
- Related: [ADR-0145](./0145-one-source-of-truth-for-region-code-changes.md), [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md), [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)

## Context

2026-10-05 실측.

| 사실 | 값 |
| --- | --- |
| `silver.parcel_boundaries` 의 판 | `vworldkr__parcel-202606` 하나, 39,861,511 행, `valid_from_utc` 2026-06-01 |
| Bronze 의 다음 판 | 같은 접두사의 `30563-N--sha256-<내용 해시>.zip` 272 개. ZIP 중앙 디렉터리를 범위 요청으로 읽은 결과 전부 `LSMD_CONT_LDREG_<코드>_202609` (시도 16 + 시군구 256), 멤버 날짜 2026-09-13~16. 키의 해시 272/272 가 `catalog.bronze_object` 의 checksum 과 같다. 원천 계약은 이 판을 몰랐다 |
| 손으로 만든 9월 핸드오프 | `silver-handoff/vworldkr__parcel/` 아래 256 개, `vworldkr__parcel:202609`(콜론), `valid_from_utc` = 수집 시각 2026-09-27T00:54:42Z |
| `silver.parcel_boundaries_202609_smoke` | 12,352 행, 한 시군구, 2026-10-01T18:04Z 의 수동 Silver 실행이 위 핸드오프 하나로 썼다 |
| 판 id 를 정하던 곳 | 변환 실행마다 손으로 넘기는 `VWORLD_PARCEL_SOURCE_SNAPSHOT_ID`, `VWORLD_PARCEL_VALID_FROM_UTC` |
| 법정동 짝 맞추기의 지번 단계 | 전국에 한 쌍(`LEGAL_DONG_PARCELS_BEFORE/AFTER`). 2026-01·03·07·09 의 개편이 각자 다른 쌍을 필요로 한다 |
| 제공자 (MK/30563) | 현재 판 하나만 준다(276 파일: 2026-09 판 272, 폐지된 인천 구의 2026-06 파일 3, 칸 정의서 1). 같은 파일 번호를 판마다 다시 쓴다 |
| 과거 판 | NA/23(일별 연속지적도형정보)에 시도 단위 전체판 11 개(2025-11-04 ~ 2026-09-08)와 일 변동분이 있다 |

결함은 넷이다. (1) 판이 계약에 없어 적재기가 어느 객체가 어느 판인지 모른다. (2) 판 id 와 유효 시작을 사람이 적어
같은 데이터가 두 철자로 갈린다(ADR-0131 이 이미 겪었다). (3) 지번 단계가 개편마다 다른 판 쌍을 쓸 수 없다. (4) VWorld
수집의 재개 건너뛰기가 파일 번호만 보아, 같은 번호의 새 판을 이미 가진 것으로 본다.

## Decision

1. **원천 계약이 판을 가진다.** `vworld-parcel-source-objects.json` v2 는 `editions{판: {provider_base_month,
   extracted_on{earliest, latest}, handoff_prefix, granularity_counts, objects}}` 와 `served_edition` 을 갖는다.
   판마다 핸드오프 자리가 다르다. 지도·카탈로그 투영·PostGIS 거울·필지 패널 Gold·시군구 대응표의 지적 시도는
   `served_edition` 하나만 읽는다(Rust `vworld_parcel_source_contract::served_edition_view`, Python
   `vworld_parcel_editions`). 2026-10-05 현재 `served_edition` 은 202606 이다.
2. **판에서 파생되는 값은 한 곳에서 만든다.** `source_snapshot_id = snapshot_id_prefix + 판`(`vworldkr__parcel-202609`),
   `valid_from_utc` = 기준월 첫 순간. 변환기·적재기는 `VWORLD_PARCEL_EDITION` 하나만 받고, 예전 세 변수를 받으면
   거부한다. Silver 잡은 `--expected-source-snapshot-id` 가 계약의 판 id 여야 하고(콜론 철자 거부), 그 id 가 아닌 행을
   가진 묶음을 쓰기 전에 거부한다.
3. **판은 나란히 쌓는다(append-only).** `silver.parcel_boundaries` 에서 판을 고르는 것은 `source_snapshot_id` 이다.
   `valid_to_utc` 로 앞 판을 닫지 않는다. 표 전체를 "현재 행"으로 읽던 패널 Gold 는 `served_edition` 으로 거른다.
   손으로 만든 9월 핸드오프와 smoke 표는 지우지 않고, 적재 경로가 읽지 않는다.
4. **지번 단계는 변경마다 제 판 쌍을 읽는다.** 폐지된 동마다 계약에서 폐지일 전에 다 뽑힌 마지막 판과 뒤에 다 뽑힌
   첫 판(`bracketing`, 추출일이 걸치면 어느 쪽도 아님)을 고른다. 계약에 그 판이 없거나 표에 적재되지 않았으면
   `awaiting_data` 이고 필요한 판을 이름으로 말한다. 적재되지 않은 판을 "필지 없음"으로 읽지 않는다. 뒤 판은 폐지된
   동이 가졌던 지번만 읽는다(결과가 같음을 시험이 보인다). `LEGAL_DONG_PARCELS_BEFORE/AFTER` 는 없앤다.
5. **재개 건너뛰기는 판을 본다.** VWorld 데이터 파일은 보관한 객체의 `provider_updated_at` 이 목록의 갱신일과 같을
   때만 건너뛴다. 목록에 갱신일이 없으면 예전처럼 파일 번호가 정한다.
6. **새 판은 매일 확인하고, 수집·측정·제안까지 자동, 계약 반영과 적재는 사람.** 작업 `vworld_parcel_edition`
   (default_pool, Spark 없음, 꺼진 채 출시)이 제공자 최신 기준월을 계약과 비교하고, 새 판이면 바로 내려받는 파일을
   모두 받아 ZIP 을 측정해 계약 항목을 제안하고 3 으로 끝난다. 시도 큰 파일 여섯은 제공자 도구(RAON) 전용이라 받지
   않는다. 그래서 계약 검사는 "시도 벌 = 시군구 벌"에서 "시도 객체가 있는 곳에 시군구 객체가 있다"로 바뀐다.
   Silver 적재는 spark 풀에 자리가 없어(ADR-0138) 작업에 넣지 않는다.
7. **가드.** `the-contract-names-where-its-objects-live.sh` 는 계약이 선언한 모든 판의 핸드오프 자리를 찾는다(첫 번째
   하나만 보던 것을 자체 시험이 거부로 보인다).

## Consequences

- 202609 적재는 병합·배포 뒤 릴리스에서 런북(`platforms/foundation-platform/docs/runbooks/vworld-parcel-editions.md`)의
  명령으로 한다. 202606 실측 기준 변환 약 40 분, 적재 약 20 분(드라이버 16g).
- 적재 뒤 인천 83 코드는 202606→202609 로 정해진다. 경기 219·충북 13 은 앞선 판(NA/23)이 필요하고, 대구 16 은
  202610 이후 판이 필요하다. 그때까지 판단 대기로 남고 이유가 판을 말한다.
- 지도를 202609 로 옮기는 것(`served_edition`)은 굽기·검증을 거치는 별도 결정이다.
- NA/23 의 과거 판을 받는 것은 소유자 승인 뒤의 일이고, 칸 정의가 달라 적재 경로를 따로 확인해야 한다.
