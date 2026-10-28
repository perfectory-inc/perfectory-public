# ADR 0326: 변경을 찾는 데 전량보다 많이 읽는 Gold 재구축은 처음부터 전량으로 하고, 출처 위치는 변경으로 세지 않는다

- Status: Accepted
- Date: 2026-10-29
- Amends: [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md) §3(전량으로 넘어가는 때),
  [ADR-0189](./0189-an-appended-silver-input-is-its-own-change-set.md) §3(읽는 배치가 바뀌면 두 배치를 다 매핑)

## Context

2026-10-28 `foundation-gold-panel-rebuild.service` 가 15:23Z 에 시작해 18:03Z 에 `result 'timeout'` 으로 죽었다
(시간 상한 160분, `orchestration/jobs.v1.json` `gold_panel_rebuild`). 18:07Z 의 다음 실행도 같은 길을 갔다.
`gold.building_panel` 의 마지막 커밋은 10-20 08:44Z 다. 아래는 ai-server 에서 읽기만 해서 잰 것이다.

실행 로그(`/var/lib/foundation-gold-panel-rebuild/building/runs/20261028T152351Z/building_panel_silver_to_gold.py.log`):

```
gold-incremental-phase affected seconds=7016.8
gold-incremental-fallback 5946011 of 5945767 PNUs have changed inputs, more than the max_changed_key_fraction 0.3
```

증분 경로가 117분 동안 "무엇이 바뀌었나" 를 계산한 끝에 **모든 PNU** 가 바뀌었다고 결론 내고 전량으로 넘어갔다.
남은 43분은 전량 재구축(10-20 실측 3,425초 = 57분)에 모자랐다. 두 번째 실행의 Spark UI(`:4040/api/v1`)에서 같은
단계가 같은 행을 읽는 것을 확인했다.

**1. 변경을 찾는 데 전량보다 많이 읽었다.** 입력 스냅숏 summary(`total-records`·`added-records`)로 센 행:

| 입력 | 고정 판 → 새 판 | 변경을 찾으며 읽는 행 | 전량이 읽는 행 |
|---|---|---:|---:|
| `silver.building_register_titles` | 통째 overwrite | 16,128,760 | 8,064,354 |
| `silver.building_register_units` | 통째 overwrite | 39,708,057 | 19,860,453 |
| `silver.building_register_unit_areas` | 통째 overwrite | 228,830,213 | 114,461,830 |
| `silver.building_register_floors` | append 1개(새 층 세대) | 21,189,570 | 42,380,380 |
| `silver.unit_official_price` | append + compaction(`replace`) | 2,036,857,700 | 1,207,165,362 |
| **합** | | **2,342,714,300** | **1,391,932,379** |

Spark UI 의 단계별 입력 행이 이 수와 맞는다(면적 228,830,213, 가격 표 1,207,165,362 를 두 번). 변경 집합을 다
찾기도 전에 전량이 읽을 행의 1.7배를 읽는다. 무엇을 찾든 병합이 더 싼 커밋일 수 없었다. 그런데 ADR-0180 은
`max_changed_key_fraction` 을 **바뀐 PNU 를 다 센 뒤에야** 비교한다.

**2. 거의 바뀌지 않았는데 "모두 바뀜" 이 됐다.** Trino 로 내용만 비교했다:

| 입력 | 전체 | 내용이 바뀐 것 |
|---|---:|---:|
| 층(10-20 새 세대 대 09-29 세대), `floor_row_id` 빼고 | 행 21,189,570 · 대장 8,039,312 | 행 29,754 · 대장 13,074 |
| 전유공용면적(09 판 대 10 판), `area_row_id` 빼고 | 대장 19,859,303 | 대장 25,114 |

`floor_row_id`·`area_row_id` 는 `building-register-floor:bronze/source=…/<원천 파일>.zip#line-<줄>`
처럼 **원천 파일 이름과 줄 번호**다(`building_register_row_identity.rs`). 매달 새 파일이 오면 모든 행의 id 가
바뀐다. 건물 패널은 이 둘을 비교 열(`BUILD_COLUMNS`)에 두었으므로 매달 모든 행이 "바뀐 행" 이다. ADR-0180 §1 은
"계보 열은 비교에서 뺀다" 고 했지만 이 둘은 이름만 id 인 계보다. 게다가 ADR-0189 §3 은 읽는 배치가 바뀌면(새 층 세대,
새 가격 배치) 두 배치의 **모든 행** 을 변경으로 보았다. 층은 8,039,312 개 대장 전부, 곧 모든 PNU 가 된다.

**시간 상한을 올리지 않는 이유.** 160분은 같은 풀의 기아 상한이 정한 값이다. 166분으로 늘리면
`map_edit_fold_admin` 의 대기 상한이 1,201분을 넘는다(`orchestration/tests/test_gold_panel_rebuild.py`). 일은 이미
57분이면 끝나는 양이므로 늘릴 이유도 없다.

### 실사례

- Snowflake dynamic table 의 `REFRESH_MODE = AUTO` 와 Databricks Delta Live Tables 의 Enzyme 은 새로고침마다
  **실행 전에** 비용 모형으로 증분과 완전 재계산 중 하나를 고른다. 바뀐 양을 다 계산해 본 뒤 고르지 않는다.
- Iceberg 는 스냅숏마다 `total-records`·`added-records` 를 summary 에 적는다. 이 판단에 행 데이터를 읽을 필요가 없다.
- dbt·Materialize 의 증분 모델은 원천의 행 키(비즈니스 키)로 변경을 찾는다. 적재 위치(파일·줄)는 키가 아니다.

## Decision

1. **계획 단계가 비용으로 고른다.** `gold_rebuild.incremental_mode` 는 바뀐 입력마다 증분 경로가 변경을 찾으며
   읽을 행(`change_read_rows`)을 스냅숏 summary 로 센다: 고정 판부터 새 판까지 모두 `append` 이면 추가된 행(ADR-0189,
   생산자와 같은 `appended_span`), 아니면 두 판의 `total-records` 합(ADR-0180 §1). 그 합이 모든 입력의 새 판
   `total-records` 합(전량이 읽는 행)보다 크면 계획은 `full` 이고 `mode_reasons` 에 두 수를 적는다
   (`costlier_to_read_the_change`). summary 에 수가 없으면 판단하지 않는다(생산자의 `max_changed_key_fraction` 이
   그대로 있다). 이것은 ADR-0312 의 "전량은 이유와 잰 비용을 적은 예외" 다.
2. **배치가 바뀐 입력은 두 배치의 행 차이만 바뀐 것이다(ADR-0189 §3 개정).** append 는 옛 배치를 건드리지 않으므로
   새 판에서 옛 배치와 새 배치를 각각 읽어 `changed_rows`(키와 지문의 다중집합 차이)를 낸다. PNU 매핑이 읽는 행은
   두 배치의 합이다. 같은 기준연도를 다시 조인한 가격 배치(10-20 15:55Z 와 10-21 07:13Z 에 각 188,736,512 행)가
   가격 있는 모든 PNU 로 번지지 않는다. 층 세대는 아래 `floor_row_id` 가 풀릴 때까지 여전히 모든 행이 다르다.
3. **원천 위치는 비교하지 않는다.** `area_row_id` 를 건물 패널의 `BUILD_COLUMNS` 에서 뺀다. Gold 는 그 값을 내지 않고,
   대표 면적을 고를 때의 마지막 동률 깨기로만 썼는데, (면적, 용도, 구조)가 같은 행은 같은 값을 내므로 출력이 달라지지
   않는다. 같은 행이 두 번 실린 것을 막던 고유성 검사는 빌드가 아니라 입력을 읽을 때(`read_inputs`) 한다.

## Consequences

- 2026-10-28 의 계획은 처음부터 전량이다(위 표의 실제 summary 로 `costlier_to_read_the_change` 를 돌린 결과:
  2,342,714,300 > 1,391,932,379). 계산한 실행 시간: 앞 단계 약 3분 + 건물 전량 57분(10-20 실측) + 그 뒤
  `gold.unit_price` 도 같은 판단으로 전량(약 2,092,694,517 > 1,235,090,169 행)이며, 그 증분 시도는 36–44분 걸렸다.
  합 약 105분, 상한 160분 안. 전에는 117분 + 57분 = 174분으로 넘쳤다.
- 이 규칙은 지금 건물 패널을 거의 늘 전량으로 보낸다. 표제·호·면적이 매달 통째 overwrite 되고 가격 표가 판마다 전체
  사본을 쌓기 때문이다. 그 원인은 Silver 쪽에서 고친다: 바뀐 키만 쌓는 대장(ADR-0318, #914)과 가격 판의 버전화
  (ADR-0317, #911·#919). 그 뒤에는 읽을 행이 바뀐 키 수가 되어 같은 규칙이 증분을 고른다. 규칙을 바꿀 필요가 없다.
- **남은 것: 층의 `floor_row_id`.** 이 id 는 by-PNU 문서로 나간다(`buildings[].floors[].floor_row_id`). 그래서
  비교에서 빼면 Gold 가 핀과 다른 내용을 갖게 되고 패리티 표본이 잡는다. 층 세대가 바뀔 때마다 모든 건물 PNU 의 문서가
  다시 쓰이는 것은 서빙 값이 원천 위치이기 때문이다. 내용 기반의 안정 식별자(대장 + 층 내용의 지문 + 같은 내용 안의
  순번)로 바꾸는 것은 문서 내용이 바뀌는 일이라 by-PNU 골든 팩(`services/foundation-by-pnu-gateway/test/fixtures/
  section-packs`)을 Rust 시험으로 다시 만들어야 한다. 별도 변경으로 한다.
- 증명: `tests/test_gold_incremental.py::PlanMode::test_the_full_rebuild_is_chosen_when_finding_the_change_reads_more_rows`
  (통째 overwrite·append·compaction 낀 append·수 없는 summary), `tests/test_gold_incremental_iceberg.py::
  test_an_appended_price_batch_is_its_own_change_set`(가격 배치 전환은 바뀐 세 PNU, 층 세대 전환은 바뀐 한 PNU,
  매번 병합 결과 = 전량 재구축).
