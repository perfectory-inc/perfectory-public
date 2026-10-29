# ADR 0322: 아직 `full` 인 작업은 각자 주인이 있고, 법정동 수집은 받기 전에 묻는다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md)(작업마다 scope, `full` 은 예외·줄기만 하는 목록), [ADR-0143](./0143-legal-dong-code-changes-come-from-code-go-kr.md)·[ADR-0144](./0144-region-code-changes-are-derived-from-downloaded-data-only.md)(법정동 수집), [ADR-0311](./0311-silver-lanes-overwrite-only-with-a-declared-reason-and-the-compare-commit-is-the-changelog.md)(Silver 비교 적재), [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md)(Gold 증분), [ADR-0316](./0316-parcel-boundaries-are-partitioned-by-edition-and-range-ordered-by-pnu.md)(경계 판 분할·시도별 타일)
- 관련 사건: 2026-10 감사 — 법정동 수집이 58 MB 전체 표를 하루 약 10번 받았고(10-04~28 객체 283개, 15 GB) 표는 그동안 한 번(09-29→10-04, +16행) 바뀌었다.

## Context

ADR-0314 는 38개 작업 모두에 `scope` 를 적게 했고 24개가 `full` 로 남았다(`tools/ci/full-scope-jobs.baseline.json`). 2026-10-28 wave 2 는 이 24개를 각자 고칠 곳에 나눴다. 같은 일을 두 PR 이 하지 않도록, 그리고 남은 것이 무엇인지 한 곳에서 보이도록 여기 적는다.

| 작업 | 주인 | 상태 |
| --- | --- | --- |
| `silver_refresh_land_characteristic`·`land_forest_ledger`·`land_individual_price`·`land_right_registration` | PR #899 (ADR-0311) | 비교 적재 구현됨. 병합 열차가 #903 과 합칠 때 scope 를 `changed-keys` 로 바꾸고 목록을 줄인다 |
| `silver_refresh_building_register_titles`·`units`·`unit_areas`, `land_use_plan`, `land_use_zone_code` | wave 2 V2 (ADR-0318) | 진행 중 |
| `silver_refresh_land_transfer_history`, `building_register_exclusive_unit` (append 레인) | V2 (ADR-0318) | 진행 중 |
| `silver_refresh_building_register_apartment_price` | V1 (ADR-0317, 기준일 SCD2) + V2 | 진행 중 |
| `gold_panel_rebuild`·`gold_kind_rebuild`·`gold_public_auction_rebuild` | PR #902 (ADR-0312) + #896 (ADR-0307) | 구현됨, 병합 대기 |
| `lakehouse_maintenance` | PR #896 (ADR-0307, 정렬 합치기) | 구현됨, 병합 대기 |
| `parcel_tile_bake` | PR #897 (시도별) + V7 (ADR-0323, ADR-0111 base+patch) | 진행 중 |
| `court_auction_collect` | PR #840 (ADR-0304, 재수집 경계를 lakehouse 에서 계산) | 결정 대기 |
| **`legal_dong_code_changes`** | **이 ADR §2** | **구현: `skip-if-unchanged`** |
| **`building_register_floor`** | **이 ADR §3 (설계)** → V2 의 hub 레인 작업과 함께 | 설계 |
| **`parcel_served_gold`** | **이 ADR §4 (설계)** | 설계, 작업은 꺼져 있다 |
| `lineage_stewardship` | 이 ADR §5 | `full` 이 옳은 단위(857행) |
| `onbid_public_auction_collect` | 이 ADR §5 (ADR-0275) | `full` 이 옳은 단위 |
| `shape_ledger_check` | 이 ADR §5 | `full` 이 옳은 단위(메타데이터만) |

## Decision

**1. 위 표가 24개 `full` 작업의 주인 목록이다.** 각 PR 은 자기 작업만 `full` 에서 빼고 `every-job-declares-its-scope.py --shrink` 로 목록을 줄인다. 이 ADR 에서 줄이는 것은 `legal_dong_code_changes` 하나다(24 → 23).

**2. 법정동 수집은 전체 표 앞에서 원천에 "바뀐 것이 있는가"를 묻는다 (changed-since probe).**

원천에는 ETag·Last-Modified 가 없다(2026-10-28 응답 머리: `Cache-Control: no-cache`, `Pragma: No-cache`, 둘 다 없음). 대신 표 화면 자신의 **변경기간 검색**(`stdate`~`enddate`)이 있고, 이것이 원천이 주는 changed-since 질의다. 잰 것:

| 질의 | 결과 |
| --- | --- |
| `stdate=2026-07-28`, `enddate=2026-10-28`, `disuseAt=ALL`, 모든 열 | 32행, 72,560 B |
| 같은 창, Trino `reference.legal_dong_code_snapshot`(2026-10-04 표)에서 생성일·폐지일이 창 안인 코드 | 32행 — 같다 |
| `stdate=2026-07-28`, `enddate` 비움 | 0행 (끝을 비우면 아무것도 주지 않는다) |
| `stdate=2026-07-28`, `enddate=9999-12-31` | 32행, 72,555 B, 3초 |
| 전체 표 | 53,403행, 약 58 MB |

그래서:

- 계약 `code-go-kr-legal-dong.contract.json` 의 `full_table.changed_since_probe` 가 필드 이름(`stdate`·`enddate`), 열린 끝(`9999-12-31`), `lookback_days`(31), `full_reconcile_days`(7)를 정한다. 다른 곳은 이 값을 다시 적지 않는다.
- 수집기(`collect-code-go-kr-legal-dong`)는 `..._CHANGED_SINCE=<YYYY-MM-DD>` 이면 같은 양식에 두 필드를 더해 보낸다. 물어보기는 언제나 dry run 이고(`..._LIVE_WRITE` 와 함께면 거부) manifest 역할은 `changed_since_probe` 다. Bronze 에는 아무것도 남지 않는다.
- 판정(`code_go_kr_legal_dong.py probe`)은 물어보기의 행을 전체 표와 같은 파서로 읽어 digest 를 낸다. **직전 전체 실행이 고정한 창(`accepted.json` `probe.since`·`probe.digest`)과 같은 창에 같은 행이고, 전체 대조가 밀리지 않았으면**(`full_checked_at_utc` 가 `full_reconcile_days` 안) `checked_at_utc` 를 남기고(`confirmed_by: probe`) 끝난다. 허브 내보내기의 신선도 점검(`projection.max_age_days`)은 그대로 이 시각을 읽는다.
- 아니면 예전처럼 전체 표를 받아 검사하고(같으면 저장 0, 다르면 Bronze·넘김), 이 실행의 물어보기를 다음 실행들의 창으로 고정한다. 물어보기를 표보다 **먼저** 하므로, 둘 사이에 표가 바뀌어도 고정된 행은 옛 것이고 다음 물어보기가 달라 다시 전체를 받는다 — 놓치지 않는다.
- 창의 시작은 고정한 날의 KST 날짜 − 31일이다: 늦게 올라온 소급 날짜도 한 달까지는 창 안이다. 창이 `lookback + reconcile` 일보다 오래되면 새 창을 연다.
- 물어보기가 볼 수 없는 변경(날짜 없이 이름만 바뀜, 31일보다 앞선 소급)은 **7일마다 전체 대조**가 잡는다. 짝 맞추기가 쓰는 것은 생성·폐지 날짜가 있는 변경뿐이다(ADR-0143 §3, `pairing.floor_date`). 그래서 짝 맞추기·계보에 들어가는 변경은 물어보기가 다음 실행에서 본다.

업계 방식: 커서 기반 증분 추출 + 주기적 전체 재동기화. Fivetran 은 변경 추적이 불완전한 원천에 증분 동기화와 주기적 re-sync 를 함께 쓰고, Airbyte 의 Incremental 동기화는 cursor field 에 lookback window 를 둔다. HTTP 원천이 조건부 요청(RFC 9110 `If-None-Match`/`If-Modified-Since`)을 주면 그것을 먼저 쓴다 — 이 원천은 주지 않아 원천 자신의 변경일 필터가 커서다.

**3. `building_register_floor` (설계): 층 Silver 를 hub 레인과 같은 비교 적재로.** 지금은 완결된 FLOOR 릴리스를 통째로 한 배치로 덧붙이고(2026-10-20: 21,189,570행, 1.065 GB, 표의 절반), 건물 패널이 가장 최근 배치 하나를 고른다(`newest_floor_batch`, #830).

- 표 `silver.building_register_floors` 에 `row_state` 와 `current_row_latest {key: 층 식별자, newest_first: [ingested_at_utc, source_snapshot_id], removal_column: row_state}` — 순서는 적재 시각이다. 배치 id 의 `v1`/`v2` 는 순서가 아니다(#830 의 교훈).
- 레인 계약에 `compare.content_columns`. 층 주기는 새 릴리스를 load 브랜치에 비교 적재하고(ADR-0311 `silver_release_compare`, 커밋이 곧 변경 기록) 바뀐·사라진 층만 `main` 에 덧붙인다.
- 건물 패널은 배치를 고르지 않고 `current_rows(frame, contract)` 로 읽는다. 레거시 `selection-v2` 배치는 첫 비교 적재가 그 키들을 대조해 사라진 층에 제거 표시를 남긴다(데이터는 지우지 않는다).
- 이 일은 V2(ADR-0318)의 hub 레인(표제부·전유부·전유공용면적) "branch staging + 건물 패널 reader" 와 **같은 reader 를 같은 방식으로** 바꾼다. 그래서 V2 와 한 PR 사슬에서 한다 — 둘이 따로 `building_panel_silver_to_gold.py` 의 입력 선택을 바꾸면 충돌한다.

**4. `parcel_served_gold` (설계): 판이 바뀌어도 바뀐 시도만.** 새 지적 판마다 전국 대조 관문(13.5분)과 서빙 Gold(59분, 2026-10-13 실측)를 다시 한다.

- 입력 지문을 시도마다 둔다: (그 판의 `silver.parcel_boundaries` 시도 행, 그 시도의 `map_edit_ledger` parcels 편집, 그 시도의 `parcel_registry`) 행 해시의 순서 무관 합 — PR #897 의 시도 archive 이름과 같은 지문법. ADR-0316 이 경계를 판으로 나누고 PNU 범위로 정렬해, 시도 하나 읽기는 파일 1~2개다.
- 대조 관문(`parcel_matching_gate.py --sido`, 이미 시도를 받는다)은 지문이 바뀐 시도만 돈다. 지문이 같은 시도는 직전 판의 관문 판정을 그대로 쓴다(판정 파일에 시도별 지문을 남긴다).
- 서빙 Gold 는 바뀐 시도의 PNU 범위만 MERGE 한다(ADR-0307 의 range 순서 유지). 그대로인 시도는 행을 다시 쓰지 않는다.
- 다음 단계의 타일은 #897 이 같은 지문으로 바뀐 시도만 굽는다. 시도 안에서 바뀐 필지만은 V7(ADR-0111 base+patch) 의 몫이다.
- 작업이 꺼져 있고(`enabled: false`) #900·#897 이 먼저 들어가야 하므로 여기서는 설계만 남긴다. 켜기 전 감독 리허설(runbook `parcel-tile-bake.md` §2)이 이 설계를 잴 자리다.

**5. `full` 이 옳은 단위인 셋.** 
- `lineage_stewardship`: 검토 큐 857행을 다시 만드는 비용이 변경 추적보다 싸다(10,000행 넘으면 다시 본다). 앞 단계(넘김 적재, 시도 묶음 계보)는 이미 새 객체·빠진 묶음만 한다.
- `onbid_public_auction_collect`: 목록에서 빠진 물건은 완결된 한 바퀴로만 안다(ADR-0275 제거 표시). Silver 적재는 바뀐 행만 덧붙인다.
- `shape_ledger_check`: 카탈로그 메타데이터만 읽는다(약 30초, 데이터 파일 0).

## Consequences

- 법정동 수집(계산, 측정 아님): 실행 한 번 = 양식 화면 41.6 KB + 물어보기 약 72 KB. 전체 표 58 MB 는 7일에 한 번과 표가 바뀐 날에만. 하루 약 10번 실행 기준 약 580 MB/일 → 약 9.4 MB/일(물어보기 1.1 MB + 전체 대조 58 MB/7). 실행 시간은 물어보기만이면 요청 간격 15초 + 약 3초.
- 전체 표를 받는 실행은 요청이 둘 늘어난다(물어보기 양식 화면과 질의, 간격 15초). 드문 날이다.
- 배포 후 첫 실행은 `accepted.json` 에 고정된 창이 없어 전체 표를 한 번 받고 창을 고정한다.
- 사이트가 변경기간 결과를 흔들면(같은 표인데 다른 행) 매번 전체를 받는다 — 빠짐은 없고 아낌만 없다. journal 의 `reason` 이 그것을 보여 준다(runbook).
- 남은 `full` 23개는 위 표의 주인 PR 이 줄인다.
