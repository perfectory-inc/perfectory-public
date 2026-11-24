# ADR 0615: DuckDB 로 옮긴 Gold 레인은 시도로 나뉜 Dagster 자산이고, 파트의 상태는 Dagster 의 것이다

- Status: Proposed (초안 — EGM 검토 전, 코드 없음)
- Date: 2026-11-24
- 고치는 곳(Accepted 가 되면): [ADR-0564](./0564-a-gold-of-row-versions-is-born-as-its-own-versions-table-beside-the-replaced-one-and-its-current-row-rule-is-rendered-once-in-the-contract-artifact.md)
  결정 3(전국 한 커밋)과 넷째 개정(이어 짓기), ADR-0333 §1(시도 파티션은 "레인이 Dagster 로 옮기면")의 실행.
- 짝: ADR-0616(Dagster 가 일을 직접 시작한다, W82c). 이 문서의 "파티션", "파트가 알리는 방법", "파트를 시작시키는 것"은 0616 이 그대로 쓴다.

## 사장에게: 세 줄

- 지금은 "얼마나 한 번에 싣고, 어디까지 했고, 실패하면 어디서 잇는가"를 레인마다 우리 코드가 정한다. 2026-11-23 에 그림자 넷이 그래서 멈췄다.
- 앞으로 **상태를 가진 단위는 시도 하나, Dagster 의 파티션**이다. 어느 시도를 지을지, 실패한 시도만 다시 돌리기, 한꺼번에 몇 개 돌릴지는 Dagster 의 기본 기능이 한다.
- 우리 코드는 둘만 한다: 시도마다의 "입력 버전"을 데이터로 내고, 한 시도를 메모리 안에서(시군구씩) 짓는다.

## Context

**11-23 에 멈춘 것.** 그림자 넷이 같은 까닭(시도나 전국을 한 문장으로)으로 멈췄다: 토지소유 OOM 4/4, 필지 패널 OOM 1/1, 실거래 열린 파일 9/9,
인허가 판정 시간 초과. 메모리는 #1676(`kind_versions_stage::build_in_parts`, 시군구 조각)이 공통 빌드에서 막았다. 남은 것은 **상태**다.

**지금 상태를 가진 것(main `ec446d36`, 코드를 읽음).**

| 우리 코드 | 하는 일 | 어디 |
|---|---|---|
| 이어 짓기 | 지문마다 상태 디렉터리, 끝난 묶음 파일, 25 분 멈춤, `partial` 결과 | `gold_versions_resume.rs`, `gold-versions-build.sh` |
| 레인 기록 | "이 커밋은 지역마다 무엇에서 지어졌나" | 실거래 레인(`real_transaction_versions_stage`) |
| 시도 루프 | 무엇을 지을지 정하는 `for sido` | `kind_versions_build` 와 자체 드라이버 4(주변·호 공시가격·서빙 경계·실거래) |
| 판정 기록 | 머리마다 판정했는지, 다음 표는 무엇인지(`last_judged`) | `gold_takeover_check.py` 의 `takeover-verdict.json` |

Dagster 쪽은 작업마다 job 하나이고(`run_jobs.py`), 그 op 가 유닛을 한 번 시작하며, 유닛은 전국을 한 Iceberg 커밋으로 붙이고 파티션 없는
materialization 하나를 알린다. Dagster 는 시도를 모른다.

**읽은 것(Dagster 1.13 문서와 1.13.27 소스; 우리 핀은 `dagster==1.13.27`).**

- 파티션: `StaticPartitionsDefinition`, `DynamicPartitionsDefinition`(자산마다 10 만 개 이하 권장).
  <https://dagster.io/docs/api/dagster/partitions>
- 한 조각의 사건: `AssetMaterialization(asset_key, partition=...)`, `AssetObservation(..., partition=...)`.
  <https://dagster.io/docs/guides/build/assets/metadata-and-tags/asset-observations>
- 백필: 기본은 파티션마다 run 하나, `BackfillPolicy.single_run()` 은 고른 파티션을 한 run 에("실패하면 모두 다시").
  <https://dagster.io/docs/examples/best-practices/partition-backfill-strategies>
- 선언적 자동화: `AutomationCondition.eager()` 는 static·dynamic 파티션에서 모든 파티션을 본다. 상류가 **관찰**되면 데이터 버전이 바뀐 때만
  갱신으로 치고, **materialize** 되면 버전과 상관없이 갱신으로 친다. <https://dagster.io/docs/guides/automate/declarative-automation>
- 동시 실행: 풀과 태그 한도는 run 에 건다. 파티션 하나가 run 하나면 지금의 `duckdb` 풀(ADR-0605)이 곧 파티션 한도다.

**스파이크 둘(2026-11-24, 노트북의 임시 venv, `dagster==1.13.27`, `DagsterInstance.ephemeral()`; 운영에 닿지 않음).**

(a) `BackfillPolicy.single_run()` 과 떨어진 키.

| 물은 것 | 결과 |
|---|---|
| 7 개 키 전부 | run 1 개, 구간 `11..48` |
| `{11, 41, 47}` | run 2 개: `11..11`, `41..47` — **이어진 구간마다 run 하나**(정의의 키 순서에서) |
| 구간 `11..41` 의 run 이 `28` 에서 실패(시도마다 `AssetMaterialization(partition=)` 을 알리던 중) | `11·26·27` = MATERIALIZED, `28·41` = FAILED, 나머지 = 없음 |
| 뒤이은 `28..41` run | 성공, `28·41` = MATERIALIZED |

소스: `dagster/_core/definitions/automation_tick_evaluation_context.py` `_build_run_requests_with_backfill_policy` 가
`partition_subset.get_partition_key_ranges()` 의 구간마다 run 요청 하나를 만든다. 그래서 떨어진 키를 한 run 에 넣는 길은 없다(우리가 만들지 않는다).

(b) 파티션마다의 데이터 버전이 `eager` 를 파티션마다 움직이는가. 상류 = 레인의 입력, 하류 = 판 표 자산(시도 파티션, `eager`).

| 상류를 알리는 방법 | 결과 |
|---|---|
| A. `@observable_source_asset(partitions_def=...)` 가 `DataVersionsByPartition` 을 돌려줌(Dagster 가 관찰을 돌린다) | 처음: 넷 모두 요청. 같은 버전 다시 관찰: 0. `41` 만 바뀜: `[41]`. `11·47` 바뀜: `[11, 47]`. static·dynamic 둘 다 같다 |
| B. op 가 `AssetObservation(partition=, tags={"dagster/data_version"})` 를 적음(지금 `run_jobs.py` 의 방식) | 한 번도 요청하지 않음 |
| C. op 가 `AssetMaterialization(partition=)` 을 적음 | 매번 넷 모두 요청(문서대로: materialize 는 버전을 보지 않는다) |

그래서 기본 기능만으로 "바뀐 시도만"이 되려면 **입력은 `observable_source_asset`, 판 표는 진짜 `@asset`** 이어야 한다. op 가 사건을 적는 지금의
모양으로는 되지 않는다.

**입력이 시도로 놓여 있는가(2026-11-24, 운영 카탈로그의 `SHOW CREATE TABLE`, 메타데이터만).** 종류 레인이 읽는 Silver 가운데 시도로 나뉜 것은
`silver.unit_official_price`(`sido`) 하나다.

| 입력 | 파티션 | 정렬 | 시도로 놓였나 |
|---|---|---|---|
| `silver.land_ownership`(토지소유) | 없음 | 없음 | 아니다 |
| `silver.parcel_boundaries`(모든 레인) | `source_snapshot_id` | `pnu` | 판본 안에서 파일이 pnu 순서다(파일 min/max 로 걸러진다) |
| `silver.building_register_titles`·`units` | 없음 | 없음 | 아니다 |
| HUB 표(`building_permit_*`, `building_register_master`·`sub_parcel` …) | `vintage` | 없음 | 아니다 |
| `silver.land_characteristic`·`land_use_plan`·`land_forest_ledger`(패널) | 없음 | 없음 | 아니다 |
| `silver.registered_factory`, `silver.onbid_*` | 없음 | 없음 | 아니다 |
| `silver.real_transaction` | `dataset_slug`, `truncate(deal_ym, 4)` | 없음 | 아니다 |
| `silver.unit_official_price` | `sido` | 없음 | **그렇다** |
| `reference.legal_dong_code_change` | 없음 | 없음 | 아니다(전국 표, 작다) |

그러니 지금은 입력이 조금만 움직여도 17 시도의 버전이 모두 움직이고, 시도마다 읽으면 표를 17 번 훑는다(W95b 가 잰 것: 2,155 키를 짓는 데 308 초,
17 시도를 모두 읽어서). "바뀐 시도만"이 얼마나 듣는가는 이 배치가 정한다.

## Decision

### 1. 파티션은 시도다

1. 상태·재시도·백필의 단위는 **시도(PNU 앞 2자)** 다. ADR-0333 §1 의 키이고, 판 표의 Iceberg 파티션이다: "파티션이 materialized" 는 "그 표의
   그 파티션에 붙었다"와 같은 말이다. 시군구(약 250)는 드라이버 안의 메모리 조각(#1676)이고 밖에서 보이지 않는다.
2. PNU 로 나뉘는 모든 Gold 레인이 **`DynamicPartitionsDefinition(name="sido")` 하나**를 같이 쓴다. 키는 입력 관찰(결정 2-1)이 낸 목록에서 들어온다
   (`build_add_request`). 시도 코드는 바뀐다(42→51, 45→52): 새 코드는 입력에 나타나는 날 파티션이 되고, 어느 레인도 내지 않게 된 코드는 같은
   관찰이 지운다(`build_delete_request`). static 목록은 코드가 바뀔 때 PR 이 필요해서 고르지 않았다. 스파이크 (b)-A 는 dynamic 에서도 같았다.
3. PNU 로 나뉘지 않는 표는 까닭을 선언한다: 스튜어드 큐 = `unit`(둘), 발행 ID = `(unit, scope)`(필지 unit 의 scope 는 시도, 읍면동은 `national`),
   산업단지 목록(1,442 행) = 파티션 없음.

### 2. 바뀐 파트가 파티션 키가 되는 길

1. **입력 관찰.** 레인마다 `@observable_source_asset(partitions_def=sido)` 하나가 공통 드라이버의 `plan` 을 불러 `DataVersionsByPartition` 을
   돌려준다. `plan` 은 한 줄의 JSON(`{"partitions": {"11": "<버전>", ...}}`)을 내고 상태를 갖지 않는다.
   버전(시도) = hash(코드 버전, 서빙 판본, 입력마다의 그 시도 지문). 지문은 Iceberg 메타데이터에서 읽는다: 시도로 나뉜 입력은 그 파티션의
   `last_updated_snapshot_id`, 그렇지 않은 입력은 표의 스냅숏 id(= 어느 변경이든 17 시도가 모두 움직인다 — 지금의 `whole_table_inputs` 를
   데이터로 말한 것). 레인이 주는 것은 지금도 주는 둘뿐이다: `inputs()` 와 `whole_table_inputs()`.
2. **Dagster 가 정한다.** 판 표는 `@asset(partitions_def=sido, automation_condition=AutomationCondition.eager(), deps=[입력 관찰])` 이다.
   스파이크 (b)-A 대로 버전이 바뀐 시도만 요청된다. 우리 센서는 없다.
3. **짓기.** 자산의 몸은 유닛을 시작하고(결정 5), 드라이버는 받은 시도를 시군구씩 짓고, 그 시도의 머리와 diff 하고, **그 시도의 Iceberg 추가 커밋
   하나**를 하고, `report_asset_materialization(partition=<시도>, data_version=<지은 버전>)` 과 그 시도의 검사(`report_asset_check`)를 알린다.
4. **커밋은 시도마다다**(ADR-0564 결정 3 을 고친다). 지금은 한 커밋이 전국의 핀·코드 버전·지문을 진다(`gold_versions_commit::Base`). 앞으로 커밋은
   제 파티션을 이름 대고 그 파티션의 핀·코드 버전·현재 행 수·지문을 적는다. `Base` 는 파티션마다 읽는다(`main` 을 걸어 그 파티션의 가장 새 커밋).
   이것은 표의 상태이지 실행의 상태가 아니다. 한 표의 시도들이 서로 다른 입력 핀에 설 수 있다. 문서는 제 PNU 의 행만 읽으므로(#1676 에서 종류
   단계 여덟을 확인) 맞는 표다.
5. **여러 시도를 한꺼번에.** 자산은 `BackfillPolicy.single_run()` 을 선언한다. 처음 짓기와 코드 버전 이동(17 개 모두)은 run 하나·입력 한 번
   읽기이고(스파이크 (a): 구간 하나), 시도마다 커밋하고 알리므로 9 번째에서 죽어도 1~8 은 Dagster 와 표에 서 있다. 남은 것을 다시 요청하는 것은
   Dagster 의 백필·재실행이다. 떨어진 키는 이어진 구간마다 run 하나가 된다(스파이크 (a)): 받아들인다.
6. **동시 실행.** run 은 지금의 `duckdb` 풀을 쥔다. 한 표의 두 run 이 커밋을 다투지 않게 레인마다 한 칸짜리 풀을 더한다(`dagster.yaml`, #1665 의 방식).

### 3. 입력을 시도로 놓는다

"바뀐 시도만"은 입력이 시도로 놓인 만큼만 듣는다. PNU 로 열쇠 지은 Silver 표의 파티션 명세에 `truncate(2, pnu)` 를 더한다(Iceberg 의 숨은
파티션·명세 진화: 새로 붙는 파일부터 적용되고 표는 다시 쓰지 않는다). 그 뒤로는 `$partitions` 의 `last_updated_snapshot_id` 가 시도의 지문이고,
시도 하나를 읽는 run 은 그 시도의 파일만 연다. 순서: 토지소유(첫 레인의 입력) → 패널의 토지 표 셋 → 표제부·전유부 → HUB 표. 표마다 명세를 바꾸는 PR 이
앞뒤의 `plan` 지문과 읽은 바이트를 잰다. 명세를 바꾸기 전의 표는 결정 2-1 대로 "전국 표"로 선다(정직하게 17 개 모두).

### 4. 지우는 것

| 지우는 것 | 대신하는 Dagster 기능 |
|---|---|
| `gold_versions_resume.rs` 전부(지문 디렉터리, 묶음 파일, 25 분 멈춤, `partial`), `FOUNDATION_PLATFORM_GOLD_VERSIONS_RESUME_DIR` | 파티션 상태(MATERIALIZED/FAILED/없음), 백필·재실행 |
| 실거래 레인의 지역 요약 기록과 묶음 짜기 | materialization 의 데이터 버전, `plan` 의 목록 |
| "무엇을 지을지" 정하는 `for sido` 루프(공통 드라이버와 자체 드라이버 4), 실거래의 자체 `PARTITION_BY node` 스풀 | run 의 파티션 키·구간 |
| `changed_pnus` 의 옛 핀 두 번째 읽기(`_old` 스풀) — 레인이 더 싸다고 잰 곳만 남는다 | 파티션 선택은 Dagster, 키 선택은 digest diff |
| `takeover-verdict.json`(`completed`, `last_judged`) | 선언한 시도 파티션의 자산 검사 이력 |
| 지역마다의 `foundation-job-outcome` 줄 | 파티션의 materialization·실패 사건 |

남는 것: `build_in_parts`(메모리 조각), digest diff 와 커밋, 결과 검사(파티션의 자산 검사가 된다).

### 5. 우리 것으로 남는 조각과, 쓰지 못한 Dagster 기능

| 우리 조각 | 쓰지 못한 기능 | 까닭 |
|---|---|---|
| `plan`(시도마다의 버전을 내는 명령) | Dagster 는 Iceberg 의 파티션 메타데이터를 읽지 않는다 | 버전의 재료(핀·코드 버전·판본)는 발행기만 안다. 돌려주는 모양은 기본(`DataVersionsByPartition`)이다 |
| 시도마다의 커밋과 `Base` | — | Iceberg 표의 상태다. Dagster 의 것이 아니다 |
| `build_in_parts` | — | 한 프로세스 안의 메모리다. 상태가 없다 |
| 유닛을 시작하는 층(`run_jobs.py` 의 `ForcedCommand`, `systemd_pipes`, `start-scheduled-job.sh`) | `PipesDockerClient`(ADR-0616 이 옮긴다) | 이 설계가 그 층에 기대는 것은 셋뿐이다: ① 유닛 하나를 인자(파티션 키·구간)와 함께 시작한다, ② 유닛의 Pipes 줄(`report_asset_materialization`·`report_asset_check`, 파티션 키 포함)이 사건이 된다, ③ 유닛의 종료 상태가 run 의 성패다. 지금의 요청은 작업 id 와 종류만 싣는다 — 파티션 키를 싣게 넓히거나 0616 의 길로 간다. 층의 재시도·`check`·결과 태그에는 기대지 않는다 |

### 6. 첫 레인과 확인

첫 레인은 **토지소유(그림자)** 다. 커밋이 없어 옮길 머리가 없고, 읽는 곳이 없고, 종류 입력 가운데 가장 크고(한 시도 15,345,613 행), 사장이 물은
바로 그 실패다. 그 뒤: 필지 패널(그림자) → 나머지 종류 그림자 → 넘겨받은 셋 → 자체 드라이버(주변·호 공시가격·서빙 경계·실거래).

운영에서 Dagster 의 기록으로 확인한다(우리 탐침 없음).

1. 17 파티션이 materialized 이고 저마다 데이터 버전을 가진다.
2. 파티션 run 마다 유닛 메모리 최고가 선언한 `memory_max` 아래다.
3. 한 시도를 실패시키면(그 키에만 작은 `memory_limit`) 그 파티션만 FAILED 이고, 재실행이 그것만 짓고, 우리 파일은 아무것도 기억하지 않는다.
4. 입력이 그대로일 때 관찰을 두 번 돌리면 요청 0. 다음 토지소유 적재 뒤에는 — 결정 3 전에는 17 개 모두, 뒤에는 움직인 시도만.
5. `takeover_equal`(시도 41)이 지금처럼 통과하고, 전국의 행 수·키 해시 합·내용 해시 합이 Spark 표와 같다.

## Consequences

- 전량 짓기 한 번이 커밋 17 개다. 스냅숏 만료는 그대로 듣는다. by-PNU 굽기는 백필이 끝난 머리를 핀한다(끝났는지는 Dagster 의 백필 상태).
- 성공한 구간 run 은 시도마다 사건이 둘이다(유닛이 알린 것, run 끝의 것; 스파이크 (a) 에서 봄). 고치지 않는다.
- 결정 3 전에는 "바뀐 것만"이 시도 단위로 듣지 않는다. 그동안의 이득은 상태·재시도·백필이다.
- 종류 줄의 `pins_from`(패널 ↔ 호 공시가격)은 파티션마다 읽는 말로 고쳐야 한다.

## 하지 않은 것, 확인하지 못한 것

- 코드가 없다. 스파이크는 프로세스 안의 임시 인스턴스였다: 데몬이 도는 운영 Dagster 에서 `eager` 가 같은 요청을 내는지는 첫 레인이 본다.
- `truncate(2, pnu)` 명세 진화를 R2 Data Catalog 가 받는지, DuckDB iceberg 확장이 그 파티션으로 거르는지는 읽지도 재지도 않았다(결정 3 의 첫 PR 이 본다).
- `silver.parcel_boundaries` 의 파일이 실제로 시도마다 갈리는지(파일의 pnu min/max)는 재지 않았다.
- 실거래의 "잇는 시군구 묶음"이 시도를 넘는지 확인하지 못했다(W81f 와 정한다). 넘으면 그 레인의 파티션은 `plan` 이 내는 묶음이다.
- Silver 는 범위 밖이다(54 레인이 공통 드라이버 하나·열쇠 앞자리 묶음; 표는 따로 보냈다).

## 기각한 대안

- **30 줄짜리 센서로 버전을 견주기.** 스파이크 (b)-A 가 기본 기능으로 됨을 보였다.
- **지금처럼 op 가 사건을 적기.** (b)-B·C: 요청이 아예 없거나 매번 전부다.
- **파티션 = 시군구.** 250 개의 상태는 사람이 읽지 못하고, 표의 Iceberg 파티션과 어긋나고, 입력을 250 번 읽는다.
- **static 시도 목록.** 코드가 바뀔 때마다 PR 이다.
- **파티션마다 run 하나(기본 백필).** 시도로 놓이지 않은 입력을 17 번 읽는다.
