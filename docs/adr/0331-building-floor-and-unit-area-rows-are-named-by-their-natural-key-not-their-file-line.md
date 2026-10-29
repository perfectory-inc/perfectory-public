# ADR 0331: 층·전유공용면적 행은 원천 파일의 줄이 아니라 자연 키로 이름 짓는다

- Status: Accepted
- Date: 2026-10-29
- Amends: [ADR-0326](./0326-a-gold-rebuild-that-costs-more-to-find-the-change-than-to-rebuild-runs-whole.md)
  (Consequences 의 "남은 것: 층의 `floor_row_id`")
- Builds on: [ADR-0320](./0320-a-document-schema-bump-bakes-only-the-documents-it-changes.md) (문서 판의 확장/수축),
  [ADR-0189](./0189-an-appended-silver-input-is-its-own-change-set.md) §3 · ADR-0326 §2 (배치가 바뀐 입력의 변경 = 두 배치의 행 차이),
  [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md) (부분 처리가 기본)

## Context

`silver.building_register_floors.floor_row_id` 와 `silver.building_register_unit_areas.area_row_id` 는
`building-register-floor:bronze/source=…/OPN<YYYYMMDD…>.zip#line-000001` 처럼 **원천 파일 이름과 줄 번호**다
(`building_register_row_identity.rs` `row_identity`). 계약 주석은 "Stable Silver row id" 라고 하지만 매달 새 파일이
오면 모든 행의 id 가 바뀐다. 줄 위치는 이미 `source_record_id`·`source_line_number` 두 계보 열에 따로 있으므로
id 는 계보를 한 번 더 적은 것일 뿐이다.

`floor_row_id` 는 by-PNU 건물 문서로 나간다(`buildings[].floors[].floor_row_id`). 그래서 ADR-0326 은 이것을 건물
패널의 비교 열에서 빼지 못했고, 층 세대가 바뀔 때마다 모든 행이 "바뀐 행" 이 된다.

**잰 것(2026-10-29, 운영 Trino 읽기 전용).** 층 두 세대(09-20 세대 21,190,810 행, 10-01 세대 21,189,570 행)를
같은 키로 맞대었다:

| 층 비교 키 | 바뀐 키(행) | 바뀐 대장 | 바뀐 건물 PNU |
|---|---:|---:|---:|
| `floor_row_id`(지금) | 21,189,570 (전부) | 8,039,312 (전부) | 5,945,767 문서 전부(ADR-0320 표) |
| 자연 키(이 결정) | **30,115** (추가 14,256 · 삭제 15,496 · 내용 363) | **13,167** | **6,817** |
| 참고: id 를 뺀 다중집합 차이(ADR-0326) | 29,754 | 13,074 | — |

자연 키의 결과는 id 를 아예 뺀 차이와 거의 같다(행 361, 대장 93 차이: 같은 층 안에서 순번이 밀린 행).

**원천에는 행 키가 없다.** 건축HUB 층별개요(`mart_djy_04`, data.go.kr `getBrFlrOulnInfo`)와 전유공용면적
(`mart_djy_06`)의 키는 대장의 `관리_건축물대장_PK`(`mgm_bldrgst_pk`) 하나뿐이고, 한 층은 용도마다, 한 호의 한 층은
용도·면적마다 행이 여럿이다. 10-01 층 세대에서 (대장, 층구분, 층번호)가 같은 행 묶음이 2,541,000 개(6,185,319 행,
최대 187 행)이고, 그중 Silver 가 남긴 열까지 다른 묶음은 64,867 개뿐이다(나머지는 용도·면적이 다른데 Silver 가 그
열을 갖지 않는다). 전유공용면적은 1/8 표본에서 (대장, 전유공용, 주부속, 층) 묶음 2,193,121 개가 행 5,477,511 개를
가진다. 그러니 자연 키는 "식별 열 + 그 묶음 안의 순번" 일 수밖에 없다.

### 실사례

- **Kimball 차원 모델링 · dbt `generate_surrogate_key`**: 원천의 업무(자연) 키 열로 대리 키를 만든다(열을 구분자로
  이어 해시). 적재 위치나 행 전체 내용은 키가 아니다 — 내용으로 만들면 값이 바뀐 행이 "지우고 새로 넣은" 행이 된다.
- **Delta Lake / Iceberg `MERGE INTO … ON <key>`, Debezium·CDC**: 변경은 키로 맞대어 찾는다. 파일·오프셋은 위치다.
- **Stripe *Online migrations at scale*, Fowler *ParallelChange***: 쓰는 쪽을 먼저 바꾸고, 옛 데이터는 읽을 수 있게
  두고, 옛 것이 더 이상 쓰이지 않을 때 걷는다(확장 → 이전 → 수축).

## Decision

1. **행 id 는 자연 키다.** `<kind>:nk1:<hex32>`, hex32 = SHA-256(식별 열들 ␟ 순번)의 앞 128비트(␟ = U+001F)
   (`natural_row_identity`). 식별 열:
   - 층: (`mgm_bldrgst_pk`, 층구분 코드 `floor_type_code_raw`, 층번호 `floor_number_raw`).
   - 전유공용면적: (`mgm_bldrgst_pk`, 전유공용구분 코드, 주부속구분명, 층구분 코드, 층번호).

   해시는 식별 열에만 쓰고 행 전체 내용에는 쓰지 않는다. 내용이 바뀐 행은 id 가 그대로이고 "바뀐 행" 이다.
   `nk1` 이 옛 위치 id(`<kind>:bronze/…#line-N`)와 새 id 를 가른다.
2. **순번은 묶음 안에서 나머지 열의 지문, 그다음 입력 순서로 매긴다**(`NaturalKeyRank`, `natural_key_occurrences`).
   지문 = 나머지 원천 열을 ␟ 로 이은 SHA-256 앞 8바이트. 그래서 다음 달 파일이 같은 행을 다른 순서로 실어도 같은
   이름이 나온다. 내용까지 같은 행끼리는 순서가 의미 없다(바꿔도 같은 행이다). 묶음에 행이 늘거나 줄면 그 묶음
   안의 순번만 밀린다 — 위 표의 361 행이다.
3. **순번은 그 대장의 행을 모두 본 다음에 매긴다.** 층 export 는 이미 대장마다 묶어 정규화한다(`floor_scratch`).
   전유공용면적 export 는 한 줄씩 흐르므로 zip 을 두 번 읽는다: 첫 번째는 행마다 20바이트(묶음 지문 8 · 내용 지문 8 ·
   입력 순서 4)를 모아 정렬해 순번을 내고, 두 번째가 그 순번으로 행을 만든다. 같은 대장의 행이 파일에서 붙어 있지
   않은 대장이 718,460 / 19,855,147 개(10월 파일)라 한 번의 흐름으로는 못 센다. 계산한 메모리: 114,461,830 행 ×
   20바이트 = 2.29 GB + 순번 458 MB, 서비스 상한 16 GB(`foundation-silver-refresh@.service`) 안. 원천이 두 읽기
   사이에 바뀌면(행 수가 다르면) 거부한다.
4. **확장/수축으로 옮긴다(append-only).**
   - **확장(이 변경)**: 쓰는 쪽만 바뀐다. 열 이름·형·필수 여부가 그대로이고 by-PNU 문서의 경로·필수 여부도 그대로다.
     ADR-0320 §1 의 판 기록(`contracts/by-pnu-documents`)이 바뀌지 않으므로 문서 판을 올리지 않고, ADR-0320 §3 의
     판단은 "같음" 이다. 읽는 쪽은 이 id 를 불투명한 문자열로만 쓴다: Gold 의 층 고유성 검사, 건물 문서의 쪽 이어붙이기
     중복 제거(`building_document.rs`), 웹 `building-edge.ts` 의 `z.string()`. 그 밖에 이 id 를 저장해 두고 다시 찾는
     곳은 없다(층에는 호의 `target_unit_row_id` 같은 덮어쓰기 기록이 없다).
   - **이전**: 다음 층 세대·전유공용면적 판이 새 id 로 실린다. 옛 세대 행은 옛 id 를 그대로 갖고 지우거나 고쳐 쓰지
     않는다. 건물 패널이 새 세대로 핀을 옮기는 첫 재구축은 모든 층 행이 한 번 다르다 — 오늘 매달 일어나는 일과 같은
     양이고 마지막이다. 그 뒤로는 바뀐 행만큼이다.
   - **수축**: 건물 패널과 by-PNU 건물 레인이 새 id 세대만 핀으로 잡으면 끝난다. 걷어낼 필드는 없다(같은 필드다).
     ADR-0320 §5 의 수축(다음 전량 세대)이 서빙 문서를 모두 새 id 로 바꾼다.
5. **크로스워크는 저장하지 않고 계산한다.** 새 id 는 행이 가진 열만의 함수다. 옛 층 행의 새 id 는 Trino 로
   언제든 다시 만든다(같은 배치 안에서):

   ```sql
   'building-register-floor:nk1:' || substr(lower(to_hex(sha256(to_utf8(
     mgm_bldrgst_pk || chr(31) || floor_type_code_raw || chr(31) || floor_number_raw || chr(31) ||
     cast(row_number() over (
       partition by source_snapshot_id, mgm_bldrgst_pk, floor_type_code_raw, floor_number_raw
       order by substr(sha256(to_utf8(floor_type_name_raw || chr(31) || coalesce(floor_label_raw, ''))), 1, 8),
                source_line_number) as varchar))))), 1, 32)
   ```

   위 표의 "자연 키" 줄이 이 식으로 잰 것이다. 전유공용면적 Silver 는 원천 코드(층구분·층번호·전유공용구분 코드)를
   남기지 않으므로 옛 행의 정확한 크로스워크는 Bronze 를 다시 읽어야 한다. 그 id 를 참조하는 곳이 없어
   필요하지 않다(Gold 는 `area_row_id` 를 비교에서 이미 뺐다, ADR-0326 §3).

## Consequences

| 항목 | 전 | 뒤 | 근거 |
|---|---|---|---|
| 새 층 세대에서 "바뀐" 층 행 | 21,189,570 | 30,115 | 실측(10-01 대 09-20, 자연 키 식) |
| 바뀐 층 대장 | 8,039,312 | 13,167 | 실측 |
| 층 때문에 다시 굽는 건물 by-PNU 문서 | 5,945,767(전부) | 6,817 PNU | 실측(표제부 PNU 조인) |
| 전유공용면적 "바뀐" 행(09 판 → 10 판) | 114,461,830(전부) | 아래 PR 본문의 측정 | 실측(Silver 열로 근사) |
| 전환 비용 | — | 다음 한 세대에 한 번 전부(지금 매달과 같음) | 계산 |
| 전유공용면적 export | zip 한 번 | 두 번 + 2.75 GB | 계산 |

- 층 레인은 여전히 매달 세대 전체를 덧붙인다(`orchestration/jobs.v1.json` `building_register_floor` 의 `full_reason`,
  ADR-0314 §4 의 Silver 부분 적재 과제). 안정 id 가 그 과제의 전제다: 비교 레인이 되면 붙이는 행이 30,115 개다.
- Gold 의 층 변경 집합(ADR-0326 §2)은 코드 변경 없이 바뀐 행만큼이 된다. 비용 판단(ADR-0326 §1)이 증분을 고를 수
  있는 입력이 하나 는다.
- data.go.kr JSON 경로(`normalize_building_register_floor_silver_rows_from_public_data_bronze_json`)는 한 응답 쪽(page)
  안에서 순번을 센다. 한 건물이 두 쪽에 걸치면 같은 id 가 둘 나오고 Gold 의 층 고유성 검사가 거부한다(조용한 겹침은
  없다). 운영 레인은 허브 일괄 파일만 쓴다.
- 시험: `building_register_row_identity`(Python `hashlib` 로 만든 기대값과 같은 id, 파일 순서가 바뀌어도 같은 순번,
  같은 내용의 두 행은 다른 순번), `building_register_unit_area_silver_plan`(순서 바꾼 두 행이 같은 id, export 가 매긴
  순번이 배치 순번을 이김), `tests/building_register_floor_silver_rows.rs`(id 모양). Gold 고정물
  (`building_panel_gold_row.json`)과 by-PNU 골든 팩(`section-packs/`)은 새 id 로 다시 만들었다.

## Rejected

- **행 전체 내용의 해시**: 값이 바뀐 행이 지워지고 새로 생긴 행이 된다. 변경 수는 같아도 "무엇이 바뀌었나" 를
  잃는다. 식별 열과 내용 열을 나누는 것이 자연 키다.
- **id 를 비교·문서에서 빼기**: 문서가 이 필드를 필수로 싣는다(판 기록). 빼면 깨는 변경(ADR-0320 §3 "깸")이고,
  Gold 가 핀과 다른 내용을 갖게 되어 패리티 표본이 잡는다(ADR-0326).
- **새 열 `floor_key` 를 더하고(문서 v5) 나중에 `floor_row_id` 를 걷기**: 확장 기간 내내 옛 필드가 매달 모든 문서를
  바꾸고, 수축이 깨는 변경이라 전량 세대가 두 번 든다. 같은 필드의 값을 바꾸는 쪽이 문서 판 기록상 "같음" 이고 전환
  비용이 한 번이다.
- **순번을 입력 순서로만 매기기**: 원천이 같은 층의 행 순서를 바꾸면 그 행들의 id 가 서로 바뀐다. 지문 순서가 그
  흔들림을 없앤다.
- **전유공용면적 export 를 DuckDB 임시 표로 묶기(층처럼)**: 1억 1천만 행의 모든 열을 디스크에 다시 쓴다. 순번에
  필요한 것은 행마다 20바이트다.
