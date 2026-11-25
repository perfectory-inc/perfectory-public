# ADR 0621: 지도 편집은 자기가 놓인 원천 상태에 묶이고, Gold 빌드는 그 상태가 그대로일 때만 편집을 적용한다 — 필지 도형 편집은 막는다

- Status: Accepted
- Date: 2026-11-25
- Supersedes: [ADR-0112](./0112-map-polygon-edits-overlay-from-a-small-store-and-fold-into-bakes.md)의 2번 중 행의 칸(이 ADR 의
  핀이 그 `base_release_id` 를 실제로 둔다), 3번 중 "대상 feature 의 존재" 확인, 개정 각주 ①의 "원장의 **모든** 편집을 순서대로
  적용", 각주 ④(없는 대상의 delete 를 세기만 함), 2026-11-19 개정(필지 유닛을 편집 경로에 올림)
- 함께 읽는 것: [ADR-0113](./0113-parcel-lineage-absorbs-reorganizations-and-gates-every-map-bake.md)(필지 계보·겹침 금지),
  ADR-0616 A6(서빙 설계와 편집 저장소의 자리), [ADR-0618](./0618-a-tile-bake-is-a-dagster-asset-partitioned-by-feature-id-prefix-and-one-asset-that-merges-gates-and-promotes.md)
  열린 질문 1(편집 접기)

## Context

ADR-0112 의 서빙 Gold 는 "현재 공식 Silver 에 원장의 모든 편집을 순서대로 적용한 것"이다(각주 ①, `served_gold_common.py`).
원천이 편집 대상을 합치거나, 나누거나, 번호를 바꾸거나, 지우거나, 다시 그려도 편집은 같은 id 로 영원히 다시 적용된다.

**코드가 지금 하는 것(2026-11-25 main 에서 읽음):**

- 사라진 id 의 upsert 는 옛 폴리곤을 새 feature 로 되살린다. 병합·분할 뒤에는 죽은 필지가 후계 위에 겹쳐 그려진다
  (`industrial_complex_boundary_served_gold.py`·`administrative_boundary_served_gold.py` 의 `apply_edits`, `map_edit.rs`).
- 원천이 다시 그린 id 의 upsert 는 새 공식 폴리곤을 영원히 덮는다. 아무것도 알리지 않는다.
- 이미 없는 id 의 delete 는 `deletes_of_absent_features` 로 세기만 한다.
- 편집은 자기가 어느 원천 판 위에서 만들어졌는지 적지 않는다. ADR-0112 2번의 `base_release_id` 는 D1 표
  (`migrations/0001_map_edit.sql`), 요청, 원장 계약(`silver.map_edit_ledger`) 어디에도 없다. 그래서 원천이 편집 밑에서 바뀐
  것을 알아챌 규칙을 세울 수 없다.

**잰 것(2026-11-25, 읽기만):**

| 것 | 값 | 출처 |
|---|---|---|
| 편집 저장소의 편집 | 세 유닛 모두 0건, 한 번도 없었음 | 공개 오버레이 `/overlay/{complex,admin,parcels}`: `folded_through_change_seq` 0, `latest_change_seq` 0. 행은 접기로만 지워지고 접기는 수위를 0 위로 올린다 |
| 편집 API 의 감사 행 | `map_edit.*` 0건(감사 표 전체는 1,992행, 10-07..11-25) | 운영 `audit.event` |
| 필지 타일 굽기 | 꺼짐(`parcel_tile_bake` `enabled: false`, #1683), 11-24 10:06Z 실패한 예행 굽기 뒤로 돌지 않음 | 릴리스 3911532a 의 작업 목록, 유닛 저널 |
| 필지 Gold 반쪽 | 켜짐(`parcel_served_gold`, 매일 07:30). 타일은 202606 부트스트랩 release 를 서빙 | 같은 곳, `catalog.vector_tile_publication_unit` |

원장(`silver.map_edit_ledger`)은 읽지 않았다(레이크하우스 자격이 든다). 원장은 편집 저장소에서만 채워지므로 비어 있다.

**사장 결정(2026-11-25, EGM 검토 뒤):** 필지 폴리곤 편집은 막는다. 건물 폴리곤은 가끔 고쳐야 하므로 같은 규칙의 편집 유닛이
된다. 편집은 대기업 수준으로 한다: 편집마다 놓인 원천 상태(스냅숏 id + 기하 체크섬)를 묶고, Gold 빌드마다 대상이 그대로면
적용하고, 1:1 번호·코드 변경이면 계보로 새 id 에 옮기고, 그 밖이면 stale 로 둔다. stale 은 적용하지 않고 공식 폴리곤을 보이며
Gold 에 목록으로 남고 Slack 으로 알린다. 겹침으로 대상을 다시 고르지 않는다(ADR-0113 §4). 필지 굽기는 이 규칙이 오를 때까지
꺼져 있다. D1 은 서빙 설계(ADR-0616 A6)까지 둔다.

이것은 OSM API 가 객체 버전을 요구하고 다르면 409 로 답하는 낙관적 동시성이고, 이 저장소가 스튜어드 결정에 이미 쓰는 규칙이다
(결정은 자기가 본 증거에만 유효하다, ADR-0115 §3 `evidence_etag`).

## Decision

1. **필지 도형은 편집할 수 없다.** foundation-api 의 편집 API 와 편집 저장소 Worker 가 유닛 `parcels` 의 upsert·delete 를 모두
   거부하고(아무것도 저장하지 않는다), 저장소 계약(`map_edit_gateway.units`)은 그 유닛을 편집할 수 없다고 적는다. 지적은 법적
   기록이고 ADR-0113 은 공식 증거만 믿는다. 필지가 틀렸으면 원천에 알린다. 쓸 데가 없어진 필지 접기 코드(`parcel_served_gold` 의
   접기 반쪽, 웹 오버레이 유닛 목록의 `parcels`)는 거부를 넣는 PR 의 바로 다음 PR 이 지운다.

2. **편집할 수 있는 유닛은 산업단지(`complex`)와 행정경계(`admin`)이고, 건물은 자기 레인이 생긴 뒤 같은 규칙으로 더한다.**
   건물 레인은 건물 Silver, 서빙 Gold, 타일 유닛이다. 지금은 셋 다 없다(주소정보 전자지도 건물 도형은 Bronze 까지, 루트 ADR-0359
   §5·ADR-0494). 레인은 따로 정한다. 그때 알아야 할 열린 사실은 둘이다.
   - 건물 id 후보 `BD_MGT_SN`(건물관리번호)은 202610 전국 기초본 10,684,580행에서 겹치지 않는다(W61c, 2026-11-11). 원천이 건물을
     바꿀 때 같은 값으로 남는지는 재지 않았다.
   - 건물에는 승계 계보 원천이 없다. 일변동은 지운 행의 키(`Deletion.TXT`)만 준다. 그래서 건물 id 가 사라지면 그 편집은 stale 이다
     (4번).

3. **편집은 자기가 놓인 원천 상태에 묶인다(핀).**
   - **저장할 때:** 편집 API 가 그 유닛이 지금 서빙하는 release(`catalog.vector_tile_publication_unit.active_release_id`)를 편집
     저장소 행의 `base_release_id` 로 적는다. 클라이언트가 보내는 값이 아니다. 타일에도 더하는 것이 없다.
   - **원장에 처음 적힐 때:** Gold 빌드가 `base_release_id` 를 둘로 풀어 원장 행에 한 번 적는다. `base_source_snapshot_id` 는 그
     release 를 지은 Silver 스냅숏이다. `base_geometry_checksum_sha256` 은 그 스냅숏에서 대상 id 의 `geometry_checksum_sha256` 이고,
     원천에 그 id 가 없었으면 null 이다. 원장은 append-only 이므로 이 값은 다시 쓰지 않는다.
   - 세 칸은 nullable 로 더한다(D1 은 새 마이그레이션, 원장은 `evolve_iceberg_table_to_contract`). 지금 편집이 0건이므로 채울 옛
     행이 없다.

4. **Gold 빌드마다 서 있는 편집은 셋 중 하나다: 적용(applied), 옮김(carried), stale.** 판정은 편집을 적용하는 유일한 곳인 서빙
   Gold 빌드가 한다. 핀과 현재 Silver 를 견준다.

   | 원천이 편집 대상에 한 일 | 결과 |
   |---|---|
   | id 가 있고 그 체크섬이 핀과 같다 | 적용 |
   | 핀에 id 가 없었고(편집이 새로 만든 feature) 지금도 없다 | upsert 는 적용, delete 는 stale |
   | id 가 사라졌고, 그 유닛의 계보 표가 효력 있는 등급의 동일성 관계로 후계 하나를 보여 준다 | 옮김: 그 후계 id 에 적용(5번) |
   | 그 밖: 같은 id 의 기하가 바뀜, 병합·분할·재측량·삭제, 계보 링크가 `pending`·`needs_review`, 편집이 만든 id 를 원천이 새로 가짐, 핀을 풀 수 없음(release 를 모르거나 스냅숏이 만료됨) | stale |

   적용과 옮김은 지금처럼 `change_seq` 순서로 겹친다. 같은 feature 의 편집은 하나하나 따로 판정한다. 원천이 바뀐 뒤 직원이 다시 그린
   편집은 새 release 에 묶이므로 적용되고, 옛 편집은 stale 로 남는다. stale 편집 하나가 빌드를 멈추지 않는다.

5. **옮김은 id 의 계보 표가 있는 유닛만 한다.** 지금 그런 표는 `silver.parcel_lineage` 하나이고 `lineage_carry::carry_candidates` 로
   읽는다. 동일성 관계(`code_change`·`jurisdiction_transfer`·`registration_conversion`)만 따르고, 효력 있는 등급만 쓰며, 최대 8 걸음이고,
   병합은 건너지 않는다. 분할을 지난 후보(`origin:` 표시)는 옮기지 않고 stale 이다. 겹치는 넓이로 대상을 고르지 않는다(ADR-0113 §4,
   `silver.parcel_lineage` 계약). 필지는 편집하지 않고(1번), 행정경계 id 는 코드가 바뀌어도 같으며(ADR-0113 §9), 산업단지와 건물에는
   계보가 없다. 그래서 오늘은 옮기는 유닛이 없고, 계보를 가진 유닛이 편집을 받을 때 옮김 코드를 짓는다. 30527 의 공식 짝이
   `silver.parcel_lineage` 에 들어오면(ADR-0113 등급 `official`, ADR-0144 §3.3) 번호만 바뀐 필지도 같은 길로 옮겨진다.

6. **stale 은 손님에게 공식 폴리곤을 보이고, Gold 에 목록으로 남고, Slack 으로 알린다.**
   - 서빙 Gold 와 타일에는 stale 편집이 없다. 그 자리에는 원천의 폴리곤이 있거나(원천이 가진 id) 아무것도 없다.
   - Gold 표 `gold.map_edit_stale` 은 편집의 결과가 바뀔 때만 행을 덧붙인다. 행은 유닛, `change_seq`, `feature_id`, 결과(`stale`
     또는 `applied`), 까닭, 판정한 빌드를 담는다. 지금의 목록은 유닛·feature 마다 가장 새 편집이 stale 인 것이다.
   - 접기는 그 빌드에서 새로 stale 이 된 편집이 있으면 `#alerts` 에 유닛과 건수를 한 줄로 보낸다. 같은 stale 을 빌드마다 다시
     알리지 않는다.
   - 직원은 그 목록을 보고 지금의 편집 API 로 다시 그린다. 새 편집은 지금 release 에 묶인다. 스튜어드 경로의 새 항목 종류는 만들지
     않는다(ADR-0263 이 편집을 그 경로 밖에 두었다).

7. **필지 타일 굽기는 ADR-0613 과 이 규칙이 둘 다 운영에 오른 뒤에 다시 켠다.** `parcel_tile_bake` 의 `disabled_reason` 이 두 조건을
   모두 적는다. 다시 켜는 PR 은 둘을 확인한다. 필지 Gold 반쪽은 그대로 돈다.

8. **편집 저장소는 D1 에 그대로 둔다.** 옮길지는 서빙 설계(ADR-0616 A6)에서 정한다. 원장이 정본이고 D1 은 접히지 않은 행만
   든다. 그래서 옮기는 일은 다 접고, 쓰는 곳을 바꾸고, 오버레이 주소를 바꾸는 것이다.

## Consequences

- 원천이 편집 밑에서 바뀌면 그 편집은 조용히 틀리지 않고 지도에서 빠진 채 목록과 경보로 보인다. 직원이 다시 그릴 때까지 손님은
  공식 폴리곤을 본다.
- 첫 202609 필지 굽기는 되살린 필지나 덮어쓴 필지를 내지 않는다. 필지 편집이 없고 막혀 있기 때문이다.
- 고치는 곳(다음 PR):
  - foundation-api 와 Worker 의 `parcels` 거부와 그 시험, 저장소 계약.
  - D1 마이그레이션(`base_release_id`)과 API 의 핀 찍기.
  - 원장 계약의 핀 세 칸과 `export-map-edit-handoff`.
  - `served_gold_common.py` 의 판정과 두 서빙 작업(`apply_edits`).
  - `gold.map_edit_stale` 계약과 Slack 한 줄.
  - 시험은 합성 id 만 쓴다:
    - 같은 id 에 체크섬이 바뀌면 stale 이다.
    - 사라진 id 의 upsert 는 되살리지 않고 stale 이다.
    - 없는 id 의 delete 는 stale 이다.
    - 새 feature 는 원천이 그 id 를 가질 때까지 적용된다.
    - 핀을 풀 수 없으면 stale 이다.
    - 같은 입력을 두 번 지으면 같은 결과다.
- 그다음 PR 이 필지 접기 코드를 지운다(1번).
- 이 규칙이 다루지 않는 것: 행정경계·산업단지 upsert 가 얼리는 속성(`canonical_code`·`official_complex_code`)이 코드 변경 뒤 원천의
  코드와 부딪히면 그 유닛의 접기가 `ValueError` 로 멈춘다. 핀은 기하만 보므로 그런 편집은 "그대로"로 판정된다. 따로 정한다.
- 되돌리기: 이 규칙은 다음 빌드부터만 작용한다. 옛 release 는 R2 에 그대로 있으므로 manifest 포인터를 옮기면 된다.
