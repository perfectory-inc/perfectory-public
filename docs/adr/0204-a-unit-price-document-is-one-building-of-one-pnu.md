# ADR 0204: 호 공시가격 문서는 PNU 하나가 아니라 PNU 의 건물 하나다

- Status: Accepted
- Date: 2026-10-13
- Amends: [ADR-0190](./0190-unit-prices-are-their-own-gold-panel-and-by-pnu-lane.md) (레인의 문서 키·Gold 의 grain),
  [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md) (묶음 색인의 키 너비)
- Applies: [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md) §7 ("PNU 문서 하나가 gzip 256 KiB 를
  넘는 종류는 … 문서 키를 좁힌다(예: 호를 `pnu` + 건물 id 로) 그리고 이 ADR 을 개정한다")
- Builds on: [ADR-0151](./0151-building-packs-serve-each-document-whole.md), [ADR-0163](./0163-a-large-legal-dong-pack-is-split-into-parts-read-whole.md)

## Context

2026-10-12 `unit-price` 레인의 첫 감독 전환(`by-pnu-pack-operator.sh unit-price cutover 1`)에서 Gold 첫 전량은 통과했고
(47,164행, 306초) 굽기가 첫 샤드에서 거부됐다: 동 조각 하나가 284,460 B 로 통째 읽기 상한
(`read_path.whole_pack_max_bytes`, 262,144 B)을 넘었다. 아무것도 발행되지 않았다.

원인은 조각 수가 아니라 문서 하나의 크기다. 운영 카탈로그에서 잰 값:

| 측정 (2026-10-12, 판 202609) | 값 |
|---|---:|
| 옛 `gold.unit_price_panel` 행(= PNU) | 47,164 |
| 원문 JSON 256 KiB 초과 PNU | 12,200 (26%) |
| 가장 큰 PNU 문서: 원문 / gzip / 호 수 | 7,122,808 B / 596,238 B / 7,374 |
| 호 하나의 gzip 크기(위 문서에서) | 약 81 B |
| 가격 있는 호를 가진 건물 | 169,097 |
| 건물 하나의 가격 있는 호: p99 / 최대 | 248 / 1,829 (≈ 148 KB gzip) |
| 건물에 연결되지 않은 가격 있는 호: PNU 하나에 최대 | 1,359 (≈ 110 KB gzip) |

대단지 하나가 PNU 하나라서, 문서를 PNU 로 두는 한 어떤 조각 규칙(ADR-0163)으로도 256 KiB 안에 넣을 수 없다. 묶음 경로가
통째 읽기를 고집하는 이유(ADR-0151: 머리+범위 읽기의 지연·CPU)도, 화면이 한 번에 600 KB 를 받는 비용도 그대로 남는다.
화면은 어차피 호 가격을 건물 하나의 호 목록을 펼칠 때만 쓴다(ADR-0193 §5).

## Decision

1. **문서 키는 `(pnu, building)` 이다.** `building` 은 그 호가 놓인 건물 문서의 건물 id(`unit_placement` 의
   `building_id`, 표제부 카탈로그 id, UUID 36자)다. 건물에 연결되지 않은 호(`building_id` NULL, 자기 PNU 에 놓임)는 nil
   UUID `00000000-0000-0000-0000-000000000000` 하나의 문서로 모인다. 그래서 웹이 건물 문서에서 보는 건물마다, 그리고
   미연결 호 묶음마다 문서가 하나다.
2. **Gold 는 새 표 `gold.unit_price` 이고 grain 은 `(pnu, building_id)` 한 행이다.** 운영의 `gold.unit_price_panel`(PNU 당 한 행, 행 있음)에 필수 열을 더하는 것은 배포 이주가 backfill 로 표현할 수 없는 grain 변경이라(2026-10-13 그렇게 병합해 모든 배포가 멈췄다) 표 이름을 바꾼다. 새 표는 그 적재가 만들고, 옛 표는 어떤 계약에도 없어 이주가 건드리지 않으며 별도로 퇴역한다. `building_id` 는 NULL 이 아니다(위 nil
   UUID). 호의 배치(`unit_placement`, ADR-0190 의 `pins_from`)·가격 이력·품질 관문은 그대로이고, 고유성 검사와 정렬이
   `(pnu, building_id)` 가 된다. 증분(ADR-0180)의 영향 단위는 여전히 PNU 다: 영향 PNU 의 행을 전부 다시 만든다.
3. **요청 경로는 `/unit-prices/by-pnu/<pnu>/<building_id>` 다.** 계약 `by_pnu_lanes.unit-price` 가 문서 키의 모양을
   한 곳에서 정한다(키 = PNU 뒤에 `/` 와 건물 id). Worker·발행기·웹은 그 정의를 읽는다. capabilities 경로는 그대로다.
4. **묶음 형식 2판은 색인 키의 너비를 머리에 적는다.** 1판(19자 PNU, 항목 28 B)은 그대로이고 필지·건물 레인은 1판을
   계속 쓴다. 2판 색인 키는 `<pnu><building_id>` 55자(ASCII), 정렬·중복 없음·동 접두사 검사는 1판과 같다. 조각
   해시는 키 전체의 `fnv1a32` 라서 한 PNU 의 건물들이 조각에 흩어진다. 동 = 키의 앞 10자.
5. **패치는 PNU 단위로 갈아 끼운다.** 변경 계산(`by_pnu_panel_delta.py`)은 PNU 를 낸다. 영향 PNU 하나의 패치는 그 PNU 의
   새 행 전부를 올리고, 서빙 중인 세대에서 그 PNU 아래 키 중 새 행에 없는 것을 지운다(툼스톤). 지워진 PNU 는 그 아래
   키 전부가 지워진다.
6. **관문.** (가)는 키마다 바이트 대조. (나)의 기준 레인(`building`)은 키의 PNU 로 읽는다. 문서 하나가 통째 읽기
   상한을 넘으면 굽기가 거부한다(지금과 같음); 측정상 최대 문서는 상한의 약 57% 다.
7. **웹은 건물의 호 목록을 펼칠 때 그 건물의 문서를, 미연결 호를 펼칠 때 nil UUID 문서를 읽는다.** 쿼리 키는
   `["by-pnu", "unit-price", pnu, building_id]`. 없음(404 absent)은 "가격 없음" 이다.

## Consequences

- 대단지 PNU 의 가격 요청이 600 KB 한 번에서 건물마다 ~수십 KB 로 준다. 건물을 여러 개 펼치면 요청이 그만큼 는다.
- 문서 수는 PNU 47,164 → (PNU, 건물) 약 17만으로 늘지만 R2 쓰기는 묶음 수가 정한다(ADR-0193 §8); 조각이 조금 늘 뿐이다.
- 묶음 형식이 두 판이 된다. Rust 쓰기·읽기, Worker 읽기, 시험 고정 묶음이 2판을 함께 다룬다.
- `unit-price` 는 아직 서빙 전이라(첫 발행 전) 옮길 서빙·캐시가 없다. 웹의 가격 읽기(아직 미배포 경로)만 바뀐다.
- 구현 순서: Gold grain(Spark·계약) → 묶음 2판 + 발행기(렌더러·굽기·관문·패치) → Worker → 웹 → 감독 전환
  (`by-pnu-pack-operator.sh unit-price cutover <g>`).

### 기각한 대안

- **문서 하나뿐인 조각은 머리+범위 읽기로 서빙** (굽기의 상한 검사만 완화): 코드는 가장 적지만, 화면이 대단지마다
  600 KB 를 받고 ADR-0151 이 피한 머리+범위 경로를 이 레인의 26% PNU 가 탄다.
- **호 범위로 쪼갠 쪽 문서(`?page=`)**: 웹이 건물 단위로 펼치는데 쪽 경계가 건물과 어긋나고, 쪽 수가 판마다 바뀌어 캐시
  키가 흔들린다.
- **건물 id 를 짧은 번호로**: 키 너비는 줄지만 웹이 아는 건물 id(UUID)와 다른 두 번째 식별자가 생긴다.
- **가격 이력 기준일 제한(최근 N년)**: ADR-0101(기준일 전부 보존)과 충돌하고, 상한 문제를 미룰 뿐이다.
