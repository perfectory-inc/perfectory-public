# ADR 0252: 건물 종류는 표제부·층·호 셋이고, 호 문서는 PNU 의 건물 하나다

- Status: Proposed
- Date: 2026-10-15
- Applies: [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md) §1·§2·§6·§7·§9 (종류 = 계약 한 줄),
  [ADR-0204](./0204-a-unit-price-document-is-one-building-of-one-pnu.md) (문서 키 `(pnu, building)`),
  [ADR-0233](./0233-every-by-pnu-pack-is-read-by-one-rule-head-then-range.md) (읽기 규칙 하나, `PackWriter` 의 문서 상한)
- Keeps: [ADR-0125](./0125-unit-building-links-require-source-parent-key-evidence.md) §4 (소비 경계의 승인 원장 재대조)

## Context

`gold.building_panel` 은 PNU 하나에 표제부·층·호(호 면적 포함)를 한 문서로 묶는다. ADR-0193 §9 는 종류를 계약 한 줄로
만들었고, 필지 종류 여섯은 그 줄로 Gold·재생성·생산자·레인을 얻었다. 건물 쪽에서 정할 것이 셋 남았다.

1. **호 문서의 크기.** 운영 Gold 를 Trino 로 잰 값(2026-10-14, 읽기 전용): 건물 하나의 호 목록이 gzip 최대 약 245KB,
   `PackWriter` 문서 상한(`request_cpu_budget.document_max_bytes`, 256KiB)의 93% 다. PNU 하나는 대단지라면 그 몇 배다.
   PNU 를 키로 두면 어떤 대단지도 구울 수 없다.
2. **호의 건물 연결 근거.** 호 섹션의 각 호는 `building_link_evidence`(원천 행·출처·입력 sha256·승인 id)를 싣는다. 묶음
   경로는 굽는 순간 이것을 활성 승인 원장과 다시 대조하고(`building_document.rs`, ADR-0125 §4) 공개 문서에서는 뺀다.
   종류 레인의 일반 렌더러(`pack_lane_document.rs`)는 `*_json` 을 그대로 낸다: 대조가 빠지고 근거가 공개된다. 근거의
   sha256 은 압축되지 않는 값이라 호 하나 gzip 의 큰 몫이기도 하다.
3. **호 면적과 전유부.** 원천 등록부는 다섯이다: 표제부, 층, 호(`building_register_units`), 호 면적
   (`building_register_unit_areas`, 1.14억 행, 호당 약 6 행), 전유부 일괄본(`building_register_exclusive_unit`).

## Decision

1. **건물 종류는 셋이다.** 모두 호스트 `building_by_pnu_gateway`, ADR-0193 §9 의 계약 한 줄.

   | 레인 | 섹션 | 문서 키 | 따르는 레인 |
   |---|---|---|---|
   | `building-titles` | `titles` | `pnu` | 묶음(`building`), 표제부가 옮겨지면 기준 |
   | `building-floors` | `floors` | `pnu` | `building-titles` |
   | `building-units` | `units` | `(pnu, building_id)`, `document_key_suffix` | `building-titles` |

   종류는 원천 값만 싣는다(ADR-0193 §5, 가드 `data-knows-no-screen`): 층 표시 문구·호 층 이름·옥탑 용도 이어 붙이기는
   묶음에만 있고 퇴역과 함께 사라진다.
2. **호 문서는 PNU 의 건물 하나다(ADR-0204 과 같은 키).** `building_id` 는 표제부 카탈로그 id, 건물에 연결되지 않은 호는
   nil UUID 문서 하나로 모인다. Gold `gold.building_units` 의 grain 은 처음부터 `(pnu, building_id)` 이다 — PNU grain 으로
   운영에 한 번이라도 만들면 나중의 grain 변경은 배포 이주가 거부한다. 그래서 호 종류의 계약 줄은 키 접미
   (`document_key_suffix`, ADR-0204 구현)가 main 에 들어온 뒤에 들어간다. 호와 호 가격은 같은 `(pnu, building_id)` 키로
   만난다. 무엇을 언제 읽어 어떻게 그릴지는 웹의 결정이다(ADR-0193 §5).
3. **건물 연결 근거는 Gold 에 남고 공개 문서에는 없다. 대조는 굽기가 한다.** 일반 렌더러는 건물 호스트의 호 종류에서
   묶음 경로와 같은 함수(`ApprovedBuildingLinks::validate`)로 각 호의 근거를 활성 승인 원장과 대조하고, 근거 칸을 빼고
   쓴다. 대조가 실패하면 묶음 경로와 같이 굽기를 거부한다(연결을 고치는 일은 Gold 의 몫이다). 두 번째 검증기는 만들지
   않는다.
4. **호 면적은 호 안에 남는다(ADR-0193 §1 의 예외 유지).** 호가 싣는 면적 값은 전용면적 합·주 용도·구조 셋이다. 이것은
   값이지 문구가 아니다. 면적 행 전부(호당 약 6 행)를 레인으로 내면 건물 키로도 최대 건물이 상한을 넘는다고 추정되고,
   그것을 그리는 화면이 없다. 면적 내역이 필요해지면 그때 키를 정해 종류 한 줄을 더한다.
5. **전유부 일괄본은 레인이 아니다.** 같은 사실(전유부의 호)이 `building_register_units` 로 이미 호 종류에 있다. 두 레인에
   같은 사실을 싣지 않는다(ADR-0193 §1). 전유부 일괄본은 호 공시가격을 호에 잇는 다리로만 쓴다.
6. **넘침.** 문서 상한은 `PackWriter` 가 지킨다(ADR-0233 §3): 넘는 건물이 생기면 굽기가 멈추고 서빙 중인 세대는 그대로다.
   굽기 실행 줄은 `max_document_bytes=` 를 함께 남기고, 레인의 최대 문서가 상한의 75% 를 넘으면 그 레인의 감시가 경고한다.
   경고가 오면 키를 더 좁히는 결정을 ADR 로 남긴다.
7. **목표(운영 규모).** ADR-0193 §7 의 줄로 잰다.

   | 대상 | 표제부 | 층 | 호 |
   |---|---|---|---|
   | Gold 전량 (`gold-kind <표> mode=full seconds=`) | ≤ 10 분 | ≤ 10 분 | ≤ 20 분 |
   | Gold 증분, 월간 릴리스 하나 | ≤ 10 분 | ≤ 10 분 | ≤ 10 분 |
   | 굽기 전량 세대 (`pack-bake <레인> mode=generation`) | ≤ 30 분 | ≤ 30 분 | ≤ 30 분 |
   | 굽기 패치 | ≤ 15 분 | ≤ 15 분 | ≤ 15 분 |
   | 최대 문서 gzip (`max_document_bytes=`) | 측정 | 측정 | ≤ 상한의 75% |

   기준점: 묶음 Gold 전량 39–40 분(ADR-0180), 호 가격 Gold 전량 306 초(ADR-0204).
8. **전환과 퇴역.** 종류마다 ADR-0193 §6 순서(Gold·대등성 → 레인·관문 (가)(나) → 웹)를 거친다. 묶음은 네 종류(표제부·층·호·
   호 가격)가 모두 웹의 정본이 될 때까지 서빙한다. 그 뒤 재생성 계약에서 `gold.building_panel` 을 빼고 굽기를 멈추고, 옛
   경로를 한 달 서빙한 뒤 Worker 경로와 객체를 지운다(삭제는 별도 승인). 되돌리기는 레인마다 직전 manifest
   (`manifest-history`)다. 묶음에서 호 가격을 빼는 일(ADR-0190, 문서 v3)은 호 가격 레인이 웹의 정본이 된 뒤에만 한다.

## Consequences

- 건물 묶음 하나가 레인 넷(표제부·층·호·호 가격)이 된다. 월간 HUB 릴리스는 바뀐 종류의 Gold 와 굽기만 부른다.
- 호 레인은 묶음 형식 2판(키 접미)을 쓰는 두 번째 레인이 된다. 새 형식은 없다.
- 공개 호 문서에 건물 연결 근거가 없다. 근거는 Gold 와 그 계보로 본다.
- 일반 렌더러가 건물 호스트의 호 종류에 한해 근거 대조를 부른다. 종류 규칙의 유일한 레인별 분기이고, 이유는 ADR-0125 다.

### 기각한 대안

- **호를 PNU 키로 두고 조각으로 나눈다:** ADR-0233 이 조각을 은퇴시켰고, 문서 하나가 상한을 넘는 문제는 조각이 풀지 못한다.
- **호를 층 단위 문서로:** 화면은 건물의 호 목록을 펼친다. 층 키는 요청을 층 수만큼 늘린다.
- **근거를 공개 문서에 그대로 둔다:** ADR-0125 의 소비 경계 대조가 사라지고, 크기의 큰 몫을 쓴다.
- **호 면적·전유부를 각자의 레인으로:** 결정 4·5.
