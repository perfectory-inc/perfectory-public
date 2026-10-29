# ADR 0320: 문서 스키마 판 올림은 바뀐 문서만큼만 굽는다 — 선택 필드만 더하는 판은 서빙 중인 문서 위에 패치로 얹고(확장/수축), 깨는 변경의 범위는 종류 레인이 그 종류로 좁힌다

- Status: Accepted
- Date: 2026-10-28
- Amends: [ADR-0141](./0141-by-pnu-serving-publishes-changed-documents-as-patch-generations.md) §5
  ("기본 세대와 다른 문서 스키마는 전량"), [ADR-0274](./0274-the-by-pnu-bake-does-only-the-work-a-change-needs.md)
  결정 2 ("문서 스키마 변경 시 전량은 그대로다")
- Builds on: [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md) (종류마다 레인),
  [ADR-0151](./0151-building-packs-serve-each-document-whole.md) (Worker 는 저장된 문서를 그대로 내준다),
  #780 (문서 판마다 필드 기록 `contracts/by-pnu-documents`)

## Context

by-PNU 문서는 모두 자기 판을 `schema_version` 으로 싣는다. 판이 바뀌는 조건은 #780 이 기계로 묶었다: 필드가 늘거나
줄거나 이름이 바뀌거나 필수 여부가 바뀌면 새 판이고, 판마다 필드 기록이 `contracts/by-pnu-documents/<판>.json` 에
남는다(`by_pnu_document_records.rs`). 그리고 굽기는 **판이 바뀌면 무조건 전량 세대**다
(`by-pnu-serving-bake.sh` 2단계, `section_packs/publish.rs` 의 패치 거부). 필지 레인은 항목 하나(`documents`)라
필드 하나가 3,986만 문서 전부를 다시 굽는다.

### 지금까지의 판 올림 넷 — 모두 선택 필드 추가였다

이 PR 의 기록 대조(`every_declared_expansion_only_adds_optional_fields`)로 넷 다 "선택 필드만 더함"임을 확인했다.

| 판 올림 | 더한 필드 | 굽기 | 실제로 내용이 바뀐 문서(측정 2026-10-28, Trino) |
|---|---|---|---|
| `parcel_by_pnu_profile` v1→v2 (#780) | `legal_dong`(선택, 안의 `name` 필수), `forest_ledger.ownership_kind_name` | 전량 39,861,511 | 거의 전부(법정동은 거의 모든 PNU 에 붙는다) |
| `parcel_forest_ledger_by_pnu` v1→v2 (#780) | `ownership_kind_name` | 전량 | 39,700,590 / 39,703,956 (99.99%) |
| `parcel_public_auction_by_pnu` v1→v2 (#877) | `rounds[].photos` | 전량 4,613 | 4,613 전부(Gold 가 사진 없는 회차에도 `photos` 칸을 쓴다). 사진이 있는 문서는 **6 (0.13%)** |
| `building_by_pnu_profile` v2→v4 | `purpose_name`, `structure_name` | 전량 5,945,767 | 거의 전부(동마다 이름이 있다) |

넷 다 새 칸이 거의 모든 문서에 붙어서 전량이 곧 "바뀐 만큼"이었다. 공매는 값이 있는 문서가 6 인데도 Gold 가 빈 목록을
모든 행에 써서 전부가 바뀌었다. 지금까지 전량이 비례했던 것은 우연이다. 규칙은 필드가 몇 문서에 닿는지 보지 않는다. 필지 패널 문서의 항목별 채움(측정 2026-10-28, `gold.parcel_panel` 39,861,511 행):

| 항목 | 비어 있지 않은 PNU | 비율 |
|---|---:|---:|
| `zonings` | 39,729,618 | 99.7% |
| `transfer_history` | 39,802,842 | 99.9% |
| `forest_ledger` | 39,703,956 | 99.6% |
| `characteristics` | 35,784,044 | 89.8% |
| `price` | 35,784,007 | 89.8% |
| `land_rights` | 422,957 | **1.06%** |

대지권 항목에 선택 필드 하나를 더하면 오늘은 3,986만 문서 전량(85분 5초, ADR-0274 의 10-16 실측)이고, 내용이 바뀌는
문서는 많아야 422,957 이다. ADR-0274 의 잰 패치 속도(고정 약 5분 22초 + 4.7천/초)로 그 패치는 **약 6.9분**(계산)이다.
공매 사진도 빈 목록을 쓰지 않았다면 4,613 전량 대신 6 문서 패치였다.

### 무엇이 이미 두 판을 견디는가

- **Worker** 는 묶음의 문서를 저장된 바이트 그대로 내준다(ADR-0151). 문서의 판을 읽지 않는다. manifest 의 패치 줄에서
  `patch`·`units` 만 읽고 모르는 칸은 무시한다(`packs.ts` `parseSectionPacks`). 한 세대 안에 두 판의 문서가 있어도
  서빙은 그대로다.
- **소비자**: 웹 필지 읽기(`parcels.ts`)는 `schema_version` 을 문자열로만 받고 모든 칸이 `nullish` 다. 건물
  (`building-edge.ts`)은 v2·v3·v4 를, 공매(`public-auction.ts`)는 v1·v2 를 받는다.
- **발행기**만 판이 하나라고 가정한다: 서빙 상태의 판은 기본 세대의 한 값이고, 그 값과 다른 판의 패치를 거부한다.

### 두 선택지

- **(가) 확장/수축(expand/contract).** 새 판이 옛 판에 선택 필드만 더한다면, 옛 판 문서는 새 판 독자에게도 올바른
  문서다(새 필드가 없음 = 그 판이 쓰지 않았음). 그러면 판 올림은 서빙 중인 문서를 다시 쓸 이유가 아니다. 변경 집합
  (Gold `row_digest`, ADR-0141)이 바뀐 문서만 새 판으로 다시 굽고, 나머지는 다음 전량 세대(압축)가 새 판으로 바꾼다.
  비용 = 내용이 바뀐 문서 수. 모든 레인(패널·종류·호 가격)에 지금 바로 걸린다.
- **(나) 종류마다 항목.** 패널 한 문서 안에 항목을 나누면 Worker 가 요청마다 합쳐야 한다 — ADR-0151 이 측정으로
  기각했고(CPU p99 19.8 ms), ADR-0274 결정 3 이 다시 확인했다. 대신 ADR-0193 이 종류를 **레인**으로 나눴고, 필지
  종류 레인 일곱이 이미 계약에 있다. 남은 일은 웹이 종류 레인을 읽고 패널 레인을 은퇴하는 것(ADR-0193 §5·§6,
  ADR-0274 결정 3)이다. 그 뒤에는 깨는 변경(필드 이름 바꿈·지움·필수로 바꿈)도 그 종류의 문서만 다시 굽는다
  (예: 대지권 422,957, 패널 3,986만이 아니다). 건물 종류(표제부·층·호, ADR-0252)는 아직 Gold 도 레인도 없다.

(가)는 선택 필드 추가 — 지금까지의 판 올림 전부 — 를 문서 단위로 비례시키고 레인 구조를 건드리지 않는다. (나)는
깨는 변경의 범위를 종류로 좁히지만 웹 전환·관문·은퇴가 종류마다 필요하고(ADR-0193 §6), (가)가 없으면 종류 레인도
판을 올릴 때마다 그 레인 전량이다(산림대장 v2, 공매 v2 가 그랬다). 둘은 서로를 대신하지 않는다. 이 ADR 은 둘을
정하고 (가)를 구현한다. (나)는 ADR-0193 의 순서를 그대로 따른다.

## Decision

1. **판은 자기가 확장하는 앞 판을 선언한다.** 종류 레인은 계약 줄의
   `document_schema_expands`(같은 문서 이름의 앞 판 목록), 패널은 판 상수 옆의 `PARCEL_DOCUMENT_SCHEMA_EXPANDS`·
   `BUILDING_DOCUMENT_SCHEMA_EXPANDS` 에 적는다. 판을 적는 곳이 이미 그 둘이라 같은 자리에 둔다. 선언은 기록이
   증명한다. 시험 `every_declared_expansion_only_adds_optional_fields` 는 다음 세 가지를 확인한다:
   - 선언한 앞 판마다 기록이 있다.
   - 옛 판의 모든 경로가 새 판에 있고, 선택이던 경로가 필수가 되지 않는다.
   - 새로 생긴 필수 경로는 새로 생긴 선택 경로 아래에만 있다(새 선택 객체는 자기 부분을 필수로 가질 수 있다).

   기록은 경로와 필수 여부만 담는다. 경로가 같고 형(type)만 바뀌는 변경은 확장이 아니다. 그런 판은 아무것도
   선언하지 않고, 첫 굽기는 전량이다. 처음 선언은 위 표의 넷이다(건물 v3 은 기록이 없어 v4 는 v2 만 확장한다).
   이 시험이 막는 사고: 2026-10-19 #780 의 v2 발행기가 v1 로 구운 세대 4 를 거부해 필지 Worker 배포가 되돌려졌다 —
   서빙 중인 판을 읽지 못하는 릴리스. 확장이 아닌 판을 확장으로 선언하면 같은 일이 패치마다 생긴다.
2. **manifest 는 패치마다 그 문서의 판을 적는다.** `section_packs.patches[].document_schema_version` 은 기본 세대의 판과
   다를 때만 있다. 없으면 기본 세대의 판이고, 이 결정 전에 쓴 manifest 는 그대로 읽힌다. 서빙 상태의 판 집합 =
   기본 세대의 판 ∪ 패치들의 판(`served_document_schema_versions`), 가장 새 문서의 판 = 가장 새 패치의 판
   (`newest_document_schema_version`). 레인 상태(`show-*-by-pnu-serving-state`)가 둘을 보고하고, 이 릴리스가 읽는 판
   (`readable_document_schema_versions` = 굽는 판 + 선언한 앞 판)도 보고한다. 발행기의 묶음 읽기(`inspect`·감시)는
   문서를 상태의 판 집합 중 하나로 확인한다. Worker 는 바뀌지 않는다.
3. **굽기의 판단은 같음 / 확장 / 깸, 셋이다(`by-pnu-serving-bake.sh` 2단계).**
   - 서빙 판 집합이 굽는 판 하나뿐: 같음, 지금과 같다.
   - 집합이 모두 읽는 판 안: 확장. 판은 굽기 방식을 정하지 않고, 변경 집합이 reflect·패치·전량을 정한다. 다른 전량
     조건(강제, `max_patches` 7, 누적 비율 0.25, 접두 길이)은 그대로다. 실행 요약에 `document_schema_expansion` 을 남긴다.
   - 그 밖: 깸, 지금처럼 전량 세대.

   발행기도 따로 지킨다: 읽지 않는 판의 문서가 서빙 중이면 패치를 거부한다(`section_packs/publish.rs` `patched`).
   객체 레인은 판 하나만 담으므로 같음/깸뿐이다.
4. **새 판의 첫 패치는 바뀌지 않은 문서의 표본으로 내용을 확인한다(`section_packs/expansion.rs`).** 기록은 모양을
   증명할 뿐이다. 옛 판 문서를 그대로 두는 것이 옳으려면, 새 렌더러가 바뀌지 않은 Gold 행에서 서빙 중인 문서와 같은
   내용을 만들어야 한다. 그래서 가장 새 문서의 판과 다른 판을 굽는 패치는 쓰기 전에 다음을 한다:
   - 변경 집합에 없는 행에서 관문 (나) 표본과 같은 해시 추첨으로 약 2,000 문서를 뽑는다.
   - 새 판으로 렌더하고, 서빙 답(Worker 와 같은 읽기)과 비교한다. `schema_version`·`source` 는 빼고, 값이 null 인
     칸은 없는 칸으로 읽는다.
   - 하나라도 다르면 패치를 거부한다. 묶음은 하나도 쓰지 않는다.

   이 검사가 막는 사고: 오류 없는 낡은 답이다. 2026-10-16 22:49 부터 per-PNU 객체가 서빙 문서와 달라졌는데도 아무것도
   실패하지 않았고, 매시간 감시가 "객체와 차이"로 실패한 뒤에야 보였다(ADR-0274). 새 판이 옛 문서를 실제로 바꾸는데
   패치가 그것을 건너뛰면 같은 종류의 낡음이다.

   거부 문구는 고칠 길 둘을 적는다: 새 필드를 그 값을 채우는 Gold 행 변경과 함께 들이거나, 사유와 함께 전량
   세대(`FOUNDATION_BY_PNU_BAKE_FORCE_FULL`)를 굽는 것. 바뀌지 않은 문서 중 비율 f 를 다르게 만드는 렌더러가
   표본을 빠져나갈 확률은 (1−f)^2000 이다(f = 0.25% 에서 1% 미만). 빠져나간 것도 다음 전량 세대가 새 판으로 다시
   굽는다. 생산자 규칙 둘:
   - 새 필드는 그 값을 채우는 Gold 변경과 같은 변경으로 들어온다(그래야 `row_digest` 가 그 행을 표시한다).
   - 값이 없을 때는 칸을 쓰지 않는다(`skip_serializing_if`; `[]` 도 값이다).
5. **수축은 다음 전량 세대다.** 압축(`max_patches`, 누적 비율, 운영자 강제)이 모든 문서를 가장 새 판으로 다시 굽고,
   서빙 판 집합은 하나로 돌아간다. 판 목록을 열거하는 소비자(`building-edge.ts`, `public-auction.ts`)는 생산자가
   판을 올리기 **전에** 새 판을 받고(확장 먼저), 옛 판은 어떤 레인 상태의 `served_document_schema_versions` 에도
   없을 때만 뺀다.
6. **(나)는 ADR-0193 의 순서 그대로 간다.** 웹이 필지 종류 레인을 읽으면(§5) 패널 레인을 은퇴하고(ADR-0274 결정 3),
   그 뒤 깨는 변경은 그 종류 레인의 전량이다. 건물 종류는 ADR-0252 의 Gold 와 레인이 먼저다. 이 ADR 은 그 순서와
   관문을 바꾸지 않는다.

## Consequences

| 판 올림 | 전 | 후 |
|---|---|---|
| 공매 사진(v2), 지금의 Gold(빈 `photos` 를 씀) | 전량 4,613 문서 | 4,613 변경 → 누적 비율을 넘어 전량(같다) |
| 공매 사진(v2), 4항 생산자 규칙대로(빈 목록을 쓰지 않음) | 전량 4,613 문서 | 패치 6 문서(측정한 사진 있는 문서) |
| 대지권 항목에 선택 필드(패널) | 전량 39,861,511 문서, 85분(실측 속도) | 패치 ≤ 422,957 문서, 약 6.9분(계산) |
| 산림대장 소유구분(v2) | 전량 39,703,956 | 바뀐 문서 39,700,590 → 누적 비율을 넘어 전량(같다, 진짜 변경) |
| 필지 법정동(v2) | 전량 39,861,511 | 거의 전부 변경 → 전량(같다) |
| 깨는 변경(이름 바꿈) | 그 레인 전량 | 그 레인 전량; 패널 은퇴 뒤에는 그 종류 레인만(6항) |

- 한 세대에 두 판의 문서가 함께 있는 기간이 생긴다. 문서마다 자기 판을 싣고 있으므로 소비자는 문서를 보고 안다.
- 새 판의 첫 패치는 표본 비교만큼 느려진다: 2,000 문서의 렌더와, 그 문서들이 놓인 동의 묶음 읽기(묶음 수만큼의
  GET, R2 Class B 는 백만 건당 과금이라 무시할 만하다).
- manifest 의 패치 줄에 칸이 하나 는다. Worker·옛 발행기는 무시한다.
- 위험: 기록이 담지 않는 형 변경을 확장으로 선언하면 1항 시험은 통과하고 4항 표본만 남는다. 표본은 확률이다. 그래서
  형 변경은 선언하지 않는다는 규칙을 1항 시험의 문구가 적는다.

### 기각한 대안

| 대안 | 판정 |
|---|---|
| Worker 가 옛 판 문서를 요청 때 새 판으로 변환(Stripe 의 버전 변환 모듈) | 기각. 요청마다 파싱·직렬화하는 CPU 는 ADR-0151 이 버린 합치기와 같은 비용이고, 관문 (가)가 지키는 "저장된 바이트 그대로"가 깨진다. |
| 판 올림마다 전량을 두고 손익분기만 낮춤 | 기각. 필드가 닿는 문서가 0.13% 여도 전량이다. |
| 모든 행을 새 판으로 렌더해 서빙 문서와 전수 비교하고 다른 것만 씀 | 기각. 쓰기는 비례하지만 렌더·읽기는 레인 전량이다. 표본(4항)이 같은 결함을 잡고, 놓친 것은 수축이 고친다. |
| 패널 문서를 묶음 안 여러 항목으로 | 기각(ADR-0151, ADR-0274 결정 3). 종류 레인이 그 단위다. |

### 참고한 사례

- Apache Iceberg 스키마 진화 — 열을 더해도 데이터 파일을 다시 쓰지 않고, 옛 파일은 새 열을 null 로 읽는다. 다시
  쓰기는 나중의 압축이 한다. 이 결정의 모양 그대로다. <https://iceberg.apache.org/docs/latest/evolution/>
- Confluent Schema Registry BACKWARD 호환 · Avro 스키마 해석 — 새 스키마의 독자가 옛 데이터를 읽을 수 있으려면 더한
  필드에 기본값이 있어야 한다(1항의 규칙).
  <https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html>,
  <https://avro.apache.org/docs/1.11.1/specification/#schema-resolution>
- Protocol Buffers "Updating a message type" — 필드 추가는 호환, 없는 필드는 기본값. <https://protobuf.dev/programming-guides/proto3/#updating>
- Stripe, *Online migrations at scale* — 이중 쓰기 → 읽기 전환 → 옛 것 제거의 4단계. <https://stripe.com/blog/online-migrations>
- Martin Fowler, *ParallelChange* (expand / migrate / contract). <https://martinfowler.com/bliki/ParallelChange.html>
- Kubernetes CRD 버전 — 여러 버전을 함께 서빙하고 저장 버전 이전은 따로 한다.
  <https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definition-versioning/>
