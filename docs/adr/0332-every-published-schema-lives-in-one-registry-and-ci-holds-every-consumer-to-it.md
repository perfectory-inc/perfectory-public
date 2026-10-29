# ADR 0332: 발행하는 모든 스키마는 레지스트리 한 곳에 살고, CI 는 모든 소비자를 그 호환성 규칙에 묶는다

- Status: Proposed
- Date: 2026-10-29
- Builds on: [ADR-0193](./0193-each-data-kind-is-its-own-gold-table-and-by-pnu-lane.md)(종류별 레인·문서 버전 이름 유도), [ADR-0320](./0320-a-document-schema-bump-bakes-only-the-documents-it-changes.md)(expand/contract, `document_schema_expands`), [ADR-0123](./0123-data-contracts-are-generated-odcs-from-the-two-sources-that-already-own-the-facts.md)(ODCS 는 두 정본에서 생성), [ADR-0327](./0327-a-worker-deploy-and-a-quarter-hourly-probe-read-what-users-read-twice.md)(배포 스모크·상시 탐침)
- 관련 사건: 2026-10-27~29 — 공매 by-PNU 문서가 `parcel_public_auction_by_pnu.v1` → `.v2`(사진 색인, #877)로, 필지 문서가 `parcel_by_pnu_profile.v1` → `.v2` 로 바뀌자 소비자 넷이 **각자** 깨졌다: (1) 매시 서빙 모니터가 매시 실패, (2) 전체 세대 발행이 옛 manifest 기록을 복사해 v2 문서 위에 v1 을 적음(#929), (3) 옛 빌드의 웹 미리보기가 v2 를 거부, (4) Worker 배포 게이트가 스모크(모니터) 실패로 세 번 되돌림.

## Context

**"무엇이 유효한 문서인가"를 정하는 곳이 열 군데 넘는다.** 2026-10-29 main 의 전수 조사(Rust·Python·셸·TS·계약):

| 자리 | 무엇을 정하나 | 형식 |
|---|---|---|
| Rust `parcel_document.rs:26,32` | 필지 문서 버전·expands 상수 | 문자열 상수 |
| Rust `foundation-contracts/src/building_panel.rs:13,19,26` | 건물 문서·페이지 버전·expands | 문자열 상수 |
| `config/r2-connections.contract.json` `by_pnu_lanes` | 종류 레인 버전(셋만 명시, 나머지는 `by_pnu_gateway_contract.rs:254-304` 가 유도) | JSON 계약 |
| `contracts/by-pnu-documents/*.json`(16개) | 버전별 필드 목록 `{path, required}` | **사람이 손으로 커밋**하는 자체 형식, 생성기 없음 |
| Rust 시험 `by_pnu_document_records.rs` | utoipa `ToSchema` 모양 = 기록 | 시험 |
| Python `by_pnu_kinds.py:22` | 가장 새 기록 파일 | 파일 이름 정렬 |
| 셸 `by-pnu-serving-bake.sh:319-353` | 같음/확장/파괴 판정 | 셸 속 Python |
| Worker `foundation-by-pnu-gateway/src` | manifest 버전만, 문서 버전은 보지 않음 | TS |
| 웹 `lib/api/{parcels,building-edge,public-auction,unit-price-edge}.ts` | 손으로 쓴 zod 4개: 임의 문자열·enum(v2/v3/v4)·enum(v1/v2)·literal(v1) | zod |
| 웹 `edge-contract.test.ts` | 건물·단가만 생산자와 대조(공매·필지는 없음) | vitest |

ODCS(ADR-0123, `contracts/data/*.odcs.yaml` 52개)는 Gold **표**만 다룬다. 문서를 담은 열은 불투명 문자열이다
(`gold.parcel_public_auction.odcs.yaml:28-31` `public_auction_json: string`). 이벤트는 다르다: Avro `.avsc` 4개가
Karapace(Confluent Schema Registry 호환)로 CI 에서 검사된다(`foundation-ci.yml` `kafka-integration`). 즉 **이벤트에는
레지스트리가 있고, 사용자에게 나가는 문서에는 없다.**

네 사고의 공통 원인은 하나다: **버전 문자열 하나가 "모양"과 "호환성"을 동시에 뜻했고**, 소비자마다 그 문자열의
허용 목록을 따로 들고 있었다. v1→v2 는 선택 필드 추가(ADR-0320 의 expand)였으므로 옛 소비자가 읽어도 안전했지만,
문자열이 바뀌었다는 이유만으로 모니터·manifest 기록·웹·게이트가 각자 거부했다.

대기업·오픈소스 사례:
- **Confluent Schema Registry**: 주제(subject)마다 버전 목록과 호환 모드(BACKWARD·FORWARD·FULL, 각 `_TRANSITIVE`)를
  두고, 등록 시 모드를 어기는 스키마를 거부한다. 소비자는 주제를 읽고, 버전은 호환 모드가 보증한다.
- **Buf**(`buf breaking`): protobuf 변경을 PR 에서 FILE/PACKAGE/WIRE 규칙으로 막는다. 스키마가 정본, 코드는 생성.
- **Pact**(consumer-driven contracts) + Pact Broker `can-i-deploy`: 소비자의 기대를 기록하고, **배포된** 소비자 판과
  생산자 판의 행렬로 배포 가능 여부를 묻는다. 10-29 의 웹 미리보기 사고는 바로 "배포된 옛 소비자" 문제다.
- Stripe API 버전: 추가는 버전 없이, 파괴만 새 버전.

## Decision

1. **레지스트리 한 곳.** `platforms/foundation-platform/contracts/schema-registry/` 가 사용자·하류로 나가는 모든
   스키마의 정본이다.
   - `registry.json`: 주제마다 `subject`, `format`, `compatibility`, `major`, `owner`(레인·생산자), `consumers[]`,
     `revisions[]`(append-only).
   - `<subject>/<major>/<revision>.schema.json`: 각 판. 한 번 들어간 파일은 바뀌지 않는다(지금의
     `by-pnu-document-records-are-append-only.sh` 규칙을 그대로 옮긴다).
   - 첫 범위: by-PNU 문서(필지·건물·건물 페이지·종류 레인 8), by-PNU manifest, 단면 팩 manifest. 다음 범위: Gold 의
     JSON 열(ODCS 가 이 주제를 참조), Avro 이벤트(현 `.avsc` 를 같은 레지스트리 목록에 `format: AVRO` 로 등록).
   - `contracts/by-pnu-documents/` 는 레지스트리에서 **생성되는 산출물**이 되었다가(이행 2단계) 소비자가 모두 옮긴 뒤
     읽는 사람이 없어지면 은퇴한다.
2. **형식: JSON 문서는 JSON Schema 2020-12, 이벤트는 Avro 를 유지, protobuf 는 쓰지 않는다.**

   | 형식 | 판단 |
   |---|---|
   | **JSON Schema 2020-12** | **채택(JSON 문서)**. 문서는 브라우저·Worker 가 JSON 으로 읽는다. Confluent SR 이 JSON Schema 호환 규칙을 정의한다. Rust(utoipa 가 이미 OpenAPI 3.1 = JSON Schema 2020-12 를 낸다)·Python(`jsonschema`)·TS(zod 4 `z.toJSONSchema`, 그리고 스키마→zod 생성) 모두 1급 |
   | Avro | 이벤트(Kafka)에 이미 쓰며 유지. 문서에 쓰면 브라우저가 Avro 를 풀어야 하고, JSON 인코딩 Avro 는 union 표기가 사람이 읽는 JSON 과 다르다 |
   | protobuf + Buf | 기각(문서): 서빙이 JSON 이라 와이어 이득이 없고, `buf breaking` 의 장점(필드 번호 규칙)은 JSON 에서 의미가 없다. 지도 타일 MVT 는 별개(prost, 외부 표준)다 |
   | ODCS 로 통합 | 기각: ODCS 는 표·열 계약(품질·SLA·소유)이고 중첩 문서 모양의 호환성 의미론이 없다. ODCS 는 레지스트리 주제를 **참조**한다(`customProperties.schema_subject`) |
3. **호환 모드는 주제마다, 기본은 `FULL_TRANSITIVE`.** 사용자에게 나가는 문서는 옛 소비자(캐시된 웹 빌드, 되돌린 Worker,
   옛 모니터)와 옛 문서(아직 서빙 중인 세대)가 동시에 산다. 그러므로 새 판은 모든 이전 판과 양방향 호환이어야 한다.
   허용되는 변경: 선택 필드 추가, enum 값 추가는 소비자가 모르는 값을 견디는 경우에만(`x-open-enum: true`).
   파괴 변경(필수 필드 추가·삭제·타입 변경·필드 의미 변경)은 **새 major** 로만 하며 ADR-0320 의 expand/contract 가
   major 사이의 이행 절차가 된다.
4. **문서는 major 만 검사받는다.** 문서의 `schema_version` 은 `<subject>.v<major>` 로 고정되고, 판은 별도 필드
   `schema_revision`(정수)로 실린다. 모니터·Worker·웹·발행기는 **주제와 major 가 맞으면 받아들이고** 판은 정보로만
   쓴다 — FULL_TRANSITIVE 가 그것을 보증하기 때문이다. 이것이 네 사고를 한 번에 없앤다: v1→v2 같은 확장은 major
   를 바꾸지 않으므로 어느 소비자의 허용 목록도 바뀌지 않는다.
5. **모든 생산자·소비자는 레지스트리에서 나온다(생성 또는 빌드 시 읽기).** 손으로 쓴 버전 목록은 없다.

   | 참가자 | 방식 |
   |---|---|
   | Rust 발행기·모니터·`foundation-contracts` | `build.rs` 가 `registry.json` 을 읽어 주제·major·현재 판 상수를 생성. 생산 타입(utoipa `ToSchema`)이 현재 판 스키마를 만족하는지 시험(지금의 기록 대조 시험의 대체) |
   | manifest 기록(#929) | 발행기는 생성된 상수(구운 판)만 기록할 수 있고, 옛 기록 복사 경로는 타입으로 막는다(manifest 의 판 필드는 생성 상수 타입) |
   | Python Spark·셸 판정 | `registry.json` 을 읽는 작은 모듈 하나(`by_pnu_kinds.py` 를 대체), 셸의 같음/확장/파괴 판정은 레지스트리의 호환 판정을 부른다 |
   | Worker | 빌드 시 `registry.json` 을 번들(wrangler JSON import)해 주제·major 를 검사 |
   | 웹 | `pnpm gen:schemas` 가 레지스트리에서 zod 를 생성(`lib/api/*.generated.ts`, openapi-typescript 산출물과 같은 자리). 손 zod 는 생성 스키마를 `extend`/`pick` 만 할 수 있다 |
   | ODCS | 렌더러(`render-data-contracts.py`)가 JSON 열의 주제를 레지스트리에서 읽어 적는다 |
6. **CI 가 강제한다.**
   - `schema-registry --check`: 새 판이 주제의 호환 모드를 지키는지(Confluent 의 JSON Schema 호환 규칙; 구현은
     CI 에 이미 있는 Karapace 의 검사기를 라이브러리로 쓰는 것을 1순위로 하고, 이행 1단계에서 확정), 파일이
     append-only 인지, 생성 산출물이 최신인지.
   - **소비자 주도 계약(Pact 식)**: 각 소비자는 자신이 **읽는 경로**를 `consumers[].reads` 로 기록한다(웹은 zod 4
     `z.toJSONSchema` 로 생성 스키마에서 사용 경로를 뽑는다). CI 는 주제의 모든 살아 있는 판이 각 소비자가 필수로 읽는
     경로를 보장하는지 검사한다. 웹·Worker 의 vitest 와 Rust 시험이 같은 레지스트리 고정 예제(합성 PNU `99999…`)로
     파싱한다.
   - **`can-i-deploy`**: 배포된 소비자 판(웹 미리보기·운영 Worker·모니터 릴리스 커밋)이 받아들이는 주제·major 를
     배포 시 `schema-registry/deployments.json` 이 아니라 **배포 기록이 이미 사는 곳**(Worker 배포 기록·릴리스 기록)
     에서 읽고, 새 major 발행 전에 "읽을 수 없는 배포된 소비자가 있는가"를 묻는다. 있으면 발행이 멈춘다.
7. **SSOT 정리.** `r2-connections.contract.json` 의 `document_schema_version`·`document_schema_expands` 와 Rust
   `*_SCHEMA_VERSION`·`*_EXPANDS` 상수는 레지스트리의 주제 이름을 가리키는 한 줄로 바뀐다. expands 목록은 "같은
   major 의 판들"로 유도되므로 따로 적지 않는다.

## 이행(strangler, 한 주제씩)

| 단계 | 내용 | 끝났다는 증거 |
|---|---|---|
| 0 (이 PR) | 공매 웹 zod 가 생산자 계약의 현재·확장 버전을 모두 받는지 시험(`edge-contract.test.ts`) — 레지스트리 없이도 바로 막을 수 있는 구멍 | vitest 녹색 |
| 1 (**첫 조각**) | 주제 `parcel_public_auction_by_pnu` 하나: `registry.json` + 기존 기록 두 개(v1·v2)를 기계적으로 JSON Schema 로 옮긴 판 1·2(번역기가 기록과 같음을 시험) + `schema-registry --check`(FULL_TRANSITIVE) + 웹 공매 zod 생성 | v1→v2 를 다시 등록하면 통과, 필수 필드 추가를 등록하면 CI 실패 |
| 2 | 같은 주제의 Rust 발행기·모니터·Worker 가 레지스트리 상수 사용, `schema_revision` 필드 추가(그 자체가 FULL 호환 확장), 소비자는 major 만 검사 | 판 3 을 등록해도 모니터·Worker·웹 코드 변경 0 |
| 3 | 단가 → 종류 레인 6 → 필지 → 건물(페이지 포함) 순으로 한 주제씩. 각 주제가 옮겨지면 그 주제의 `contracts/by-pnu-documents` 파일은 생성 산출물이 된다 | 손 버전 목록 0(가드) |
| 4 | manifest 주제, `can-i-deploy` 를 Worker 자동 배포·웹 미리보기 배포에 연결 | 옛 웹 빌드가 못 읽는 major 를 발행하려 하면 멈춤(시험) |
| 5 | Gold JSON 열 주제를 ODCS 가 참조, Avro 이벤트를 같은 목록에 등록 | ODCS `--check` 가 주제 참조를 검사 |

첫 조각 추정: 약 3일(번역기·검사기 1.5, 웹 생성 0.5, CI 배선·검토 1). 공매를 고른 이유: 사고의 레인이고, 판이 둘뿐이며,
소비자가 넷(모니터·Worker·웹·발행기)으로 모든 종류를 대표하고, ADR-0303 으로 이미 자기 굽기 잡이 있어 다른 레인을
건드리지 않는다.

## 네 결정의 전체 순서(2026-10-29 구조 전환 묶음)

이 ADR 은 같은 날 제안된 넷 중 하나다: 스키마(이 ADR), 자산 오케스트레이션
([ADR-0333](./0333-gold-tables-and-serving-lanes-are-partitioned-assets-materialized-by-upstream-partition-changes.md)),
여정 SLO·WAP([ADR-0334](./0334-each-user-journey-has-an-slo-and-every-data-release-is-written-audited-and-canaried-before-it-is-served.md)),
계산 격리([ADR-0335](./0335-batch-compute-leaves-the-serving-host-at-a-declared-trigger.md)). 서로의 첫 조각은 독립이라
나란히 갈 수 있고, 의존은 셋뿐이다: ADR-0334 의 Audit(가)는 이 ADR 의 레지스트리 검증을, ADR-0333 의 파티션 검사는
ADR-0334 의 Audit 를, ADR-0335 의 이전 단위는 ADR-0333 의 자산 파티션을 쓴다.

| 순서 | 조각 | ADR |
|---|---|---|
| 1 | 공매 주제 레지스트리 + 웹 생성(3일) ∥ 단가 자산 분리 + 조정기(5일) ∥ 노드 지표·`isolation_class`(1일) | 0332 · 0333 · 0335 |
| 2 | 공매 여정 SLI·예산(2일) → 공매 발행 WAP 섀도 읽기(3일) | 0334 |
| 3 | 공매 주제를 모든 공매 소비자가 major 로 검사 → 다음 주제들 | 0332 |
| 4 | 계약 `partitions`·생성기 → 필지 패널 `sido` 파티션 → evolve/adopt 판정 | 0333 |
| 5 | 데이터 카나리, 오류 예산 정책을 배포에 | 0334 |
| 6 | 방아쇠 도달 시 배치 호스트로 레인 하나씩 | 0335 |

## 가드(ADR-0001)

- **`schema-registry --check`**: 막는 사고 — 10-29 공매 v2 처럼 버전 문자열이 바뀐 확장이 소비자 넷을 각자 깨뜨린 일;
  호환 모드를 어긴 판은 병합 전에 거부된다.
- **손 버전 목록 금지**(`no-hand-written-schema-versions`, 옮겨진 주제부터): 막는 사고 — 10-29 웹 `public-auction.ts`
  의 손 enum 과 모니터의 manifest 기록이 생산자와 따로 놀아 v2 를 거부한 일.
- **소비자 경로 보장 검사**: 막는 사고 — #929 처럼 전체 세대가 옛 기록을 복사해 소비자가 받을 판을 잘못 안 일.
- **`can-i-deploy`**: 막는 사고 — 10-29 옛 빌드의 웹 미리보기가 v2 를 거부한 일(배포된 소비자를 아무도 묻지 않았다).

## 기각한 대안

- **지금 체계(ADR-0320 expands)를 더 촘촘히**: expands 는 생산자 쪽 선언이다. 소비자가 그 목록을 각자 복사하는 구조는
  남고, 배포된 옛 소비자 문제를 풀지 못한다.
- **Confluent/Karapace 를 운영 서비스로 띄우고 런타임 조회**: 문서 소비자(브라우저·Worker)가 런타임에 레지스트리를
  부르면 가용성이 하나 늘고 지연이 생긴다. 레지스트리는 **빌드 시점 정본**(Buf 의 BSR 처럼 저장소 안)으로 두고,
  Karapace 는 CI 의 검사기로만 쓴다. Kafka 를 다시 운영하면(ADR-0118 ③ 보류) 같은 파일을 Karapace 에 등록한다.
- **Rust 타입이 정본, 스키마는 파생**(schemars/utoipa 만): 생산자가 하나인 주제에는 맞지만, 소비자 쪽 호환 판정이
  "Rust 를 바꾸면 스키마도 바뀐다"가 되어 리뷰 전에 파괴 변경을 막지 못한다. Buf·Confluent 모두 스키마가 먼저다.
  다만 Rust 타입은 시험으로 스키마에 묶이므로 생산 코드의 이중 정의는 생기지 않는다.

## Consequences

- 문서 모양 변경은 "레지스트리에 판 등록 → CI 호환 검사 → 생성 → 생산자 배포" 한 길만 남는다. 확장 변경에는 소비자
  배포가 필요 없다.
- 새 필드 `schema_revision` 과 `schema_version` 의 major 고정은 문서 자체의 확장이므로 이행 2단계에서 한 주제씩 FULL
  호환으로 들어간다. 옛 문자열(`...v2`)은 contract 단계에서 사라진다.
- 비용: 생성기·검사기 유지, CI 에 레지스트리 검사 한 잡(변경이 있을 때만, ADR-0324). 운영 비용 0.
