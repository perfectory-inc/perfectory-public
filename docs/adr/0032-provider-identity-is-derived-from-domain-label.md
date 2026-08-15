---
status: current
owner: foundation-platform
doc_type: adr
last_reviewed: 2026-08-15
---

# ADR 0032: 제공기관 ID는 도메인 라벨에서 파생한다

- Status: Accepted
- Date: 2026-08-15
- 대체: [FP-ADR-0014](../../platforms/foundation-platform/docs/adr/0014-bronze-source-slug-canonical-naming.md)의 D2와 D6 중 제공기관 유예
- 유지: FP-ADR-0014의 D1, D3, D4, D5와 D6의 식별자별 예외
- 관련: [ADR-0027 모든 가드는 자기 위협 모델을 선언한다](./0027-every-guard-declares-its-threat-model.md)

## Context

FP-ADR-0014 D2는 제공기관 라벨과 `providerid`의 수동 대응표를 유일한 관리 표로 정했다. 실제
구현은 그 표를 다시 `KNOWN_PROVIDER_IDS`와 `provider_id()`의 `match` arm으로 나눴고, 둘이
어긋나도 컴파일과 테스트가 통과했다. `rt.molit.go.kr`은 D2에 등록되지 않은 채 코드에서만
`rtmolitkr`로 추가됐고, 라벨의 `go`가 빠진 값을 테스트가 기대값으로 고정했다. 목록 둘을
대조하는 검사를 더 붙이면 세 번째 지식 사본이 생길 뿐 이 결함을 만들 수 있는 구조는 남는다.

라벨도 기관 이름, 호스트, 등록 도메인이 섞였다. VWorld 수집 경로는 `www.vworld.kr`,
`dw.vworld.kr`, `api.vworld.kr` 창구를 함께 사용하지만 라벨은 `VWorld`였다. 주소정보 라벨은
`juso`였다. 반대로 `rt.molit.go.kr`은 현재 한 창구의 호스트가 곧 라벨이다. 따라서 “도메인”만
정하면 등록 도메인과 실제 호스트 가운데 무엇을 쓰는지 다시 갈릴 수 있다.

저장소 안에는 `juso.go.kr`과 `factoryon.go.kr`을 실제 호출하는 URL 증거가 없다.
`factoryon.go.kr`은 기존 라벨로만 존재하며 `juso.go.kr`은 이번 운영자 결정으로 교정한다. 두
값을 실측한 사실로 가장하지 않고 운영자가 승인한 제공기관 주소로 취급한다.

## Decision

1. **라벨 규칙은 다음 문장을 정본으로 한다.**

   > **라벨은 그 기관의 주소다. 여러 창구(`www`·`api`·`dw`)가 있으면 그 창구들이 공유하는 부분을 쓴다.
   > providerid 는 라벨에서 점만 뺀 것이고, 그 외에는 아무것도 빼지 않는다.**

   이 규칙은 등록 도메인만 허용한다는 뜻이 아니다. `rt.molit.go.kr`처럼 창구가 하나면 전체
   호스트가 라벨이고, VWorld처럼 여러 창구가 있으면 공유 접미사 `vworld.kr`이 라벨이다.

2. **승인된 도메인 라벨만 수동 관리하고 `providerid`는 파생한다.** 코드의 단일 SSOT는
   `APPROVED_PROVIDER_DOMAINS`이며 `provider_id(label)`은 승인 목록의 원소에 대해서만 점(`.`)을
   제거한 값을 반환한다. 별도 ID 목록과 라벨→ID 대응표를 두지 않는다. 파생 ID는 소문자 ASCII
   영숫자여야 하고 서로 충돌하지 않아야 한다. `is_canonical_source_slug`도 같은 목록에서 파생한
   ID로 판정한다.

3. **승인 목록은 다음 여덟 라벨이다.** `vworld.kr`, `data.go.kr`, `rt.molit.go.kr`,
   `hub.go.kr`, `juso.go.kr`, `mois.go.kr`, `factoryon.go.kr`, `industryland.or.kr`.
   `industryland.or.kr`은 산업단지 공고 수집의 제공기관 신원을 미리 등록하는 것뿐이며, 이 결정은
   수집기나 외부 호출을 추가하지 않는다.

4. **`rt.molit.go.kr`의 파생 ID는 `rtmolitgokr`이다.** 기존 `rtmolitkr`은 ADR 근거 없이
   라벨에서 `go`를 추가로 제거한 결함이다. FP-ADR-0014 D5의 출시 전 재수집 원칙을 그대로
   적용한다. 새 ID로 새 Bronze prefix와 `source_catalog` 행을 만들고 검증한 뒤에만 기존 것을
   제거하며, 기존 immutable key를 제자리 수정하거나 R2에서 rename하지 않는다.

5. **`mixed_public_source`는 제공기관이 아닌 명명된 legacy sentinel이다.** 현재 10개 POI
   항목에만 허용하고 `dataset_slug`와 정본 Bronze `source_slug` 생성을 금지한다. 카탈로그 parity는
   이 sentinel만 명시적으로 건너뛰며, 다른 미등록 라벨은 실패시킨다. 실제 제공기관을 확인하기
   전까지 도메인을 추측하지 않는다.

6. **FP-ADR-0014 D6의 서로 다른 식별자는 그대로 유지한다.** `endpoint_slug`, lane의 `lane_id`와
   CLI command token, 정규화 transformer slug는 제공기관 라벨이 아니므로 바꾸지 않는다.
   `landing/provider=vworldkr/` 같은 파생 ID 경로도 VWorld ID가 변하지 않으므로 유지한다.

7. **소문자 `vworld` 저장값은 이 라벨 계약 밖이다.** `vworld_cadastral_ingest`,
   `vworld_land_register_ingest`, `vworld_ned_attribute_ingest`의 `const PROVIDER: &str = "vworld"`는
   DB `catalog.source_catalog.provider`에 기록하는 내부 source 계열 값이고 카탈로그의 제공기관
   라벨과 의도적으로 구별된다. 이 결정에서 바꾸지 않는다.

8. **위협 모델은 잘못된 수동 대응의 재발을 예방하는 것이다.** 운영자가 새 제공기관 도메인을
   단일 목록에 등록한 뒤 ID 사본을 잘못 쓰거나 한쪽만 고치는 상태는 표현할 수 없게 한다. 도메인
   모양과 파생 ID 충돌은 Rust 테스트가 거부한다. 이는 승인 목록과 생성기를 함께 악의적으로
   바꾸는 유지보수자, `source_slug` 생성기를 우회하는 새 생산자, 도메인 소유권의 외부 변경을
   막지는 못한다. 생성기 우회는 기존 Bronze write-boundary의 정본 slug 검증과 코드 리뷰가,
   승인 목록 변경은 보호 브랜치와 운영자 검토가 맡는다.

## 예방을 선택한 이유

ADR-0027 결정 4는 예방 가능한 자리에서 탐지기를 만들지 말라고 요구한다. 이 문제의 유효 상태는
도메인 라벨 하나에서 ID를 결정론적으로 계산할 수 있으므로, 두 목록을 보존하고 parity scanner를
추가할 이유가 없다. 한 목록과 파생 함수로 바꾸면 과거 `rtmolitkr` 같은 독립 ID를 작성할 API가
사라진다. Rust 분석기와 단위 테스트는 남은 입력 경계인 도메인 문법과 파생 충돌만 검사한다.

## 기각한 대안

### 라벨과 ID 대응표를 하나의 tuple 목록으로 합친다

두 배열의 불일치는 막지만 라벨에서 점 이외의 문자를 제거한 ID도 여전히 표현할 수 있다. 규칙이
함수로 완전히 결정되는 값을 수동 데이터로 남겨 같은 결함의 입력 경로를 보존하므로 기각한다.

### 기존 두 목록을 대조하는 셸 가드를 추가한다

Rust 구문과 의미를 셸에서 다시 해석하고 세 번째 지식 사본을 만든다. Rust 사실은 compiler와
테스트가 소유해야 한다는 ADR-0027 결정 5에도 어긋나며, 결함을 예방하지 않고 사후 탐지만 하므로
기각한다.

### 모든 라벨을 `.go.kr` 등록 도메인으로 제한한다

`industryland.or.kr`이라는 승인된 비-`.go.kr` 도메인을 거부하고, `rt.molit.go.kr`의 실제 한 창구
호스트를 `molit.go.kr`로 임의 축약한다. 운영자가 정한 주소 규칙보다 좁고 실제 필요를 표현하지
못하므로 기각한다.

### `mixed_public_source`의 실제 기관을 추측해 등록한다

10개 항목은 현재 수집기·확인된 출처·생성 가능한 Bronze key가 없다. 근거 없는 도메인을 정본에
넣으면 부채를 사실처럼 고정하므로 명명된 sentinel로만 격리한다.

## Consequences

- 제공기관 라벨 `VWorld`와 `juso`는 각각 `vworld.kr`과 `juso.go.kr`로 바뀌지만 파생 ID는
  `vworldkr`과 `jusogokr`로 유지되어 기존 Bronze slug 값은 바뀌지 않는다.
- `rt.molit.go.kr`의 ID와 Bronze slug prefix는 `rtmolitgokr`로 바뀐다. FP-ADR-0014 D5의
  재수집·검증 후 제거 순서를 따른다.
- 승인 목록의 모든 값은 도메인 모양이어야 하고, 점 제거 결과가 서로 충돌하면 테스트가 실패한다.
  `.go.kr` 전용 규칙은 아니며 `industryland.or.kr`이 알려진 통과 경계를 고정한다.
- 카탈로그와 형제 정책 JSON은 같은 provider 라벨 집합을 읽는 parity 테스트에 포함된다. 새
  미등록 라벨은 조용히 skip되지 않는다.
- 제공기관 label 변경은 카탈로그와 런타임의 join key를 함께 바꾸므로 코드와 정본 JSON을 한
  커밋에서 원자적으로 전환한다.
