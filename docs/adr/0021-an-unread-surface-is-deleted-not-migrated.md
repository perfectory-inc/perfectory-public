---
status: current
owner: repository-maintainers
doc_type: adr
last_reviewed: 2026-08-09
---

# ADR 0021: 아무도 읽지 않는 표면은 옮기지 않고 지운다

- Status: Accepted
- Date: 2026-08-09
- 관련: [ADR-0019 소속은 한쪽의 컬럼이 아니라 기간을 가진 사실이다](./0019-membership-is-a-dated-fact-not-a-column.md) (§이행 순서 2를 이 ADR이 좁힌다), [ADR-0020 도형은 사실의 근거가 아니다](./0020-geometry-is-not-evidence-for-a-fact.md)

## Context

[ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md) §이행 순서 2는 "산단 스코프 읽기를
소속 표로 옮긴다"고 적었다. 그 읽기가 몇 개인지, 누가 쓰는지는 세어 보지 않았다.

세어 보니 넷이고, 넷 다 `catalog.parcel.complex_id`를 읽는다.

| 라우트 | 저장소 안 호출자 | 트래픽·인증 정책 등록부 | 공개 표면 테스트 |
|---|---|---|---|
| `/catalog/v1/complexes/{id}/anchor-summary` | 없음 | **dawneer 소비 표면으로 선언** | **존재를 강제** |
| `/catalog/v1/complexes/{id}/parcels` | 없음 | 없음 | 없음 |
| `/catalog/v1/complexes/{id}/buildings` | 없음 | 없음 | 없음 |
| `/catalog/v1/complexes/{id}/manufacturers` | 없음 | 없음 | 없음 |

`traffic-auth-policy-registry.v1.json`의 `foundation_platform.consumer.dawneer_catalog_reads`가
`allowed_surfaces`에 `anchor-summary`를 적고 있고, `catalog_published_surface.rs`가 OpenAPI에서
그 경로가 사라지면 실패한다. Dawneer는 이 모노레포 밖 제품이므로 저장소 안에 호출자가 없는 것이
곧 소비자가 없다는 뜻이 아니다 — **등록부가 유일한 증거이고, 그 등록부는 셋에 대해 침묵한다.**

gongzzang이 부르는 Catalog 경로는 PNU 기준 둘뿐이다 —
`catalog/v1/parcels/by-pnu/{pnu}`와 `.../buildings`. 산단 기준 경로는 하나도 부르지 않는다.

제품 판단도 같은 방향이다. 산업단지 필지 목록은 지금 다루지 않기로 했다. Gongzzang은 산업단지
제품이 아니며, 그 목록을 전제한 것은 `industrial-complex-ssot-model.md`의 **제안** 문서다.

## Decision

### 1. 소비자가 선언되지 않은 산단 스코프 읽기 셋을 지운다

`/complexes/{id}/parcels`, `/complexes/{id}/buildings`, `/complexes/{id}/manufacturers`를 라우트,
포트, 저장소 구현, 테스트, OpenAPI에서 제거한다. `ParcelResponse`처럼 다른 라우트가 쓰는 타입은
남긴다.

옮기지 않고 지우는 이유는 **옮기는 것이 공짜가 아니기 때문이다.** 각 읽기를 소속 표로 이전하려면
"현재"의 기준일 계약을 정하고, 구·신 경로가 같은 답을 내는 대조 테스트를 붙이고, 소속이 없는
필지·기간이 끝난 소속을 어떻게 다룰지 정해야 한다. 아무도 부르지 않는 세 라우트를 위해 그 값을
치르고 나면, 결과물은 **여전히 아무도 부르지 않는 세 라우트**다.

### 2. `anchor-summary`는 지우지 않고 옮긴다

등록부가 소비자를 지명하고 공개 표면 테스트가 존재를 강제하므로, 이것은 [ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md)
§이행 순서 2가 말한 이전 대상으로 남는다. 2단계의 읽기 이전은 **네 개가 아니라 하나**가 된다.

### 3. 지운 것은 사라진 채로 두지 않는다

`industrial-complex-ssot-model.md`는 이 셋을 제안 상태로 적고 있다. 그 표에서 지우고, 왜 없는지를
이 ADR로 연결한다. 문서가 없는 라우트를 계속 광고하면 다음 사람이 "구현이 빠졌다"고 읽는다.

## 기각한 대안

### 네 개를 모두 소속 표로 옮긴다 (ADR-0019 §이행 순서 2 그대로)

가장 보수적이고, 나중에 산단 목록 제품이 생기면 이미 준비돼 있다.

기각한 이유는 준비의 대상이 **불확실하기 때문이다.** 지금 옮기면 "현재"의 정의와 소속 종류 포함
정책을 지금 정해야 하는데, 그 답은 제품이 정해질 때 달라진다. 지금 정한 답은 검증할 소비자가 없어
틀려도 아무도 모르고, 제품이 생기면 다시 정해야 한다. 그때 새로 만드는 비용은 지금 옮기는 비용보다
크지 않다 — 소속 표는 남아 있고, 읽는 질의 하나를 쓰는 일이다.

### 네 개를 모두 지운다

`anchor-summary`도 저장소 안에서는 아무도 부르지 않으므로 같이 지우자는 안.

기각한 이유는 등록부다. 저장소 안 호출자가 없다는 것은 **이 저장소가 아는 전부**이고, 크로스 저장소
소비자는 등록부와 공개 표면 테스트로만 표현된다. 둘 다 `anchor-summary`를 지명한다. 그 선언을 실물
호출자가 없다는 이유로 무시하는 것은, 등록부를 근거로 쓰지 않겠다는 뜻이다.

### 라우트를 남기고 410 Gone을 반환하게 한다

소비자가 있을지 모르니 명시적으로 사라졌다고 알려 주자는 안. 공개 API였다면 옳다.

기각한 이유는 이 표면이 공개된 적이 없기 때문이다. 등록부가 소비자를 열거하고 그 목록에 셋이
없으므로, 410을 받을 대상이 정의상 존재하지 않는다. 아무도 받지 않을 응답을 위해 라우트·핸들러·
계약 항목을 유지하는 것은 지우는 것보다 큰 표면이다.

## Consequences

- **2단계가 4분의 1로 줄어든다.** 읽기 이전 대상이 `anchor-summary` 하나다.
- **3단계에서 지울 것도 줄어든다.** `list_parcels_by_complex`·`list_buildings_by_complex`·
  `list_manufacturers_by_complex`와 그 SQL이 `catalog.parcel.complex_id`를 읽던 네 자리 중 셋이
  이 증분에서 사라진다.
- **OpenAPI 사본 둘과 pin 해시가 함께 바뀐다.** `foundation-platform-catalog-api-contract.v1.pin.json`이
  전체 SHA-256을 고정하므로 갱신하지 않으면 gongzzang 쪽 검사가 실패한다.
- **`ParcelResponse`는 남는다.** `/parcels/{id}`, `/parcels/by-pnu/{pnu}`, kind PATCH가 계속 쓴다.
  그 타입의 `complex_id` 필드 제거는 [ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md)
  §Decision 3의 3단계 그대로다.
- **되돌리는 비용은 낮다.** 지운 것은 질의 세 개와 그 배선이며, 소속 표가 남아 있으므로 산단 목록
  제품이 생기면 그때의 계약으로 새로 쓴다.

## 남은 부채

1. **`anchor-summary`의 읽기 이전이 남아 있다.** §Decision 2. 그때 "현재"의 기준일 계약을 정해야
   하며, 그 결정은 이 ADR이 하지 않는다.
2. **`industrial-complex-ssot-model.md`는 여전히 제안 문서다.** 이 증분은 지운 셋만 그 표에서
   덜어낸다. 그 문서 전체가 [ADR-0019](./0019-membership-is-a-dated-fact-not-a-column.md)·
   [ADR-0020](./0020-geometry-is-not-evidence-for-a-fact.md) 이후의 모델과 맞는지는 별도 검토가
   필요하다.
3. **등록부와 실물 라우트를 대조하는 검사가 없다.** 이 ADR은 등록부를 근거로 셋을 지웠지만,
   등록부가 지명한 표면이 실제로 존재하는지는 `anchor-summary` 등 다섯 경로에 대해서만
   `catalog_published_surface.rs`가 확인한다. 등록부의 나머지 항목은 아무도 대조하지 않는다.
