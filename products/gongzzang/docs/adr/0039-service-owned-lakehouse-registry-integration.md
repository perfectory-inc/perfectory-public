# ADR 0039 - 서비스 소유 레이크하우스와 Foundation Platform 레지스트리

| Field | Value |
|---|---|
| Date | 2026-06-05 |
| Status | Accepted |
| Preceded by | [ADR 0030](./0030-three-service-architecture.md), [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md), [ADR 0036](./0036-static-vector-tile-runtime-contract.md), [ADR 0038](./0038-listing-marker-serving-index-filter-mask.md) |
| Foundation Platform counterpart | `../../../../platforms/foundation-platform/docs/adr/0009-cross-service-lakehouse-registry-control-plane.md` |
| Gongzzang policy SSOT | [Lakehouse Registry integration policy](../architecture/platform-integration/lakehouse-registry-policy.v1.json) |

## 배경

Gongzzang은 listing·listing photo·listing marker projection·Onbid sale·court auction·market
data 같은 product data를 소유한다. Foundation Platform은 parcel·building·industrial complex·
PNU anchor·public/reference spatial layer 같은 Catalog/common data를 소유한다.

두 종류의 data 모두 Bronze/Silver/Gold pipeline이 필요하지만 `bronze/`와 `gold/`만으로 owner를
알 수 없는 root-level R2 namespace에 섞어서는 안 된다.

대기업 사례에서 확인한 목표 패턴은 domain이 물리 저장소를 소유하고 중앙 registry/catalog이
governance·lineage·access policy·active-version을 관리하는 방식이다. R2 Data Catalog나
Iceberg가 queryable table을 관리할 수 있지만 Foundation Platform Lakehouse Registry는
cross-service asset identity와 consumer-contract control plane으로 남는다.

## 결정

Gongzzang은 Gongzzang 소유 데이터셋에 Gongzzang 소유 레이크하우스 저장 namespace를 사용한다.

```text
gongzzang-lakehouse-prod/
|-- bronze/
|-- silver/
|-- gold/
|-- media/
`-- __r2_data_catalog/
```

이 diagram은 논리적 운영 topology와 namespace 계약을 정의하며 현재 실제 provisioned resource
inventory를 주장하지 않는다. account binding, provisioning state, location hint, storage
class는 private operational evidence이며 이 ADR에서 단정하지 않는다.

Gongzzang은 Gongzzang 소유 Onbid·court auction·listing·market·media pipeline object를
Foundation Platform 소유 R2 namespace에 쓰지 않는다.

Foundation Platform은 top-level control plane으로 남는다. Lakehouse Registry가 Gongzzang 소유
asset location, active version, lineage, quality evidence, consumer binding을 기록한다.
Registry metadata가 Gongzzang에서 Foundation Platform으로 data ownership을 이전하지 않는다.

```text
Gongzzang collector / worker
→ Gongzzang 소유 object를 Gongzzang 소유 R2 namespace에 쓴다.
→ checksum·size·row count·lineage evidence를 검증한다.
→ Foundation Platform Lakehouse Registry에 run/artifact/version을 등록한다.

Gongzzang app/API
→ canonical R2 key를 추측하지 않는다.
→ Foundation Platform Registry/API contract로 active governed asset을 해석한다.
```

## 소유권 표

| Asset | Data owner | Storage namespace | Registry owner |
|---|---|---|---|
| Listing OLTP data | Gongzzang | Gongzzang DB | Gongzzang |
| Listing photos | Gongzzang | Gongzzang lakehouse `media/listing-photo/` | Foundation Platform registry for governed assets only |
| Listing marker Gold tiles/indexes | Gongzzang | Gongzzang lakehouse `gold/` | Foundation Platform Lakehouse Registry |
| Onbid Bronze/Silver/Gold | Gongzzang | Gongzzang lakehouse | Foundation Platform Lakehouse Registry |
| Court auction Bronze/Silver/Gold | Gongzzang | Gongzzang lakehouse | Foundation Platform Lakehouse Registry |
| Parcel/building/PNU anchor | Foundation Platform | Foundation Platform lakehouse | Foundation Platform Lakehouse Registry |

## 금지 사항

- Gongzzang 소유 lakehouse data를 Foundation Platform 소유 root `bronze/`·`gold/` namespace에 쓴다.
- V-World/data.go.kr Catalog ingestion crate를 Gongzzang에 재도입한다.
- Foundation Platform R2 object key를 public API로 취급한다.
- Bronze raw API response body를 Gongzzang Postgres JSONB에 canonical archive로 저장한다.
- legacy `R2_*` 설정에 조용히 쓰는 fallback path를 추가한다.

## 설정 경계

Gongzzang lakehouse pipelines use `GONGZZANG_LAKEHOUSE_R2_*` configuration.

Listing photo upload storage는 runtime edge에서 `LISTING_PHOTO_R2_*`로 남긴다. upload signing,
download signing, object verification, user-media authorization이 batch pipeline과 runtime
관심사가 다르기 때문이다. bucket은 같은 Gongzzang lakehouse bucket일 수 있지만 object
namespace는 `media/listing-photo/` 아래에 있어야 한다.

`media/` namespace는 listing 사진 같은 Gongzzang 소유 binary media와 향후 listing video,
floor plan, broker-upload document를 위한 것이다. Bronze/Silver/Gold를 대체하지 않는다.
AI extraction output, embedding, normalized caption, quality report, searchable metadata는 이
media object에서 파생한 governed dataset 또는 index로 등록한다.

Foundation Platform Catalog/raw-data storage는 Gongzzang runtime 설정 밖에 남는다. Gongzzang은
Foundation Platform R2 object key를 직접 읽지 않고 Foundation Platform contract로 governed
Catalog artifact를 해석한다.

## 현재 버킷 해석

기존 R2 bucket에 root-level `bronze/`, `gold/`, `silver-handoff/`가 보여도 자동으로
cross-service bucket이 되는 것은 아니다. migration이 명시적으로 달리 정하지 않는 한 Gongzzang은
그 root-level namespace를 Foundation Platform 소유 current/legacy lakehouse material로 취급하고
새 Gongzzang 소유 Bronze·Gold dataset을 추가하지 않는다.

## 영향

긍정적 효과:

- Gongzzang이 product data ownership을 유지한다.
- Foundation Platform은 모든 business fact의 owner가 되지 않고 central control plane으로 남는다.
- R2 object layout·active version·lineage·quality evidence를 하나의 governed registry에서 찾을 수 있다.
- 향후 AI/vector index가 bucket folder를 훑지 않고 Foundation Platform과 Gongzzang의 등록 asset을 읽는다.

비용:

- Gongzzang pipeline이 write 후 artifact를 등록해야 한다.
- Gongzzang 소유 lakehouse namespace에 새 env/config와 guard가 필요하다.
- 기존 root-level R2 object는 cleanup이나 migration 전에 inventory해야 한다.

## 전환 메모

1. Foundation Platform inventory가 owner·lineage·active/retention status를 분류할 때까지 기존
   R2 object를 삭제하지 않는다.
2. 새 Onbid/court auction Bronze write 전에 Gongzzang 소유 lakehouse namespace를 만들거나 지정한다.
3. Gongzzang pipeline 완료 단계에 Foundation Platform Registry 등록을 추가한다.
4. 명시적 owner namespace 없는 새 shared root medallion prefix를 거부하는 CI check를 추가한다.
5. Foundation Platform Catalog 소비는 발행된 API/event/artifact contract로 유지한다.

## 강제 지점

lakehouse registry 연동 정책의 SSOT는
`docs/architecture/platform-integration/lakehouse-registry-policy.v1.json`, wired into the
platform integration index. The contract requires consistency with the Foundation Platform boundary
contract, the required R2 env bucket names, the listing photo media namespace, and the absence of
unmanaged root `gongzzang/bronze`, `gongzzang/silver`, or `gongzzang/gold` writes in active
implementation paths.
