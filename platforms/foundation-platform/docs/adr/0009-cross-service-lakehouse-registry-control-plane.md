# ADR 0009 - 교차 서비스 레이크하우스 레지스트리 제어면

| Field | Value |
|---|---|
| Date | 2026-06-05 |
| Status | Accepted |
| Scope | `foundation-platform` Lakehouse Registry bounded context, cross-service R2 bucket ownership, data asset discovery |
| Related ADRs | [ADR 0002](./0002-r2-primary-object-storage.md), [ADR 0005](./0005-object-lake-layout-and-indexing.md), [ADR 0006](./0006-lakehouse-table-format-and-serving-architecture.md), [ADR 0007](./0007-netflix-style-lakehouse-compute-architecture.md), [ADR 0008](./0008-pnu-anchor-pbf-marker-tile-contract.md) |

## 배경

`foundation-platform`, `gongzzang`, `dawneer`는 각자 data body를 독립적으로 소유해야 하지만,
회사는 lakehouse asset을 discover·authorize·promote·audit·rollback할 하나의 거버넌스
방식을 여전히 필요로 한다.

legacy 또는 단일 소유자 R2 버킷에는 다음과 같은 root-level medallion prefix가 있을 수 있다.

```text
bronze/
silver-handoff/
gold/
__r2_data_catalog/
```

그 layout은 single-owner bucket namespace에서만 허용된다. object key만으로는 다음을 답할
수 없으므로 깨끗한 cross-service namespace가 아니다.

- 어느 service가 data를 소유하는지
- 어느 service가 쓸 수 있는지
- 어느 version이 active인지
- 어떤 Bronze input이 Silver·Gold artifact를 만들었는지
- consumer가 읽을 수 있는지

`foundation-platform` 위에 새 top-level service를 추가하면 기존 control plane이 충분히
강화되기 전에 또 다른 control plane을 만든다. 소유권 명확성은 좋아지지 않고 운영 표면만
늘어난다.

## 결정

`Lakehouse Registry`는 데이터 자산을 담당하는 Foundation Platform bounded context다.
이는 organization-wide identity나 application control plane이 아니다.

```text
foundation-platform
├─ Catalog
│  └─ parcel, building, industrial complex, PNU anchor, public/reference layers
└─ Lakehouse Registry
   └─ storage namespace, data asset, version, lineage, quality, and access registry
```

Lakehouse Registry는 service 소유 R2 bucket의 lakehouse asset metadata를 관리한다.
`foundation-platform`이 모든 service business data의 owner가 되는 것은 아니다.

```text
Data owner:
  foundation-platform owns Catalog/common/public spatial data.
  gongzzang owns listings, listing media, Onbid sale data, court auction data, and market data.
  dawneer owns Dawneer workbench/product-specific data.

Registry owner:
  foundation-platform은 registry record, policy check, active pointer, lineage, discovery API를 소유한다.
```

## 물리 저장 모델

운영에서는 환경별 service 소유 bucket을 사용한다. provisioned bucket binding이 owner
경계를 제공하므로 각 bucket은 표준 medallion root layout을 사용한다. 아래 이름은 논리적
placeholder이며 실제 Cloudflare resource가 활성화되었다는 뜻이 아니다.

```text
<foundation-platform-bucket>/
├─ bronze/
├─ silver/
├─ gold/
└─ __r2_data_catalog/

<gongzzang-bucket>/
├─ bronze/
├─ silver/
├─ gold/
└─ __r2_data_catalog/

<dawneer-bucket>/
├─ bronze/
├─ silver/
├─ gold/
└─ __r2_data_catalog/
```

물리 버킷 하나가 꼭 필요해도 service ownership을 첫 번째 의미 있는 prefix로 둔다.

```text
<environment>/foundation-platform/bronze/
<environment>/gongzzang/bronze/
<environment>/dawneer/bronze/
```

새 cross-service data를 다음처럼 owner가 없는 root에 쓰지 않는다.

```text
bronze/source=...
gold/...
```

단, bucket이 명시적으로 single-owner bucket인 경우는 예외다.

## 레지스트리 책임

Lakehouse Registry는 다음을 기록한다.

- storage namespace: provider, account, bucket, environment, owner service, 허용 root prefix
- data asset: `foundation_platform.gold.parcel_marker_anchor`, `gongzzang.bronze.onbid_sale` 같은 안정적인 qualified name
- dataset version: immutable version id, schema version, table format, active/previous/retired 상태
- object artifact: object key, byte size, checksum, row count, content type, retention class
- ingestion run: source, request fingerprint, rate policy, result state, written object
- lineage edge: Bronze object set → Silver table snapshot → Gold artifact
- quality check: row count, null rate, schema 호환성, spatial validity, checksum 검증
- access policy: 각 asset을 어느 service가 read·write·promote·consume할 수 있는지
- consumer binding: 어떤 app/API/event contract가 어떤 active version을 소비하는지

데이터 본문은 R2/Iceberg에 남긴다. PostgreSQL에는 bulk payload가 아닌 control-plane metadata를
저장한다.

## API 경계

Consumer는 object key를 추론하지 않는다. `foundation-platform`에 active asset을 요청하거나
자신의 service 소유 artifact를 등록한다.

Initial API shapes:

```text
POST /internal/lakehouse/namespaces
POST /internal/lakehouse/assets
POST /internal/lakehouse/ingestion-runs
POST /internal/lakehouse/artifacts
POST /internal/lakehouse/lineage
POST /internal/lakehouse/promotions
GET  /internal/lakehouse/assets/{qualified_name}/active
GET  /internal/lakehouse/assets/{qualified_name}/versions/{version}
GET  /internal/lakehouse/assets/{qualified_name}/lineage
```

public/product API는 축소한 read-only contract를 노출할 수 있지만 write/promotion endpoint는
internal이며 service-authenticated 상태로 남는다.

## R2 Data Catalog와 Iceberg

Cloudflare R2 Data Catalog는 Iceberg table metadata provider로 남는다. business ownership
SSOT가 아니다.

```text
R2 bucket / Iceberg table metadata = table storage/catalog provider
foundation-platform Lakehouse Registry = ownership, discovery, active version, lineage, quality, policy SSOT
```

각 service 소유 bucket은 Iceberg table이 필요할 때 R2 Data Catalog를 켤 수 있다. raw
unstructured artifact는 일반 R2 object로 남을 수 있지만 governed pipeline에 참여하면
여전히 registry에 등록해야 한다.

## 레거시 버킷 해석

root-level `bronze/`, `gold/`, `silver-handoff/` prefix가 있는 legacy bucket은 새 write
전에 정확히 하나의 owner namespace로 분류해야 한다. 기존 prefix만으로 ownership, active
status, migration 완료를 증명할 수 없다. Foundation Platform에 할당된 root에 product 소유
asset을 추가하지 않는다.

## 프로비저닝 계약

이 ADR은 특정 account·bucket·prefix·storage class·region이 현재 provisioned되었다고
주장하지 않는다. 다음을 모두 검증한 뒤에만 namespace를 active로 만든다.

1. infrastructure-as-code or an approved provisioning record binds environment, account, bucket,
   owner service, and allowed prefix;
2. the Lakehouse Registry contains the matching namespace record;
3. credentials are scoped to the owner and permitted roots;
4. a bounded write/read/reconciliation check succeeds;
5. the evidence and resource identifiers are stored in the private operations evidence system under
   [root ADR 0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md).

retired 또는 smoke-only resource는 inventory로 현재 registry record, lineage edge, consumer
binding, pipeline reference가 없음을 증명한 뒤에만 제거할 수 있다.

## 금지 사항

- product service가 canonical data의 R2 key를 추측하는 것
- product service가 `foundation-platform` 소유 bucket/root에 쓰는 것
- `foundation-platform`이 Gongzzang 소유 business data를 Catalog 소유 사실로 취급하는 것
- 첫 partition이 `bronze/`, `silver/`, `gold/`뿐인 새 multi-service bucket
- raw public API payload body를 PostgreSQL JSONB에 primary Bronze store로 저장하는 것
- registry state·checksum·quality evidence·lineage 없이 object 존재만으로 promotion하는 것

## 전환 경로

1. 발견한 legacy root-level layout은 owner와 consumer를 분류할 때까지 동결한다.
2. environment마다 `foundation-platform`, `gongzzang`, `dawneer`의 service 소유 namespace
   binding을 provisioning한다.
3. `foundation-platform`에 `Lakehouse Registry` schema와 internal API를 구현한다.
4. 검증된 Foundation Platform Bronze/Silver/Gold object를 inventory하고 existing asset으로
   등록한다.
5. Onbid·court auction을 포함한 새 Gongzzang 소유 pipeline을 Gongzzang 소유 lakehouse
   namespace로 향하게 한다.
6. write 검증 후 Gongzzang artifact를 foundation-platform Lakehouse Registry에 등록한다.
7. owner namespace 없는 root-level shared medallion prefix를 새 object key와 env variable이
   사용하지 못하도록 boundary check를 추가한다.
8. inventory·lineage·consumer-binding 검증 후에만 legacy root-level object를 migration하거나
   retire한다.

## 영향

긍정적 효과:

- `foundation-platform`이 platform control plane으로 남고 성급한 네 번째 service를 만들지 않는다.
- discovery와 governance는 중앙화하면서 service data ownership은 명확히 남는다.
- bucket IAM·lifecycle·retention·blast radius를 service별로 설정할 수 있다.
- business logic이 registry contract에 의존하므로 R2 Data Catalog/Iceberg를 교체할 수 있다.
- bounded context가 격리되어 나중에 별도 `data-platform` service로 추출할 수 있다.

비용:

- 더 많은 bucket과 registry record를 provisioning하고 audit해야 한다.
- service는 write 후 artifact를 등록해야 하며 직접 object-key convention만으로는 부족하다.
- 모든 legacy root-level bucket은 cleanup 전에 분류해야 한다.

## 종료 기준

- `foundation-platform`에 `Lakehouse Registry` bounded context 설계와 구현 계획이 있다.
- 새 governed object는 정확히 하나의 service 소유 storage namespace에 속한다.
- Registry API가 consumer에게 raw R2 key를 공개하지 않고 active asset을 해석한다.
- boundary check가 새 root-level multi-service `bronze/`, `silver/`, `gold/` write를 거부한다.
- 발견된 Foundation Platform R2 object는 삭제나 migration 전에 inventory된다.
