# ADR 0014 - Bronze 원천 slug 정본 이름과 단일 생성기

- Status: Accepted (naming + generator)
- Date: 2026-06-23
- Owner: foundation-platform
- `docs/architecture/bronze-key-naming-and-catalog-principle.md` §4의 "물리 key를 안정적으로
  유지" 권고를 대체한다.
  `docs/architecture/bronze-key-naming-and-catalog-principle.md` §4 **for the source-slug segment only**
  (owner가 일회성 표준화 migration을 선택했고 비용을 수용했다.)

## 배경

Bronze object key는 `bronze/source={source_slug}/run_id=.../partition=.../part-NNNNNN.ext`
(`crates/catalog/catalog-domain/src/bronze.rs:472`)다. `source_slug` segment는 현재 **일관되지
않고 SSOT 없는 5개 단절된 위치에서 생성된다**(2026-06-23 6-agent audit).

1. **Catalog** `docs/catalog/public-source-endpoint-catalog.v1.json` - 67 hand-authored
   `bronze.source_slug` literals (hyphen-cased, e.g. `hub-building-building-register`,
   `data-go-kr-building-register-getbrtitleinfo`). Only the `building_hub_bulk` lane reads them at runtime.
2. **In-code `OPERATION_SPECS` tables** - `real_transaction_ingest.rs:406-491` (13) and
   `vworld_ned_attribute_ingest.rs:390-454` (7) hardcode slug literals keyed by operation.
3. **Per-binary `DEFAULT_SOURCE_SLUG` constants** - `building_register_ingest.rs:40`
   (`molit-building-register`), `vworld_cadastral_ingest.rs:35` (`vworld-cadastral`),
   `vworld_land_register_ingest.rs:36` (`vworld-land-register`). **These DIVERGE from the catalog**
   for the same data (a separate `molit-*` lineage).
4. **National-run pilot defaults** (`*-national-pilot`) + shard-writer **derived** slugs
   (`vworld-cadastral-national-{sigungu}-{bjdong}`, `national_data_collection_shard_manifest_writer.rs:643`).
5. **Fallback formatter** `building_hub_bulk_collection_plan.rs:318` =
   `hub-go-kr-public-bulk-task-{group}-{code}` (the opaque codes the owner flagged).

같은 dataset의 두 slug가 이미 조용히 달라질 수 있다. 이 이름은 engine 간 이식성도 없다.
slug가 미래 Silver/Gold table name이 되며 dash는 BigQuery dataset에서 깨지고 Databricks에서는
backtick이 필요하다(`docs/architecture/bronze-key-naming-and-catalog-principle.md` §3 참조).

## 결정

### D1 - 정본 slug 형식
`source_slug = {providerid}__{dataset_slug}`로 한다. 모두 lowercase이고 `dataset_slug`는
`snake_case`이며 provider와 dataset 사이에 **double underscore**를 둔다. dbt `source__entity`,
BigQuery(`-` 금지, `_` 허용), Databricks(lowercase) 관례에 근거하며 principle doc을 따른다.

### D2 - 제공기관 ID 표(유일한 수동 관리 표)
| catalog `provider` | providerid |
|---|---|
| `VWorld` | `vworldkr` |
| `data.go.kr` | `datagokr` |
| `hub.go.kr` | `hubgokr` |
| `juso` | `jusogokr` |
| `mois.go.kr` | `moisgokr` |
| `factoryon.go.kr` | `factoryongokr` |

### D3 - `operation`(API 호출 ID)과 `dataset_slug`(의미 식별자) 분리; 단일 생성기
slug은 `operation`에서 파생하면 안 된다. data.go.kr에서 operation은 원시 API method
(`getBrTitleInfo`)지만 승인 slug는 semantic(`datagokr__building_register_main`)이다.
`snake_case(operation)` 변환은 `datagokr__get_br_title_info`를 만들어 잘못된다. 따라서 각
source는 **서로 다른 identifier 두 개**를 가진다.
- **`operation`** - provider-native API call id(예: `getBrTitleInfo`, `getRTMSDataSvcAptTradeDev`,
  `parcel`). 실제 provider 호출에 사용하며 이 ADR에서 **변경하지 않는다**.
- **`dataset_slug`**(또는 `canonical_source_dataset`) - `snake_case`인 canonical semantic
  dataset identity(예: `building_register_main`). source별로 정한다. hub/vworld/mois/factoryon은
  보통 operation과 같고, data.go.kr은 의미 있는 이름을 사용한다(operation→dataset_slug map은
  data.go.kr rename table의 일부다).

단일 generator는 `source_slug(provider, dataset_slug) = {providerid(provider)}__{dataset_slug}`다.
§Context의 모든 producer(catalog authoring, `OPERATION_SPECS`, `DEFAULT_SOURCE_SLUG`, pilot/derived
slug, fallback formatter)는 literal을 직접 쓰지 않고 이를 호출한다. `public-source-endpoint-catalog.v1.json`
각 endpoint에 새 **`dataset_slug`** field를 추가하고 기존 `bronze.source_slug`는 **derived** 값으로
만든다. CI는 범위 내 모든 entry에서 `bronze.source_slug == source_slug(provider, dataset_slug)`를
확인한다. 이로써 5-way divergence를 제거하고 승인된 data.go.kr semantic name을 생성할 수 있다.

### D4 - slug 문자 검증 완화(차단 전제 조건)
`validate_source_slug`(`bronze.rs:484-503`)는 현재 `[a-z0-9-]`만 허용하고 **`_`를 거부**한다.
따라서 새 `__` slug를 쓰려면 `_`를 허용하도록 넓혀야 한다. DB column은
`catalog.source_catalog.slug` CHECK already allows `_` (`^[a-z0-9][a-z0-9_-]*$`,
`migrations/20260513000001:27`)에서 이미 `_`를 허용하므로 Rust validator만 바꾼다. 이는
key-format **contract 변경**이며 배포 후 format은 동결한다(추가 변경은 새 ADR + migration).

### D5 - 전환은 출시 전 재수집이며 제자리 수정이 아님
**0 user**이고 commit된 Bronze data가 없으므로(manifest/ledger/evidence는 `target/audit/` 아래
runtime이며 `catalog.bronze_object`와 `catalog.outbox_event` DB row만 남음) 다음 migration을
선택한다: code 반영 → **새 slug와 새 R2 prefix·새 `source_catalog` row로 재수집** → 검증 →
**그 뒤에만 기존 prefix/row 삭제**. immutable key를 R2 copy하고 DB를 다시 쓰는 위험을 피하며
“migration 검증 전 삭제 금지”를 자연스럽게 지킨다. in-place copy migration(Strategy B)은
대체할 수 있는 Bronze data가 있을 때의 선택지일 뿐 현재 사용하지 않는다.

### D6 - 명시적 범위 밖(이름을 바꾸지 않음)
- `endpoint_slug`(camelCase routing identity, 예: `data-go-kr-building-register-getBrTitleInfo`)는
  다른 identifier다. 이를 검증하는 test는 기존 값을 유지하며 일괄 find/replace하지 않는다.
- `national-data-normalization-contract.v1.json` transformer slug(별도 Silver namespace)
- `public-data-bronze-lane-registry.v1.json`의 `lane_id`와 CLI command token
- `mixed_public_source` / POI(10)와 등록되지 않은 `hub-go-kr-public-bulk-task-*`는 연기한다.
  처음 사용할 때 등록하고 이름을 정한다.

## 영향

- **Claim-check break for any data already written under old keys.** R2 has no rename; the object_key
  is referenced by `bronze_object.object_key`, the `collection.raw_written.bronze_object_key` event,
  ledger/manifests, and Silver/Gold lineage. Re-collect (D5) sidesteps this pre-launch; in-place
  migration would require copying objects + updating rows in lockstep.
- **dedupe_key drift** - slug is embedded in `bronze_object.dedupe_key` (`{slug}:...`,
  `public_data_bronze_plan.rs:186`); re-collect starts a clean dedupe namespace.
- **No bare `datagokr__building_register`** - the divergent per-binary default `molit-building-register`
  (data.go.kr building-register API) must resolve to the SPECIFIC sub-type its run collects (e.g.
  `datagokr__building_register_main` for `getBrTitleInfo`) via the operation->dataset_slug map, NOT a
  bare `datagokr__building_register`, which would collide/ambiguate with the 10 approved building-register
  sub-type slugs. The bare default constant is removed in favor of the generator.
- **catalog sha256 re-pin** - the catalog file is sha256-pinned at plan-compile + execute
  (`national_data_collection_plan_compile.rs:94`, `endpoint_catalog.rs:36`); editing it forces
  regenerating manifests/plans.
- **Runtime guards + many test pins must move** - e.g. `real_transaction.rs:73`
  `starts_with("data-go-kr-real-transaction-")`, plus pinned slug literals across ~15 test files
  (enumerated in the plan). `endpoint_slug` pins stay.
- **gongzzang (downstream consumer)** receives new `bronze_object_key`/`endpoint_slug` values in
  `collection.raw_written`; the event schema is unchanged (value-only change).

## 참고 문서
- `docs/catalog/bronze-source-slug-rename.v1.md` - old->new + operation->dataset_slug mapping
  (owner-approved SSOT; the generator's human-readable projection and executable migration map).
- `docs/architecture/bronze-key-naming-and-catalog-principle.md` - naming sources + the
  stable-keys principle this ADR overrides for the slug segment.
- Dated impact-audit evidence is retained outside the public code tree under
  [root ADR-0007](../../../../docs/adr/0007-public-code-private-operations-boundary.md).
