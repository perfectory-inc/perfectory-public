# ADR 0019 - Bronze 읽을 수 있는 객체 lake와 Postgres catalog SSOT

Status: Accepted
Date: 2026-06-28
Owner: foundation-platform
Supersedes/refines: [ADR 0015](./0015-bronze-object-key-content-addressed-layout.md) (object-key layout)
Related: [ADR 0016](./0016-bronze-commit-protocol.md), [ADR 0017](./0017-bronze-collection-protocol.md),
[ADR 0018](./0018-vworld-collection-channel-strategy.md),
[source-change-detection-policy](../catalog/source-change-detection-policy.md)

## Decision

R2 객체 path는 운영을 위한 사람이 읽을 수 있는 물리 layout이다. identity·integrity·date·
lineage의 단일 정본은 Postgres Bronze Catalog다.

## Context

Bronze raw objects need two different kinds of identity:

- **Coverage identity**: which source/request/file piece this object represents. This drives skip,
  coverage, and re-collection. Example: a real-transaction request for `lawd=11680` and
  `deal_ymd=202605`, or a hub bulk file with `provider_file_id=OPN...`.
- **Content identity**: whether the bytes are identical. This is the SHA-256 checksum.

Dates also have two different meanings:

- **Request scope**: a date/month passed to the source to select data. Example:
  `DEAL_YMD=202605`. This belongs in the coverage identity.
- **Descriptive metadata**: a period, 기준일, 갱신일, or fallback collection date stamped on a file
  or inferred from provider inventory. This belongs in typed catalog metadata.

We considered three layouts:

1. **Date-free path**: 기계적으로 정규화되지만 request-scope date를 표현하기 어렵고 덜 읽힌다.
2. **Content-addressed blob path**(`bronze/blob/sha256/...`): 순수하지만 opaque하고 streaming
   bulk file에서는 전체 digest를 알아야 최종 key를 정할 수 있어 비용이 크다.
3. **Readable path + Postgres catalog truth**: 운영에서는 읽기 쉽고 correctness는 catalog에 둔다.
   이 ADR은 이 option을 채택한다.

## Adopted Model

### R2 is physical layout, not truth

R2 keys remain readable:

```text
bronze/source=hubgokr__building_register_main/OPN209912310000000008.zip
bronze/source=vworldkr__boundary_census_emd/20991231DS99994-9007.zip
bronze/source=datagokr__real_transaction_industrial_trade/period=2026-05/lawd=11680/page-000001.json
bronze/source=datagokr__building_register_main/sigungu=11680/bjdong=10300/page-000001.json
bronze/source=vworldkr__land_register/pnu=9999900601100010000/page-000001.json
```

path는 사람·R2 browsing·smoke 검증·incident triage에 유용하다. code는 skip·dedupe·coverage·
freshness·lineage를 결정하기 위해 path를 parse하지 않는다.

`period`, `lawd`, `sigungu`, `bjdong`, `pnu` 같은 request-scope partition은 요청한 coverage
slice를 구분하면 API-page key에 남긴다. bulk file의 provider period·snapshot date·updated
date·fallback collection date는 descriptive metadata이므로 physical identity에 참여하지
않으며 provider file id를 leaf로 사용한다.

### Postgres Bronze Catalog is truth

Every recorded Bronze object carries:

```text
source_slug
source_identity_key
object_key
checksum_sha256
snapshot_period
snapshot_date
snapshot_granularity
snapshot_basis
provider_file_id
provider_file_name
provider_updated_at
request_params
ingestion_run_id
collected_at
```

`source_identity_key`는 “어느 source/request/file 조각인가?”에 답한다.
`checksum_sha256`는 “byte가 동일한가?”에 답한다.
둘은 경로 규칙이 아니라 Catalog를 통해서만 연결한다.

### Date policy

- `snapshot_period`는 `2026-05` 같은 사람이 읽는 bucket이다.
- `snapshot_date`는 canonical as-of date다. month 단위 data는 해당 month 첫날을 쓰고
  `snapshot_granularity=month`로 기록한다.
- `snapshot_granularity`는 `day` 또는 `month`다.
- `snapshot_basis`는 date가 존재하는 이유를 기록한다.
  - `provider_snapshot_date`
  - `provider_file_period`
  - `request_month`
  - `provider_updated_at`
  - `collected_at_fallback`

새 Bronze object에는 항상 `snapshot_date`를 채운다. provider에 기준일이 없으면 갱신일을
사용하고 둘 다 없으면 `collected_at_fallback`을 사용한다. basis에 fallback을 명시한다.

### Identity policy

source identity는 source별이며 한 곳에서 생성한다.

```text
hub/vworld bulk       = provider_file_id
real-transaction API  = lawd + deal_ymd + page + page_size
building-register API = sigungu + bjdong + page + page_size
V-World PNU API       = pnu + page + page_size
V-World cadastral API = pnu/emd/fingerprint + page + page_size
```

Provider request parameter는 raw lineage로 `request_params`에 보존한다. 이는 중복이 아니라
다른 audit 질문에 답하기 위한 것이다.

### Dedupe policy

`dedupe_key` is derived from the catalog identity and checksum:

```text
dedupe_key = source_slug + ":" + source_identity_key + ":sha256=" + checksum_sha256
```

어떤 레인도 dedupe key를 임의 형식으로 만들 수 없다.

## Consequences

- 운영자는 여전히 R2 path를 직접 읽을 수 있다.
- filename이나 provider ID 변경이 조용히 정본이 되지 않는다. catalog와 checksum이 결정한다.
- 같은 byte는 checksum으로 인식하고 변경 byte는 새 content state로 기록한다.
- Silver/Gold는 Iceberg형 lakehouse로 미루며 Bronze는 raw file + catalog로 남긴다.
- API/event contract version은 명시적으로 유지하고 semantic data version은 R2 object key가
  아니라 Catalog/Iceberg metadata에 둔다.

## Non-goals

- Content-addressed blob storage for Bronze (`bronze/blob/sha256/...`) is not adopted.
- Iceberg/Delta/Hudi are not introduced for Bronze.
- R2 path migration alone is not a correctness change; correctness comes from the catalog contract.
