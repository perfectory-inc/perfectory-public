# ADR 0027 - 정적 지도 계층에 대체 데이터를 넣지 않음

| Field | Value |
|---|---|
| Date | 2026-05-08 |
| Status | Superseded operationally by [ADR 0034](./0034-catalog-ownership-handover-to-foundation-platform.md), [ADR 0036](./0036-static-vector-tile-runtime-contract.md), and [ADR 0048](./0048-horizontal-platform-redefinition.md) |

## 결정

static map layer는 소유자가 source별 dataset, schema, validation policy, manifest entry를
함께 정의한다. 다른 layer의 data를 이름만 바꿔 불완전한 layer가 사용 가능한 것처럼
보이게 해서는 안 된다.

오래된 Gongzzang ETL activation switch는 더 이상 현재가 아니다. Foundation
Platform이 parcel, administrative, industrial-complex, building과 기타 public/reference
spatial layer를 소유한다. 발행된 vector-tile manifest만 layer 사용 가능 여부를 나타내는
runtime 정본이다.

## 런타임 계약

- Gongzzang은 검증된 manifest contract에 있는 layer만 등록한다.
- 각 manifest layer는 자신의 `source_layer`, zoom range, artifact identity를 유지한다.
- 없는 layer는 사용할 수 없다. parcel data에서 합성하거나 product deployment switch로
  활성화하지 않는다.

## 영향

- administrative·industrial-complex·building layer를 추가하려면 Foundation 발행을
  변경한 뒤 Gongzzang이 contract를 소비해야 한다.
- workflow 파일명과 과거 ETL crate 이름은 역사적 구현 detail이며 이 결정의 일부가 아니다.

## 강제 지점

- [ADR 0036](./0036-static-vector-tile-runtime-contract.md)가 consumer manifest
  contract를 소유한다.
- `docs/architecture/foundation-platform-boundary.v1.json`이 owner 경계와 product 쪽에서
  금지된 구현을 기록한다.
