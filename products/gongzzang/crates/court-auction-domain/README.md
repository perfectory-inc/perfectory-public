# court-auction-domain

`CourtAuction` Aggregate와 reader port를 정의하는 storage-agnostic read-only domain
contract crate에요. 수집, 변환, 저장, 스케줄링, publish 및 reader 구현은 이 crate의 책임이
아니며 현재 완료된 pipeline을 전제하지 않아요. 이 공개 계약과 테스트는 특정 시점의 수집
증거에 의존하지 않습니다.

## 책임

- 한국 법원 경매 데이터의 storage-neutral 읽기 계약을 정의해요. Aggregate는 *read-only* —
  mutation 메서드 0개.
- 한 필지(`Pnu`)에 다수 사건이 가능해요.
- `CourtAuctionReader` trait 포트 — 구현체는 crate 외부 adapter의 책임이에요.
- `ReaderError` enum — `NotFound` / `Fetch` / `Parse`.
- `CourtAuctionKind` (BC-internal, 강제/임의/기타).
- `CourtAuctionStatus` (BC-internal, 예정/진행중/낙찰/취하/유찰).
  `is_active()` 헬퍼는 `Upcoming` + `InProgress` 필터링용이에요.

## 의존

- `shared-kernel` (`Pnu`, `MoneyKrw`, `PointSrid`, `SpatialScope`).
- 다른 BC 의존 *없어요*.

## 예시

```rust,ignore
use court_auction_domain::reader::CourtAuctionReader;
let active = reader.fetch_active().await?;
```
