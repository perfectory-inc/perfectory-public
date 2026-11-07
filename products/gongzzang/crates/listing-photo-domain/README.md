---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# listing-photo-domain

`ListingPhoto` Aggregate (RDS 동적 + R2 binary) crate에요.

## 책임

- `listing_photo` 테이블(`migrations/20260719000102_core_tables.sql`)에 대응하는 `ListingPhoto`
  Aggregate 정의해요.
- 메타데이터(크기, content-type, 캡션, 순서)는 RDS, 실제 바이너리는
  R2 객체로 분리 — `r2_key` 로 참조해요. 새 사진은 내용주소 키(`media/listing-photo/sha256/<hex>.<ext>`)와
  `content_sha256`, 표시 사다리는 그 해시 옆(root ADR-0427). 형식은 `shared_kernel::media::MediaFormat`.
- `try_new` 생성자에서 `r2_key` 비어있음, `display_order` 음수,
  `caption` 200자 초과를 차단해요.
- `soft_delete`(idempotent), `reorder`, `confirm_upload`(저장소 확인 후 크기 기록), `is_active`,
  `is_upload_confirmed` 도메인 메서드 제공.
- 도메인 이벤트 `ListingPhotoUploadConfirmed` (`listing_photo.upload_confirmed`) — outbox publisher의
  lakehouse sink가 소비해요.
- `ListingPhotoRepository` trait — `Postgres` 구현은 `gongzzang-persistence`의 `PgListingPhotoRepository`.

## 의존

- `shared-kernel` (`Id`, `ListingMarker`, `ListingPhotoMarker`, `DomainEvent`, `MutationContext`).
- `listing-domain` 의존 *없어요* — `ListingMarker`는
  `shared-kernel::id` 거쳐 phantom-typed FK로 참조해요.

## 예시

```rust,ignore
use listing_photo_domain::entity::ListingPhoto;
let photo = ListingPhoto::try_new(/* … */)?;
```
