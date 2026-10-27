# ADR 0298: content-addressed 미디어는 내용 해시 write-once 저장소이고, 객체마다 Bronze 원장 행을 만들지 않는다

- Status: Accepted
- Date: 2026-10-26
- Builds on: [ADR-0290 §7](./0290-court-auction-details-are-refetched-by-change-and-sale-date-and-photos-are-content-addressed.md)(사진은 내용 해시로 한 번만 저장), [ADR-0296 §2](./0296-public-auction-serves-full-info-and-shares-one-content-addressed-media-store.md)(경매·공매 공용 미디어 저장소), [ADR-0297](./0297-parcel-media-captures-originals-first-and-pre-generates-display-sizes.md)(원본 우선·파생), [FP-ADR-0016](../../platforms/foundation-platform/docs/adr/0016-bronze-commit-protocol.md)(Bronze 쓰기 관문)
- Amends: ADR-0290 §7 · ADR-0296 §2 — "사진은 FP-ADR-0016 관문으로 쓴다"는 문구를, **미디어 객체에 한해** 아래 write-once 내용주소 저장소로 구체화한다(원문의 raw 응답 Bronze 적재는 FP-ADR-0016 그대로).

## Context

ADR-0290 §7 과 ADR-0296 §2 는 사진·첨부(미디어)를 "내용 해시로 한 번만 저장"하고 그 쓰기를 **"FP-ADR-0016 관문으로 쓴다"**고 적었다. FP-ADR-0016(Bronze 쓰기 관문)은 객체마다 `bronze_object` **원장 행**을 만들고 소스/적재런과 묶어 복구 가능한 커밋을 준다 — gzip JSONL 원문 응답(한 API·한 run = 객체 하나)에 맞는 설계다.

구현(`foundation-shared-kernel/media.rs`, `foundation-outbox-publisher/src/onbid_media.rs`)은 미디어를 `<prefix>/sha256/<해시>.<확장자>` 로 **write-once**(이미 있으면 안 올림) 저장하고, sha256 을 객체 메타데이터로 박는다 — **객체별 bronze_object 원장 행은 만들지 않는다**. 즉 코드가 두 ADR 의 "관문" 문구와 어긋나 있었다(2026-10-26 SSOT 대조로 발견).

둘 중 무엇이 미디어의 정본 메커니즘인지 정해야 한다. 공매만 물건 ~7.8만 × 사진 여러 장 ≈ 수십만 장, 경매까지면 수백만이다.

## Decision

**content-addressed 미디어는 "내용 해시 write-once 객체 저장소"다. 객체마다 Bronze 원장 행(`bronze_object`)을 만들지 않는다.**

1. **키·쓰기**: 키 = `<prefix>/sha256/<해시>.<확장자>`(media.rs). 쓰기 = `put_object(CreateOnly)` + sha256 을 객체 메타로 기록. 같은 바이트는 같은 키라 한 번만 저장(중복제거). 키가 이미 있으면 건너뛴다.
2. **무결성·복구는 내용주소 그 자체로 얻는다**: 키가 곧 내용의 해시이므로 재시도는 멱등이고(같은 키 다시 시도), 충돌은 곧 동일 바이트다. FP-ADR-0016 의 "원장 행으로 반쪽 커밋 조정"은 미디어엔 불필요하다 — 조정할 원장 행 자체를 두지 않는다.
3. **참조·계보·감사·만료의 정본은 silver 사진표**(ADR-0296 §3)다: (물건·순번·제목·해시·크기·형식·처음 본 시각). "어떤 물건이 어떤 사진 해시를 갖는가"는 여기서 읽고, 여기서 감사·정리한다. 미디어 객체 자체는 해시로만 식별되는 불변 blob.
4. **raw 응답 Bronze 는 그대로 FP-ADR-0016**: 물건상세/목록/입찰결과 gzip JSONL 은 지금처럼 `bronze_committer` 로 원장 행과 함께 커밋(한 API·한 run = 객체 하나). 이 ADR 은 **미디어 blob 에만** 적용된다. Bronze 는 응답 안의 사진 값을 `sha256:<해시>` 참조로 바꿔 보관(ADR-0290 §7)하므로, 응답+미디어로 원본을 복원할 수 있다.

## Consequences

- `onbid_media.rs` 의 `put_object(CreateOnly, sha256)` 가 바로 이 메커니즘이다 — **코드가 이미 정본과 일치**(이 ADR 로 어긋남이 닫힌다). `bronze_committer` 로 미디어를 라우팅하지 않는다.
- `bronze_object` 원장이 수백만 blob 행으로 부풀지 않는다. 정석(Git·Docker/OCI·Nix store)과 같다 — blob 은 내용주소로 저장하고 상위 구조가 참조한다.
- 경매 사진(ADR-0290 §7, 미구현)도 같은 저장소·같은 메커니즘을 쓴다.
- 감사·만료가 필요하면 silver 사진표(+ R2 인벤토리)로 한다. 지금은 append-only([[append-only-for-all-data]]).

## Alternatives rejected

- **미디어도 FP-ADR-0016 관문(객체별 bronze_object 행)** — 원문 문구대로. 수백만 dedup blob 에 원장 행을 달아 테이블을 부풀리는데, 해시 키가 이미 주는 무결성·멱등 위에 얻는 게 없다. 기각.
- **코드를 그냥 두고 ADR 침묵** — SSOT 어긋남을 문서로 안 닫으면 다음 사람이 "관문이라 했는데 왜 put_object?"로 또 헷갈린다(2026-10-26 내가 그랬다). 기각.

## Open

- silver 사진표의 정확한 열과 "처음 본 시각/마지막 확인" 은 첫 수집 실측 뒤 ADR-0296 §3 계약에서 확정.
- R2 인벤토리 기반 미디어 감사·고아 탐지는 필요해질 때(지금은 append-only).
