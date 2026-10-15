# ADR 0242: 대체된 묶음 세대는 다음 세대가 서빙된 지 7일 뒤에 지운다

- Status: Accepted
- Date: 2026-10-14
- Builds on: [ADR-0147](./0147-by-pnu-documents-are-served-from-section-packs.md)(묶음 세대),
  [ADR-0233](./0233-every-by-pnu-pack-is-read-by-one-rule-head-then-range.md)(조각 없는 g2 재굽기)

## Context

ADR-0233 으로 필지 종류 6레인을 조각 없이 g2 로 다시 굽는다. 운영 중인 용도지역·공시지가 g1 과 미발행 특성 등 g1 은
레인마다 약 12GB 다. 묶음은 Gold 에서 언제든 다시 구울 수 있는 서빙 사본이고 정본은 Gold 다. 지금까지 대체된 세대를
지우는 경로가 없어 사본이 두 벌씩 영구히 쌓였다. 코디네이터 결정(2026-10-14): 새 세대로 넘어간 뒤 7일은 되돌리기용으로
두고, 그 뒤 옛 세대 객체를 지운다.

## Decision

1. **계약** `by_pnu_section_packs.superseded_generation_keep_days` = 7.
2. **"넘어간 때"는 manifest 가 말한다.** 서빙 중인 세대를 처음 이름한 manifest(현재 manifest 와 manifest 기록의 교체된
   것들 중 가장 이른 `published_at_utc`)가 그 세대의 서빙 시작이다. 별도의 기록을 두지 않는다.
3. **명령** `retire-<lane>-superseded-pack-generations`: 섹션마다 서빙 세대보다 **작은** 세대만 대상이다. 서빙 세대와 그보다
   큰 세대(되돌리기로 옛 세대를 서빙하는 동안의 새 세대)는 건드리지 않는다. 기간 안이면 남긴다(`kept_until`). 지울 세대
   아래의 키는 모두 그 세대의 묶음(기본·패치)이거나 조각 색인이어야 하고, 다른 키가 하나라도 있으면 아무것도 지우지 않고
   멈춘다. `<LANE>_PACK_RETIRE_EXECUTE=true` 가 없으면 무엇을 지울지만 말한다.
4. **매일 돈다.** 예약 작업 `by_pnu_serving_bake` 의 `all` 이 모든 레인을 구운 뒤 `retire` 를 레인마다 그 레인의 잠금 안에서
   부른다. 실패한 레인은 작업을 실패로 만들고 다음 날 다시 시도한다(두 번째 사본이 조용히 남지 않는다).
5. 지우는 경로는 이것 하나다. 나머지 서빙 객체는 그대로 create-only 다.

## Consequences

- 레인마다 g2 가 서빙된 지 7일 뒤 첫 예약 실행에서 g1 이 지워진다. R2 삭제는 과금되지 않는다.
- 7일이 지나면 g1 을 이름한 manifest 기록으로는 되돌릴 수 없다. 그 뒤의 되돌리기는 Gold 에서 다시 굽는 것이다.
- 패치 세대도 그 기본 세대 아래에 있으므로 함께 지워진다.
