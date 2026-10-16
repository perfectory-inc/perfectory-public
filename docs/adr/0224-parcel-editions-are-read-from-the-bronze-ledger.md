# ADR 0224: 필지 판의 객체는 Bronze 장부에서 읽고, 원천 계약은 규칙만 갖는다

- Status: Accepted
- Date: 2026-10-14
- Builds on: [ADR-0169](./0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md) §1–2
  (장부 옆 ZIP 안 이름, "계약에는 규칙만, 사실은 장부에"), [ADR-0148](./0148-cadastral-parcel-editions-are-held-side-by-side.md)
  (필지 판을 나란히 쌓는다)
- Amends: ADR-0148 의 "판은 원천 계약에 하나씩 적는다"와 판 제안(`propose`)·계약 PR 절차. ADR-0169 Consequences 의
  "필지 판의 측정 도구는 필지 판 제안이 새 표를 읽게 될 때까지 남는다"를 끝낸다.

## Context

- `infra/lakehouse/contracts/vworld-parcel-source-objects.json` 은 판마다 객체 272 개(키·크기·데이터셋 이름·지역 코드·
  알갱이)와 추출일·핸드오프 자리를 손으로 적었다. 판 둘로 160 KB 였고, 새 판마다 수집 작업이 낸 제안을 사람이 PR 로
  넣었다.
- 같은 사실이 이미 장부에 있다. 2026-10-14 운영 장부 읽기: `vworldkr__parcel` 객체 545 개(판 둘 × 272 + 칸 정의서 1)가
  `catalog.bronze_object` 에 있다. ZIP 안 이름은 ADR-0169 §1 의 표가 담는데, 측정 기본 목록(`vworldkr__land*,hubgokr__*`)에
  필지가 빠져 있어 545 개 모두 아직 안 쟀다(다른 원천 12,330 개는 모두 잼).
- 같은 목록이 두 곳에 있으면 어긋날 때 어느 쪽이 맞는지 아무도 모른다. 판을 늘리는 일은 사람을 거쳤고, 측정 도구
  (`vworld_parcel_edition_members.py`, GDAL 컨테이너)가 장부 측정과 따로 하나 더 있었다.

## Decision

1. **판의 객체는 장부의 것이다.** 발행기 `vworld-parcel-editions` 가 실행할 때마다 장부(`catalog.bronze_object` +
   `catalog.bronze_object_member`)에서 판을 만든다(`vworld_parcel_source_contract::editions`). 객체의 ZIP 안 이름 중 정확히
   하나가 계약의 `member_name` 에 맞으면 그 판의 객체이고, 지역 코드 두 자리는 시도, 다섯 자리는 시군구다. 추출일은 판의
   모든 멤버 날짜 범위, 핸드오프 자리는 `handoff_prefix/edition=<판>`(그 규칙 전에 적재한 202606 만 닫힌
   `handoff_prefix_legacy`)이다. 출력은 예전 계약 항목과 같은 모양이라 Python·셸은 `vworld_parcel_editions.py --editions` 로
   그대로 읽는다.
2. **거부와 건너뜀.** 아직 안 잰 객체가 있으면 거부하고 측정 명령을 이름 짓는다(그 객체가 가장 새 판일 수 있다). 같은
   판·지역 객체가 둘이면 제공자 갱신일이 늦은 것, 같으면 거부한다. 시도 객체가 시군구 객체 밖을 덮거나 앞 판보다 덜 덮는
   판(`new_edition_bounds`, 예전 `propose` 의 검사)은 반쯤 받은 것으로 건너뛰고 이름을 남긴다. 지도가 쓰는 판이 장부에
   없거나 건너뛰어지면 거부한다.
3. **계약에는 규칙만.** 계약(schema_version 3)은 `member_name`, `load_granularity`, `new_edition_bounds`, 핸드오프 규칙,
   `snapshot_id_prefix`, 그리고 사람의 결정인 `served_edition` 과 그 시도 `served_edition_sido` 를 갖는다. 시도는 컴파일되어
   들어가는 시군구 크로스워크(ADR-0142)가 DB 없이 읽어야 해서 남겼고, 장부를 읽을 때마다 지도가 쓰는 판의 시군구 객체가
   덮는 시도와 같은지 확인한다. **`editions`·`objects` 키는 계약 어디에도 다시 둘 수 없다:** Rust(`parse`)와
   Python(`load`) 둘 다 그런 계약을 거부하고, 시험이 실제 계약과 심은 위반으로 그것을 본다.
4. **측정은 하나.** 필지를 `bronze-object-members.sh` 의 기본 목록에 넣는다(매일 수집 뒤). 판 작업
   (`vworld-parcel-edition-collect.sh`)은 그 스크립트로 재고 장부에서 판을 읽는다. GDAL 측정 도구와 판 제안·`proposed/`
   파일·계약 PR 은 지운다. 새 판을 받은 날 작업은 3 으로 끝나고 "new edition waiting for Silver load" 를 보낸다.
5. **읽는 곳.** 카탈로그 투영 적재와 전국 PostGIS 미러는 같은 DB 연결로 장부를 읽는다(`served_edition_view_from_ledger`).
   변환·적재 스크립트와 법정동 짝 맞추기(`--parcel-editions`, 정기·미리 보기 둘 다)는 발행기 출력 파일을 넘긴다.
   `source_snapshot_id` 와 지도가 쓰는 판만 읽는 Spark 작업들은 계약만으로 돈다.

## Consequences

- 계약 160 KB → 5 KB. 판마다의 손 PR 이 없어진다. 새 판은 수집·측정 뒤 장부에 나타나고, 적재만 런북으로 사람이 한다
  (spark 풀 자리, ADR-0138).
- 배포 직후 필지 객체 545 개가 아직 안 쟀으므로, 다음 매일 수집(03:40)이 재기 전까지 장부를 읽는 명령은 측정 명령을
  이름 지으며 거부한다. lineage_stewardship(06:50)은 그 뒤에 돈다. 더 빨리 하려면 운영자가
  `bronze-object-members.sh vworldkr__parcel` 을 한 번 돌린다(객체마다 범위 GET 1–3 번).
- 장부를 읽는 셸은 `DATABASE_URL` 이 필요하다. 없으면 `database-url.sh` 로 만든다.
- 기각한 대안: 판마다 추출일·핸드오프 자리만 남기고 객체만 지우기 — 판마다의 손 PR 이 그대로 남는다. Python 이 DB 를
  직접 읽기 — 장부 읽기가 Rust 와 Python 두 벌이 된다. 크로스워크의 시도도 장부에서 읽기 — 컴파일된 순수 커널이
  DB 를 알게 된다.
