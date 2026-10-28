# ADR 0310: 긴 굽기는 고정한 스냅숏에서 끝내고, 그다음 패치로 따라간다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0141](./0141-by-pnu-serving-publishes-changed-documents-as-patch-generations.md)(패치 세대·변경분),
  [ADR-0099](./0099-daily-serving-updates-bake-only-changed-parcels.md)(row_digest 로 바뀐 필지만),
  [ADR-0274](./0274-the-by-pnu-bake-does-only-the-work-a-change-needs.md)(`max_patches`·누적 비율·손익분기),
  [ADR-0146](./0146-a-by-pnu-lane-is-re-based-by-verifying-what-it-serves-and-its-reflected-snapshot-is-pinned.md) §2(서빙이 반영한 스냅숏을 태그로 고정),
  [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md)(배포가 중단 가능한 작업을 멈춘다),
  [ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md) H(이어하기)
- 관련 사건: 2026-10-24 parcel 레인 세대 5 가 15시간 굽다가 Gold 가 움직여 통째로 버려졌다.

## Context

전량 by-PNU 굽기(`scripts/ops/by-pnu-serving-bake.sh` → 섹션 팩 export)는 시작한 Gold 스냅숏을 각 샤드에
`…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID` 로 알려 줬지만, export 는 **표의 현재 스냅숏**을 읽고 그것이 고정값과 다르면
"moved during the bake" 로 거부했다. 스크립트는 그 거부를 "다음 실행이 처음부터"로 다뤘고, 다음 실행은 새 Gold
스냅숏을 보고 `in-progress*.json` 의 기록(다른 스냅숏)을 버린 채 **그 위 새 세대**를 처음부터 시작했다.

2026-10-23~28 실측:

- 세대 5: 10-23 11:15 시작 → 10-24 02:04 Gold 가 움직여 15시간 굽은 것을 버림. 세대 6 은 0 에서 다시.
- ADR-0301 이후 병합마다 배포가 이 굽기를 SIGTERM 으로 멈춘다. 멈춘 굽기의 샤드 요약은 실행 디렉터리
  (`/data/foundation-platform/by-pnu-bake/<lane>/runs/<gold>-packs-gN`)에 남아 **같은 스냅숏이면** 이어 굽지만,
  그 사이 Gold 가 한 번만 움직여도 전부 버려졌다.
- 그래서 parcel 프로필 v1→v2 스키마 변경(#780)의 전량 재굽기가 9일 넘게 끝나지 못했고, 서빙은 10일 넘게 낡았다.

Gold 는 하루 한 번 이상 움직이고(입력 주기 1440분 이하), 전량 굽기는 정상일 때 1.5시간이지만 느려지면 15시간,
배포로 끊기면 여러 날에 걸친다. **입력 주기보다 길어질 수 있는 작업이 입력이 움직일 때마다 처음부터 다시 하면
영영 끝나지 않는다.**

이미 있는 장치(새 장치를 만들지 않고 넓힌다):

- 고정값: `…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID`(export·publish 가 이미 받는다). publish 는 이미 그 스냅숏 기준
  (`load_snapshot_record_count`, 반영 태그)으로 검사한다 — 현재 스냅숏을 보지 않는다.
- 이어하기 기록: `in-progress.json`·`in-progress-packs.json`(`gold_iceberg_snapshot_id`·`mode`·`base_generation`·`target`).
- 실행 디렉터리의 샤드 요약(`shard-*.json`, 각자 `gold_iceberg_snapshot_id`·`document_schema_version`)과 "요약이 있으면
  그 샤드는 끝남" 규칙.
- 섞인 세대 거부: 팩 머리마다 Gold 스냅숏이 기록되고 `refuse_a_foreign_generation`(`first_pack_key`, #852)이 세대 첫
  팩의 스냅숏을 이번 실행의 스냅숏과 대조한다. 발행 전 `by_pnu_bake_complete` 가 모든 샤드 요약의 스냅숏을 대조한다.
- 패치 경로: `by_pnu_panel_delta.py`(row_digest 변경분, 반영 스냅숏→현재), `max_patches`·`max_cumulative_change_ratio`·
  `max_delta_fraction`.
- 만료: `lakehouse-maintenance.contract.json` `snapshot_expiry.retain_days` 30, 월말·연말 태그, by-PNU 발행이 반영
  스냅숏에 거는 `served-<lane>-…` 태그(ADR-0146 §2).

## Decision

1. **굽기는 고정한 스냅숏을 읽는다.** export(필지·건물 객체, 섹션 팩)는 `…_EXPECTED_GOLD_ICEBERG_SNAPSHOT_ID` 가 있으면
   표의 현재 스냅숏이 아니라 **그 스냅숏**의 manifest list 를 읽는다
   (`IcebergRestCatalog::load_snapshot_manifest_list`, `by_pnu_serving_pins::load_baked_snapshot`). Gold 가 굽기 도중
   움직여도 굽기는 멈추지 않는다. "moved during the bake" 거부는 없어진다.
2. **끊긴 full 굽기는 다음 실행이 같은 세대·같은 스냅숏으로 끝낸다.** 스크립트는 변경분·모드 판단보다 먼저 이어하기
   기록을 본다. 기록이 full 이고, 기록의 base 가 지금 서빙 base 와 같고, 그 세대가 아직 발행되지 않았고(`target > base`),
   기록의 스냅숏이 지금 Gold 와 다르면, 이번 실행의 스냅숏을 기록의 스냅숏으로 두고 full 로 그 세대를 이어 굽는다.
   같은 실행 디렉터리라 끝난 샤드는 요약이 있어 건너뛴다(진짜 이어하기). 실행 요약은 굽는 스냅숏
   (`gold_iceberg_snapshot_id`)과 지금 Gold(`gold_now_iceberg_snapshot_id`)를 함께 적는다.
3. **발행 뒤에는 패치로 따라간다.** 고정 세대가 발행되면 서빙이 반영한 스냅숏은 고정 스냅숏이고, 다음 실행은 기존 경로
   그대로 고정→현재 row_digest 변경분으로 패치(또는 reflect)를 낸다. `max_patches`·누적 비율·`max_delta_fraction`
   상한은 바꾸지 않는다 — 넘으면 지금처럼 full(현재 스냅숏으로 새로 고정) 또는 거부다.
4. **한 세대는 한 스냅숏·한 스키마다(섞인 발행 금지).**
   - 모든 샤드가 같은 고정값을 읽으므로 팩 머리의 스냅숏이 같다. `refuse_a_foreign_generation`(`first_pack_key`)은 그대로
     첫 팩의 스냅숏을 이번 스냅숏(=고정값)과 대조한다.
   - `by_pnu_bake_complete` 는 샤드 요약의 스냅숏이 하나인지에 더해 **문서 스키마가 하나인지**도 검사한다.
   - 끝난 샤드의 문서 스키마가 지금 export 의 스키마와 다르면 고정 세대를 이어 굽지 않는다(새 세대가 그 위로 간다).
   - 완료 검사에서 거부된 세대(`refused_incomplete` 를 기록에 남김)는 Gold 가 움직인 뒤 이어 굽지 않는다 — 다시 해도
     결과가 같다. 같은 스냅숏의 재실행은 지금처럼 이어 굽는다(손으로 요약을 지우고 다시 굽는 길).
   - 서빙은 append-only 그대로: 새 세대·새 패치만 쓰고, 버린 세대는 서빙되지 않은 채 남는다(지우기는 수동).
5. **고정 스냅숏이 만료되면 그 세대를 버리고 현재 스냅숏으로 새로 굽는다.** 카탈로그가 고정값을 더 갖고 있지 않으면
   export·publish 가 "`no longer holds snapshot`" 으로 거부하고, 스크립트는 재시도 없이 이어하기 기록을 지우고
   (`abandoned: …`) 실패로 끝낸다(경보). 다음 실행은 그 위 세대로 현재 스냅숏을 굽는다.
   - 왜 이것인가: 만료된 스냅숏의 행은 다시 읽을 수 없으니 그 세대를 끝낼 방법이 없다. 남은 샤드를 현재 스냅숏으로
     채우는 것은 4번(섞인 발행 금지) 위반이다. 사람을 기다리며 멈춰 두는 것은 서빙을 낡게 둘 뿐 얻는 것이 없다.
   - 왜 굽기 시작에 태그를 걸지 않는가: 만료 창(`retain_days` 30, 스냅숏 커밋 시각부터)은 굽기 시작 시점의 새 스냅숏을
     30일 지킨다. 고정 세대가 끝나기까지 30일을 넘는 일은 그 자체가 고장이고, 그때 세대를 버리는 것이 맞다. 태그는
     굽기 시작마다 카탈로그 쓰기(커밋)를 하나 더하고 풀어 줄 책임을 남기는데, 그 값을 치를 이유가 없다. 발행된 세대의
     반영 스냅숏은 이미 `served-<lane>-…` 태그(ADR-0146 §2)로 만료에서 지켜지므로 3번의 패치 기준점은 안전하다.
6. **규칙: 입력 주기보다 길어질 수 있는 작업은 고정한 입력에서 이어서 끝낼 수 있어야 한다.** 입력이 움직였다는 이유로
   끝낸 일을 버리지 않는다. by-PNU 굽기는 위 1–5로 이를 지킨다.
   - 가드(ADR-0001: 무엇을 막는가 — 2026-10-24 세대 5 가 Gold 이동으로 15시간 만에 버려진 일):
     `orchestration/tests/test_by_pnu_serving_bake.py`
     `test_a_full_bake_gold_moved_under_finishes_at_its_pin_then_patches_forward` 가 "배포로 끊긴 full 굽기 → Gold 이동 →
     다음 실행이 같은 세대·같은 스냅숏을 끝난 샤드 없이 이어 굽고 발행 → 그다음 실행이 패치"를 고정한다. 만료·스키마
     불일치·거부된 세대·두 스냅숏/두 스키마 요약의 경로도 같은 파일에, 운영자 팩 굽기는 `test_by_pnu_pack_bake.py`,
     고정 스냅숏 읽기는 `lakehouse-infrastructure` 어댑터 테스트가 지킨다.
   - 왜 jobs.v1.json 수준 가드가 아닌가: 문제의 길이는 한 실행의 상한이 아니라 실행 여러 번에 걸친 일의 길이다.
     by-PNU 굽기의 실행 상한(1260분)은 입력 주기(1440분)보다 짧아서 `time_limit_minutes` 비교로는 바로 이 사건을
     못 잡는다. 여러 실행에 걸친 길이는 어디에도 선언된 숫자가 없으므로, 동작 테스트가 이 규칙의 기계 검사다.

## Consequences

- Gold 가 굽기 도중·실행 사이에 몇 번 움직여도 전량 굽기는 끝난다. 배포로 끊겨도 끝난 샤드는 다시 굽지 않는다.
  #780 같은 스키마 변경 재굽기가 "Gold 가 멈출 때까지"를 기다리지 않는다.
- 고정 세대 발행 직후 서빙은 고정 스냅숏 기준이다. 다음 실행(입력 이벤트 또는 다음 날 10:15)이 패치로 현재까지
  따라간다 — 그 사이 한 실행분 낡음은 지금(전량을 처음부터 다시)보다 훨씬 짧다.
- 고정 세대가 진행 중인 동안 그 레인은 패치를 내지 않는다(지금도 full 이 필요한 상태에서는 패치를 내지 않는다).
- 고정→현재 변경분이 `max_delta_fraction` 을 넘으면 지금처럼 변경분이 거부된다(ADR-0141 §3). 그때는 운영자가 사유와
  함께 full 을 강제한다 — 이 ADR 이 바꾸지 않는다.
- 운영자 팩 굽기(`by-pnu-pack-bake.sh`)도 같은 export 를 쓰므로 Gold 이동에 멈추지 않고 첫 샤드의 스냅숏으로 끝까지
  굽는다. 만료된 고정값은 재시도하지 않는다.
- 배포 순서: 이 변경이 배포되면 진행 중이던 굽기는 ADR-0301 대로 멈추고, 다음 실행은 기존 기록(`in-progress-packs.json`,
  같은 형식)을 그대로 읽어 고정 스냅숏으로 이어 굽는다. 기록 형식은 바뀌지 않는다(`refused_incomplete` 만 거부 때 덧붙음).

## Alternatives rejected

- **Gold 작업을 멈춰 굽기를 기다리기(운영 절차)** — 사람이 손으로 하는 일이고, 수집·Gold 신선도를 굽기에 묶는다.
  runbook 에 적혀 있었지만 지켜지지 않았고 그 결과가 세대 5다.
- **남은 샤드를 현재 스냅숏으로 굽기** — 한 세대에 두 스냅숏이 섞인다. 발행 검사가 거부하고, 거부하지 않는다면
  서빙이 어느 시점의 데이터인지 말할 수 없다.
- **굽기 시작마다 Iceberg 태그로 고정** — 위 5번. 30일 창 안에서 필요 없고, 쓰기·해제 책임만 는다.
- **jobs.v1.json 에 새 필드("resumes at pin")** — 선언만 있고 검사할 길이가 없다(위 6번). 동작 테스트가 실제 행동을
  고정한다.
