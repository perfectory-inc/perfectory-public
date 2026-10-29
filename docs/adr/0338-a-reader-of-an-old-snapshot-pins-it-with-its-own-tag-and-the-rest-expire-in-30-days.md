# ADR 0338: 옛 스냅숏을 읽는 독자는 자기 태그로 고정하고, 나머지는 30일 뒤 만료된다

- Status: Accepted
- Date: 2026-10-29
- Supersedes: [ADR-0238](./0238-every-lakehouse-table-is-maintained-on-a-schedule.md) 결정 3(`protected_snapshots`),
  [ADR-0310](./0310-a-long-bake-finishes-at-its-pin-then-patches-forward.md) 5번의 "굽기 시작에 태그를 걸지 않는다"
- Amends: [ADR-0332](./0332-an-ingest-record-is-copied-into-a-table-property-before-its-snapshot-can-expire.md) 4번(만료 보호를 남긴다)
- Builds on: [ADR-0237](./0237-lakehouse-snapshots-are-kept-30-days-12-month-ends-and-every-year-end.md)(30일·12개월·영구),
  [ADR-0146](./0146-a-by-pnu-lane-is-re-based-by-verifying-what-it-serves-and-its-reflected-snapshot-is-pinned.md) §2(`served-` 태그),
  [ADR-0180](./0180-panel-gold-recomputes-only-the-pnus-whose-inputs-changed.md)(`foundation-gold-input-` 태그)

## Context

ADR-0237 은 스냅숏을 30일 보존한다. 그러나 정비 작업의 만료 보호(`lakehouse_maintenance.protected_snapshots`)가
두 가지를 지키려고 만료 하한을 끌어내렸다.

- main 계보에서 적재 기록(`foundation.ingest-batch-objects`)을 가진 모든 스냅숏. 가장 오래된 기록이 첫 적재라서,
  기록이 있는 표는 첫 적재 뒤로 아무것도 만료하지 않았다.
- Gold 표에서 Silver 판(`foundation.source-iceberg-snapshots`)을 적은 가장 새 커밋.

ADR-0332 가 적재 기록을 표 속성으로 옮겼다. 그래서 기록은 더 이상 옛 스냅숏이 필요 없다. 그런데 보호가 우연히
함께 지키던 독자가 있었다. 이 결정 전에 스냅숏을 id 로 읽는 곳을 전수 조사했다(`snapshot-id` 읽기 옵션,
`VERSION AS OF`, Rust `load_snapshot_manifest_list`; 17개 파일 25곳).

| 독자 | 읽는 옛 스냅숏 | 지금 지키는 것 |
|---|---|---|
| Gold 증분 재구축(`gold_incremental`, `lakehouse_snapshot_pins`) | 현재 Gold 를 만든 Silver 판 | 태그 `foundation-gold-input-<gold>`(이미 있음) |
| Gold 재구축 계획(`gold_rebuild.recorded_pins`·`head_build_pins`) | 머리에서 판을 적은 커밋까지의 Gold 계보 | **보호뿐** |
| FLOOR 이력 검사(`building_register_floor_history`) | 증인 스냅숏과 머리까지의 append 계보 | **보호뿐**(증인이 적재 기록을 가져서) |
| by-PNU 굽기(`load_baked_snapshot`, ADR-0310) | 이어 굽는 세대가 고정한 Gold 스냅숏 | **없음**. ADR-0310 은 "30일 창이 굽기 시작부터 지킨다"고 봤으나, 창은 스냅숏 커밋 시각부터 센다 |
| by-PNU 변경분(`by_pnu_panel_delta`) | 서빙 manifest 가 반영한 스냅숏 | 태그 `served-<lane>-…`(이미 있음) |
| 재시도 인증(`read_recorded_append`) | 기록한 append | 없음. 만료되면 다시 붙이지 않고 거부한다(ADR-0332) |
| 운영자 명령(`unit_official_price_projection_load`, `parcel-boundary-equality.py`) | 운영자가 이름한 스냅숏 | 없음. 만료되면 Trino 가 실패하고 아무것도 올리지 않는다 |
| `changelog.changes_between` | 소비자의 옛 판부터의 단계 | 없음. 운영 호출자가 없고, 단계가 빠지면 None 으로 두 스냅숏 비교로 간다 |
| 나머지 8개 파일 | 같은 실행에서 읽은 머리 | 머리는 만료되지 않는다 |

**잰 것(2026-10-29, 운영 Trino `$snapshots`·`$refs`·`$history`·`$all_entries`·`$all_manifests` 읽기 전용, 표 51개).**

- 스냅숏 674개, 그중 30일이 넘은 것 185개. 적재 기록을 가진 표 29개(ADR-0332 의 27 은 그 측정 범위 기준).
- 지금 규칙으로 만료되는 것: 1개(`silver.administrative_boundaries`). 보호가 나머지를 붙든다.
- 그 밖에 이미 걸린 태그: `served-` 10개 표, `foundation-gold-input-` 18개 표, `monthly-` 27개 표. 손으로 만든
  `foundation_ingest_checkpoint_retained_*` 2개(만료 나이 무한)는 이 저장소 코드가 만들지 않으며 손대지 않는다.
- 진행 중인 굽기 고정값(`in-progress*.json`): `gold.building_panel` 1개, `gold.parcel_forest_ledger` 1개.

### 대기업·오픈소스는 어떻게 하나

- **Apache Iceberg 문서(Branching and Tagging)**: 오래 남길 기준점은 태그로 걸고 `max-ref-age-ms` 로 각자의 수명을
  준다. 스냅숏 만료는 살아 있는 참조가 가리키는 스냅숏을 지우지 않는다. 그래서 만료 정책은 하나로 두고, 오래 필요한
  쪽이 태그를 건다.
- **Netflix(Iceberg 를 만든 곳)**: 감사·재현용 시점은 태그로 남기고, 일반 만료는 표마다 정해진 창으로 돈다.
  Write-Audit-Publish 의 감사 브랜치도 같은 참조 모델이다(ADR-0321 §5 가 이미 따른다).
- **Delta Lake**: `delta.deletedFileRetentionDuration` 하나로 VACUUM 하고, 오래 남길 시점은 `SHALLOW CLONE` 으로
  따로 고정한다. 보존 정책을 독자마다 비틀지 않는다.
- **Apache Flink Iceberg sink**: 요약에 둔 체크포인트 기록 때문에 "그 스냅숏을 만료하지 말라"고 문서가 경고한다.
  우리 보호가 그 모양이었고, ADR-0332 가 기록을 옮겨 그 의존을 끊었다.

## Decision

1. **보호를 걷는다.** `protected_snapshots` 는 없어진다. 만료는 30일 창(`retain_days`)과 `retain_last` 로 돌고,
   Iceberg 규칙대로 태그가 가리키는 스냅숏만 남긴다.
2. **옛 스냅숏을 읽는 독자는 자기 태그를 건다.** 태그는 독자가 기대는 데이터를 커밋하는 흐름 안에서 만들고, 각자의
   규칙으로 푼다. 목록과 규칙은 `lakehouse-maintenance.contract.json` `snapshot_expiry.reader_pins.tags` 하나에 있다.

   | 태그 | 만드는 곳 | 푸는 규칙 |
   |---|---|---|
   | `served-<lane>-…`(ADR-0146 §2) | by-PNU 발행, manifest 를 쓰기 전 | 더 새 발행이 살아 있으면 `release_older` |
   | `baking-<lane>-<스냅숏>`(**새로**) | 굽기의 각 export 샤드, 고정 스냅숏을 읽기 전(`pin_bake`) | 레인의 다음 발행(`release_bake_pins`). 그 세대는 서빙되거나(그러면 `served-` 가 같은 스냅숏을 잡는다) 버려졌다 |
   | `foundation-gold-input-<gold>`(ADR-0180) | Gold 생산자 커밋 뒤 Silver 입력마다 | Gold 의 다음 성공 커밋이 새 판으로 옮긴다. 떠난 스냅숏은 그때부터 30일 창이 유예다 |
   | `foundation-gold-build`(**새로**) | `Outcome.commit` 이 판을 적은 커밋에(`retain_build`) | Gold 의 다음 생산자 커밋이 옮긴다 |
   | `foundation-floor-history-witness`(**새로**) | FLOOR 이력 검사가 증인을 인증한 뒤(`pin_witness`) | FLOOR 증인 파일과 함께 손으로 |
   | `monthly-`·`yearly-`(ADR-0237) | 정비 작업 | 12개월 창 / 영구 |

   `max-ref-age-ms`(`RETAIN`)는 쓰지 않는다. Iceberg 의 그 나이는 스냅숏 커밋 시각부터 세므로, 오래 바뀌지 않은
   스냅숏을 방금 고정한 독자가 바로 잃는다(ADR-0237 6번과 같은 이유). 수명은 위의 "푸는 규칙" 이 정한다.
3. **계보를 걷는 독자의 태그는 그 계보까지 지킨다.** Iceberg 는 태그가 가리킨 스냅숏만 남기고, 그와 머리 사이 커밋은
   지운다. Gold 계획과 FLOOR 검사는 그 사이를 걷는다. 계약에서 `walks_main_back_to_it` 인 태그가 main 계보에 있으면,
   만료 하한(`older_than`)을 그 스냅숏의 커밋 시각으로 내린다(`expiry_floor_millis`). Gold 는 판을 적은 커밋 뒤에
   압축 커밋만 오므로 지키는 것은 그 몇 개다. FLOOR 는 append 만 쌓는 표라 데이터 파일을 더 붙들지 않는다.
4. **태그가 아직 없으면 그 표는 기다린다.** `required_on` 에 있는 표(Gold 판을 적은 표 전부, `silver.building_register_floors`)는
   해당 태그가 생기기 전에는 아무것도 만료하지 않고 `maintenance-skip … reason=reader_pin_missing` 을 남긴다. Gold 는
   다음 생산자 커밋, FLOOR 는 다음 FLOOR 실행이 태그를 건다. 기다리는 쪽이 안전한 쪽이다.
5. **만료는 먼저 말하고, 아는 고정값이 있으면 거부한다.**
   - 매 실행, 표마다 Iceberg `RemoveSnapshots` 규칙을 그대로 옮긴 계산(`would_expire`)으로 만료될 스냅숏 id 를
     먼저 로그에 남긴다(`maintenance-would|expiring step=snapshot_expiry … ids=…`). `--dry-run` 은 여기서 멈춘다.
   - 정비 래퍼(`lakehouse-maintenance.sh`)는 Spark 전에 이 호스트가 카탈로그 밖에 쥔 고정값을 모은다:
     by-PNU 각 레인의 `in-progress*.json`(이어 굽는 세대의 고정 스냅숏)과 `state.json`(마지막으로 읽은 서빙 상태의
     반영 스냅숏). 만료될 스냅숏에 그것이 있으면 그 표는 실패하고 아무것도 지우지 않는다. 태그가 생기기 전에 시작한
     굽기와 태그 쓰기에 실패한 경우를 잡는다.
   - 만료 뒤 남은 스냅숏을 다시 읽어, 로그에 없던 스냅숏이 사라졌으면 그 표를 실패로 남긴다.
6. **스냅숏을 id 로 읽는 코드는 모두 계약에 있다.** `reader_pins.readers` 가 파일마다 읽는 곳의 수와 그것을 지키는
   것(독자 태그, 같은 실행의 머리 `head`, 또는 지키지 않는 이유)을 적는다. 시험이 트리를 훑어 수를 맞춘다.
7. **첫 실행 순서.** 배포 뒤 첫 예약 실행 전에 운영자가 `lakehouse-maintenance.sh --dry-run` 을 한 번 돌려 표마다
   `ids=` 를 아래 표와 맞춘다. 다르면 예약 실행을 멈추고 원인을 본다. 이 결정은 #935(ADR-0332) 다음에 배포한다: 기록이
   속성으로 옮겨지기 전에 기록 스냅숏이 만료되면 객체가 다시 붙는다. 같은 실행 안에서 속성 복사가 만료보다 먼저이고,
   복사가 실패하면 만료로 가지 않는다.

## Consequences

**표마다 스냅숏 수(2026-10-29 실측 → 첫 만료 뒤 계산).** 첫 만료 뒤 = 30일 안 + 태그(지금 있는 것과 이 실행이 만들
달 끝 태그) + 머리 + 계보 태그(3번)로 계산했다. 데이터 해제는 `$all_entries` 로 파일마다 그 파일을 가진 스냅숏이
모두 만료되는지 본 값이다.

| 표 | 지금 | 30일 넘음 | 첫 만료 뒤 | 만료 | 데이터 해제 |
|---|---:|---:|---:|---:|---:|
| silver.administrative_boundaries | 4 | 1 | 3 | 1 | 0 |
| silver.building_register_apartment_price | 35 | 21 | 15 | 20 | 0 |
| silver.building_register_floors | 3 | 1 | 3 → 2(증인 태그 뒤) | 0 → 1 | 0 → 1.20 GB |
| silver.building_register_titles | 7 | 2 | 6 | 1 | 0.69 GB |
| silver.building_register_unit_areas | 16 | 3 | 14 | 2 | 8.76 GB |
| silver.building_register_units | 11 | 4 | 8 | 3 | 2.43 GB |
| silver.land_characteristic | 18 | 17 | 2 | 16 | 0 |
| silver.land_forest_ledger | 33 | 17 | 17 | 16 | 0 |
| silver.land_individual_price | 18 | 17 | 2 | 16 | 0 |
| silver.land_right_registration | 23 | 17 | 7 | 16 | 0 |
| silver.land_transfer_history | 34 | 17 | 18 | 16 | 0 |
| silver.land_use_plan | 18 | 17 | 2 | 16 | 0 |
| silver.parcel_boundaries | 42 | 25 | 20 | 22 | 10.90 GB |
| silver.unit_official_price | 50 | 16 | 35 | 15 | 0 |
| 나머지 37개 표 | 362 | 10 | 362 | 0 | 0 |
| **합계** | **674** | **185** | **514 → 513** | **160 → 161** | **22.77 → 23.97 GB** |

- 나머지 37개 표의 30일 넘은 스냅숏 10개는 달 끝 태그나 머리라서 남는다. Gold 표는 30일 넘은 커밋이 거의 없고,
  `foundation-gold-build` 가 생길 때까지 기다린다(4번).
- 데이터 0 인 표는 append 만 쌓였거나 지금 파일이 그대로라서, 만료가 메타데이터(스냅숏 항목, manifest list 161개,
  manifest 29개 0.43 MB)만 줄인다. 그래도 매 커밋이 다시 쓰는 `metadata.json` 이 그만큼 작아진다.
- 이 표를 넘어선 뒤로는 30일 창이 매일 굴러간다. 적재가 잦은 공매 표(`onbid_*`, 지금 49~60개, 모두 30일 안)도
  30일치만 남는다.
- 비용: 계보 태그가 지키는 것은 Gold 의 판 커밋 뒤 압축 몇 개와 FLOOR 계보뿐이다. 새 카탈로그 쓰기는 Gold 커밋마다
  태그 이동 하나, 굽기 샤드마다 태그 확인 하나(같은 이름·같은 스냅숏이면 커밋 없음), 발행마다 굽기 태그 해제다.
- 되돌리기: 만료는 되돌릴 수 없다. 그래서 5번의 사전 로그·거부·사후 대조, 7번의 dry-run 순서가 있다. 계약의
  `retain_days` 를 늘리는 것만으로 다음 실행부터 덜 지운다.
- 버리는 것: 운영자 명령이 이름한 옛 스냅숏(6번 목록의 "지키지 않음")은 30일 뒤 읽을 수 없다. 오래 둘 시점은 운영자가
  태그를 건 뒤 읽는다.

### 가드(ADR-0001)

- `infra/lakehouse/spark/tests/test_snapshot_readers_are_pinned.py` — **막는 위험: 태그 없이 옛 스냅숏을 읽는 독자.**
  이 결정의 감사에서 by-PNU 굽기의 고정 스냅숏과 FLOOR 증인을 읽는 코드가 어떤 태그로도 지켜지지 않고, 표 29개에서
  스냅숏 161개(약 24 GB)를 붙든 보호에 우연히 기대고 있었다. 새로 id 로 읽는 곳은 계약에 무엇이 지키는지 적어야 통과한다.
- `test_lakehouse_maintenance.py` `ReaderPinTest`: 기록만으로는 남지 않음, 계보 태그의 하한, 태그 없는 표는 기다림,
  `RemoveSnapshots` 규칙(태그·나이 지난 태그·브랜치 밖·`retain_last`), 카탈로그 밖 고정값 거부.
- `test_month_end_tags_iceberg.py`(고정 Iceberg 런타임): 태그 없는 스냅숏은 창 밖에서 지워지고, Gold 는 태그 전에는
  기다리고 태그 뒤에는 계보만 남으며, 굽기 고정값이 걸린 표는 거부된다.
- `by_pnu_serving_pins` Rust 시험: 굽기 태그는 샤드마다 다시 걸어도 하나이고, 발행은 그 레인의 굽기 태그만 푼다.
- `test_lakehouse_maintenance_script.py`: 래퍼가 `in-progress*.json`·`state.json` 의 스냅숏을 모아 작업에 넘긴다.

## Rejected

- **보호를 그대로 두기(ADR-0332 4번)** — 표 29개가 30일 보존을 받지 못하고, 독자가 무엇에 기대는지 아무 데도 적히지
  않는다. 이번 감사에서 태그 없는 독자 둘이 그 보호에 우연히 기대고 있었다.
- **독자마다 보존 정책을 비트는 하한 계산을 늘리기**(지금 보호의 방식) — 메타데이터 모양에서 "누가 읽을 것" 을 추측한다.
  추측이 틀리면 조용히 지우거나 조용히 영원히 남긴다. 태그는 독자 스스로 남기는 선언이다.
- **`max-ref-age-ms`(`RETAIN n DAYS`)로 태그 수명 주기** — 커밋 시각부터 센다. 오래 바뀌지 않은 Gold 를 오늘 고정한
  굽기가 오늘 잃는다(2번).
- **굽기 고정값을 정비 작업이 `in-progress*.json` 으로만 지키기** — 같은 호스트의 파일에 기대는 우회다. 정비가 다른
  호스트로 옮기면(ADR-0335) 조용히 사라진다. 태그가 정본이고, 파일 대조는 태그 이전 굽기와 태그 실패를 잡는 이중 확인이다.
- **FLOOR 증인을 계약에 스냅숏 id 로 적기** — 증인은 의도적으로 저장소 밖 비공개 파일이다("영수증이 자기 권위를 정할 수
  없다"). 증인을 인증한 검사 자신이 태그를 건다.
- **Gold 계획이 계보 대신 태그를 바로 읽게 바꾸기** — 압축만 사이에 있다는 확인(`head_build_pins`)을 잃는다. 그 몇 개
  커밋을 남기는 비용이 더 싸다.
