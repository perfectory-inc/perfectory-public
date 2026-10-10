---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-10
---

# Silver 레인 새로 고침 — 건축HUB·VWorld 토지 레인 런북

[루트 ADR-0169](../../../../docs/adr/0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md)
②·③의 운영 절차다. 레인 하나가 Bronze 장부(`catalog.bronze_object`)에서 가장 새 완전한 원천 판을 고른다. Silver 표가
그 판을 이미 가졌으면 수 초 만에 `unchanged` 로 끝나고, 아니면 내보내고 적재한다. 판을 계약에 적어 올리는 PR 은 없다.
VWorld 토지 레인의 판은 장부 옆 ZIP 안 이름(`catalog.bronze_object_member`, ①)에서 읽는다(5 절).

| 무엇 | 정본 |
| --- | --- |
| 레인 목록과 레인이 하는 일 | 발행기 `run-silver-refresh` (`remote_lakehouse_job/silver_refresh/`) |
| 레인의 원천 역할·내보내기·Spark 크기·쓰기 방식 | 레인 계약의 `silver_refresh` 블록 (`infra/lakehouse/contracts/hub-building-register-*-source-objects.json`, `vworld-land-*-source-objects.json`) |
| 판 규칙 | 같은 계약의 `release_rule` (ADR-0169 §2) |
| 실행 계정·환경 파일·시간·메모리 한계 | `infra/systemd/foundation-silver-refresh@.service` → `scripts/ops/silver-refresh.sh` |
| 환경 파일 | `config/runtime-secrets.contract.json` 의 유닛 `foundation-silver-refresh@.service`, 실행 `silver-refresh-plan` |
| 작업 폴더·증거 | `/data/foundation-platform/silver-refresh/<레인>/work`(실행마다 비운다), `…/<레인>/evidence/<identity>/`(요약, 쌓기만) |

| 레인 (유닛 인스턴스) | Silver 표 | 원천 | 쓰기 |
| --- | --- | --- | --- |
| `building-register-titles` | `silver.building_register_titles` | 표제부 | 한 묶음 overwrite |
| `building-register-units` | `silver.building_register_units` | 전유부 + 같은 날 표제부·기본개요 | 한 묶음 overwrite |
| `building-register-unit-areas` | `silver.building_register_unit_areas` | 전유공용면적 | 한 묶음 overwrite |
| `building-register-apartment-price` | `silver.building_register_apartment_price` | 공동주택가격 | 부분 4개씩 append |
| `building-register-exclusive-unit` | `silver.building_register_exclusive_unit` | 전유부(다리) | 부분 4개씩 append |
| `land-characteristic` | `silver.land_characteristic` | 토지특성 `AL_D195` 시도 17 | 첫 4 객체 overwrite, 나머지 4개씩 append |
| `land-forest-ledger` | `silver.land_forest_ledger` | 임야대장 `AL_D003` 시도 17 | 같음 |
| `land-individual-price` | `silver.land_individual_price` | 개별공시지가 `AL_D151` 시도 17 | 같음 |
| `land-right-registration` | `silver.land_right_registration` | 대지권등록 `AL_D006` 시도 17 | 같음 |
| `land-transfer-history` | `silver.land_transfer_history` | 토지이동이력 `AL_D157` 시도 17 | 같음 |
| `land-use-plan` | `silver.land_use_plan` | 토지이용계획 `AL_D155` 시도 17 | 같음 |
| `land-use-zone-code` | `silver.land_use_zone_code` | 용도지역 코드표 `LART_LMISZONE` 전국 1 | 한 객체 overwrite |

## 1. 한 번의 실행

1. 장부에서 레인 원천의 행을 읽고, 모든 역할이 있는 가장 새 제공자 월을 고른다. 빠진 역할이 있는 더 새 월은
   `skipped_incomplete_release=<YYYYMM>` 로 남기고 넘어간다.
2. Silver 표의 스냅숏 요약(`foundation.ingest-batch-objects`, 적재기 `append_batch_once` 의 기록)을 카탈로그에서
   읽는다. 그 판이 있으면 `already_loaded`, 더 새 판이 있으면 `newer_release_loaded` 다. 둘 다 `unchanged` 로 끝난다.
3. 아니면 ZIP 을 장부의 크기·SHA-256 로 확인하며 받는다. 그다음 레인의 내보내기를 돌리고 compose `spark` 로
   적재한다. 마지막으로 표의 기록을 다시 읽는다.
4. 마지막 줄:

   ```text
   silver-refresh-outcome lane=<레인> outcome=changed|unchanged reason=<…> release=<YYYYMM> identity=<…> rows=<…>
   foundation-job-outcome changed|unchanged
   ```

   둘째 줄은 모든 예약 작업이 끝에 찍는 줄이다(ADR-0171). `--plan` 은 찍지 않는다.

레인은 한 번에 하나만 돈다. 각 레인이 compose `spark` 의 상한(20g) 전부를 쓰기 때문이다. 두 번째 레인은
`refused: another lane holds …/refresh.lock` 로 75 를 내고 끝난다.

## 2. 예약 작업으로서 (꺼진 채 등록)

레인 열둘은 `orchestration/jobs.v1.json` 의 작업 `silver_refresh_building_register_*`·`silver_refresh_land_*` 다
([루트 ADR-0171](../../../../docs/adr/0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)).
`started_by: inputs` 라서 source_sweep 이 새 파일을 받은 실행(`foundation-job-outcome changed`) 뒤에 곧 시작하고,
07:00 UTC 에도 시작한다(대체 시각). `spark` 3 슬롯이라 한 번에 하나씩 돈다. 실행은 마지막 줄에
`foundation-job-outcome changed|unchanged` 를 찍고, `changed` 면 Airflow 가 Gold 재생성을 시작한다.

모두 `enabled: false` 다. 레인마다 3 절의 첫 감독 실행을 마친 뒤 켠다. 그때까지 Airflow 는 이 유닛을 시작하지
않는다. 운영자가 다른 Spark 작업이 돌지 않을 때 시작한다. 예약 작업 DAG 를 멈추는 법은
[예약 작업 운영](./production-orchestrator-cutover.md)에 있다.

시간 한계는 150 분(유닛 `TimeoutStartSec`, 작업 `timeout_minutes` 160)이다. 이보다 길면 매시 접기가 굶주림 상한을
넘는다(ADR-0171 §5). 첫 감독 실행이 150 분 가까이 걸리면 켜지 말고 그 수치로 결정을 다시 연다.

## 3. 레인의 첫 감독 실행

레인마다 한 번, 이 순서로 한다. 권하는 첫 순서는 표제부 → 전유부 → 면적 → 공동주택가격 → 전유부 다리다.

**먼저 알 것: 첫 `changed` 는 Gold 재생성과 굽기를 부른다.** 지금 Silver 의 판은 손 적재가 다른
`source_snapshot_id` 로 넣었다. 그래서 첫 실행은 같은 월이어도 `changed` 다. 세 overwrite 레인은 표를 다시
쓰고, 다음 `gold_panel_rebuild` 가 그것을 보고 Gold 를 다시 만든다. 그다음 by-PNU 굽기가 돈다(R2 쓰기 비용).
`systemctl` 로 손수 시작한 실행은 Airflow 이벤트를 남기지 않으므로 Gold 는 대체 시각(08:45)에 돈다. 원치 않으면
확인이 끝날 때까지 `gold_panel_rebuild` DAG 를 멈춘다.

1. 릴리스가 상태 폴더와 승인을 만들었는지, 호스트에 환경 파일이 다 있는지 본다.

   ```bash
   ls -ld /data/foundation-platform/silver-refresh
   ls /etc/systemd/system/foundation-silver-refresh@.service.d/
   sudo python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py host
   ```

   폴더가 없으면 `foundation-release.sh timers` 를 다시 돌린다. 허브 내보내기는 대응표 투영이 있어야 돈다
   ([법정동 코드 변경 런북 2 절](./legal-dong-code-changes.md#2-before-a-hub-export)).

2. 계획만 본다. 받기·내보내기·쓰기는 없다.

   ```bash
   lane=building-register-titles
   sudo systemd-run --wait --pipe --collect -p User=foundation-platform -p Group=foundation-platform \
     $(python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py properties silver-refresh-plan) \
     /opt/foundation-platform/current/scripts/ops/silver-refresh.sh "${lane}" --plan
   ```

   마지막 줄은 `silver-refresh-plan lane=… outcome=would_change|unchanged release=<YYYYMM> …` 다. `release` 가 장부의
   기대한 월인지, 건너뛴 월(`skipped_incomplete_release`)이 있으면 그 까닭이 맞는지 본다.

3. 돌린다. oneshot 이라 `--no-block` 으로 시작하고 journal 을 본다.

   ```bash
   sudo systemctl start --no-block "foundation-silver-refresh@${lane}.service"
   journalctl -fu "foundation-silver-refresh@${lane}.service" -o cat
   ```

4. 확인한다.

   ```bash
   journalctl -u "foundation-silver-refresh@${lane}.service" --since -6h -o cat \
     | grep -a -E '^silver-refresh|silver-scalar-handoff-(pnu-null-share|iceberg)' | tail -8
   ls "/data/foundation-platform/silver-refresh/${lane}/evidence/"
   ```

   - 마지막 줄이 `outcome=changed` 이고 `rows` 가 0 이 아니다.
   - 세 overwrite 레인에서 `silver-scalar-handoff-pnu-null-share` 가 `within_bounds` 다(ADR-0142).
   - `evidence/<identity>/` 에 `export-summary.json`·`spark-summary-*.json` 이 있다.
   - 3 을 다시 돌리면 수 초 만에 `outcome=unchanged reason=already_loaded` 다. 이것이 "이미 반영" 판단이 실물에서
     도는 증거다.
   - 첫 실행의 시간과 Spark 최대 메모리를 적는다. `MemoryMax=4G`(내보내기)와 계약의 `driver_memory`(16g)는 아직
     실측이 아니다. 특히 면적은 1억 행을 한 묶음으로 넣는다.

5. 측정한 시간이 150 분 안이면 그 레인의 `enabled` 를 `true` 로, `disabled_reason` 을 지우는 PR 을 낸다
   (ADR-0122 §4). 마지막 줄 `foundation-job-outcome changed|unchanged` 가 `silver-refresh-outcome` 바로 뒤에
   찍혔는지도 본다.

공동주택가격 조합(`unit_official_price.py`, 시도마다 한 번)은 ④까지 손으로 돈다. 두 부분 레인이 같은 월을 가진
뒤에 돌린다. `--vintage` 를 빼면 두 표가 함께 가진 가장 새 월을 쓰고, 한쪽만 새 월이면 거부한다.

## 4. 거부와 고치는 법

| 문구 | 뜻 | 할 일 |
| --- | --- | --- |
| `the ledger holds no complete release` | 어느 월에도 모든 역할이 없다 | 매일 수집(`source_sweep`)이 그 원천을 받았는지 본다 |
| `… share the latest provider update date …; refusing to choose` | 한 월에 같은 역할의 객체가 둘이고 갱신일로 가를 수 없다 | 장부의 두 행을 비교한다. 장부는 쌓기만 하므로 고르는 결정은 ADR 로 남긴다 |
| `different provider export days` | 전유부와 표제부·기본개요가 다른 날 내보낸 파일이다 | 같은 날의 파일이 장부에 올 때까지 기다린다 |
| `names …, outside the ledger's month` / `is not a … Bronze object` | 장부 행과 파일 이름이 다른 월·원천을 말한다 | 수집 장부를 조사한다(사고) |
| `R2 bytes differ from the ledger's size or SHA-256` | Bronze 객체가 장부와 다르다 | R2 사고로 다룬다. 다시 돌리지 않는다 |
| `Refusing the export: …` (대응표) | 투영이 없거나 낡았다 | [법정동 코드 변경 런북 2 절](./legal-dong-code-changes.md#2-before-a-hub-export)의 표 |
| `pnu_null_share` 거부 | 대지 PNU NULL 비율이 상한·상승폭을 넘었다 | ADR-0142. 크로스워크나 원천을 먼저 본다 |
| `a lane contract names no release` | 계약에 `selected_vintage`·`objects` 가 다시 들어갔다 | 지운다. 판은 장부가 정한다 |
| `another lane holds …/refresh.lock` (75) | 다른 레인이 돈다 | 그 레인이 끝난 뒤 다시 시작한다 |
| `… have no ZIP member reading yet …` | 토지 레인 원천에 안 이름을 잰 적 없는 객체가 있다. 그 객체가 가장 새 판일 수 있다 | 문구가 이름 짓는 명령(`bronze-object-members.sh <원천>`)을 돌리고 다시 시작한다 (5 절) |
| `the ledger holds no vintage of … covering 17 regions` | 어느 판도 시도 17 을 다 덮지 않는다 | `skipped_incomplete_release=<판> regions=<n>` 줄을 본다. 시도 체계가 바뀌었으면(통합) 계약의 `completeness.count` 를 PR 로 고친다 |
| `refused_object=… members the lane's rule reads …` | 한 ZIP 에 레인이 읽을 CSV 가 둘이거나 날짜가 아니다 | 그 객체는 후보에서 빠진다. 그 때문에 판이 불완전하면 원천을 조사한다 |
| `load batch … holds the source_snapshot_ids …` | 버킷에 이미 있던 토지 핸드오프가 다른 `source_snapshot_id` 로 쓰였다 | 그 판의 핸드오프를 누가 썼는지 본다. gold.parcel_panel 은 표마다 id 하나만 읽는다 |

중간에 끊긴 실행: systemd 로 시작했으면 `ExecStopPost` 가 Spark 컨테이너를 치운다. 손으로 스크립트를 돌렸으면
처음에 찍힌 `INVOCATION_ID` 로 치운다.

```bash
INVOCATION_ID=<찍힌 값> /opt/foundation-platform/current/scripts/ops/silver-refresh.sh cleanup
```

## 5. VWorld 토지 레인 (③)

손 적재(`land-use-plan-handoff-export.sh`·`land-use-batch-load.sh`, 계약의 `objects[]`·`selected_vintage`)는
은퇴했다. 토지 일곱 표는 이 레인들로만 들어간다.

**판.** 레인 원천의 장부 행을 ZIP 안 이름과 함께 읽는다(정해진 `zip` 읽기만). 계약 `silver_refresh.member_name` 에
맞는 안 이름이 하나인 객체가 후보이고, 이름의 `region` 이 시도, `vintage` 가 판(`YYYYMMDD`)이다. 시도
`completeness.count`(17)를 모두 덮는 판 중 가장 새것이 판이다. 같은 시도·판이 둘이면 `provider_updated_at` 이 늦은
것이고, 같으면 거부한다. 용도지역 코드표는 전국 객체 하나다(장부 `snapshot_date` 가 가장 새것). 같은 접두사의 다른
데이터셋(도형 SHP, DBF, 변경분 `CH_*`, `.xlsx`)은 이름이 맞지 않아 후보가 아니다.

**이미 반영.** 토지 표의 적재 기록은 Bronze 객체 키(`source_record_id`)다. 판의 객체가 모두 기록에 있으면
`already_loaded`, 더 새 판의 객체가 하나라도 있으면 `newer_release_loaded` 다. 그래서 손 적재로 들어간 판도 같은 판이면
`unchanged` 다. 첫 감독 실행은 장부에 더 새 완전한 판이 있을 때만 `changed` 다.

**적재.** 객체마다 레인의 기존 내보내기가 R2 에서 R2 로 핸드오프 하나(`<handoff_prefix>/<객체 이름>.jsonl.gz`)를
쓴다. 이미 있으면 내보내기가 건너뛴다. 그다음 시도 순서로 4 개씩 Spark 적재를 돌린다. 첫 묶음이 `overwrite`(앞 판을
바꿔 쓴다), 나머지는 `append` 다. gold.parcel_panel 이 표마다 `source_snapshot_id` 하나(`vworldkr-<원천>:<판>`)만
읽기 때문이다. 중간에 끊기면 다음 실행이 기록에 없는 첫 묶음부터 잇는다. 그 사이 표는 판의 일부만 갖는다. Gold
재생성은 같은 `spark` 풀이라 레인과 겹치지 않고, 잃는 행이 많은 Gold 는 거부한다.

### 첫 감독 실행 (레인마다)

권하는 순서는 코드표 → 공시지가 → 토지이용계획 → 토지특성 → 임야대장 → 대지권등록 → 토지이동이력이다(작은 것부터).

1. 그 원천의 ZIP 안 이름을 잰다(①). 이미 잰 객체는 건너뛰므로 다시 돌려도 된다.

   ```bash
   source=vworldkr__land_individual_price
   sudo systemd-run --wait --collect --pipe -p User=foundation-platform \
     $(python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py properties bronze-object-members) \
     /opt/foundation-platform/current/scripts/ops/bronze-object-members.sh "${source}"
   ```

2. 3 절의 2–4 를 `lane=land-individual-price` 로 한다. 계획에서 볼 것:
   - `release=<YYYYMMDD>` 가 기대한 판이고, `region=… object=…` 줄이 시도 17 개다.
   - `skipped_incomplete_release` 와 `refused_object` 줄이 있으면 까닭이 맞는지 본다.
   - 손 적재된 판과 같으면 `outcome=unchanged reason=already_loaded` 다.
3. `changed` 였으면: `evidence/land-<…>-<판>/` 에 `export-summary-<시도>.json` 17 개와 `spark-summary-*.json` 이
   있고, 다시 돌리면 `already_loaded` 다. 토지이용계획·토지이동이력은 1~2억 행이라 150 분 안에 끝나는지 본다.
   넘으면 다음 실행이 이어 적재하지만, 켜기 전에 그 수치로 결정을 다시 연다(2 절).
4. 매일 수집(`source_sweep`)은 레인들 뒤에 1 의 측정을 인자 없이 돈다(ADR-0169 §1 개정 기록, 2026-10-10). 한 실행이
   2000 개까지라 과거 객체는 여러 날에 걸쳐 채워진다: `journalctl -u foundation-source-sweep.service` 의 줄 끝
   `| members measured= failed= selected=` 에서 `selected` 가 2000 이면 아직 남았을 수 있다. 첫 감독 실행 전에는
   1 의 명령을 그 원천에 돌려 두면 기다리지 않는다. 측정이 실패하면(`failed>0` 또는 `members status=no-summary`)
   sweep 이 빨갛고 결과 줄을 내지 않는다 — 못 잰 객체 이름은 같은 저널의 실행 로그 끝(`  | … cannot measure`)에 있고
   다음 실행이 다시 잰다. 그 사이 새 객체마다 레인이 "잰 적 없는 객체"로 거부하는 것은 의도한 거부다(판을 추측하지
   않는다).
