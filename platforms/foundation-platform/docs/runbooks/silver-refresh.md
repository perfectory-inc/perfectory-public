---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-09
---

# Silver 레인 새로 고침 — 건축HUB 레인 런북

[루트 ADR-0169](../../../../docs/adr/0169-silver-lanes-pick-their-source-release-from-the-ledger-and-refresh-themselves.md)
②의 운영 절차다. 레인 하나가 Bronze 장부(`catalog.bronze_object`)에서 가장 새 완전한 원천 판을 고른다. Silver 표가
그 판을 이미 가졌으면 수 초 만에 `unchanged` 로 끝나고, 아니면 내보내고 적재한다. 판을 계약에 적어 올리는 PR 은 없다.

| 무엇 | 정본 |
| --- | --- |
| 레인 목록과 레인이 하는 일 | 발행기 `run-silver-refresh` (`remote_lakehouse_job/silver_refresh/`) |
| 레인의 원천 역할·내보내기·Spark 크기·쓰기 방식 | 레인 계약의 `silver_refresh` 블록 (`infra/lakehouse/contracts/hub-building-register-*-source-objects.json`) |
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
   ```

레인은 한 번에 하나만 돈다. 각 레인이 compose `spark` 의 상한(20g) 전부를 쓰기 때문이다. 두 번째 레인은
`refused: another lane holds …/refresh.lock` 로 75 를 내고 끝난다.

## 2. 아직 예약 작업이 아니다

레인은 `orchestration/jobs.v1.json` 에 없다. `spark` 풀의 굶주림 상한이 이미 꽉 찼기 때문이다(ADR-0169 §5 ②).
그래서 Airflow 는 이 유닛을 시작하지 않는다. 운영자가 다른 Spark 작업이 돌지 않을 때 시작한다. 예약 작업 DAG 를
멈추는 법은 [예약 작업 운영](./production-orchestrator-cutover.md)에 있다.

## 3. 레인의 첫 감독 실행

레인마다 한 번, 이 순서로 한다. 권하는 첫 순서는 표제부 → 전유부 → 면적 → 공동주택가격 → 전유부 다리다.

**먼저 알 것: 첫 `changed` 는 Gold 재생성과 굽기를 부른다.** 지금 Silver 의 판은 손 적재가 다른
`source_snapshot_id` 로 넣었다. 그래서 첫 실행은 같은 월이어도 `changed` 다. 세 overwrite 레인은 표를 다시
쓰고, 다음 `gold_panel_rebuild`(08:45)가 그것을 보고 Gold 를 다시 만든다. 그다음 by-PNU 굽기가 돈다(R2 쓰기 비용).
원치 않으면 확인이 끝날 때까지 `gold_panel_rebuild` DAG 를 멈춘다.

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

5. 그 레인을 예약에 올리는 일은 2 절의 결정 뒤에 별도 PR 로 한다(ADR-0122 §4).

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

중간에 끊긴 실행: systemd 로 시작했으면 `ExecStopPost` 가 Spark 컨테이너를 치운다. 손으로 스크립트를 돌렸으면
처음에 찍힌 `INVOCATION_ID` 로 치운다.

```bash
INVOCATION_ID=<찍힌 값> /opt/foundation-platform/current/scripts/ops/silver-refresh.sh cleanup
```
