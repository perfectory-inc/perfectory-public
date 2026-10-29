# ADR 0333: Gold 표와 서빙 레인은 파티션된 자산이고, 상류 파티션의 변경이 그 파티션만 만든다

- Status: Proposed
- Date: 2026-10-29
- Builds on: [ADR-0118](./0118-scheduled-data-work-runs-in-airflow-and-reports-lineage.md)(Airflow, Dagster 재검토 조건), [ADR-0122](./0122-airflow-starts-each-jobs-systemd-unit-and-waits-systemd-runs-it.md)·[ADR-0270](./0270-server-jobs-come-from-one-spec-and-run-through-one-runner.md)(한 spec·한 실행기), [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md)(풀·메모리 조합), [ADR-0171](./0171-scheduled-jobs-are-chained-by-the-data-their-runs-changed.md)(작업 단위 Airflow 자산), [ADR-0303](./0303-each-by-pnu-lane-is-its-own-schedule-pipeline.md)(레인별 굽기), [ADR-0305](./0305-a-check-that-found-no-work-is-that-jobs-turn.md)(`takes_turns`), [ADR-0312](./0312-gold-commits-record-their-changed-pnus-and-every-rebuild-is-partial-or-says-why.md)(바뀐 PNU 기록), [ADR-0314](./0314-partial-processing-is-the-default-and-full-is-a-contract-exception.md)(부분이 기본), [ADR-0308](./0308-the-deploy-stops-only-the-jobs-its-release-holds-and-skips-a-step-whose-inputs-did-not-change.md)(배포는 잡는 작업만)
- 관련 사건: 2026-10-23~28 — `all` 굽기가 레인 10개를 차례로 돌아 20시간짜리 필지 레인이 공매를 며칠 막았다(ADR-0303 이 공매만 떼어 냄). 2026-10-28 패널 재생성 하나가 parcel → building → unit_price 를 160분 한 상한 안에 묶어 두 번 시간 초과, unit_price 가 잘렸다(#923, ADR-0326). 같은 날 by-PNU 굽기가 14시간 37분 돌다 신호로 끝났다. 배포 0단계는 작업을 최대 5시간 기다렸다(#908). `takes_turns` 가 할 일 없는 레인 뒤에 굽기를 영원히 미뤘다(ADR-0305). 2026-10-28 21:38 UTC `gold_public_auction_rebuild` 가 끝나고 자산 이벤트 3912 를 냈으나 `by_pnu_public_auction_bake` 실행은 한 번도 큐에 들어가지 않았다 — 데이터는 다음 날 10:45 대체 시각까지 약 13시간 기다린다(별도 최소 수정 진행 중).

## Context

측정(2026-10-29, main 과 ai-server 읽기 전용):

| 항목 | 값 |
|---|---|
| 작업 | `orchestration/jobs.v1.json` 39개(사용 34), 전부 같은 실행기: Airflow DAG → SSH → systemd 유닛(ADR-0122) |
| 시작 방식 | `schedule` 20, `inputs` 19(ADR-0171: `AssetOrTimeSchedule` + `AssetAny`, Airflow 3.3.2) |
| 범위(ADR-0314) | `full` 23, `skip-if-unchanged` 9, `new-objects` 4, `changed-keys` 3 |
| 풀 | `spark` 3슬롯, 작업 30개가 사용(3슬롯 25·2슬롯 2·1슬롯 3), 호스트 최악 조합 60.95g / 62g |
| 상한 | 150분×13, 160분×3, 굽기 1200분 |
| 파이프라인 그래프 | 노드 142(serving_surface 41, serving_group 36, silver 31, gold 19, source_group 13, reference 2), 간선 138. **파티션 개념 없음** |
| 자산의 단위 | 작업 출력 = 그래프 노드 = Airflow `Asset` 하나. 한 실행이 노드 전체를 다시 확인한다 |
| 10-28 패널 재생성 | 18:05→20:45 UTC 160분 시간 초과(그 전 실행도 18:03 시간 초과) |
| 10-28 by-PNU 굽기 | 00:42→15:19 UTC, `Result=signal`, 메모리 최고 6.9G |
| 10-29 04:31 실패 상태 유닛 | 14 |

이미 가진 것: 자산 이름(그래프 노드 = DataHub 데이터셋), 자산 이벤트로 하류 시작(ADR-0171), 바뀐 PNU 기록(ADR-0312),
부분 처리 선언(ADR-0314), 레인별 굽기 첫 증분(ADR-0303). **없는 것은 파티션이다.** 그래서 "무엇이 바뀌었나"는 PNU
수준까지 아는데, "무엇을 돌리나"는 노드 전체(전국·전 레인) 단위로 정해진다. 실행 단위가 크니 상한이 길어지고, 긴
실행이 풀·배포·다른 레인을 막는다.

대기업·오픈소스 사례:
- **Dagster software-defined assets + partitions + declarative automation**: 자산 × 파티션(정적·시간·다차원)이 실행
  단위이고, 상류 파티션이 갱신되면 `AutomationCondition.eager()` 가 그 파티션만 다시 만든다. 자산 검사(asset check)가
  파티션 단위로 막는다.
- **Netflix Maestro**: 신호(signal) 기반 트리거 — 테이블 파티션이 준비됐다는 신호가 그 파티션의 워크플로만 시작한다.
- **Airflow AIP-76(3.2+)**: 파티션된 자산 이벤트(`partition_key`), 시간·범위 파티션 매퍼, `PartitionedAssetTimetable`.
  우리는 3.3.2 를 이미 돌린다.

## Decision

1. **자산 = Gold 표 × 파티션, 서빙 레인 × 파티션.** 실행·상한·재시도·신선도·배포 보류의 단위는 자산 파티션이다.
   - Gold 패널(필지·건물): 파티션 차원 `sido`(17 + 합성 대역). Silver 변경의 PNU 앞 2자리가 파티션 키다(ADR-0312
     의 바뀐 PNU 가 그대로 키를 준다).
   - 종류 Gold(단가·공매·용도지역 …): 자산 하나씩(파티션 없음 또는 `sido`, 크기로 정함).
   - by-PNU 서빙: 레인마다 자산(`serving.<lane>`), 필지·건물 레인은 `sido` 파티션(문서 패치가 이미 PNU 범위 단위다).
   - 타일: ADR-0316 이 이미 시도별로 굽는다 — 그 단위를 그대로 파티션으로 등록한다.
2. **정본은 그래프와 작업 계약 둘뿐이다(새 파일 없음).** `pipeline-graph.v1.json` 의 노드에 `partitions`
   (`{dimension, keys_from}`)를, `jobs.v1.json` 의 작업에 `materializes`(노드 id 목록)를 더한다. 작업 하나는 **자산
   하나**를 만든다(가드). 파티션 하나의 실행은 같은 작업을 `--partition <key>` 로 부른다. 스케줄러 설정은 이 둘에서
   생성되며 손으로 쓰지 않는다(ADR-0171 의 생성 경로 확장).
3. **엔진: 지금은 Airflow 3.3 파티션 자산으로 진화(evolve), 정해진 시험에 지면 Dagster 로(adopt).**

   | 기준 | Airflow 3.3 진화 | Dagster OSS 채택 |
   |---|---|---|
   | 자산·파티션 모델 | AIP-76 파티션 이벤트·매퍼, 단일 차원 | 다차원 파티션, 파티션별 백필 UI, 자산 검사 1급 — **모델은 더 풍부하다** |
   | 이벤트 기반 | `PartitionedAssetTimetable` | `AutomationCondition` |
   | 계보(DataHub/OpenLineage) | 이미 연결(ADR-0118·0171) | `dagster-openlineage` 별도 |
   | 실행기 | 지금 그대로(SSH → systemd, ADR-0122) | `dagster-pipes` 로 같은 systemd 를 부를 수 있음 |
   | 호스트 메모리 | 추가 0(현 Airflow 묶음 약 1.1 GiB) | 웹서버·데몬·코드 위치·DB 약 1.5~2.5 GiB 추가 — 가용 36 GiB 대비 결정 요인 아님 |
   | 스케줄러 수 | 하나 | 이행 동안 둘(airlift) |

   전환 비용은 판단에 넣지 않았다(소유자 지시). 갈림은 모델의 풍부함(Dagster) 대 **스케줄러·계보 정본이 하나로
   남는 것**(Airflow)이다. 이 결정은 계약(그래프 + 작업)을 엔진 중립으로 두고 Airflow 를 **생성 대상**으로 쓴다.
   그러면 엔진 교체는 생성기 교체가 된다. ADR-0118 의 재검토 조건 중 "배포해야만 드러나는 오케스트레이션 결함의
   반복"은 10-23~29 로 충족됐다 — 이 ADR 이 그 재검토다.

   **Dagster 로 넘어가는 시험(첫 두 조각에서 판정):** (a) 파티션 하나를 다른 파티션과 독립한 상한·재시도로 돌릴 수
   있는가, (b) 상류 파티션 이벤트가 하류의 같은 키 파티션만 시작하는가, (c) 파티션 하나만 백필할 수 있는가,
   (d) 파티션 단위 검사가 하류를 막을 수 있는가, (e) §7 의 조정 루프를 생성된 Airflow 작업으로 5분 주기에 메타데이터 DB
   부하 없이 돌릴 수 있는가(Dagster 는 이것을 내장한다). 하나라도 Airflow 3.3 에서 안 되면 `dagster-airlift` 로 레인 하나씩
   옮기고 같은 계약에서 Dagster 정의를 생성한다.
4. **`all` 같은 묶음 작업을 없앤다.** 서로 의존하지 않는 두 자산을 한 실행에 묶지 않는다. `by-pnu-serving-bake.sh all`
   과 `gold_panel_rebuild` 의 parcel → building → unit_price 사슬은 자산별 작업으로 나뉜다. 순서는 묶음이 아니라 자산
   간선이 정한다(building 이 parcel 을 읽으면 그 간선이 사슬을 만든다).
5. **배포는 무관한 자산을 비우지 않는다.** ADR-0308 의 "릴리스가 잡는 작업"을 자산으로 좁힌다: 릴리스 차이가 바꾼
   코드(ADR-0321 의 Gold 커밋 코드 기록과 같은 경로 목록)를 만드는 자산의 실행만 보류하고, 그 밖의 파티션 실행은
   계속된다. 배치 호스트가 따로 생기면(ADR-0335) 서빙 배포는 배치를 아예 건드리지 않는다.
6. **풀과 차례.** 풀 슬롯·메모리 조합(ADR-0138)은 자산 파티션 실행에 그대로 적용된다. 파티션 실행은 짧아지므로
   `takes_turns`(차례 미루기)는 필요가 없어지고 은퇴한다: 굶주림 상한은 파티션 하나의 최장 실행으로 계산된다.
7. **트리거는 사건이 아니라 상태로 정한다(놓친 트리거가 구조적으로 없다).** 10-28 이벤트 3912 사고는 "이벤트가
   전달돼야 하류가 돈다"(edge-triggered)는 구조의 결함이다. 대신 Kubernetes 컨트롤러·Dagster declarative automation
   과 같은 **조정 루프(level-triggered)** 를 정본으로 한다:
   - 자산 파티션의 실행은 자기가 **읽은 상류 판**(Gold 는 Iceberg 스냅숏 id, 서빙은 manifest 세대)을 결과 줄과 커밋
     기록에 남긴다(ADR-0321 의 커밋 기록 확장).
   - 조정기(그래프에서 생성되는 짧은 주기 작업, 계약 `reconcile_minutes`, 기본 5분)가 파티션마다 "상류의 최신 판 ≠
     내가 읽은 판"이고 실행 중·대기 중인 실행이 없으면 실행을 넣는다. 자산 이벤트는 지연을 줄이는 빠른 경로일 뿐이고,
     이벤트가 사라져도 다음 조정 주기에 반드시 시작된다. 시각 대체(ADR-0171 §2)는 조정기로 대체된다.
   - **지연은 지표다.** 파티션마다 `asset_lag_seconds`(상류 최신 판 커밋 시각 − 내가 읽은 판의 커밋 시각)와
     `asset_unscheduled_seconds`(지연이 있는데 실행이 없는 시간)를 Prometheus 로 낸다. 후자가 `reconcile_minutes` 의
     3배를 넘으면 경보한다 — 조정기 자체가 멈춘 경우까지 잡는다. 신선도(아래 8)는 사용자 쪽, 이 둘은 스케줄러 쪽 증거다.
8. **신선도와 검사는 자산 파티션마다.** ADR-0306 의 레인 신선도 예산은 자산의 `freshness` 로 일반화되고, 자산 검사
   (ADR-0334 의 Audit)가 실패한 파티션은 하류를 시작하지 않는다.

## 현재 → 새 모델 대응

| 지금 | 새 모델 |
|---|---|
| `jobs.v1.json` 작업(39) | 작업 = 자산 하나의 만드는 법(`materializes`), 파티션 인자 |
| 그래프 노드(142) | 자산. Gold·serving 노드에 `partitions` |
| ADR-0171 `AssetAny` 시작 + 시각 대체 | 조정 루프(상태)가 정본, 파티션 이벤트(`partition_key`)는 빠른 경로 |
| `foundation-job-outcome changed|unchanged` | 파티션별 결과 줄(`... partition=<key>`) |
| systemd 타이머(호스트 점검: 모니터·탐침) | 데이터 작업이 아니므로 그대로(ADR-0118 §1) |
| `takes_turns`, `all` 유닛 | 은퇴 |

## 이행(strangler, 한 자산씩)

| 단계 | 내용 | 끝났다는 증거 |
|---|---|---|
| 1 (**첫 조각**) | `gold.unit_price` 와 `serving.unit-price` 를 각자 자산 작업으로: `gold_panel_rebuild` 와 `all` 굽기에서 단가를 빼고, 단가 Gold 커밋 이벤트가 단가 레인 굽기를 시작. 두 자산에 "읽은 상류 판" 기록과 `asset_lag_seconds`·`asset_unscheduled_seconds` 지표, 조정기(이 두 자산과 공매 Gold → 공매 굽기 간선만) | 패널 재생성 시간 초과가 단가를 자르지 못함(구조). 이벤트를 일부러 버린 시험에서 조정기가 5분 안에 하류를 시작. 10-28 이벤트 3912 사고를 재현하면 15분 안에 경보 |
| 2 | 계약에 `partitions`·`materializes`·`reconcile_minutes` 추가와 생성기. 조정기를 모든 `inputs` 작업으로, 시각 대체 은퇴. 공매(이미 자기 작업, ADR-0303)를 첫 **파티션 없는** 생성 자산으로 옮겨 생성 경로 검증 | 손으로 쓴 DAG 0, 기존 시험 통과 |
| 3 | `gold.parcel_panel` 을 `sido` 파티션으로(Airflow AIP-76). 위 시험 (a)~(d) 판정 → evolve/adopt 결정 기록 | 한 시도 변경이 그 시도만 재생성 |
| 4 | `serving.parcel`·`serving.building` 을 `sido` 파티션으로, `all` 은퇴 | 20시간 굽기 → 시도 하나의 최장 실행 |
| 5 | 종류 레인 6개, 타일 파티션 등록, `takes_turns` 은퇴, 배포 보류를 자산 단위로 | `jobs.v1.json` 에 묶음 작업 0(가드) |

첫 조각 추정: 약 5일(작업 분리·유닛 1.5, 자산 간선·DAG 생성 1, 읽은 판 기록·조정기·지표 1, 메모리 예산·굶주림 상수
갱신 0.5, 운영 확인 1).
단가를 고른 이유: #923 의 피해 자산이고, Gold 와 서빙 두 끝을 한 번에 뚫으며(AGENTS.md "한 경로를 끝까지"), 작아서
운영 굽기에 영향이 없다.

## 가드(ADR-0001)

- **작업 하나 = 자산 하나**(`every-job-materializes-one-asset`, 옮겨진 작업부터, 줄기만 하는 기준선): 막는 사고 —
  10-28 패널 재생성이 세 자산을 한 160분 상한에 묶어 unit_price 가 잘린 일.
- **파티션 있는 노드의 작업은 `--partition` 을 받는다**: 막는 사고 — 20시간 필지 레인이 공매를 며칠 막은 일(전국 단위
  실행).
- **모든 `inputs` 간선은 조정기가 덮는다**(생성 시험: 그래프의 상류 → 하류 간선마다 조정 규칙 하나) +
  `asset_unscheduled_seconds` 경보: 막는 사고 — 10-28 21:38 공매 Gold 이벤트 3912 가 하류 굽기를 큐에 넣지 못해
  13시간 지연될 뻔했는데 아무 지표도 그것을 말하지 않은 일.
- **배포 보류 집합은 바뀐 코드의 자산뿐**(ADR-0308 시험 확장): 막는 사고 — 배포 0단계가 무관한 작업을 5시간 기다린 일
  (#908), 10-28 배포가 굽기를 신호로 끝낸 일.

## 기각한 대안

- **Dagster 즉시 전면 채택**: 모델은 더 낫지만 계약을 엔진 중립으로 만드는 일이 어느 쪽이든 먼저이고, 그 위에서 시험
  (a)~(d)로 판정하는 것이 근거 있는 선택이다. 판정 전 교체는 측정 없는 결정이다.
- **Netflix Maestro 채택**: JVM 서비스 + 자체 DB, 우리 규모(호스트 하나·작업 39)에 맞지 않고 계보 연동이 없다
  (ADR-0270 에서도 기각).
- **상한만 늘리기(160 → 240분)**: 굶주림 상한(ADR-0138/0171)이 같이 늘어 모든 작업의 대기가 길어진다. 실행 단위를
  줄이는 것이 근본이다(ADR-0326 도 상한을 올리지 않았다).
- **`all` 을 병렬 실행으로**: 메모리 예산(60.95g/62g)을 넘는다. 파티션은 실행을 작게 하고, 병렬은 그 위에서 풀이 정한다.

## Consequences

- 실행 수는 늘고(시도 17 × 자산) 실행 하나는 짧아진다. Airflow 메타데이터 DB 행이 늘어난다(하루 수백 행 수준, 계산).
- 파티션 키를 결과 줄·Gold 커밋 기록·manifest 패치가 같은 이름으로 써야 한다 — 이름은 그래프의 `partitions` 하나에서
  온다.
- 전량 처리(ADR-0314 `full`)는 "모든 파티션 실행"으로 표현되며, 선언·비용·추적 규칙은 그대로다.
