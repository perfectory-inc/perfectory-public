# ADR 0306: by-PNU 레인은 서빙하는 manifest 가 신선도 예산보다 오래되면 경보한다

- Status: Accepted
- Date: 2026-10-28
- Builds on: [ADR-0299](./0299-engineering-hygiene-is-a-set-of-machine-enforced-invariants.md)·[ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md)(위생 B SLO·C 무음실패), [ADR-0151](./0151-building-packs-serve-each-document-whole.md)·[ADR-0160](./0160-one-by-pnu-gateway-source-serves-both-lanes.md)(매시 서빙 모니터), [ADR-0301](./0301-deploy-stops-an-interruptible-job-rather-than-waiting-hours-and-a-long-pause-alerts.md)(배포가 굽기를 멈춤)
- 관련 사건: 2026-10-28 — by-PNU 서빙이 10~12일 얼어 있었다(마지막 실제 발행: building 10-16, parcel 10-19, 종류 레인 10-17/18, 공매 10-24). 그동안 작업은 `changed`/`unchanged`/`waiting` 을 말했고 매시 모니터 10개는 모두 통과했다.

## Context

레인마다 매시 도는 `foundation-by-pnu-serving-monitor@<lane>` 은 표본 PNU 를 운영 주소에서 읽어 **서빙 manifest 가
가리키는 문서와 같은지**만 본다. 얼어 있던 12일 동안 주소는 낡은 manifest 에 충실했으므로 모니터는 매시 초록이었다.
작업 상태는 작업이 한 일을 말할 뿐 사용자가 받는 것을 말하지 않는다. 있는 신선도 경보
(`FoundationPlatformLakehouseBatchStale`, `FoundationPlatformIngestionRunStale`)는 수집·적재 기록을 보고, 서빙 산출물은
아무도 보지 않았다.

Google SRE Workbook(데이터 파이프라인 장)은 파이프라인의 1차 SLI 를 **산출물의 신선도**로 둔다 — 작업 성공률이
아니라 소비자가 받는 데이터의 나이. 서빙 manifest 의 `published_at_utc` 가 바로 그것이다: 모든 발행(패치·전량·
반영만)이 그 값을 새로 쓰고, 그 외에는 아무도 manifest 를 바꾸지 않는다.

## Decision

1. **산출물 나이를 잰다.** 매시 서빙 모니터가 이미 읽는 서빙 manifest 의 `published_at_utc` 와 지금의 차이를
   보고서(`served_published_at_utc`, `served_age_hours`)에 적고, 예산을 넘으면 `stale` 로 실행을 실패시킨다.
   실패는 유닛의 기존 `OnFailure=foundation-unit-failed@` 로 Slack 에 간다. 새 유닛·새 타이머·새 경로는 없다 —
   10개 레인 모두 이미 그 모니터를 매시 돌린다.
2. **예산은 계약 한 곳**: `config/r2-connections.contract.json` 의 `by_pnu_section_packs.freshness_budget_hours`
   (모든 레인 공통, 지금 48). 모든 레인이 같은 일일 `by_pnu_serving_bake` 로 갱신되므로 레인별 값은 두지 않는다.
   48 의 근거는 계약의 `freshness_budget_hours_reason`(일일 두 차례 — 한 차례를 통째로 놓치면 이튿날 울린다; 36 은
   13시간 넘는 전량 굽기를 위반으로 센다).
3. **`stale` 은 `passed` 와 따로 둔다.** Worker 배포(`worker_autodeploy.py`)는 `passed` 를 "이 버전이 레인이 서빙하는
   것을 낸다" 로 읽는다. 데이터가 묵은 것은 Worker 버전 탓이 아니므로 배포를 막지 않는다. 세대·기록 상태를 읽는
   리허설(`MONITOR_PACK_GENERATION`·`MONITOR_HISTORY_STATE`)은 나이를 판정하지 않는다.
4. **런북**: 유닛의 `Documentation=` 이 런북([parcel-by-pnu-serving-bake.md §9](../../platforms/foundation-platform/docs/runbooks/parcel-by-pnu-serving-bake.md))을 가리키고,
   `notify-unit-failure.sh` 가 유닛이 `Documentation=` 을 가지면 Slack 알림에 붙인다.

가드가 막는 실제 사고(루트 ADR-0001 가드 정책): **2026-10-16~28 모든 by-PNU 레인이 10~12일 묵은 문서를 서빙하는데
작업과 모니터가 모두 성공을 말해 아무도 몰랐다.** 자체 시험(`monitor.rs` tests)이 그 모양 — 서빙 검사 통과 +
10-16 발행 manifest — 을 실패로 판정한다.

## Consequences

- 서빙이 얼면 이틀 안에 Slack 에 레인 이름과 나이가 온다. 작업 상태가 무엇이든 같다.
- 병합·배포 시점에 레인이 아직 묵어 있으면 레인마다 매시 알림이 온다(사실이므로 끄지 않는다). 굽기가 발행하면
  다음 시각에 멈춘다.
- 위생 불변식(ADR-0299 §5)의 첫 줄이 된다: AGENTS.md `## 위생 불변식`.
- 모니터 자체가 멈추면(타이머 꺼짐) 이 경보도 없다 — 데드맨은 이 결정 밖이다.

## Alternatives rejected

- **Prometheus 규칙 + 새 메트릭** — 메트릭은 `foundation-api` 하나만 스크랩되고, R2 manifest 를 읽는 주체는 publisher
  다. API 에 R2 읽기를 더하는 것은 새 메커니즘이다. 이미 manifest 를 매시 읽고 Slack 경로를 가진 모니터를 넓힌다.
- **실행 요약(`run-summary.json`)·작업 결과 줄로 판정** — 그것이 바로 이번에 초록이던 신호다. 사용자가 받는 것은
  manifest 다.
- **Gold 스냅숏 대비 지연(manifest 반영 스냅숏 ≠ Gold 현재)** — Gold 재생성이 멈춰도 둘이 같아 초록이 된다. 시계
  나이는 원인과 무관하게 잡는다.
