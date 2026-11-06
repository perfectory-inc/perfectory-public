# ADR 0387: R2 의 미디어와 그것을 이름 대는 Silver 색인을 매일 대조한다 — 없는 객체와 고아를 경보한다

- Status: Accepted
- Date: 2026-11-04
- Implements: [ADR-0299](./0299-engineering-hygiene-is-a-set-of-machine-enforced-invariants.md) G-recon,
  [ADR-0300](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md) §2·Open("R2↔색인 대조의 정확한 스키마·cadence")

## Context

ADR-0300 §2 는 정합(G)의 최우선 구현으로 **R2 내용주소 객체 ↔ silver 색인 대조**를 골랐다. ADR-0299 G-recon 의
현황은 "Iceberg 만 orphan GC; 객체 저장소 대사 없음" 이었고, main 에서 다시 찾아도 그대로였다.

- `lakehouse_maintenance` 의 `orphan_cleanup` 은 Iceberg 표 위치 안의 파일만 본다. `onbid/media` 는 어느 표 위치에도 없다.
- `audit-r2-inventory` 는 Bronze 키를 Postgres 카탈로그와 맞추는 손 도구다. 예약 작업이 아니고 미디어를 모른다.
- `asset_reconcile` 은 자산의 입력 판과 읽은 판을 맞춘다. 객체 존재는 보지 않는다.

여기서 "색인"은 `silver.onbid_real_estate_item_photo` 다. 수집기는 사진·첨부를 한 번만
`onbid/media/sha256/<해시>.<확장자>` 에 쓰고(ADR-0290 §7, `foundation-shared-kernel` media.rs), Bronze 줄에
해시를 남긴다. Silver 적재가 그 해시를 색인에 넣는다. 필지 화면은 색인의 `(sha256, media_format)` 으로 사진을
부른다(`onbidMediaSrc`). 둘이 어긋나는 길은 두 가지다.

| 어긋남 | 사용자에게 | 실제로 있었던 일 |
|---|---|---|
| 없음: 색인이 이름 대는 키가 R2 에 없다 | 깨진 사진 | 10-26 R2 의 "객체 없음"을 오류로 읽어 run 의 사진이 하나도 저장되지 않았다(errored 20·downloaded 0, #867). 그때는 줄도 없었지만, 저장과 색인이 따로 움직인다는 것을 아무것도 재지 않았다 |
| 고아: R2 에 있는데 어느 색인 줄도 이름 대지 않는다 | 아무도 서빙하지 않는 저장 비용 | 10-26 수집이 40분 한도에 걸렸다. Bronze 커밋이 run 끝에 한 번이라 run 이 통째로 커밋되지 않았고, 그 run 이 이미 올린 사진은 이름 대는 줄 없이 남았다 |

실측(2026-11-04, 읽기 전용):

| 항목 | 값 | 근거 |
|---|---|---|
| 색인 행 / 서로 다른 키 | 94,672 / 29,738 (jpg 23,217·pdf 4,755·png 1,753·gif 13) | Trino `SELECT DISTINCT` 4초 |
| 색인 마지막 적재 | 2026-10-31 18:50 UTC | `max(ingested_at_utc)` |
| 표본 300 키의 실물 | 300 개 모두 200 | 필지 엣지 `GET Range: bytes=0-0` |
| 바뀐 것만 읽을 때 색인 키(실측) | 새 적재가 없는 날 6,074 (마지막 적재 2시간치), 첫 run 29,738 | 같은 SQL, 워터마크 10-31 18:50 |
| 한 run 의 목록 비용(계산) | ⌈객체 수/1,000⌉ ≈ 30 Class A ($4.50/백만) = $0.000135/run, $0.004/월 | R2 요금표 |

R2 목록 전체는 이 셸 계정으로 읽지 못했다(키 파일은 root 전용). 고아 수는 첫 run 이 잰다.

## Decision

1. **대조는 작업 하나다.** `media_reconcile`(`orchestration/jobs.v1.json`, 매일 06:15 UTC, `default_pool`,
   10분, 배포 뒤 한 번)이 `scripts/ops/media-reconcile.sh` 를 돌린다. 무엇을 맞추는지는 계약
   `data-go-kr-onbid.contract.json` 의 `media.reconcile` 한 곳에 둔다(`index_table`, `orphan_min_age_hours`).
   접두어는 이미 있는 `media.object_prefix` 를 쓴다. 경매 미디어가 생기면 그 계약에 같은 블록을 둔다.
2. **순서는 색인 먼저, 목록 나중이다.** 수집기는 블롭을 그것을 이름 대는 줄보다 먼저 쓴다. 그래서 색인을 읽은 뒤
   찍은 목록에 없는 키는 확실히 없는 것이고, 여유를 두지 않는다. 목록은 새 publisher 명령 `list-lakehouse-objects` 가
   lakehouse **읽기 전용** 키로 찍는다(Silver 적재 계획이 Bronze 를 찍는 그 키·그 `inventory_audit`).
3. **고아는 여유가 지난 것만 센다.** `orphan_min_age_hours`(48)보다 젊은, 이름 없는 객체는 아직 Silver 로 가는 줄이
   있을 수 있으므로 `young_unreferenced_count` 로만 적는다. 형식이 다른 같은 해시(`<h>.png` 와 `<h>.jpg`)는 서로
   다른 키다 — 화면은 색인의 형식으로 부른다.
4. **색인은 바뀐 것만 읽는다(ADR-0314, `scope: new-objects`).** 색인은 덧붙이기만 하므로 그것이 이름 대는 키 집합은
   늘기만 한다. 대조기는 그 집합과 읽은 가장 새 `ingested_at_utc` 를 상태(`referenced.json`)에 두고, 다음 run 은 그
   시각에서 2시간을 뺀 뒤로 적재된 줄만 읽는다(적재는 시작 때 시각을 찍고 40분 안에 커밋한다; 두 번 읽힌 키는 한 키).
   첫 run 이나 상태가 없어진 run 만 색인 전체를 한 번 읽는다. 상태는 비교가 성공한 뒤에만 쓴다. 목록은 줄일 수 없다 —
   키가 해시라 ListObjectsV2 는 시간으로 좁혀지지 않는다. 그래서 목록은 키·크기만(1,000 개에 호출 1번) 찍고 블롭은
   읽지 않는다. Iceberg `remove_orphan_files` 가 표 위치를 목록으로 찍는 것과 같고, `lakehouse_maintenance` 가
   메타데이터만 읽고 `new-objects` 인 것과 같은 선언이다.
5. **결과는 남기고 경보한다.** run 마다 보고서 하나를 `/var/lib/foundation-platform/media-reconcile/reports/<UTC>.json`
   에 덧붙인다(덮어쓰지 않는다). `foundation_media_reconcile_*` 지표를 node-exporter textfile 로 쓰고, 기존
   Alertmanager → Slack 이 받는다: `FoundationMediaObjectMissing`(없음 > 0), `FoundationMediaObjectOrphaned`(고아 > 0),
   `FoundationMediaReconcileStale`(30시간 비교 없음). 작업은 아무것도 지우거나 고치지 않는다. 결과는 항상 `unchanged` 다.
6. **가드.** `orchestration/tests/test_media_reconcile.py` 가 가짜 목록에 고아 하나와 없는 객체 하나를 심고 둘 다
   잡는지 확인한다. 여유 안의 객체는 세지 않는지, 경보가 읽는 지표를 대조기가 쓰는지도 확인한다.
   AGENTS.md `위생 불변식` 에 한 줄을 둔다.

## Consequences

- 사진이 R2 에서 사라지거나 색인이 블롭 없이 앞서 가면 다음 날 06:15 안에 Slack 이 말한다. 지금은 아무도 몰랐다.
- 색인 적재가 멈추면(11-04 현재 마지막 적재 10-31 18:50) 그 뒤 저장된 사진이 48시간 뒤 고아로 보인다. 그것도
  알아야 할 어긋남이다. 경보 문구가 두 원인(커밋 없는 수집, 밀린 Silver)을 함께 말한다.
- 비용: 하루 약 30 Class A($0.000135) + Trino 4초 안쪽. 블롭 바이트는 읽지 않는다. 상태 파일은 키 3만 개 ≈ 2.6MB.
- 하지 않은 것: 고아 정리(삭제). 정리는 보고서가 몇 run 동안 같은 키를 보인 뒤 사람의 결정으로 한다
  (Iceberg 도 `older_than` 과 dry-run 을 먼저 둔다). 지도 타일은 색인이 없고 통째 교체라(FP-ADR-0004) 이 대조의
  대상이 아니다.
- 대기업 사례: AWS S3 Inventory + Athena 로 객체 목록을 메타데이터 표와 맞추는 대조, Apache Iceberg
  `remove_orphan_files`(목록 대 참조 집합, 나이 문턱), Kubernetes 컨트롤러의 desired/actual 대조(ADR-0300 §2).
