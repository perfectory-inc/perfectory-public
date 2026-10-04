# ADR 0146: by-PNU 레인은 서빙 중인 것을 검증해 재기준하고, 반영 스냅숏은 태그로 고정한다

- Status: Accepted
- Date: 2026-10-04
- Related: [ADR-0141](./0141-by-pnu-serving-publishes-changed-documents-as-patch-generations.md) (변경 집합과 패치 세대), [ADR-0099](./0099-daily-serving-updates-bake-only-changed-parcels.md) (`row_digest`), [ADR-0138](./0138-scheduled-jobs-share-one-pool-sized-by-every-slot-combination.md), [ADR-0111](./0111-update-map-tiles-by-changed-tiles-served-from-the-edge.md)

## 왜 각주가 아니라 새 ADR 인가

ADR-0141 은 "변경 집합을 계산할 수 없으면 전량 굽기"라고만 정했다. 이 ADR 은 그 자리에 새 운영 경로(검증 재기준,
명령 하나와 manifest 필드 하나)를 더하고, 모든 발행에 새 부작용(Iceberg 태그 쓰기)을 붙인다. 둘 다 ADR-0141 의
결정 범위를 넓히므로 날짜 각주로는 부족하다.

## Context

2026-10-04 필지 레인의 첫 감독 굽기가 거부됐다. 사실은 다음과 같다(운영 ai-server, 읽기 전용 확인).

| 사실 | 근거 |
|---|---|
| 필지 manifest(v1)가 반영한 Gold 스냅숏은 2026-09-09 전국 판이다 | 서빙 manifest |
| 그 스냅숏은 **만료되지 않았다**. `gold.parcel_panel` 메타데이터에 스냅숏 4개가 모두 있다(09-09 두 개, 10-01, 10-04). ref 는 `main` 하나다 | Iceberg REST `loadTable` |
| 거부 이유는 "그 스냅숏에 `row_digest` 가 없다"였다. 지문(ADR-0099)이 생기기 전에 쓴 판이다 | 굽기 작업의 `delta.log` |
| 변경 집합 잡은 "스냅숏 없음"과 "지문 없음"을 같은 종료 코드 3 으로 냈고, 굽기는 둘 다 "no comparison snapshot" 으로 적었다. 그래서 만료로 오인됐다 | `by_pnu_panel_delta.py`, `by-pnu-serving-bake.sh` |
| 전량 굽기는 약 81시간, R2 쓰기(Class A) 약 4천만 회다 | ADR-0141, 런북 |
| 09-09 판과 10-01 판의 내용 차이는 0건으로 측정됐다(옛 판의 지문을 생산자 함수로 계산). 10-04 판은 같은 입력을 무조건 다시 만든 판이다 | 런북 8절 측정 |
| 서빙 객체에는 `source.iceberg_snapshot_id` 가 들어 있다. 내용이 같아도 바이트는 스냅숏마다 다르다 | `parcel_document.rs` |
| 이 저장소의 스냅숏 만료(`lakehouse_maintenance.py`, 7일·`retain_last` 1)는 Silver 적재 뒤에만 돈다. Gold 에는 돈 적이 없다 | `lakehouse-batch-load.sh`, `land-use-batch-load.sh`, 위 메타데이터 |
| R2 Data Catalog 에는 관리형 스냅숏 만료가 있다(켜야 동작, 기본 30일·최근 5개 보존). 태그를 존중하는지는 문서에 없다. Gold 의 스냅숏 4개는 기본 보존 5개 안이라 켜져 있어도 아직 지울 것이 없다 | Cloudflare 문서 "R2 Data Catalog table maintenance" |

지금 실패의 원인은 만료가 아니다. 그러나 반영 스냅숏을 지키는 장치도 없다. Gold 에 만료를 붙이는 날(유지보수를 Gold 로
넓히거나 관리형 만료를 켜는 날) 같은 거부가 진짜 만료로 다시 온다.

## Decision

1. **검증 재기준(verified re-base).** 반영 스냅숏과 비교할 수 없을 때(만료·지문 없음), 운영자는 전량 굽기 대신 서빙
   중인 것을 직접 검증해 레인을 새 스냅숏 위에 다시 세울 수 있다.
   - 명령 `verify-parcel-by-pnu-serving-rebase` 가 manifest 가 서빙하는 객체를 **전부** 읽는다. PNU 마다 가장 새
     패치의 객체, 없으면 기본 세대의 객체다. 읽기 전용이고, 동시성은 상한이 있고(기본 64, 굽기는 128 을 넘긴다),
     첫 자리 샤드·10만 개 묶음 단위로 작업 디렉터리에 기록해 다시 실행하면 이어 한다.
   - 비교는 **`source` 블록을 뺀 내용**이다. 양쪽 다 `source` 를 지우고 키를 정렬한 압축 JSON 의 SHA-256 을 낸다.
     Gold 쪽은 익스포터의 문서 조립 함수(`parcel_document::build`)로 만든 문서다. 바이트 비교는 쓰지 않는다
     (`source` 때문에 늘 다르다).
   - 판정은 넷이다: 같음 / 바뀜 / 서빙에만 있음(Gold 에서 삭제) / Gold 에만 있음(신규, 또는 툼스톤 뒤에 다시 생김).
   - 결과는 변경 집합 잡과 **같은 세 파일**(upserts, deletes, 요약)이다. 그래서 이후는 ADR-0141 의 경로 그대로다.
     전부 같으면 `reflected_gold_iceberg_snapshot_id` 만 옮기는 발행, 다르면 보통의 패치(문서와 툼스톤), 비율을
     넘으면 거부다. 덮어쓰기는 없다.
   - 거부: 읽기 실패 하나(3회 시도 뒤), 목록 개수 ≠ manifest 의 `base_object_count`·패치 개수, 응답 가능한 PNU 수
     ≠ `object_count`, 판정 합 ≠ Gold 행 수, 다른 실행의 작업 디렉터리, 기본 세대의 문서 스키마가 다름,
     변경이 `max_delta_fraction` 초과. 어느 경우든 변경 집합 파일을 쓰지 않는다.
   - 굽기 스크립트의 `FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE=true` 와 필수
     `FOUNDATION_BY_PNU_BAKE_VERIFIED_REBASE_REASON` 으로 켠다(`FORCE_FULL` 과 같은 모양, 함께 쓰면 거부). 필지 레인만
     된다. 건물 문서는 런타임 DB 의 승인 연결을 함께 읽으므로 이 명령이 렌더할 수 없다. 건물은 전량 굽기로 재기준한다.
   - 기록: 발행된 manifest 에 `verified_rebase`(run id, 이유, 방법, 기준 스냅숏, 읽은 수, 판정 수)를 싣는다. 그
     manifest 만 싣고, 다음 발행이 그것을 manifest history 로 옮긴다. 실행 요약에도 run id 와 수치가 남는다.
   - **동등성 근거.** `row_digest` 가 덮는 Gold 내용 칼럼(계약의 칼럼 − `row_digest`·`source_snapshot_id`·
     `published_at_utc`)은 모두 문서에 나온다. 시험이 계약에서 칼럼 목록을 읽어 칼럼마다 값을 바꾸고, 내용
     지문이 바뀌는지 본다. 계보 칼럼과 `source` 만 바꾸면 지문이 그대로인지도 본다. 따라서 이 명령이 "같음"이라 한
     PNU 는 `row_digest` 가 보는 모든 내용을 서빙 문서가 이미 담고 있다. 반대 방향은 일부러 다르다. JSON 섹션
     문자열의 공백·키 순서처럼 DTO 를 거치면 사라지는 차이는 `row_digest` 는 바꾸지만 이 명령은 같다고 본다.
     서빙 문서가 같으므로 다시 구울 이유가 없다.
2. **반영 스냅숏 고정.** 모든 manifest 발행(전량·패치·반영·되돌리기)은 그 manifest 의 반영 스냅숏에 Iceberg 태그
   `served-{unit}-{발행시각}-{스냅숏}` 을 단다.
   - 순서: 태그를 먼저 만든다 → manifest 를 쓴다 → 쓰기가 실패하면 새 태그를 지운다 → 성공하면 같은 레인의 다른
     `served-{unit}-` 태그를 지운다. 태그를 만들 수 없으면 발행을 거부하고 manifest 는 그대로 둔다(되돌리기만 예외, Consequences). 옛 태그 해제가
     실패하면 경고만 남긴다. 고정이 하나 더 남는 쪽은 안전하고, 다음 발행이 지운다.
   - 태그는 옮기지 않는다. 이름 하나는 늘 한 스냅숏을 가리킨다. 그래서 실패한 발행이 서빙 중인 고정을 풀 수 없다.
   - 태그에는 `max-ref-age-ms` 를 주지 않는다. Iceberg 의 `expire_snapshots` 는 살아 있는 ref 가 가리키는 스냅숏을
     지우지 않는다. 시험이 고정된 Iceberg 런타임과 유지보수 잡의 만료 호출(`expire_snapshots_sql`)로 확인한다:
     태그 단 스냅숏은 남고, 태그 없는 옛 스냅숏은 지워지며, 태그를 풀면 다음 만료가 지운다.
   - 쓰는 곳은 퍼블리셔의 REST 카탈로그 클라이언트다(`POST .../tables/{table}`, `set-snapshot-ref`/
     `remove-snapshot-ref`, 요구조건 `assert-ref-snapshot-id`). 같은 카탈로그에 Spark 가 커밋하는 표준 끝점이다.
     로컬 리허설 저장소는 같은 태그를 저장소 옆 파일에 적어, 리허설이 운영 카탈로그를 건드리지 않는다.
3. **두 거부를 구별한다.** 변경 집합 잡은 스냅숏 없음을 3, 지문 없음을 5 로 낸다. 굽기는 각각 다른 문장을 남기고
   실행 요약의 `refused` 에 `no_comparison_snapshot` / `comparison_snapshot_has_no_row_digest` 를 적는다. 두 문장
   모두 재기준 방법 둘(검증 재기준, 전량)을 안내한다.

## 기각한 대안

- **(B) 지문 없는 기준 스냅숏의 `row_digest` 를 생산자 함수로 즉석 계산해 변경 집합을 낸다.** 10-03 측정에서 한 일이고
  469초면 끝난다. 그러나 이것은 "서빙 객체 = 옛 스냅숏을 렌더한 것"을 믿는다. 지금 서빙 중인 기본 세대는 손 스크립트
  (2026-09-12, 저장소 밖)가 구웠고, 그 시기에는 495만 개가 모자란 굽기가 조용히 끝난 일도 있었다(런북 8절). 그 믿음을
  증명하는 것이 없다. 검증 재기준은 서빙 바이트를 직접 읽으므로 그 가정이 필요 없고, 진짜 만료에도 쓸 수 있다.
- **ETag(MD5) 목록 대조.** 목록 요청 4만 번이면 끝나 가장 싸다. 그러나 이 저장소는 ETag 를 내용 증거로 쓰지 않는다
  (`object_storage/copy.rs`: ETag·메타데이터 체크섬은 충분하지 않다). 또 `source` 블록을 옛 값으로 맞춰 다시 렌더해야
  하므로 옛 렌더러와의 바이트 동일성까지 가정한다.
- **보존 하한(최근 N 개 또는 N 일 보존)만으로 지킨다.** 서빙 manifest 는 몇 주씩 같은 스냅숏을 반영할 수 있다. 숫자 하한은
  "얼마나 오래"를 추측해야 하고, 추측이 틀리면 같은 거부가 온다. 태그는 서빙이 끝날 때까지 정확히 그 스냅숏만 지킨다.
- **매번 전량 굽기.** 81시간·4천만 쓰기다. 내용이 바뀌지 않은 날에 그 비용을 낼 이유가 없다.

## Consequences

- 재기준 비용은 읽기다. 필지 전국은 R2 GET(Class B) 39,861,511 회, List(Class A) 약 4만 회, Gold 전 표 스캔 10회다.
  표본 두 시군구(49,080·34,564 객체)가 전부 "같음"이었고, 동시성 256 에서 전국 약 14–18시간으로 추정된다(런북 8절
  "검증 재기준 측정"). 전량 굽기(약 81시간, 쓰기 4천만)와 달리 쓰기는 다른 문서 수만큼이다.
- 모든 발행이 카탈로그에 쓴다(태그 하나 생성, 이전 것 해제). 카탈로그가 태그 커밋을 거부하면 발행이 거부된다.
  R2 Data Catalog 가 `set-snapshot-ref` 를 받는지는 첫 운영 발행에서 확인된다. 거부되면 manifest 는 그대로이고,
  그때는 이 ADR 을 개정해 다른 고정 수단을 정한다.
- R2 관리형 스냅숏 만료가 태그를 존중하는지는 문서에 없다. Gold 에 관리형 만료를 켜기 전에 태그 존중을 같은 방식
  (태그 단 스냅숏을 두고 만료를 돌려 남는지)으로 먼저 확인한다. 이 저장소의 Spark 만료는 존중한다(시험).
- 되돌리기도 발행이므로 되돌린 manifest 의 반영 스냅숏에 새 태그가 붙는다. 그 스냅숏이 이미 만료돼 태그를 달 수
  없으면, 되돌리기는 경고를 남기고 태그 없이 진행하며 이전 태그를 풀지 않는다. 사고에서 돌아가는 길을 고정이 막으면
  안 되기 때문이다. 다음 굽기는 비교할 스냅숏이 없어 거부되고, 그때 검증 재기준이 답이다.
- 건물 레인은 이 명령을 쓰지 못한다(1절). 건물에서 같은 일이 생기면 전량 굽기다.
