# ADR 0325: 요청 하나가 복사할 수 있는 크기를 넘는 문서는 쪽(page)으로 나눠 서빙하고, 문서 하나가 레인을 멈추지 않는다

- Status: Accepted
- Date: 2026-10-29
- Applies: [ADR-0233](./0233-every-by-pnu-pack-is-read-by-one-rule-head-then-range.md) §3(요청 하나가 복사하는 바이트 상한,
  `PackWriter` 가 지킨다) — 상한은 그대로 두고, 넘는 문서를 거부 대신 쪽으로 낸다
- Keeps: [ADR-0252](./0252-the-building-kinds-are-titles-floors-and-units-keyed-by-building.md)(건물 종류 레인의 키), [ADR-0204](./0204-a-unit-price-document-is-one-building-of-one-pnu.md)(접미 키)

## Context

2026-10-28 14:47–14:53Z, `foundation-by-pnu-serving-bake.service` 의 건물 레인(`building-by-pnu`, `gold.building_panel`)이
문서 스키마 v2 → v4 전량 세대를 굽다가 shard 1 에서 멈췄다.

```
the document of <PNU> is 268404 bytes compressed, above the 262144 one request may copy
(request_cpu_budget.document_max_bytes); narrow the lane's document key (root ADR-0233)
```

같은 거부를 6번 재시도하고 "nothing was published" 로 끝났다. 대단지 PNU 하나(건물 48동, 호 5,433개)의 문서 하나가
레인 전체 5,945,767개 문서의 발행을 막았고, 건물 레인은 10-16 이후 갱신이 없어 서빙 데이터가 585시간 묵었다.
10-14 측정 최대(256,326B, ADR-0233 §3)는 v2 문서였고, v4 가 건물마다 주용도·구조 이름을 더해 상한을 넘었다.

꼬리 전체를 읽기 전용으로 쟀다(2026-10-28, Trino 로 Gold 행을 읽어 서버의 Python 이 v4 문서를 같은 순서·형식으로
재구성해 gzip; 거부된 문서의 실제 크기 268,404B 로 보정, 오차 약 ±5%).

| Gold 행의 JSON 크기(연결 근거 포함) | 행 수 | 문서 gzip |
|---|---:|---|
| < 1MB | 5,938,299 | 1–2MB 행보다 작다 |
| 1–2MB | 5,696 | 표본 564개(10%), 최대 73KB |
| 2–4MB | 1,556 | 전수, 최대 181KB |
| ≥ 4MB | 216 | 전수 |

| 문서 gzip 크기 (2MB 이상 1,772행 전수) | 문서 수 |
|---|---:|
| 192–256 KiB (상한의 75–100%) | 46 |
| 256–320 KiB | 7 |
| 320–400 KiB | 5 |
| 400 KiB 이상 (최대 약 483KB = 상한의 1.84배) | 5 |
| **상한 초과 합계** | **17** |
| 상한의 75% 초과 / 50% 초과 | 63 / 248 |

- 건물 하나의 호 목록 최대 약 160KB(상한의 61%), 표제부·층만은 최대 26KB 다.
- 호 공시가격(`official_price_history`)을 빼도 4개는 상한을 넘는다.

### 대안과 기각

1. **상한을 올린다.** CPU 로는 감당된다: ADR-0233 의 측정(136KB 복사·머리 파싱·이진 탐색 0.06–0.2ms)으로 512KiB 복사는
   약 0.25–0.75ms, 따뜻한 p99 8ms(ADR-0233 2026-10-15 개정)에 더해도 관문 `worker_cpu_p99_max_ms`(10ms) 안이다. 그러나
   오늘의 꼬리(483KB)를 여유 있게 덮으려면 1MiB 가 필요하고, 대단지 호는 계속 늘어 절벽이 옮겨갈 뿐이다. 클라이언트는
   수 MB JSON 을 한 번에 파싱한다. **기각**: 상한은 계약이고 ADR-0233 의 이유("요청 CPU 가 데이터 크기에 비례하지 않는다")는
   그대로 맞다.
2. **문서 키를 `(pnu, building_id)` 로 좁힌다(ADR-0204 접미 키, 과제의 선택지 a).** 건물 하나의 호 목록이 이미 상한의
   61% 이고 단일 건물 최대 호 수는 5,372개(ADR-0076)다 — 건물 키도 상한을 넘을 수 있어 보장이 되지 않는다. 또 화면은 PNU 의
   건물 목록을 한 번에 그리므로 요청이 건물 수(최대 224)만큼 는다. 건물 종류 레인(ADR-0252: 표제부·층은 PNU, 호는
   `(pnu, building_id)`)은 장기 구조로 그대로 두고, 그 레인에도 이 결정의 쪽이 똑같이 적용된다.
3. **쪽으로 나눈다(선택지 b).** 큰 문서만 나뉘고, 나머지 5.9백만 문서는 바이트 그대로이며, 한 요청은 여전히 문서
   멤버 하나만 복사한다. **채택.**

대기업의 같은 문제: DynamoDB 는 항목 400KB 상한을 넘는 데이터를 정렬 키로 여러 항목에 나누고 `Query` 를
`LastEvaluatedKey` 로 쪽 단위로 읽게 한다. Google API 설계 지침 AIP-158 과 GitHub REST(`Link: rel="next"`)는 커지는
목록을 쪽으로 내고 쪽마다 일관된 스냅숏을 요구하며, GitHub 트리 API 는 상한을 넘으면 `truncated: true` 로 *말하고*
조용히 자르지 않는다. Parquet 의 페이지 색인과 ZIP 의 중앙 디렉터리는 큰 객체 안의 조각을 머리의 표 하나로 범위 읽기
가능하게 한다. Kafka 는 `max.message.bytes` 를 넘는 레코드를 데드레터로 보내고 파티션을 멈추지 않는다.

## Decision

1. **상한은 그대로다.** `request_cpu_budget.document_max_bytes`(256KiB)는 바꾸지 않는다(대안 1).
2. **넘는 문서는 쪽으로 서빙한다.** 문서의 gzip 멤버가 `document_max_bytes` 를 넘으면, 굽기는 문서를 순서대로 쪽으로
   자른다. 쪽마다 멤버가 계약 `request_cpu_budget.pages.page_target_bytes`(192KiB, 상한의 75% — ADR-0252 §6 의 경고선이라
   쪽이 4분의 1 자라도 들어간다) 이하가 되는 가장 적은 쪽 수를 고르고, 최대 `pages.max_pages`(64)다.
   - **쪽은 독립된 문서다**: 스키마 `foundation-platform.building_by_pnu_page.v1`(`contracts/by-pnu-documents` 에 기록),
     `page: {index, count}` 와 문서의 `buildings`·`unlinked_units` 중 자기 몫. 건물이 쪽 경계에 걸리면 각 쪽에 같은
     필드로 나오고 층은 첫 쪽에만, 호는 이어진다. 읽는 쪽은 1..count 를 순서대로 잇고 연속한 같은 id 건물을 하나로
     합친다(`join_pages`, 웹 `joinBuildingPages`). 굽기는 쪽을 이은 결과가 원래 문서와 **바이트까지 같을 때만** 쓴다.
   - **자체 스키마**이므로 쪽을 모르는 클라이언트는 쪽 1 을 통문서로 받아들이지 않고 거부한다(조용한 잘림 없음).
   - 상한 이하 문서는 지금과 같은 v4 통문서, 같은 바이트다.
3. **묶음 형식: 머리의 쪽 표.** 쪽 문서의 색인 항목은 쪽 1 의 멤버를 가리키고, 쪽 2.. 의 멤버는 본문에서 바로 뒤에
   잇는다. 묶음 머리 JSON 의 `pages: {"<key>": [len1, len2, …]}` 가 쪽마다 멤버 길이를 적는다. 색인 항목 크기와
   `format_version` 은 그대로이고, 쪽 문서가 없는 묶음은 표가 없어 바이트가 같다. 읽는 쪽(Rust `Pack::page`, Worker
   `pageEntry`)은 항목과 표가 어긋나면(첫 길이가 항목 길이와 다름, 본문 밖, 쪽 수 2..max_pages 밖, 멤버가 상한 초과)
   거부한다.
4. **문서 하나가 레인을 멈추지 않는다(보류, withheld).** 어떤 자름도 맞지 않는 문서(건물·호 하나가 쪽 목표보다 큼,
   또는 max_pages 초과)는 `withheld` 사유를 담은 쪽 문서 하나로 답하고, 나머지는 발행한다. 굽기 요약
   (`PackExportSummary.withheld`)이 키와 사유를 적고, `by-pnu-serving-bake.sh` 는 발행 뒤 `WITHHELD (root ADR-0325 §4)`
   줄을 남기고 실행을 실패로 끝내 유닛 실패 알림(Slack)이 간다. `all` 실행에서는 그 레인이 0 으로 끝나 뒤따르는
   레인(호 가격)도 굽고, 실행 끝에서 실패한다. 문서 수 불변식(문서 수 = Gold 행 수)은 그대로다. 이 가드가 막는
   사고: 2026-10-28 문서 하나가 건물 레인 전량 발행을 6번 거부해 585시간 묵은 데이터를 서빙한 일.
5. **요청 경로.** 쪽 1 은 지금의 `<prefix><pnu>`, 쪽 n 은 `<prefix><pnu>/<n>`(2..max_pages, 앞자리 0 없음 — 쪽 하나에
   경로 하나). Worker 는 PNU 의 항목을 찾은 **그 묶음**에서 쪽을 읽는다: 새 패치가 문서를 통째로 다시 썼으면 옛 기본
   세대의 쪽 2 는 답이 되지 않아(404) 두 굽기의 쪽이 섞이지 않는다. 엣지 사본 키는 쪽 1 이 PNU 그대로, 쪽 n 이
   `<pnu>/<n>` 이다. 웹은 쪽 1 이 쪽 문서면 나머지 쪽을 동시에 읽고, 모든 쪽의 `source.iceberg_snapshot_id`·`count` 가
   같을 때만 잇는다(발행이 사이에 끼면 실패 → 질의 재시도가 쪽 1 부터 다시).
6. **측정을 남긴다.** 굽기 요약은 `paged_documents` 를 센다.

## Consequences

- 건물 레인은 v4 전량 세대를 어차피 다시 구워야 한다(기본 세대가 v2). 이 결정이 더하는 재굽기는 없다. 전량 세대:
  법정동 약 19,010 묶음 → R2 Class A 약 19,010회(약 0.09달러, 약 130원), 굽기 약 40분(ADR-0151 측정 2,329초 기준),
  쪽 문서 17개의 추가 gzip 은 무시할 만하다.
- 배포 순서: Worker(쪽 경로)는 병합 뒤 호스트 자동 배포(ADR-0175)로 굽기보다 먼저 나간다. 웹(공짱)은 이 PR 의
  `building-edge.ts` 가 쪽을 읽는다. 웹이 배포되기 전에는 쪽 문서 PNU 17곳의 건물 패널이 스키마 거부로 오류를 보인다 —
  잘린 목록을 보이지 않는다. 나머지 PNU 는 그대로다.
- 옛 릴리스의 Rust 읽기(`PackHeader` 는 `deny_unknown_fields`)는 쪽 표가 있는 묶음을 거부한다: 되돌리기 뒤 그 동의
  감시·검사가 실패할 수 있다. 서빙(Worker)은 옛 버전도 쪽 1 을 그대로 낸다.
- 다른 레인(필지·종류 레인)의 렌더러는 쪽을 만들지 않는다. 묶음·Worker 의 쪽 읽기는 레인과 무관하므로 그 레인의 문서가
  상한에 닿으면 렌더러에 같은 자름을 붙인다.
