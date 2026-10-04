---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-04
---

# 법정동 코드 변경 (code.go.kr) — 수집·적재·스튜어드 런북

[루트 ADR-0143](../../../../docs/adr/0143-legal-dong-code-changes-come-from-code-go-kr.md) 과 [ADR-0144](../../../../docs/adr/0144-region-code-changes-are-derived-from-downloaded-data-only.md) 의
운영 절차다. 법정동 코드 변경은 내려받은 데이터로만 정한다. 날짜와 상위 코드는 code.go.kr 전체 표에서, 이름이 바뀐
동·나뉜 동은 개편 전후 필지 스냅숏의 지번에서, 지번까지 새로 매겨진 동은 필지 번호 공식 이력에서 온다. code.go.kr 의
코드변경안내 게시판은 원천이 아니다. 허브 대장 내보내기(표제부·전유부·면적·허브 공통)는 이 절차가 만든 시군구 대응표
투영을 읽는다. 손으로 만든 27쌍(`sigungu-canonical-crosswalk.contract.json`)은 비교 기준으로만 남는다.

| 무엇 | 정본 |
| --- | --- |
| 요청 주소·양식·간격(15초 이상)·표 크기 한계·짝 맞추기 시작일·지번 겹침 하한·투영 나이 한계 | `infra/lakehouse/contracts/code-go-kr-legal-dong.contract.json` |
| 수집 작업 일정·풀·켜짐 | `orchestration/jobs.v1.json` 의 `legal_dong_code_changes` (default_pool, Spark 없음) |
| 수집 실행 계정·환경·시간 상한 | `infra/systemd/foundation-legal-dong-code.service` → `scripts/ops/legal-dong-code-collect.sh` |
| 적재·짝 맞추기 | `lineage_stewardship` 단위의 첫 단계, `scripts/ops/legal-dong-code-load.sh` |
| 응답 원본 | Bronze `codegokr__legal_dong_code_table` |
| 파서·짝 규칙 | `infra/lakehouse/spark/jobs/code_go_kr_legal_dong.py` (지번 집합은 `parcel_lineage.py` 의 것을 쓴다) |
| 표 | `reference.legal_dong_code_snapshot`, `reference.legal_dong_code_change`, `reference.sigungu_canonical_crosswalk` |
| 지번 근거 | `silver.parcel_boundaries` 의 스냅숏 둘(개편 전·후), 필지 번호 공식 이력은 `silver.parcel_lineage` 의 `official` 행 |
| 내보내기가 읽는 투영 | `/var/lib/foundation-platform/legal-dong-code/sigungu-crosswalk.projection.json` (+ 같은 폴더의 `latest-legal-dong-snapshot.json`, 수집 상태 `accepted.json`) |
| 스튜어드 목록 | `/var/lib/foundation-platform/legal-dong-code/steward-review.json` |

## 1. 끝에서 끝까지

```text
legal_dong_code_changes (05:15, default_pool)
  1. collect-code-go-kr-legal-dong             전체 표 → Bronze (요청 둘: 양식 화면, 표)
  2. stage-handoff                             형식·표 크기 검사; 행이 바뀌었으면 → pending/<표 객체>/
                                               바뀐 것이 없으면 넘기지 않는다(journal 에 unchanged)
                                               통과하면 어느 쪽이든 accepted.json 의 checked_at_utc 를 남긴다
lineage_stewardship (06:50, spark 3자리)
  0a. legal-dong-code-load.sh                  대기 넘김마다: 스냅숏 적재 → 최신 표지 → 짝 맞추기 → 투영·목록 교체
                                               → loaded/ 로 옮김. 실패하면 단위 전체가 실패(계보로 넘어가지 않음)
  0.. 계보 스튜어드 순환                          (ADR-0115)
허브 내보내기 (수동)
  FOUNDATION_SIGUNGU_CROSSWALK_PROJECTION=…projection.json  → 투영을 읽고 씨앗 27쌍과 비교, 다르면 거부
                                               수집 확인이 max_age_days 보다 오래됐거나, 수집이 넘긴 표가 투영의 표가
                                               아니거나, 카탈로그의 대응표 스냅숏이 투영의 것과 다르면 거부
```

짝은 이 순서로 정한다(`code_go_kr_legal_dong.pair_changes`). 앞 단계가 정하지 못한 코드만 다음 단계로 간다.

| 순서 | 근거 (`source`) | 언제 짝이 되나 |
| --- | --- | --- |
| 0 | 변경 표에 이미 기록된 짝, 스튜어드 짝(`steward:<id>`) | 그대로 둔다. 원장은 덮어쓰지 않는다 |
| 1 | 날짜·이름 (`derived:code-go-kr:date+name:<날>`) | 같은 날(또는 폐지일 = 생성일 − 1) 생성된 같은 단계 코드 중, 상위 단위가 이어지고 시도를 뺀 이름이 같은 것이 하나 |
| 2 | 지번 겹침 (`derived:parcel-jibun:<스냅숏 쌍>`) | 개편 뒤 스냅숏에서 새로 필지를 가진 동 중 옛 동 지번을 가장 많이 가진 하나가 계약의 `jibun_overlap_min_share` 이상(ADR-0113 §5) |
| 3 | 필지 번호 공식 이력 (`official:parcel-history`) | 필지 계보의 필지고유번호변동연혁 짝이 옛 동의 필지를 모두 한 새 동으로 잇는다 |
| 4 | 상위 단위 묶기 (`derived:children`) | 2·3 으로 짝지어진 동들의 지번이 하한 이상 한 시군구(시도)로 갔다 |

1–4 는 아무것도 바뀌지 않을 때까지 되풀이한다. 남은 코드는 목록에 상태(판단 대기·분할·스튜어드)와 함께
남는다(5 절). 폴리곤은 어느 단계의 근거도 아니다(ADR-0113 §4).

## 2. Before a hub export

허브 내보내기 넷(`export-building-register-title-silver-handoff`, `…-unit-…`, `…-unit-area-…`, 허브 공통 내보내기)은
대응표 투영 없이는 돌지 않는다.

```bash
export FOUNDATION_SIGUNGU_CROSSWALK_PROJECTION=/var/lib/foundation-platform/legal-dong-code/sigungu-crosswalk.projection.json
```

내보내기는 Iceberg 카탈로그(`FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI`, `…_WAREHOUSE`, `…_CATALOG_TOKEN`,
`lakehouse-control` 의 `.env.lakehouse`)에 `reference.sigungu_canonical_crosswalk` 의 현재 스냅숏을 묻는다. 투영이
적은 스냅숏과 같아야 한다.

내보내기가 거부하는 경우와 고치는 법:

| 거부 문구 | 뜻 | 할 일 |
| --- | --- | --- |
| `FOUNDATION_SIGUNGU_CROSSWALK_PROJECTION is not set` | 투영을 가리키지 않았다 | 위 변수를 둔다 |
| `cannot read the crosswalk projection` / `latest-snapshot marker … is missing` | 짝 맞추기가 한 번도 안 돌았다 | 3 절의 감독 첫 실행 |
| `… is stale` | 더 새 전체 표가 적재(또는 수집이 넘김)됐는데 짝 맞추기가 아직 그 표로 돌지 않았거나 실패했다 | `lineage_stewardship` 이 넘김을 적재하게 둔다. 실패했으면 원인을 고치고 다시 돌린다 |
| `collector's state … is missing` | 수집이 한 번도 검사를 통과하지 못했다 | 3 절의 감독 첫 실행 |
| `no collection has confirmed … for N days` | 수집 작업이 `projection.max_age_days` 넘게 성공하지 못했다 | `legal_dong_code_changes` 의 실패 알림·journal 을 본다(4 절) |
| `catalog's current snapshot is …` | 투영을 쓴 뒤 대응표 표가 바뀌었다(다른 쓰기, 롤백) | 계보 단위를 다시 돌려 투영을 새로 쓴다 |
| `Iceberg catalog is not configured` | 내보내기 환경에 카탈로그 변수가 없다 | `.env.lakehouse` 가 있는 `lakehouse-control` 에서 돌린다 |
| `disagrees with the hand pairs` | 투영이 씨앗 27쌍과 다르다 | 6 절 |
| `UnmappedGovernedSigungu` | 통합 시도의 코드에 짝이 없다 | 스튜어드 목록을 본다(5 절) |

내보내기 요약(`sigungu_sido.crosswalk`, 허브 공통은 `sigungu_crosswalk`)에는 투영 경로, sha256, 전체 표 스냅숏,
대응표 표의 Iceberg 스냅숏 id 가 남는다.

## 3. Turning it on

작업은 꺼진 채(`enabled: false`) 출시된다. 켜기 전에 한 번 감독해서 돌린다. 순서는 수집 → 짝 맞추기 → 내보내기다.

1. 릴리스가 `/var/lib/foundation-platform/legal-dong-code` 를 만든다(`foundation-release.sh` timers). 없으면 계보
   단위가 시작하지 못하므로 먼저 확인한다.
2. 수집을 손으로 한 번 돌린다.

   ```bash
   sudo systemctl start foundation-legal-dong-code.service
   tail -3 /var/lib/foundation-platform/legal-dong-code/journal.log
   ls /var/lib/foundation-platform/legal-dong-code/pending/
   ```

   첫 실행은 넘김 하나를 쓴다.
3. 계보 단위를 한 번 돌린다(Airflow 에서 `lineage_stewardship` 을 수동 실행, 또는 다음 06:50).
4. 투영이 씨앗 27쌍을 재현했는지 본다. 같으면 아래가 아무 줄도 내지 않는다.

   ```bash
   python3 - <<'PY'
   import json
   p = json.load(open("/var/lib/foundation-platform/legal-dong-code/sigungu-crosswalk.projection.json"))
   s = json.load(open("/opt/foundation-platform/current/infra/lakehouse/contracts/sigungu-canonical-crosswalk.contract.json"))
   a = {(e["current_code"], e["superseded_code"]) for e in p["sigungu"]}
   b = {(e["current_code"], e["superseded_code"]) for e in s["sigungu"]}
   print(sorted(a ^ b))
   PY
   ```

5. 별도 PR 에서 `legal_dong_code_changes` 의 `enabled` 를 `true` 로 바꾸고 `disabled_reason` 을 지운다(ADR-0122 §4).

### 지번 근거 켜기 (2·3 단계)

2·3 단계는 꺼진 채 출시된다. 그 동안 날짜·이름 규칙이 못 정한 동(이름이 바뀐 동, 나뉜 동, 지번이 새로 매겨진 동)은
`status = awaiting_data`(판단 대기)로 목록에 남는다. 목록의 `jibun` 칸이 `jibun evidence off` 라고 말한다. ADR-0144 §4
대로 이름 규칙으로 추정하지 않고, 스튜어드도 이 항목은 승인할 수 없다. 데이터가 들어오면 다음 실행이 정한다.

- **2 단계**는 `silver.parcel_boundaries` 에 개편 전 스냅숏과 개편 뒤(새 코드) 스냅숏이 둘 다 있어야 한다. 둘을
  정했으면 계보 단위 환경에 `LEGAL_DONG_PARCELS_BEFORE=<source_snapshot_id>` 와 `LEGAL_DONG_PARCELS_AFTER=<…>` 를
  둔다(`legal-dong-code-load.sh`). 짝 맞추기는 두 스냅숏에서 폐지된 동과 새로 생긴 동의 PNU 만 읽는다.
- **3 단계**는 `LEGAL_DONG_PARCEL_LINEAGE_TABLE=silver.parcel_lineage` 로 켠다. 그 표의 `official` 행 중
  `evidence_kind = parcel_number_history`(필지고유번호변동연혁) 인 짝만 읽는다. 토지이동이력 문구에서 온 `official`
  행은 계보가 자기 동 대응으로 번호를 옮겨 적은 것이라 독립 근거가 아니다. 필지고유번호변동연혁은 아직 수집되지
  않으므로, 지금 켜도 이 단계는 짝을 만들지 않는다.

## 4. 알림이 뜻하는 것

실패는 단위가 0 이 아닌 값으로 끝나는 것이다. 알림은 Airflow 실패 알림으로 슬랙에 간다(ADR-0122). 작업 기록은
`/var/lib/foundation-platform/legal-dong-code/journal.log` 와 실행마다의 `runs/<시각>/run.log` 에 있다.

| 어디서 | 문구 | 뜻 | 할 일 |
| --- | --- | --- | --- |
| 수집 | `returned HTTP …`, `answered with an empty body` | 사이트가 응답하지 않았다 | 다음 날 다시 받는다. 이틀 넘으면 사이트를 확인한다 |
| 수집 | `columns changed`, `no table headed` | 사이트 화면 형식이 바뀌었다 | 아무것도 적재되지 않았다. 원본은 Bronze 에 있다. 계약의 머리글을 새 형식에 맞추는 PR 을 낸다 |
| 수집 | `TableShrunk` | 전체 표가 직전보다 계약의 한계(1%) 넘게 줄었다 | 깨진 응답이다. 아무것도 넘기지 않았다. 다음 날 다시 받는다 |
| 계보 단위 0a | `TableShrunk`, `SourceFormatError` | 표 스냅숏과 비교해 거부 | 넘김은 `pending/` 에 남는다. 원인을 고친 뒤 단위를 다시 돌린다 |
| 계보 단위 0a | `would be dropped without a word` | 적재 대장이 새 행의 적재 단위를 이미 안다고 했다 | 실행 id 가 겹쳤다는 뜻이다. 행은 쓰지 않았다. 같은 초에 두 실행이 돌았는지 본다 |
| 계보 단위 0a | `does not hold the projected entries` | 대응표 표 쓰기가 투영과 어긋났다 | 투영은 바뀌지 않았다. 카탈로그를 확인한다 |

넘김이 없는 날은 `legal-dong-code no pending handoff` 가 journal 에 남는다. "아무 일 없음"과 "확인 안 함"을
구별한다.

## 5. 스튜어드 승인

목록은 짝 맞추기마다 `steward-review.json` 으로 바뀐다. 항목은 두 종류다.

- `pair`: 1–4 단계가 짝을 정하지 못한 폐지 코드. `status` 가 누구 몫인지 말한다(ADR-0144 §3–4).

  | `status` | 뜻 | 누가 정하나 |
  | --- | --- | --- |
  | `awaiting_data` | 정할 수 있는 단계의 데이터가 없다(필지 스냅숏 쌍, 또는 지번이 모두 떠났는데 변동연혁이 없다) | 데이터. 스튜어드 승인은 거부된다 |
  | `split` | 지번이 여러 새 동으로 나뉘었고 어느 것도 하한에 못 미친다. `split_into` 가 어디로 몇 개 갔는지 적는다 | 짝이 아니다. 지번별 연결은 필지 계보(ADR-0113)가 한다. 승인은 거부된다 |
  | `steward` | 모든 단계가 데이터를 갖고도 정하지 못했다 | 스튜어드 |

  `jibun` 칸은 2 단계가 본 것이다: `jibun evidence off`, `no parcels before`, `no 지번 in any new code`, 또는
  `best <코드> share <비율>`(`(tied)` 는 같은 수의 지번을 가진 동이 둘 이상). `candidates` 가 비면 후보 없음이다.
  요약(`pairs-summary.json`)의 `review_by_status` 가 상태별 개수다.
- `crosswalk`: 시군구 하나가 둘로 나뉘거나 둘이 하나로 합쳐져 대응표 한 칸이 될 수 없는 경우.

승인은 파일 한 건이다. 쓰기 전에 지금 목록과 대조한다. 목록에 없는 코드, `steward` 상태가 아닌 항목, 후보 밖의
코드는 그 자리에서 거부된다.

```bash
sudo -u foundation-platform /opt/foundation-platform/current/scripts/ops/legal-dong-code-collect.sh steward \
  --steward <직원 id> --reason "<근거: 문서, 확인한 자료>" --approve <옛 코드>=<새 코드>
```

결정은 `steward/pending/` 에 쌓이고, 다음 계보 단위 실행이 표와 다시 대조해 `steward:<id>` 행으로 기록한다.
기록된 결정은 `steward/recorded/` 로, 그 사이 목록이 바뀌어 맞지 않게 된 결정은 `steward/rejected/` 로 옮겨진다.
옮겨진 이유는 그 실행의 `pairs-summary.json` 에 있다. 대기 넘김이 없는 날에도 대기 중인 결정이 있으면 최신
스냅숏으로 짝 맞추기만 돈다. 그 실행은 자기 실행 id 로 적재하므로, 승인으로 새로 정해진 아래 단위의 짝도 같은
날 기록된다. 화면은 더니어 계보 검토 창구(ADR-0114)로 옮겨 갈 자리다. 그 전까지는 이 명령이다.

## 6. When the crosswalk disagrees with the seed

투영이 씨앗이 다스리는 시도(지금은 12)에서 27쌍과 다르면 내보내기가 거부한다. 차이는 거부 문구에 짝 단위로 나온다.

1. `reference.legal_dong_code_change` 에서 그 시군구 짝의 `source` 와 `rule_verdict` 를 본다. 데이터가 바꾼 것이면
   씨앗이 낡은 것이다. 씨앗을 고치는 PR 을 낸다. 씨앗의 짝은 원래 파생값이다(ADR-0142 Consequences).
2. 규칙이 만든 짝이 틀렸으면 스튜어드가 그 옛 코드에 짝을 승인한다(5 절). 다음 실행의 투영이 바뀐다.
3. 둘 중 어느 쪽인지 정해지기 전에는 내보내기를 돌리지 않는다.

씨앗이 모르는 새 통합 시도는 차이가 아니다. 지적도에는 없고 짝이 지적도의 옛 시도를 가리키는 시도는 투영이
스스로 다스린다. 짝을 빠뜨리면 내보내기가 그 코드를 대며 멈춘다(ADR-0142).

## 7. 측정 (2026-10-04)

| 무엇 | 값 |
| --- | --- |
| 전체 표 | 53,403 행, 약 58MB, 요청 약 5초, 파싱 약 7초 |
| 대응표 | 시도 12 가 29·46 을 대체, 시군구 27쌍 = 씨앗 27쌍. 날짜·이름 규칙만으로 같은 27쌍 |

## 8. 남은 일

- 지번 근거(2 단계)를 켜려면 개편 뒤 코드로 된 필지 스냅숏이 `silver.parcel_boundaries` 에 있어야 한다(3 절).
- 필지고유번호변동연혁(3 단계의 원천)은 아직 수집되지 않는다.
- 시도 단위 통합만 대응표가 다스린다. 인천 구 재편처럼 시도 안의 재번호는 필지 계보(ADR-0113)의 몫이다.
- data.go.kr `getStanReginCdList` 스냅숏 차이(ADR-0104)를 보조 검증으로 붙이는 일은 이 런북 밖이다.
- 스튜어드 화면은 더니어로 옮겨야 한다.
