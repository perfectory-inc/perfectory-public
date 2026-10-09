---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-10-05
---

# 법정동 코드 변경 (code.go.kr) — 수집·적재·스튜어드 런북

[루트 ADR-0143](../../../../docs/adr/0143-legal-dong-code-changes-come-from-code-go-kr.md) 과 [ADR-0144](../../../../docs/adr/0144-region-code-changes-are-derived-from-downloaded-data-only.md) 의
운영 절차다. 법정동 코드 변경은 내려받은 데이터로만 정한다. 날짜와 상위 코드는 code.go.kr 전체 표에서, 이름이 바뀐
동·나뉜 동은 개편 전후 필지 스냅숏의 지번에서, 지번까지 새로 매겨진 동은 필지 번호 공식 이력에서 온다. code.go.kr 의
코드변경안내 게시판은 원천이 아니다.

지역 코드 변경의 정본은 `reference.legal_dong_code_change` 하나다(루트 ADR-0145). 허브 대장 내보내기(표제부·전유부·
면적·허브 공통)가 읽는 시군구 대응표 투영, 행정경계 id 의 선행 코드, 필지 계보의 동 짝은 모두 그 표의 view 다
(`infra/lakehouse/spark/jobs/legal_dong_code_change_views.py`). 손으로 만든 27쌍
(`infra/lakehouse/contracts/sigungu-crosswalk-baseline.json`)은 시험 자료이고, 첫 실운영이 재현할 때까지만
비교 기준이다(6 절). 허브 임시값 코드와 부재 시도 상한은 `hub-register-feed.contract.json` 에 있다.

| 무엇 | 정본 |
| --- | --- |
| 요청 주소·양식·간격(15초 이상)·표 크기 한계·짝 맞추기 시작일·지번 겹침 하한·투영 나이 한계 | `infra/lakehouse/contracts/code-go-kr-legal-dong.contract.json` |
| 수집 작업 일정·풀·켜짐 | `orchestration/jobs.v1.json` 의 `legal_dong_code_changes`, `parcel_number_change_history` (둘 다 default_pool, Spark 없음) |
| 수집 실행 계정·환경·시간 상한 | `infra/systemd/foundation-legal-dong-code.service` → `scripts/ops/legal-dong-code-collect.sh` |
| 적재·짝 맞추기 | `lineage_stewardship` 단위의 첫 단계, `scripts/ops/legal-dong-code-load.sh` |
| 응답 원본 | Bronze `codegokr__legal_dong_code_table` |
| 파서·짝 규칙 | `infra/lakehouse/spark/jobs/code_go_kr_legal_dong.py` (지번 집합은 `parcel_lineage.py` 의 것을 쓴다) |
| 표 | `reference.legal_dong_code_snapshot`(원본), `reference.legal_dong_code_change`(정본, 유일하게 쓰는 표) |
| 지번 근거 | `silver.parcel_boundaries` 의 스냅숏 둘(개편 전·후). 필지 계보는 근거가 아니다(계보가 이 표를 읽는다) |
| 필지 번호 공식 이력 | `silver.parcel_number_change_history` (VWorld 필지고유번호변동연혁 MK/30527). 형식·격리·줄어듦 한계는 `infra/lakehouse/contracts/vworld-parcel-number-change-history.contract.json`, 수집은 `parcel_number_change_history` 작업(`scripts/ops/parcel-number-change-collect.sh`), 상태는 `/var/lib/foundation-platform/parcel-number-change/` |
| 저장하면 안 되는 곳 | `infra/lakehouse/contracts/region-code-holders.json` (폐기된 보관처, 짝 모양; `scripts/guard/region-code-pairs-have-one-home.sh` 가 지킨다) |
| 내보내기가 읽는 투영 | `/var/lib/foundation-platform/legal-dong-code/sigungu-crosswalk.projection.json` (+ 같은 폴더의 `latest-legal-dong-snapshot.json`, 수집 상태 `accepted.json`) |
| 스튜어드 목록 | `/var/lib/foundation-platform/legal-dong-code/steward-review.json` |

## 1. 끝에서 끝까지

```text
legal_dong_code_changes (05:15, default_pool)
  1. collect-code-go-kr-legal-dong             전체 표 → Bronze (요청 둘: 양식 화면, 표)
  2. stage-handoff                             형식·표 크기 검사; 행이 바뀌었으면 → pending/<표 객체>/
                                               바뀐 것이 없으면 넘기지 않는다(journal 에 unchanged)
                                               통과하면 어느 쪽이든 accepted.json 의 checked_at_utc 를 남긴다
parcel_number_change_history (05:25, default_pool; 감독 실행 전까지 꺼짐)
  1. plan/inventory-vworld-dataset-files       30527 목록만. 시도 파일마다 제공자 갱신일을 accepted.json 과 비교
  2. ingest-vworld-dataset-files               갱신일이 바뀐 파일만 Bronze 로(같은 파일 번호라도 다시 받는다)
  3. stage-handoff                             Bronze 에서 읽기 키로 되읽어 형식·격리·줄어듦 검사 → pending/<넘김>/
                                               바뀐 파일이 없으면 넘기지 않는다(journal 에 unchanged)
lineage_stewardship (06:50, spark 3자리)
  0a. legal-dong-code-load.sh                  먼저 30527 대기 넘김을 silver.parcel_number_change_history 에 쌓는다.
                                               그다음 대기 넘김마다: 스냅숏 적재 → 최신 표지 → 짝 맞추기 → 투영·목록 교체
                                               → loaded/ 로 옮김. 실패하면 단위 전체가 실패(계보로 넘어가지 않음)
                                               어느 넘김이든 적재 전에 pairing-owed.json 을 쓰고, 짝 맞추기가 성공해야 지운다(4 절)
  0.. 계보 스튜어드 순환                          (ADR-0115)
허브 내보내기 (수동)
  FOUNDATION_SIGUNGU_CROSSWALK_PROJECTION=…projection.json  → 투영을 읽고 (비교가 켜져 있으면) 기준 27쌍과 비교,
                                               다르면 거부. 수집 확인이 max_age_days 보다 오래됐거나, 수집이 넘긴 표가
                                               투영의 표가 아니거나, 카탈로그의 변경표 스냅숏이 투영의 것과 다르면 거부
```

짝은 이 순서로 정한다(`code_go_kr_legal_dong.pair_changes`). 앞 단계가 정하지 못한 코드만 다음 단계로 간다.

| 순서 | 근거 (`source`) | 언제 짝이 되나 |
| --- | --- | --- |
| 0 | 변경 표에 이미 기록된 짝, 스튜어드 짝(`steward:<id>`) | 그대로 둔다. 원장은 덮어쓰지 않는다 |
| 1 | 날짜·이름 (`derived:code-go-kr:date+name:<날>`) | 같은 날(또는 폐지일 = 생성일 − 1) 생성된 같은 단계 코드 중, 상위 단위가 이어지고 시도를 뺀 이름이 같은 것이 하나 |
| 2 | 필지 번호 공식 이력 (`official:parcel-history`) | 짝 맞추기 기간(계약의 `floor_date` 이후, 날짜를 읽을 수 없는 행 제외) 안의 필지고유번호변동연혁 짝이 정한다: 동 단위 행(대장구분 0, 본번·부번 0000; `detail` = `dong_level`)이 새 코드 하나를 가리키거나, 필지 행이 모두 한 새 동으로 가면서 앞 판의 옛 동 필지를 `jibun_overlap_min_share` 이상 잇는다(`parcel_level`). 그보다 적으면 `awaiting_data`(`official partial`). 표가 비었으면 꺼져 있다 |
| 3 | 지번 겹침 (`derived:parcel-jibun:<판 쌍>`) | 폐지를 감싸는 두 판에서, 같은 시도·변경일에 새로 생겨 필지를 가진 동 중 옛 동의 같은 땅(지목·면적 일치)을 가장 많이 가진 하나가 계약의 `jibun_overlap_min_share` 이상(ADR-0113 §5, ADR-0148) |
| 4 | 상위 단위 묶기 (`derived:children`) | 2·3 으로 짝지어진 동들의 지번이 하한 이상 한 시군구(시도)로 갔다 |

2 와 3 이 같은 동을 서로 다른 새 코드로 정하면, 또는 지번 짝(이미 기록된 것 포함)이나 날짜·이름 짝이 같은 변경의
결정적인 공식 근거가 옮기는 코드(하나·분할·사슬) 어느 것도 아니면
(루트 [ADR-0150](../../../../docs/adr/0150-the-official-parcel-number-history-decides-before-the-jibun-sets.md),
[ADR-0156](../../../../docs/adr/0156-only-decisive-official-evidence-of-the-same-change-contradicts-a-derived-pair.md)) 짝 맞추기는 `PairingConflict` 로 멈춘다: 두 답을 다 적고
아무것도 쓰지 않는다(4 절). 결정적인 근거는 동 단위 행, 또는 앞 판 필지의 `jibun_overlap_min_share` 이상을 한 코드로 옮기는
필지 행이다. 같은 변경은 토지이동일자가 code.go.kr 폐지일이나 그 다음 날인 행이다. 그보다 적은 필지 행(경계 조정)이나 다른
날의 행은 짝을 반박하지 않고, 요약의 `official_partial_moves`(옛 코드별 필지 수)에 보인다. 다른 날의 결정적 근거가 짝과 다른 코드를 가리키면 실행은 계속되지만 요약의 `official_off_window_decisive` 와 스튜어드 목록(`status` = `official_disagrees_off_window`, 승인 대상 아님)에 남는다. 사람이 두 원천을 확인한다(4 절). 어느 쪽도 조용히 이기지 않는다. 공식 필지 행이 옛 동을 여러 새 동으로 나누면 짝이 아니라 `split` 이다.

1–4 는 아무것도 바뀌지 않을 때까지 되풀이한다. 남은 코드는 목록에 상태(판단 대기·분할·스튜어드)와 함께
남는다(5 절). 폴리곤은 어느 단계의 근거도 아니다(ADR-0113 §4).

## 2. Before a hub export

허브 내보내기 넷(`export-building-register-title-silver-handoff`, `…-unit-…`, `…-unit-area-…`, 허브 공통 내보내기)은
대응표 투영 없이는 돌지 않는다.

```bash
export FOUNDATION_SIGUNGU_CROSSWALK_PROJECTION=/var/lib/foundation-platform/legal-dong-code/sigungu-crosswalk.projection.json
```

허브 레인 새로 고침(`scripts/ops/silver-refresh.sh`, [Silver 레인 새로 고침 런북](./silver-refresh.md))은 이 경로를
스스로 둔다. 아래 거부는 그 실행의 journal 에도 같은 문구로 나온다.

내보내기는 Iceberg 카탈로그(`FOUNDATION_PLATFORM_LAKEHOUSE_CATALOG_URI`, `…_WAREHOUSE`, `…_CATALOG_TOKEN`,
`lakehouse-control` 의 `.env.lakehouse`)에 `reference.legal_dong_code_change` 의 현재 스냅숏을 묻는다. 투영이
적은 스냅숏(`change_table_snapshot_id`)과 같아야 한다. 투영 형식은 `sigungu_crosswalk_projection.v2` 이고, 표처럼
옛 코드 → 새 코드(`old_code`, `new_code`) 방향이다.

내보내기가 거부하는 경우와 고치는 법:

| 거부 문구 | 뜻 | 할 일 |
| --- | --- | --- |
| `FOUNDATION_SIGUNGU_CROSSWALK_PROJECTION is not set` | 투영을 가리키지 않았다 | 위 변수를 둔다 |
| `cannot read the crosswalk projection` / `latest-snapshot marker … is missing` | 짝 맞추기가 한 번도 안 돌았다 | 3 절의 감독 첫 실행 |
| `… is stale` | 더 새 전체 표가 적재(또는 수집이 넘김)됐는데 짝 맞추기가 아직 그 표로 돌지 않았거나 실패했다 | `lineage_stewardship` 이 넘김을 적재하게 둔다. 실패했으면 원인을 고치고 다시 돌린다 |
| `collector's state … is missing` | 수집이 한 번도 검사를 통과하지 못했다 | 3 절의 감독 첫 실행 |
| `no collection has confirmed … for N days` | 수집 작업이 `projection.max_age_days` 넘게 성공하지 못했다 | `legal_dong_code_changes` 의 실패 알림·journal 을 본다(4 절) |
| `catalog's current snapshot is …` | 투영을 쓴 뒤 변경표가 바뀌었다(다른 쓰기, 롤백) | 계보 단위를 다시 돌려 투영을 새로 쓴다 |
| `is a view of …, not of reference.legal_dong_code_change` / schema `…v1` | 옛 짝 맞추기가 쓴 투영이다 | 새 릴리스로 계보 단위를 다시 돌린다 |
| `Iceberg catalog is not configured` | 내보내기 환경에 카탈로그 변수가 없다 | `.env.lakehouse` 가 있는 `lakehouse-control` 에서 돌린다 |
| `disagrees with the hand pairs` | 비교가 켜져 있고 투영이 기준 27쌍과 다르다 | 6 절 |
| `baseline_comparison is off without retired_by` | 비교를 증거 없이 껐다 | 6 절의 '기준 비교 끄기' |
| `UnmappedGovernedSigungu` | 통합 시도의 코드에 짝이 없다 | 스튜어드 목록을 본다(5 절) |

내보내기 요약(`sigungu_sido.crosswalk`, 허브 공통은 `sigungu_crosswalk`)에는 투영 경로, sha256, 전체 표 스냅숏,
변경표의 Iceberg 스냅숏 id, 기준 비교가 켜졌는지(`baseline_comparison`)가 남는다.

## 3. Turning it on

작업은 꺼진 채(`enabled: false`) 출시된다. 켜기 전에 한 번 감독해서 돌린다. 순서는 수집 → 짝 맞추기 → 내보내기다.

1. 릴리스가 `/var/lib/foundation-platform/legal-dong-code` 를 만든다(`foundation-release.sh` timers). 없으면 계보
   단위가 시작하지 못하므로 먼저 확인한다.
2. 수집을 손으로 한 번 돌린다.

   ```bash
   sudo systemctl start foundation-legal-dong-code.service
   journalctl -u foundation-legal-dong-code.service --since today -o cat | tail -20  # journal.log 와 같은 줄(ADR-0174)
   ls /var/lib/foundation-platform/legal-dong-code/pending/
   ```

   첫 실행은 넘김 하나를 쓴다.
3. 계보 단위를 한 번 돌린다(Airflow 에서 `lineage_stewardship` 을 수동 실행, 또는 다음 06:50).
4. 투영이 기준 27쌍을 재현했는지 본다. 같으면 아래가 아무 줄도 내지 않는다.

   ```bash
   python3 - <<'PY'
   import json
   p = json.load(open("/var/lib/foundation-platform/legal-dong-code/sigungu-crosswalk.projection.json"))
   s = json.load(open("/opt/foundation-platform/current/infra/lakehouse/contracts/sigungu-crosswalk-baseline.json"))
   a = {(e["old_code"], e["new_code"]) for e in p["sigungu"]}
   b = {(e["old_code"], e["new_code"]) for e in s["sigungu"]}
   print(sorted(a ^ b))
   PY
   ```

5. 별도 PR 에서 `legal_dong_code_changes` 의 `enabled` 를 `true` 로 바꾸고 `disabled_reason` 을 지운다(ADR-0122 §4).

### 지번 근거 켜기 (2·3 단계)

데이터가 없는 동안 날짜·이름 규칙이 못 정한 동(이름이 바뀐 동, 나뉜 동, 지번이 새로 매겨진 동)은
`status = awaiting_data`(판단 대기)로 목록에 남는다. 목록의 `jibun` 칸이 무엇이 없는지(필요한 판, 또는 3 단계의
원천) 말한다. ADR-0144 §4
대로 이름 규칙으로 추정하지 않고, 스튜어드도 이 항목은 승인할 수 없다. 데이터가 들어오면 다음 실행이 정한다.

- **2 단계**는 켜져 있다(`legal-dong-code-load.sh` 의 `--jibun-evidence editions`, 루트 ADR-0148). 폐지된 동마다
  원천 계약(`vworld-parcel-source-objects.json`)의 판 중 폐지일 전에 다 뽑힌 마지막 판과 뒤에 다 뽑힌 첫 판을 고르고,
  `silver.parcel_boundaries` 에서 그 두 판의 PNU 를 읽는다(뒤 판은 폐지된 동이 가졌던 지번만). 개편마다 제 판 쌍을
  쓴다. 후보는 같은 단계의 새 코드 중 옛 코드와 같은 시도(또는 시도 짝이 옮겨 간 시도)에 있고 폐지일이나 그 다음 날
  생긴 것뿐이다: 전국에서 바닥 날짜 뒤에 생긴 아무 코드나 재면, 본번 범위가 넓은 먼 리가 지번을 모두 덮어 진짜
  후계자를 이긴다. 번호가 같아도 같은 땅일 때만 센다: 지목이 같고 면적이 계약의 `pairing.land_match` 안이어야
  한다(지목은 `jibun` 끝 글자, 면적은 경계에서 Spark 실행기가 잰다). 공식 필지 번호 이력이 들어오면 그것이 먼저
  정하고, 지번 단계와 다르면 실행이 `PairingConflict` 로 멈춘다(아무것도 쓰지 않는다; 두 답을 보고 원인을 찾는다).
  판이 계약에 없거나 표에 적재되지 않았으면 그 동은 `awaiting_data` 이고 `jibun` 칸이 필요한 판을 이름으로
  말한다. 판의 수집·적재는 [VWorld 연속지적도 판 런북](./vworld-parcel-editions.md)이다. 환경 변수
  `LEGAL_DONG_PARCELS_BEFORE/AFTER` 는 없어졌다.
- **공식 이력**은 `silver.parcel_number_change_history` 가 행을 가지면 켜진다: `legal-dong-code-load.sh` 가 그 넘김을
  하나라도 적재했으면(`/var/lib/foundation-platform/parcel-number-change/loaded/`) `--official-history-table` 을 넘긴다
  (루트 ADR-0150). 그 전까지 지번이 모두 떠난 동은 판단 대기다. 필지 계보(`silver.parcel_lineage`)를 대신 읽지 않는다:
  계보가 이 표의 동 짝을 읽으므로, 계보를 근거로 읽으면 서로가 서로를 읽는다(루트 ADR-0145 §2). 짝 맞추기 작업에는 그
  옵션이 없고, 시험이 되돌아오는 것을 막는다.

  감독 첫 실행(수집 작업은 그 뒤에 켠다, ADR-0122 §4):

  0. 수집 단위는 Bronze 되읽기용 읽기 키를 전용 파일 `/etc/foundation-platform/lakehouse-reader.env` 에서 받는다
     (계약 `config/runtime-secrets.contract.json` 의 그룹 `lakehouse-reader`, 루트 ADR-0153). root 가 한 번 만든다:
     읽기 키 두 이름(`FOUNDATION_PLATFORM_R2_LAKEHOUSE_READER_ACCESS_KEY_ID`, `..._READER_SECRET_ACCESS_KEY`)만,
     `root:root 0600`. 값은 기존 `parcel-publication.env` 의 같은 이름에서 옮긴다(셸에 찍지 않는다).
     그다음 `sudo python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py host` 가 `OK` 여야
     한다(이름만 출력). 배포의 `verify`·`timers` 도 같은 확인을 하고, 맞지 않으면 멈춘다.
  1. 릴리스가 `/var/lib/foundation-platform/parcel-number-change` 를 만들었는지 본다. 계보 단위의
     `ReadWritePaths` 가 이 경로를 대므로 없으면 계보 단위가 시작하지 못한다.
  2. 수집을 손으로 한 번 돌린다. 첫 실행은 목록의 시도 zip 전부(2026-10-05 측정 19개, 약 8MB)와 테이블 정의서를 받는다.

     ```bash
     sudo systemctl start foundation-parcel-number-change.service
     journalctl -u foundation-parcel-number-change.service --since today -o cat | tail -20  # journal.log 와 같은 줄(ADR-0174)
     ls /var/lib/foundation-platform/parcel-number-change/pending/
     ```

  3. 계보 단위를 한 번 돌린다. journal 에 `parcel-number-change loaded` 가 남고, 짝 맞추기 요약
     (`pairs-summary.json`)의 `official_parcel_links` 가 기간 안 짝 수, `pairs_by_evidence.official_parcel_history` 가
     공식 이력이 정한 짝 수다. 법정동 표 넘김이 없는 날이면 최신 스냅숏으로 짝 맞추기만 다시 돈다.
  4. 공식 이력이 이미 기록된 지번·날짜·이름 짝이나 이번 지번 단계와 다르게 정하면 실행이 `PairingConflict` 로 멈춘다(4 절).
  5. 별도 PR 에서 `parcel_number_change_history` 의 `enabled` 를 `true` 로 바꾸고 `disabled_reason` 을 지운다.

  수집이 넘긴 것을 다시 받고 싶으면 `accepted.json` 에서 그 파일 키(`<download_ds_id>-<파일 번호>`)를 지운다. 다음
  실행이 그 파일을 다시 받는다. 적재 대장이 Bronze 객체 단위로 막으므로 같은 객체가 두 번 쌓이지는 않는다.

### 운영 데이터로 미리 보기 (`--validate-only`)

짝 맞추기를 바꾼 릴리스가 `current` 가 된 뒤, 정기 실행이 변경표에 쌓기 전에 같은 판단을 운영 데이터로 한 번 본다.
최신 스냅숏·기록된 짝·`silver.parcel_boundaries` 의 판 PNU 를 정기 실행과 똑같이 읽고 아무것도 쓰지 않는다.
`lineage_stewardship` 과 같은 spark 자리를 쓰므로 그 단위가 돌지 않을 때 돌린다(Airflow 화면에서 확인).

```bash
# ai-server
sudo systemd-run --wait --pipe --collect -p User=foundation-platform -p Group=foundation-platform \
  $(python3 /opt/foundation-platform/current/scripts/deploy/runtime_secrets.py properties legal-dong-code-validate) \
  /opt/foundation-platform/current/scripts/ops/legal-dong-code-validate.sh
```

출력 첫 줄이 실행 폴더(`/var/lib/foundation-platform/lineage-stewardship/lakehouse/runs/validate-<시각>/`)다. 그 다음
요약 한 줄과 `would append <level> <옛 코드> -> <새 코드> <근거> <상세>` 줄이 쌓였을 짝마다 하나씩 나온다. 볼 것:

- `derived:parcel-jibun:` 짝의 옛 코드와 새 코드가 같은 시도이거나, 옛 시도가 시도 짝으로 옮겨 간 시도다(예: 29·46 →
  전남광주). 다른 시도로 가는 짝이 하나라도 있으면 정기 실행을 멈추고(`legal_dong_code_changes` 를 끈다) 알린다.
- 나뉜 동은 짝이 아니라 `steward-review.json` 의 `status = split` 이고 `split_into` 가 같은 시도의 새 동들이다.
- 요약 줄의 `awaiting_by_sido`·`awaiting_by_evidence` 가 판단 대기를 시도별·기다리는 근거(어느 판, 또는 공식 이력)별로
  센다. 정기 실행의 `pairs-summary.json` 과 `steward-review.json` 에도 같은 칸이 있다.
- `run.log` 에 Spark 오류가 없다. 실패하면 요약이 없어 마지막 python 이 `No such file` 로 끝난다.

## 4. 알림이 뜻하는 것

실패는 단위가 0 이 아닌 값으로 끝나는 것이다. 알림은 Airflow 실패 알림으로 슬랙에 간다(ADR-0122). 작업 기록은
`/var/lib/foundation-platform/legal-dong-code/journal.log` 와 실행마다의 `runs/<시각>/run.log` 에 있고, 운영자는
`journalctl -u <유닛>` 으로 같은 줄과 실패한 실행 로그의 끝을 읽는다(ADR-0174; 상태 디렉터리는 서비스 계정만 읽는다).

| 어디서 | 문구 | 뜻 | 할 일 |
| --- | --- | --- | --- |
| 수집 | `returned HTTP …`, `answered with an empty body` | 사이트가 응답하지 않았다 | 다음 날 다시 받는다. 이틀 넘으면 사이트를 확인한다 |
| 수집 | `columns changed`, `no table headed` | 사이트 화면 형식이 바뀌었다 | 아무것도 적재되지 않았다. 원본은 Bronze 에 있다. 계약의 머리글을 새 형식에 맞추는 PR 을 낸다 |
| 수집 | `TableShrunk` | 전체 표가 직전보다 계약의 한계(1%) 넘게 줄었다 | 깨진 응답이다. 아무것도 넘기지 않았다. 다음 날 다시 받는다 |
| 계보 단위 0a | `TableShrunk`, `SourceFormatError` | 표 스냅숏과 비교해 거부 | 넘김은 `pending/` 에 남는다. 원인을 고친 뒤 단위를 다시 돌린다 |
| 계보 단위 0a | `would be dropped without a word` | 적재 대장이 새 행의 적재 단위를 이미 안다고 했다 | 실행 id 가 겹쳤다는 뜻이다. 행은 쓰지 않았다. 같은 초에 두 실행이 돌았는지 본다 |
| 계보 단위 0a | `read back a crosswalk other than the one this run planned` | 쓰고 다시 읽은 변경표의 view 가 계획과 다르다(그 사이 다른 쓰기) | 투영은 바뀌지 않았다. 다른 쓰기를 찾고 단위를 다시 돌린다 |
| 계보 단위 0a | `PairingConflict` (`the official parcel-number history (…) says …, the 지번 step … says …` 또는 `… (dong_level 또는 parcel_level, dated …) moves it into …, the pair from derived:… says …`) | 공식 이력과 지번 겹침(또는 이미 기록된 파생 짝)이 같은 동을 서로 다른 새 코드로 정했다. 파생 짝은 같은 변경 기간의 결정적 공식 근거와만 비교한다(ADR-0156) | 아무것도 쓰지 않았다. 두 원천을 확인한다: 판이 잘못 적재됐으면 판을 바로잡고, 공식 이력이 틀렸으면 스튜어드 짝으로 기록한다(5 절) |
| 30527 수집 | `the header is not …`, `fields, the header`, `PNU parts are not` | 제공자 파일 형식이 바뀌었다 | 아무것도 넘기지 않았고 상태도 그대로다. 원본은 Bronze 에 있다. 계약의 `header`·`pnu_parts` 를 맞추는 PR 을 낸다 |
| 30527 수집 | `rows have no date, above the contract's share` | 날짜를 읽을 수 없는 행이 계약의 한계를 넘었다(열이 밀렸다) | 위와 같다 |
| 30527 수집 | `shrunk from` | 같은 파일이 직전 판보다 계약의 한계 넘게 줄었다 | 깨진 파일이다. 다음 날 다시 받는다. 계속되면 제공자 화면을 확인한다 |
| 30527 수집 | `did not land in Bronze` | 받기가 실패한 파일이 있다 | 다음 실행이 다시 받는다 |
| 30527 수집 | `refused before any side effect: missing …` (종료 78) | 단위의 환경 파일에 이름이 없다. 아무것도 받거나 쓰지 않았다 | `runtime_secrets.py host` 로 어느 파일에 무엇이 없는지 본다(0 단계) |
| 30527 수집 | `is not a content-addressed key` / `not the ones its key names` | 수집기가 내용 해시 키로 쓰지 않았거나, 되읽은 바이트가 키의 해시와 다르다 | 넘기지 않는다. 릴리스가 `BRONZE_KEY=content_addressed` 를 넘기는지, 객체가 바뀌지 않았는지 본다(루트 ADR-0152) |
| 30527 Bronze | `30527-<번호>.zip` 20개(2026-10-05 16:41Z, 내용 해시 없는 키) | 첫 실행이 넘김 전에 멈추며 남긴 대체된 객체다. 아무 넘김·Silver 행도 가리키지 않는다 | 지우지 않는다(덧붙이기만, 버킷 잠금). R2 재고 감사의 삭제 후보에 나와도 그대로 둔다 |
| 계보 단위 0a | `is not the file the handoff checked` | 넘김의 파일이 검사 뒤 바뀌었다 | 넘김은 `pending/` 에 남는다. 그 넘김을 지우고 `accepted.json` 의 해당 키를 지워 다시 받게 한다 |

넘김이 없는 날은 `legal-dong-code no pending handoff` 가 journal 에 남는다. "아무 일 없음"과 "확인 안 함"을
구별한다.

짝 맞추기를 빚진 동안에는 `legal-dong-code pairing owed since <실행>` 이 실행마다 journal 에 남는다. 빚의 정본은
`/var/lib/foundation-platform/legal-dong-code/pairing-owed.json` 하나다: 0a 가 어느 넘김이든 적재하기 전에 쓰고(빚진
넘김 이름, 짝을 맞출 스냅숏 날짜, 처음 빚진 실행), 짝 맞추기의 쌓기와 설치가 끝난 뒤에만 지운다. 그 파일이 있으면
대기 넘김도 스튜어드 결정도 없는 날에도 최신 스냅숏으로 다시 짝을 맞춘다(`re-paired (pairing owed)`). 짝 맞추기가
실패하면 단위는 여전히 실패하고 파일은 남는다. 그래서 30527 넘김이 `loaded/` 로 옮겨진 뒤 `PairingConflict` 로 멈춘
날(2026-10-06)처럼 대기 넘김이 사라져도, 원인을 고친 다음 실행이 다시 맞춘다. 손으로 파일을 만들거나 지우지 않는다.
성공한 짝 맞추기는 `last-pairing.json` 에 남는다(실행, 스냅숏 날짜, 갚은 빚). 그 기록도 빚도 없는데 `loaded/` 에
넘김이 있으면(이 기록이 생기기 전에 적재한 호스트) 0a 가 그 넘김들로 한 번 빚을 만들고 갚는다: 첫 실행에 짝 맞추기가
한 번 더 돈다.

## 5. 스튜어드 승인

목록은 짝 맞추기마다 `steward-review.json` 으로 바뀐다. 항목은 두 종류다.

- `pair`: 1–4 단계가 짝을 정하지 못한 폐지 코드. `status` 가 누구 몫인지 말한다(ADR-0144 §3–4).

  | `status` | 뜻 | 누가 정하나 |
  | --- | --- | --- |
  | `awaiting_data` | 정할 수 있는 단계의 데이터가 없다(필지 스냅숏 쌍, 또는 지번이 모두 떠났는데 변동연혁이 없다) | 데이터. 스튜어드 승인은 거부된다 |
  | `split` | 지번이 여러 새 동으로 나뉘었고 어느 것도 하한에 못 미친다. `split_into` 가 어디로 몇 개 갔는지 적는다 | 짝이 아니다. 지번별 연결은 필지 계보(ADR-0113)가 한다. 승인은 거부된다 |
  | `steward` | 모든 단계가 데이터를 갖고도 정하지 못했다 | 스튜어드 |

  `jibun` 칸은 2 단계가 본 것이다: `needs … edition …`(판 대기, VWorld 연속지적도 판 런북 5 절), `no parcels before`, `no 지번 in any new code`, 또는
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

## 6. When the crosswalk disagrees with the baseline

`code-go-kr-legal-dong.contract.json` 의 `projection.baseline_comparison.required` 가 `true` 인 동안, 투영이 기준
(`sigungu-crosswalk-baseline.json`)이 다스리는 시도(지금은 12)에서 27쌍과 다르면 내보내기가 거부한다. 차이는
거부 문구에 짝 단위로 나온다.

1. `reference.legal_dong_code_change` 에서 그 시군구 짝의 `source` 와 `rule_verdict` 를 본다. 데이터가 바꾼 것이면
   기준이 낡은 것이다. 기준의 짝은 원래 파생값이다(ADR-0142 Consequences).
2. 규칙이 만든 짝이 틀렸으면 스튜어드가 그 옛 코드에 짝을 승인한다(5 절). 다음 실행의 투영이 바뀐다.
3. 둘 중 어느 쪽인지 정해지기 전에는 내보내기를 돌리지 않는다.

기준이 모르는 새 통합 시도는 차이가 아니다. 지적도에는 없고 짝이 지적도의 옛 시도를 가리키는 시도는 투영이
스스로 다스린다. 짝을 빠뜨리면 내보내기가 그 코드를 대며 멈춘다(ADR-0142).

### Retiring the baseline comparison

ADR-0145 §3: 27쌍은 첫 실운영에서 변경표가 그것을 재현하면 운영 대조에서 빠진다. 끄는 것은 계약 값 하나지만,
증거 없이는 끌 수 없다(내보내기가 거부한다).

1. 3 절 4 번 확인이 아무 줄도 내지 않은 실행을 고른다.
2. 그 실행의 투영 파일에서 `change_table_snapshot_id`, `legal_dong_snapshot_record` 를, 파일 자체의 sha256 을
   `sha256sum sigungu-crosswalk.projection.json` 으로 얻는다.
3. PR 에서 `projection.baseline_comparison` 을 `{"required": false, "retired_by": {"change_table_snapshot_id": …,
   "legal_dong_snapshot_record": …, "projection_sha256": …}}` 로 바꾼다. 그 뒤로 기준 파일은 시험 자료로만 남는다.

## 7. 측정 (2026-10-04)

| 무엇 | 값 |
| --- | --- |
| 전체 표 | 53,403 행, 약 58MB, 요청 약 5초, 파싱 약 7초 |
| 대응표 | 시도 12 가 29·46 을 대체, 시군구 27쌍 = 기준 27쌍. 날짜·이름 규칙만으로 같은 27쌍(시험 시 `--validate-only`) |
| 첫 운영 실행 (2026-10-04, 감독) | 수집 53,403 행 → 계보 단위가 적재·짝 맞추기, 투영 27쌍 = 기준 27쌍(차이 0). 목록 331건 모두 `awaiting_data`(지번 근거 꺼짐). 이 실행이 기준 비교를 끈 증거다(계약의 `retired_by`) |
| 공식 이력·지번 첫 운영 (2026-10-06, 감독) | 30527 수집 741,347 행(19 시도 파일 + 정의서, 내용 키), 재수집은 쓰기 없음. 첫 짝 맞추기는 경계 조정(필지 13개)을 반박으로 읽어 `PairingConflict` 로 아무것도 쓰지 않고 멈췄다 → ADR-0156 으로 고친 뒤, 실패한 짝 맞추기를 다시 하게 한 ADR(PR #349) 으로 재실행: 대기 331 중 294 해소(공식 이력 259, 지번 34, 묶기 1), 충돌 0, 경계 조정 5개 동은 `official_partial_moves` 로만 기록. 남은 37: `awaiting_data` 32(대구 16 은 202610 이후 판, 경기 15·충북 1 은 2026-01 이전 판 필요), `split` 3, `steward` 2 |
| 30527 목록 (2026-10-05) | 파일 20개: 시도 zip 19개(2026-09-15판 15, 전남·광주 2026-06-14판, 특별자치도 이전 이름의 강원·전북 옛 판 둘) + 테이블 정의서 xlsx 1. 모두 단일 파일 내려받기(500MB 미만). 전남광주 통합 파일은 아직 없다 |
| 30527 형식 (인천·경기 실측) | zip 하나에 UTF-8 세로 막대(파이프) 구분 텍스트 하나, 머리글 포함 13칸. 옛 PNU = 앞 5칸, 새 PNU = 다음 5칸(19자리, 전 행). 인천 18,471 행(1995-01-01 ~ 2026-07-01), 경기 356,046 행. 날짜를 읽을 수 없는 행 인천 1·경기 13 |
| 30527 동 단위 행 | 한꺼번에 다시 매긴 동은 필지마다가 아니라 동마다 한 행(대장구분 0)으로 온다. 인천 2026-07-01 은 44행 모두 동 단위(중구 28110 → 28125); 동구·서구·영종·검단 부분은 2026-09-15판에 아직 없다. 화성 2026-02-01 은 195행 모두 동 단위(41590 → 41591 120, 41593 52, 41597 14, 41595 9) |
| 30527 전체 (2026-10-05, 19개 시도 파일) | 741,347 행, 동 단위 18,988. 격리 비율 파일당 0 ~ 1.46e-3(경북 42/28,733), 형식이 어긋난 PNU 1행(서울). 파일 안 완전 중복 0 |
| 30527 코드별 대조 (code.go.kr 폐지 코드) | 인천 2026-07(동 80): 44 동 단위(중구 원도심 → 제물포구), 영종 8·동구 7·서구 21 은 2026 기록 없음. 화성 2026-02(207): 리·동 194 동 단위, 읍·면 코드 13 은 행이 없다(그 아래 리만 온다), 능동은 두 구로 나뉘고 오산동은 다시 폐지된 코드로 간다. 충북 음성 대소면 2026-03-25: 리 12/12, 면 코드는 없음. 대구 달성 구지면 2026-09-30: 0/16(파일이 변경 전 판) |
| 공식 이력 미리 보기 (지번 근거 없이, 기록된 짝 없이) | 판단 대기 331 → 72. 공식 이력이 259 짝을 정했고 모두 동 단위. 날짜·이름 규칙과 충돌 0 |

## 8. 남은 일

- 지번 근거(2 단계)는 판이 계약과 `silver.parcel_boundaries` 에 있어야 정한다. 2026-10-05: 202609 적재 대기, 경기·충북은 앞선 판(NA/23), 대구는 202610 이후 판이 필요하다.
- 필지고유번호변동연혁 수집 작업은 감독 첫 실행 전까지 꺼져 있다(3 절). 제공자 반영이 몇 달 늦으므로 code.go.kr 를
  대신하지 않고 보탠다.
- 필지 계보(ADR-0113)는 아직 이 표의 필지 행을 `official` 등급 근거로 읽지 않는다. 읽게 할 때는 이 표를 직접 읽고
  옮겨 담지 않는다(ADR-0145: 공식 짝의 집은 이 표 하나).
- 시도 단위 통합만 대응표가 다스린다. 인천 구 재편처럼 시도 안의 재번호는 필지 계보(ADR-0113)의 몫이다.
- data.go.kr `getStanReginCdList` 스냅숏 차이(ADR-0104)를 보조 검증으로 붙이는 일은 이 런북 밖이다.
- 스튜어드 화면은 더니어로 옮겨야 한다.
