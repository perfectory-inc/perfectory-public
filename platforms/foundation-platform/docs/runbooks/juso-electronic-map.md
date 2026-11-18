---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-11
---

# 주소정보 전자지도 변동분 수집 런북

루트 ADR-0475 의 운영 절차다. 질문·응답 형식·결과 코드·요청 코드·종류는
`infra/lakehouse/contracts/juso-electronic-map.contract.json` 하나에 있다. 작업은 `juso_electronic_map`
(`foundation-juso-electronic-map.service`, 매일 23:30 UTC), 스크립트는 `scripts/ops/juso-electronic-map-cycle.sh`.

## 승인키 놓기 (한 번)

승인키는 business.juso.go.kr 마이페이지 → 연계신청내역 → 상세에 있다. 값은 어디에도 적지 않는다.

```sh
# 데이터 호스트에서, root 로: 한 줄 FOUNDATION_PLATFORM_JUSO_APP_KEY=<승인키> 인 파일을 만든 뒤
foundation-release.sh env-set juso <그 파일>
```

`/etc/foundation-platform/juso.env` 가 root:root 0600 으로 놓인다(config/runtime-secrets.contract.json 묶음 `juso`).
다음 일정의 run 이 첫 측정 run 이다(루트 ADR-0457). 사람이 시작하지 않는다.

## 기초 전체본 놓기 (딱 한 번, 루트 ADR-0494)

변동분에는 처음 한 번 깔 전체본이 없다. 전체본은 로그인·신청·승인이 있어야 받을 수 있어 작업이 대신 받지 못한다.
**한 번만** 받는다: 그 달 전체본과 그 뒤로 매일 받는 변동분이면 늘 최신이라 다음 달 신청은 없다.

1. **신청(사람)** — business.juso.go.kr → 주소정보 다운로드(`/jst/jstAddressDownload`) → **도로명주소 전자지도**
   (11종: 건물·건물군·도로구간·실폭도로·기초구간·출입구·기초구역·시도·시군구·읍면동·리) → **[월전체] 가장 최근 달**,
   지역 **전국**, 형식 SHP → 장바구니 → 신청하기(본인인증). 2026-11-11 에 **202610 전국 전체분**을 신청했다(처리 중).
2. **받기(사람, 본인인증)** — 승인되면 마이페이지 → 다운로드 신청내역에서 받는다. 압축을 풀지 않는다. 이름도 바꾸지 않는다.
3. **놓기(에이전트)** — 받은 파일을 데이터 호스트의 `/data/foundation-platform/juso-electronic-map-base/<YYYYMM>/` 에
   그대로 둔다(예: `.../202610/`). 계약 `base.inbox`, 디렉터리는 릴리스가 1777 로 만든다(운영자 계정이 쓸 수 있다).
   ```sh
   ssh ai-server mkdir -p /data/foundation-platform/juso-electronic-map-base/202610
   scp <받은 파일> ai-server:/data/foundation-platform/juso-electronic-map-base/202610/
   ```
   옮기는 중인 파일(`.part`·`.crdownload`·`.tmp`·`.partial`, `.` 으로 시작)은 다음 run 이 기다린다.
   전국을 고르면 시도마다 zip 하나씩 16개가 온다(202610: `서울특별시.zip` … `전북특별자치도.zip`, 4,591,506,695 bytes).
   zip 마다 `<시도코드>/` 폴더 하나에 11 레이어의 `.shp`·`.shx`·`.dbf`(`TL_SPBD_BULD` …)와 맨 위 `GRS80_UTMK.prj`·배치 PDF, 35개.
4. **싣기(작업, 사람 없음)** — 다음 일정 run(매일 23:30 UTC)이 zip 마다 안 목록을 읽어 계약 `base.layers` 의 레이어가 다
   있는지 보고, **zip 하나를 Bronze 객체 하나로** 기초 원천 `jusogokr__electronic_map_base`(작업
   `juso_electronic_map_base`, 기간 YYYY-MM, 잎 `base-<YYYYMM>-<시도코드>`)에 바이트 그대로, SHA-256 으로 이름 붙여
   싣는다 — 종류마다 복사하지 않는다. 종류마다 읽을 안 파일은 run 의 `juso_base.members` 와 표지에 적히고,
   `measure-bronze-object-members` 가 zip 안 목록 전체를 장부 옆에 쌓는다. 실은 파일은
   `/var/lib/foundation-platform/juso-electronic-map/collector/base/<YYYYMM>/<파일>.json` 에 적혀 다음 run 은 열지 않는다.
   inbox 의 파일은 지우지 않는다(지워도 Bronze 에는 남는다).

| 결과 | 어디 |
|---|---|
| 실은 파일·객체 키·`replay_from`·종류별 안 파일 | run manifest 의 `base.landed` |
| 받지 못한 것(zip 이 아님, 레이어가 빠진 zip, 달 폴더가 아님, 폴더 안 폴더) | manifest `base.refused`, run 실패 → 알림. 파일을 고쳐 다시 두면 다음 run 이 싣는다 |

Silver 는 종류마다 기초 달의 표 + `replay_from`(기초 달 한 달 전 1일, 202610 이면 2026-09-01)부터의 변동분을 기준일·생성일
순으로 덮는다. 겹치는 날을 다시 덮어도 같은 행이 된다(변동은 행 전체 또는 키 삭제).

## 상태 읽기

| 무엇 | 어디 |
|---|---|
| run 결과 한 줄 | `journalctl -u foundation-juso-electronic-map.service` — `waiting`(키 없음), `ok … {"requests", "bytes", "files", "stops"}`, `COLLECT FAILED …` |
| 코드마다 멈춘 이유(manifest `stops`) | `Done`(어제까지 다 물음 또는 파일 있던 창 30 개), `ByteCap`(run 6 GB), `Deadline`(코드 몫의 시간 예산 — 파일 없는 창은 시간만 센다), 다음 run 이 이어 묻는다 |
| 요청 코드마다 다음에 물을 날 | `/var/lib/foundation-platform/juso-electronic-map/collector/<cntc_cd>/cursor.json` |
| run 의 manifest(물은 창·받은 파일·멈춘 이유) | `/var/lib/foundation-platform/juso-electronic-map/runs/<run>/collect/manifest.json` |
| Bronze | 변동분: `jusogokr__<kind>` 11 원천, 객체 이름 = 파일명·재반영·생성일. 기초: `jusogokr__electronic_map_base` 한 원천, 시도 zip 하나 = 객체 하나 |
| 읽지 못한 응답(죽은 편지) | `/var/lib/foundation-platform/juso-electronic-map/runs/<run>/collect/quarantine/<cntc_cd>-<from>-<to>.bin`(받은 그대로, 계약 `quarantine_bytes` 까지)·`.json`(이유·`total_count`·읽은 기록 수). 한 번만 쓰고 지우지 않는다 |

## 알림이 왔을 때

- **`refused` (E0001–E0004)**: 키가 틀렸거나 승인 전이다. 키를 다시 놓는다. 다음 일정이 스스로 다시 묻는다 — 지울 파일은 없다.
- **`refused` (E1008)**: 연계 신청에 그 요청 코드의 자료가 없다. 신청에 자료(300001·300002·300003·300006)를 더한다.
- **`provider` (E1005·E1999)**: 제공처 오류. 그 코드의 커서는 그대로라 다음 run 이 같은 창을 다시 묻는다.
- **`is not the contract's … it is kept at …`**: 응답이 계약의 모양이 아니다. 그 run 의 `quarantine/` 에 받은 바이트가
  있다. 그것으로 실제 모양을 확인하고 파서·계약을 고친다. 커서는 움직이지 않았으니 다음 run 이 같은 창을 다시 묻는다.
- **그 밖의 실패**: manifest 의 `failed` 가 이유다. 수집기가 잘못 만든 질문이면 코드를 고친다.

## 응답의 모양 (2026-11-11 확인)

제공처 가이드 §3.2 와 제공처 자신의 Java 클라이언트(`ADSClient-1.1.jar`, `ADSReceiver.makeReceiveData`)가 같다.

- 응답 머리 `total_count` 가 기록 수, `err_code` 가 응답 코드다(`normal_count`·`retry_count`·`approval_yn` 도 온다).
- 기록 = 100 B 머리 + 파일. 머리의 파일명이 비어 있으면 파일이 없는 하루의 답이다(예: `E1001`). 그 뒤에는 바이트가 없다.
- 한 창(최대 10일)의 날마다 답이 다를 수 있다(어떤 날은 파일, 어떤 날은 `E1001`). 거절·우리 잘못·제공처 오류가
  하나라도 있으면 그것이 창의 답이다.
