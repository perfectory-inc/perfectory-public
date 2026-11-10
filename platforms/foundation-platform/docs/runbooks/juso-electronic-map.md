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

## 상태 읽기

| 무엇 | 어디 |
|---|---|
| run 결과 한 줄 | `journalctl -u foundation-juso-electronic-map.service` — `waiting`(키 없음), `ok … {"requests", "bytes", "files", "stops"}`, `COLLECT FAILED …` |
| 요청 코드마다 다음에 물을 날 | `/var/lib/foundation-platform/juso-electronic-map/collector/<cntc_cd>/cursor.json` |
| run 의 manifest(물은 창·받은 파일·멈춘 이유) | `/var/lib/foundation-platform/juso-electronic-map/runs/<run>/collect/manifest.json` |
| Bronze | `jusogokr__<kind>` 11 원천, 객체 이름 = 파일명·재반영·생성일 |

## 알림이 왔을 때

- **`refused` (E0001–E0004)**: 키가 틀렸거나 승인 전이다. 키를 다시 놓는다. 다음 일정이 스스로 다시 묻는다 — 지울 파일은 없다.
- **`refused` (E1008)**: 연계 신청에 그 요청 코드의 자료가 없다. 신청에 자료(300001·300002·300003·300006)를 더한다.
- **`provider` (E1005·E1999)**: 제공처 오류. 그 코드의 커서는 그대로라 다음 run 이 같은 창을 다시 묻는다.
- **그 밖의 실패**: manifest 의 `failed` 가 이유다. 수집기가 잘못 만든 질문이면 코드를 고친다.
