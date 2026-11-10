---
status: current
owner: foundation-platform
doc_type: runbook
last_reviewed: 2026-11-11
---

# 모양 원장 버킷 — 사람이 하는 일은 없다

[루트 ADR-0476](../../../../docs/adr/0476-production-shape-ledgers-are-published-by-the-host-and-ci-reads-them-with-no-copy.md).
운영 표 모양 원장(Iceberg 표의 열·행 유무, Postgres 의 행 있는 표)은 호스트가 찍어 R2 에 쌓고, CI 는 그것을 공개 읽기 주소에서
받아 PR 을 판정한다. 원장을 PR 로 복사하는 단계도, GitHub 토큰도, App 도, 소유자 단계도 없다.

## 결론

| 무엇 | 어디 | 누가 만드나 |
| --- | --- | --- |
| 버킷 `foundation-platform-shape-ledger-prod` | Cloudflare R2 | `foundation-edge-settings.service` 가 `infra/shape-ledger/main.tf` 를 적용 |
| 공개 읽기 주소 `https://shape-ledger.perfectory.io` | 그 버킷의 사용자 도메인 | 같은 적용 |
| 잠금 규칙(`shape-ledger/objects/`·`shape-ledger/history/` 무기한) | 그 버킷 | 같은 적용 |
| 쓰기 열쇠 | 점검 유닛의 RuntimeDirectory `writer.json`, 실행마다 20분 | 유닛의 root `ExecStartPre` 가 계정 토큰으로 발급(`scripts/ops/shape-ledger-writer-key.py`) |
| 읽기 열쇠 | 없다(공개 읽기). 원장 내용은 이 공개 저장소에 이미 있던 것이다 | — |

## 버킷 안

- `shape-ledger/objects/<원장 이름>/<모양 SHA-256>.json` — 모양마다 한 번만 쓴다(`If-None-Match: *`). 같은 모양을 다시 찍으면 아무것도
  쓰지 않는다. 지우거나 덮을 수 없다(잠금 규칙).
- `shape-ledger/latest.json` — 유일하게 바뀌는 머리. 지금의 두 원장 객체와 `checked_at`(운영이 마지막으로 확인해 준 때).
- `shape-ledger/history/<시각>.json` — 머리가 가리키는 쌍이 바뀐 때의 머리 사본. 지우거나 덮을 수 없다.

## 무엇이 언제 도나

- 점검(`foundation-shape-ledger-check.service`)은 배포 뒤 한 번과 매일 12:30 UTC. 로그의
  `captures published to foundation-platform-shape-ledger-prod/shape-ledger/latest.json` 이 성공이다.
- CI(`foundation-ci.yml` 의 세 작업)는 `scripts/ci/production-shape-fetch.py` 로 머리와 두 객체를 받아 모양 해시를 확인하고 작업 트리의 원장
  자리에 놓은 뒤 판정한다. 실행 요약에 머리의 `checked_at` 과 해시가 남는다.
- 운영이 바뀌면(새 표, 열 추가, 행이 생김) 다음 점검이 새 객체와 머리를 쓰고, 그 뒤의 모든 CI 가 바뀐 운영으로 판정한다.

## 실패와 경보

- `foundation-job-outcome waiting shape ledgers unpublished since …`: 이번 실행이 올리지 못했다(이유가 같은 줄에). 48시간 안이면
  기다린다.
- 48시간 넘게 한 번도 못 올렸으면 `failed … over 48h` 로 실패하고 경보가 간다. 그동안 CI 는 마지막으로 올라간 머리로 판정하며, 그
  머리의 `checked_at` 이 72시간보다 오래면 CI 요약에 경고가 찍힌다.
- 흔한 원인: `ExecStartPre` 줄이 `no key minted: …`(토큰 파일 없음·버킷 아직 없음·R2 거절). 버킷이 없으면 엣지 적용 로그
  `journalctl -u foundation-edge-settings.service` 의 `shape ledger` 줄을 본다.
- 표 배치 실패(`LAYOUT …`)는 원장과 무관하게 바로 실패다(루트 ADR-0314).

## 확인 (읽기만)

```bash
journalctl -u foundation-shape-ledger-check.service --since -2d --no-pager | grep -E 'captures|outcome|minted'
curl -s https://shape-ledger.perfectory.io/shape-ledger/latest.json
```
