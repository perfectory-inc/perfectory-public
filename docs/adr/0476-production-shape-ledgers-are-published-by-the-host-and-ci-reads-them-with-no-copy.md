# ADR 0476: 운영 표 모양 원장은 호스트가 버킷에 쌓고 CI 가 그것을 읽는다 — PR 복사도 사람 단계도 없다

- Status: Accepted
- Date: 2026-11-11
- Supersedes: [ADR-0279](./0279-a-stale-shape-ledger-arrives-as-a-prepared-pull-request-from-ci.md)
- Amends: [ADR-0259](./0259-ci-migrates-the-production-table-shape.md) §5 (CI 가 판정하는 원장은 커밋된 사본이 아니라 운영이 올린 것)
- Builds on: [ADR-0436](./0436-the-consoles-cloudflare-edge-is-opentofu-applied-by-the-host-with-the-existing-account-token-and-the-allowlist-is-the-identity-staff-list.md)(호스트가 계정 토큰으로 OpenTofu 적용),
  [ADR-0153](./0153-runtime-secrets-have-one-contract.md)·[ADR-0247](./0247-credential-variable-names-are-derived-from-the-cloudflare-token-name.md)(런타임 비밀 계약),
  [ADR-0260](./0260-a-scheduled-job-ends-by-saying-its-outcome-and-silence-is-failure.md)(작업 결과 줄)

## Context

`foundation-shape-ledger-check` 는 운영 표 모양(Iceberg 표의 열·행 유무, Postgres 의 행 있는 표)을 매일 찍는다. CI 는 PR 마다
커밋된 두 원장(`lakehouse-production-shape.json`, `postgres-production-shape.json`)으로 판정한다. 운영이 움직이면 커밋된 원장이
낡고, 48시간이 지나면 점검이 실패한다. 그러면 누군가 `/var/lib/foundation-shape-ledger/*.json` 을 PR 로 복사해야 했다.

ADR-0279 는 그 복사를 없애려 했으나 2026-11-11 W49·W50 실측으로 사슬이 **한 번도 만들어지지 않았다**.

| 고리 | 상태(2026-11-11) |
|---|---|
| 버킷 `foundation-platform-shape-ledger-prod` | 없음(계정 버킷 목록 11개에 없음) |
| 호스트 `/etc/foundation-platform/shape-ledger.env` | 없음 |
| perfectory-ops 환경 `shape-ledger` 의 비밀 | 비어 있음 |
| PR 을 여는 GitHub App | 없음 |
| perfectory-ops 워크플로 | 비교만 하고 PR 은 열지 않음 |

결과: `shape_ledger_check` 가 10-31 부터 매일 실패했다(운영 준비 표 12번, 28 표 낡음). 사장 단계 다섯 개가 그 사슬을 막고 있었다.

가진 열쇠로 무엇을 할 수 있는지 먼저 쟀다(값은 출력하지 않음).

| 열쇠 | 할 수 있는 것 | 못 하는 것 |
|---|---|---|
| 계정 토큰 `foundation-platform-workers-writer-prod`(호스트 `cloudflare-deploy`, 노트북 같은 이름) | R2 버킷 목록·생성, DNS·영역 읽기(ADR-0436), **R2 임시 자격 증명 발급**(`POST /accounts/{id}/r2/temp-access-credentials`, 버킷·접두사·권한·TTL 범위, 실측 200) | 오래 사는 API 토큰 만들기(`/user/tokens`·`/accounts/{id}/tokens` 403, code 9109) |
| 노트북 `gh` | 저장소·환경 비밀 쓰기 | 호스트에서 만든 값을 받을 길이 없다(서버 쓰기·비밀 읽기 금지) |

그래서 브리프가 그린 "버킷 전용 장기 쓰기 열쇠 + CI 의 읽기 전용 장기 열쇠(GitHub 비밀)" 는 지금 열쇠로 만들 수 없다. 장기 토큰을 만들려면
토큰 발급 권한을 가진 새 토큰이 필요하고, 그것은 사장 단계다. 임시 자격 증명은 최대 7일이라 CI 비밀로 두면 매주 누군가 바꿔 넣어야 하고,
호스트가 바꿔 넣으려면 호스트에 GitHub 토큰이 있어야 한다 — ADR-0279 가 거부한 것이다.

업계의 같은 문제와 답:
- **단기·범위 자격 증명**: AWS STS `AssumeRole` + 세션 정책, GCP 단기 서비스 계정 토큰, Vault 동적 비밀. 작업마다 범위를 좁힌 자격 증명을
  받고 끝나면 버린다. R2 임시 자격 증명이 이것의 R2 판이다.
- **내용 주소·불변 객체 + 움직이는 머리 하나**: Git(객체 + ref), Iceberg(메타데이터 파일 + 포인터), OCI 레지스트리(다이제스트 + 태그),
  Terraform 원격 상태. 쌓기만 하고 "지금" 은 포인터 하나다.
- **비밀이 아닌 산출물은 공개로 읽는다**: 공개 패키지 레지스트리, 공개 스키마 레지스트리, Terraform 공개 레지스트리. 원장은 테이블·열
  이름뿐이고 이 공개 저장소에 처음부터 커밋돼 있었다. 읽기 열쇠는 지킬 것이 없는 비밀이다.

## Decision

1. **버킷은 코드다.** `infra/shape-ledger/main.tf` 가 버킷 `foundation-platform-shape-ledger-prod`(apac), 잠금 규칙, 공개 읽기 사용자 도메인
   `shape-ledger.perfectory.io` 를 만든다. `scripts/deploy/edge_settings.py` 가 콘솔 엣지 다음, 앞문 앞에 따로 적용한다(같은 계정 토큰,
   같은 상태 버킷의 `shape-ledger/terraform.tfstate`, 실패는 서로를 막지 않는다). 버킷 이름과 공개 주소는 R2 연결 계약
   (`definitions/platform.definition.json` 의 `connections.shape_ledger`)에서만 온다.
2. **쌓기만 한다.** 버킷 안은 셋이다.
   - `shape-ledger/objects/<원장>/<모양 SHA-256>.json`: 모양(찍은 시각 `captured_at` 을 뺀 정규 JSON)의 해시로 이름 붙인 객체.
     없을 때만 쓴다(`If-None-Match: *`, 있으면 412 로 아무것도 바뀌지 않음).
   - `shape-ledger/history/<시각>.json`: 머리가 가리키는 쌍이 바뀐 때의 머리 사본.
   - `shape-ledger/latest.json`: 유일하게 바뀌는 머리. 두 객체의 키·해시, `checked_at`(운영이 마지막으로 확인한 때), 릴리스.

   objects 와 history 는 R2 잠금 규칙 `Indefinite` 로 누구도 덮거나 지우지 못한다. 같은 모양을 다시 찍으면 머리만 쓴다(바뀐 것만).
3. **쓰기 열쇠는 실행마다 20분.** 점검 유닛의 `ExecStartPre=+`(root, 제어 체크아웃) 가 `scripts/ops/shape-ledger-writer-key.py` 로
   `cloudflare-deploy` 그룹 파일을 스스로 읽어 R2 임시 자격 증명을 받는다: 그 버킷, 접두사 `shape-ledger/` 만, `object-read-write`,
   1,200초. 유닛의 RuntimeDirectory `writer.json`(0400, 점검 계정 소유)에 두고 실행이 끝나면 systemd 가 지운다. 점검 프로세스는 계정
   토큰을 보지 않는다. 오래 사는 쓰기 열쇠는 어디에도 없다. 런타임 비밀 그룹 `shape-ledger` 와 토큰 두 줄(`…-writer-prod`,
   `…-reader-prod`)은 지운다.
4. **CI 는 공개 주소에서 읽는다.** `foundation-ci.yml` 의 세 작업(rust-quality, suites, postgres-integration)이 판정 전에
   `scripts/ci/production-shape-fetch.py` 를 돌린다. 머리를 읽고, 두 객체의 모양 해시를 머리와 맞춰 보고, 작업 트리의 원장 자리에 쓴다.
   해시가 틀리거나 머리가 가리킨 객체가 없으면 단계가 실패한다. 머리가 72시간보다 오래면 경고한다. 공개 워크플로는 여전히 비밀을
   읽지 않는다(`check-workflow-policy.sh` 예외 추가 없음). fork PR 도 같은 원장으로 판정된다.
5. **커밋된 사본은 다음 PR 에서 지운다(확장 → 축소).** 첫 머리가 올라가기 전에는 CI 가 커밋된 원장으로 판정하고 그렇다고 말한다(이 PR 의
   병합 큐 실행이 아직 없는 버킷에 막히지 않게). 첫 머리를 확인한 뒤 후속 PR 이 커밋된 두 파일과 이 대체 경로를 지우고, 호스트에서
   그것을 읽는 곳(`lakehouse_schema_migrate.py` 의 원장 대조, `deploy-steps.contract.json` 의 `per_table`)을 호스트 캡처로 옮긴다.
6. **점검의 실패 기준이 바뀐다.** 운영이 움직인 것은 실패가 아니다(`MOVED …` 로 이름만 적고, 다음 CI 가 그것으로 판정한다).
   올리지 못한 실행은 `waiting`, **48시간 동안 한 번도 올리지 못하면** `failed`(ADR-0260)다. 표 배치 실패(ADR-0314)는 그대로 즉시 실패.
   새 모양을 쓴 실행은 `changed`, 아니면 `unchanged`.
7. **없앤 것**: `scripts/ops/shape-ledger-compare.py`(perfectory-ops 비교), 점검 안의 `refresh_pull_request`·GitHub 호출, App 계획,
   런북의 사장 단계 전부. perfectory-ops 의 `shape-ledger` 워크플로·환경은 그 저장소의 PR 로 지운다.

이 결정의 가드(ADR-0001): `orchestration/tests/test_shape_ledger_check.py` 의
`test_two_published_runs_48_hours_apart_both_pass_though_production_moved` — 2026-10-31~11-11 커밋된 원장이 낡아 점검이 매일 실패하고
사람의 복사를 기다린 사고를 막는다. `scripts/ci/test_production_shape_fetch.py` 의 해시 거부 시험은 머리와 다른 객체로 PR 이 판정되는
것을 막는다.

## Consequences

- 사장 할 일이 0 이다. 버킷·도메인·잠금은 병합 뒤 자동 배포의 엣지 적용이, 열쇠는 매 실행이 만든다.
- 비용(계산): 객체는 모양이 바뀔 때만 쓰인다(추정 하루 0~2회: 표 생성·열 추가·첫 행), 머리는 실행마다 1회(하루 약 2회), 각 수십 KB. 저장 1 GB 미만·
  쓰기 월 수백 회·읽기는 CI 실행당 3회로 R2 무료 한도(저장 10 GB, A 100만·B 1,000만/월) 안이다. 송출 요금은 R2 에 없다. 0원.
- 운영이 바뀌면 다음 점검 뒤의 모든 PR 이 바뀐 운영으로 판정된다. 같은 커밋의 CI 결과가 운영에 따라 달라질 수 있다 — 의도한 것이다
  (낡은 사본으로 통과시키는 것보다 낫다). 실행 요약에 판정한 머리의 `checked_at`·키가 남는다.
- 공개 읽기 도메인이 생긴다. 담긴 것은 표·열 이름과 행 유무뿐이며 이 저장소에 이미 있던 것이다. 원장에 비밀이 될 내용을 넣는 변경은
  이 결정을 다시 열어야 한다.
- 위험: 계정 토큰에 사용자 도메인을 붙일 권한이 없으면 엣지 적용이 실패하고(경보) CI 는 커밋된 원장으로 계속 판정한다(§5 의 대체 경로).
  그 경우에만 토큰 권한 보강이 사장 단계로 남는다. 노트북에서 같은 이름의 토큰으로 버킷 목록·임시 자격 증명은 확인했지만 도메인 생성은
  쓰기라 해 보지 않았다.
- 잠금 규칙 `Indefinite` 는 우리도 지우지 못한다. 쌓이는 양이 작아(모양 하나 수십 KB) 받아들인다.
