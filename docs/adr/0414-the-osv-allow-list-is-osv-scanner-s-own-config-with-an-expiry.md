# ADR 0414: OSV 허용 목록은 OSV-Scanner 자신의 설정이고, 모든 항목에 만료일이 있다

- Status: Accepted
- Date: 2026-11-04
- Amends: [ADR-0028](./0028-supply-chain-vulnerability-gate-uses-an-osv-ratchet.md) 3·4항 (기준선과 그 검사기),
  [ADR-0278](./0278-security-advisories-reach-slack-through-the-existing-alertmanager.md) 3항 (`advisory` 주석을 내는 곳)

## Context

ADR-0028 의 OSV 게이트는 OSV-Scanner 보고서를 우리 TSV 기준선(`tools/osv-vulnerability-baseline.tsv`)과
우리 검사기(파이썬 200줄, 자기 시험 140줄)로 비교했다. OSV-Scanner v2.5.0(`tools/osv-scanner.env`)은 같은 일을
자기 설정 `[[IgnoredVulns]]`(`id`·`reason`·`ignoreUntil`)로 한다. 2026-11-04 고정 바이너리로 잰 사실:

| 경우 | 종료 코드 |
|---|---|
| 실제 트리 lockfile 12개, `--config osv-scanner.toml` | 0 |
| 같은 트리, 설정 없음 (기준선 5개 권고, 발생 9건) | 1 |
| 합성 `Cargo.lock`(smallvec 1.6.0 = RUSTSEC-2021-0003), 목록에 없음 | 1 |
| 같은 파일, `ignoreUntil` 이 지난 항목 | 1 |
| 같은 파일, 별칭 `GHSA-43w2-9j62-hq99` 만 적음 | 0 (별칭도 무시된다) |

설정은 lockfile 과 같은 디렉터리의 것만 적용되므로, 영역마다 흩어진 lockfile 에는 `--config` 하나로 건다.

## Decision

1. **허용 목록은 루트 `osv-scanner.toml` 하나다.** `cargo xtask ci osv-gate` 가 `--config` 로 모든 추적
   lockfile 에 건다. 목록에 없는 권고는 종료 코드 1 로 게이트를 실패시킨다 — PR #76 에서 RustSec 에 없는
   `GHSA-h395-gr6q-cpjc` 가 통과한 사고, 2026-10-26 zstd 권고(ADR-0302)를 막는 장치가 그대로다.
2. **모든 항목은 `reason` 과 `ignoreUntil` 을 가진다.** 다시 볼 날을 길어야 90일 뒤로 적는다. 날이 지나면
   스캐너가 그 권고를 다시 보고해 게이트가 실패한다. 기준선의 "줄어도 실패"(낡은 행)와 발생 수 세기는
   이 만료로 대신한다 — 낡은 항목은 늦어도 만료 때 다시 검토된다.
3. **`advisory` 주석은 xtask 가 낸다.** 보고서에는 목록이 허용하지 않은 것만 남으므로, GitHub Actions 안에서
   실패할 때 그 묶음마다 `::error title=advisory::<ID> <생태계>/<패키지>` 한 줄을 낸다(ADR-0278 의 읽는 쪽은 그대로).
   npm override 제안(`tools/npm/security-overrides.mjs propose`)도 같은 보고서를 읽으므로 따로 기준선을 걸러 내지 않는다.
4. 기준선 TSV, `scripts/guard/check-osv-vulnerability-baseline.py`, 그 자기 시험은 지운다.

## Consequences

- 우리 코드 약 350줄이 빠지고 판정은 업스트림 스캐너 하나다. 이 결정을 고칠 때는 스캐너 문서의
  설정 형식을 본다(<https://google.github.io/osv-scanner/configuration/>).
- 같은 권고가 새 lockfile 에 하나 더 생겨도 목록의 ID 가 덮으므로 실패하지 않는다. 받아들인 권고가 더 넓게
  퍼지는 것은 만료 때 검토에서 본다.
- 처음 옮긴 다섯 항목은 2027-02-02(90일 뒤) 만료다. minimatch 만 원래 적힌 재확인일 2026-11-30 이다.
