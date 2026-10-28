# ADR 0302: 보안(취약점) 의존성 업데이트는 CI 통과 시 자동 머지한다(major 포함), 그리고 실패한 보안 게이트는 반드시 알려야 한다

- Status: Accepted
- Date: 2026-10-27
- Amends: [ADR-0097](./0097-dependencies-track-latest-and-version-pins-live-in-one-contract.md) 결정 1("major 는 사람이 검토하는 초안")을 **보안 업데이트에 한해** 예외로 둔다(비보안 major 는 그대로 사람 검토).
- Builds on: [ADR-0271](./0271-dependency-updates-arrive-as-renovate-pull-requests-and-main-keeps-one-advisory-issue.md)(Renovate PR·일일 advisory·추적 이슈), [ADR-0300 §C](./0300-every-hygiene-family-is-adopted-reconciliation-is-the-spine.md)(무음 실패 금지)
- 관련 사건: 2026-10-26~27 zstd RUSTSEC-2026-0391/0395-0399(parquet 경유 전이 의존)가 공급망 게이트(cargo-deny)를 막아 **모든 PR 머지가 ~3일간 정지**했는데, 아무도 몰랐다.

## Context

우리는 Renovate(ADR-0271)로 의존성 업데이트를 자동화하고, `osvVulnerabilityAlerts` 와 `vulnerabilityAlerts`(즉시, security 라벨)로 보안 업데이트를 연다. 그런데 2026-10-26 zstd advisory 는 **세 가지가 겹쳐 자동으로 안 풀렸다**:

1. **전이 의존 + 부모 major**: 취약한 건 zstd(직접 안 씀), 고치려면 부모 `parquet 59→60`(major)을 올려야 하는데, ADR-0097 은 major 를 **초안(사람 검토)** 으로 연다. 즉 자동 머지 대상이 아니었다.
2. **고친 버전이 방금 나옴**: parquet 60 이 막 릴리스돼, 그 전엔 고칠 버전 자체가 없었다.
3. **알림은 왔으나 "머지 차단"으로 안 읽혔다**: 일일 advisory 실패 시 "이슈 하나를 열어 추적"(ADR-0271)하게 돼 있는데, 그 이슈를 **이슈가 꺼진 공개 저장소에 만들려 해 아무것도 못 만든다**(죽은 장치). **다만 Slack 경보는 따로 작동했다** — 서버 Alertmanager 의 `FoundationSecurityAdvisory`(osv-ratchet)가 이 zstd advisory 들을 `#alerts` 로 쐈다(2026-10-27 실물 확인). 즉 완전 무음이 아니라, 경보가 "advisory 하나 떴다" 수준으로 읽혀 **"모든 PR 머지가 막혔다"는 영향도로 전달되지 못해 며칠 방치**됐다.

보안 게이트는 설계대로 "고칠 때까지 막는" 게 맞다. 문제는 **고치는 속도**이고, 그건 (a) 보안 수정의 자동 머지와 (b) 막혔음을 즉시 아는 알림, 둘로 좁혀진다.

## Decision

**1. 보안(취약점) 업데이트는 CI 가 녹색이면 자동 머지한다 — major 라도.** Renovate `vulnerabilityAlerts.automerge = true`. **CI 가 관문**이다: 빌드·시험·공급망을 깨는 보안 major 는 큐가 머지하지 않는다(빨간불). 근거: **미패치 취약점 방치 위험 > 회귀 위험**이고, 빌드/시험을 통과하는 변경의 잔여 회귀 위험은 보안 노출보다 작다. 2026-10-26 사례(arrow/parquet 59→60)가 바로 이 모양 — CI 가 호환을 전부 검증했다.

**2. 비보안 major 는 그대로 사람 검토(초안)** — ADR-0097 결정 1 유지. 이 ADR 은 **보안에 한해서만** 예외다.

**3. 보안 advisory 의 Slack 경보는 이미 작동한다 — 유지하고, 영향도만 분명히 한다(ADR-0300 §C).** Alertmanager `FoundationSecurityAdvisory` 가 osv-ratchet 로 `#alerts` 에 쏘므로 새 채널·GitHub Slack 앱은 **불필요**하다. 남은 개선은 둘(선택·비긴급): (a) 이 경보에 **"이 advisory 가 모든 PR 머지를 막고 있음"** 영향도를 실어 긴급도를 분명히 한다; (b) ADR-0271 의 **죽은 GitHub-이슈 추적 장치**(이슈 꺼진 저장소 겨냥)는 **제거하거나** perfectory-ops 로 돌린다(지금은 redundant·무해).

## Consequences

- 보안 수정(이번 zstd/parquet 포함)이 **사람 없이 자동으로 흐른다** — CI 통과 시. "자동으로 안 되냐"의 답.
- CI 를 깨는 보안 major 는 머지되지 않고 **알림(3번)으로 드러나** 사람이 빠르게 고친다 — 3일 무음 재발 방지.
- 데이터 경로 핵심 의존(arrow/parquet/sqlx 등)이 **CI 를 통과하지만 동작이 미묘하게 바뀌는** 보안 major 는 자동 머지된다 — 그 잔여 위험은 받아들이되, 회귀가 한 번이라도 새면 그 계열을 security-automerge 예외로 뺀다(Open).
- 알림(3번)은 이미 Slack(`FoundationSecurityAdvisory`→`#alerts`)으로 작동 중이라 **owner 조치 불필요** — 영향도 문구·죽은 이슈장치 처분만 비긴급 후속.

## Alternatives rejected

- **보안 major 도 사람 검토 유지** — 이번처럼 전이+major 보안 수정이 며칠씩 막힌다. CI 가 관문이므로 자동 머지가 안전하다. 기각.
- **보안 advisory 를 deny.toml 로 waive** — 구멍을 덮을 뿐 안 고친다. 근본 수정(patched 버전 bump)이 정답. 기각(2026-10-26 zstd 는 #872 로 bump).
- **알림 없이 자동 머지만** — 자동 머지가 CI 에서 깨지는 경우(이번 arrow 처럼 깨졌다면) 다시 무음이 된다. 알림이 짝이다. 기각.

## Open

- ADR-0271 의 죽은 GitHub-이슈 추적 장치 처분(제거 vs perfectory-ops)과 `FoundationSecurityAdvisory` 경보에 "머지 차단" 영향도 추가 — 둘 다 선택·비긴급(Slack 경보 자체는 작동 중).
- security-automerge 에서 제외할 "핵심 데이터 경로" 의존 목록은 회귀가 실제로 샐 때 추가.
