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
3. **알림이 죽어 있었다**: 일일 advisory 실패 시 "이슈 하나를 열어 추적"(ADR-0271)하게 돼 있으나, 그 이슈를 **공개 저장소에 만들려 하는데 이 저장소는 GitHub 이슈가 꺼져 있다** → 추적 이슈가 안 만들어지고, push 알림도 없어 **완전 무음**. 3일을 배포·확인 중 우연히 발견했다.

보안 게이트는 설계대로 "고칠 때까지 막는" 게 맞다. 문제는 **고치는 속도**이고, 그건 (a) 보안 수정의 자동 머지와 (b) 막혔음을 즉시 아는 알림, 둘로 좁혀진다.

## Decision

**1. 보안(취약점) 업데이트는 CI 가 녹색이면 자동 머지한다 — major 라도.** Renovate `vulnerabilityAlerts.automerge = true`. **CI 가 관문**이다: 빌드·시험·공급망을 깨는 보안 major 는 큐가 머지하지 않는다(빨간불). 근거: **미패치 취약점 방치 위험 > 회귀 위험**이고, 빌드/시험을 통과하는 변경의 잔여 회귀 위험은 보안 노출보다 작다. 2026-10-26 사례(arrow/parquet 59→60)가 바로 이 모양 — CI 가 호환을 전부 검증했다.

**2. 비보안 major 는 그대로 사람 검토(초안)** — ADR-0097 결정 1 유지. 이 ADR 은 **보안에 한해서만** 예외다.

**3. 실패한 보안 게이트(및 main CI)는 반드시 push 알림을 낸다(ADR-0300 §C 무음 실패 금지).** 현재의 추적-이슈 장치(ADR-0271)는 **이슈가 꺼진 공개 저장소**를 겨냥해 아무것도 못 만든다 → 사실상 알림 없음. 이를 복구한다 — **owner 조치가 필요한 부분**: ① 이 저장소의 Issues 를 켜거나, ② 추적 이슈를 **비공개 `perfectory-inc/perfectory-ops`**(이슈 사용) 로 보내거나(스코프 토큰), ③ Slack/호출기 webhook 을 붙인다. 셋 중 하나가 서기 전까지 GitHub 측 실패의 "무음 실패 금지"는 미충족임을 명시한다.

## Consequences

- 보안 수정(이번 zstd/parquet 포함)이 **사람 없이 자동으로 흐른다** — CI 통과 시. "자동으로 안 되냐"의 답.
- CI 를 깨는 보안 major 는 머지되지 않고 **알림(3번)으로 드러나** 사람이 빠르게 고친다 — 3일 무음 재발 방지.
- 데이터 경로 핵심 의존(arrow/parquet/sqlx 등)이 **CI 를 통과하지만 동작이 미묘하게 바뀌는** 보안 major 는 자동 머지된다 — 그 잔여 위험은 받아들이되, 회귀가 한 번이라도 새면 그 계열을 security-automerge 예외로 뺀다(Open).
- 알림(3번)의 실제 복구는 owner 조치 뒤 완료된다 — 그 전까지 이 ADR 이 그 공백을 소유한다.

## Alternatives rejected

- **보안 major 도 사람 검토 유지** — 이번처럼 전이+major 보안 수정이 며칠씩 막힌다. CI 가 관문이므로 자동 머지가 안전하다. 기각.
- **보안 advisory 를 deny.toml 로 waive** — 구멍을 덮을 뿐 안 고친다. 근본 수정(patched 버전 bump)이 정답. 기각(2026-10-26 zstd 는 #872 로 bump).
- **알림 없이 자동 머지만** — 자동 머지가 CI 에서 깨지는 경우(이번 arrow 처럼 깨졌다면) 다시 무음이 된다. 알림이 짝이다. 기각.

## Open

- 알림 채널(이슈 켜기 / perfectory-ops / Slack) 선택은 owner 결정.
- security-automerge 에서 제외할 "핵심 데이터 경로" 의존 목록은 회귀가 실제로 샐 때 추가.
