# AI 주도 유지보수 운영 모델

> **목적**: 비기술 소유자(유지보수를 직접 못 함)를 위해, **시스템과 AI 에이전트가
> 대기업 SRE 역할을 대신** 굴러가게 만드는 설계. 소유자의 인터페이스는 "주당 몇 번의
> 잘 설명된 승인 탭 + 주간 다이제스트 1건"으로 줄인다.
>
> 근거 패턴은 Dependabot/Renovate, OpenSSF Scorecard, cert-manager, Argo CD selfHeal,
> OPA처럼 검증된 자동 유지보수 도구와 운영 모델에서 가져옵니다.
---

## 1. 핵심 전제 — 왜 이 repo는 AI 유지보수가 가능한가

이 모델은 다음과 같은 **기계 강제 운영**을 전제로 합니다.

- 검증 SSOT `cargo xtask verify <area>`와 repository-owned guard
- cargo-deny, gitleaks, SBOM 같은 공급망 게이트
- SLO 정책 SSOT(`docs/observability/slo-policy.v1.example.json`)
- 관측성 규칙과 대시보드의 선언적 정의
- 장애 유형별 runbook

→ **CI가 진짜 권위(authority)다.** AI의 일은 판단을 발명하는 게 아니라,
*사람이 이미 인코딩한 게이트에 대해 repo를 끊임없이 GREEN으로 유지*하는 것.

---

## 2. 3-레인 분류 — 모든 발견은 셋 중 하나

```
발견(finding)
   ├─ AUTONOMOUS      green + 되돌릴 수 있음 + 기존 게이트 안 → AI가 머지
   ├─ ONE-TAP APPROVAL 안전하지만 결과 중대/모호 → AI가 완성+브리프, 소유자 1탭 대기
   └─ NEVER-DELEGATED  §4 → AI 정지, 증거 보존, 에스컬레이션
```

**AUTONOMOUS 예**: 전체 CI(클리피·테스트·cargo-deny·SBOM·check 서브커맨드 테스트) 통과한
patch/minor 의존성 범프, 문서-코드 드리프트 수정, Rust 소스에서 OpenAPI/TS 타입 재생성.
→ AI가 브랜치 → CI가 증명 → 머지.

**ONE-TAP 예**: major 범프, 마이그레이션/auth/PII 건드리는 것, 깨끗한 업그레이드
없는 신규 advisory, 비용 이상치, 멈춘 cron. → AI가 **되돌릴 수 있는** 변경 + 한 화면
평문 브리프("무엇·왜·blast radius·롤백·비용 델타") 준비 후 승인/거절 1탭 대기.
탭은 승인 결정 아티팩트로 기록되어 감사 가능.

**하드 스톱** (AI가 항상 지킴): force-push 금지, reset 금지, AWS/Cloudflare 콘솔
직접 수정 금지(인프라는 Pulumi/코드만), 게이트를 끄거나 우회해서 CI를 통과시키는 것
금지, 파괴적 DB/R2 작업 금지, 시크릿 커밋 금지. **실패한 가드레일을 자기 추론보다
우선**한다.

---

## 3. 스케줄 듀티 — AI 에이전트가 cron으로 소유

> Claude Code의 scheduled/cron 에이전트로 구현. 각 실행은 worktree의 새 세션이고,
> 관련 runbook/정책을 *스펙*으로 읽고, 기존 스크립트를 돌려 **증거 번들**을 낸다
> (느낌이 아니라).

| 듀티 | 주기 | AI가 자율로 | 소유자 승인 필요 |
|------|------|------------|-----------------|
| **의존성 봇 PR 처리** (cargo) | 매 평일 + PR 발생 시 | 봇 설정 선설치 → CI green 확인 → patch/minor & 마이그/auth/PII/event-schema 무관 확인 → 머지. lockfile-only 범프는 배치 | major 범프, 공개 API/OpenAPI/event 스키마 변경, cargo-deny 플래그, 컴파일에 코드변경 필요, deny.toml allow 밖 라이선스 |
| **보안·공급망 스윕** | 매일(advisory) + 주간(gitleaks+SBOM) | cargo-deny/audit, syft SBOM 재생성, gitleaks, 정책 JSON과 diff. 깨끗한 in-policy 업그레이드는 fix PR | 깨끗한 경로 없는 live advisory, 신규 exploitable, gitleaks 적중 → 즉시 에스컬레이션. ignore 목록 무단 수정 절대 안 함 |
| **SLO/알림/readiness** | 매일(합성) + 주간(드리프트) | live-readonly-smoke + k6 read smoke, /healthz·/readyz·/metrics 스크랩, 5xx·p95/p99·DB풀·outbox pending/retry·ingestion freshness·R2 에러를 SLO와 비교. 룰/대시보드가 SSOT와 드리프트하면 sync PR | 바뀐 Prometheus 룰/대시보드의 **프로덕션 배포**(runbook이 별도 ops 단계로 명시), SLO 완화, 실제 breach 알림 ack/snooze. 지속 breach = 사람 호출 |
| **백업-복구 검증** (드릴) | 주간(throwaway 복구) + 매일(존재·신선도) | 최신 PG 백업·R2/Iceberg 스냅샷 존재·신선 확인 → 격리 throwaway DB로 복구 → 마이그상태·스키마계약 체크 + read smoke → 복구시간·무결성 기록 | 공유/프로덕션 리소스 대상 복구, 보존/스케줄 변경, 비용 절감용 백업 삭제. 드릴은 throwaway read-only |
| **문서-코드 드리프트** | 주간 + 매 머지 PR(CI) | 계약/커버리지 가드레일 재실행(openapi/event-schema/runbook/ci-coverage/db-schema/pii), 파생 아티팩트 재생성. 문서가 stale면 doc-sync PR; 코드가 ADR-lock 계약에서 드리프트면 flag | 실제 계약/동작 변경(breaking OpenAPI/event diff, 신규 PII 필드, ADR과 모순되는 마이그). AI는 계약을 바꿔 "고치지" 않고 flag만 |
| **고아 가드레일·경계 감시** | 주간 | 모든 `check-*` 서브커맨드가 테스트를 보유하고 ci.yml 에 배선됐는지 확인, 금지 crate 경계 누출 없음, 임시방편 마커(AGENTS.md §1)·LLM/MCP dep 없음, 아무것도 단언 안 하는 가드레일 없음. 떨어진 체크 재배선 PR | 가드레일을 **제거/약화**(복원과 반대)는 ADR + 소유자 탭 필요. AI는 고아를 자율 복원하되 은퇴는 절대 단독 안 함 |

---

## 4. 절대 위임 불가 (원리상 AI/자동화에 못 맡김)

1. **프로덕션 데이터 삭제/파괴적 변경** — 테이블 drop/truncate, R2/Iceberg 네임스페이스
   purge, user/staff identity 하드 삭제, 실데이터 비가역 작업. AI는 계획·스테이징만, 실행은 사람.
2. **돈 쓰기/비용 약정 변경** — 신규 인프라, V-World 쿼터 증액, 인스턴스 스케일,
   R2 보존/티어 변경. 비용 이상치는 에스컬레이션, 지출로 자가해결 금지.
3. **법무·개인정보·컴플라이언스** — 신규 PII 필드, 보존/라이니지/라이선스 변경,
   공공데이터 재배포, 침해/규제 대응(PIPC/개인정보보호법). 책임지는 사람 필요.
4. **새 아키텍처·비가역 계약 변경** — 새 datastore/프레임워크, 배포된 URL codec/
   OpenAPI/event-schema 깨기, 새 ADR 필요한 것. 하위호환은 영구. AI는 ADR 초안만,
   비준 불가.
5. **auth/인가/신원 변경** — Staff Identity/Authz 로직, 역할, 세션/시크릿 처리. 보안적
   비가역. green CI만으로 안전 증명 안 됨.
6. **가드레일/CI 게이트 끄기·우회** — force-push, reset --hard, 브랜치 삭제, red 머지
   포함. 게이트는 헌법, 개정은 사람만.
7. **사고/장애의 고객·비즈니스 영향에 대한 최종 책임** — 권한 있는 사람이 결과를
   소유해야 함. AI는 책임 주체(of record)가 될 수 없음.

---

## 5. 정직한 한계 — "AI가 알아서 유지보수"가 실제로 깨지는 곳

1. **playbook 없는 새벽 3시 SEV1.** AI는 runbook된 경로(provider-outage,
   lakehouse-incident, iceberg-rollback)와 *탐지*엔 탁월. 그러나 진짜 장애는 보통
   runbook이 *예상 못 한* 케이스 — 롤백하면 정상 데이터도 잃는 부분 손상, "데이터
   손실인가 일시 blip인가"의 모호한 판단, 모든 선택지가 나쁜 cascade. AI는 사람을
   호출하고 **선을 지켜야**(자율 행동 멈춤, 증거 보존, 선택지 제시) 한다. 되돌릴 수
   없는 fix를 즉흥하면 안 됨. **닿을 수 있는 on-call 사람이 없으면, 3am에 권한을
   만들어낼 방법이 정말로 없다.**
2. **게이트가 중재 못 하는 모호한 트레이드오프.** 많은 유지보수 결정은 pass/fail이
   아님: 취약한 transitive dep를 ignore-justification으로 핀할까 vs 릴리스 막을까,
   필요한 fix 위해 SLO 소폭 후퇴 수용할까, 기술부채 지금 vs 나중. CI는 green/red를
   주지 "그럴 가치 있나"를 안 준다. AI는 트레이드오프를 정직히 펼치되, 프로젝트
   리스크 선호 선택은 사람/비즈니스 판단. 자신 있게 말하는 AI가 나쁜 선택을 확정된
   것처럼 보이게 만들 수 있음.
3. **책임은 양도 불가.** green CI는 필요조건이지 충분조건이 아님 — 게이트는 *누군가
   인코딩한 것만* 잡는다. 게이트가 *안* 덮은 게 프로덕션에서 터지면 "CI 통과 후 AI가
   머지했다"는 규제기관·고객·소유자 양심에 답이 안 된다. 유지보수를 직접 못 하는
   비기술 소유자도 책임 주체임을 벗어날 수 없다. **소유자가 모든 탭을 이해 없이
   고무도장 찍으면, one-tap 승인은 연극이 되고 안전모델이 붕괴.**
4. **'유지된 것처럼 보이는' 함정.** AI가 repo를 green으로, 다이제스트를 안심되게
   유지하면 게이트가 측정 안 하는 느린 침식을 가릴 수 있음 — 드리프트하는 비용,
   쌓이는 무시된 advisory, *복구는 되지만 데이터가 미묘히 틀린* 백업, 통과하지만
   아무것도 의미 없게 테스트하는 가드레일. "유지됨"이 "실제 건강함"이 아니라 "모든
   체크 통과"를 조용히 의미할 수 있음. **게이트 자체를 주기적으로 사람이 리뷰하는
   것(우리가 옳은 걸 테스트하나?)은 어떤 스케줄도 자가인증 못 하는 부분.**

---

## 6. 제어 요구사항

이 운영 모델을 활성화하려면 다음 제어가 모두 존재해야 합니다.

- dependency update automation과 명시적 code ownership
- 주기적 보안·공급망 검사와 변경 불가능한 검증 도구 핀
- 모든 PR에 적용되는 required checks와 고아 가드 탐지
- 격리된 backup/restore drill
- §3 듀티의 실행 주체, 증거 보관 위치, 실패 escalation 경로

자동 머지나 scheduled agent보다 branch protection과 required checks가 먼저 강제되어야
합니다. 실제 설치 상태와 실행 이력은 공개 아키텍처 문서가 아니라 private operations
inventory가 소유합니다.

---

## 7. 소유자의 실제 인터페이스 (요약)

```
매일      (아무것도 안 함 — AI가 green 유지, 봇PR 자율머지)
주 N회    잘 설명된 승인 탭 몇 개 (major 범프 / auth·PII 변경 / 비용 이상)
주 1회    다이제스트 1건 읽기: 자율머지된 것 / 대기 중 / 저하 중 / 안전하게 못 건드린 것
분기 1회  ★사람만 가능★ 게이트 자체 리뷰: "우리가 옳은 걸 테스트하나?"
사고 시   ★사람만 가능★ playbook 없는 판단 + 최종 책임
```

이 4줄이 "대기업 수준 유지보수를 비기술 소유자가 감당 가능하게" 만드는 전부다.
나머지는 시스템과 AI가 진다. **단, 분기 게이트 리뷰와 사고 판단·책임은 양도되지
않는다** — 그게 정직한 경계다.
