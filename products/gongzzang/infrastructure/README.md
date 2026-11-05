---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# infrastructure/

Pulumi(TypeScript) IaC다. 결정은 [ADR-0009](../docs/adr/0009-iac-pulumi.md).

## 지금 있는 것

- `index.ts` — 유일한 Pulumi 프로그램. `security/aws-wafv2-edge-policy.generated.json` manifest를 읽어
  AWS WAFv2 WebACL(`gongzzang-edge-waf`)을 만들고, `wafRegionalResourceArn` config(또는
  `GONGZZANG_WAF_REGIONAL_RESOURCE_ARN`)가 있으면 REGIONAL 대상에 연결한다. 자세한 것은
  [security/README.md](security/README.md).
- `Pulumi.local-preview.yaml` — 자격증명 검사를 끈 `ap-northeast-2` 로컬 preview 스택.
- `package.json` 스크립트 — `typecheck`(`tsc --noEmit`), `preview`(`pulumi preview`).
- `docker/` — 로컬 개발용 Docker Compose([docker/README.md](docker/README.md)). Pulumi와 무관하다.

네트워크·컴퓨트·데이터베이스·메시징·관측성 같은 다른 AWS 자원은 이 디렉터리에서 관리하지 않는다.

## 정책

- 모든 변경은 PR로 하고 `pulumi preview` 결과를 붙인다.
- destroy 작업은 사용자 명시 승인이 필요하다.
- 스택별 secrets는 Pulumi config (`pulumi config set --secret`)에 둔다.
