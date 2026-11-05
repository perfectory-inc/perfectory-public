---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-11-05
---

# 보안 인프라

Pulumi 인프라 스택이 소비하는 traffic/auth edge 생성 산출물을 둔다.

- `traffic-auth-edge-policy.generated.json`은 `docs/architecture/traffic-auth-policy-registry.v1.json`에서
  생성한 provider 중립 edge ingress projection이다.
- `aws-wafv2-edge-policy.generated.json`은 그 projection에서 파생한 AWS WAFv2/Pulumi 규칙 manifest다.

생성 파일을 직접 수정하지 않는다. 레지스트리를 바꾼 뒤 다음 명령으로 다시 생성한다
(CI `traffic-auth-policy-drift`가 같은 명령으로 drift를 검사한다).
`cargo run -p gongzzang-api --bin generate-traffic-auth-policy`.

현재 상태: `../index.ts`가 WAFv2 manifest를 읽어 WebACL을 만든다. scope는 Pulumi config `wafScope`
(기본 `REGIONAL`, manifest의 `scope_options` 안에서만)로 정한다. REGIONAL 스택은 `wafRegionalResourceArn`
config 또는 `GONGZZANG_WAF_REGIONAL_RESOURCE_ARN`에 대상 ALB/API Gateway ARN을 넣으면 WebACL을 연결한다.
CloudFront scope에서는 ARN 연결이 거부되며, 이 디렉터리에는 CloudFront distribution 연결이 없다.
