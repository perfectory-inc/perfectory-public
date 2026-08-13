---
status: current
owner: gongzzang-제품
doc_type: README
last_reviewed: 2026-07-29
---

# 보안 인프라

Pulumi 인프라 스택이 소비하는 traffic/auth edge 생성 산출물을 둔다.

- `traffic-auth-edge-policy.generated.json`은 `docs/architecture/traffic-auth-policy-registry.v1.json`에서
  생성한 provider 중립 edge ingress projection이다.
- `aws-wafv2-edge-policy.generated.json`은 그 projection에서 파생한 AWS WAFv2/Pulumi 규칙 manifest다.

생성 파일을 직접 수정하지 않는다. 레지스트리를 바꾼 뒤 다음 명령으로 다시 생성한다.
`cargo run -p api --bin generate-traffic-auth-policy`.

현재 상태: WAFv2 규칙 의도는 생성·drift 검사 후 `../index.ts`가 사용한다. 승격 전 local Pulumi
preview가 경고 없이 통과해야 한다. Regional production stack은 `wafRegionalResourceArn`에 대상
ALB/API Gateway ARN을 넣어 WebACL을 연결할 수 있다. CloudFront 연결은 전역 WebACL이 distribution
설정으로 연결되므로 CloudFront distribution module이 소유한다. 운영 배포 admission에는
`GONGZZANG_WAF_REGIONAL_RESOURCE_ARN`과 regional ingress용 `regional_association=planned`
Pulumi preview 증거가 필요하다.
